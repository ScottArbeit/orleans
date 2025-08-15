using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Implementation of <see cref="IQueueAdapterReceiver"/> for Azure Service Bus streaming.
/// Provides the receiving path to dequeue messages from Service Bus entities and feed them to the cache.
/// This implementation follows non-rewindable semantics similar to ASQ/SQS.
/// </summary>
internal class ServiceBusAdapterReceiver : IQueueAdapterReceiver, IDisposable
{
    private readonly QueueId _queueId;
    private readonly ServiceBusStreamOptions _options;
    private readonly ServiceBusDataAdapter _dataAdapter;
    private readonly ILogger<ServiceBusAdapterReceiver> _logger;
    private readonly ServiceBusClient _serviceBusClient;
    private readonly ServiceBusStreamFailureHandler? _failureHandler;
    private ServiceBusReceiver? _serviceBusReceiver;
    private readonly ConcurrentQueue<ReceivedMessage> _messageQueue;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private Task? _backgroundPumpTask;
    private bool _disposed;
    private volatile bool _shutdown;
    private long _sequenceCounter;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusAdapterReceiver"/> class.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <param name="dataAdapter">The data adapter for message conversion.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="failureHandler">Optional failure handler for tracking delivery failures.</param>
    public ServiceBusAdapterReceiver(
        QueueId queueId,
        ServiceBusStreamOptions options,
        ServiceBusDataAdapter dataAdapter,
        ILogger<ServiceBusAdapterReceiver> logger,
        ServiceBusStreamFailureHandler? failureHandler = null)
    {
        _queueId = queueId;
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _dataAdapter = dataAdapter ?? throw new ArgumentNullException(nameof(dataAdapter));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _failureHandler = failureHandler;

        _serviceBusClient = CreateServiceBusClient(options);
        _messageQueue = new ConcurrentQueue<ReceivedMessage>();
        _cancellationTokenSource = new CancellationTokenSource();

        _logger.LogInformation(
            "ServiceBus adapter receiver initialized for queue {QueueId} with entity kind '{EntityKind}', " +
            "entity name '{EntityName}', batch size {BatchSize}, prefetch count {PrefetchCount}",
            _queueId,
            _options.EntityKind,
            ServiceBusEntityNamer.GetEntityName(_options),
            _options.Receiver.ReceiveBatchSize,
            _options.Receiver.PrefetchCount);
    }

    /// <summary>
    /// Initializes the receiver by creating the Service Bus receiver and starting the background pump.
    /// </summary>
    /// <param name="timeout">The timeout for initialization.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task Initialize(TimeSpan timeout)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ServiceBusAdapterReceiver));
        }

        try
        {
            // Create Service Bus receiver based on entity type
            _serviceBusReceiver = CreateServiceBusReceiver(_serviceBusClient, _options);

            // Start the background pump
            _backgroundPumpTask = Task.Run(async () => await BackgroundPumpAsync(_cancellationTokenSource.Token));

            _logger.LogInformation(
                "ServiceBus adapter receiver initialized and background pump started for queue {QueueId} " +
                "on entity '{EntityName}'",
                _queueId,
                ServiceBusEntityNamer.GetEntityName(_options));

            await Task.CompletedTask;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to initialize ServiceBus adapter receiver for queue {QueueId} on entity '{EntityName}'",
                _queueId,
                ServiceBusEntityNamer.GetEntityName(_options));
            throw;
        }
    }

    /// <summary>
    /// Retrieves message batches from the internal queue populated by the background pump.
    /// </summary>
    /// <param name="maxCount">The maximum number of message batches to retrieve.</param>
    /// <returns>The message batches.</returns>
    public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        if (_disposed || _shutdown)
        {
            return Task.FromResult<IList<IBatchContainer>>(Array.Empty<IBatchContainer>());
        }

        var messages = new List<IBatchContainer>();
        var count = 0;

        while (count < maxCount && _messageQueue.TryDequeue(out var receivedMessage))
        {
            try
            {
                // Convert Service Bus message to batch container using the data adapter
                var sequenceId = Interlocked.Increment(ref _sequenceCounter);
                var batchContainer = _dataAdapter.FromQueueMessage(receivedMessage.ServiceBusReceivedMessage, sequenceId);
                
                // Store the received message for later completion/abandonment
                batchContainer.ReceivedMessage = receivedMessage;
                messages.Add(batchContainer);
                count++;

                _logger.LogTrace(
                    "Converted Service Bus message {MessageId} to batch container for stream {StreamNamespace}:{StreamKey}",
                    receivedMessage.ServiceBusReceivedMessage.MessageId,
                    batchContainer.StreamId.GetNamespace(),
                    batchContainer.StreamId.GetKeyAsString());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed to convert Service Bus message {MessageId} to batch container, abandoning message",
                    receivedMessage.ServiceBusReceivedMessage.MessageId);

                // Abandon the message on conversion failure
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await _serviceBusReceiver!.AbandonMessageAsync(receivedMessage.ServiceBusReceivedMessage);
                    }
                    catch (Exception abandonEx)
                    {
                        _logger.LogWarning(abandonEx,
                            "Failed to abandon Service Bus message {MessageId} after conversion failure",
                            receivedMessage.ServiceBusReceivedMessage.MessageId);
                    }
                });
            }
        }

        if (messages.Count > 0)
        {
            _logger.LogDebug(
                "Retrieved {MessageCount} messages for queue {QueueId} from Service Bus entity '{EntityName}'",
                messages.Count,
                _queueId,
                ServiceBusEntityNamer.GetEntityName(_options));
        }

        return Task.FromResult<IList<IBatchContainer>>(messages);
    }

    /// <summary>
    /// Notifies the receiver that messages were delivered successfully, so they can be completed.
    /// On failure, messages will be abandoned for redelivery.
    /// Messages marked as having delivery failures will be abandoned to allow Service Bus retries and DLQ handling.
    /// </summary>
    /// <param name="messages">The message batches that were delivered.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        if (_disposed || _serviceBusReceiver is null)
        {
            return;
        }

        var tasks = new List<Task>();
        var completedCount = 0;
        var abandonedCount = 0;

        foreach (var message in messages)
        {
            if (message is ServiceBusBatchContainer serviceBusContainer && 
                serviceBusContainer.ReceivedMessage is not null)
            {
                var receivedMessage = serviceBusContainer.ReceivedMessage;
                
                // Check if delivery failed using the failure handler, or fallback to batch container flag
                var deliveryFailed = (_failureHandler?.IsTokenFailed(serviceBusContainer.SequenceToken) == true) || 
                                   serviceBusContainer.DeliveryFailed;
                                   
                if (deliveryFailed)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            await _serviceBusReceiver.AbandonMessageAsync(receivedMessage.ServiceBusReceivedMessage);
                            Interlocked.Increment(ref abandonedCount);
                            
                            // Clear the failed token from tracking
                            _failureHandler?.ClearFailedToken(serviceBusContainer.SequenceToken);
                            
                            _logger.LogDebug(
                                "Abandoned Service Bus message {MessageId} for stream {StreamNamespace}:{StreamKey} due to delivery failure",
                                receivedMessage.ServiceBusReceivedMessage.MessageId,
                                serviceBusContainer.StreamId.GetNamespace(),
                                serviceBusContainer.StreamId.GetKeyAsString());
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex,
                                "Failed to abandon Service Bus message {MessageId} after delivery failure",
                                receivedMessage.ServiceBusReceivedMessage.MessageId);
                        }
                    }));
                }
                else
                {
                    // Delivery succeeded, complete the message
                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            await _serviceBusReceiver.CompleteMessageAsync(receivedMessage.ServiceBusReceivedMessage);
                            Interlocked.Increment(ref completedCount);
                            
                            _logger.LogTrace(
                                "Completed Service Bus message {MessageId} for stream {StreamNamespace}:{StreamKey}",
                                receivedMessage.ServiceBusReceivedMessage.MessageId,
                                serviceBusContainer.StreamId.GetNamespace(),
                                serviceBusContainer.StreamId.GetKeyAsString());
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex,
                                "Failed to complete Service Bus message {MessageId}, abandoning for redelivery",
                                receivedMessage.ServiceBusReceivedMessage.MessageId);

                            try
                            {
                                await _serviceBusReceiver.AbandonMessageAsync(receivedMessage.ServiceBusReceivedMessage);
                                Interlocked.Increment(ref abandonedCount);
                            }
                            catch (Exception abandonEx)
                            {
                                _logger.LogError(abandonEx,
                                    "Failed to abandon Service Bus message {MessageId} after completion failure",
                                    receivedMessage.ServiceBusReceivedMessage.MessageId);
                            }
                        }
                    }));
                }
            }
        }

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks);
            
            _logger.LogDebug(
                "Message delivery completed for queue {QueueId}: {CompletedCount} completed, {AbandonedCount} abandoned",
                _queueId,
                completedCount,
                abandonedCount);
        }
    }

    /// <summary>
    /// Shuts down the receiver and stops the background pump.
    /// </summary>
    /// <param name="timeout">The timeout for shutdown.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task Shutdown(TimeSpan timeout)
    {
        if (_disposed)
        {
            return;
        }

        _logger.LogInformation(
            "Shutting down ServiceBus adapter receiver for queue {QueueId} on entity '{EntityName}'",
            _queueId,
            ServiceBusEntityNamer.GetEntityName(_options));

        _shutdown = true;

        // Cancel the background pump
        _cancellationTokenSource.Cancel();

        // Wait for the background pump to finish
        if (_backgroundPumpTask is not null)
        {
            try
            {
                await _backgroundPumpTask.WaitAsync(timeout);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Background pump did not shut down cleanly within timeout for queue {QueueId}",
                    _queueId);
            }
        }

        _logger.LogInformation(
            "ServiceBus adapter receiver shut down for queue {QueueId}",
            _queueId);
    }

    /// <summary>
    /// Disposes the receiver and releases Service Bus resources.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            _shutdown = true;
            _cancellationTokenSource.Cancel();
            
            _serviceBusReceiver?.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(10));
            _serviceBusClient?.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(10));
            _cancellationTokenSource.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Error disposing ServiceBus adapter receiver for queue {QueueId}",
                _queueId);
        }

        _disposed = true;
        _logger.LogInformation(
            "ServiceBus adapter receiver disposed for queue {QueueId}",
            _queueId);
    }

    /// <summary>
    /// Background pump that continuously receives messages from Service Bus and enqueues them for processing.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private async Task BackgroundPumpAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Starting background pump for queue {QueueId} on Service Bus entity '{EntityName}'",
            _queueId,
            ServiceBusEntityNamer.GetEntityName(_options));

        try
        {
            while (!cancellationToken.IsCancellationRequested && !_shutdown)
            {
                try
                {
                    if (_serviceBusReceiver is null)
                    {
                        _logger.LogWarning("Service Bus receiver is null, stopping background pump for queue {QueueId}", _queueId);
                        break;
                    }

                    // Receive messages from Service Bus
                    var receivedMessages = await _serviceBusReceiver.ReceiveMessagesAsync(
                        maxMessages: _options.Receiver.ReceiveBatchSize,
                        maxWaitTime: TimeSpan.FromSeconds(5),
                        cancellationToken: cancellationToken);

                    if (receivedMessages.Count > 0)
                    {
                        // Enqueue received messages for processing
                        foreach (var receivedMessage in receivedMessages)
                        {
                            var wrappedMessage = new ReceivedMessage(receivedMessage);
                            _messageQueue.Enqueue(wrappedMessage);

                            // Start lock renewal if enabled
                            if (_options.Receiver.LockAutoRenew)
                            {
                                _ = Task.Run(() => StartLockRenewalAsync(wrappedMessage, cancellationToken));
                            }
                        }

                        _logger.LogTrace(
                            "Received {MessageCount} messages from Service Bus entity '{EntityName}' for queue {QueueId}",
                            receivedMessages.Count,
                            ServiceBusEntityNamer.GetEntityName(_options),
                            _queueId);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Expected when shutting down
                    break;
                }
                catch (ServiceBusException sbEx) when (sbEx.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
                {
                    _logger.LogError(sbEx,
                        "Service Bus entity '{EntityName}' not found for queue {QueueId}. Stopping background pump.",
                        ServiceBusEntityNamer.GetEntityName(_options),
                        _queueId);
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Error in background pump for queue {QueueId} on Service Bus entity '{EntityName}'",
                        _queueId,
                        ServiceBusEntityNamer.GetEntityName(_options));

                    // Wait before retrying to avoid tight loop on persistent errors
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                }
            }
        }
        finally
        {
            _logger.LogInformation(
                "Background pump stopped for queue {QueueId} on Service Bus entity '{EntityName}'",
                _queueId,
                ServiceBusEntityNamer.GetEntityName(_options));
        }
    }

    /// <summary>
    /// Starts automatic lock renewal for a received message if enabled.
    /// </summary>
    /// <param name="receivedMessage">The received message.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private async Task StartLockRenewalAsync(ReceivedMessage receivedMessage, CancellationToken cancellationToken)
    {
        if (_serviceBusReceiver is null)
        {
            return;
        }

        try
        {
            var renewalInterval = TimeSpan.FromSeconds(30); // Renew every 30 seconds
            var maxRenewalDuration = _options.Receiver.LockRenewalDuration;
            var startTime = DateTimeOffset.UtcNow;

            while (!cancellationToken.IsCancellationRequested && 
                   !receivedMessage.IsCompleted && 
                   (DateTimeOffset.UtcNow - startTime) < maxRenewalDuration)
            {
                await Task.Delay(renewalInterval, cancellationToken);

                if (!receivedMessage.IsCompleted && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        await _serviceBusReceiver.RenewMessageLockAsync(receivedMessage.ServiceBusReceivedMessage, cancellationToken);
                        
                        _logger.LogTrace(
                            "Renewed lock for Service Bus message {MessageId}",
                            receivedMessage.ServiceBusReceivedMessage.MessageId);
                    }
                    catch (ServiceBusException sbEx) when (sbEx.Reason == ServiceBusFailureReason.MessageLockLost)
                    {
                        _logger.LogWarning(
                            "Lock lost for Service Bus message {MessageId}, stopping renewal",
                            receivedMessage.ServiceBusReceivedMessage.MessageId);
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex,
                            "Failed to renew lock for Service Bus message {MessageId}",
                            receivedMessage.ServiceBusReceivedMessage.MessageId);
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected when shutting down
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Error in lock renewal for Service Bus message {MessageId}",
                receivedMessage.ServiceBusReceivedMessage.MessageId);
        }
    }

    /// <summary>
    /// Creates the Service Bus client based on the configured connection options.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <returns>A configured Service Bus client.</returns>
    /// <exception cref="InvalidOperationException">Thrown when connection configuration is invalid.</exception>
    private static ServiceBusClient CreateServiceBusClient(ServiceBusStreamOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return new ServiceBusClient(options.ConnectionString);
        }

        if (!string.IsNullOrWhiteSpace(options.FullyQualifiedNamespace) && options.Credential is not null)
        {
            return new ServiceBusClient(options.FullyQualifiedNamespace, options.Credential);
        }

        throw new InvalidOperationException(
            "Either ConnectionString or both FullyQualifiedNamespace and Credential must be configured for Service Bus streaming.");
    }

    /// <summary>
    /// Creates the Service Bus receiver based on the configured entity type.
    /// </summary>
    /// <param name="client">The Service Bus client.</param>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <returns>A configured Service Bus receiver.</returns>
    /// <exception cref="InvalidOperationException">Thrown when entity configuration is invalid.</exception>
    private static ServiceBusReceiver CreateServiceBusReceiver(ServiceBusClient client, ServiceBusStreamOptions options)
    {
        var receiverOptions = new ServiceBusReceiverOptions
        {
            PrefetchCount = options.Receiver.PrefetchCount,
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        };

        return options.EntityKind switch
        {
            EntityKind.Queue => client.CreateReceiver(options.QueueName, receiverOptions),
            EntityKind.TopicSubscription => client.CreateReceiver(options.TopicName, options.SubscriptionName, receiverOptions),
            _ => throw new InvalidOperationException($"Unsupported entity kind: {options.EntityKind}")
        };
    }

    /// <summary>
    /// Wrapper for received Service Bus messages that tracks completion state.
    /// </summary>
    public class ReceivedMessage
    {
        public ReceivedMessage(ServiceBusReceivedMessage serviceBusReceivedMessage)
        {
            ServiceBusReceivedMessage = serviceBusReceivedMessage ?? throw new ArgumentNullException(nameof(serviceBusReceivedMessage));
        }

        public ServiceBusReceivedMessage ServiceBusReceivedMessage { get; }
        public bool IsCompleted { get; set; }
    }
}