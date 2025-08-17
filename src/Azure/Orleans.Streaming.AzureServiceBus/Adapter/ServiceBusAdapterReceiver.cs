using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Orleans.Streaming.AzureServiceBus.Telemetry;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Implementation of <see cref="IQueueAdapterReceiver"/> for Azure Service Bus streaming.
/// Provides the receiving path to dequeue messages from Service Bus entities and feed them to the cache.
/// This implementation follows non-rewindable semantics similar to ASQ/SQS.
/// </summary>
internal class ServiceBusAdapterReceiver : IQueueAdapterReceiver, IDisposable
{
    private readonly QueueId _queueId;
    private readonly string _providerName;
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
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <param name="dataAdapter">The data adapter for message conversion.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="failureHandler">Optional failure handler for tracking delivery failures.</param>
    public ServiceBusAdapterReceiver(
        QueueId queueId,
        string providerName,
        ServiceBusStreamOptions options,
        ServiceBusDataAdapter dataAdapter,
        ILogger<ServiceBusAdapterReceiver> logger,
        ServiceBusStreamFailureHandler? failureHandler = null)
    {
        _queueId = queueId;
        _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _dataAdapter = dataAdapter ?? throw new ArgumentNullException(nameof(dataAdapter));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _failureHandler = failureHandler;

        _serviceBusClient = CreateServiceBusClient(options);
        _messageQueue = new ConcurrentQueue<ReceivedMessage>();
        _cancellationTokenSource = new CancellationTokenSource();

        // Register cache size observer for metrics
        ServiceBusStreamingMetrics.RegisterCacheSizeObserver(() => 
            new Measurement<int>(_messageQueue.Count, new KeyValuePair<string, object?>("queue_id", _queueId.ToString())));

        _logger.LogInformation(
            "ServiceBus adapter receiver initialized for queue {QueueId} with entity kind '{EntityKind}', " +
            "entity name '{EntityName}', batch size {BatchSize}, prefetch count {PrefetchCount}",
            _queueId,
            _options.EntityKind,
            ServiceBusEntityNamer.GetEntityName(_options),
            _options.Receiver.ReceiveBatchSize,
            _options.Receiver.PrefetchCount);

        // Warn if concurrency > 1 as it breaks ordering guarantees
        if (_options.Receiver.MaxConcurrentHandlers > 1)
        {
            _logger.LogWarning(
                "ServiceBus adapter receiver for queue {QueueId} is configured with MaxConcurrentHandlers = {MaxConcurrentHandlers}. " +
                "This breaks message ordering guarantees. For strict ordering, use MaxConcurrentHandlers = 1.",
                _queueId,
                _options.Receiver.MaxConcurrentHandlers);
        }
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
                        receivedMessage.IsCompleted = true;
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
            var entityName = ServiceBusEntityNamer.GetEntityName(_options);
            ServiceBusStreamingMetrics.RecordReceiveBatch(
                _providerName, 
                entityName, 
                _queueId.ToString(), 
                messages.Count);

            _logger.LogDebug(
                "Retrieved {MessageCount} messages for queue {QueueId} from Service Bus entity '{EntityName}'",
                messages.Count,
                _queueId,
                entityName);
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
                            receivedMessage.IsCompleted = true;
                            Interlocked.Increment(ref abandonedCount);

                            // Track potential DLQ candidates (best-effort detection based on delivery count)
                            var deliveryCount = receivedMessage.ServiceBusReceivedMessage.DeliveryCount;
                            const int DlqSuspectedThreshold = 5; // Typical max delivery count before DLQ
                            
                            if (deliveryCount >= DlqSuspectedThreshold)
                            {
                                ServiceBusStreamingMetrics.RecordDlqSuspected(
                                    _providerName,
                                    ServiceBusEntityNamer.GetEntityName(_options),
                                    _queueId.ToString(),
                                    1);
                                
                                _logger.LogWarning(
                                    "Message {MessageId} for stream {StreamNamespace}:{StreamKey} may be sent to DLQ after abandonment (delivery count: {DeliveryCount})",
                                    receivedMessage.ServiceBusReceivedMessage.MessageId,
                                    serviceBusContainer.StreamId.GetNamespace(),
                                    serviceBusContainer.StreamId.GetKeyAsString(),
                                    deliveryCount);
                            }

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
                            receivedMessage.IsCompleted = true;
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
                                receivedMessage.IsCompleted = true;
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

            var entityName = ServiceBusEntityNamer.GetEntityName(_options);
            
            if (completedCount > 0)
            {
                ServiceBusStreamingMetrics.RecordMessagesCompleted(_providerName, entityName, _queueId.ToString(), completedCount);
            }
            
            if (abandonedCount > 0)
            {
                ServiceBusStreamingMetrics.RecordMessagesAbandoned(_providerName, entityName, _queueId.ToString(), abandonedCount);
            }

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

        // Dispose Service Bus resources to release prefetched/locked messages.
        try
        {
            if (_serviceBusReceiver is not null)
            {
                await _serviceBusReceiver.DisposeAsync();
                _serviceBusReceiver = null;
            }

            await _serviceBusClient.DisposeAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Error disposing Service Bus resources during shutdown for queue {QueueId}",
                _queueId);
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

                        // Check for cache pressure (simple heuristic: warn if queue size exceeds threshold)
                        var currentCacheSize = _messageQueue.Count;
                        const int CachePressureThreshold = 1000; // Configurable threshold
                        
                        if (currentCacheSize > CachePressureThreshold)
                        {
                            ServiceBusStreamingMetrics.RecordCachePressureTrigger(_providerName, _queueId.ToString());
                            
                            _logger.LogWarning(
                                "Cache pressure detected for queue {QueueId}: {CacheSize} messages queued (threshold: {Threshold})",
                                _queueId, currentCacheSize, CachePressureThreshold);
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
                    var entityName = ServiceBusEntityNamer.GetEntityName(_options);
                    ServiceBusStreamingMetrics.RecordReceiveFailure(_providerName, entityName, _queueId.ToString());

                    _logger.LogError(sbEx,
                        "Service Bus entity '{EntityName}' not found for queue {QueueId}. Stopping background pump.",
                        entityName,
                        _queueId);
                    break;
                }
                catch (Exception ex)
                {
                    var entityName = ServiceBusEntityNamer.GetEntityName(_options);
                    ServiceBusStreamingMetrics.RecordReceiveFailure(_providerName, entityName, _queueId.ToString());

                    _logger.LogError(ex,
                        "Error in background pump for queue {QueueId} on Service Bus entity '{EntityName}'",
                        _queueId,
                        entityName);

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
        /// <summary>
        /// Initializes a new instance of the <see cref="ReceivedMessage"/> class.
        /// </summary>
        /// <param name="serviceBusReceivedMessage">The underlying Service Bus received message.</param>
        public ReceivedMessage(ServiceBusReceivedMessage serviceBusReceivedMessage)
        {
            ServiceBusReceivedMessage = serviceBusReceivedMessage ?? throw new ArgumentNullException(nameof(serviceBusReceivedMessage));
        }

        /// <summary>
        /// Gets the underlying Service Bus received message.
        /// </summary>
        public ServiceBusReceivedMessage ServiceBusReceivedMessage { get; }

        /// <summary>
        /// Gets or sets a value indicating whether this message has been completed or abandoned.
        /// Used to stop lock-renewal tasks when applicable.
        /// </summary>
        public bool IsCompleted { get; set; }
    }
}
