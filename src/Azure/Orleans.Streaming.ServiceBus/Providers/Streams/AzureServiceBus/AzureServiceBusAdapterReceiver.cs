#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streaming.ServiceBus.Storage;
using Orleans.Streams;

namespace Orleans.Providers.Streams.AzureServiceBus;

/// <summary>
/// Receives batches of messages from Azure Service Bus and delivers them to Orleans streaming.
/// Manages processor lifecycle, handles message acknowledgment, and coordinates with Orleans streaming 
/// for backpressure and flow control.
/// </summary>
internal class AzureServiceBusAdapterReceiver : IQueueAdapterReceiver
{
    private readonly AzureServiceBusOptions _options;
    private readonly IQueueDataAdapter<ServiceBusMessage, IBatchContainer> _dataAdapter;
    private readonly ILogger _logger;
    private readonly string _subscriptionName;
    
    private AzureServiceBusDataManager? _manager;
    private ServiceBusProcessor? _processor;
    private ServiceBusSessionProcessor? _sessionProcessor;
    private readonly List<PendingDelivery> _pendingDeliveries = new();
    private TaskCompletionSource<IList<IBatchContainer>>? _messagesTcs;
    private readonly object _processorLock = new();
    private long _sequenceId = 0;
    private bool _processorStarted = false;
    private bool _disposed = false;

    /// <summary>
    /// Initializes a new instance of the <see cref="AzureServiceBusAdapterReceiver"/> class.
    /// </summary>
    /// <param name="dataManager">The Azure Service Bus data manager.</param>
    /// <param name="dataAdapter">The data adapter for message conversion.</param>
    /// <param name="options">The Azure Service Bus configuration options.</param>
    /// <param name="subscriptionName">The subscription name for topic topology.</param>
    /// <param name="logger">The logger instance.</param>
    public AzureServiceBusAdapterReceiver(
        AzureServiceBusDataManager dataManager,
        IQueueDataAdapter<ServiceBusMessage, IBatchContainer> dataAdapter,
        AzureServiceBusOptions options,
        string? subscriptionName,
        ILogger logger)
    {
        _manager = dataManager ?? throw new ArgumentNullException(nameof(dataManager));
        _dataAdapter = dataAdapter ?? throw new ArgumentNullException(nameof(dataAdapter));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _subscriptionName = subscriptionName ?? string.Empty;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc/>
    public Task Initialize(TimeSpan timeout)
    {
        ThrowIfDisposed();
        
        if (_manager is null)
        {
            throw new InvalidOperationException("Data manager is not available.");
        }

        // Initialize data manager connection
        return _manager.InitAsync();
    }

    /// <inheritdoc/>
    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        ThrowIfDisposed();
        
        if (_manager is null)
        {
            return new List<IBatchContainer>();
        }

        // Start processor if not already running
        await EnsureProcessorStarted();

        // Wait for messages or timeout
        using var cts = new CancellationTokenSource(_options.MaxWaitTime);
        
        try
        {
            lock (_processorLock)
            {
                if (_messagesTcs is null || _messagesTcs.Task.IsCompleted)
                {
                    _messagesTcs = new TaskCompletionSource<IList<IBatchContainer>>(TaskCreationOptions.RunContinuationsAsynchronously);
                }
            }

            // Wait for messages to arrive via processor events
            var completedTask = await Task.WhenAny(
                _messagesTcs.Task,
                Task.Delay(_options.MaxWaitTime, cts.Token)
            );

            if (completedTask == _messagesTcs.Task)
            {
                return await _messagesTcs.Task;
            }
            
            // Timeout occurred - return empty list
            return new List<IBatchContainer>();
        }
        catch (OperationCanceledException)
        {
            // Timeout occurred
            return new List<IBatchContainer>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred while getting queue messages from Service Bus");
            return new List<IBatchContainer>();
        }
    }

    /// <inheritdoc/>
    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        ThrowIfDisposed();
        
        if (messages.Count == 0)
        {
            return;
        }

        try
        {
            // Get sequence tokens of delivered messages
            var deliveredTokens = messages.Select(message => message.SequenceToken).ToList();
            
            // Find oldest delivered message
            var oldest = deliveredTokens.Max();
            
            // Find all pending messages at or before the oldest delivered token
            List<PendingDelivery> completedDeliveries;
            lock (_pendingDeliveries)
            {
                completedDeliveries = _pendingDeliveries
                    .Where(pendingDelivery => !pendingDelivery.Token.Newer(oldest))
                    .ToList();
                
                if (completedDeliveries.Count == 0)
                {
                    return;
                }
                
                // Remove completed deliveries from pending list
                foreach (var delivery in completedDeliveries)
                {
                    _pendingDeliveries.Remove(delivery);
                }
            }

            // Complete the messages that were successfully delivered
            var messagesToComplete = completedDeliveries
                .Where(delivery => deliveredTokens.Any(token => token.Equals(delivery.Token)))
                .ToList();

            if (messagesToComplete.Count > 0 && !_options.AutoCompleteMessages)
            {
                // Complete messages manually if auto-complete is disabled
                foreach (var delivery in messagesToComplete)
                {
                    try
                    {
                        if (delivery.ReceivedMessage is not null)
                        {
                            await delivery.ReceivedMessage.CompleteMessageAsync(delivery.Message);
                        }
                        else if (delivery.SessionReceivedMessage is not null)
                        {
                            await delivery.SessionReceivedMessage.CompleteMessageAsync(delivery.Message);
                        }
                        _logger.LogDebug("Completed Service Bus message {MessageId}", delivery.Message.MessageId);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to complete Service Bus message {MessageId}", delivery.Message.MessageId);
                    }
                }
            }

            // Handle messages that were not delivered (potential failures)
            var failedDeliveries = completedDeliveries.Except(messagesToComplete).ToList();
            if (failedDeliveries.Count > 0 && !_options.AutoCompleteMessages)
            {
                foreach (var delivery in failedDeliveries)
                {
                    try
                    {
                        if (delivery.ReceivedMessage is not null)
                        {
                            await delivery.ReceivedMessage.AbandonMessageAsync(delivery.Message);
                        }
                        else if (delivery.SessionReceivedMessage is not null)
                        {
                            await delivery.SessionReceivedMessage.AbandonMessageAsync(delivery.Message);
                        }
                        _logger.LogWarning("Abandoned Service Bus message {MessageId} due to delivery failure", delivery.Message.MessageId);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to abandon Service Bus message {MessageId}", delivery.Message.MessageId);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred while processing delivered messages");
        }
    }

    /// <inheritdoc/>
    public async Task Shutdown(TimeSpan timeout)
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            // Stop processors
            await StopProcessors();

            // Complete any pending TaskCompletionSource
            lock (_processorLock)
            {
                _messagesTcs?.TrySetCanceled();
                _messagesTcs = null;
            }

            // Clear pending deliveries
            lock (_pendingDeliveries)
            {
                _pendingDeliveries.Clear();
            }

            // Dispose data manager
            if (_manager is not null)
            {
                await _manager.DisposeAsync();
                _manager = null;
            }

            _disposed = true;
            _logger.LogInformation("Service Bus adapter receiver shutdown completed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Service Bus adapter receiver shutdown");
            throw;
        }
    }

    /// <summary>
    /// Ensures the processor is started and configured correctly.
    /// </summary>
    private Task EnsureProcessorStarted()
    {
        if (_processorStarted || _manager is null)
        {
            return Task.CompletedTask;
        }

        lock (_processorLock)
        {
            if (_processorStarted)
            {
                return Task.CompletedTask;
            }

            try
            {
                if (_options.RequiresSession == true)
                {
                    StartSessionProcessor();
                }
                else
                {
                    StartRegularProcessor();
                }

                _processorStarted = true;
                _logger.LogInformation("Started Service Bus processor for entity {EntityName}", _manager.EntityName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start Service Bus processor for entity {EntityName}", _manager.EntityName);
                throw;
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Starts the regular (non-session) Service Bus processor.
    /// </summary>
    private void StartRegularProcessor()
    {
        if (_manager is null)
        {
            throw new InvalidOperationException("Data manager is not available.");
        }

        _processor = _manager.CreateProcessor(string.IsNullOrEmpty(_subscriptionName) ? null : _subscriptionName);
        _processor.ProcessMessageAsync += OnMessageReceived;
        _processor.ProcessErrorAsync += OnProcessorError;
        
        _ = Task.Run(async () => 
        {
            try
            {
                await _processor.StartProcessingAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start Service Bus processor");
            }
        });
    }

    /// <summary>
    /// Starts the session-enabled Service Bus processor.
    /// </summary>
    private void StartSessionProcessor()
    {
        if (_manager is null)
        {
            throw new InvalidOperationException("Data manager is not available.");
        }

        _sessionProcessor = _manager.CreateSessionProcessor(string.IsNullOrEmpty(_subscriptionName) ? null : _subscriptionName);
        _sessionProcessor.ProcessMessageAsync += OnSessionMessageReceived;
        _sessionProcessor.ProcessErrorAsync += OnProcessorError;
        
        _ = Task.Run(async () => 
        {
            try
            {
                await _sessionProcessor.StartProcessingAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start Service Bus session processor");
            }
        });
    }

    /// <summary>
    /// Stops the Service Bus processors.
    /// </summary>
    private async Task StopProcessors()
    {
        if (_processor is not null)
        {
            try
            {
                await _processor.StopProcessingAsync();
                await _processor.DisposeAsync();
                _processor = null;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error stopping Service Bus processor");
            }
        }

        if (_sessionProcessor is not null)
        {
            try
            {
                await _sessionProcessor.StopProcessingAsync();
                await _sessionProcessor.DisposeAsync();
                _sessionProcessor = null;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error stopping Service Bus session processor");
            }
        }

        _processorStarted = false;
    }

    /// <summary>
    /// Handles messages received from the regular processor.
    /// </summary>
    private async Task OnMessageReceived(ProcessMessageEventArgs args)
    {
        try
        {
            var sequenceId = Interlocked.Increment(ref _sequenceId);
            
            // Convert ServiceBusReceivedMessage to ServiceBusMessage for data adapter
            var serviceBusMessage = CreateServiceBusMessage(args.Message);
            var batchContainer = _dataAdapter.FromQueueMessage(serviceBusMessage, sequenceId);
            
            var pendingDelivery = new PendingDelivery(batchContainer.SequenceToken, args.Message, args, null);
            
            lock (_pendingDeliveries)
            {
                _pendingDeliveries.Add(pendingDelivery);
            }

            // Deliver message to Orleans streaming
            TaskCompletionSource<IList<IBatchContainer>>? currentTcs = null;
            lock (_processorLock)
            {
                currentTcs = _messagesTcs;
            }

            if (currentTcs is not null && !currentTcs.Task.IsCompleted)
            {
                currentTcs.TrySetResult(new List<IBatchContainer> { batchContainer });
            }

            _logger.LogDebug("Received Service Bus message {MessageId} for stream {StreamId}", 
                args.Message.MessageId, batchContainer.StreamId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing received Service Bus message {MessageId}", args.Message.MessageId);
            
            // If auto-complete is disabled, abandon the message
            if (!_options.AutoCompleteMessages)
            {
                try
                {
                    await args.AbandonMessageAsync(args.Message);
                }
                catch (Exception abandonEx)
                {
                    _logger.LogError(abandonEx, "Failed to abandon message {MessageId} after processing error", args.Message.MessageId);
                }
            }
        }
    }

    /// <summary>
    /// Handles messages received from the session processor.
    /// </summary>
    private async Task OnSessionMessageReceived(ProcessSessionMessageEventArgs args)
    {
        try
        {
            var sequenceId = Interlocked.Increment(ref _sequenceId);
            
            // Convert ServiceBusReceivedMessage to ServiceBusMessage for data adapter
            var serviceBusMessage = CreateServiceBusMessage(args.Message);
            var batchContainer = _dataAdapter.FromQueueMessage(serviceBusMessage, sequenceId);
            
            var pendingDelivery = new PendingDelivery(batchContainer.SequenceToken, args.Message, null, args);
            
            lock (_pendingDeliveries)
            {
                _pendingDeliveries.Add(pendingDelivery);
            }

            // Deliver message to Orleans streaming
            TaskCompletionSource<IList<IBatchContainer>>? currentTcs = null;
            lock (_processorLock)
            {
                currentTcs = _messagesTcs;
            }

            if (currentTcs is not null && !currentTcs.Task.IsCompleted)
            {
                currentTcs.TrySetResult(new List<IBatchContainer> { batchContainer });
            }

            _logger.LogDebug("Received Service Bus session message {MessageId} for stream {StreamId} in session {SessionId}", 
                args.Message.MessageId, batchContainer.StreamId, args.SessionId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing received Service Bus session message {MessageId}", args.Message.MessageId);
            
            // If auto-complete is disabled, abandon the message
            if (!_options.AutoCompleteMessages)
            {
                try
                {
                    await args.AbandonMessageAsync(args.Message);
                }
                catch (Exception abandonEx)
                {
                    _logger.LogError(abandonEx, "Failed to abandon session message {MessageId} after processing error", args.Message.MessageId);
                }
            }
        }
    }

    /// <summary>
    /// Handles processor errors.
    /// </summary>
    private Task OnProcessorError(ProcessErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Service Bus processor error occurred. Source: {ErrorSource}, EntityPath: {EntityPath}", 
            args.ErrorSource, args.EntityPath);
        
        // Complete any waiting TaskCompletionSource with an empty result
        TaskCompletionSource<IList<IBatchContainer>>? currentTcs = null;
        lock (_processorLock)
        {
            currentTcs = _messagesTcs;
        }

        if (currentTcs is not null && !currentTcs.Task.IsCompleted)
        {
            currentTcs.TrySetResult(new List<IBatchContainer>());
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Creates a ServiceBusMessage from a ServiceBusReceivedMessage for use with the data adapter.
    /// </summary>
    /// <param name="receivedMessage">The received message from Service Bus.</param>
    /// <returns>A ServiceBusMessage instance compatible with the data adapter.</returns>
    private static ServiceBusMessage CreateServiceBusMessage(ServiceBusReceivedMessage receivedMessage)
    {
        var message = new ServiceBusMessage(receivedMessage.Body)
        {
            ContentType = receivedMessage.ContentType,
            CorrelationId = receivedMessage.CorrelationId,
            MessageId = receivedMessage.MessageId,
            Subject = receivedMessage.Subject,
            To = receivedMessage.To,
            ReplyTo = receivedMessage.ReplyTo,
            ReplyToSessionId = receivedMessage.ReplyToSessionId,
            SessionId = receivedMessage.SessionId,
            TimeToLive = receivedMessage.TimeToLive,
            PartitionKey = receivedMessage.PartitionKey
        };

        // Copy application properties
        foreach (var kvp in receivedMessage.ApplicationProperties)
        {
            message.ApplicationProperties[kvp.Key] = kvp.Value;
        }

        return message;
    }

    /// <summary>
    /// Throws an exception if the receiver has been disposed.
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(AzureServiceBusAdapterReceiver));
        }
    }

    /// <summary>
    /// Represents a pending message delivery awaiting acknowledgment.
    /// </summary>
    private class PendingDelivery
    {
        public PendingDelivery(StreamSequenceToken token, ServiceBusReceivedMessage message, ProcessMessageEventArgs? receivedMessage = null, ProcessSessionMessageEventArgs? sessionReceivedMessage = null)
        {
            Token = token ?? throw new ArgumentNullException(nameof(token));
            Message = message ?? throw new ArgumentNullException(nameof(message));
            ReceivedMessage = receivedMessage;
            SessionReceivedMessage = sessionReceivedMessage;
        }

        public StreamSequenceToken Token { get; }
        public ServiceBusReceivedMessage Message { get; }
        public ProcessMessageEventArgs? ReceivedMessage { get; }
        public ProcessSessionMessageEventArgs? SessionReceivedMessage { get; }
    }
}