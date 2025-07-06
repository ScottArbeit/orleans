#nullable enable
using System;
using System.Collections.Concurrent;
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
    private readonly ConcurrentQueue<PendingDelivery> _pendingDeliveries = new();
    private TaskCompletionSource<IList<IBatchContainer>>? _messagesTcs;
    private readonly SemaphoreSlim _processorSemaphore = new(1, 1);
    private readonly SemaphoreSlim _messagesSemaphore = new(1, 1);
    private long _sequenceId = 0;
    private bool _processorStarted = false;
    private bool _disposed = false;
    private readonly List<IBatchContainer> _batchAccumulator = new();
    private Timer? _batchTimer;
    private Timer? _cleanupTimer;

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

        // Start cleanup timer for pending deliveries
        _cleanupTimer = new Timer(CleanupPendingDeliveries, null, 
            _options.PendingDeliveryTimeout, _options.PendingDeliveryTimeout);

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
            await _messagesSemaphore.WaitAsync(cts.Token);
            try
            {
                if (_messagesTcs is null || _messagesTcs.Task.IsCompleted)
                {
                    _messagesTcs = new TaskCompletionSource<IList<IBatchContainer>>(TaskCreationOptions.RunContinuationsAsynchronously);
                }
            }
            finally
            {
                _messagesSemaphore.Release();
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
            
            // Find newest delivered message (using Max is correct - see Orleans patterns)
            var newest = deliveredTokens.Max();
            
            // Find all pending messages at or before the newest delivered token
            var completedDeliveries = new List<PendingDelivery>();
            var tempQueue = new ConcurrentQueue<PendingDelivery>();
            
            // Process pending deliveries
            while (_pendingDeliveries.TryDequeue(out var pendingDelivery))
            {
                if (!pendingDelivery.Token.Newer(newest))
                {
                    completedDeliveries.Add(pendingDelivery);
                }
                else
                {
                    tempQueue.Enqueue(pendingDelivery);
                }
            }
            
            // Re-enqueue remaining deliveries
            while (tempQueue.TryDequeue(out var pendingDelivery))
            {
                _pendingDeliveries.Enqueue(pendingDelivery);
            }

            if (completedDeliveries.Count == 0)
            {
                return;
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
            // Stop timers
            _batchTimer?.Dispose();
            _cleanupTimer?.Dispose();

            // Stop processors
            await StopProcessors();

            // Complete any pending TaskCompletionSource
            await _messagesSemaphore.WaitAsync();
            try
            {
                _messagesTcs?.TrySetCanceled();
                _messagesTcs = null;
            }
            finally
            {
                _messagesSemaphore.Release();
            }

            // Clear pending deliveries
            while (_pendingDeliveries.TryDequeue(out _))
            {
                // Just clearing the queue
            }

            // Clear batch accumulator
            lock (_batchAccumulator)
            {
                _batchAccumulator.Clear();
            }

            // Dispose data manager
            if (_manager is not null)
            {
                await _manager.DisposeAsync();
                _manager = null;
            }

            // Dispose semaphores
            _processorSemaphore?.Dispose();
            _messagesSemaphore?.Dispose();

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
    private async Task EnsureProcessorStarted()
    {
        if (_processorStarted || _manager is null)
        {
            return;
        }

        await _processorSemaphore.WaitAsync();
        try
        {
            if (_processorStarted)
            {
                return;
            }

            var retryCount = 0;
            Exception? lastException = null;

            while (retryCount <= _options.ProcessorStartupRetries)
            {
                try
                {
                    if (_options.RequiresSession == true)
                    {
                        await StartSessionProcessor();
                    }
                    else
                    {
                        await StartRegularProcessor();
                    }

                    _processorStarted = true;
                    _logger.LogInformation("Started Service Bus processor for entity {EntityName} after {RetryCount} attempts", 
                        _manager.EntityName, retryCount);
                    return;
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    retryCount++;
                    
                    if (retryCount <= _options.ProcessorStartupRetries)
                    {
                        _logger.LogWarning(ex, "Failed to start Service Bus processor for entity {EntityName} (attempt {RetryCount}/{MaxRetries}). Retrying in {Delay}", 
                            _manager.EntityName, retryCount, _options.ProcessorStartupRetries + 1, _options.ProcessorStartupRetryDelay);
                        
                        await Task.Delay(_options.ProcessorStartupRetryDelay);
                    }
                }
            }

            _logger.LogError(lastException, "Failed to start Service Bus processor for entity {EntityName} after {RetryCount} attempts", 
                _manager.EntityName, retryCount);
            throw lastException ?? new InvalidOperationException("Failed to start Service Bus processor");
        }
        finally
        {
            _processorSemaphore.Release();
        }
    }

    /// <summary>
    /// Starts the regular (non-session) Service Bus processor.
    /// </summary>
    private async Task StartRegularProcessor()
    {
        if (_manager is null)
        {
            throw new InvalidOperationException("Data manager is not available.");
        }

        _processor = _manager.CreateProcessor(string.IsNullOrEmpty(_subscriptionName) ? null : _subscriptionName);
        _processor.ProcessMessageAsync += OnMessageReceived;
        _processor.ProcessErrorAsync += OnProcessorError;
        
        await _processor.StartProcessingAsync();
    }

    /// <summary>
    /// Starts the session-enabled Service Bus processor.
    /// </summary>
    private async Task StartSessionProcessor()
    {
        if (_manager is null)
        {
            throw new InvalidOperationException("Data manager is not available.");
        }

        _sessionProcessor = _manager.CreateSessionProcessor(string.IsNullOrEmpty(_subscriptionName) ? null : _subscriptionName);
        _sessionProcessor.ProcessMessageAsync += OnSessionMessageReceived;
        _sessionProcessor.ProcessErrorAsync += OnProcessorError;
        
        await _sessionProcessor.StartProcessingAsync();
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
            
            // Check pending deliveries count and cleanup if needed
            if (_pendingDeliveries.Count >= _options.MaxPendingDeliveries)
            {
                CleanupPendingDeliveries(null);
            }
            
            _pendingDeliveries.Enqueue(pendingDelivery);

            // Handle batching
            _ = HandleMessageBatching(batchContainer);

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
            
            // Check pending deliveries count and cleanup if needed
            if (_pendingDeliveries.Count >= _options.MaxPendingDeliveries)
            {
                CleanupPendingDeliveries(null);
            }
            
            _pendingDeliveries.Enqueue(pendingDelivery);

            // Handle batching
            _ = HandleMessageBatching(batchContainer);

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
    private async Task OnProcessorError(ProcessErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Service Bus processor error occurred. Source: {ErrorSource}, EntityPath: {EntityPath}", 
            args.ErrorSource, args.EntityPath);
        
        // Complete any waiting TaskCompletionSource with an empty result
        await _messagesSemaphore.WaitAsync();
        try
        {
            if (_messagesTcs is not null && !_messagesTcs.Task.IsCompleted)
            {
                _messagesTcs.TrySetResult(new List<IBatchContainer>());
            }
        }
        finally
        {
            _messagesSemaphore.Release();
        }
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
    /// Handles message batching logic.
    /// </summary>
    private Task HandleMessageBatching(IBatchContainer batchContainer)
    {
        lock (_batchAccumulator)
        {
            _batchAccumulator.Add(batchContainer);

            // Check if we should deliver the batch immediately
            if (_options.MaxBatchSize <= 1 || _batchAccumulator.Count >= _options.MaxBatchSize)
            {
                DeliverBatch();
                return Task.CompletedTask;
            }

            // Start batch timer if not already running
            if (_batchTimer is null)
            {
                _batchTimer = new Timer(OnBatchTimeout, null, _options.BatchTimeout, Timeout.InfiniteTimeSpan);
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Called when the batch timeout expires.
    /// </summary>
    private void OnBatchTimeout(object? state)
    {
        lock (_batchAccumulator)
        {
            if (_batchAccumulator.Count > 0)
            {
                DeliverBatch();
            }
        }
    }

    /// <summary>
    /// Delivers the current batch to Orleans streaming.
    /// Must be called within a lock on _batchAccumulator.
    /// </summary>
    private void DeliverBatch()
    {
        if (_batchAccumulator.Count == 0)
        {
            return;
        }

        var batch = _batchAccumulator.ToList();
        _batchAccumulator.Clear();

        // Dispose existing timer
        _batchTimer?.Dispose();
        _batchTimer = null;

        // Deliver batch asynchronously
        _ = Task.Run(async () =>
        {
            await _messagesSemaphore.WaitAsync();
            try
            {
                if (_messagesTcs is not null && !_messagesTcs.Task.IsCompleted)
                {
                    _messagesTcs.TrySetResult(batch);
                }
            }
            finally
            {
                _messagesSemaphore.Release();
            }
        });
    }

    /// <summary>
    /// Cleans up old pending deliveries to prevent memory leaks.
    /// </summary>
    private void CleanupPendingDeliveries(object? state)
    {
        var cutoffTime = DateTime.UtcNow - _options.PendingDeliveryTimeout;
        var tempQueue = new ConcurrentQueue<PendingDelivery>();
        var cleanedCount = 0;

        // Process all pending deliveries
        while (_pendingDeliveries.TryDequeue(out var pendingDelivery))
        {
            if (pendingDelivery.Timestamp > cutoffTime)
            {
                tempQueue.Enqueue(pendingDelivery);
            }
            else
            {
                cleanedCount++;
            }
        }

        // Re-enqueue recent deliveries
        while (tempQueue.TryDequeue(out var pendingDelivery))
        {
            _pendingDeliveries.Enqueue(pendingDelivery);
        }

        if (cleanedCount > 0)
        {
            _logger.LogDebug("Cleaned up {CleanedCount} old pending deliveries", cleanedCount);
        }
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
            Timestamp = DateTime.UtcNow;
        }

        public StreamSequenceToken Token { get; }
        public ServiceBusReceivedMessage Message { get; }
        public ProcessMessageEventArgs? ReceivedMessage { get; }
        public ProcessSessionMessageEventArgs? SessionReceivedMessage { get; }
        public DateTime Timestamp { get; }
    }
}