using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streaming.ServiceBus;
using Orleans.Streams;

namespace Orleans.Providers.Streams.ServiceBus;

/// <summary>
/// Receives batches of messages from a Service Bus queue using ServiceBusProcessor.
/// </summary>
public sealed partial class ServiceBusQueueAdapterReceiver : IQueueAdapterReceiver, IAsyncDisposable
{
    private static readonly Counter<int> MessagesProcessedCounter = ServiceBusInstrumentation.Meter.CreateCounter<int>(
        "servicebus.queue.messages_processed",
        description: "Number of Service Bus queue messages processed");

    private readonly string _queueName;
    private readonly ServiceBusOptions _options;
    private readonly ILogger<ServiceBusQueueAdapterReceiver> _logger;
    private readonly ServiceBusClientFactory _clientFactory;
    private readonly string _optionsName;
    private readonly ConcurrentQueue<IBatchContainer> _pendingMessages = new();
    private readonly SemaphoreSlim _receiveSemaphore = new(1, 1);

    private ServiceBusClient? _serviceBusClient;
    private ServiceBusProcessor? _processor;
    private volatile bool _disposed;

    public ServiceBusQueueAdapterReceiver(
        string queueName,
        string optionsName,
        ServiceBusOptions options,
        ServiceBusClientFactory clientFactory,
        ILogger<ServiceBusQueueAdapterReceiver> logger)
    {
        _queueName = queueName ?? throw new ArgumentNullException(nameof(queueName));
        _optionsName = optionsName ?? throw new ArgumentNullException(nameof(optionsName));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task Initialize(TimeSpan timeout)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ServiceBusQueueAdapterReceiver));
        }

        using var activity = ServiceBusOptions.ActivitySource.StartActivity("receiver.initialize");
        activity?.SetTag("messaging.system", "azureservicebus");
        activity?.SetTag("messaging.destination.name", _queueName);

        try
        {
            _serviceBusClient = await _clientFactory.GetClientAsync(_optionsName);

            var processorOptions = new ServiceBusProcessorOptions
            {
                PrefetchCount = _options.PrefetchCount,
                MaxConcurrentCalls = _options.MaxConcurrentCalls,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                AutoCompleteMessages = false // We'll handle completion manually
            };

            _processor = _serviceBusClient.CreateProcessor(_queueName, processorOptions);
            _processor.ProcessMessageAsync += ProcessMessageAsync;
            _processor.ProcessErrorAsync += ProcessErrorAsync;

            await _processor.StartProcessingAsync();

            LogReceiverInitialized(_queueName, _options.PrefetchCount);
        }
        catch (Exception ex)
        {
            LogInitializationFailed(ex, _queueName);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        if (_disposed || _processor is null)
        {
            return [];
        }

        await _receiveSemaphore.WaitAsync();
        try
        {
            var messages = new List<IBatchContainer>();
            var actualMaxCount = maxCount <= 0 ? int.MaxValue : maxCount;

            // Dequeue available messages up to the max count
            while (messages.Count < actualMaxCount && _pendingMessages.TryDequeue(out var message))
            {
                messages.Add(message);
            }

            return messages;
        }
        finally
        {
            _receiveSemaphore.Release();
        }
    }

    public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        // Service Bus messages are automatically completed in ProcessMessageAsync
        // This is a no-op for Service Bus since we handle completion at processing time
        return Task.CompletedTask;
    }

    public async Task Shutdown(TimeSpan timeout)
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            if (_processor is not null)
            {
                _processor.ProcessMessageAsync -= ProcessMessageAsync;
                _processor.ProcessErrorAsync -= ProcessErrorAsync;

                using var cts = new CancellationTokenSource(timeout);
                await _processor.StopProcessingAsync(cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            LogShutdownTimeout(_queueName, timeout);
        }
        catch (Exception ex)
        {
            LogShutdownError(ex, _queueName);
        }
        finally
        {
            await DisposeAsync();
        }
    }

    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        using var activity = ServiceBusOptions.ActivitySource.StartActivity("message.process");
        activity?.SetTag("messaging.system", "azureservicebus");
        activity?.SetTag("messaging.destination.name", _queueName);
        activity?.SetTag("messaging.message.id", args.Message.MessageId);
        activity?.SetTag("messaging.servicebus.delivery_count", args.Message.DeliveryCount);

        try
        {
            // Create stream ID from queue name
            var streamId = StreamId.Create("", _queueName);

            // Parse message body
            var events = new List<object>();
            Dictionary<string, object> requestContext = [];

            if (args.Message.Body.Length > 0)
            {
                var bodyText = args.Message.Body.ToString();
                
                // Try to deserialize as JSON first, fallback to string
                try
                {
                    var jsonElement = JsonSerializer.Deserialize<JsonElement>(bodyText);
                    events.Add(jsonElement);
                }
                catch
                {
                    events.Add(bodyText);
                }
            }

            // Add message properties to request context
            foreach (var property in args.Message.ApplicationProperties)
            {
                requestContext[property.Key] = property.Value;
            }

            // Add Service Bus specific properties
            requestContext["ServiceBus.MessageId"] = args.Message.MessageId;
            requestContext["ServiceBus.CorrelationId"] = args.Message.CorrelationId;
            requestContext["ServiceBus.Subject"] = args.Message.Subject;
            requestContext["ServiceBus.DeliveryCount"] = args.Message.DeliveryCount;

            // Create batch container
            var sequenceToken = new EventSequenceTokenV2(DateTime.UtcNow.Ticks);
            var batchContainer = new ServiceBusBatchContainer(streamId, events, requestContext, sequenceToken);

            // Enqueue the message for processing
            _pendingMessages.Enqueue(batchContainer);

            // Complete the message on successful processing
            await args.CompleteMessageAsync(args.Message);

            // Increment the processed counter
            MessagesProcessedCounter.Add(1, 
                new KeyValuePair<string, object?>("messaging.destination.name", _queueName));

            LogMessageProcessed(args.Message.MessageId, _queueName, args.Message.DeliveryCount);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            
            // Check if we should move to dead letter queue
            if (args.Message.DeliveryCount >= _options.MaxDeliveryAttempts)
            {
                await args.DeadLetterMessageAsync(args.Message, "Max delivery attempts exceeded", ex.Message);
                LogMessageDeadLettered(args.Message.MessageId, _queueName, args.Message.DeliveryCount, ex);
            }
            else
            {
                // Abandon the message to retry
                await args.AbandonMessageAsync(args.Message);
                LogMessageAbandoned(args.Message.MessageId, _queueName, args.Message.DeliveryCount, ex);
            }
        }
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        using var activity = ServiceBusOptions.ActivitySource.StartActivity("error.process");
        activity?.SetTag("messaging.system", "azureservicebus");
        activity?.SetTag("messaging.destination.name", _queueName);
        activity?.SetStatus(ActivityStatusCode.Error, args.Exception.Message);

        LogProcessorError(args.Exception, _queueName, args.ErrorSource.ToString());
        
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        try
        {
            if (_processor is not null)
            {
                await _processor.DisposeAsync();
            }
        }
        catch (Exception ex)
        {
            LogDisposeError(ex, _queueName);
        }
        finally
        {
            _receiveSemaphore.Dispose();
        }
    }

    [LoggerMessage(
        Level = LogLevel.Information,
        EventId = 1,
        Message = "ServiceBus queue receiver initialized for queue '{QueueName}' with PrefetchCount={PrefetchCount}")]
    private partial void LogReceiverInitialized(string queueName, int prefetchCount);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 2,
        Message = "Failed to initialize ServiceBus queue receiver for queue '{QueueName}'")]
    private partial void LogInitializationFailed(Exception exception, string queueName);

    [LoggerMessage(
        Level = LogLevel.Debug,
        EventId = 3,
        Message = "Processed ServiceBus message '{MessageId}' from queue '{QueueName}' (DeliveryCount={DeliveryCount})")]
    private partial void LogMessageProcessed(string messageId, string queueName, int deliveryCount);

    [LoggerMessage(
        Level = LogLevel.Warning,
        EventId = 4,
        Message = "Abandoned ServiceBus message '{MessageId}' from queue '{QueueName}' (DeliveryCount={DeliveryCount}) due to processing error")]
    private partial void LogMessageAbandoned(string messageId, string queueName, int deliveryCount, Exception exception);

    [LoggerMessage(
        Level = LogLevel.Warning,
        EventId = 5,
        Message = "Dead lettered ServiceBus message '{MessageId}' from queue '{QueueName}' (DeliveryCount={DeliveryCount}) after max delivery attempts")]
    private partial void LogMessageDeadLettered(string messageId, string queueName, int deliveryCount, Exception exception);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 6,
        Message = "ServiceBus processor error for queue '{QueueName}' from source '{ErrorSource}'")]
    private partial void LogProcessorError(Exception exception, string queueName, string errorSource);

    [LoggerMessage(
        Level = LogLevel.Warning,
        EventId = 7,
        Message = "ServiceBus queue receiver shutdown timeout for queue '{QueueName}' after {Timeout}")]
    private partial void LogShutdownTimeout(string queueName, TimeSpan timeout);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 8,
        Message = "Error during ServiceBus queue receiver shutdown for queue '{QueueName}'")]
    private partial void LogShutdownError(Exception exception, string queueName);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 9,
        Message = "Error during ServiceBus queue receiver disposal for queue '{QueueName}'")]
    private partial void LogDisposeError(Exception exception, string queueName);
}