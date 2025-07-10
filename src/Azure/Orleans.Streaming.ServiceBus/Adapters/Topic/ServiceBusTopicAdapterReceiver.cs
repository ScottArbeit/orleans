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
/// Receives batches of messages from a Service Bus topic subscription using ServiceBusProcessor.
/// </summary>
public sealed partial class ServiceBusTopicAdapterReceiver : IQueueAdapterReceiver, IAsyncDisposable
{
    private static readonly Counter<int> MessagesProcessedCounter = ServiceBusInstrumentation.Meter.CreateCounter<int>(
        "servicebus.topic.messages_processed",
        description: "Number of Service Bus topic messages processed");

    private static readonly Counter<int> MessagesDeadLetteredCounter = ServiceBusInstrumentation.Meter.CreateCounter<int>(
        "servicebus.topic.dead_lettered",
        description: "Number of Service Bus topic messages dead lettered");

    private readonly string _subscriptionName;
    private readonly ServiceBusOptions _options;
    private readonly ILogger<ServiceBusTopicAdapterReceiver> _logger;
    private readonly ServiceBusClientFactory _clientFactory;
    private readonly string _optionsName;
    private readonly ConcurrentQueue<IBatchContainer> _pendingMessages = new();
    private readonly SemaphoreSlim _receiveSemaphore = new(1, 1);

    private ServiceBusClient? _serviceBusClient;
    private ServiceBusProcessor? _processor;
    private volatile bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusTopicAdapterReceiver"/> class.
    /// </summary>
    /// <param name="subscriptionName">The name of the Service Bus subscription.</param>
    /// <param name="optionsName">The name of the options configuration.</param>
    /// <param name="options">The Service Bus configuration options.</param>
    /// <param name="clientFactory">The Service Bus client factory.</param>
    /// <param name="logger">The logger instance.</param>
    public ServiceBusTopicAdapterReceiver(
        string subscriptionName,
        string optionsName,
        ServiceBusOptions options,
        ServiceBusClientFactory clientFactory,
        ILogger<ServiceBusTopicAdapterReceiver> logger)
    {
        _subscriptionName = subscriptionName ?? throw new ArgumentNullException(nameof(subscriptionName));
        _optionsName = optionsName ?? throw new ArgumentNullException(nameof(optionsName));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Initializes the receiver by creating and starting the Service Bus processor.
    /// </summary>
    /// <param name="timeout">The timeout for initialization (not used for Service Bus).</param>
    /// <returns>A task representing the asynchronous initialization operation.</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the receiver has been disposed.</exception>
    public async Task Initialize(TimeSpan timeout)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ServiceBusTopicAdapterReceiver));
        }

        using var activity = ServiceBusOptions.ActivitySource.StartActivity("receiver.initialize");
        activity?.SetTag("messaging.system", "azureservicebus");
        activity?.SetTag("messaging.destination.name", GetTopicName());
        activity?.SetTag("messaging.servicebus.subscription.name", _subscriptionName);

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

            var topicName = GetTopicName();
            _processor = _serviceBusClient.CreateProcessor(topicName, _subscriptionName, processorOptions);
            _processor.ProcessMessageAsync += ProcessMessageAsync;
            _processor.ProcessErrorAsync += ProcessErrorAsync;

            await _processor.StartProcessingAsync();

            LogReceiverInitialized(topicName, _subscriptionName, _options.PrefetchCount);
        }
        catch (Exception ex)
        {
            LogInitializationFailed(ex, GetTopicName(), _subscriptionName);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Gets queue messages that have been received and are ready for processing.
    /// </summary>
    /// <param name="maxCount">The maximum number of messages to return. If 0 or negative, returns all available messages.</param>
    /// <returns>A list of batch containers with the received messages.</returns>
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

    /// <summary>
    /// Notifies the receiver that the specified messages have been delivered successfully.
    /// This is a no-op for Service Bus since messages are completed during processing.
    /// </summary>
    /// <param name="messages">The messages that have been delivered.</param>
    /// <returns>A completed task.</returns>
    public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        // Service Bus messages are automatically completed in ProcessMessageAsync
        // This is a no-op for Service Bus since we handle completion at processing time
        return Task.CompletedTask;
    }

    /// <summary>
    /// Shuts down the receiver by stopping the Service Bus processor.
    /// </summary>
    /// <param name="timeout">The timeout for shutdown operations.</param>
    /// <returns>A task representing the asynchronous shutdown operation.</returns>
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
            LogShutdownTimeout(GetTopicName(), _subscriptionName, timeout);
        }
        catch (Exception ex)
        {
            LogShutdownError(ex, GetTopicName(), _subscriptionName);
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
        activity?.SetTag("messaging.destination.name", GetTopicName());
        activity?.SetTag("messaging.servicebus.subscription.name", _subscriptionName);
        activity?.SetTag("messaging.message.id", args.Message.MessageId);
        activity?.SetTag("messaging.servicebus.delivery_count", args.Message.DeliveryCount);

        try
        {
            // Create stream ID from subject or fallback to subscription name
            var streamIdKey = args.Message.Subject ?? _subscriptionName;
            var streamId = StreamId.Create("", streamIdKey);

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
            requestContext["ServiceBus.Subject"] = args.Message.Subject ?? string.Empty;
            requestContext["ServiceBus.DeliveryCount"] = args.Message.DeliveryCount;
            requestContext["ServiceBus.SubscriptionName"] = _subscriptionName;

            // Create batch container
            var sequenceToken = new EventSequenceTokenV2(DateTime.UtcNow.Ticks);
            var batchContainer = new ServiceBusBatchContainer(streamId, events, requestContext, sequenceToken);

            // Enqueue the message for processing
            _pendingMessages.Enqueue(batchContainer);

            // Complete the message on successful processing
            await args.CompleteMessageAsync(args.Message);

            // Increment the processed counter
            MessagesProcessedCounter.Add(1, 
                new KeyValuePair<string, object?>("messaging.destination.name", GetTopicName()),
                new KeyValuePair<string, object?>("messaging.servicebus.subscription.name", _subscriptionName));

            LogMessageProcessed(args.Message.MessageId, GetTopicName(), _subscriptionName, args.Message.DeliveryCount);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            
            // Check if we should move to dead letter queue
            if (args.Message.DeliveryCount >= _options.MaxDeliveryAttempts)
            {
                await args.DeadLetterMessageAsync(args.Message, "Max delivery attempts exceeded", ex.Message);
                
                // Increment dead letter counter
                MessagesDeadLetteredCounter.Add(1,
                    new KeyValuePair<string, object?>("messaging.destination.name", GetTopicName()),
                    new KeyValuePair<string, object?>("messaging.servicebus.subscription.name", _subscriptionName));

                LogMessageDeadLettered(args.Message.MessageId, GetTopicName(), _subscriptionName, args.Message.DeliveryCount, ex);
            }
            else
            {
                // Abandon the message to retry
                await args.AbandonMessageAsync(args.Message);
                LogMessageAbandoned(args.Message.MessageId, GetTopicName(), _subscriptionName, args.Message.DeliveryCount, ex);
            }
        }
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        using var activity = ServiceBusOptions.ActivitySource.StartActivity("error.process");
        activity?.SetTag("messaging.system", "azureservicebus");
        activity?.SetTag("messaging.destination.name", GetTopicName());
        activity?.SetTag("messaging.servicebus.subscription.name", _subscriptionName);
        activity?.SetStatus(ActivityStatusCode.Error, args.Exception.Message);

        LogProcessorError(args.Exception, GetTopicName(), _subscriptionName, args.ErrorSource.ToString());
        
        return Task.CompletedTask;
    }

    private string GetTopicName()
    {
        // Use the first entity name as the topic name, or generate from prefix
        if (!string.IsNullOrWhiteSpace(_options.EntityName))
        {
            return _options.EntityName;
        }

        if (_options.EntityNames is not null && _options.EntityNames.Count > 0)
        {
            return _options.EntityNames[0];
        }

        return _options.QueueNamePrefix + "-topic";
    }

    /// <summary>
    /// Asynchronously releases all resources used by the <see cref="ServiceBusTopicAdapterReceiver"/>.
    /// </summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
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
            LogDisposeError(ex, GetTopicName(), _subscriptionName);
        }
        finally
        {
            _receiveSemaphore.Dispose();
        }
    }

    [LoggerMessage(
        Level = LogLevel.Information,
        EventId = 1,
        Message = "ServiceBus topic receiver initialized for topic '{TopicName}' subscription '{SubscriptionName}' with PrefetchCount={PrefetchCount}")]
    private partial void LogReceiverInitialized(string topicName, string subscriptionName, int prefetchCount);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 2,
        Message = "Failed to initialize ServiceBus topic receiver for topic '{TopicName}' subscription '{SubscriptionName}'")]
    private partial void LogInitializationFailed(Exception exception, string topicName, string subscriptionName);

    [LoggerMessage(
        Level = LogLevel.Debug,
        EventId = 3,
        Message = "Processed ServiceBus message '{MessageId}' from topic '{TopicName}' subscription '{SubscriptionName}' (DeliveryCount={DeliveryCount})")]
    private partial void LogMessageProcessed(string messageId, string topicName, string subscriptionName, int deliveryCount);

    [LoggerMessage(
        Level = LogLevel.Warning,
        EventId = 4,
        Message = "Abandoned ServiceBus message '{MessageId}' from topic '{TopicName}' subscription '{SubscriptionName}' (DeliveryCount={DeliveryCount}) due to processing error")]
    private partial void LogMessageAbandoned(string messageId, string topicName, string subscriptionName, int deliveryCount, Exception exception);

    [LoggerMessage(
        Level = LogLevel.Warning,
        EventId = 5,
        Message = "Dead lettered ServiceBus message '{MessageId}' from topic '{TopicName}' subscription '{SubscriptionName}' (DeliveryCount={DeliveryCount}) after max delivery attempts")]
    private partial void LogMessageDeadLettered(string messageId, string topicName, string subscriptionName, int deliveryCount, Exception exception);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 6,
        Message = "ServiceBus processor error for topic '{TopicName}' subscription '{SubscriptionName}' from source '{ErrorSource}'")]
    private partial void LogProcessorError(Exception exception, string topicName, string subscriptionName, string errorSource);

    [LoggerMessage(
        Level = LogLevel.Warning,
        EventId = 7,
        Message = "ServiceBus topic receiver shutdown timeout for topic '{TopicName}' subscription '{SubscriptionName}' after {Timeout}")]
    private partial void LogShutdownTimeout(string topicName, string subscriptionName, TimeSpan timeout);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 8,
        Message = "Error during ServiceBus topic receiver shutdown for topic '{TopicName}' subscription '{SubscriptionName}'")]
    private partial void LogShutdownError(Exception exception, string topicName, string subscriptionName);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 9,
        Message = "Error during ServiceBus topic receiver disposal for topic '{TopicName}' subscription '{SubscriptionName}'")]
    private partial void LogDisposeError(Exception exception, string topicName, string subscriptionName);
}