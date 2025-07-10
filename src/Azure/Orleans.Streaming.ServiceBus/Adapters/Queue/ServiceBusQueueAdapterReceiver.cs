using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.ServiceBus;

/// <summary>
/// Queue adapter receiver for Azure Service Bus queues using ServiceBusProcessor.
/// </summary>
internal class ServiceBusQueueAdapterReceiver : IQueueAdapterReceiver
{
    private static readonly ActivitySource ActivitySource = ServiceBusOptions.ActivitySource;
    private static readonly Meter Meter = new("Orleans.ServiceBus");
    private static readonly Counter<int> MessagesProcessedCounter = Meter.CreateCounter<int>("servicebus.queue.messages_processed");

    private readonly ILogger logger;
    private readonly string queueName;
    private readonly ServiceBusOptions options;
    private readonly QueueId queueId;
    
    private ServiceBusClient? serviceBusClient;
    private ServiceBusProcessor? processor;
    private bool isInitialized = false;
    private bool isShuttingDown = false;

    // Queue for collecting received messages
    private readonly Queue<IBatchContainer> receivedMessages = new();
    private readonly object receivedMessagesLock = new();

    public ServiceBusQueueAdapterReceiver(
        QueueId queueId,
        string queueName,
        ServiceBusOptions options,
        ILogger logger)
    {
        this.queueId = queueId;
        this.queueName = queueName;
        this.options = options;
        this.logger = logger;
    }

    public async Task Initialize(TimeSpan timeout)
    {
        if (isInitialized)
            return;

        logger.LogInformation("Initializing ServiceBus queue adapter receiver for queue {QueueName}", queueName);

        try
        {
            // Create ServiceBus client
            serviceBusClient = await CreateServiceBusClient();
            
            // Configure processor options
            var processorOptions = new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = 1, // Process one message at a time per receiver
                PrefetchCount = options.PrefetchCount,
                AutoCompleteMessages = false, // We'll handle completion manually
                MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5)
            };

            // Create processor
            processor = serviceBusClient.CreateProcessor(queueName, processorOptions);
            
            // Set up event handlers
            processor.ProcessMessageAsync += ProcessMessageAsync;
            processor.ProcessErrorAsync += ProcessErrorAsync;

            // Start processing
            await processor.StartProcessingAsync();
            
            isInitialized = true;
            logger.LogInformation("ServiceBus queue adapter receiver initialized for queue {QueueName}", queueName);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to initialize ServiceBus queue adapter receiver for queue {QueueName}", queueName);
            throw;
        }
    }

    public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        if (!isInitialized || isShuttingDown)
            return Task.FromResult<IList<IBatchContainer>>(Array.Empty<IBatchContainer>());

        var messages = new List<IBatchContainer>();
        
        lock (receivedMessagesLock)
        {
            var count = Math.Min(maxCount, receivedMessages.Count);
            for (int i = 0; i < count; i++)
            {
                if (receivedMessages.TryDequeue(out var message))
                {
                    messages.Add(message);
                }
            }
        }

        return Task.FromResult<IList<IBatchContainer>>(messages);
    }

    public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        // Messages are completed automatically in ProcessMessageAsync
        // This is called after successful delivery to all consumers
        logger.LogDebug("Messages delivered successfully for queue {QueueName}, count: {MessageCount}", queueName, messages.Count);
        return Task.CompletedTask;
    }

    public async Task Shutdown(TimeSpan timeout)
    {
        if (!isInitialized || isShuttingDown)
            return;

        isShuttingDown = true;
        logger.LogInformation("Shutting down ServiceBus queue adapter receiver for queue {QueueName}", queueName);

        try
        {
            if (processor is not null)
            {
                await processor.StopProcessingAsync();
                await processor.DisposeAsync();
            }

            if (serviceBusClient is not null)
            {
                await serviceBusClient.DisposeAsync();
            }

            logger.LogInformation("ServiceBus queue adapter receiver shutdown completed for queue {QueueName}", queueName);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error during ServiceBus queue adapter receiver shutdown for queue {QueueName}", queueName);
            throw;
        }
    }

    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        using var activity = ActivitySource.StartActivity("ServiceBus.ProcessMessage");
        activity?.SetTag("messaging.system", "azureservicebus");
        activity?.SetTag("messaging.destination", queueName);
        activity?.SetTag("messaging.destination_kind", "queue");

        try
        {
            // Convert message body to IBatchContainer
            var batchContainer = ConvertMessageToBatchContainer(args.Message);
            
            // Add to received messages queue
            lock (receivedMessagesLock)
            {
                receivedMessages.Enqueue(batchContainer);
            }

            // Complete the message
            await args.CompleteMessageAsync(args.Message);
            
            // Increment counter
            MessagesProcessedCounter.Add(1, new KeyValuePair<string, object?>("queue.name", queueName));
            
            logger.LogDebug("Processed message from queue {QueueName}, MessageId: {MessageId}", queueName, args.Message.MessageId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing message from queue {QueueName}, MessageId: {MessageId}", queueName, args.Message.MessageId);
            
            // Check delivery count and handle accordingly
            if (args.Message.DeliveryCount >= options.MaxDeliveryAttempts)
            {
                logger.LogWarning("Message exceeded max delivery attempts ({MaxAttempts}), moving to dead letter queue. Queue: {QueueName}, MessageId: {MessageId}", 
                    options.MaxDeliveryAttempts, queueName, args.Message.MessageId);
                
                await args.DeadLetterMessageAsync(args.Message, "MaxDeliveryAttemptsExceeded", 
                    $"Message failed processing after {args.Message.DeliveryCount} attempts");
            }
            else
            {
                // Let the message be retried by not completing it
                logger.LogInformation("Message will be retried. Queue: {QueueName}, MessageId: {MessageId}, DeliveryCount: {DeliveryCount}", 
                    queueName, args.Message.MessageId, args.Message.DeliveryCount);
            }
        }
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        using var activity = ActivitySource.StartActivity("ServiceBus.ProcessError");
        activity?.SetTag("messaging.system", "azureservicebus");
        activity?.SetTag("messaging.destination", queueName);
        activity?.SetTag("error.type", args.Exception.GetType().Name);

        logger.LogError(args.Exception, "ServiceBus processor error for queue {QueueName}. Source: {ErrorSource}, FullyQualifiedNamespace: {Namespace}", 
            queueName, args.ErrorSource, args.FullyQualifiedNamespace);

        return Task.CompletedTask;
    }

    private ServiceBusQueueBatchContainer ConvertMessageToBatchContainer(ServiceBusReceivedMessage message)
    {
        // Create StreamId from queue name
        var streamId = StreamId.Create("ServiceBusQueue", queueName);
        
        // Extract message body - assume it's a serialized event
        var eventData = message.Body.ToObjectFromJson<object>();
        var events = new List<object> { eventData };
        
        // Extract request context from message properties
        var requestContext = new Dictionary<string, object>();
        foreach (var prop in message.ApplicationProperties)
        {
            requestContext[prop.Key] = prop.Value;
        }

        // Create sequence token from message metadata
        var sequenceToken = new EventSequenceToken(
            sequenceNumber: long.Parse(message.SequenceNumber.ToString()),
            eventIndex: 0);

        var batchContainer = new ServiceBusQueueBatchContainer(streamId, events, requestContext);
        batchContainer.RealSequenceToken = sequenceToken;

        return batchContainer;
    }

    private async Task<ServiceBusClient> CreateServiceBusClient()
    {
        if (options.CreateClient is not null)
        {
            return await options.CreateClient();
        }
        
        if (options.ServiceBusClient is not null)
        {
            return options.ServiceBusClient;
        }

        throw new InvalidOperationException("No ServiceBus client configuration found. Use ServiceBusOptions.ConfigureServiceBusClient to configure the client.");
    }
}