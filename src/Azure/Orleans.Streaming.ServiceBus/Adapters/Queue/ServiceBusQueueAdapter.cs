using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.ServiceBus;

/// <summary>
/// Queue adapter for Azure Service Bus queues.
/// </summary>
internal class ServiceBusQueueAdapter : IQueueAdapter
{
    private static readonly ActivitySource ActivitySource = ServiceBusOptions.ActivitySource;

    private readonly ServiceBusOptions options;
    private readonly HashRingBasedPartitionedStreamQueueMapper streamQueueMapper;
    private readonly ILoggerFactory loggerFactory;
    private readonly ILogger logger;
    private readonly ConcurrentDictionary<QueueId, ServiceBusQueueAdapterReceiver> receivers = new();
    private readonly ConcurrentDictionary<string, ServiceBusSender> senders = new();
    
    private ServiceBusClient? serviceBusClient;

    public string Name { get; }
    public bool IsRewindable => false;
    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    public ServiceBusQueueAdapter(
        string name,
        ServiceBusOptions options,
        HashRingBasedPartitionedStreamQueueMapper streamQueueMapper,
        ILoggerFactory loggerFactory)
    {
        Name = name;
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.streamQueueMapper = streamQueueMapper ?? throw new ArgumentNullException(nameof(streamQueueMapper));
        this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        this.logger = loggerFactory.CreateLogger<ServiceBusQueueAdapter>();
    }

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        var queueName = streamQueueMapper.QueueToPartition(queueId);
        var receiver = new ServiceBusQueueAdapterReceiver(
            queueId,
            queueName,
            options,
            loggerFactory.CreateLogger<ServiceBusQueueAdapterReceiver>());

        receivers.TryAdd(queueId, receiver);
        return receiver;
    }

    public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken? token, Dictionary<string, object> requestContext)
    {
        if (token is not null)
        {
            throw new ArgumentException("ServiceBus stream provider currently does not support non-null StreamSequenceToken.", nameof(token));
        }

        using var activity = ActivitySource.StartActivity("ServiceBus.QueueMessageBatch");
        activity?.SetTag("messaging.system", "azureservicebus");
        activity?.SetTag("messaging.destination_kind", "queue");

        var queueId = streamQueueMapper.GetQueueForStream(streamId);
        var queueName = streamQueueMapper.QueueToPartition(queueId);
        
        activity?.SetTag("messaging.destination", queueName);

        try
        {
            var sender = await GetOrCreateSender(queueName);
            var eventsList = events.ToList();
            
            logger.LogDebug("Sending {EventCount} events to queue {QueueName} for stream {StreamId}", 
                eventsList.Count, queueName, streamId);

            foreach (var evt in eventsList)
            {
                var message = CreateServiceBusMessage(evt, requestContext);
                await sender.SendMessageAsync(message);
            }

            logger.LogDebug("Successfully sent {EventCount} events to queue {QueueName} for stream {StreamId}", 
                eventsList.Count, queueName, streamId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to send events to queue {QueueName} for stream {StreamId}", queueName, streamId);
            throw;
        }
    }

    private async Task<ServiceBusSender> GetOrCreateSender(string queueName)
    {
        if (senders.TryGetValue(queueName, out var existingSender))
        {
            return existingSender;
        }

        var client = await GetServiceBusClient();
        var sender = client.CreateSender(queueName);
        
        senders.TryAdd(queueName, sender);
        return sender;
    }

    private async Task<ServiceBusClient> GetServiceBusClient()
    {
        if (serviceBusClient is not null)
        {
            return serviceBusClient;
        }

        if (options.CreateClient is not null)
        {
            serviceBusClient = await options.CreateClient();
            return serviceBusClient;
        }
        
        if (options.ServiceBusClient is not null)
        {
            serviceBusClient = options.ServiceBusClient;
            return serviceBusClient;
        }

        throw new InvalidOperationException("No ServiceBus client configuration found. Use ServiceBusOptions.ConfigureServiceBusClient to configure the client.");
    }

    private static ServiceBusMessage CreateServiceBusMessage<T>(T eventData, Dictionary<string, object> requestContext)
    {
        // Serialize the event data
        var json = JsonConvert.SerializeObject(eventData);
        var message = new ServiceBusMessage(json);

        // Add request context as message properties
        if (requestContext is not null)
        {
            foreach (var kvp in requestContext)
            {
                message.ApplicationProperties[kvp.Key] = kvp.Value;
            }
        }

        // Add additional metadata
        message.ApplicationProperties["EventType"] = typeof(T).Name;
        message.ApplicationProperties["Timestamp"] = DateTimeOffset.UtcNow.ToString("O");

        return message;
    }
}