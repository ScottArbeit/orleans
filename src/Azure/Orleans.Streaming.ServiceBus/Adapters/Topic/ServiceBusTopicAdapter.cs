using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streaming.ServiceBus;
using Orleans.Streams;

namespace Orleans.Providers.Streams.ServiceBus;

/// <summary>
/// Azure Service Bus topic adapter that implements streaming over Service Bus topics with subscriptions.
/// </summary>
public sealed partial class ServiceBusTopicAdapter : IQueueAdapter, IAsyncDisposable
{
    private readonly string _providerName;
    private readonly ServiceBusOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<ServiceBusTopicAdapter> _logger;
    private readonly HashRingBasedPartitionedStreamQueueMapper _streamQueueMapper;
    private readonly ServiceBusClientFactory _clientFactory;
    private readonly ConcurrentDictionary<QueueId, ServiceBusTopicAdapterReceiver> _receivers = new();

    private ServiceBusClient? _serviceBusClient;
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new();
    private volatile bool _disposed;

    /// <summary>
    /// Gets the name of the provider.
    /// </summary>
    public string Name { get; }
    
    /// <summary>
    /// Gets a value indicating whether the adapter supports rewinding.
    /// </summary>
    public bool IsRewindable => false;
    
    /// <summary>
    /// Gets the direction of the stream provider.
    /// </summary>
    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusTopicAdapter"/> class.
    /// </summary>
    /// <param name="providerName">The name of the stream provider.</param>
    /// <param name="options">The Service Bus configuration options.</param>
    /// <param name="streamQueueMapper">The stream queue mapper for partitioning.</param>
    /// <param name="clientFactory">The Service Bus client factory.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public ServiceBusTopicAdapter(
        string providerName,
        ServiceBusOptions options,
        HashRingBasedPartitionedStreamQueueMapper streamQueueMapper,
        ServiceBusClientFactory clientFactory,
        ILoggerFactory loggerFactory)
    {
        _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _streamQueueMapper = streamQueueMapper ?? throw new ArgumentNullException(nameof(streamQueueMapper));
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _logger = loggerFactory.CreateLogger<ServiceBusTopicAdapter>();

        Name = providerName;
    }

    /// <summary>
    /// Creates a receiver for the specified queue.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>A queue adapter receiver instance.</returns>
    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ServiceBusTopicAdapter));
        }

        return _receivers.GetOrAdd(queueId, id =>
        {
            var subscriptionName = _streamQueueMapper.QueueToPartition(id);
            var logger = _loggerFactory.CreateLogger<ServiceBusTopicAdapterReceiver>();
            return new ServiceBusTopicAdapterReceiver(subscriptionName, _providerName, _options, _clientFactory, logger);
        });
    }

    /// <summary>
    /// Queues a batch of messages to the Service Bus topic.
    /// </summary>
    /// <typeparam name="T">The type of events in the batch.</typeparam>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">The events to queue.</param>
    /// <param name="token">The stream sequence token (not supported for Service Bus).</param>
    /// <param name="requestContext">The request context containing additional properties.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown when a non-null token is provided.</exception>
    /// <exception cref="ObjectDisposedException">Thrown when the adapter has been disposed.</exception>
    public async Task QueueMessageBatchAsync<T>(
        StreamId streamId, 
        IEnumerable<T> events, 
        StreamSequenceToken? token, 
        Dictionary<string, object> requestContext)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ServiceBusTopicAdapter));
        }

        ArgumentNullException.ThrowIfNull(events);

        if (token is not null)
        {
            throw new ArgumentException("ServiceBus stream provider does not support non-null StreamSequenceToken.", nameof(token));
        }

        using var activity = ServiceBusInstrumentation.ActivitySource.StartActivity("message.send");
        activity?.SetTag(ServiceBusInstrumentation.Tags.MessagingSystem, ServiceBusInstrumentation.TagValues.MessagingSystemValue);
        activity?.SetTag(ServiceBusInstrumentation.Tags.MessagingOperation, "send");
        activity?.SetTag(ServiceBusInstrumentation.Tags.ServiceBusEntityType, ServiceBusInstrumentation.TagValues.EntityTypeTopic);

        try
        {
            var queueId = _streamQueueMapper.GetQueueForStream(streamId);
            var subscriptionName = _streamQueueMapper.QueueToPartition(queueId);
            
            // For topics, we send to the topic name, not the subscription
            var topicName = GetTopicName();
            
            activity?.SetTag(ServiceBusInstrumentation.Tags.MessagingDestinationName, topicName);

            var sender = await GetOrCreateSenderAsync(topicName);

            // Create Service Bus message
            var messageBody = JsonSerializer.Serialize(events);
            var serviceBusMessage = new ServiceBusMessage(messageBody)
            {
                Subject = streamId.ToString(),
                MessageId = Guid.NewGuid().ToString(),
                CorrelationId = Guid.NewGuid().ToString()
            };

            // Add subscription routing information
            serviceBusMessage.ApplicationProperties["Orleans.SubscriptionName"] = subscriptionName;

            // Add request context as application properties
            if (requestContext is not null)
            {
                foreach (var kvp in requestContext)
                {
                    if (kvp.Value is not null)
                    {
                        serviceBusMessage.ApplicationProperties[kvp.Key] = kvp.Value;
                    }
                }
            }

            await sender.SendMessageAsync(serviceBusMessage);

            LogMessageSent(serviceBusMessage.MessageId, topicName, streamId.ToString());
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            LogSendError(ex, streamId.ToString());
            throw;
        }
    }

    private async Task<ServiceBusSender> GetOrCreateSenderAsync(string topicName)
    {
        if (_senders.TryGetValue(topicName, out var existingSender))
        {
            return existingSender;
        }

        // Initialize client if needed
        _serviceBusClient ??= await _clientFactory.GetClientAsync(_providerName);

        var sender = _serviceBusClient.CreateSender(topicName);
        _senders[topicName] = sender;
        return sender;
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
    /// Asynchronously releases all resources used by the <see cref="ServiceBusTopicAdapter"/>.
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
            // Dispose all receivers
            foreach (var receiver in _receivers.Values)
            {
                await receiver.DisposeAsync();
            }
            _receivers.Clear();

            // Dispose all senders
            foreach (var sender in _senders.Values)
            {
                await sender.DisposeAsync();
            }
            _senders.Clear();
        }
        catch (Exception ex)
        {
            LogDisposeError(ex);
        }
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        EventId = 1,
        Message = "Sent ServiceBus message '{MessageId}' to topic '{TopicName}' for stream '{StreamId}'")]
    private partial void LogMessageSent(string messageId, string topicName, string streamId);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 2,
        Message = "Failed to send message to ServiceBus for stream '{StreamId}'")]
    private partial void LogSendError(Exception exception, string streamId);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 3,
        Message = "Error during ServiceBus topic adapter disposal")]
    private partial void LogDisposeError(Exception exception);
}