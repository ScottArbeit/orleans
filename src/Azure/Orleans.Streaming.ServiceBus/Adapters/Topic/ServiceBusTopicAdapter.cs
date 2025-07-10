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

    public string Name { get; }
    public bool IsRewindable => false;
    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

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

        using var activity = ServiceBusOptions.ActivitySource.StartActivity("message.send");
        activity?.SetTag("messaging.system", "azureservicebus");
        activity?.SetTag("messaging.operation", "send");

        try
        {
            var queueId = _streamQueueMapper.GetQueueForStream(streamId);
            var subscriptionName = _streamQueueMapper.QueueToPartition(queueId);
            
            // For topics, we send to the topic name, not the subscription
            var topicName = GetTopicName();
            
            activity?.SetTag("messaging.destination.name", topicName);

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