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
/// Azure Service Bus queue adapter that implements streaming over Service Bus queues.
/// </summary>
public sealed partial class ServiceBusQueueAdapter : IQueueAdapter, IAsyncDisposable
{
    private readonly string _providerName;
    private readonly ServiceBusOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<ServiceBusQueueAdapter> _logger;
    private readonly HashRingBasedPartitionedStreamQueueMapper _streamQueueMapper;
    private readonly ServiceBusClientFactory _clientFactory;
    private readonly ConcurrentDictionary<QueueId, ServiceBusQueueAdapterReceiver> _receivers = new();

    private ServiceBusClient? _serviceBusClient;
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new();
    private volatile bool _disposed;

    public string Name { get; }
    public bool IsRewindable => false;
    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    public ServiceBusQueueAdapter(
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
        _logger = loggerFactory.CreateLogger<ServiceBusQueueAdapter>();

        Name = providerName;
    }

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ServiceBusQueueAdapter));
        }

        return _receivers.GetOrAdd(queueId, id =>
        {
            var queueName = _streamQueueMapper.QueueToPartition(id);
            var logger = _loggerFactory.CreateLogger<ServiceBusQueueAdapterReceiver>();
            return new ServiceBusQueueAdapterReceiver(queueName, _providerName, _options, _clientFactory, logger);
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
            throw new ObjectDisposedException(nameof(ServiceBusQueueAdapter));
        }

        ArgumentNullException.ThrowIfNull(events);

        if (token is not null)
        {
            throw new ArgumentException("ServiceBus stream provider does not support non-null StreamSequenceToken.", nameof(token));
        }

        using var activity = ServiceBusInstrumentation.ActivitySource.StartActivity("message.send");
        activity?.SetTag(ServiceBusInstrumentation.Tags.MessagingSystem, ServiceBusInstrumentation.TagValues.MessagingSystemValue);
        activity?.SetTag(ServiceBusInstrumentation.Tags.MessagingOperation, "send");
        activity?.SetTag(ServiceBusInstrumentation.Tags.ServiceBusEntityType, ServiceBusInstrumentation.TagValues.EntityTypeQueue);

        try
        {
            var queueId = _streamQueueMapper.GetQueueForStream(streamId);
            var queueName = _streamQueueMapper.QueueToPartition(queueId);
            
            activity?.SetTag(ServiceBusInstrumentation.Tags.MessagingDestinationName, queueName);

            var sender = await GetOrCreateSenderAsync(queueName);

            // Create Service Bus message
            var messageBody = JsonSerializer.Serialize(events);
            var serviceBusMessage = new ServiceBusMessage(messageBody)
            {
                Subject = streamId.ToString(),
                MessageId = Guid.NewGuid().ToString(),
                CorrelationId = Guid.NewGuid().ToString()
            };

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

            LogMessageSent(serviceBusMessage.MessageId, queueName, streamId.ToString());
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            LogSendError(ex, streamId.ToString());
            throw;
        }
    }

    private async Task<ServiceBusSender> GetOrCreateSenderAsync(string queueName)
    {
        if (_senders.TryGetValue(queueName, out var existingSender))
        {
            return existingSender;
        }

        // Initialize client if needed
        _serviceBusClient ??= await _clientFactory.GetClientAsync(_providerName);

        var sender = _serviceBusClient.CreateSender(queueName);
        _senders[queueName] = sender;
        return sender;
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
        Message = "Sent ServiceBus message '{MessageId}' to queue '{QueueName}' for stream '{StreamId}'")]
    private partial void LogMessageSent(string messageId, string queueName, string streamId);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 2,
        Message = "Failed to send message to ServiceBus for stream '{StreamId}'")]
    private partial void LogSendError(Exception exception, string streamId);

    [LoggerMessage(
        Level = LogLevel.Error,
        EventId = 3,
        Message = "Error during ServiceBus queue adapter disposal")]
    private partial void LogDisposeError(Exception exception);
}