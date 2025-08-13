using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Implementation of <see cref="IQueueAdapter"/> for Azure Service Bus streaming.
/// Provides the publishing path to send events to Service Bus queues or topics.
/// This implementation follows non-rewindable semantics similar to ASQ/SQS.
/// </summary>
internal class ServiceBusAdapter : IQueueAdapter, IDisposable
{
    private readonly string _providerName;
    private readonly ServiceBusStreamOptions _options;
    private readonly ServiceBusDataAdapter _dataAdapter;
    private readonly ServiceBusQueueMapper _queueMapper;
    private readonly ILogger<ServiceBusAdapter> _logger;
    private readonly ServiceBusClient _serviceBusClient;
    private readonly ServiceBusSender _serviceBusSender;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusAdapter"/> class.
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <param name="dataAdapter">The data adapter for message conversion.</param>
    /// <param name="queueMapper">The queue mapper.</param>
    /// <param name="logger">The logger.</param>
    /// <exception cref="ArgumentNullException">Thrown when any required parameter is null.</exception>
    /// <exception cref="ArgumentException">Thrown when providerName is null or whitespace.</exception>
    public ServiceBusAdapter(
        string providerName,
        ServiceBusStreamOptions options,
        ServiceBusDataAdapter dataAdapter,
        ServiceBusQueueMapper queueMapper,
        ILogger<ServiceBusAdapter> logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(dataAdapter);
        ArgumentNullException.ThrowIfNull(queueMapper);
        ArgumentNullException.ThrowIfNull(logger);

        _providerName = providerName;
        _options = options;
        _dataAdapter = dataAdapter;
        _queueMapper = queueMapper;
        _logger = logger;

        // Create Service Bus client
        _serviceBusClient = CreateServiceBusClient(options);

        // Create Service Bus sender based on entity type
        _serviceBusSender = CreateServiceBusSender(_serviceBusClient, options);

        _logger.LogInformation(
            "ServiceBus adapter initialized for provider '{ProviderName}' with entity kind '{EntityKind}', " +
            "entity name '{EntityName}', batch size {BatchSize}",
            _providerName,
            _options.EntityKind,
            ServiceBusEntityNamer.GetEntityName(_options),
            _options.Publisher.BatchSize);
    }

    /// <summary>
    /// Gets the name of the adapter.
    /// </summary>
    public string Name => _providerName;

    /// <summary>
    /// Gets the stream direction supported by the adapter. Service Bus supports only writing in this implementation.
    /// </summary>
    public StreamProviderDirection Direction => StreamProviderDirection.WriteOnly;

    /// <summary>
    /// Gets a value indicating whether this is a rewindable stream adapter.
    /// Service Bus adapter is non-rewindable following ASQ/SQS semantics.
    /// </summary>
    public bool IsRewindable => false;

    /// <summary>
    /// Writes a batch of events to the Service Bus queue or topic.
    /// </summary>
    /// <typeparam name="T">The event type.</typeparam>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">The events to write.</param>
    /// <param name="token">The sequence token (ignored for Service Bus as we use ephemeral tokens).</param>
    /// <param name="requestContext">The request context.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task QueueMessageBatchAsync<T>(
        StreamId streamId,
        IEnumerable<T> events,
        StreamSequenceToken? token,
        Dictionary<string, object> requestContext)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ServiceBusAdapter));
        }

        ArgumentNullException.ThrowIfNull(events);
        ArgumentNullException.ThrowIfNull(requestContext);

        var eventList = events.ToList();
        if (eventList.Count == 0)
        {
            _logger.LogDebug("Empty event batch received for stream {StreamNamespace}:{StreamKey}, ignoring",
                streamId.GetNamespace(), streamId.GetKeyAsString());
            return;
        }

        try
        {
            // Process events in batches according to configured batch size
            var batchSize = _options.Publisher.BatchSize;
            var batches = CreateBatches(eventList, batchSize);

            foreach (var batch in batches)
            {
                await SendBatchAsync(streamId, batch, token, requestContext);
            }

            _logger.LogDebug(
                "Successfully queued {EventCount} events in {BatchCount} batches for stream {StreamNamespace}:{StreamKey}",
                eventList.Count,
                batches.Count,
                streamId.GetNamespace(),
                streamId.GetKeyAsString());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to queue {EventCount} events for stream {StreamNamespace}:{StreamKey} to Service Bus entity '{EntityName}'",
                eventList.Count,
                streamId.GetNamespace(),
                streamId.GetKeyAsString(),
                ServiceBusEntityNamer.GetEntityName(_options));
            throw;
        }
    }

    /// <summary>
    /// Creates a queue receiver. Not implemented for write-only adapter.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>Not implemented.</returns>
    /// <exception cref="NotImplementedException">This adapter is write-only.</exception>
    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        throw new NotImplementedException("ServiceBus adapter in this step is write-only. Receiver will be implemented in later steps.");
    }

    /// <summary>
    /// Disposes the adapter and releases Service Bus client resources.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            _serviceBusSender?.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(10));
            _serviceBusClient?.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(10));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing Service Bus resources for provider '{ProviderName}'", _providerName);
        }

        _disposed = true;
        _logger.LogInformation("ServiceBus adapter disposed for provider '{ProviderName}'", _providerName);
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
    /// Creates the Service Bus sender based on the configured entity type.
    /// </summary>
    /// <param name="client">The Service Bus client.</param>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <returns>A configured Service Bus sender.</returns>
    /// <exception cref="InvalidOperationException">Thrown when entity configuration is invalid.</exception>
    private static ServiceBusSender CreateServiceBusSender(ServiceBusClient client, ServiceBusStreamOptions options)
    {
        return options.EntityKind switch
        {
            EntityKind.Queue => client.CreateSender(options.QueueName),
            EntityKind.TopicSubscription => client.CreateSender(options.TopicName),
            _ => throw new InvalidOperationException($"Unsupported entity kind: {options.EntityKind}")
        };
    }

    /// <summary>
    /// Creates batches of events based on the configured batch size.
    /// </summary>
    /// <typeparam name="T">The event type.</typeparam>
    /// <param name="events">The events to batch.</param>
    /// <param name="batchSize">The maximum batch size.</param>
    /// <returns>A collection of event batches.</returns>
    private static List<List<T>> CreateBatches<T>(List<T> events, int batchSize)
    {
        var batches = new List<List<T>>();
        
        for (int i = 0; i < events.Count; i += batchSize)
        {
            var remainingCount = Math.Min(batchSize, events.Count - i);
            var batch = events.GetRange(i, remainingCount);
            batches.Add(batch);
        }

        return batches;
    }

    /// <summary>
    /// Sends a single batch of events to Service Bus.
    /// </summary>
    /// <typeparam name="T">The event type.</typeparam>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">The events to send.</param>
    /// <param name="token">The sequence token.</param>
    /// <param name="requestContext">The request context.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private async Task SendBatchAsync<T>(
        StreamId streamId,
        List<T> events,
        StreamSequenceToken? token,
        Dictionary<string, object> requestContext)
    {
        // Convert events to Service Bus messages using the data adapter
        var serviceBusMessage = _dataAdapter.ToQueueMessage(streamId, events, token, requestContext);

        // Apply session ID strategy if configured
        ApplySessionStrategy(serviceBusMessage, streamId);

        // Apply message TTL if configured
        if (_options.Publisher.MessageTimeToLive > TimeSpan.Zero)
        {
            serviceBusMessage.TimeToLive = _options.Publisher.MessageTimeToLive;
        }

        try
        {
            // Send the message to Service Bus
            await _serviceBusSender.SendMessageAsync(serviceBusMessage);

            _logger.LogDebug(
                "Successfully sent batch of {EventCount} events for stream {StreamNamespace}:{StreamKey} " +
                "to Service Bus entity '{EntityName}' with message ID '{MessageId}'",
                events.Count,
                streamId.GetNamespace(),
                streamId.GetKeyAsString(),
                ServiceBusEntityNamer.GetEntityName(_options),
                serviceBusMessage.MessageId);
        }
        catch (ServiceBusException sbEx) when (sbEx.Reason == ServiceBusFailureReason.QuotaExceeded)
        {
            _logger.LogWarning(sbEx,
                "Service Bus quota exceeded while sending batch of {EventCount} events for stream {StreamNamespace}:{StreamKey} " +
                "to entity '{EntityName}'. Consider reducing batch size or implementing throttling.",
                events.Count,
                streamId.GetNamespace(),
                streamId.GetKeyAsString(),
                ServiceBusEntityNamer.GetEntityName(_options));
            throw;
        }
        catch (ServiceBusException sbEx) when (sbEx.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
        {
            _logger.LogError(sbEx,
                "Service Bus entity '{EntityName}' not found while sending batch for stream {StreamNamespace}:{StreamKey}. " +
                "Ensure the entity exists or enable AutoCreateEntities.",
                ServiceBusEntityNamer.GetEntityName(_options),
                streamId.GetNamespace(),
                streamId.GetKeyAsString());
            throw;
        }
    }

    /// <summary>
    /// Applies the session ID strategy to the Service Bus message if configured.
    /// </summary>
    /// <param name="message">The Service Bus message.</param>
    /// <param name="streamId">The stream identifier.</param>
    private void ApplySessionStrategy(ServiceBusMessage message, StreamId streamId)
    {
        if (_options.Publisher.SessionIdStrategy == SessionIdStrategy.UseStreamId)
        {
            // Use the stream ID as the session ID for ordered message delivery
            message.SessionId = streamId.ToString();
            
            _logger.LogTrace(
                "Applied session ID '{SessionId}' to message for stream {StreamNamespace}:{StreamKey}",
                message.SessionId,
                streamId.GetNamespace(),
                streamId.GetKeyAsString());
        }
    }
}