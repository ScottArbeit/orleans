#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Providers.Streams.AzureServiceBus;

/// <summary>
/// Azure Service Bus data adapter that converts between Orleans streaming messages and Azure Service Bus messages.
/// Handles serialization, metadata, and message properties.
/// </summary>
[SerializationCallbacks(typeof(OnDeserializedCallbacks))]
public class AzureServiceBusDataAdapter : IQueueDataAdapter<ServiceBusMessage, IBatchContainer>, IOnDeserialized
{
    private readonly ILogger _logger;
    private Serializer<AzureServiceBusBatchContainer> _serializer;

    /// <summary>
    /// Initializes a new instance of the <see cref="AzureServiceBusDataAdapter"/> class.
    /// </summary>
    /// <param name="serializer">The Orleans serializer.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public AzureServiceBusDataAdapter(Serializer serializer, ILoggerFactory loggerFactory)
    {
        if (serializer is null)
        {
            throw new ArgumentNullException(nameof(serializer));
        }

        if (loggerFactory is null)
        {
            throw new ArgumentNullException(nameof(loggerFactory));
        }

        _serializer = serializer.GetSerializer<AzureServiceBusBatchContainer>();
        _logger = loggerFactory.CreateLogger<AzureServiceBusDataAdapter>();
    }

    /// <summary>
    /// Creates a Service Bus message from Orleans stream event data.
    /// </summary>
    /// <typeparam name="T">The stream event type.</typeparam>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">The events to include in the message.</param>
    /// <param name="token">The stream sequence token.</param>
    /// <param name="requestContext">The request context to propagate.</param>
    /// <returns>A new Service Bus message containing the serialized batch container.</returns>
    public ServiceBusMessage ToQueueMessage<T>(
        StreamId streamId, 
        IEnumerable<T> events, 
        StreamSequenceToken? token, 
        Dictionary<string, object> requestContext)
    {
        if (events is null)
        {
            throw new ArgumentNullException(nameof(events));
        }

        requestContext ??= new Dictionary<string, object>();

        try
        {
            // Create the batch container with Orleans events
            var eventList = events.Cast<object>().ToList();
            var batchContainer = new AzureServiceBusBatchContainer(streamId, eventList, requestContext);

            // Serialize the batch container using Orleans serialization
            var serializedData = _serializer.SerializeToArray(batchContainer);

            // Create Service Bus message
            var message = new ServiceBusMessage(serializedData)
            {
                ContentType = "application/x-orleans-batch",
                CorrelationId = streamId.ToString(),
                MessageId = Guid.NewGuid().ToString()
            };

            // Add stream metadata as application properties
            message.ApplicationProperties["StreamId"] = streamId.ToString();
            if (streamId.GetNamespace() is string streamNamespace)
            {
                message.ApplicationProperties["StreamNamespace"] = streamNamespace;
            }

            // Add request context as application properties
            foreach (var kvp in requestContext)
            {
                if (kvp.Value is string strValue)
                {
                    message.ApplicationProperties[$"RequestContext.{kvp.Key}"] = strValue;
                }
                else if (kvp.Value is not null)
                {
                    // Convert non-string values to string representation
                    message.ApplicationProperties[$"RequestContext.{kvp.Key}"] = kvp.Value.ToString();
                }
            }

            // Add sequence token information if available
            if (token is EventSequenceTokenV2 eventToken)
            {
                message.ApplicationProperties["SequenceNumber"] = eventToken.SequenceNumber;
                message.ApplicationProperties["EventIndex"] = eventToken.EventIndex;
            }

            _logger.LogDebug("Created Service Bus message for stream {StreamId} with {EventCount} events", 
                streamId, eventList.Count);

            return message;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create Service Bus message for stream {StreamId}", streamId);
            throw new InvalidOperationException($"Failed to serialize events to Service Bus message for stream {streamId}", ex);
        }
    }

    /// <summary>
    /// Creates a batch container from a Service Bus message.
    /// </summary>
    /// <param name="message">The Service Bus message to deserialize.</param>
    /// <param name="sequenceId">The sequence identifier from the message broker.</param>
    /// <returns>The deserialized batch container.</returns>
    public IBatchContainer FromQueueMessage(ServiceBusMessage message, long sequenceId)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        try
        {
            // Deserialize the batch container from message body
            var batchContainer = _serializer.Deserialize(message.Body.ToArray());
            
            if (batchContainer is null)
            {
                throw new InvalidOperationException("Deserialized batch container is null");
            }

            // Create sequence token from Service Bus metadata
            var sequenceNumber = sequenceId;
            var eventIndex = 0;

            // Try to get more specific sequence information from application properties
            if (message.ApplicationProperties.TryGetValue("SequenceNumber", out var storedSeqNum) && 
                storedSeqNum is long seqNum)
            {
                sequenceNumber = seqNum;
            }

            if (message.ApplicationProperties.TryGetValue("EventIndex", out var storedEventIndex) && 
                storedEventIndex is int eventIdx)
            {
                eventIndex = eventIdx;
            }

            // Set the real sequence token using the Service Bus sequence information
            batchContainer.RealSequenceToken = new EventSequenceTokenV2(sequenceNumber, eventIndex);

            _logger.LogDebug("Deserialized Service Bus message for stream {StreamId} with sequence {SequenceNumber}", 
                batchContainer.StreamId, sequenceNumber);

            return batchContainer;
        }
        catch (Exception ex) when (ex is not ArgumentNullException)
        {
            _logger.LogError(ex, "Failed to deserialize Service Bus message with MessageId: {MessageId}", 
                message.MessageId);
            throw new InvalidOperationException($"Failed to deserialize Service Bus message: {message.MessageId}", ex);
        }
    }

    /// <summary>
    /// Called when the adapter is deserialized to restore dependencies.
    /// </summary>
    /// <param name="context">The deserialization context.</param>
    void IOnDeserialized.OnDeserialized(DeserializationContext context)
    {
        _serializer = context.ServiceProvider.GetRequiredService<Serializer<AzureServiceBusBatchContainer>>();
    }
}