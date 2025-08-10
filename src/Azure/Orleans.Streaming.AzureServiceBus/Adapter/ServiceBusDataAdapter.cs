using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Azure Service Bus data adapter that converts between Orleans streaming events and Service Bus messages.
/// </summary>
public class ServiceBusDataAdapter : IQueueDataAdapter<ServiceBusMessage, ServiceBusBatchContainer>
{
    private readonly Serializer<ServiceBusBatchContainer> serializer;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusDataAdapter"/> class.
    /// </summary>
    /// <param name="serializer">The serializer for batch containers.</param>
    public ServiceBusDataAdapter(Serializer<ServiceBusBatchContainer> serializer)
    {
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    /// <summary>
    /// Creates a Service Bus message from stream event data.
    /// </summary>
    /// <typeparam name="T">The stream event type.</typeparam>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">The events to include in the message.</param>
    /// <param name="token">The sequence token (ignored for Service Bus as we use ephemeral tokens).</param>
    /// <param name="requestContext">The request context.</param>
    /// <returns>A new Service Bus message containing the stream events.</returns>
    public ServiceBusMessage ToQueueMessage<T>(
        StreamId streamId, 
        IEnumerable<T> events, 
        StreamSequenceToken? token, 
        Dictionary<string, object>? requestContext)
    {
        ArgumentNullException.ThrowIfNull(events);

        var eventList = events.Cast<object>().ToList();
        var batchContainer = ServiceBusBatchContainer.CreateContainer(streamId, eventList, requestContext ?? new Dictionary<string, object>());
        
        // Serialize the batch container to bytes
        var messageBody = serializer.SerializeToArray(batchContainer);
        
        // Create the Service Bus message
        var serviceBusMessage = new ServiceBusMessage(messageBody)
        {
            ContentType = Headers.ContentType
        };

        // Add stream metadata to application properties
        serviceBusMessage.ApplicationProperties[Headers.StreamNamespace] = streamId.GetNamespace();
        serviceBusMessage.ApplicationProperties[Headers.StreamId] = streamId.GetKeyAsString();
        
        // Add sequence token information if available
        if (token is not null)
        {
            serviceBusMessage.ApplicationProperties[Headers.SequenceToken] = JsonSerializer.Serialize(new
            {
                SequenceNumber = token.SequenceNumber,
                EventIndex = token.EventIndex
            });
        }

        return serviceBusMessage;
    }

    /// <summary>
    /// Creates a batch container from a Service Bus message.
    /// </summary>
    /// <param name="queueMessage">The Service Bus message.</param>
    /// <param name="sequenceId">The sequence identifier for creating ephemeral tokens.</param>
    /// <returns>A batch container with the deserialized events.</returns>
    public ServiceBusBatchContainer FromQueueMessage(ServiceBusMessage queueMessage, long sequenceId)
    {
        ArgumentNullException.ThrowIfNull(queueMessage);

        // Deserialize the batch container from the message body
        var messageBody = queueMessage.Body.ToArray();
        var batchContainer = serializer.Deserialize(messageBody);

        // Extract stream metadata from application properties
        var streamNamespace = GetApplicationProperty<string>(queueMessage, Headers.StreamNamespace);
        var streamKey = GetApplicationProperty<string>(queueMessage, Headers.StreamId);
        
        if (streamNamespace is null || streamKey is null)
        {
            throw new InvalidOperationException(
                $"Service Bus message is missing required stream metadata. " +
                $"Expected properties: {Headers.StreamNamespace}, {Headers.StreamId}");
        }

        // Reconstruct the stream ID
        var streamId = StreamId.Create(streamNamespace, streamKey);

        // Create a new batch container with the ephemeral sequence token
        return ServiceBusBatchContainer.CreateWithSequenceId(
            streamId, 
            GetEventsFromBatchContainer(batchContainer), 
            GetRequestContextFromBatchContainer(batchContainer),
            sequenceId);
    }

    /// <summary>
    /// Gets an application property value from a Service Bus message.
    /// </summary>
    /// <typeparam name="T">The expected type of the property value.</typeparam>
    /// <param name="message">The Service Bus message.</param>
    /// <param name="propertyKey">The property key.</param>
    /// <returns>The property value, or default(T) if not found.</returns>
    private static T? GetApplicationProperty<T>(ServiceBusMessage message, string propertyKey)
    {
        if (message.ApplicationProperties.TryGetValue(propertyKey, out var value) && value is T typedValue)
        {
            return typedValue;
        }
        return default(T);
    }

    /// <summary>
    /// Extracts the events list from a batch container using reflection.
    /// This is needed because the events field is private in the deserialized container.
    /// </summary>
    /// <param name="batchContainer">The batch container.</param>
    /// <returns>The list of events.</returns>
    private static List<object> GetEventsFromBatchContainer(ServiceBusBatchContainer batchContainer)
    {
        // Since we're deserializing our own container, we can use GetEvents to reconstruct the events list
        var events = new List<object>();
        foreach (var eventTuple in batchContainer.GetEvents<object>())
        {
            events.Add(eventTuple.Item1);
        }
        return events;
    }

    /// <summary>
    /// Extracts the request context from a batch container.
    /// </summary>
    /// <param name="batchContainer">The batch container.</param>
    /// <returns>The request context dictionary.</returns>
    private static Dictionary<string, object> GetRequestContextFromBatchContainer(ServiceBusBatchContainer batchContainer)
    {
        // For now, return an empty dictionary since request context is private
        // In a full implementation, this would need proper access to the private field
        return new Dictionary<string, object>();
    }
}