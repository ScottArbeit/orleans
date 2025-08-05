using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Azure.Messaging.ServiceBus;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus.Messages
{
    /// <summary>
    /// Factory for creating Azure Service Bus messages from Orleans stream events and Service Bus messages.
    /// Handles serialization, batching, and metadata management.
    /// </summary>
    public class AzureServiceBusMessageFactory
    {
        private readonly Serializer _serializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusMessageFactory"/> class.
        /// </summary>
        /// <param name="serializer">The Orleans serializer.</param>
        public AzureServiceBusMessageFactory(Serializer serializer)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        /// <summary>
        /// Creates an Azure Service Bus message from an Orleans stream event.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="eventData">The event data to serialize.</param>
        /// <param name="requestContext">The current request context.</param>
        /// <returns>An Azure Service Bus message ready for publishing.</returns>
        public AzureServiceBusMessage CreateFromStreamEvent(
            StreamId streamId,
            object eventData,
            Dictionary<string, object>? requestContext = null)
        {
            if (eventData == null)
                throw new ArgumentNullException(nameof(eventData));

            // Serialize the event data
            var payload = _serializer.SerializeToArray(eventData);

            // Create sequence token - we'll use timestamp as sequence number for now
            var sequenceToken = new AzureServiceBusSequenceToken(
                DateTimeOffset.UtcNow.Ticks, 
                0, 
                1);

            // Create metadata with current timestamp
            var metadata = new ServiceBusMessageMetadata(
                messageId: Guid.NewGuid().ToString(),
                enqueuedTime: DateTimeOffset.UtcNow);

            return new AzureServiceBusMessage(
                streamId,
                sequenceToken,
                payload,
                requestContext,
                metadata);
        }

        /// <summary>
        /// Creates an Azure Service Bus message from a received Service Bus message.
        /// </summary>
        /// <param name="serviceBusMessage">The received Service Bus message.</param>
        /// <param name="streamId">The target stream identifier.</param>
        /// <returns>An Orleans-compatible message container.</returns>
        public AzureServiceBusMessage CreateFromServiceBusMessage(
            ServiceBusReceivedMessage serviceBusMessage,
            StreamId streamId)
        {
            if (serviceBusMessage == null)
                throw new ArgumentNullException(nameof(serviceBusMessage));

            // Create sequence token from Service Bus sequence number
            var sequenceToken = new AzureServiceBusSequenceToken(
                serviceBusMessage.SequenceNumber,
                0,
                serviceBusMessage.DeliveryCount);

            // Extract metadata from Service Bus message
            var metadata = new ServiceBusMessageMetadata(
                messageId: serviceBusMessage.MessageId,
                enqueuedTime: serviceBusMessage.EnqueuedTime,
                correlationId: serviceBusMessage.CorrelationId,
                sessionId: serviceBusMessage.SessionId,
                replyTo: serviceBusMessage.ReplyTo,
                subject: serviceBusMessage.Subject,
                applicationProperties: serviceBusMessage.ApplicationProperties?.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                partitionKey: serviceBusMessage.PartitionKey,
                timeToLive: serviceBusMessage.TimeToLive);

            // Use the message body as payload
            var payload = serviceBusMessage.Body.ToMemory();

            return new AzureServiceBusMessage(
                streamId,
                sequenceToken,
                payload,
                metadata: metadata);
        }

        /// <summary>
        /// Creates a Service Bus message for publishing from an Orleans stream event.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="eventData">The event data.</param>
        /// <param name="requestContext">The request context.</param>
        /// <param name="applicationProperties">Additional application properties.</param>
        /// <returns>A Service Bus message ready for publishing.</returns>
        public ServiceBusMessage CreateServiceBusMessage(
            StreamId streamId,
            object eventData,
            Dictionary<string, object>? requestContext = null,
            Dictionary<string, object>? applicationProperties = null)
        {
            if (eventData == null)
                throw new ArgumentNullException(nameof(eventData));

            // Serialize the event data
            var payload = _serializer.SerializeToArray(eventData);
            
            // Create the Service Bus message
            var serviceBusMessage = new ServiceBusMessage(payload)
            {
                MessageId = Guid.NewGuid().ToString(),
                Subject = eventData.GetType().Name
            };

            // Add stream information to application properties
            serviceBusMessage.ApplicationProperties["StreamId"] = streamId.ToString();
            serviceBusMessage.ApplicationProperties["StreamNamespace"] = streamId.Namespace;

            // Add request context if provided
            if (requestContext != null)
            {
                serviceBusMessage.ApplicationProperties["RequestContext"] = 
                    Convert.ToBase64String(_serializer.SerializeToArray(requestContext));
            }

            // Add additional application properties
            if (applicationProperties != null)
            {
                foreach (var kvp in applicationProperties)
                {
                    serviceBusMessage.ApplicationProperties[kvp.Key] = kvp.Value;
                }
            }

            return serviceBusMessage;
        }

        /// <summary>
        /// Creates a batch of Azure Service Bus messages from multiple stream events.
        /// </summary>
        /// <param name="streamEvents">The stream events to batch.</param>
        /// <param name="batchId">The batch identifier.</param>
        /// <returns>A batch container with all the events.</returns>
        public AzureServiceBusBatchContainer CreateBatch(
            IEnumerable<(StreamId StreamId, object EventData, Dictionary<string, object>? RequestContext)> streamEvents,
            string? batchId = null)
        {
            var eventList = streamEvents?.ToList() ?? throw new ArgumentNullException(nameof(streamEvents));
            
            if (eventList.Count == 0)
            {
                throw new ArgumentException("Batch must contain at least one event.", nameof(streamEvents));
            }

            batchId ??= Guid.NewGuid().ToString();

            // Create individual messages
            var messages = eventList.Select(evt => 
                CreateFromStreamEvent(evt.StreamId, evt.EventData, evt.RequestContext)).ToList();

            return AzureServiceBusBatchContainer.CreateBatch(messages, batchId);
        }

        /// <summary>
        /// Validates that a message meets size and content requirements.
        /// </summary>
        /// <param name="message">The message to validate.</param>
        /// <param name="maxMessageSize">The maximum allowed message size in bytes.</param>
        /// <exception cref="ArgumentException">Thrown if the message is invalid.</exception>
        public void ValidateMessage(AzureServiceBusMessage message, long maxMessageSize = 256 * 1024)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            if (message.Payload.Length > maxMessageSize)
            {
                throw new ArgumentException(
                    $"Message payload size ({message.Payload.Length} bytes) exceeds maximum allowed size ({maxMessageSize} bytes).",
                    nameof(message));
            }

            if (string.IsNullOrEmpty(message.Metadata.MessageId))
            {
                throw new ArgumentException("Message must have a valid MessageId.", nameof(message));
            }

            if (message.SequenceToken == null)
            {
                throw new ArgumentException("Message must have a valid SequenceToken.", nameof(message));
            }
        }

        /// <summary>
        /// Extracts request context from a Service Bus message's application properties.
        /// </summary>
        /// <param name="serviceBusMessage">The Service Bus message.</param>
        /// <returns>The extracted request context, or null if not present.</returns>
        public Dictionary<string, object>? ExtractRequestContext(ServiceBusReceivedMessage serviceBusMessage)
        {
            if (serviceBusMessage?.ApplicationProperties?.TryGetValue("RequestContext", out var contextValue) == true
                && contextValue is string contextString)
            {
                try
                {
                    var contextBytes = Convert.FromBase64String(contextString);
                    return _serializer.Deserialize<Dictionary<string, object>>(contextBytes);
                }
                catch
                {
                    // If deserialization fails, return null
                    return null;
                }
            }

            return null;
        }

        /// <summary>
        /// Extracts the stream identifier from a Service Bus message's application properties.
        /// </summary>
        /// <param name="serviceBusMessage">The Service Bus message.</param>
        /// <param name="defaultStreamId">The default stream ID to use if not found in message.</param>
        /// <returns>The extracted stream identifier.</returns>
        public StreamId ExtractStreamId(ServiceBusReceivedMessage serviceBusMessage, StreamId? defaultStreamId = null)
        {
            if (serviceBusMessage?.ApplicationProperties?.TryGetValue("StreamId", out var streamIdValue) == true
                && streamIdValue is string streamIdString)
            {
                if (StreamId.TryParse(streamIdString, out var streamId))
                {
                    return streamId;
                }
            }

            return defaultStreamId ?? throw new ArgumentException(
                "Could not extract StreamId from message and no default provided.", 
                nameof(serviceBusMessage));
        }
    }
}