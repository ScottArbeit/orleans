using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus.Messages
{
    /// <summary>
    /// Batch container that delivers payload and stream position information for Azure Service Bus messages.
    /// Implements IBatchContainer to provide Orleans streaming compatibility.
    /// </summary>
    [GenerateSerializer, Immutable]
    public sealed class AzureServiceBusMessage : IBatchContainer
    {
        /// <summary>
        /// Gets the stream identifier for the stream this message is part of.
        /// </summary>
        [Id(0)]
        public StreamId StreamId { get; init; }

        /// <summary>
        /// Gets the stream sequence token for this message.
        /// </summary>
        [Id(1)]
        public StreamSequenceToken SequenceToken { get; init; }

        /// <summary>
        /// Gets the serialized payload data.
        /// </summary>
        [Id(2)]
        public ReadOnlyMemory<byte> Payload { get; init; }

        /// <summary>
        /// Gets the request context data for Orleans context propagation.
        /// </summary>
        [Id(3)]
        public Dictionary<string, object> RequestContext { get; init; }

        /// <summary>
        /// Gets the Service Bus specific metadata.
        /// </summary>
        [Id(4)]
        public ServiceBusMessageMetadata Metadata { get; init; }

        /// <summary>
        /// Gets the batch identifier for batch operations.
        /// </summary>
        [Id(5)]
        public string? BatchId { get; init; }

        /// <summary>
        /// Gets the position within a batch for batch operations.
        /// </summary>
        [Id(6)]
        public int BatchPosition { get; init; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusMessage"/> class.
        /// </summary>
        public AzureServiceBusMessage()
        {
            RequestContext = new Dictionary<string, object>();
            Metadata = new ServiceBusMessageMetadata();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusMessage"/> class.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="sequenceToken">The sequence token.</param>
        /// <param name="payload">The serialized payload.</param>
        /// <param name="requestContext">The request context.</param>
        /// <param name="metadata">The Service Bus metadata.</param>
        /// <param name="batchId">The batch identifier.</param>
        /// <param name="batchPosition">The position within the batch.</param>
        public AzureServiceBusMessage(
            StreamId streamId,
            StreamSequenceToken sequenceToken,
            ReadOnlyMemory<byte> payload,
            Dictionary<string, object>? requestContext = null,
            ServiceBusMessageMetadata? metadata = null,
            string? batchId = null,
            int batchPosition = 0)
        {
            StreamId = streamId;
            SequenceToken = sequenceToken ?? throw new ArgumentNullException(nameof(sequenceToken));
            Payload = payload;
            RequestContext = requestContext ?? new Dictionary<string, object>();
            Metadata = metadata ?? new ServiceBusMessageMetadata();
            BatchId = batchId;
            BatchPosition = batchPosition;
        }

        /// <inheritdoc/>
        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            // For single messages, we yield one event
            // The payload will be deserialized by the consumer
            if (Payload.IsEmpty)
                yield break;

            // Create a sequence token for this specific event
            var eventToken = SequenceToken;
            if (BatchPosition > 0 && SequenceToken is AzureServiceBusSequenceToken serviceBusToken)
            {
                eventToken = new AzureServiceBusSequenceToken(
                    serviceBusToken.ServiceBusSequenceNumber,
                    BatchPosition,
                    serviceBusToken.DeliveryCount);
            }

            // We return the payload bytes as object, letting Orleans handle deserialization
            yield return new Tuple<T, StreamSequenceToken>((T)(object)Payload.ToArray(), eventToken);
        }

        /// <inheritdoc/>
        public bool ImportRequestContext()
        {
            if (RequestContext.Count == 0)
                return false;

            RequestContextExtensions.Import(RequestContext);
            return true;
        }

        /// <summary>
        /// Creates a new message with updated batch information.
        /// </summary>
        /// <param name="batchId">The new batch identifier.</param>
        /// <param name="batchPosition">The new batch position.</param>
        /// <returns>A new message with updated batch information.</returns>
        public AzureServiceBusMessage WithBatchInfo(string batchId, int batchPosition)
        {
            return new AzureServiceBusMessage(
                StreamId,
                SequenceToken,
                Payload,
                RequestContext,
                Metadata,
                batchId,
                batchPosition);
        }

        /// <summary>
        /// Creates a new message with updated sequence token.
        /// </summary>
        /// <param name="sequenceToken">The new sequence token.</param>
        /// <returns>A new message with updated sequence token.</returns>
        public AzureServiceBusMessage WithSequenceToken(StreamSequenceToken sequenceToken)
        {
            return new AzureServiceBusMessage(
                StreamId,
                sequenceToken,
                Payload,
                RequestContext,
                Metadata,
                BatchId,
                BatchPosition);
        }
    }
}