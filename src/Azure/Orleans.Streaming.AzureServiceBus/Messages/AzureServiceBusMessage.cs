using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

#nullable enable

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
        /// Gets the deserialized event objects.
        /// </summary>
        [Id(2)]
        public List<object> Events { get; init; }

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
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="sequenceToken">The sequence token.</param>
        /// <param name="events">The event objects.</param>
        /// <param name="requestContext">The request context.</param>
        /// <param name="metadata">The Service Bus metadata.</param>
        /// <param name="batchId">The batch identifier.</param>
        /// <param name="batchPosition">The position within the batch.</param>
        public AzureServiceBusMessage(
            StreamId streamId,
            StreamSequenceToken sequenceToken,
            IEnumerable<object>? events = null,
            Dictionary<string, object>? requestContext = null,
            ServiceBusMessageMetadata? metadata = null,
            string? batchId = null,
            int batchPosition = 0)
        {
            StreamId = streamId;
            SequenceToken = sequenceToken ?? throw new ArgumentNullException(nameof(sequenceToken));
            Events = events?.ToList() ?? new List<object>();
            RequestContext = requestContext ?? new Dictionary<string, object>();
            Metadata = metadata ?? new ServiceBusMessageMetadata();
            BatchId = batchId;
            BatchPosition = batchPosition;
        }

        /// <inheritdoc/>
        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            // Return events cast to the requested type with proper sequence tokens
            return Events.OfType<T>().Select((evt, index) =>
            {
                var eventToken = SequenceToken;
                if (BatchPosition > 0 && SequenceToken is EventSequenceTokenV2 sequenceTokenV2)
                {
                    eventToken = new EventSequenceTokenV2(
                        sequenceTokenV2.SequenceNumber,
                        BatchPosition + index);
                }
                else if (index > 0 && SequenceToken is EventSequenceTokenV2 tokenV2)
                {
                    eventToken = new EventSequenceTokenV2(
                        tokenV2.SequenceNumber,
                        index);
                }

                return Tuple.Create<T, StreamSequenceToken>(evt, eventToken);
            });
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
                Events,
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
                Events,
                RequestContext,
                Metadata,
                BatchId,
                BatchPosition);
        }
    }
}