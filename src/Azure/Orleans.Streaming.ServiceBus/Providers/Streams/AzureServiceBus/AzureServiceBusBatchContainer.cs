#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.AzureServiceBus
{
    /// <summary>
    /// Azure Service Bus batch container that holds events and provides stream processing capabilities
    /// </summary>
    [Serializable]
    [GenerateSerializer]
    public class AzureServiceBusBatchContainer : IBatchContainer
    {
        [JsonProperty]
        [Id(0)]
        public StreamId StreamId { get; set; }

        [JsonProperty]
        [Id(1)]
        public List<object> Events { get; set; } = new();

        [JsonProperty]
        [Id(2)]
        public Dictionary<string, object>? RequestContext { get; set; } = new();

        [JsonProperty]
        [Id(3)]
        private AzureServiceBusSequenceTokenV2 sequenceToken;

        /// <summary>
        /// Gets the stream sequence token for the start of this batch
        /// </summary>
        public StreamSequenceToken SequenceToken => sequenceToken;

        /// <summary>
        /// Sets the real sequence token for internal use
        /// </summary>
        internal AzureServiceBusSequenceTokenV2 RealSequenceToken
        {
            set { sequenceToken = value; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusBatchContainer"/> class.
        /// </summary>
        /// <remarks>
        /// This constructor is for serialization use only.
        /// </remarks>
        public AzureServiceBusBatchContainer()
        {
            StreamId = StreamId.Create("", "");
            Events = new List<object>();
            RequestContext = new Dictionary<string, object>();
            sequenceToken = new AzureServiceBusSequenceTokenV2();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusBatchContainer"/> class.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="events">The events in this batch.</param>
        /// <param name="requestContext">The request context.</param>
        public AzureServiceBusBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object> requestContext)
        {
            if (events == null) throw new ArgumentNullException(nameof(events), "Message contains no events");

            StreamId = streamId;
            Events = events;
            RequestContext = requestContext ?? new Dictionary<string, object>();
            sequenceToken = new AzureServiceBusSequenceTokenV2();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusBatchContainer"/> class.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="events">The events in this batch.</param>
        /// <param name="requestContext">The request context.</param>
        /// <param name="sequenceToken">The sequence token.</param>
        [JsonConstructor]
        public AzureServiceBusBatchContainer(
            StreamId streamId,
            List<object> events,
            Dictionary<string, object>? requestContext,
            AzureServiceBusSequenceTokenV2 sequenceToken)
        {
            if (events == null) throw new ArgumentNullException(nameof(events), "Message contains no events");

            StreamId = streamId;
            Events = events;
            RequestContext = requestContext ?? new Dictionary<string, object>();
            this.sequenceToken = sequenceToken;
        }

        /// <inheritdoc/>
        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return Events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, sequenceToken.CreateSequenceTokenForEvent(i)));
        }

        /// <inheritdoc/>
        public bool ImportRequestContext()
        {
            if (RequestContext != null && RequestContext.Count > 0)
            {
                RequestContextExtensions.Import(RequestContext);
                return true;
            }
            return false;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"[AzureServiceBusBatchContainer:Stream={StreamId},#Items={Events.Count}]";
        }
    }
}