#nullable enable
using System;
using System.Globalization;
using Newtonsoft.Json;
using Orleans.Streams;

namespace Orleans.Providers.Streams.AzureServiceBus
{
    /// <summary>
    /// Azure Service Bus sequence token that tracks Service Bus sequence number and event index for ordering
    /// </summary>
    [Serializable]
    [GenerateSerializer]
    public class AzureServiceBusSequenceTokenV2 : StreamSequenceToken
    {
        /// <summary>
        /// Gets the Service Bus sequence number for ordering
        /// </summary>
        [Id(0)]
        [JsonProperty]
        public override long SequenceNumber { get; protected set; }

        /// <summary>
        /// Gets the event index within the batch for secondary ordering
        /// </summary>
        [Id(1)]
        [JsonProperty]
        public override int EventIndex { get; protected set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusSequenceTokenV2"/> class.
        /// </summary>
        /// <remarks>
        /// This constructor is exposed for serializer use only.
        /// </remarks>
        public AzureServiceBusSequenceTokenV2()
        {
            SequenceNumber = 0;
            EventIndex = 0;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusSequenceTokenV2"/> class.
        /// </summary>
        /// <param name="sequenceNumber">The Service Bus sequence number.</param>
        public AzureServiceBusSequenceTokenV2(long sequenceNumber)
        {
            SequenceNumber = sequenceNumber;
            EventIndex = 0;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusSequenceTokenV2"/> class.
        /// </summary>
        /// <param name="sequenceNumber">The Service Bus sequence number.</param>
        /// <param name="eventIndex">The event index within the batch.</param>
        public AzureServiceBusSequenceTokenV2(long sequenceNumber, int eventIndex)
        {
            SequenceNumber = sequenceNumber;
            EventIndex = eventIndex;
        }

        /// <summary>
        /// Creates a sequence token for a specific event in the current batch
        /// </summary>
        /// <param name="eventIndex">The event index.</param>
        /// <returns>A new sequence token.</returns>
        public AzureServiceBusSequenceTokenV2 CreateSequenceTokenForEvent(int eventIndex)
        {
            return new AzureServiceBusSequenceTokenV2(SequenceNumber, eventIndex);
        }

        /// <inheritdoc/>
        public override int CompareTo(StreamSequenceToken? other)
        {
            if (other == null)
                return 1;

            var token = other as AzureServiceBusSequenceTokenV2;
            if (token == null)
                throw new ArgumentOutOfRangeException(nameof(other));

            int difference = SequenceNumber.CompareTo(token.SequenceNumber);
            return difference != 0 ? difference : EventIndex.CompareTo(token.EventIndex);
        }

        /// <inheritdoc/>
        public override bool Equals(StreamSequenceToken? other)
        {
            var token = other as AzureServiceBusSequenceTokenV2;
            return token != null && (token.SequenceNumber == SequenceNumber &&
                                     token.EventIndex == EventIndex);
        }

        /// <inheritdoc/>
        public override bool Equals(object? obj)
        {
            return Equals(obj as AzureServiceBusSequenceTokenV2);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return HashCode.Combine(SequenceNumber, EventIndex);
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"[{SequenceNumber}:{EventIndex}]";
        }
    }
}