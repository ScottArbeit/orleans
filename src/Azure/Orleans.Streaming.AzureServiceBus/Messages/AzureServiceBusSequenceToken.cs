using System;
using System.Globalization;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus.Messages
{
    /// <summary>
    /// Sequence token for Azure Service Bus messages that tracks message position within the queue/topic.
    /// For Service Bus, we track the enqueue sequence number and event index for batch operations.
    /// </summary>
    [Serializable]
    [GenerateSerializer]
    public class AzureServiceBusSequenceToken : EventSequenceTokenV2
    {
        /// <summary>
        /// Gets the Service Bus message sequence number, which is unique per entity (queue/topic).
        /// </summary>
        [Id(2)]
        public long ServiceBusSequenceNumber { get; }

        /// <summary>
        /// Gets the delivery count for this message (how many times it has been delivered).
        /// </summary>
        [Id(3)]
        public int DeliveryCount { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusSequenceToken"/> class.
        /// </summary>
        /// <param name="serviceBusSequenceNumber">The Service Bus sequence number.</param>
        /// <param name="eventIndex">The event index within a batch.</param>
        /// <param name="deliveryCount">The delivery count.</param>
        public AzureServiceBusSequenceToken(long serviceBusSequenceNumber, int eventIndex = 0, int deliveryCount = 1)
            : base(serviceBusSequenceNumber, eventIndex)
        {
            ServiceBusSequenceNumber = serviceBusSequenceNumber;
            DeliveryCount = deliveryCount;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusSequenceToken"/> class.
        /// </summary>
        /// <remarks>
        /// This constructor is exposed for serializer use only.
        /// </remarks>
        public AzureServiceBusSequenceToken() : base()
        {
        }

        /// <inheritdoc/>
        public override bool Equals(StreamSequenceToken other)
        {
            if (other is not AzureServiceBusSequenceToken otherToken)
                return false;

            return ServiceBusSequenceNumber == otherToken.ServiceBusSequenceNumber
                && EventIndex == otherToken.EventIndex;
        }

        /// <inheritdoc/>
        public override int CompareTo(StreamSequenceToken other)
        {
            if (other is not AzureServiceBusSequenceToken otherToken)
                return 1;

            int result = ServiceBusSequenceNumber.CompareTo(otherToken.ServiceBusSequenceNumber);
            return result != 0 ? result : EventIndex.CompareTo(otherToken.EventIndex);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return HashCode.Combine(ServiceBusSequenceNumber, EventIndex);
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "[ServiceBusSequenceToken: ServiceBusSeq={0}, EventIndex={1}, DeliveryCount={2}]", 
                ServiceBusSequenceNumber, EventIndex, DeliveryCount);
        }
    }
}