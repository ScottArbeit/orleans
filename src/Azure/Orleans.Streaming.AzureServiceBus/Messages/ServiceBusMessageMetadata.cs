using System;
using System.Collections.Generic;
using Orleans.Serialization;

namespace Orleans.Streaming.AzureServiceBus.Messages
{
    /// <summary>
    /// Metadata associated with an Azure Service Bus message, preserving service bus properties.
    /// </summary>
    [GenerateSerializer, Immutable]
    public sealed class ServiceBusMessageMetadata
    {
        /// <summary>
        /// Gets the unique message identifier.
        /// </summary>
        [Id(0)]
        public string MessageId { get; init; } = string.Empty;

        /// <summary>
        /// Gets the correlation identifier for request-reply patterns.
        /// </summary>
        [Id(1)]
        public string? CorrelationId { get; init; }

        /// <summary>
        /// Gets the session identifier for session-enabled queues/topics.
        /// </summary>
        [Id(2)]
        public string? SessionId { get; init; }

        /// <summary>
        /// Gets the time when the message was enqueued in Service Bus.
        /// </summary>
        [Id(3)]
        public DateTimeOffset EnqueuedTime { get; init; }

        /// <summary>
        /// Gets the reply-to address for request-reply patterns.
        /// </summary>
        [Id(4)]
        public string? ReplyTo { get; init; }

        /// <summary>
        /// Gets the subject/label of the message.
        /// </summary>
        [Id(5)]
        public string? Subject { get; init; }

        /// <summary>
        /// Gets the application-specific properties associated with the message.
        /// </summary>
        [Id(6)]
        public Dictionary<string, object> ApplicationProperties { get; init; } = new();

        /// <summary>
        /// Gets the partition key for partitioned entities.
        /// </summary>
        [Id(7)]
        public string? PartitionKey { get; init; }

        /// <summary>
        /// Gets the time-to-live value for the message.
        /// </summary>
        [Id(8)]
        public TimeSpan? TimeToLive { get; init; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceBusMessageMetadata"/> class.
        /// </summary>
        public ServiceBusMessageMetadata()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceBusMessageMetadata"/> class.
        /// </summary>
        /// <param name="messageId">The message identifier.</param>
        /// <param name="enqueuedTime">The time when the message was enqueued.</param>
        /// <param name="correlationId">The correlation identifier.</param>
        /// <param name="sessionId">The session identifier.</param>
        /// <param name="replyTo">The reply-to address.</param>
        /// <param name="subject">The message subject.</param>
        /// <param name="applicationProperties">The application properties.</param>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="timeToLive">The time-to-live value.</param>
        public ServiceBusMessageMetadata(
            string messageId,
            DateTimeOffset enqueuedTime,
            string? correlationId = null,
            string? sessionId = null,
            string? replyTo = null,
            string? subject = null,
            Dictionary<string, object>? applicationProperties = null,
            string? partitionKey = null,
            TimeSpan? timeToLive = null)
        {
            MessageId = messageId ?? throw new ArgumentNullException(nameof(messageId));
            EnqueuedTime = enqueuedTime;
            CorrelationId = correlationId;
            SessionId = sessionId;
            ReplyTo = replyTo;
            Subject = subject;
            ApplicationProperties = applicationProperties ?? new Dictionary<string, object>();
            PartitionKey = partitionKey;
            TimeToLive = timeToLive;
        }
    }
}