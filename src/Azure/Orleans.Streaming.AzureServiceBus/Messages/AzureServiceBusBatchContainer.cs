using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Messages
{
    /// <summary>
    /// Batch container that holds multiple Azure Service Bus messages for efficient processing.
    /// Implements IBatchContainerBatch for Orleans batch streaming operations.
    /// </summary>
    [GenerateSerializer, Immutable]
    public sealed class AzureServiceBusBatchContainer : IBatchContainerBatch
    {
        /// <summary>
        /// Gets the stream identifier for the stream this batch is part of.
        /// </summary>
        [Id(0)]
        public StreamId StreamId { get; init; }

        /// <summary>
        /// Gets the stream sequence token for the start of this batch.
        /// </summary>
        [Id(1)]
        public StreamSequenceToken SequenceToken { get; init; }

        /// <summary>
        /// Gets the batch containers comprising this batch.
        /// </summary>
        [Id(2)]
        public List<IBatchContainer> BatchContainers { get; init; }

        /// <summary>
        /// Gets the batch identifier.
        /// </summary>
        [Id(3)]
        public string BatchId { get; init; }

        /// <summary>
        /// Gets the request context data for Orleans context propagation.
        /// </summary>
        [Id(4)]
        public Dictionary<string, object> RequestContext { get; init; }

        /// <summary>
        /// Gets the time when this batch was created.
        /// </summary>
        [Id(5)]
        public DateTimeOffset BatchCreatedTime { get; init; }

        /// <summary>
        /// Gets the total number of events in this batch.
        /// </summary>
        public int EventCount => BatchContainers.Sum(container => 
            container.GetEvents<object>().Count());

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusBatchContainer"/> class.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <param name="sequenceToken">The sequence token for the batch start.</param>
        /// <param name="batchContainers">The containers in this batch.</param>
        /// <param name="batchId">The batch identifier.</param>
        /// <param name="requestContext">The request context.</param>
        public AzureServiceBusBatchContainer(
            StreamId streamId,
            StreamSequenceToken sequenceToken,
            IEnumerable<IBatchContainer> batchContainers,
            string batchId,
            Dictionary<string, object>? requestContext = null)
        {
            StreamId = streamId;
            SequenceToken = sequenceToken ?? throw new ArgumentNullException(nameof(sequenceToken));
            BatchContainers = batchContainers?.ToList() ?? throw new ArgumentNullException(nameof(batchContainers));
            BatchId = batchId ?? throw new ArgumentNullException(nameof(batchId));
            RequestContext = requestContext ?? new Dictionary<string, object>();
            BatchCreatedTime = DateTimeOffset.UtcNow;

            // Validate that all containers belong to the same stream
            if (BatchContainers.Any(container => !container.StreamId.Equals(streamId)))
            {
                throw new ArgumentException("All batch containers must belong to the same stream.", nameof(batchContainers));
            }
        }

        /// <inheritdoc/>
        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            // Aggregate events from all containers in the batch
            foreach (var container in BatchContainers)
            {
                foreach (var eventTuple in container.GetEvents<T>())
                {
                    yield return eventTuple;
                }
            }
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
        /// Adds a message to this batch.
        /// </summary>
        /// <param name="message">The message to add.</param>
        /// <returns>A new batch container with the added message.</returns>
        public AzureServiceBusBatchContainer AddMessage(AzureServiceBusMessage message)
        {
            if (!message.StreamId.Equals(StreamId))
            {
                throw new ArgumentException("Message must belong to the same stream as the batch.", nameof(message));
            }

            var newContainers = new List<IBatchContainer>(BatchContainers) { message };
            
            return new AzureServiceBusBatchContainer(
                StreamId,
                SequenceToken,
                newContainers,
                BatchId,
                RequestContext);
        }

        /// <summary>
        /// Creates a new batch from a collection of individual messages.
        /// </summary>
        /// <param name="messages">The messages to include in the batch.</param>
        /// <param name="batchId">The batch identifier.</param>
        /// <returns>A new batch container.</returns>
        public static AzureServiceBusBatchContainer CreateBatch(
            IEnumerable<AzureServiceBusMessage> messages,
            string batchId)
        {
            var messageList = messages?.ToList() ?? throw new ArgumentNullException(nameof(messages));
            
            if (messageList.Count == 0)
            {
                throw new ArgumentException("Batch must contain at least one message.", nameof(messages));
            }

            // All messages should belong to the same stream
            var streamId = messageList[0].StreamId;
            if (messageList.Any(msg => !msg.StreamId.Equals(streamId)))
            {
                throw new ArgumentException("All messages in batch must belong to the same stream.", nameof(messages));
            }

            // Use the earliest sequence token as the batch token
            var earliestToken = messageList
                .Select(msg => msg.SequenceToken)
                .Where(token => token != null)
                .Min() ?? messageList[0].SequenceToken;

            // Update messages with batch information
            var batchMessages = messageList
                .Select((msg, index) => msg.WithBatchInfo(batchId, index))
                .Cast<IBatchContainer>()
                .ToList();

            return new AzureServiceBusBatchContainer(
                streamId,
                earliestToken,
                batchMessages,
                batchId);
        }

        /// <summary>
        /// Splits this batch into smaller batches of the specified size.
        /// </summary>
        /// <param name="maxBatchSize">The maximum number of containers per batch.</param>
        /// <returns>An enumerable of smaller batch containers.</returns>
        public IEnumerable<AzureServiceBusBatchContainer> Split(int maxBatchSize)
        {
            if (maxBatchSize <= 0)
            {
                throw new ArgumentException("Batch size must be positive.", nameof(maxBatchSize));
            }

            var batchNumber = 0;
            for (int i = 0; i < BatchContainers.Count; i += maxBatchSize)
            {
                var subBatch = BatchContainers.Skip(i).Take(maxBatchSize);
                var subBatchId = $"{BatchId}_part_{batchNumber++}";
                
                yield return new AzureServiceBusBatchContainer(
                    StreamId,
                    SequenceToken,
                    subBatch,
                    subBatchId,
                    RequestContext);
            }
        }
    }
}