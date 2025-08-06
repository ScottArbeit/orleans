using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus.Messages;
using Xunit;

namespace TesterAzureUtils.AzureServiceBus.Messages
{
    /// <summary>
    /// Tests for Azure Service Bus batch container functionality.
    /// </summary>
    [Trait("Category", "AzureServiceBus")]
    public class AzureServiceBusBatchContainerTests
    {
        private readonly Serializer _serializer;

        public AzureServiceBusBatchContainerTests()
        {
            var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
            services.AddSerializer();
            var serviceProvider = services.BuildServiceProvider();
            _serializer = serviceProvider.GetRequiredService<Serializer>();
        }

        [Fact]
        public void CreateBatchFromMessages()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var messages = new[]
            {
                CreateTestMessage(streamId, 100, "event1"),
                CreateTestMessage(streamId, 101, "event2"),
                CreateTestMessage(streamId, 102, "event3")
            };

            // Act
            var batch = AzureServiceBusBatchContainer.CreateBatch(messages, "test-batch");

            // Assert
            Assert.Equal(streamId, batch.StreamId);
            Assert.Equal("test-batch", batch.BatchId);
            Assert.Equal(3, batch.BatchContainers.Count);
            Assert.Equal(3, batch.EventCount);
            
            // Check that batch IDs were assigned correctly
            for (int i = 0; i < messages.Length; i++)
            {
                var batchedMessage = batch.BatchContainers[i] as AzureServiceBusMessage;
                Assert.NotNull(batchedMessage);
                Assert.Equal("test-batch", batchedMessage.BatchId);
                Assert.Equal(i, batchedMessage.BatchPosition);
            }
        }

        [Fact]
        public void BatchSerializationRoundTrip()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var messages = new[]
            {
                CreateTestMessage(streamId, 100, "event1"),
                CreateTestMessage(streamId, 101, "event2")
            };
            var originalBatch = AzureServiceBusBatchContainer.CreateBatch(messages, "test-batch");

            // Act
            var serialized = _serializer.SerializeToArray(originalBatch);
            var deserializedBatch = _serializer.Deserialize<AzureServiceBusBatchContainer>(serialized);

            // Assert
            Assert.Equal(originalBatch.StreamId, deserializedBatch.StreamId);
            Assert.Equal(originalBatch.BatchId, deserializedBatch.BatchId);
            Assert.Equal(originalBatch.BatchContainers.Count, deserializedBatch.BatchContainers.Count);
            Assert.Equal(originalBatch.EventCount, deserializedBatch.EventCount);
        }

        [Fact]
        public void GetEventsFromBatch()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var messages = new[]
            {
                CreateTestMessage(streamId, 100, "event1"),
                CreateTestMessage(streamId, 101, "event2"),
                CreateTestMessage(streamId, 102, "event3")
            };
            var batch = AzureServiceBusBatchContainer.CreateBatch(messages, "test-batch");

            // Act
            var events = batch.GetEvents<byte[]>().ToList();

            // Assert
            Assert.Equal(3, events.Count);
            
            // Events should be in order
            for (int i = 0; i < events.Count; i++)
            {
                var expectedPayload = _serializer.SerializeToArray($"event{i + 1}");
                Assert.Equal(expectedPayload, events[i].Item1);
            }
        }

        [Fact]
        public void AddMessageToBatch()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var initialMessages = new[]
            {
                CreateTestMessage(streamId, 100, "event1"),
                CreateTestMessage(streamId, 101, "event2")
            };
            var batch = AzureServiceBusBatchContainer.CreateBatch(initialMessages, "test-batch");
            var newMessage = CreateTestMessage(streamId, 102, "event3");

            // Act
            var updatedBatch = batch.AddMessage(newMessage);

            // Assert
            Assert.NotSame(batch, updatedBatch);
            Assert.Equal(2, batch.BatchContainers.Count);
            Assert.Equal(3, updatedBatch.BatchContainers.Count);
            Assert.Equal(3, updatedBatch.EventCount);
        }

        [Fact]
        public void AddMessageWithDifferentStreamThrows()
        {
            // Arrange
            var streamId1 = StreamId.Create("test-namespace", "test-key1");
            var streamId2 = StreamId.Create("test-namespace", "test-key2");
            var initialMessage = CreateTestMessage(streamId1, 100, "event1");
            var batch = AzureServiceBusBatchContainer.CreateBatch(new[] { initialMessage }, "test-batch");
            var differentStreamMessage = CreateTestMessage(streamId2, 101, "event2");

            // Act & Assert
            Assert.Throws<ArgumentException>(() => batch.AddMessage(differentStreamMessage));
        }

        [Fact]
        public void SplitBatchIntoSmallerBatches()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var messages = Enumerable.Range(1, 7)
                .Select(i => CreateTestMessage(streamId, 100 + i, $"event{i}"))
                .ToArray();
            var batch = AzureServiceBusBatchContainer.CreateBatch(messages, "test-batch");

            // Act
            var splitBatches = batch.Split(3).ToList();

            // Assert
            Assert.Equal(3, splitBatches.Count);
            
            // First two batches should have 3 items
            Assert.Equal(3, splitBatches[0].BatchContainers.Count);
            Assert.Equal(3, splitBatches[1].BatchContainers.Count);
            
            // Last batch should have 1 item
            Assert.Equal(1, splitBatches[2].BatchContainers.Count);
            
            // Check batch IDs
            Assert.Equal("test-batch_part_0", splitBatches[0].BatchId);
            Assert.Equal("test-batch_part_1", splitBatches[1].BatchId);
            Assert.Equal("test-batch_part_2", splitBatches[2].BatchId);
        }

        [Fact]
        public void ImportRequestContextFromBatch()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var requestContext = new Dictionary<string, object>
            {
                { "BatchRequestId", Guid.NewGuid().ToString() }
            };
            var messages = new[]
            {
                CreateTestMessage(streamId, 100, "event1")
            };
            var batch = new AzureServiceBusBatchContainer(
                streamId,
                new EventSequenceTokenV2(100),
                messages,
                "test-batch",
                requestContext);

            // Act
            var importResult = batch.ImportRequestContext();

            // Assert
            Assert.True(importResult);
        }

        [Fact]
        public void CreateBatchValidatesStreamConsistency()
        {
            // Arrange
            var streamId1 = StreamId.Create("test-namespace", "test-key1");
            var streamId2 = StreamId.Create("test-namespace", "test-key2");
            var messages = new[]
            {
                CreateTestMessage(streamId1, 100, "event1"),
                CreateTestMessage(streamId2, 101, "event2") // Different stream
            };

            // Act & Assert
            Assert.Throws<ArgumentException>(() => 
                AzureServiceBusBatchContainer.CreateBatch(messages, "test-batch"));
        }

        [Fact]
        public void CreateBatchWithEmptyMessagesThrows()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => 
                AzureServiceBusBatchContainer.CreateBatch(Array.Empty<AzureServiceBusMessage>(), "test-batch"));
        }

        [Fact]
        public void CreateBatchWithNullMessagesThrows()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                AzureServiceBusBatchContainer.CreateBatch(null, "test-batch"));
        }

        [Fact]
        public void SplitWithInvalidBatchSizeThrows()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var message = CreateTestMessage(streamId, 100, "event1");
            var batch = AzureServiceBusBatchContainer.CreateBatch(new[] { message }, "test-batch");

            // Act & Assert
            Assert.Throws<ArgumentException>(() => batch.Split(0).ToList());
            Assert.Throws<ArgumentException>(() => batch.Split(-1).ToList());
        }

        private AzureServiceBusMessage CreateTestMessage(StreamId streamId, long sequenceNumber, string eventData)
        {
            var sequenceToken = new EventSequenceTokenV2(sequenceNumber, 0);
            var payload = _serializer.SerializeToArray(eventData);
            var metadata = new ServiceBusMessageMetadata(
                Guid.NewGuid().ToString(),
                DateTimeOffset.UtcNow);

            return new AzureServiceBusMessage(
                streamId,
                sequenceToken,
                payload,
                metadata: metadata);
        }
    }
}