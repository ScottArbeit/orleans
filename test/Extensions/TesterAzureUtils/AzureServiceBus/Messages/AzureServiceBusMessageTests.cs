using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus.Messages;
using Xunit;

#nullable enable

namespace TesterAzureUtils.AzureServiceBus.Messages
{
    /// <summary>
    /// Tests for Azure Service Bus message serialization and deserialization.
    /// </summary>
    [Trait("Category", "AzureServiceBus")]
    public class AzureServiceBusMessageTests
    {
        private readonly Serializer _serializer;

        public AzureServiceBusMessageTests()
        {
            var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
            services.AddSerializer();
            var serviceProvider = services.BuildServiceProvider();
            _serializer = serviceProvider.GetRequiredService<Serializer>();
        }

        [Fact]
        public void MessageSerializationRoundTrip()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var sequenceToken = new EventSequenceTokenV2(12345, 0);
            var events = new List<object> { "test-payload", 42, new { Property = "value" } };
            var requestContext = new Dictionary<string, object> { { "key1", "value1" }, { "key2", 42 } };
            var metadata = new ServiceBusMessageMetadata(
                "test-message-id",
                DateTimeOffset.UtcNow,
                "correlation-id",
                "session-id");

            var originalMessage = new AzureServiceBusMessage(
                streamId,
                sequenceToken,
                events,
                requestContext,
                metadata,
                "batch-id",
                5);

            // Act
            var serialized = _serializer.SerializeToArray(originalMessage);
            var deserializedMessage = _serializer.Deserialize<AzureServiceBusMessage>(serialized);

            // Assert
            Assert.Equal(originalMessage.StreamId, deserializedMessage.StreamId);
            Assert.Equal(originalMessage.SequenceToken, deserializedMessage.SequenceToken);
            Assert.Equal(originalMessage.Events.Count, deserializedMessage.Events.Count);
            Assert.Equal(originalMessage.Events.First().ToString(), deserializedMessage.Events.First().ToString());
            Assert.Equal(originalMessage.RequestContext.Count, deserializedMessage.RequestContext.Count);
            Assert.Equal(originalMessage.Metadata.MessageId, deserializedMessage.Metadata.MessageId);
            Assert.Equal(originalMessage.BatchId, deserializedMessage.BatchId);
            Assert.Equal(originalMessage.BatchPosition, deserializedMessage.BatchPosition);
        }

        [Fact]
        public void BatchContainerImplementation()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var sequenceToken = new EventSequenceTokenV2(12345, 0);
            var testData = "test-event";
            var events = new List<object> { testData };
            var message = new AzureServiceBusMessage(streamId, sequenceToken, events);

            // Act
            var retrievedEvents = message.GetEvents<string>();
            var importResult = message.ImportRequestContext();

            // Assert
            Assert.Single(retrievedEvents);
            Assert.False(importResult); // No request context to import
            
            var eventTuple = Assert.Single(retrievedEvents);
            Assert.Equal(testData, eventTuple.Item1);
            Assert.Equal(sequenceToken, eventTuple.Item2);
        }

        [Fact]
        public void MessageWithRequestContextImportsCorrectly()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var sequenceToken = new EventSequenceTokenV2(12345, 0);
            var events = new List<object> { "test-payload" };
            var requestContext = new Dictionary<string, object>
            {
                { "ActivityId", Guid.NewGuid().ToString() },
                { "UserId", "test-user" }
            };

            var message = new AzureServiceBusMessage(
                streamId,
                sequenceToken,
                events,
                requestContext);

            // Act
            var importResult = message.ImportRequestContext();

            // Assert
            Assert.True(importResult);
        }

        [Fact]
        public void EmptyMessageHandling()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var sequenceToken = new EventSequenceTokenV2(12345, 0);
            var emptyEvents = new List<object>();

            var message = new AzureServiceBusMessage(streamId, sequenceToken, emptyEvents);

            // Act
            var events = message.GetEvents<object>();

            // Assert
            Assert.Empty(events);
        }

        [Fact]
        public void WithBatchInfoCreatesNewInstance()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var sequenceToken = new EventSequenceTokenV2(12345, 0);
            var events = new List<object> { "test-payload" };
            var originalMessage = new AzureServiceBusMessage(streamId, sequenceToken, events);

            // Act
            var updatedMessage = originalMessage.WithBatchInfo("new-batch-id", 10);

            // Assert
            Assert.NotSame(originalMessage, updatedMessage);
            Assert.Equal("new-batch-id", updatedMessage.BatchId);
            Assert.Equal(10, updatedMessage.BatchPosition);
            Assert.Equal(originalMessage.StreamId, updatedMessage.StreamId);
            Assert.Equal(originalMessage.Events.Count, updatedMessage.Events.Count);
            Assert.Equal(originalMessage.Events.First().ToString(), updatedMessage.Events.First().ToString());
        }

        [Fact]
        public void WithSequenceTokenCreatesNewInstance()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var originalToken = new EventSequenceTokenV2(12345, 0);
            var newToken = new EventSequenceTokenV2(67890, 1);
            var events = new List<object> { "test-payload" };
            var originalMessage = new AzureServiceBusMessage(streamId, originalToken, events);

            // Act
            var updatedMessage = originalMessage.WithSequenceToken(newToken);

            // Assert
            Assert.NotSame(originalMessage, updatedMessage);
            Assert.Equal(newToken, updatedMessage.SequenceToken);
            Assert.Equal(originalMessage.StreamId, updatedMessage.StreamId);
            Assert.Equal(originalMessage.Events.Count, updatedMessage.Events.Count);
            Assert.Equal(originalMessage.Events.First().ToString(), updatedMessage.Events.First().ToString());
        }

        [Fact]
        public void ServiceBusMessageMetadataRoundTrip()
        {
            // Arrange
            var metadata = new ServiceBusMessageMetadata(
                "message-123",
                DateTimeOffset.Parse("2024-01-15T10:30:00Z"),
                "correlation-456",
                "session-789",
                "reply-to-queue",
                "test-subject",
                new Dictionary<string, object> { { "prop1", "value1" }, { "prop2", 123 } },
                "partition-key",
                TimeSpan.FromMinutes(30));

            // Act
            var serialized = _serializer.SerializeToArray(metadata);
            var deserialized = _serializer.Deserialize<ServiceBusMessageMetadata>(serialized);

            // Assert
            Assert.Equal(metadata.MessageId, deserialized.MessageId);
            Assert.Equal(metadata.EnqueuedTime, deserialized.EnqueuedTime);
            Assert.Equal(metadata.CorrelationId, deserialized.CorrelationId);
            Assert.Equal(metadata.SessionId, deserialized.SessionId);
            Assert.Equal(metadata.ReplyTo, deserialized.ReplyTo);
            Assert.Equal(metadata.Subject, deserialized.Subject);
            Assert.Equal(metadata.PartitionKey, deserialized.PartitionKey);
            Assert.Equal(metadata.TimeToLive, deserialized.TimeToLive);
            Assert.Equal(metadata.ApplicationProperties.Count, deserialized.ApplicationProperties.Count);
        }

        [Fact]
        public void EventSequenceTokenComparison()
        {
            // Arrange
            var token1 = new EventSequenceTokenV2(100, 0);
            var token2 = new EventSequenceTokenV2(200, 0);
            var token3 = new EventSequenceTokenV2(100, 1);
            var token4 = new EventSequenceTokenV2(100, 0);

            // Act & Assert
            Assert.True(token1.CompareTo(token2) < 0);
            Assert.True(token2.CompareTo(token1) > 0);
            Assert.True(token1.CompareTo(token3) < 0);
            Assert.True(token1.CompareTo(token4) == 0);
            Assert.True(token1.Equals(token4));
            Assert.False(token1.Equals(token2));
        }

        [Fact]
        public void SequenceTokenSerialization()
        {
            // Arrange
            var token = new EventSequenceTokenV2(12345, 2);

            // Act
            var serialized = _serializer.SerializeToArray(token);
            var deserialized = _serializer.Deserialize<EventSequenceTokenV2>(serialized);

            // Assert
            Assert.Equal(token.SequenceNumber, deserialized.SequenceNumber);
            Assert.Equal(token.EventIndex, deserialized.EventIndex);
        }
    }
}