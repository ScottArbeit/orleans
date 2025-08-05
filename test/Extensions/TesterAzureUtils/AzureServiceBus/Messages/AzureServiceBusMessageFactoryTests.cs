using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus.Messages;
using Xunit;

namespace TesterAzureUtils.AzureServiceBus.Messages
{
    /// <summary>
    /// Tests for Azure Service Bus message factory functionality.
    /// </summary>
    [Trait("Category", "AzureServiceBus")]
    public class AzureServiceBusMessageFactoryTests
    {
        private readonly Serializer _serializer;
        private readonly AzureServiceBusMessageFactory _factory;

        public AzureServiceBusMessageFactoryTests()
        {
            var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
            services.AddSerializer();
            var serviceProvider = services.BuildServiceProvider();
            _serializer = serviceProvider.GetRequiredService<Serializer>();
            _factory = new AzureServiceBusMessageFactory(_serializer);
        }

        [Fact]
        public void CreateFromStreamEventSuccess()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var eventData = new TestEvent { Id = 123, Name = "test-event" };
            var requestContext = new Dictionary<string, object>
            {
                { "UserId", "test-user" },
                { "RequestId", Guid.NewGuid().ToString() }
            };

            // Act
            var message = _factory.CreateFromStreamEvent(streamId, eventData, requestContext);

            // Assert
            Assert.Equal(streamId, message.StreamId);
            Assert.NotNull(message.SequenceToken);
            Assert.True(message.Payload.Length > 0);
            Assert.Equal(requestContext.Count, message.RequestContext.Count);
            Assert.NotNull(message.Metadata);
            Assert.NotEmpty(message.Metadata.MessageId);
        }

        [Fact]
        public void CreateFromStreamEventWithNullDataThrows()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                _factory.CreateFromStreamEvent(streamId, null));
        }

        [Fact]
        public void CreateServiceBusMessageSuccess()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var eventData = new TestEvent { Id = 456, Name = "service-bus-event" };
            var requestContext = new Dictionary<string, object> { { "TraceId", "trace-123" } };
            var appProperties = new Dictionary<string, object> { { "Version", "1.0" } };

            // Act
            var serviceBusMessage = _factory.CreateServiceBusMessage(
                streamId, 
                eventData, 
                requestContext, 
                appProperties);

            // Assert
            Assert.NotNull(serviceBusMessage);
            Assert.NotEmpty(serviceBusMessage.MessageId);
            Assert.Equal("TestEvent", serviceBusMessage.Subject);
            Assert.True(serviceBusMessage.ApplicationProperties.ContainsKey("StreamId"));
            Assert.True(serviceBusMessage.ApplicationProperties.ContainsKey("StreamNamespace"));
            Assert.True(serviceBusMessage.ApplicationProperties.ContainsKey("RequestContext"));
            Assert.True(serviceBusMessage.ApplicationProperties.ContainsKey("Version"));
            Assert.Equal("1.0", serviceBusMessage.ApplicationProperties["Version"]);
        }

        [Fact]
        public void CreateBatchFromStreamEventsSuccess()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new[]
            {
                (streamId, (object)new TestEvent { Id = 1, Name = "event1" }, (Dictionary<string, object>?)null),
                (streamId, (object)new TestEvent { Id = 2, Name = "event2" }, new Dictionary<string, object> { { "prop", "value" } }),
                (streamId, (object)new TestEvent { Id = 3, Name = "event3" }, (Dictionary<string, object>?)null)
            };

            // Act
            var batch = _factory.CreateBatch(events, "test-batch");

            // Assert
            Assert.Equal(streamId, batch.StreamId);
            Assert.Equal("test-batch", batch.BatchId);
            Assert.Equal(3, batch.BatchContainers.Count);
            Assert.Equal(3, batch.EventCount);
        }

        [Fact]
        public void CreateBatchWithEmptyEventsThrows()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => 
                _factory.CreateBatch(Array.Empty<(StreamId, object, Dictionary<string, object>?)>()));
        }

        [Fact]
        public void CreateBatchWithNullEventsThrows()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                _factory.CreateBatch(null));
        }

        [Fact]
        public void ValidateMessageSuccess()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var eventData = new TestEvent { Id = 123, Name = "test" };
            var message = _factory.CreateFromStreamEvent(streamId, eventData);

            // Act & Assert - Should not throw
            _factory.ValidateMessage(message);
        }

        [Fact]
        public void ValidateMessageWithNullThrows()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => _factory.ValidateMessage(null));
        }

        [Fact]
        public void ValidateMessageWithOversizedPayloadThrows()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var largeData = new string('x', 1000); // Large string
            var eventData = new TestEvent { Id = 123, Name = largeData };
            var message = _factory.CreateFromStreamEvent(streamId, eventData);

            // Act & Assert
            Assert.Throws<ArgumentException>(() => _factory.ValidateMessage(message, 100)); // Small limit
        }

        [Fact]
        public void ValidateMessageWithInvalidMetadataThrows()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var sequenceToken = new EventSequenceTokenV2(123);
            var payload = "test"u8.ToArray();
            var metadata = new ServiceBusMessageMetadata(); // Empty message ID

            var message = new AzureServiceBusMessage(streamId, sequenceToken, payload, metadata: metadata);

            // Act & Assert
            Assert.Throws<ArgumentException>(() => _factory.ValidateMessage(message));
        }

        [Fact]
        public void ExtractRequestContextFromServiceBusMessage()
        {
            // Arrange
            var originalContext = new Dictionary<string, object>
            {
                { "UserId", "test-user" },
                { "SessionId", "session-123" }
            };
            var contextBytes = _serializer.SerializeToArray(originalContext);
            var contextString = Convert.ToBase64String(contextBytes);

            var serviceBusMessage = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString("test"),
                messageId: "msg-123",
                applicationProperties: new Dictionary<string, object>
                {
                    { "RequestContext", contextString }
                });

            // Act
            var extractedContext = _factory.ExtractRequestContext(serviceBusMessage);

            // Assert
            Assert.NotNull(extractedContext);
            Assert.Equal(2, extractedContext.Count);
            Assert.Equal("test-user", extractedContext["UserId"]);
            Assert.Equal("session-123", extractedContext["SessionId"]);
        }

        [Fact]
        public void ExtractRequestContextWithInvalidDataReturnsNull()
        {
            // Arrange
            var serviceBusMessage = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString("test"),
                messageId: "msg-123",
                applicationProperties: new Dictionary<string, object>
                {
                    { "RequestContext", "invalid-base64" }
                });

            // Act
            var extractedContext = _factory.ExtractRequestContext(serviceBusMessage);

            // Assert
            Assert.Null(extractedContext);
        }

        [Fact]
        public void ExtractStreamIdFromServiceBusMessage()
        {
            // Arrange
            var expectedStreamId = StreamId.Create("test-namespace", "test-key");
            var serviceBusMessage = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString("test"),
                messageId: "msg-123",
                applicationProperties: new Dictionary<string, object>
                {
                    { "StreamId", expectedStreamId.ToString() }
                });

            // Act
            var extractedStreamId = _factory.ExtractStreamId(serviceBusMessage);

            // Assert
            Assert.Equal(expectedStreamId, extractedStreamId);
        }

        [Fact]
        public void ExtractStreamIdWithInvalidDataUsesDefault()
        {
            // Arrange
            var defaultStreamId = StreamId.Create("default", "default-key");
            var serviceBusMessage = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString("test"),
                messageId: "msg-123",
                applicationProperties: new Dictionary<string, object>
                {
                    { "StreamId", "invalid-stream-id" }
                });

            // Act
            var extractedStreamId = _factory.ExtractStreamId(serviceBusMessage, defaultStreamId);

            // Assert
            Assert.Equal(defaultStreamId, extractedStreamId);
        }

        [Fact]
        public void ExtractStreamIdWithNoDataAndNoDefaultThrows()
        {
            // Arrange
            var serviceBusMessage = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString("test"),
                messageId: "msg-123");

            // Act & Assert
            Assert.Throws<ArgumentException>(() => _factory.ExtractStreamId(serviceBusMessage));
        }

        [Fact]
        public void CreateFromServiceBusMessageSuccess()
        {
            // Arrange
            var streamId = StreamId.Create("test-namespace", "test-key");
            var serviceBusMessage = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString("test-payload"),
                messageId: "msg-123",
                sequenceNumber: 12345,
                deliveryCount: 2,
                correlationId: "corr-456",
                sessionId: "session-789",
                subject: "test-subject",
                replyTo: "reply-queue",
                partitionKey: "partition-1",
                timeToLive: TimeSpan.FromMinutes(30),
                enqueuedTime: DateTimeOffset.Parse("2024-01-15T10:30:00Z"),
                applicationProperties: new Dictionary<string, object>
                {
                    { "custom-prop", "custom-value" }
                });

            // Act
            var message = _factory.CreateFromServiceBusMessage(serviceBusMessage, streamId);

            // Assert
            Assert.Equal(streamId, message.StreamId);
            Assert.NotNull(message.SequenceToken);
            
            var token = Assert.IsType<EventSequenceTokenV2>(message.SequenceToken);
            Assert.Equal(12345, token.SequenceNumber);
            
            Assert.Equal("msg-123", message.Metadata.MessageId);
            Assert.Equal("corr-456", message.Metadata.CorrelationId);
            Assert.Equal("session-789", message.Metadata.SessionId);
            Assert.Equal("test-subject", message.Metadata.Subject);
            Assert.Equal("reply-queue", message.Metadata.ReplyTo);
            Assert.Equal("partition-1", message.Metadata.PartitionKey);
            Assert.Equal(TimeSpan.FromMinutes(30), message.Metadata.TimeToLive);
            Assert.Equal(DateTimeOffset.Parse("2024-01-15T10:30:00Z"), message.Metadata.EnqueuedTime);
            Assert.Single(message.Metadata.ApplicationProperties);
            Assert.Equal("custom-value", message.Metadata.ApplicationProperties["custom-prop"]);
        }

        private class TestEvent
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }
    }
}