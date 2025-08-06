using System;
using System.Collections.Generic;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus.Messages;
using Xunit;

namespace TesterAzureUtils.AzureServiceBus.Messages
{
    /// <summary>
    /// Integration tests that validate the complete message flow from Orleans events
    /// through Azure Service Bus messages and back to Orleans batch containers.
    /// </summary>
    [Trait("Category", "AzureServiceBus")]
    [Trait("Category", "Integration")]
    public class MessageIntegrationTests
    {
        private readonly Serializer _serializer;
        private readonly AzureServiceBusMessageFactory _factory;

        public MessageIntegrationTests()
        {
            var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
            services.AddSerializer();
            var serviceProvider = services.BuildServiceProvider();
            _serializer = serviceProvider.GetRequiredService<Serializer>();
            _factory = new AzureServiceBusMessageFactory(_serializer);
        }

        [Fact]
        public void CompleteMessageFlowFromEventToServiceBusAndBack()
        {
            // Arrange - Create an Orleans stream event
            var streamId = StreamId.Create("integration-test", "user-123");
            var originalEvent = new UserEvent
            {
                UserId = "user-123",
                Action = "login",
                Timestamp = DateTimeOffset.UtcNow,
                Properties = new Dictionary<string, object>
                {
                    { "IP", "192.168.1.1" },
                    { "UserAgent", "Mozilla/5.0" }
                }
            };
            var requestContext = new Dictionary<string, object>
            {
                { "TraceId", "trace-12345" },
                { "SpanId", "span-67890" }
            };

            // Act 1: Create Orleans message from stream event
            var orleansMessage = _factory.CreateFromStreamEvent(streamId, originalEvent, requestContext);

            // Act 2: Convert to Service Bus message for publishing
            var serviceBusMessage = _factory.CreateServiceBusMessage(streamId, originalEvent, requestContext);

            // Act 3: Simulate receiving the Service Bus message
            var receivedMessage = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: serviceBusMessage.Body,
                messageId: serviceBusMessage.MessageId,
                subject: serviceBusMessage.Subject,
                correlationId: serviceBusMessage.CorrelationId,
                sessionId: serviceBusMessage.SessionId,
                replyTo: serviceBusMessage.ReplyTo,
                partitionKey: serviceBusMessage.PartitionKey,
                timeToLive: serviceBusMessage.TimeToLive,
                sequenceNumber: 12345,
                deliveryCount: 1,
                enqueuedTime: DateTimeOffset.UtcNow,
                applicationProperties: serviceBusMessage.ApplicationProperties);

            // Act 4: Convert back to Orleans message
            var receivedOrleansMessage = _factory.CreateFromServiceBusMessage(receivedMessage, streamId);

            // Act 5: Extract events from the message
            var events = receivedOrleansMessage.GetEvents<byte[]>();

            // Assert - Verify the complete round trip
            Assert.Single(events);
            var eventTuple = Assert.Single(events);
            
            // Deserialize the event data to verify it matches
            var deserializedEvent = _serializer.Deserialize<UserEvent>(eventTuple.Item1);
            
            Assert.Equal(originalEvent.UserId, deserializedEvent.UserId);
            Assert.Equal(originalEvent.Action, deserializedEvent.Action);
            Assert.Equal(originalEvent.Properties.Count, deserializedEvent.Properties.Count);
            
            // Verify metadata preservation
            Assert.Equal(serviceBusMessage.MessageId, receivedOrleansMessage.Metadata.MessageId);
            Assert.Equal(serviceBusMessage.Subject, receivedOrleansMessage.Metadata.Subject);
            
            // Verify stream information was preserved in Service Bus properties
            Assert.True(serviceBusMessage.ApplicationProperties.ContainsKey("StreamId"));
            Assert.Equal(streamId.ToString(), serviceBusMessage.ApplicationProperties["StreamId"]);
        }

        [Fact]
        public void BatchProcessingIntegrationTest()
        {
            // Arrange - Create multiple events
            var streamId = StreamId.Create("batch-test", "batch-123");
            var events = new[]
            {
                new UserEvent { UserId = "user-1", Action = "login", Timestamp = DateTimeOffset.UtcNow },
                new UserEvent { UserId = "user-2", Action = "logout", Timestamp = DateTimeOffset.UtcNow },
                new UserEvent { UserId = "user-3", Action = "update", Timestamp = DateTimeOffset.UtcNow }
            };

            var streamEvents = events.Select(e => (streamId, (object)e, (Dictionary<string, object>?)null));

            // Act 1: Create batch from stream events
            var batch = _factory.CreateBatch(streamEvents, "integration-batch");

            // Act 2: Serialize and deserialize the batch (simulates persistence/transport)
            var serializedBatch = _serializer.SerializeToArray(batch);
            var deserializedBatch = _serializer.Deserialize<AzureServiceBusBatchContainer>(serializedBatch);

            // Act 3: Extract all events from the batch
            var batchEvents = deserializedBatch.GetEvents<byte[]>();

            // Assert
            Assert.Equal(3, batchEvents.Count());
            Assert.Equal("integration-batch", deserializedBatch.BatchId);
            Assert.Equal(streamId, deserializedBatch.StreamId);

            // Verify each event was preserved correctly
            var eventList = batchEvents.ToList();
            for (int i = 0; i < events.Length; i++)
            {
                var deserializedEvent = _serializer.Deserialize<UserEvent>(eventList[i].Item1);
                Assert.Equal(events[i].UserId, deserializedEvent.UserId);
                Assert.Equal(events[i].Action, deserializedEvent.Action);
            }
        }

        [Fact]
        public void RequestContextPreservationThroughServiceBus()
        {
            // Arrange
            var streamId = StreamId.Create("context-test", "ctx-123");
            var originalEvent = new UserEvent { UserId = "user-123", Action = "test" };
            var requestContext = new Dictionary<string, object>
            {
                { "TraceId", "trace-abc123" },
                { "UserId", "calling-user-456" },
                { "TenantId", "tenant-789" }
            };

            // Act 1: Create Service Bus message with request context
            var serviceBusMessage = _factory.CreateServiceBusMessage(streamId, originalEvent, requestContext);

            // Act 2: Extract request context from Service Bus message
            var simulatedReceivedMessage = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: serviceBusMessage.Body,
                messageId: serviceBusMessage.MessageId,
                applicationProperties: serviceBusMessage.ApplicationProperties);

            var extractedContext = _factory.ExtractRequestContext(simulatedReceivedMessage);

            // Assert
            Assert.NotNull(extractedContext);
            Assert.Equal(requestContext.Count, extractedContext.Count);
            Assert.Equal("trace-abc123", extractedContext["TraceId"]);
            Assert.Equal("calling-user-456", extractedContext["UserId"]);
            Assert.Equal("tenant-789", extractedContext["TenantId"]);
        }

        [Fact]
        public void MessageValidationWithRealWorldConstraints()
        {
            // Arrange - Create a message that's close to Azure Service Bus limits
            var streamId = StreamId.Create("validation-test", "large-message");
            var largeEvent = new UserEvent
            {
                UserId = "user-123",
                Action = "bulk-operation",
                Properties = new Dictionary<string, object>()
            };

            // Add properties to approach but not exceed the 256KB limit
            for (int i = 0; i < 1000; i++)
            {
            for (int i = 0; i < PropertyCountForLargeEvent; i++)
            {
                largeEvent.Properties[$"property_{i}"] = new string('x', PropertyValueLengthForLargeEvent); // 200 chars each
            }

            var message = _factory.CreateFromStreamEvent(streamId, largeEvent);

            // Act & Assert - Should validate successfully for standard Service Bus (256KB)
            _factory.ValidateMessage(message, 256 * 1024);

            // Should fail for smaller limit
            Assert.Throws<ArgumentException>(() => _factory.ValidateMessage(message, 100 * 1024));
        }

        private class UserEvent
        {
            public string UserId { get; set; } = string.Empty;
            public string Action { get; set; } = string.Empty;
            public DateTimeOffset Timestamp { get; set; }
            public Dictionary<string, object> Properties { get; set; } = new();
        }
    }
}