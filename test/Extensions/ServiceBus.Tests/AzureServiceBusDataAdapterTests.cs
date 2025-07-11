using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.AzureServiceBus;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using TestExtensions;
using Xunit;

namespace ServiceBus.Tests
{
    [Collection(TestEnvironmentFixture.DefaultCollection)]
    public class AzureServiceBusDataAdapterTests
    {
        private readonly AzureServiceBusDataAdapter _adapter;
        private readonly ILoggerFactory _loggerFactory;
        private readonly TestEnvironmentFixture _fixture;

        public AzureServiceBusDataAdapterTests(TestEnvironmentFixture fixture)
        {
            _fixture = fixture;
            _loggerFactory = _fixture.Client.ServiceProvider.GetRequiredService<ILoggerFactory>();
            
            // Get serializer from the test fixture
            var serializer = _fixture.Serializer;
            _adapter = new AzureServiceBusDataAdapter(serializer, _loggerFactory);
        }

        [Fact]
        public void Constructor_WithNullSerializer_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new AzureServiceBusDataAdapter(null!, _loggerFactory));
        }

        [Fact]
        public void Constructor_WithNullLoggerFactory_ThrowsArgumentNullException()
        {
            var serializer = _fixture.Serializer;
            
            Assert.Throws<ArgumentNullException>(() => new AzureServiceBusDataAdapter(serializer, null!));
        }

        [Fact]
        public void ToQueueMessage_WithValidEvents_CreatesServiceBusMessage()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<string> { "event1", "event2", "event3" };
            var requestContext = new Dictionary<string, object> { { "key1", "value1" }, { "key2", "value2" } };
            var token = new EventSequenceTokenV2(100, 5);

            var message = _adapter.ToQueueMessage(streamId, events, token, requestContext);

            Assert.NotNull(message);
            Assert.Equal("application/x-orleans-batch", message.ContentType);
            Assert.Equal(streamId.ToString(), message.CorrelationId);
            Assert.NotNull(message.MessageId);
            Assert.NotEmpty(message.Body.ToArray());

            // Check application properties
            Assert.True(message.ApplicationProperties.ContainsKey("StreamId"));
            Assert.Equal(streamId.ToString(), message.ApplicationProperties["StreamId"]);
            
            if (streamId.GetNamespace() is string streamNamespace)
            {
                Assert.True(message.ApplicationProperties.ContainsKey("StreamNamespace"));
                Assert.Equal(streamNamespace, message.ApplicationProperties["StreamNamespace"]);
            }

            Assert.True(message.ApplicationProperties.ContainsKey("RequestContext.key1"));
            Assert.Equal("value1", message.ApplicationProperties["RequestContext.key1"]);
            Assert.True(message.ApplicationProperties.ContainsKey("RequestContext.key2"));
            Assert.Equal("value2", message.ApplicationProperties["RequestContext.key2"]);

            Assert.True(message.ApplicationProperties.ContainsKey("SequenceNumber"));
            Assert.Equal(100L, message.ApplicationProperties["SequenceNumber"]);
            Assert.True(message.ApplicationProperties.ContainsKey("EventIndex"));
            Assert.Equal(5, message.ApplicationProperties["EventIndex"]);
        }

        [Fact]
        public void ToQueueMessage_WithNullEvents_ThrowsArgumentNullException()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var requestContext = new Dictionary<string, object>();

            Assert.Throws<ArgumentNullException>(() => 
                _adapter.ToQueueMessage<string>(streamId, null!, null, requestContext));
        }

        [Fact]
        public void ToQueueMessage_WithNullRequestContext_HandlesGracefully()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<string> { "event1" };

            var message = _adapter.ToQueueMessage(streamId, events, null, null);

            Assert.NotNull(message);
            Assert.Equal("application/x-orleans-batch", message.ContentType);
            Assert.Equal(streamId.ToString(), message.CorrelationId);
        }

        [Fact]
        public void ToQueueMessage_WithNonStringRequestContextValues_ConvertsToString()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<string> { "event1" };
            var requestContext = new Dictionary<string, object> 
            { 
                { "intValue", 42 }, 
                { "boolValue", true },
                { "stringValue", "test" }
            };

            var message = _adapter.ToQueueMessage(streamId, events, null, requestContext);

            Assert.NotNull(message);
            Assert.Equal("42", message.ApplicationProperties["RequestContext.intValue"]);
            Assert.Equal("True", message.ApplicationProperties["RequestContext.boolValue"]);
            Assert.Equal("test", message.ApplicationProperties["RequestContext.stringValue"]);
        }

        [Fact]
        public void FromQueueMessage_WithValidMessage_ReturnsBatchContainer()
        {
            // First create a message
            var streamId = StreamId.Create("test-namespace", "test-key");
            var originalEvents = new List<string> { "event1", "event2" };
            var requestContext = new Dictionary<string, object> { { "key", "value" } };
            var token = new EventSequenceTokenV2(100, 0);

            var message = _adapter.ToQueueMessage(streamId, originalEvents, token, requestContext);

            // Now deserialize it
            var batchContainer = _adapter.FromQueueMessage(message, 200);

            Assert.NotNull(batchContainer);
            Assert.Equal(streamId, batchContainer.StreamId);
            
            var deserializedEvents = batchContainer.GetEvents<string>().ToList();
            Assert.Equal(2, deserializedEvents.Count);
            Assert.Equal("event1", deserializedEvents[0].Item1);
            Assert.Equal("event2", deserializedEvents[1].Item1);

            // Check that the sequence token preserved the stored sequence number from the message
            // When sequence properties are present in the message, they should be used instead of the sequenceId parameter
            var sequenceToken = batchContainer.SequenceToken as EventSequenceTokenV2;
            Assert.NotNull(sequenceToken);
            Assert.Equal(100, sequenceToken.SequenceNumber); // Should use stored value, not the sequenceId parameter (200)
        }

        [Fact]
        public void FromQueueMessage_WithSequenceNumberInProperties_UsesStoredSequenceNumber()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<string> { "event1" };
            var requestContext = new Dictionary<string, object>();
            var token = new EventSequenceTokenV2(100, 5);

            var message = _adapter.ToQueueMessage(streamId, events, token, requestContext);

            var batchContainer = _adapter.FromQueueMessage(message, 999);

            var sequenceToken = batchContainer.SequenceToken as EventSequenceTokenV2;
            Assert.NotNull(sequenceToken);
            Assert.Equal(100, sequenceToken.SequenceNumber); // Should use stored value, not the sequenceId parameter
            Assert.Equal(5, sequenceToken.EventIndex);
        }

        [Fact]
        public void FromQueueMessage_WithNullMessage_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => _adapter.FromQueueMessage(null!, 100));
        }

        [Fact]
        public void RoundTripSerialization_PreservesAllData()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var originalEvents = new List<object> { "string-event", 42, true, 3.14 };
            var requestContext = new Dictionary<string, object> 
            { 
                { "string-key", "string-value" },
                { "int-key", 99 },
                { "bool-key", false }
            };
            var token = new EventSequenceTokenV2(123, 7);

            // Serialize to Service Bus message
            var message = _adapter.ToQueueMessage(streamId, originalEvents, token, requestContext);

            // Deserialize back to batch container
            var batchContainer = _adapter.FromQueueMessage(message, 456);

            // Verify stream identity is preserved
            Assert.Equal(streamId, batchContainer.StreamId);

            // Verify events are preserved
            var deserializedEvents = batchContainer.GetEvents<object>().ToList();
            Assert.Equal(4, deserializedEvents.Count);

            // Verify sequence token
            var sequenceToken = batchContainer.SequenceToken as EventSequenceTokenV2;
            Assert.NotNull(sequenceToken);
            Assert.Equal(123, sequenceToken.SequenceNumber);
            Assert.Equal(7, sequenceToken.EventIndex);

            // Verify request context can be imported
            Assert.True(batchContainer.ImportRequestContext());
        }

        [Fact]
        public void ToQueueMessage_WithEmptyEvents_CreatesValidMessage()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<string>();
            var requestContext = new Dictionary<string, object>();

            var message = _adapter.ToQueueMessage(streamId, events, null, requestContext);

            Assert.NotNull(message);
            Assert.Equal("application/x-orleans-batch", message.ContentType);
            Assert.NotEmpty(message.Body.ToArray());
        }

        [Fact]
        public void ToQueueMessage_WithLargeEventCollection_HandlesCorrectly()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = Enumerable.Range(1, 1000).Select(i => $"event-{i}").ToList();
            var requestContext = new Dictionary<string, object>();

            var message = _adapter.ToQueueMessage(streamId, events, null, requestContext);

            Assert.NotNull(message);
            Assert.True(message.Body.ToArray().Length > 0);
            
            // Verify round-trip with large collection
            var batchContainer = _adapter.FromQueueMessage(message, 1);
            var deserializedEvents = batchContainer.GetEvents<string>().ToList();
            Assert.Equal(1000, deserializedEvents.Count);
            Assert.Equal("event-1", deserializedEvents[0].Item1);
            Assert.Equal("event-1000", deserializedEvents[999].Item1);
        }

        [Fact]
        public void ToQueueMessage_WithoutSequenceToken_DoesNotAddSequenceProperties()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<string> { "event1" };
            var requestContext = new Dictionary<string, object>();

            var message = _adapter.ToQueueMessage(streamId, events, null, requestContext);

            Assert.NotNull(message);
            Assert.False(message.ApplicationProperties.ContainsKey("SequenceNumber"));
            Assert.False(message.ApplicationProperties.ContainsKey("EventIndex"));
        }

        [Fact]
        public void FromQueueMessage_WithMissingSequenceProperties_UsesProvidedSequenceId()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<string> { "event1" };
            var requestContext = new Dictionary<string, object>();

            var message = _adapter.ToQueueMessage(streamId, events, null, requestContext);
            
            // Remove sequence properties to simulate missing metadata
            message.ApplicationProperties.Remove("SequenceNumber");
            message.ApplicationProperties.Remove("EventIndex");

            var batchContainer = _adapter.FromQueueMessage(message, 777);

            var sequenceToken = batchContainer.SequenceToken as EventSequenceTokenV2;
            Assert.NotNull(sequenceToken);
            Assert.Equal(777, sequenceToken.SequenceNumber);
            Assert.Equal(0, sequenceToken.EventIndex);
        }
    }
}