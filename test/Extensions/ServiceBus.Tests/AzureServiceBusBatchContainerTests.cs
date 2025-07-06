using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Providers.Streams.AzureServiceBus;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Xunit;

namespace ServiceBus.Tests
{
    public class AzureServiceBusBatchContainerTests
    {
        [Fact]
        public void Constructor_WithEventsAndContext_SetsProperties()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<object> { "event1", "event2" };
            var context = new Dictionary<string, object> { { "key", "value" } };

            var container = new AzureServiceBusBatchContainer(streamId, events, context);

            Assert.Equal(streamId, container.StreamId);
            Assert.NotNull(container.SequenceToken);
        }

        [Fact]
        public void Constructor_WithNullContext_HandlesGracefully()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<object> { "event1" };

            var container = new AzureServiceBusBatchContainer(streamId, events, null);

            Assert.Equal(streamId, container.StreamId);
            Assert.NotNull(container.SequenceToken);
        }

        [Fact]
        public void Constructor_WithNullEvents_ThrowsArgumentNullException()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var context = new Dictionary<string, object>();

            Assert.Throws<ArgumentNullException>(() => 
                new AzureServiceBusBatchContainer(streamId, null!, context));
        }

        [Fact]
        public void GetEvents_ReturnsTypedEventsWithSequenceTokens()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<object> { "string1", 42, "string2", 99 };
            var context = new Dictionary<string, object>();
            var sequenceToken = new EventSequenceTokenV2(100, 0);

            var container = new AzureServiceBusBatchContainer(streamId, events, context, sequenceToken);

            var stringEvents = container.GetEvents<string>().ToList();
            var intEvents = container.GetEvents<int>().ToList();

            Assert.Equal(2, stringEvents.Count);
            Assert.Equal(2, intEvents.Count);
            
            Assert.Equal("string1", stringEvents[0].Item1);
            Assert.Equal("string2", stringEvents[1].Item1);
            Assert.Equal(42, intEvents[0].Item1);
            Assert.Equal(99, intEvents[1].Item1);

            // Check sequence tokens for events
            var firstStringToken = stringEvents[0].Item2 as EventSequenceTokenV2;
            var secondStringToken = stringEvents[1].Item2 as EventSequenceTokenV2;
            
            Assert.NotNull(firstStringToken);
            Assert.NotNull(secondStringToken);
            Assert.Equal(100, firstStringToken.SequenceNumber);
            Assert.Equal(100, secondStringToken.SequenceNumber);
            Assert.Equal(0, firstStringToken.EventIndex);
            Assert.Equal(1, secondStringToken.EventIndex);
        }

        [Fact]
        public void ImportRequestContext_WithEmptyContext_ReturnsTrue()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<object> { "event1" };
            var context = new Dictionary<string, object>();

            var container = new AzureServiceBusBatchContainer(streamId, events, context);

            var result = container.ImportRequestContext();

            Assert.True(result);
        }

        [Fact]
        public void ImportRequestContext_WithNullContext_ReturnsFalse()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<object> { "event1" };

            var container = new AzureServiceBusBatchContainer(streamId, events, null);

            var result = container.ImportRequestContext();

            Assert.False(result);
        }

        [Fact]
        public void ImportRequestContext_WithData_ReturnsTrue()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<object> { "event1" };
            var context = new Dictionary<string, object> { { "key", "value" } };

            var container = new AzureServiceBusBatchContainer(streamId, events, context);

            var result = container.ImportRequestContext();

            Assert.True(result);
        }

        [Fact]
        public void ToString_ReturnsCorrectFormat()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<object> { "event1", "event2" };
            var context = new Dictionary<string, object>();

            var container = new AzureServiceBusBatchContainer(streamId, events, context);

            var result = container.ToString();

            Assert.Contains("AzureServiceBusBatchContainer", result);
            Assert.Contains("Stream=", result);
            Assert.Contains("#Items=2", result);
        }

        [Fact]
        public void SequenceToken_ReturnsInternalSequenceToken()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<object> { "event1" };
            var context = new Dictionary<string, object>();
            var sequenceToken = new EventSequenceTokenV2(123, 5);

            var container = new AzureServiceBusBatchContainer(streamId, events, context, sequenceToken);

            Assert.Equal(sequenceToken, container.SequenceToken);
        }
    }
}