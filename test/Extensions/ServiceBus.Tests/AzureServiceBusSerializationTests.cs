using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Orleans.Providers.Streams.AzureServiceBus;
using Orleans.Runtime;
using Xunit;

namespace ServiceBus.Tests
{
    public class AzureServiceBusSerializationTests
    {
        [Fact]
        public void SequenceToken_JsonSerialization_RoundTrip()
        {
            var originalToken = new AzureServiceBusSequenceTokenV2(12345, 67);

            var json = JsonConvert.SerializeObject(originalToken);
            var deserializedToken = JsonConvert.DeserializeObject<AzureServiceBusSequenceTokenV2>(json);

            Assert.Equal(originalToken.SequenceNumber, deserializedToken.SequenceNumber);
            Assert.Equal(originalToken.EventIndex, deserializedToken.EventIndex);
            Assert.True(originalToken.Equals(deserializedToken));
        }

        [Fact]
        public void BatchContainer_JsonSerialization_RoundTrip()
        {
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<object> { "string-event", 42, true };
            var context = new Dictionary<string, object> { { "user", "test-user" }, { "correlation", "abc123" } };
            var sequenceToken = new AzureServiceBusSequenceTokenV2(999, 3);

            var originalContainer = new AzureServiceBusBatchContainer(streamId, events, context, sequenceToken);

            var json = JsonConvert.SerializeObject(originalContainer);
            var deserializedContainer = JsonConvert.DeserializeObject<AzureServiceBusBatchContainer>(json);

            Assert.Equal(originalContainer.StreamId, deserializedContainer.StreamId);
            Assert.Equal(originalContainer.Events.Count, deserializedContainer.Events.Count);
            Assert.Equal(originalContainer.RequestContext.Count, deserializedContainer.RequestContext.Count);
            
            var originalSeqToken = originalContainer.SequenceToken as AzureServiceBusSequenceTokenV2;
            var deserializedSeqToken = deserializedContainer.SequenceToken as AzureServiceBusSequenceTokenV2;
            
            Assert.NotNull(originalSeqToken);
            Assert.NotNull(deserializedSeqToken);
            Assert.Equal(originalSeqToken.SequenceNumber, deserializedSeqToken.SequenceNumber);
            Assert.Equal(originalSeqToken.EventIndex, deserializedSeqToken.EventIndex);
        }

        [Fact]
        public void SequenceTokenOrdering_MultipleTokens_CorrectOrder()
        {
            var tokens = new[]
            {
                new AzureServiceBusSequenceTokenV2(100, 1),
                new AzureServiceBusSequenceTokenV2(50, 5),
                new AzureServiceBusSequenceTokenV2(100, 0),
                new AzureServiceBusSequenceTokenV2(200, 0),
                new AzureServiceBusSequenceTokenV2(100, 2)
            };

            Array.Sort(tokens, (a, b) => a.CompareTo(b));

            // Expected order: (50,5), (100,0), (100,1), (100,2), (200,0)
            Assert.Equal(50, tokens[0].SequenceNumber);
            Assert.Equal(5, tokens[0].EventIndex);
            
            Assert.Equal(100, tokens[1].SequenceNumber);
            Assert.Equal(0, tokens[1].EventIndex);
            
            Assert.Equal(100, tokens[2].SequenceNumber);
            Assert.Equal(1, tokens[2].EventIndex);
            
            Assert.Equal(100, tokens[3].SequenceNumber);
            Assert.Equal(2, tokens[3].EventIndex);
            
            Assert.Equal(200, tokens[4].SequenceNumber);
            Assert.Equal(0, tokens[4].EventIndex);
        }
    }
}