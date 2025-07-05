using System;
using System.Collections.Generic;
using Orleans.Providers.Streams.AzureServiceBus;
using Orleans.Runtime;
using Xunit;

namespace ServiceBus.Tests
{
    public class AzureServiceBusSequenceTokenV2Tests
    {
        [Fact]
        public void Constructor_SetsProperties()
        {
            var token = new AzureServiceBusSequenceTokenV2(123, 5);

            Assert.Equal(123, token.SequenceNumber);
            Assert.Equal(5, token.EventIndex);
        }

        [Fact]
        public void DefaultConstructor_SetsDefaultValues()
        {
            var token = new AzureServiceBusSequenceTokenV2();

            Assert.Equal(0, token.SequenceNumber);
            Assert.Equal(0, token.EventIndex);
        }

        [Fact]
        public void CompareTo_OrdersBySequenceNumberFirst()
        {
            var token1 = new AzureServiceBusSequenceTokenV2(100, 5);
            var token2 = new AzureServiceBusSequenceTokenV2(200, 1);

            Assert.True(token1.CompareTo(token2) < 0);
            Assert.True(token2.CompareTo(token1) > 0);
        }

        [Fact]
        public void CompareTo_OrdersByEventIndexSecond()
        {
            var token1 = new AzureServiceBusSequenceTokenV2(100, 1);
            var token2 = new AzureServiceBusSequenceTokenV2(100, 5);

            Assert.True(token1.CompareTo(token2) < 0);
            Assert.True(token2.CompareTo(token1) > 0);
        }

        [Fact]
        public void CompareTo_EqualTokensReturnZero()
        {
            var token1 = new AzureServiceBusSequenceTokenV2(100, 5);
            var token2 = new AzureServiceBusSequenceTokenV2(100, 5);

            Assert.Equal(0, token1.CompareTo(token2));
        }

        [Fact]
        public void Equals_SameValues_ReturnsTrue()
        {
            var token1 = new AzureServiceBusSequenceTokenV2(100, 5);
            var token2 = new AzureServiceBusSequenceTokenV2(100, 5);

            Assert.True(token1.Equals(token2));
            Assert.True(token1.Equals((object)token2));
        }

        [Fact]
        public void Equals_DifferentValues_ReturnsFalse()
        {
            var token1 = new AzureServiceBusSequenceTokenV2(100, 5);
            var token2 = new AzureServiceBusSequenceTokenV2(100, 6);
            var token3 = new AzureServiceBusSequenceTokenV2(101, 5);

            Assert.False(token1.Equals(token2));
            Assert.False(token1.Equals(token3));
        }

        [Fact]
        public void GetHashCode_SameValues_SameHash()
        {
            var token1 = new AzureServiceBusSequenceTokenV2(100, 5);
            var token2 = new AzureServiceBusSequenceTokenV2(100, 5);

            Assert.Equal(token1.GetHashCode(), token2.GetHashCode());
        }

        [Fact]
        public void ToString_ReturnsCorrectFormat()
        {
            var token = new AzureServiceBusSequenceTokenV2(123, 5);

            Assert.Equal("[123:5]", token.ToString());
        }

        [Fact]
        public void CreateSequenceTokenForEvent_ReturnsCorrectToken()
        {
            var baseToken = new AzureServiceBusSequenceTokenV2(100, 1);
            var eventToken = baseToken.CreateSequenceTokenForEvent(3);

            Assert.Equal(100, eventToken.SequenceNumber);
            Assert.Equal(3, eventToken.EventIndex);
        }
    }
}