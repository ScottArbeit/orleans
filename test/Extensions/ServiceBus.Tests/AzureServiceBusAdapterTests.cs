using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.AzureServiceBus;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using TestExtensions;
using Xunit;

namespace ServiceBus.Tests
{
    [Collection(TestEnvironmentFixture.DefaultCollection)]
    public class AzureServiceBusAdapterTests
    {
        private readonly TestEnvironmentFixture _fixture;
        private readonly ILoggerFactory _loggerFactory;
        private readonly AzureServiceBusDataAdapter _dataAdapter;

        public AzureServiceBusAdapterTests(TestEnvironmentFixture fixture)
        {
            _fixture = fixture;
            _loggerFactory = _fixture.Client.ServiceProvider.GetRequiredService<ILoggerFactory>();
            
            // Get serializer from the test fixture
            var serializer = _fixture.Serializer;
            _dataAdapter = new AzureServiceBusDataAdapter(serializer, _loggerFactory);
        }

        [Fact]
        public void Constructor_WithNullDataAdapter_ThrowsArgumentNullException()
        {
            var streamQueueMapper = CreateTestStreamQueueMapper();
            var options = CreateTestOptions();

            Assert.Throws<ArgumentNullException>(() => 
                new AzureServiceBusAdapter(null!, streamQueueMapper, _loggerFactory, options, "TestProvider"));
        }

        [Fact]
        public void Constructor_WithNullStreamQueueMapper_ThrowsArgumentNullException()
        {
            var options = CreateTestOptions();

            Assert.Throws<ArgumentNullException>(() => 
                new AzureServiceBusAdapter(_dataAdapter, null!, _loggerFactory, options, "TestProvider"));
        }

        [Fact]
        public void Constructor_WithNullLoggerFactory_ThrowsArgumentNullException()
        {
            var streamQueueMapper = CreateTestStreamQueueMapper();
            var options = CreateTestOptions();

            Assert.Throws<ArgumentNullException>(() => 
                new AzureServiceBusAdapter(_dataAdapter, streamQueueMapper, null!, options, "TestProvider"));
        }

        [Fact]
        public void Constructor_WithNullOptions_ThrowsArgumentNullException()
        {
            var streamQueueMapper = CreateTestStreamQueueMapper();

            Assert.Throws<ArgumentNullException>(() => 
                new AzureServiceBusAdapter(_dataAdapter, streamQueueMapper, _loggerFactory, null!, "TestProvider"));
        }

        [Fact]
        public void Constructor_WithNullProviderName_ThrowsArgumentNullException()
        {
            var streamQueueMapper = CreateTestStreamQueueMapper();
            var options = CreateTestOptions();

            Assert.Throws<ArgumentNullException>(() => 
                new AzureServiceBusAdapter(_dataAdapter, streamQueueMapper, _loggerFactory, options, null!));
        }

        [Fact]
        public void Properties_ReturnsExpectedValues()
        {
            var streamQueueMapper = CreateTestStreamQueueMapper();
            var options = CreateTestOptions();
            var adapter = new AzureServiceBusAdapter(_dataAdapter, streamQueueMapper, _loggerFactory, options, "TestProvider");

            Assert.Equal("TestProvider", adapter.Name);
            Assert.False(adapter.IsRewindable);
            Assert.Equal(StreamProviderDirection.ReadWrite, adapter.Direction);
        }

        [Fact]
        public void CreateReceiver_ThrowsNotImplementedException()
        {
            var streamQueueMapper = CreateTestStreamQueueMapper();
            var options = CreateTestOptions();
            var adapter = new AzureServiceBusAdapter(_dataAdapter, streamQueueMapper, _loggerFactory, options, "TestProvider");
            var queueId = QueueId.GetQueueId("test", 0, 0);

            Assert.Throws<NotImplementedException>(() => adapter.CreateReceiver(queueId));
        }

        [Fact]
        public async Task QueueMessageBatchAsync_WithNonNullToken_ThrowsArgumentException()
        {
            var streamQueueMapper = CreateTestStreamQueueMapper();
            var options = CreateTestOptions();
            var adapter = new AzureServiceBusAdapter(_dataAdapter, streamQueueMapper, _loggerFactory, options, "TestProvider");
            
            var streamId = StreamId.Create("test", "stream1");
            var events = new[] { "test event" };
            var token = new EventSequenceToken(1, 0);
            var requestContext = new Dictionary<string, object>();

            var exception = await Assert.ThrowsAsync<ArgumentException>(
                () => adapter.QueueMessageBatchAsync(streamId, events, token, requestContext));
            
            Assert.Equal("token", exception.ParamName);
            Assert.Contains("Azure Service Bus stream provider currently does not support non-null StreamSequenceToken", exception.Message);
        }

        [Fact]
        public void GetEntityName_ForQueueTopology_WithQueueName_ReturnsQueueName()
        {
            var streamQueueMapper = CreateTestStreamQueueMapper();
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue,
                QueueName = "TestQueue"
            };
            var adapter = new AzureServiceBusAdapter(_dataAdapter, streamQueueMapper, _loggerFactory, options, "TestProvider");
            
            // Use reflection to test the private method
            var method = typeof(AzureServiceBusAdapter).GetMethod("GetEntityName", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var queueId = QueueId.GetQueueId("TestQueue", 0, 0);
            var result = (string)method!.Invoke(adapter, new object[] { queueId });
            
            Assert.Equal("TestQueue", result);
        }

        [Fact]
        public void GetEntityName_ForTopicTopology_WithTopicName_ReturnsTopicName()
        {
            var streamQueueMapper = CreateTestStreamQueueMapper();
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Topic,
                TopicName = "TestTopic"
            };
            var adapter = new AzureServiceBusAdapter(_dataAdapter, streamQueueMapper, _loggerFactory, options, "TestProvider");
            
            // Use reflection to test the private method
            var method = typeof(AzureServiceBusAdapter).GetMethod("GetEntityName", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var queueId = QueueId.GetQueueId("TestQueue", 0, 0);
            var result = (string)method!.Invoke(adapter, new object[] { queueId });
            
            Assert.Equal("TestTopic", result);
        }

        [Fact]
        public void GetEntityName_ForQueueTopology_WithoutQueueName_ReturnsPartitionName()
        {
            var streamQueueMapper = CreateTestStreamQueueMapper();
            var options = new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue
            };
            var adapter = new AzureServiceBusAdapter(_dataAdapter, streamQueueMapper, _loggerFactory, options, "TestProvider");
            
            // Use reflection to test the private method
            var method = typeof(AzureServiceBusAdapter).GetMethod("GetEntityName", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var queueId = QueueId.GetQueueId("TestQueue", 0, 0);
            var result = (string)method!.Invoke(adapter, new object[] { queueId });
            
            Assert.Equal("partition0", result); // Should use the first partition from our test mapper
        }

        private static HashRingBasedPartitionedStreamQueueMapper CreateTestStreamQueueMapper()
        {
            var partitionIds = new[] { "partition0", "partition1" };
            return new HashRingBasedPartitionedStreamQueueMapper(partitionIds, "TestQueue");
        }

        private static AzureServiceBusOptions CreateTestOptions()
        {
            return new AzureServiceBusOptions
            {
                TopologyType = ServiceBusTopologyType.Queue
            };
        }
    }
}