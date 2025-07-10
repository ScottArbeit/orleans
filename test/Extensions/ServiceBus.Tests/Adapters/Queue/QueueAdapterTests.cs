using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Providers.Streams.ServiceBus;
using Orleans.Runtime;
using Orleans.Streaming.ServiceBus;
using Orleans.Streams;
using Xunit;

namespace ServiceBus.Tests.Adapters.Queue;

[TestCategory("BVT")]
[TestCategory("ServiceBus")]
[TestCategory("QueueAdapter")]
public class QueueAdapterTests
{
    [Fact]
    public void ServiceBusQueueAdapter_Constructor_ShouldSetProperties()
    {
        // Arrange
        var loggerFactory = NullLoggerFactory.Instance;
        var serviceBusOptions = new ServiceBusOptions
        {
            PrefetchCount = 10,
            MaxConcurrentCalls = 2,
            MaxDeliveryAttempts = 3,
            PartitionCount = 2,
            QueueNamePrefix = "test-queue"
        };

        var queueNames = new List<string> { "test-queue-0", "test-queue-1" };
        var streamQueueMapper = new HashRingBasedPartitionedStreamQueueMapper(queueNames, "test-provider");

        var optionsMonitor = Substitute.For<IOptionsMonitor<ServiceBusOptions>>();
        optionsMonitor.Get("test-provider").Returns(serviceBusOptions);
        var clientFactory = new ServiceBusClientFactory(optionsMonitor);

        // Act
        var adapter = new ServiceBusQueueAdapter(
            "test-provider",
            serviceBusOptions,
            streamQueueMapper,
            clientFactory,
            loggerFactory);

        // Assert
        Assert.Equal("test-provider", adapter.Name);
        Assert.False(adapter.IsRewindable);
        Assert.Equal(StreamProviderDirection.ReadWrite, adapter.Direction);
    }

    [Fact]
    public void ServiceBusQueueAdapter_CreateReceiver_ShouldReturnReceiver()
    {
        // Arrange
        var loggerFactory = NullLoggerFactory.Instance;
        var serviceBusOptions = new ServiceBusOptions();
        var queueNames = new List<string> { "test-queue-0" };
        var streamQueueMapper = new HashRingBasedPartitionedStreamQueueMapper(queueNames, "test-provider");

        var optionsMonitor = Substitute.For<IOptionsMonitor<ServiceBusOptions>>();
        var clientFactory = new ServiceBusClientFactory(optionsMonitor);

        var adapter = new ServiceBusQueueAdapter(
            "test-provider",
            serviceBusOptions,
            streamQueueMapper,
            clientFactory,
            loggerFactory);

        // Get the actual queue ID from the mapper
        var queueId = streamQueueMapper.GetAllQueues().First();

        // Act
        var receiver = adapter.CreateReceiver(queueId);

        // Assert
        Assert.NotNull(receiver);
        Assert.IsType<ServiceBusQueueAdapterReceiver>(receiver);
    }

    [Fact]
    public void ServiceBusQueueAdapter_CreateReceiver_ShouldReturnSameInstanceForSameQueueId()
    {
        // Arrange
        var loggerFactory = NullLoggerFactory.Instance;
        var serviceBusOptions = new ServiceBusOptions();
        var queueNames = new List<string> { "test-queue-0" };
        var streamQueueMapper = new HashRingBasedPartitionedStreamQueueMapper(queueNames, "test-provider");

        var optionsMonitor = Substitute.For<IOptionsMonitor<ServiceBusOptions>>();
        var clientFactory = new ServiceBusClientFactory(optionsMonitor);

        var adapter = new ServiceBusQueueAdapter(
            "test-provider",
            serviceBusOptions,
            streamQueueMapper,
            clientFactory,
            loggerFactory);

        // Get the actual queue ID from the mapper
        var queueId = streamQueueMapper.GetAllQueues().First();

        // Act
        var receiver1 = adapter.CreateReceiver(queueId);
        var receiver2 = adapter.CreateReceiver(queueId);

        // Assert
        Assert.Same(receiver1, receiver2);
    }

    [Fact]
    public async Task ServiceBusQueueAdapter_QueueMessageBatchAsync_WithNonNullToken_ShouldThrow()
    {
        // Arrange
        var loggerFactory = NullLoggerFactory.Instance;
        var serviceBusOptions = new ServiceBusOptions();
        var queueNames = new List<string> { "test-queue-0" };
        var streamQueueMapper = new HashRingBasedPartitionedStreamQueueMapper(queueNames, "test-provider");

        var optionsMonitor = Substitute.For<IOptionsMonitor<ServiceBusOptions>>();
        var clientFactory = new ServiceBusClientFactory(optionsMonitor);

        var adapter = new ServiceBusQueueAdapter(
            "test-provider",
            serviceBusOptions,
            streamQueueMapper,
            clientFactory,
            loggerFactory);

        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = new[] { "test-event" };
        var token = new EventSequenceTokenV2(1);
        var requestContext = new Dictionary<string, object>();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() =>
            adapter.QueueMessageBatchAsync(streamId, events, token, requestContext));
    }

    [Fact]
    public async Task ServiceBusQueueAdapter_DisposeAsync_ShouldNotThrow()
    {
        // Arrange
        var loggerFactory = NullLoggerFactory.Instance;
        var serviceBusOptions = new ServiceBusOptions();
        var queueNames = new List<string> { "test-queue-0" };
        var streamQueueMapper = new HashRingBasedPartitionedStreamQueueMapper(queueNames, "test-provider");

        var optionsMonitor = Substitute.For<IOptionsMonitor<ServiceBusOptions>>();
        var clientFactory = new ServiceBusClientFactory(optionsMonitor);

        var adapter = new ServiceBusQueueAdapter(
            "test-provider",
            serviceBusOptions,
            streamQueueMapper,
            clientFactory,
            loggerFactory);

        // Act & Assert
        await adapter.DisposeAsync(); // Should not throw
    }
}

[TestCategory("BVT")]
[TestCategory("ServiceBus")]
[TestCategory("BatchContainer")]
public class ServiceBusBatchContainerTests
{
    [Fact]
    public void ServiceBusBatchContainer_Constructor_ShouldSetProperties()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = new List<object> { "event1", "event2" };
        var requestContext = new Dictionary<string, object> { { "key1", "value1" } };

        // Act
        var container = new ServiceBusBatchContainer(streamId, events, requestContext);

        // Assert
        Assert.Equal(streamId, container.StreamId);
        Assert.NotNull(container.SequenceToken);
    }

    [Fact]
    public void ServiceBusBatchContainer_Constructor_WithNullEvents_ShouldThrow()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-key");
        var requestContext = new Dictionary<string, object>();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new ServiceBusBatchContainer(streamId, null!, requestContext));
    }

    [Fact]
    public void ServiceBusBatchContainer_GetEvents_ShouldReturnFilteredEvents()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = new List<object> { "string-event", 42, "another-string" };
        var requestContext = new Dictionary<string, object>();
        var container = new ServiceBusBatchContainer(streamId, events, requestContext);

        // Act
        var stringEvents = container.GetEvents<string>().ToList();
        var intEvents = container.GetEvents<int>().ToList();

        // Assert
        Assert.Equal(2, stringEvents.Count);
        Assert.Equal("string-event", stringEvents[0].Item1);
        Assert.Equal("another-string", stringEvents[1].Item1);
        Assert.Single(intEvents);
        Assert.Equal(42, intEvents[0].Item1);
    }

    [Fact]
    public void ServiceBusBatchContainer_ImportRequestContext_WithEmptyContext_ShouldReturnFalse()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = new List<object> { "event" };
        var requestContext = new Dictionary<string, object>();
        var container = new ServiceBusBatchContainer(streamId, events, requestContext);

        // Act
        var result = container.ImportRequestContext();

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void ServiceBusBatchContainer_ImportRequestContext_WithContext_ShouldReturnTrue()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = new List<object> { "event" };
        var requestContext = new Dictionary<string, object> { { "key1", "value1" } };
        var container = new ServiceBusBatchContainer(streamId, events, requestContext);

        // Act
        var result = container.ImportRequestContext();

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void ServiceBusBatchContainer_ToString_ShouldContainStreamIdAndEventCount()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = new List<object> { "event1", "event2" };
        var requestContext = new Dictionary<string, object>();
        var container = new ServiceBusBatchContainer(streamId, events, requestContext);

        // Act
        var result = container.ToString();

        // Assert
        Assert.Contains("ServiceBusBatchContainer", result);
        Assert.Contains("#Items=2", result);
    }
}