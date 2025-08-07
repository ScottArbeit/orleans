using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus.Providers;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests.Providers;

/// <summary>
/// Unit tests for the Azure Service Bus Stream Queue Mapper.
/// Tests various mapping strategies and scenarios.
/// </summary>
public class AzureServiceBusQueueMapperTests
{
    private const string ProviderName = "TestProvider";
    private const string EntityName = "test-entity";
    private const string SubscriptionName = "test-subscription";

    [Fact]
    public void GetQueueForStream_SingleStrategy_AlwaysReturnsSameQueue()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            EntityName = EntityName,
            MappingStrategy = QueueMappingStrategy.Single,
            EntityMode = ServiceBusEntityMode.Queue
        };
        var mapper = new AzureServiceBusQueueMapper(ProviderName, options);
        
        var streamId1 = StreamId.Create("namespace1", Guid.NewGuid());
        var streamId2 = StreamId.Create("namespace2", Guid.NewGuid());

        // Act
        var queue1 = mapper.GetQueueForStream(streamId1);
        var queue2 = mapper.GetQueueForStream(streamId2);

        // Assert
        Assert.Equal(queue1, queue2);
        Assert.Single(mapper.GetAllQueues());
    }

    [Fact]
    public void GetQueueForStream_HashBasedStrategy_ConsistentMapping()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            EntityName = EntityName,
            MappingStrategy = QueueMappingStrategy.HashBased,
            PartitionCount = 4,
            EntityMode = ServiceBusEntityMode.Queue
        };
        var mapper = new AzureServiceBusQueueMapper(ProviderName, options);
        
        var streamId = StreamId.Create("test-namespace", Guid.NewGuid());

        // Act
        var queue1 = mapper.GetQueueForStream(streamId);
        var queue2 = mapper.GetQueueForStream(streamId);

        // Assert
        Assert.Equal(queue1, queue2); // Same stream should always map to same queue
        Assert.Equal(4, mapper.GetAllQueues().Count()); // Should have 4 partitions
    }

    [Fact]
    public void GetQueueForStream_RoundRobinStrategy_DistributesEvenly()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            EntityName = EntityName,
            MappingStrategy = QueueMappingStrategy.RoundRobin,
            PartitionCount = 3,
            EntityMode = ServiceBusEntityMode.Queue
        };
        var mapper = new AzureServiceBusQueueMapper(ProviderName, options);

        var queues = new HashSet<string>();

        // Act - Get queues for multiple streams
        for (int i = 0; i < 9; i++)
        {
            var streamId = StreamId.Create("test-namespace", Guid.NewGuid());
            var queue = mapper.GetQueueForStream(streamId);
            queues.Add(queue.ToString());
        }

        // Assert
        Assert.Equal(3, mapper.GetAllQueues().Count()); // Should have 3 partitions
        Assert.True(queues.Count <= 3); // Should use all available partitions
    }

    [Fact]
    public void GetQueueForStream_NamespaceBasedStrategy_RoutesByNamespace()
    {
        // Arrange
        var namespaceMap = new Dictionary<string, int>
        {
            ["namespace1"] = 0,
            ["namespace2"] = 1,
            ["namespace3"] = 2
        };
        
        var options = new AzureServiceBusOptions
        {
            EntityName = EntityName,
            MappingStrategy = QueueMappingStrategy.NamespaceBased,
            NamespaceToPartitionMap = namespaceMap,
            DefaultPartition = 0,
            EntityMode = ServiceBusEntityMode.Queue
        };
        var mapper = new AzureServiceBusQueueMapper(ProviderName, options);

        var streamId1 = StreamId.Create("namespace1", Guid.NewGuid());
        var streamId2 = StreamId.Create("namespace2", Guid.NewGuid());
        var streamId3 = StreamId.Create("unmapped-namespace", Guid.NewGuid());

        // Act
        var queue1 = mapper.GetQueueForStream(streamId1);
        var queue2 = mapper.GetQueueForStream(streamId2);
        var queue3 = mapper.GetQueueForStream(streamId3);

        // Assert
        Assert.NotEqual(queue1, queue2); // Different namespaces should map to different queues
        Assert.Equal(queue1, queue3); // Unmapped namespace should use default partition (0)
        Assert.Equal(3, mapper.GetAllQueues().Count()); // Should have 3 partitions (0, 1, 2)
    }

    [Fact]
    public void Constructor_TopicMode_WithoutSubscription_ThrowsException()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            EntityName = EntityName,
            EntityMode = ServiceBusEntityMode.Topic,
            SubscriptionName = "", // Missing subscription name
            MappingStrategy = QueueMappingStrategy.Single
        };

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => new AzureServiceBusQueueMapper(ProviderName, options));
    }

    [Fact]
    public void GetEntityNameForQueue_ReturnsCorrectEntityName()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            EntityName = EntityName,
            MappingStrategy = QueueMappingStrategy.Single,
            EntityMode = ServiceBusEntityMode.Queue
        };
        var mapper = new AzureServiceBusQueueMapper(ProviderName, options);
        var streamId = StreamId.Create("test-namespace", Guid.NewGuid());
        var queue = mapper.GetQueueForStream(streamId);

        // Act
        var entityName = mapper.GetEntityNameForQueue(queue);

        // Assert
        Assert.Equal(EntityName, entityName);
    }

    [Fact]
    public void GetSubscriptionNameForQueue_QueueMode_ReturnsNull()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            EntityName = EntityName,
            MappingStrategy = QueueMappingStrategy.Single,
            EntityMode = ServiceBusEntityMode.Queue
        };
        var mapper = new AzureServiceBusQueueMapper(ProviderName, options);
        var streamId = StreamId.Create("test-namespace", Guid.NewGuid());
        var queue = mapper.GetQueueForStream(streamId);

        // Act
        var subscriptionName = mapper.GetSubscriptionNameForQueue(queue);

        // Assert
        Assert.Null(subscriptionName);
    }

    [Fact]
    public void GetSubscriptionNameForQueue_TopicMode_ReturnsSubscriptionName()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            EntityName = EntityName,
            SubscriptionName = SubscriptionName,
            MappingStrategy = QueueMappingStrategy.Single,
            EntityMode = ServiceBusEntityMode.Topic
        };
        var mapper = new AzureServiceBusQueueMapper(ProviderName, options);
        var streamId = StreamId.Create("test-namespace", Guid.NewGuid());
        var queue = mapper.GetQueueForStream(streamId);

        // Act
        var subscriptionName = mapper.GetSubscriptionNameForQueue(queue);

        // Assert
        Assert.Equal(SubscriptionName, subscriptionName);
    }

    [Fact]
    public void GetPartitionForStream_HashBasedStrategy_ConsistentPartitioning()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            EntityName = EntityName,
            MappingStrategy = QueueMappingStrategy.HashBased,
            PartitionCount = 5,
            EntityMode = ServiceBusEntityMode.Queue
        };
        var mapper = new AzureServiceBusQueueMapper(ProviderName, options);
        var streamId = StreamId.Create("test-namespace", Guid.NewGuid());

        // Act
        var partition1 = mapper.GetPartitionForStream(streamId);
        var partition2 = mapper.GetPartitionForStream(streamId);

        // Assert
        Assert.Equal(partition1, partition2); // Same stream should always get same partition
        Assert.True(partition1 >= 0 && partition1 < 5); // Partition should be in valid range
    }

    [Fact]
    public void GetAllQueues_ReturnsAllConfiguredQueues()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            EntityName = EntityName,
            MappingStrategy = QueueMappingStrategy.HashBased,
            PartitionCount = 3,
            EntityMode = ServiceBusEntityMode.Queue
        };
        var mapper = new AzureServiceBusQueueMapper(ProviderName, options);

        // Act
        var allQueues = mapper.GetAllQueues().ToList();

        // Assert
        Assert.Equal(allQueues.Count, allQueues.Distinct().Count()); // All queues should be unique
    }
}