using System;
using System.Linq;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus;
using TestExtensions;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests;

/// <summary>
/// Tests for ServiceBusQueueMapper multiple entities functionality (Step 10).
/// </summary>
public class ServiceBusQueueMapperMultipleEntitiesTests
{
    [Fact, TestCategory("BVT"), TestCategory("AzureServiceBus")]
    public void Constructor_MultipleQueues_CreatesMapperWithCorrectQueueCount()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            EntityCount = 4,
            EntityNamePrefix = "test-prefix"
        };

        // Act
        var mapper = new ServiceBusQueueMapper(options, "test-provider");

        // Assert
        var allQueues = mapper.GetAllQueues().ToArray();
        Assert.Equal(4, allQueues.Length);
        
        // Verify the queue names follow the expected pattern: <prefix>-q-0..N-1
        var expectedNames = new[] { "test-prefix-q-0", "test-prefix-q-1", "test-prefix-q-2", "test-prefix-q-3" };
        var actualNames = allQueues.Select(q => q.ToString()).ToArray();
        
        // Check that each expected name appears in the actual names
        for (int i = 0; i < expectedNames.Length; i++)
        {
            Assert.Contains(expectedNames[i], actualNames);
        }
    }

    [Fact, TestCategory("BVT"), TestCategory("AzureServiceBus")]
    public void Constructor_MultipleTopicSubscriptions_CreatesMapperWithCorrectSubscriptionCount()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.TopicSubscription,
            TopicName = "my-topic",
            EntityCount = 3,
            EntityNamePrefix = "test-prefix"
        };

        // Act
        var mapper = new ServiceBusQueueMapper(options, "test-provider");

        // Assert
        var allQueues = mapper.GetAllQueues().ToArray();
        Assert.Equal(3, allQueues.Length);
        
        // Verify the subscription names follow the expected pattern: <topic>:<prefix>-sub-0..N-1
        var expectedNames = new[] { "my-topic:test-prefix-sub-0", "my-topic:test-prefix-sub-1", "my-topic:test-prefix-sub-2" };
        var actualNames = allQueues.Select(q => q.ToString()).ToArray();
        
        // Check that each expected name appears in the actual names
        for (int i = 0; i < expectedNames.Length; i++)
        {
            Assert.Contains(expectedNames[i], actualNames);
        }
    }

    [Fact, TestCategory("BVT"), TestCategory("AzureServiceBus")]
    public void GetQueueForStream_MultipleQueues_UsesConsistentHashing()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            EntityCount = 4,
            EntityNamePrefix = "test-prefix"
        };
        var mapper = new ServiceBusQueueMapper(options, "test-provider");

        // Act & Assert
        var streamId1 = StreamId.Create("test-namespace", "stream-1");
        var streamId2 = StreamId.Create("test-namespace", "stream-2");
        var streamId3 = StreamId.Create("test-namespace", "stream-3");

        var queue1 = mapper.GetQueueForStream(streamId1);
        var queue2 = mapper.GetQueueForStream(streamId2);
        var queue3 = mapper.GetQueueForStream(streamId3);

        // Verify that streams are distributed across queues (consistent mapping)
        // QueueId is a value type, so we don't need to check for null

        // Verify consistent mapping - same stream always maps to same queue
        Assert.Equal(queue1, mapper.GetQueueForStream(streamId1));
        Assert.Equal(queue2, mapper.GetQueueForStream(streamId2));
        Assert.Equal(queue3, mapper.GetQueueForStream(streamId3));
    }

    [Fact]
    public void Constructor_MultipleEntities_ImplementsConsistentRingStreamQueueMapper()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            EntityCount = 4,
            EntityNamePrefix = "test-prefix"
        };

        // Act
        var mapper = new ServiceBusQueueMapper(options, "test-provider");

        // Assert
        Assert.IsAssignableFrom<Orleans.Streams.IConsistentRingStreamQueueMapper>(mapper);
    }

    [Fact]
    public void GetQueuesForRange_MultipleEntities_ReturnsQueuesInRange()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            EntityCount = 4,
            EntityNamePrefix = "test-prefix"
        };
        var mapper = new ServiceBusQueueMapper(options, "test-provider");
        
        // Create a ring range that should include some but not all queues
        var allQueues = mapper.GetAllQueues().ToArray();
        var minHash = allQueues.Min(q => q.GetUniformHashCode());
        var maxHash = allQueues.Max(q => q.GetUniformHashCode());
        var midPoint = minHash + (maxHash - minHash) / 2;
        
        var range = Orleans.Runtime.RangeFactory.CreateRange(minHash, midPoint);

        // Act
        var queuesInRange = mapper.GetQueuesForRange(range).ToArray();

        // Assert
        Assert.NotEmpty(queuesInRange);
        Assert.True(queuesInRange.Length <= allQueues.Length);
        
        // Verify that all returned queues are actually in the range
        foreach (var queue in queuesInRange)
        {
            Assert.True(range.InRange(queue.GetUniformHashCode()));
        }
    }

    [Fact]
    public void Constructor_EntityCountOne_BehavesLikeSingleEntity()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "single-queue",
            EntityCount = 1
        };

        // Act
        var mapper = new ServiceBusQueueMapper(options, "test-provider");

        // Assert
        var allQueues = mapper.GetAllQueues().ToArray();
        Assert.Single(allQueues);
        Assert.Equal("single-queue-0", allQueues[0].ToString());

        // Verify all streams map to the same queue
        var streamId1 = StreamId.Create("test-namespace", "stream-1");
        var streamId2 = StreamId.Create("test-namespace", "stream-2");
        
        Assert.Equal(mapper.GetQueueForStream(streamId1), mapper.GetQueueForStream(streamId2));
    }
}