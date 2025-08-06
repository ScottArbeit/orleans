using System;
using System.Collections.Generic;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus.Providers;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests.Providers;

/// <summary>
/// Unit tests for the ServiceBusEntityMapper helper class.
/// Tests entity mapping, validation, and partition selection logic.
/// </summary>
public class ServiceBusEntityMapperTests
{
    private const string ProviderName = "TestProvider";
    private const string QueueName = "test-queue";
    private const string TopicName = "test-topic";
    private const string SubscriptionName = "test-subscription";

    [Fact]
    public void ForQueue_CreatesValidQueueId()
    {
        // Act
        var queueId = ServiceBusEntityMapper.ForQueue(QueueName, ProviderName);

        // Assert
        Assert.NotEqual(default, queueId);
        var entityName = ServiceBusEntityMapper.GetEntityNameFromQueueId(queueId, ProviderName);
        Assert.Equal(QueueName, entityName);
    }

    [Fact]
    public void ForQueue_WithPartition_CreatesPartitionedQueueId()
    {
        // Act
        var queueId = ServiceBusEntityMapper.ForQueue(QueueName, ProviderName, 2);

        // Assert
        Assert.NotEqual(default, queueId);
        var entityName = ServiceBusEntityMapper.GetEntityNameFromQueueId(queueId, ProviderName);
        Assert.Equal($"{QueueName}-2", entityName);
    }

    [Fact]
    public void ForTopicSubscription_CreatesValidQueueId()
    {
        // Act
        var queueId = ServiceBusEntityMapper.ForTopicSubscription(TopicName, SubscriptionName, ProviderName);

        // Assert
        Assert.NotEqual(default, queueId);
        var entityName = ServiceBusEntityMapper.GetEntityNameFromQueueId(queueId, ProviderName);
        var subscriptionName = ServiceBusEntityMapper.GetSubscriptionNameFromQueueId(queueId, ProviderName);
        
        Assert.Equal(TopicName, entityName);
        Assert.Equal(SubscriptionName, subscriptionName);
    }

    [Fact]
    public void ForTopicSubscription_WithPartition_CreatesPartitionedQueueId()
    {
        // Act
        var queueId = ServiceBusEntityMapper.ForTopicSubscription(TopicName, SubscriptionName, ProviderName, 3);

        // Assert
        Assert.NotEqual(default, queueId);
        var entityName = ServiceBusEntityMapper.GetEntityNameFromQueueId(queueId, ProviderName);
        var subscriptionName = ServiceBusEntityMapper.GetSubscriptionNameFromQueueId(queueId, ProviderName);
        
        Assert.Equal($"{TopicName}-3", entityName);
        Assert.Equal(SubscriptionName, subscriptionName);
    }

    [Fact]
    public void GetSubscriptionNameFromQueueId_QueueMode_ReturnsNull()
    {
        // Arrange
        var queueId = ServiceBusEntityMapper.ForQueue(QueueName, ProviderName);

        // Act
        var subscriptionName = ServiceBusEntityMapper.GetSubscriptionNameFromQueueId(queueId, ProviderName);

        // Assert
        Assert.Null(subscriptionName);
    }

    [Fact]
    public void CreatePartitionedQueues_Queue_CreatesCorrectNumber()
    {
        // Act
        var queueIds = ServiceBusEntityMapper.CreatePartitionedQueues(
            QueueName, null, ProviderName, 4, ServiceBusEntityMode.Queue);

        // Assert
        Assert.Equal(4, queueIds.Length);
        
        for (int i = 0; i < 4; i++)
        {
            var entityName = ServiceBusEntityMapper.GetEntityNameFromQueueId(queueIds[i], ProviderName);
            Assert.Equal($"{QueueName}-{i}", entityName);
        }
    }

    [Fact]
    public void CreatePartitionedQueues_Topic_CreatesCorrectNumber()
    {
        // Act
        var queueIds = ServiceBusEntityMapper.CreatePartitionedQueues(
            TopicName, SubscriptionName, ProviderName, 3, ServiceBusEntityMode.Topic);

        // Assert
        Assert.Equal(3, queueIds.Length);
        
        for (int i = 0; i < 3; i++)
        {
            var entityName = ServiceBusEntityMapper.GetEntityNameFromQueueId(queueIds[i], ProviderName);
            var subscriptionName = ServiceBusEntityMapper.GetSubscriptionNameFromQueueId(queueIds[i], ProviderName);
            
            Assert.Equal($"{TopicName}-{i}", entityName);
            Assert.Equal(SubscriptionName, subscriptionName);
        }
    }

    [Fact]
    public void SelectPartitionForStream_SinglePartition_ReturnsZero()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", Guid.NewGuid());

        // Act
        var partition = ServiceBusEntityMapper.SelectPartitionForStream(streamId, 1);

        // Assert
        Assert.Equal(0, partition);
    }

    [Fact]
    public void SelectPartitionForStream_MultiplePartitions_ConsistentMapping()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", Guid.NewGuid());

        // Act
        var partition1 = ServiceBusEntityMapper.SelectPartitionForStream(streamId, 5);
        var partition2 = ServiceBusEntityMapper.SelectPartitionForStream(streamId, 5);

        // Assert
        Assert.Equal(partition1, partition2); // Same stream should always get same partition
        Assert.True(partition1 >= 0 && partition1 < 5); // Partition should be in valid range
    }

    [Fact]
    public void SelectPartitionForNamespace_MappedNamespace_ReturnsCorrectPartition()
    {
        // Arrange
        var namespaceMap = new Dictionary<string, int>
        {
            ["namespace1"] = 1,
            ["namespace2"] = 3
        };
        var streamId = StreamId.Create("namespace1", Guid.NewGuid());

        // Act
        var partition = ServiceBusEntityMapper.SelectPartitionForNamespace(streamId, namespaceMap, 0);

        // Assert
        Assert.Equal(1, partition);
    }

    [Fact]
    public void SelectPartitionForNamespace_UnmappedNamespace_ReturnsDefault()
    {
        // Arrange
        var namespaceMap = new Dictionary<string, int>
        {
            ["namespace1"] = 1,
            ["namespace2"] = 3
        };
        var streamId = StreamId.Create("unmapped-namespace", Guid.NewGuid());

        // Act
        var partition = ServiceBusEntityMapper.SelectPartitionForNamespace(streamId, namespaceMap, 5);

        // Assert
        Assert.Equal(5, partition);
    }

    [Theory]
    [InlineData("valid-name")]
    [InlineData("valid_name")]
    [InlineData("ValidName123")]
    [InlineData("a")]
    public void ValidateEntityName_ValidNames_DoesNotThrow(string entityName)
    {
        // Act & Assert
        var exception = Record.Exception(() => ServiceBusEntityMapper.ValidateEntityName(entityName));
        Assert.Null(exception);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void ValidateEntityName_InvalidNames_ThrowsException(string entityName)
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => ServiceBusEntityMapper.ValidateEntityName(entityName));
    }

    [Theory]
    [InlineData("name/with/slash")]
    [InlineData("name\\with\\backslash")]
    [InlineData("name?with?question")]
    [InlineData("name#with#hash")]
    public void ValidateEntityName_InvalidCharacters_ThrowsException(string entityName)
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => ServiceBusEntityMapper.ValidateEntityName(entityName));
    }

    [Fact]
    public void ValidateEntityName_TooLong_ThrowsException()
    {
        // Arrange
        var longName = new string('a', ServiceBusEntityMapper.MaxEntityNameLength + 1);

        // Act & Assert
        Assert.Throws<ArgumentException>(() => ServiceBusEntityMapper.ValidateEntityName(longName));
    }

    [Fact]
    public void ForQueue_NullParameters_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => ServiceBusEntityMapper.ForQueue(null!, ProviderName));
        Assert.Throws<ArgumentNullException>(() => ServiceBusEntityMapper.ForQueue(QueueName, null!));
    }

    [Fact]
    public void ForTopicSubscription_NullParameters_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => ServiceBusEntityMapper.ForTopicSubscription(null!, SubscriptionName, ProviderName));
        Assert.Throws<ArgumentNullException>(() => ServiceBusEntityMapper.ForTopicSubscription(TopicName, null!, ProviderName));
        Assert.Throws<ArgumentNullException>(() => ServiceBusEntityMapper.ForTopicSubscription(TopicName, SubscriptionName, null!));
    }

    [Fact]
    public void CreatePartitionedQueues_InvalidPartitionCount_ThrowsException()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            ServiceBusEntityMapper.CreatePartitionedQueues(QueueName, null, ProviderName, 0, ServiceBusEntityMode.Queue));
        Assert.Throws<ArgumentException>(() => 
            ServiceBusEntityMapper.CreatePartitionedQueues(QueueName, null, ProviderName, -1, ServiceBusEntityMode.Queue));
    }
}