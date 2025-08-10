using System;
using System.Linq;
using Orleans.Streaming.AzureServiceBus;
using Orleans.Streams;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests;

/// <summary>
/// Tests for ServiceBusEntityNamer utility class.
/// </summary>
public class ServiceBusEntityNamerTests
{
    [Fact]
    public void GetEntityName_QueueEntityKind_ReturnsQueueName()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };

        // Act
        var result = ServiceBusEntityNamer.GetEntityName(options);

        // Assert
        Assert.Equal("test-queue", result);
    }

    [Fact]
    public void GetEntityName_TopicSubscriptionEntityKind_ReturnsTopicColonSubscription()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.TopicSubscription,
            TopicName = "test-topic",
            SubscriptionName = "test-subscription"
        };

        // Act
        var result = ServiceBusEntityNamer.GetEntityName(options);

        // Assert
        Assert.Equal("test-topic:test-subscription", result);
    }

    [Fact]
    public void GetEntityName_NullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => ServiceBusEntityNamer.GetEntityName(null!));
    }

    [Fact]
    public void GetEntityName_QueueEntityKind_NullQueueName_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = null!
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => ServiceBusEntityNamer.GetEntityName(options));
        Assert.Contains("QueueName must be configured", exception.Message);
    }

    [Fact]
    public void GetEntityName_QueueEntityKind_EmptyQueueName_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = ""
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => ServiceBusEntityNamer.GetEntityName(options));
        Assert.Contains("QueueName must be configured", exception.Message);
    }

    [Fact]
    public void GetEntityName_TopicSubscriptionEntityKind_NullTopicName_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.TopicSubscription,
            TopicName = null!,
            SubscriptionName = "test-subscription"
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => ServiceBusEntityNamer.GetEntityName(options));
        Assert.Contains("TopicName must be configured", exception.Message);
    }

    [Fact]
    public void GetEntityName_TopicSubscriptionEntityKind_NullSubscriptionName_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.TopicSubscription,
            TopicName = "test-topic",
            SubscriptionName = null!
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => ServiceBusEntityNamer.GetEntityName(options));
        Assert.Contains("SubscriptionName must be configured", exception.Message);
    }

    [Fact]
    public void GetEntityName_UnsupportedEntityKind_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = (EntityKind)999 // Invalid enum value
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => ServiceBusEntityNamer.GetEntityName(options));
        Assert.Contains("Unsupported entity kind", exception.Message);
    }
}

/// <summary>
/// Tests for ServiceBusQueueMapper implementation.
/// </summary>
public class ServiceBusQueueMapperTests
{
    [Fact]
    public void Constructor_ValidQueueOptions_CreatesMapper()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };

        // Act
        var mapper = new ServiceBusQueueMapper(options, "test-provider");

        // Assert
        Assert.NotNull(mapper);
        Assert.Equal("test-queue-0", mapper.QueueId.ToString());
    }

    [Fact]
    public void Constructor_ValidTopicSubscriptionOptions_CreatesMapper()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.TopicSubscription,
            TopicName = "test-topic",
            SubscriptionName = "test-subscription"
        };

        // Act
        var mapper = new ServiceBusQueueMapper(options, "test-provider");

        // Assert
        Assert.NotNull(mapper);
        Assert.Equal("test-topic:test-subscription-0", mapper.QueueId.ToString());
    }

    [Fact]
    public void Constructor_NullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new ServiceBusQueueMapper(null!, "test-provider"));
    }

    [Fact]
    public void Constructor_NullProviderName_ThrowsArgumentNullException()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new ServiceBusQueueMapper(options, null!));
    }

    [Fact]
    public void Constructor_EmptyProviderName_ThrowsArgumentException()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => new ServiceBusQueueMapper(options, ""));
    }

    [Fact]
    public void GetAllQueues_ReturnsExactlyOneQueue()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };
        var mapper = new ServiceBusQueueMapper(options, "test-provider");

        // Act
        var queues = mapper.GetAllQueues().ToArray();

        // Assert
        Assert.Single(queues);
        Assert.Equal("test-queue-0", queues[0].ToString());
    }

    [Fact]
    public void GetQueueForStream_DifferentStreamIds_ReturnsSameQueue()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };
        var mapper = new ServiceBusQueueMapper(options, "test-provider");
        var streamId1 = StreamId.Create("namespace1", "key1");
        var streamId2 = StreamId.Create("namespace2", "key2");

        // Act
        var queue1 = mapper.GetQueueForStream(streamId1);
        var queue2 = mapper.GetQueueForStream(streamId2);

        // Assert
        Assert.Equal(queue1, queue2);
        Assert.Equal("test-queue-0", queue1.ToString());
    }

    [Fact]
    public void GetQueueForStream_MultipleCalls_ReturnsSameInstanceDeterministically()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };
        var mapper = new ServiceBusQueueMapper(options, "test-provider");
        var streamId = StreamId.Create("namespace", "key");

        // Act
        var queue1 = mapper.GetQueueForStream(streamId);
        var queue2 = mapper.GetQueueForStream(streamId);

        // Assert
        Assert.Equal(queue1, queue2);
    }

    [Fact]
    public void QueueId_ReturnsConfiguredQueueId()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.TopicSubscription,
            TopicName = "my-topic",
            SubscriptionName = "my-sub"
        };
        var mapper = new ServiceBusQueueMapper(options, "test-provider");

        // Act
        var queueId = mapper.QueueId;

        // Assert
        Assert.Equal("my-topic:my-sub-0", queueId.ToString());
    }
}