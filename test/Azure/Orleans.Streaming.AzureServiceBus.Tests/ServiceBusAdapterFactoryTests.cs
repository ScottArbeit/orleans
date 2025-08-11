namespace Orleans.Streaming.AzureServiceBus.Tests;

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Streaming.AzureServiceBus;
using Orleans.Streaming.AzureServiceBus.Configuration;
using Orleans.Streams;
using Xunit;

/// <summary>
/// Tests for ServiceBusAdapterFactory integration.
/// </summary>
public class ServiceBusAdapterFactoryTests
{
    [Fact]
    public void Create_ValidQueueConfiguration_CreatesFactoryWithCorrectQueueId()
    {
        // Arrange
        var services = new ServiceCollection();
        services.Configure<ServiceBusStreamOptions>("test-provider", options =>
        {
            options.EntityKind = EntityKind.Queue;
            options.QueueName = "test-queue";
        });
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider");

        // Assert
        Assert.NotNull(factory);
        var queueMapper = factory.GetStreamQueueMapper();
        Assert.IsType<ServiceBusQueueMapper>(queueMapper);
        
        // Verify the mapper returns the expected queue
        var streamId = StreamId.Create("test-namespace", "test-key");
        var queueId = queueMapper.GetQueueForStream(streamId);
        Assert.Equal("test-queue-0", queueId.ToString());
    }

    [Fact]
    public void Create_ValidTopicSubscriptionConfiguration_CreatesFactoryWithCorrectQueueId()
    {
        // Arrange
        var services = new ServiceCollection();
        services.Configure<ServiceBusStreamOptions>("test-provider", options =>
        {
            options.EntityKind = EntityKind.TopicSubscription;
            options.TopicName = "test-topic";
            options.SubscriptionName = "test-subscription";
        });
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider");

        // Assert
        Assert.NotNull(factory);
        var queueMapper = factory.GetStreamQueueMapper();
        Assert.IsType<ServiceBusQueueMapper>(queueMapper);
        
        // Verify the mapper returns the expected queue
        var streamId = StreamId.Create("test-namespace", "test-key");
        var queueId = queueMapper.GetQueueForStream(streamId);
        Assert.Equal("test-topic:test-subscription-0", queueId.ToString());
    }

    [Fact]
    public void Create_NullServices_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => ServiceBusAdapterFactory.Create(null!, "test-provider"));
    }

    [Fact]
    public void GetStreamQueueMapper_AllStreamsMapToSameQueue()
    {
        // Arrange
        var services = new ServiceCollection();
        services.Configure<ServiceBusStreamOptions>("test-provider", options =>
        {
            options.EntityKind = EntityKind.Queue;
            options.QueueName = "shared-queue";
        });
        var serviceProvider = services.BuildServiceProvider();
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider");
        var queueMapper = factory.GetStreamQueueMapper();

        // Act
        var streamId1 = StreamId.Create("namespace1", "key1");
        var streamId2 = StreamId.Create("namespace2", "key2");
        var streamId3 = StreamId.Create("namespace3", "key3");

        var queueId1 = queueMapper.GetQueueForStream(streamId1);
        var queueId2 = queueMapper.GetQueueForStream(streamId2);
        var queueId3 = queueMapper.GetQueueForStream(streamId3);

        // Assert
        Assert.Equal(queueId1, queueId2);
        Assert.Equal(queueId2, queueId3);
        Assert.Equal("shared-queue-0", queueId1.ToString());
    }

    [Fact]
    public async Task CreateAdapter_ThrowsNotImplementedException()
    {
        // Arrange
        var services = new ServiceCollection();
        services.Configure<ServiceBusStreamOptions>("test-provider", options =>
        {
            options.EntityKind = EntityKind.Queue;
            options.QueueName = "test-queue";
        });
        var serviceProvider = services.BuildServiceProvider();
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider");

        // Act & Assert
        await Assert.ThrowsAsync<NotImplementedException>(() => factory.CreateAdapter());
    }

    [Fact]
    public void GetQueueAdapterCache_ThrowsNotImplementedException()
    {
        // Arrange
        var services = new ServiceCollection();
        services.Configure<ServiceBusStreamOptions>("test-provider", options =>
        {
            options.EntityKind = EntityKind.Queue;
            options.QueueName = "test-queue";
        });
        var serviceProvider = services.BuildServiceProvider();
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider");

        // Act & Assert
        Assert.Throws<NotImplementedException>(() => factory.GetQueueAdapterCache());
    }

    [Fact]
    public async Task GetDeliveryFailureHandler_ThrowsNotImplementedException()
    {
        // Arrange
        var services = new ServiceCollection();
        services.Configure<ServiceBusStreamOptions>("test-provider", options =>
        {
            options.EntityKind = EntityKind.Queue;
            options.QueueName = "test-queue";
        });
        var serviceProvider = services.BuildServiceProvider();
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider");
        var queueId = QueueId.GetQueueId("test", 0, 0);

        // Act & Assert
        await Assert.ThrowsAsync<NotImplementedException>(() => factory.GetDeliveryFailureHandler(queueId));
    }
}
