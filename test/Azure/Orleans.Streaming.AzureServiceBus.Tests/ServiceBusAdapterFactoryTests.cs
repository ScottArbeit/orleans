namespace Orleans.Streaming.AzureServiceBus.Tests;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus;
using Orleans.Streaming.AzureServiceBus.Configuration;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Orleans.Streams;
using Xunit;

/// <summary>
/// Tests for ServiceBusAdapterFactory integration.
/// </summary>
[Collection(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusAdapterFactoryTests
{
    private readonly ServiceBusEmulatorFixture _fixture;

    public ServiceBusAdapterFactoryTests(ServiceBusEmulatorFixture fixture)
    {
        _fixture = fixture;
    }
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
    public async Task CreateAdapter_ValidConfiguration_CreatesServiceBusAdapter()
    {
        // Arrange
        var services = new ServiceCollection();
        services.Configure<ServiceBusStreamOptions>("test-provider", options =>
        {
            options.EntityKind = EntityKind.Queue;
            options.QueueName = ServiceBusEmulatorFixture.QueueName;
            options.ConnectionString = _fixture.ServiceBusConnectionString;
        });
        
        // Add Orleans serialization services
        services.AddSerializer();
        
        var serviceProvider = services.BuildServiceProvider();
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider");

        // Act
        var adapter = await factory.CreateAdapter();

        // Assert
        Assert.NotNull(adapter);
        Assert.IsType<ServiceBusAdapter>(adapter);
        Assert.Equal("test-provider", adapter.Name);
        Assert.Equal(StreamProviderDirection.WriteOnly, adapter.Direction);
        Assert.False(adapter.IsRewindable);
        
        // Verify the adapter can actually send a message through the emulator
        var uniqueKey = $"factory-test-{Guid.NewGuid():N}";
        var streamId = StreamId.Create("test-namespace", uniqueKey);
        var events = new[] { "test-event" };
        var requestContext = new Dictionary<string, object>();

        // This should complete without error, confirming real Service Bus connectivity
        await adapter.QueueMessageBatchAsync(streamId, events, null, requestContext);
        
        // Clean up: drain the message we sent
        await using var client = _fixture.CreateServiceBusClient();
        await using var receiver = client.CreateReceiver(ServiceBusEmulatorFixture.QueueName);
        
        while (true)
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(100));
            if (message is null) break;
            await receiver.CompleteMessageAsync(message);
        }
        
        // Cleanup
        if (adapter is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    [Fact]
    public void GetQueueAdapterCache_ReturnsSimpleQueueAdapterCache()
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

        // Act
        var cache = factory.GetQueueAdapterCache();

        // Assert
        Assert.NotNull(cache);
        Assert.IsType<SimpleQueueAdapterCache>(cache);
    }

    [Fact]
    public async Task GetDeliveryFailureHandler_ReturnsServiceBusStreamDeliveryFailureHandler()
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

        // Act
        var failureHandler = await factory.GetDeliveryFailureHandler(queueId);

        // Assert
        Assert.NotNull(failureHandler);
        Assert.False(failureHandler.ShouldFaultSubsriptionOnError);
    }

    [Fact]
    public void Cache_IsNonRewindable()
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

        // Act
        var cache = factory.GetQueueAdapterCache();
        var queueId = QueueId.GetQueueId("test", 0, 0);
        var queueCache = cache.CreateQueueCache(queueId);

        // Assert
        Assert.NotNull(queueCache);
        Assert.IsType<SimpleQueueCache>(queueCache);
        
        // SimpleQueueCache is non-rewindable by design - it doesn't support rewind operations
        // This test confirms that we're using the correct cache type for Service Bus
    }
}
