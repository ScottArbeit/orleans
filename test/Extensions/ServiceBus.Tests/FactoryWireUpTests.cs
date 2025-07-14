using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Providers.Streams.ServiceBus;
using Orleans.Runtime;
using Orleans.Streaming.ServiceBus;
using Orleans.Streams;
using Xunit;

namespace ServiceBus.Tests;

/// <summary>
/// Tests for ServiceBus stream provider factory wiring and TestCluster integration.
/// </summary>
[TestCategory("ServiceBus"), TestCategory("Streaming")]
public class FactoryWireUpTests
{

    /// <summary>
    /// Tests that the ServiceBusAdapterFactory creates a queue adapter instance.
    /// </summary>
    [Fact]
    public async Task ServiceBusAdapterFactory_CreatesQueueAdapter()
    {
        // Arrange
        var name = "TestServiceBusProvider";
        var serviceBusOptions = new ServiceBusOptions
        {
            EntityType = ServiceBusEntityType.Queue,
            QueueNamePrefix = "test-queue",
            PartitionCount = 4
        };
        serviceBusOptions.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=TestKey;SharedAccessKey=dGVzdGtleXZhbHVlMTIzNDU2Nzg5MA==");
        var cacheOptions = new SimpleQueueCacheOptions();
        var loggerFactory = NullLoggerFactory.Instance;

        var factory = new ServiceBusAdapterFactory(name, serviceBusOptions, cacheOptions, loggerFactory);
        factory.Init();

        // Act
        var adapter = await factory.CreateAdapter();

        // Assert
        Assert.NotNull(adapter);
        Assert.IsType<ServiceBusQueueAdapter>(adapter);
        Assert.Equal(name, adapter.Name);
        Assert.False(adapter.IsRewindable);
        Assert.Equal(StreamProviderDirection.ReadWrite, adapter.Direction);
    }

    /// <summary>
    /// Tests that the ServiceBusAdapterFactory creates a topic adapter instance.
    /// </summary>
    [Fact]
    public async Task ServiceBusAdapterFactory_CreatesTopicAdapter()
    {
        // Arrange
        var name = "TestServiceBusProvider";
        var serviceBusOptions = new ServiceBusOptions
        {
            EntityType = ServiceBusEntityType.Topic,
            EntityName = "test-topic",
            SubscriptionNamePrefix = "test-subscription",
            PartitionCount = 2
        };
        serviceBusOptions.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=TestKey;SharedAccessKey=dGVzdGtleXZhbHVlMTIzNDU2Nzg5MA==");
        var cacheOptions = new SimpleQueueCacheOptions();
        var loggerFactory = NullLoggerFactory.Instance;

        var factory = new ServiceBusAdapterFactory(name, serviceBusOptions, cacheOptions, loggerFactory);
        factory.Init();

        // Act
        var adapter = await factory.CreateAdapter();

        // Assert
        Assert.NotNull(adapter);
        Assert.IsType<ServiceBusTopicAdapter>(adapter);
        Assert.Equal(name, adapter.Name);
        Assert.False(adapter.IsRewindable);
        Assert.Equal(StreamProviderDirection.ReadWrite, adapter.Direction);
    }

    /// <summary>
    /// Tests that the ServiceBusAdapterFactory gets a SimpleQueueAdapterCache instance.
    /// </summary>
    [Fact]
    public void ServiceBusAdapterFactory_GetsQueueAdapterCache()
    {
        // Arrange
        var name = "TestServiceBusProvider";
        var serviceBusOptions = new ServiceBusOptions
        {
            EntityType = ServiceBusEntityType.Queue
        };
        serviceBusOptions.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=TestKey;SharedAccessKey=dGVzdGtleXZhbHVlMTIzNDU2Nzg5MA==");
        var cacheOptions = new SimpleQueueCacheOptions();
        var loggerFactory = NullLoggerFactory.Instance;

        var factory = new ServiceBusAdapterFactory(name, serviceBusOptions, cacheOptions, loggerFactory);

        // Act
        var cache = factory.GetQueueAdapterCache();

        // Assert
        Assert.NotNull(cache);
        Assert.IsType<SimpleQueueAdapterCache>(cache);
    }

    /// <summary>
    /// Tests that the ServiceBusAdapterFactory gets a stream queue mapper instance.
    /// </summary>
    [Fact]
    public void ServiceBusAdapterFactory_GetsStreamQueueMapper()
    {
        // Arrange
        var name = "TestServiceBusProvider";
        var serviceBusOptions = new ServiceBusOptions
        {
            EntityType = ServiceBusEntityType.Queue
        };
        serviceBusOptions.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=TestKey;SharedAccessKey=dGVzdGtleXZhbHVlMTIzNDU2Nzg5MA==");
        var cacheOptions = new SimpleQueueCacheOptions();
        var loggerFactory = NullLoggerFactory.Instance;

        var factory = new ServiceBusAdapterFactory(name, serviceBusOptions, cacheOptions, loggerFactory);

        // Act
        var mapper = factory.GetStreamQueueMapper();

        // Assert
        Assert.NotNull(mapper);
        Assert.IsType<HashRingBasedPartitionedStreamQueueMapper>(mapper);
    }

    /// <summary>
    /// Tests that the ServiceBusAdapterFactory gets a delivery failure handler instance.
    /// </summary>
    [Fact]
    public async Task ServiceBusAdapterFactory_GetsDeliveryFailureHandler()
    {
        // Arrange
        var name = "TestServiceBusProvider";
        var serviceBusOptions = new ServiceBusOptions
        {
            EntityType = ServiceBusEntityType.Queue
        };
        serviceBusOptions.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=TestKey;SharedAccessKey=dGVzdGtleXZhbHVlMTIzNDU2Nzg5MA==");
        var cacheOptions = new SimpleQueueCacheOptions();
        var loggerFactory = NullLoggerFactory.Instance;

        var factory = new ServiceBusAdapterFactory(name, serviceBusOptions, cacheOptions, loggerFactory);
        factory.Init();
        var queueId = QueueId.GetQueueId("test-queue-0", 0, 1);

        // Act
        var handler = await factory.GetDeliveryFailureHandler(queueId);

        // Assert
        Assert.NotNull(handler);
        Assert.IsType<NoOpStreamDeliveryFailureHandler>(handler);
    }

    /// <summary>
    /// Tests that the ServiceBusAdapterFactory constructs via its static factory method.
    /// </summary>
    [Fact]
    public void ServiceBusAdapterFactory_ConstructsViaStaticFactory()
    {
        // Arrange
        var name = "TestServiceBusProvider";
        var services = new ServiceCollection();
        
        // Configure the required services
        services.Configure<ServiceBusOptions>(name, options =>
        {
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=TestKey;SharedAccessKey=dGVzdGtleXZhbHVlMTIzNDU2Nzg5MA==");
            options.EntityType = ServiceBusEntityType.Queue;
            options.QueueNamePrefix = "test-queue";
            options.PartitionCount = 4;
        });
        services.Configure<SimpleQueueCacheOptions>(name, options =>
        {
            options.CacheSize = 1024;
        });
        services.AddSingleton<ServiceBusClientFactory>();
        services.AddLogging();
        
        var serviceProvider = services.BuildServiceProvider();
        
        // Act
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, name);

        // Assert
        Assert.NotNull(factory);
        Assert.IsType<ServiceBusAdapterFactory>(factory);
    }
}