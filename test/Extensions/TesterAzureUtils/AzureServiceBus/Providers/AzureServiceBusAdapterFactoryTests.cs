using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus.Providers;
using Xunit;

namespace TesterAzureUtils.AzureServiceBus.Providers;

/// <summary>
/// Unit tests for the Azure Service Bus adapter factory.
/// Tests the core factory functionality, configuration validation, and component creation.
/// </summary>
[Trait("Category", "AzureServiceBus")]
[Trait("Category", "Unit")]
public class AzureServiceBusAdapterFactoryTests
{
    private readonly IServiceProvider _serviceProvider;
    private readonly AzureServiceBusOptions _validOptions;

    public AzureServiceBusAdapterFactoryTests()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddLogging();
        _serviceProvider = services.BuildServiceProvider();

        _validOptions = new AzureServiceBusOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test",
            EntityName = "test-queue",
            EntityMode = ServiceBusEntityMode.Queue,
            BatchSize = 32,
            MaxConcurrentCalls = 1
        };
    }

    [Fact]
    public void Constructor_WithValidConfiguration_SucceedsAndValidatesConfig()
    {
        // Arrange
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };
        var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();

        // Act & Assert - Should not throw
        var factory = new AzureServiceBusAdapterFactory(
            "test-provider",
            _validOptions,
            cacheOptions,
            _serviceProvider,
            loggerFactory);

        Assert.NotNull(factory);
    }

    [Fact]
    public void Constructor_WithNullName_ThrowsArgumentNullException()
    {
        // Arrange
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };
        var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AzureServiceBusAdapterFactory(
            null!,
            _validOptions,
            cacheOptions,
            _serviceProvider,
            loggerFactory));
    }

    [Fact]
    public void Constructor_WithEmptyConnectionString_ThrowsArgumentException()
    {
        // Arrange
        var invalidOptions = new AzureServiceBusOptions
        {
            ConnectionString = "",
            EntityName = "test-queue",
            EntityMode = ServiceBusEntityMode.Queue
        };
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };
        var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();

        // Act & Assert
        Assert.Throws<ArgumentException>(() => new AzureServiceBusAdapterFactory(
            "test-provider",
            invalidOptions,
            cacheOptions,
            _serviceProvider,
            loggerFactory));
    }

    [Fact]
    public void Constructor_WithEmptyEntityName_ThrowsArgumentException()
    {
        // Arrange
        var invalidOptions = new AzureServiceBusOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test",
            EntityName = "",
            EntityMode = ServiceBusEntityMode.Queue
        };
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };
        var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();

        // Act & Assert
        Assert.Throws<ArgumentException>(() => new AzureServiceBusAdapterFactory(
            "test-provider",
            invalidOptions,
            cacheOptions,
            _serviceProvider,
            loggerFactory));
    }

    [Fact]
    public void Constructor_WithTopicModeButNoSubscription_ThrowsArgumentException()
    {
        // Arrange
        var invalidOptions = new AzureServiceBusOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test",
            EntityName = "test-topic",
            EntityMode = ServiceBusEntityMode.Topic,
            SubscriptionName = "" // Missing subscription name for topic mode
        };
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };
        var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();

        // Act & Assert
        Assert.Throws<ArgumentException>(() => new AzureServiceBusAdapterFactory(
            "test-provider",
            invalidOptions,
            cacheOptions,
            _serviceProvider,
            loggerFactory));
    }

    [Fact]
    public async Task CreateAdapter_WithValidConfiguration_ReturnsAdapter()
    {
        // Arrange
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };
        var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();
        var factory = new AzureServiceBusAdapterFactory(
            "test-provider",
            _validOptions,
            cacheOptions,
            _serviceProvider,
            loggerFactory);

        // Act
        var adapter = await factory.CreateAdapter();

        // Assert
        Assert.NotNull(adapter);
        Assert.Equal("test-provider", adapter.Name);
        Assert.True(adapter.IsRewindable);
    }

    [Fact]
    public void GetStreamQueueMapper_ReturnsSameInstance()
    {
        // Arrange
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };
        var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();
        var factory = new AzureServiceBusAdapterFactory(
            "test-provider",
            _validOptions,
            cacheOptions,
            _serviceProvider,
            loggerFactory);

        // Act
        var mapper1 = factory.GetStreamQueueMapper();
        var mapper2 = factory.GetStreamQueueMapper();

        // Assert
        Assert.NotNull(mapper1);
        Assert.Same(mapper1, mapper2); // Should return the same instance
        Assert.NotEmpty(mapper1.GetAllQueues());
    }

    [Fact]
    public void GetQueueAdapterCache_ReturnsSameInstance()
    {
        // Arrange
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };
        var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();
        var factory = new AzureServiceBusAdapterFactory(
            "test-provider",
            _validOptions,
            cacheOptions,
            _serviceProvider,
            loggerFactory);

        // Act
        var cache1 = factory.GetQueueAdapterCache();
        var cache2 = factory.GetQueueAdapterCache();

        // Assert
        Assert.NotNull(cache1);
        Assert.Same(cache1, cache2); // Should return the same instance
    }

    [Fact]
    public async Task GetDeliveryFailureHandler_ReturnsHandler()
    {
        // Arrange
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };
        var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();
        var factory = new AzureServiceBusAdapterFactory(
            "test-provider",
            _validOptions,
            cacheOptions,
            _serviceProvider,
            loggerFactory);

        var mapper = factory.GetStreamQueueMapper();
        var queueId = mapper.GetAllQueues().First();

        // Act
        var handler = await factory.GetDeliveryFailureHandler(queueId);

        // Assert
        Assert.NotNull(handler);
        Assert.False(handler.ShouldFaultSubscriptionOnError); // Service Bus handler should not fault by default
    }

    [Fact]
    public void StaticCreate_WithValidServices_ReturnsFactory()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddLogging();
        services.Configure<AzureServiceBusOptions>("test-provider", options =>
        {
            options.ConnectionString = _validOptions.ConnectionString;
            options.EntityName = _validOptions.EntityName;
            options.EntityMode = _validOptions.EntityMode;
        });
        services.Configure<SimpleQueueCacheOptions>("test-provider", options =>
        {
            options.CacheSize = 1024;
        });
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var factory = AzureServiceBusAdapterFactory.Create(serviceProvider, "test-provider");

        // Assert
        Assert.NotNull(factory);
    }

    [Fact]
    public async Task Dispose_CleansUpResources()
    {
        // Arrange
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };
        var loggerFactory = _serviceProvider.GetRequiredService<ILoggerFactory>();
        var factory = new AzureServiceBusAdapterFactory(
            "test-provider",
            _validOptions,
            cacheOptions,
            _serviceProvider,
            loggerFactory);

        // Act
        factory.Dispose();

        // Assert - After disposal, operations should throw ObjectDisposedException
        Assert.Throws<ObjectDisposedException>(() => factory.GetStreamQueueMapper());
        Assert.Throws<ObjectDisposedException>(() => factory.GetQueueAdapterCache());
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await factory.CreateAdapter());
    }
}
