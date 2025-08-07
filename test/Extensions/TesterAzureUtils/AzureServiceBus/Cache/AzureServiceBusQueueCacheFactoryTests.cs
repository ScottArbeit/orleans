using System;
using Microsoft.Extensions.Logging;
using Orleans.Streaming.AzureServiceBus.Cache;
using Orleans.Streaming.AzureServiceBus.Providers;
using Xunit;

namespace Tester.AzureUtils.AzureServiceBus.Cache;

/// <summary>
/// Tests for Azure Service Bus queue cache factory functionality.
/// </summary>
public class AzureServiceBusQueueCacheFactoryTests
{
    private readonly ILoggerFactory _loggerFactory;
    private const string TestProviderName = "test-provider";

    public AzureServiceBusQueueCacheFactoryTests()
    {
        _loggerFactory = new LoggerFactory();
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesFactory()
    {
        // Arrange & Act
        var factory = new AzureServiceBusQueueCacheFactory(
            TestProviderName,
            _loggerFactory);

        // Assert
        Assert.NotNull(factory);
    }

    [Fact]
    public void CreateQueueCacheAdapter_ReturnsValidAdapter()
    {
        // Arrange
        var factory = new AzureServiceBusQueueCacheFactory(
            TestProviderName,
            _loggerFactory);

        // Act
        var adapter = factory.CreateQueueCacheAdapter();

        // Assert
        Assert.NotNull(adapter);
        Assert.IsType<AzureServiceBusQueueCache>(adapter);
    }

    [Fact]
    public void CreatePressureMonitor_ReturnsValidMonitor()
    {
        // Arrange
        var factory = new AzureServiceBusQueueCacheFactory(
            TestProviderName,
            _loggerFactory);

        // Act
        var monitor = factory.CreatePressureMonitor();

        // Assert
        Assert.NotNull(monitor);
        Assert.IsType<ServiceBusCachePressureMonitor>(monitor);
    }

    [Fact]
    public void CreateEvictionPolicy_ReturnsValidPolicy()
    {
        // Arrange
        var factory = new AzureServiceBusQueueCacheFactory(
            TestProviderName,
            _loggerFactory);

        // Act
        var policy = factory.CreateEvictionPolicy();

        // Assert
        Assert.NotNull(policy);
        Assert.IsType<CacheEvictionPolicy>(policy);
    }

    [Fact]
    public void CreateQueueCacheAdapter_WithCustomOptions_UsesOptions()
    {
        // Arrange
        var customOptions = new AzureServiceBusCacheOptions
        {
            MaxCacheSize = 2048,
            MessageTTL = TimeSpan.FromMinutes(15),
            EvictionStrategy = CacheEvictionStrategy.FIFO,
            PressureThreshold = 0.7
        };

        var factory = new AzureServiceBusQueueCacheFactory(
            TestProviderName,
            _loggerFactory,
            customOptions);

        // Act
        var adapter = factory.CreateQueueCacheAdapter();
        var policy = factory.CreateEvictionPolicy();

        // Assert
        Assert.NotNull(adapter);
        Assert.Equal(2048, policy.MaxCacheSize);
        Assert.Equal(TimeSpan.FromMinutes(15), policy.MessageTTL);
        Assert.Equal(CacheEvictionStrategy.FIFO, policy.Strategy);
        Assert.Equal(0.7, policy.PressureThreshold);
    }

    [Fact]
    public void Constructor_WithNullProviderName_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new AzureServiceBusQueueCacheFactory(null!, _loggerFactory));
    }

    [Fact]
    public void Constructor_WithNullLoggerFactory_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new AzureServiceBusQueueCacheFactory(TestProviderName, null!));
    }
}