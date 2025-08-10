using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Streaming.AzureServiceBus;
using Orleans.Streaming.AzureServiceBus.Configuration;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests;

/// <summary>
/// Tests for Service Bus stream configuration and extension methods.
/// </summary>
public class ServiceBusStreamConfigurationTests
{
    [Fact]
    public void SiloBuilderExtensions_AddServiceBusStreams_WithOptions_RegistersCorrectly()
    {
        // Arrange
        var services = new ServiceCollection();
        var builder = new TestSiloBuilder(services);

        // Act
        builder.AddServiceBusStreams("test-provider", options =>
        {
            options.ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";
            options.QueueName = "test-queue";
        });

        // Assert
        var serviceProvider = services.BuildServiceProvider();
        
        // Verify that the options are registered
        var optionsSnapshot = serviceProvider.GetService<Microsoft.Extensions.Options.IOptionsSnapshot<ServiceBusStreamOptions>>();
        Assert.NotNull(optionsSnapshot);
        
        var options = optionsSnapshot.Get("test-provider");
        Assert.Equal("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test", options.ConnectionString);
        Assert.Equal("test-queue", options.QueueName);
    }

    [Fact]
    public void SiloBuilderExtensions_AddServiceBusStreams_WithConfigurator_RegistersCorrectly()
    {
        // Arrange
        var services = new ServiceCollection();
        var builder = new TestSiloBuilder(services);

        // Act
        builder.AddServiceBusStreams("test-provider", configurator =>
        {
            configurator.ConfigureServiceBus(ob => ob.Configure(options =>
            {
                options.ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";
                options.EntityKind = EntityKind.TopicSubscription;
                options.TopicName = "test-topic";
                options.SubscriptionName = "test-subscription";
            }));
            configurator.ConfigureCache(2048);
            configurator.ConfigurePartitioning(2);
        });

        // Assert
        var serviceProvider = services.BuildServiceProvider();
        
        // Verify that the options are registered
        var optionsSnapshot = serviceProvider.GetService<Microsoft.Extensions.Options.IOptionsSnapshot<ServiceBusStreamOptions>>();
        Assert.NotNull(optionsSnapshot);
        
        var options = optionsSnapshot.Get("test-provider");
        Assert.Equal(EntityKind.TopicSubscription, options.EntityKind);
        Assert.Equal("test-topic", options.TopicName);
        Assert.Equal("test-subscription", options.SubscriptionName);
    }

    [Fact]
    public void ClientBuilderExtensions_AddServiceBusStreams_WithOptions_RegistersCorrectly()
    {
        // Arrange
        var services = new ServiceCollection();
        var builder = new TestClientBuilder(services);

        // Act
        builder.AddServiceBusStreams("test-provider", options =>
        {
            options.ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";
            options.QueueName = "client-queue";
        });

        // Assert
        var serviceProvider = services.BuildServiceProvider();
        
        // Verify that the options are registered
        var optionsSnapshot = serviceProvider.GetService<Microsoft.Extensions.Options.IOptionsSnapshot<ServiceBusStreamOptions>>();
        Assert.NotNull(optionsSnapshot);
        
        var options = optionsSnapshot.Get("test-provider");
        Assert.Equal("client-queue", options.QueueName);
    }

    [Fact]
    public void ServiceBusStreamOptions_DefaultValues_AreCorrect()
    {
        // Arrange & Act
        var options = new ServiceBusStreamOptions();

        // Assert
        Assert.Equal(string.Empty, options.ConnectionString);
        Assert.Equal(string.Empty, options.FullyQualifiedNamespace);
        Assert.Null(options.Credential);
        Assert.Equal(EntityKind.Queue, options.EntityKind);
        Assert.Equal("orleans-stream", options.QueueName);
        Assert.Equal(string.Empty, options.TopicName);
        Assert.Equal(string.Empty, options.SubscriptionName);
        Assert.True(options.AutoCreateEntities);
        Assert.Equal(1, options.EntityCount);
        Assert.Equal(string.Empty, options.EntityNamePrefix);
        Assert.False(options.AllowRewind);

        // Publisher defaults
        Assert.Equal(100, options.Publisher.BatchSize);
        Assert.Equal(TimeSpan.FromDays(14), options.Publisher.MessageTimeToLive);
        Assert.Equal(SessionIdStrategy.None, options.Publisher.SessionIdStrategy);
        Assert.Equal("orleans_", options.Publisher.PropertiesPrefix);

        // Receiver defaults
        Assert.Equal(0, options.Receiver.PrefetchCount);
        Assert.Equal(32, options.Receiver.ReceiveBatchSize);
        Assert.Equal(1, options.Receiver.MaxConcurrentHandlers);
        Assert.True(options.Receiver.LockAutoRenew);
        Assert.Equal(TimeSpan.FromMinutes(5), options.Receiver.LockRenewalDuration);
        Assert.Equal(10, options.Receiver.MaxDeliveryCount);

        // Cache defaults
        Assert.Equal(4096, options.Cache.MaxCacheSize);
        Assert.Equal(TimeSpan.FromMinutes(10), options.Cache.CacheEvictionAge);
        Assert.Equal(0.7, options.Cache.CachePressureSoft);
        Assert.Equal(0.9, options.Cache.CachePressureHard);

        // Dead letter defaults
        Assert.True(options.DeadLetterHandling.LogDeadLetters);
        Assert.True(options.DeadLetterHandling.SurfaceMetrics);
        Assert.Equal(string.Empty, options.DeadLetterHandling.ForwardAddress);
    }

    [Fact]
    public void EnvironmentVariableOverrideConventions_Documentation()
    {
        // This test documents the environment variable override conventions for tests
        // Environment variables should follow the pattern:
        // Orleans__Streaming__ServiceBus__{ProviderName}__{PropertyPath}
        
        // Examples:
        // Orleans__Streaming__ServiceBus__MyProvider__ConnectionString
        // Orleans__Streaming__ServiceBus__MyProvider__EntityKind
        // Orleans__Streaming__ServiceBus__MyProvider__Publisher__BatchSize
        // Orleans__Streaming__ServiceBus__MyProvider__Receiver__MaxConcurrentHandlers
        // Orleans__Streaming__ServiceBus__MyProvider__Cache__MaxCacheSize
        
        // For nested configuration objects, use double underscores to separate levels
        
        Assert.True(true); // This test is purely for documentation
    }
}

/// <summary>
/// Test implementation of ISiloBuilder for unit testing.
/// </summary>
internal class TestSiloBuilder : ISiloBuilder
{
    public TestSiloBuilder(IServiceCollection services)
    {
        Services = services;
        Configuration = new ConfigurationBuilder().Build();
    }

    public IServiceCollection Services { get; }

    public IConfiguration Configuration { get; }

    public ISiloBuilder ConfigureServices(Action<IServiceCollection> configureServices)
    {
        configureServices(Services);
        return this;
    }

    public IDictionary<object, object> Properties => new Dictionary<object, object>();

    // Other ISiloBuilder methods not needed for testing
    public ISiloBuilder ConfigureAppConfiguration(Action<HostBuilderContext, IConfigurationBuilder> configureDelegate) => this;
    public ISiloBuilder ConfigureServices(Action<HostBuilderContext, IServiceCollection> configureDelegate) => this;
    public ISiloBuilder UseEnvironment(string environment) => this;
    public ISiloBuilder UseServiceProviderFactory<TContainerBuilder>(IServiceProviderFactory<TContainerBuilder> factory) where TContainerBuilder : notnull => this;
    public ISiloBuilder UseServiceProviderFactory<TContainerBuilder>(Func<HostBuilderContext, IServiceProviderFactory<TContainerBuilder>> factory) where TContainerBuilder : notnull => this;
    public ISiloBuilder ConfigureContainer<TContainerBuilder>(Action<HostBuilderContext, TContainerBuilder> configureDelegate) => this;
}

/// <summary>
/// Test implementation of IClientBuilder for unit testing.
/// </summary>
internal class TestClientBuilder : IClientBuilder
{
    public TestClientBuilder(IServiceCollection services)
    {
        Services = services;
        Configuration = new ConfigurationBuilder().Build();
    }

    public IServiceCollection Services { get; }

    public IConfiguration Configuration { get; }

    public IClientBuilder ConfigureServices(Action<IServiceCollection> configureServices)
    {
        configureServices(Services);
        return this;
    }

    public IDictionary<object, object> Properties => new Dictionary<object, object>();

    // Other IClientBuilder methods not needed for testing
    public IClientBuilder ConfigureAppConfiguration(Action<IConfigurationBuilder> configureDelegate) => this;
    public IClientBuilder ConfigureServices(Action<HostBuilderContext, IServiceCollection> configureDelegate) => this;
    public IClientBuilder UseEnvironment(string environment) => this;
    public IClientBuilder UseServiceProviderFactory<TContainerBuilder>(IServiceProviderFactory<TContainerBuilder> factory) where TContainerBuilder : notnull => this;
    public IClientBuilder UseServiceProviderFactory<TContainerBuilder>(Func<HostBuilderContext, IServiceProviderFactory<TContainerBuilder>> factory) where TContainerBuilder : notnull => this;
    public IClientBuilder ConfigureContainer<TContainerBuilder>(Action<HostBuilderContext, TContainerBuilder> configureDelegate) => this;
}