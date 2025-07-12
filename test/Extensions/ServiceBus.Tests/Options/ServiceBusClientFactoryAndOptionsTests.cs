using System;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Streaming.ServiceBus;
using Xunit;

namespace ServiceBus.Tests.Options;

/// <summary>
/// Unit tests for ServiceBus client factory and options validation.
/// Tests validate configuration defaults, validation logic, and client caching behavior.
/// </summary>
[TestCategory("BVT")]
[TestCategory("ServiceBus")]
public class ServiceBusClientFactoryAndOptionsTests
{
    /// <summary>
    /// Validates that ServiceBusOptions has the expected default values.
    /// </summary>
    [Fact]
    public void Options_Defaults()
    {
        var options = new ServiceBusOptions();

        Assert.Equal(ServiceBusEntityType.Queue, options.EntityType);
        Assert.Equal(8, options.PartitionCount);
        Assert.Equal(TimeSpan.FromSeconds(5), options.MaxWaitTime);
        Assert.Equal("orleans-stream", options.QueueNamePrefix);
        Assert.Equal("orleans-subscription", options.SubscriptionNamePrefix);
        Assert.Equal(0, options.PrefetchCount);
        Assert.Equal(1, options.ReceiveBatchSize);
        Assert.Equal(Environment.ProcessorCount, options.MaxConcurrentCalls);
        Assert.Equal(10, options.MaxDeliveryAttempts);
        Assert.Null(options.ConnectionString);
        Assert.Null(options.FullyQualifiedNamespace);
        Assert.Null(options.TokenCredential);
        Assert.Null(options.EntityName);
        Assert.Null(options.EntityNames);
        Assert.Null(options.SubscriptionName);
        Assert.Null(options.SubscriptionNames);
        Assert.Null(options.ServiceBusClient);
    }

    /// <summary>
    /// Validates that configuring both connection string and namespace/credential combination
    /// results in proper validation behavior.
    /// </summary>
    [Fact]
    public void Options_InvalidCombination()
    {
        var options = new ServiceBusOptions();
        var connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";
        var fullyQualifiedNamespace = "test.servicebus.windows.net";
        var credential = new DefaultAzureCredential();

        // First configure with connection string
        options.ConfigureServiceBusClient(connectionString);
        Assert.Equal(connectionString, options.ConnectionString);
        Assert.Null(options.FullyQualifiedNamespace);
        Assert.Null(options.TokenCredential);

        // Then configure with namespace and credential - this should overwrite
        options.ConfigureServiceBusClient(fullyQualifiedNamespace, credential);
        Assert.Equal(fullyQualifiedNamespace, options.FullyQualifiedNamespace);
        Assert.Equal(credential, options.TokenCredential);

        // Both configuration methods should work without throwing
        // The CreateClient delegate is internal, so we can't directly test it,
        // but we can verify the configuration doesn't throw
    }

    /// <summary>
    /// Validates that the ServiceBusClientFactory caches clients for the same options configuration.
    /// </summary>
    [Fact]
    public async Task Factory_CachesClient()
    {
        // Arrange
        var services = new ServiceCollection();
        const string optionsName = "test-options";
        const string connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";

        services.Configure<ServiceBusOptions>(optionsName, options =>
        {
            options.ConfigureServiceBusClient(connectionString);
        });
        services.AddOptions<ServiceBusOptions>();

        var serviceProvider = services.BuildServiceProvider();
        var optionsMonitor = serviceProvider.GetRequiredService<IOptionsMonitor<ServiceBusOptions>>();
        var factory = new ServiceBusClientFactory(optionsMonitor);

        try
        {
            // Act - Get client twice with same options name
            var client1 = await factory.GetClientAsync(optionsName);
            var client2 = await factory.GetClientAsync(optionsName);

            // Assert - Should return the same instance
            Assert.Same(client1, client2);
        }
        finally
        {
            await factory.DisposeAsync();
        }
    }

    /// <summary>
    /// Validates that the ServiceBusClientFactory creates separate clients for different namespace configurations.
    /// </summary>
    [Fact]
    public async Task Factory_DifferentNamespace_SeparateClient()
    {
        // Arrange
        var services = new ServiceCollection();
        const string optionsName1 = "test-options-1";
        const string optionsName2 = "test-options-2";
        const string connectionString1 = "Endpoint=sb://namespace1.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";
        const string connectionString2 = "Endpoint=sb://namespace2.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";

        services.Configure<ServiceBusOptions>(optionsName1, options =>
        {
            options.ConfigureServiceBusClient(connectionString1);
        });

        services.Configure<ServiceBusOptions>(optionsName2, options =>
        {
            options.ConfigureServiceBusClient(connectionString2);
        });
        
        services.AddOptions<ServiceBusOptions>();

        var serviceProvider = services.BuildServiceProvider();
        var optionsMonitor = serviceProvider.GetRequiredService<IOptionsMonitor<ServiceBusOptions>>();
        var factory = new ServiceBusClientFactory(optionsMonitor);

        try
        {
            // Act - Get clients for different options configurations
            var client1 = await factory.GetClientAsync(optionsName1);
            var client2 = await factory.GetClientAsync(optionsName2);

            // Assert - Should return different instances
            Assert.NotSame(client1, client2);
        }
        finally
        {
            await factory.DisposeAsync();
        }
    }

    /// <summary>
    /// Validates that the ServiceBusClientFactory throws when disposed.
    /// </summary>
    [Fact]
    public async Task Factory_ThrowsWhenDisposed()
    {
        // Arrange
        var services = new ServiceCollection();
        const string optionsName = "test-options";

        services.Configure<ServiceBusOptions>(optionsName, options =>
        {
            options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
        });
        services.AddOptions<ServiceBusOptions>();

        var serviceProvider = services.BuildServiceProvider();
        var optionsMonitor = serviceProvider.GetRequiredService<IOptionsMonitor<ServiceBusOptions>>();
        var factory = new ServiceBusClientFactory(optionsMonitor);

        // Act
        await factory.DisposeAsync();

        // Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(() => factory.GetClientAsync(optionsName));
    }

    /// <summary>
    /// Validates that the ServiceBusClientFactory throws for null or empty options name.
    /// </summary>
    [Fact]
    public async Task Factory_ThrowsForInvalidOptionsName()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddOptions<ServiceBusOptions>();
        var serviceProvider = services.BuildServiceProvider();
        var optionsMonitor = serviceProvider.GetRequiredService<IOptionsMonitor<ServiceBusOptions>>();
        var factory = new ServiceBusClientFactory(optionsMonitor);

        try
        {
            // Act & Assert - Check null argument
            await Assert.ThrowsAsync<ArgumentException>(() => factory.GetClientAsync(null!));
            await Assert.ThrowsAsync<ArgumentException>(() => factory.GetClientAsync(string.Empty));
            
            // For whitespace strings, the factory will try to get options and fail with InvalidOperationException
            // because no client configuration is found
            await Assert.ThrowsAsync<InvalidOperationException>(() => factory.GetClientAsync("   "));
        }
        finally
        {
            await factory.DisposeAsync();
        }
    }

    /// <summary>
    /// Validates that the ServiceBusClientFactory throws when no client configuration is available.
    /// </summary>
    [Fact]
    public async Task Factory_ThrowsWhenNoClientConfiguration()
    {
        // Arrange
        var services = new ServiceCollection();
        const string optionsName = "test-options";

        // Configure options without any client configuration
        services.Configure<ServiceBusOptions>(optionsName, options => { });
        services.AddOptions<ServiceBusOptions>();

        var serviceProvider = services.BuildServiceProvider();
        var optionsMonitor = serviceProvider.GetRequiredService<IOptionsMonitor<ServiceBusOptions>>();
        var factory = new ServiceBusClientFactory(optionsMonitor);

        try
        {
            // Act & Assert
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => factory.GetClientAsync(optionsName));
            Assert.Contains("No ServiceBus client configuration found", exception.Message);
            Assert.Contains(optionsName, exception.Message);
        }
        finally
        {
            await factory.DisposeAsync();
        }
    }
}