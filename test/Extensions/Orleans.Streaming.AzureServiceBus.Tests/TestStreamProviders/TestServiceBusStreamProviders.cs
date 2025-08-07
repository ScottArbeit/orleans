using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streaming.AzureServiceBus.Providers;

namespace Orleans.Streaming.AzureServiceBus.Tests.TestStreamProviders;

/// <summary>
/// Test stream provider for Azure Service Bus Queue scenarios.
/// Configures the provider for deterministic testing with queue-based messaging.
/// </summary>
public class TestServiceBusQueueStreamProvider : AzureServiceBusAdapterFactory
{
    public TestServiceBusQueueStreamProvider(
        string name,
        AzureServiceBusOptions options,
        SimpleQueueCacheOptions cacheOptions,
        IServiceProvider serviceProvider,
        ILoggerFactory loggerFactory)
        : base(name, options, cacheOptions, serviceProvider, loggerFactory)
    {
    }

    /// <summary>
    /// Creates a test-configured Service Bus queue stream provider.
    /// </summary>
    public static TestServiceBusQueueStreamProvider Create(
        IServiceProvider serviceProvider,
        string providerName = "TestSBQueueProvider",
        string connectionString = "ServiceBus=UseDevelopmentEmulator=true;",
        string queueName = "test-queue")
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = connectionString,
            EntityName = queueName,
            EntityMode = ServiceBusEntityMode.Queue,
            BatchSize = 10,
            MaxConcurrentCalls = 1,
            OperationTimeout = TimeSpan.FromSeconds(10)
        };

        var cacheOptions = new SimpleQueueCacheOptions
        {
            CacheSize = 1024,
            NumQueues = 4
        };

        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
        
        return new TestServiceBusQueueStreamProvider(
            providerName,
            options,
            cacheOptions,
            serviceProvider,
            loggerFactory);
    }
}

/// <summary>
/// Test stream provider for Azure Service Bus Topic/Subscription scenarios.
/// Configures the provider for deterministic testing with topic-based messaging.
/// </summary>
public class TestServiceBusTopicStreamProvider : AzureServiceBusAdapterFactory
{
    public TestServiceBusTopicStreamProvider(
        string name,
        AzureServiceBusOptions options,
        SimpleQueueCacheOptions cacheOptions,
        IServiceProvider serviceProvider,
        ILoggerFactory loggerFactory)
        : base(name, options, cacheOptions, serviceProvider, loggerFactory)
    {
    }

    /// <summary>
    /// Creates a test-configured Service Bus topic stream provider.
    /// </summary>
    public static TestServiceBusTopicStreamProvider Create(
        IServiceProvider serviceProvider,
        string providerName = "TestSBTopicProvider",
        string connectionString = "ServiceBus=UseDevelopmentEmulator=true;",
        string topicName = "test-topic",
        string subscriptionName = "test-subscription")
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = connectionString,
            EntityName = topicName,
            EntityMode = ServiceBusEntityMode.Topic,
            BatchSize = 10,
            MaxConcurrentCalls = 1,
            OperationTimeout = TimeSpan.FromSeconds(10),
            SubscriptionName = subscriptionName
        };

        var cacheOptions = new SimpleQueueCacheOptions
        {
            CacheSize = 1024,
            NumQueues = 4
        };

        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
        
        return new TestServiceBusTopicStreamProvider(
            providerName,
            options,
            cacheOptions,
            serviceProvider,
            loggerFactory);
    }
}