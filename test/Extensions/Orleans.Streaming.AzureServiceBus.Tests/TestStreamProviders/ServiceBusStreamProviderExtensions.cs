using Orleans.Configuration;
using Orleans.Hosting;

namespace Orleans.Streaming.AzureServiceBus.Tests.TestStreamProviders;

/// <summary>
/// Extension methods for configuring Service Bus stream providers in tests.
/// </summary>
public static class ServiceBusStreamProviderExtensions
{
    /// <summary>
    /// Configures a Service Bus queue stream provider with test defaults.
    /// </summary>
    public static ISiloBuilder AddServiceBusQueueStreams(
        this ISiloBuilder builder,
        string name,
        Action<AzureServiceBusOptions> configure)
    {
        return builder.AddAzureServiceBusStreaming(name, configure);
    }

    /// <summary>
    /// Configures a Service Bus topic stream provider with test defaults.
    /// </summary>
    public static ISiloBuilder AddServiceBusTopicStreams(
        this ISiloBuilder builder,
        string name,
        Action<AzureServiceBusOptions> configure)
    {
        return builder.AddAzureServiceBusStreaming(name, configure);
    }

    /// <summary>
    /// Configures a Service Bus queue stream provider for clients with test defaults.
    /// </summary>
    public static IClientBuilder AddServiceBusQueueStreams(
        this IClientBuilder builder,
        string name,
        Action<AzureServiceBusOptions> configure)
    {
        return builder.AddAzureServiceBusStreaming(name, configure);
    }

    /// <summary>
    /// Configures a Service Bus topic stream provider for clients with test defaults.
    /// </summary>
    public static IClientBuilder AddServiceBusTopicStreams(
        this IClientBuilder builder,
        string name,
        Action<AzureServiceBusOptions> configure)
    {
        return builder.AddAzureServiceBusStreaming(name, configure);
    }

    /// <summary>
    /// Configures test defaults for Azure Service Bus options.
    /// </summary>
    public static void ConfigureTestDefaults(this AzureServiceBusOptions options, string connectionString)
    {
        options.ConnectionString = connectionString;
        options.BatchSize = 10;
        options.MaxConcurrentCalls = 1;
        options.OperationTimeout = TimeSpan.FromSeconds(30);
    }

    /// <summary>
    /// Configures test defaults for Service Bus queue options.
    /// </summary>
    public static void ConfigureQueueTestDefaults(
        this AzureServiceBusOptions options, 
        string connectionString,
        string queueName = "test-queue")
    {
        options.ConfigureTestDefaults(connectionString);
        options.EntityName = queueName;
        options.EntityMode = ServiceBusEntityMode.Queue;
    }

    /// <summary>
    /// Configures test defaults for Service Bus topic options.
    /// </summary>
    public static void ConfigureTopicTestDefaults(
        this AzureServiceBusOptions options,
        string connectionString,
        string topicName = "test-topic",
        string subscriptionName = "test-subscription")
    {
        options.ConfigureTestDefaults(connectionString);
        options.EntityName = topicName;
        options.EntityMode = ServiceBusEntityMode.Topic;
        options.SubscriptionName = subscriptionName;
    }
}