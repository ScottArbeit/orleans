using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus.Providers;

namespace Orleans.Hosting;

/// <summary>
/// Extension methods for configuring Azure Service Bus streaming in IServiceCollection.
/// </summary>
public static class ServiceCollectionAzureServiceBusExtensions
{
    /// <summary>
    /// Adds Azure Service Bus streaming to the service collection with action-based configuration.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configure">The configuration delegate.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddAzureServiceBusStreaming(
        this IServiceCollection services,
        string name,
        Action<AzureServiceBusOptions> configure)
    {
        if (services is null)
            throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Stream provider name cannot be null or whitespace.", nameof(name));
        if (configure is null)
            throw new ArgumentNullException(nameof(configure));

        return services.Configure(name, configure)
            .AddAzureServiceBusStreamingServices(name);
    }

    /// <summary>
    /// Adds Azure Service Bus streaming to the service collection with IConfiguration-based configuration.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configuration">The configuration section to bind.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddAzureServiceBusStreaming(
        this IServiceCollection services,
        string name,
        IConfiguration configuration)
    {
        if (services is null)
            throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Stream provider name cannot be null or whitespace.", nameof(name));
        if (configuration is null)
            throw new ArgumentNullException(nameof(configuration));

        return services.Configure<AzureServiceBusOptions>(name, configuration)
            .AddAzureServiceBusStreamingServices(name);
    }

    /// <summary>
    /// Adds Azure Service Bus queue streaming to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="connectionString">The Azure Service Bus connection string.</param>
    /// <param name="queueName">The queue name.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddAzureServiceBusQueueStreaming(
        this IServiceCollection services,
        string name,
        string connectionString,
        string queueName)
    {
        if (services is null)
            throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Stream provider name cannot be null or whitespace.", nameof(name));
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be null or whitespace.", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(queueName))
            throw new ArgumentException("Queue name cannot be null or whitespace.", nameof(queueName));

        return services.AddAzureServiceBusStreaming(name, options =>
        {
            options.ConnectionString = connectionString;
            options.EntityName = queueName;
            options.EntityMode = ServiceBusEntityMode.Queue;
        });
    }

    /// <summary>
    /// Adds Azure Service Bus topic streaming to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="connectionString">The Azure Service Bus connection string.</param>
    /// <param name="topicName">The topic name.</param>
    /// <param name="subscriptionName">The subscription name.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddAzureServiceBusTopicStreaming(
        this IServiceCollection services,
        string name,
        string connectionString,
        string topicName,
        string subscriptionName)
    {
        if (services is null)
            throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Stream provider name cannot be null or whitespace.", nameof(name));
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be null or whitespace.", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentException("Topic name cannot be null or whitespace.", nameof(topicName));
        if (string.IsNullOrWhiteSpace(subscriptionName))
            throw new ArgumentException("Subscription name cannot be null or whitespace.", nameof(subscriptionName));

        return services.AddAzureServiceBusStreaming(name, options =>
        {
            options.ConnectionString = connectionString;
            options.EntityName = topicName;
            options.SubscriptionName = subscriptionName;
            options.EntityMode = ServiceBusEntityMode.Topic;
        });
    }

    /// <summary>
    /// Adds the core Azure Service Bus streaming services to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <returns>The service collection.</returns>
    private static IServiceCollection AddAzureServiceBusStreamingServices(
        this IServiceCollection services,
        string name)
    {
        // Add validation for the configuration
        services.AddTransient<IConfigurationValidator>(serviceProvider =>
            AzureServiceBusOptionsValidator.Create(serviceProvider, name));

        return services;
    }
}