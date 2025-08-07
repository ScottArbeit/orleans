using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus.Providers;

namespace Orleans.Hosting;

/// <summary>
/// Extension methods for configuring Azure Service Bus streaming on Orleans clients.
/// </summary>
public static class ClientBuilderAzureServiceBusExtensions
{
    /// <summary>
    /// Adds Azure Service Bus streaming to the client with action-based configuration.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configure">The configuration delegate.</param>
    /// <returns>The client builder.</returns>
    public static IClientBuilder AddAzureServiceBusStreaming(
        this IClientBuilder builder,
        string name,
        Action<AzureServiceBusOptions> configure)
    {
        if (builder is null)
            throw new ArgumentNullException(nameof(builder));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Stream provider name cannot be null or whitespace.", nameof(name));
        if (configure is null)
            throw new ArgumentNullException(nameof(configure));

        return builder.AddPersistentStreams(
            name,
            AzureServiceBusAdapterFactory.Create,
            streamConfigurator =>
            {
                streamConfigurator.Configure<AzureServiceBusOptions>(optionsBuilder => optionsBuilder.Configure(configure));
                streamConfigurator.ConfigureComponent(AzureServiceBusOptionsValidator.Create);
            });
    }

    /// <summary>
    /// Adds Azure Service Bus streaming to the client with IConfiguration-based configuration.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configuration">The configuration section to bind.</param>
    /// <returns>The client builder.</returns>
    public static IClientBuilder AddAzureServiceBusStreaming(
        this IClientBuilder builder,
        string name,
        IConfiguration configuration)
    {
        if (builder is null)
            throw new ArgumentNullException(nameof(builder));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Stream provider name cannot be null or whitespace.", nameof(name));
        if (configuration is null)
            throw new ArgumentNullException(nameof(configuration));

        return builder.AddPersistentStreams(
            name,
            AzureServiceBusAdapterFactory.Create,
            streamConfigurator =>
            {
                streamConfigurator.Configure<AzureServiceBusOptions>(optionsBuilder =>
                    optionsBuilder.Bind(configuration));
                streamConfigurator.ConfigureComponent(AzureServiceBusOptionsValidator.Create);
            });
    }

    /// <summary>
    /// Adds Azure Service Bus queue streaming to the client.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="connectionString">The Azure Service Bus connection string.</param>
    /// <param name="queueName">The queue name.</param>
    /// <returns>The client builder.</returns>
    public static IClientBuilder AddAzureServiceBusQueueStreaming(
        this IClientBuilder builder,
        string name,
        string connectionString,
        string queueName)
    {
        if (builder is null)
            throw new ArgumentNullException(nameof(builder));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Stream provider name cannot be null or whitespace.", nameof(name));
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be null or whitespace.", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(queueName))
            throw new ArgumentException("Queue name cannot be null or whitespace.", nameof(queueName));

        return builder.AddAzureServiceBusStreaming(name, options =>
        {
            options.ConnectionString = connectionString;
            options.EntityName = queueName;
            options.EntityMode = ServiceBusEntityMode.Queue;
        });
    }

    /// <summary>
    /// Adds Azure Service Bus topic streaming to the client.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="connectionString">The Azure Service Bus connection string.</param>
    /// <param name="topicName">The topic name.</param>
    /// <param name="subscriptionName">The subscription name.</param>
    /// <returns>The client builder.</returns>
    public static IClientBuilder AddAzureServiceBusTopicStreaming(
        this IClientBuilder builder,
        string name,
        string connectionString,
        string topicName,
        string subscriptionName)
    {
        if (builder is null)
            throw new ArgumentNullException(nameof(builder));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Stream provider name cannot be null or whitespace.", nameof(name));
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be null or whitespace.", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentException("Topic name cannot be null or whitespace.", nameof(topicName));
        if (string.IsNullOrWhiteSpace(subscriptionName))
            throw new ArgumentException("Subscription name cannot be null or whitespace.", nameof(subscriptionName));

        return builder.AddAzureServiceBusStreaming(name, options =>
        {
            options.ConnectionString = connectionString;
            options.EntityName = topicName;
            options.SubscriptionName = subscriptionName;
            options.EntityMode = ServiceBusEntityMode.Topic;
        });
    }
}