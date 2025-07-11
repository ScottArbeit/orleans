using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using System;

namespace Orleans.Hosting
{
    /// <summary>
    /// Extension methods for configuring Azure Service Bus streaming providers on the client builder.
    /// </summary>
    public static class ClientBuilderExtensions
    {
        /// <summary>
        /// Configure client to use Azure Service Bus persistent stream provider.
        /// </summary>
        /// <param name="builder">The client builder.</param>
        /// <param name="name">The name of the provider.</param>
        /// <param name="configureOptions">The configuration delegate.</param>
        /// <returns>The client builder.</returns>
        /// <example>
        /// <code>
        /// clientBuilder.AddAzureServiceBusStreaming("MyProvider", options =>
        /// {
        ///     options.ConfigureServiceBusClient("connectionString");
        ///     options.TopologyType = ServiceBusTopologyType.Queue;
        ///     options.QueueName = "myqueue";
        /// });
        /// </code>
        /// </example>
        public static IClientBuilder AddAzureServiceBusStreaming(
            this IClientBuilder builder,
            string name,
            Action<AzureServiceBusOptions> configureOptions)
        {
            if (builder is null)
            {
                throw new ArgumentNullException(nameof(builder));
            }

            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("A non-null, non-empty value must be provided.", nameof(name));
            }

            if (configureOptions is null)
            {
                throw new ArgumentNullException(nameof(configureOptions));
            }

            return builder.ConfigureServices(services =>
            {
                services.Configure<AzureServiceBusOptions>(name, configureOptions);
                services.ConfigureNamedOptionForLogging<AzureServiceBusOptions>(name);
                services.AddSingleton<IConfigurationValidator>(provider =>
                    AzureServiceBusOptionsValidator.Create(provider, name));
            });
        }

        /// <summary>
        /// Configure client to use Azure Service Bus persistent stream provider with default name.
        /// </summary>
        /// <param name="builder">The client builder.</param>
        /// <param name="configureOptions">The configuration delegate.</param>
        /// <returns>The client builder.</returns>
        /// <example>
        /// <code>
        /// clientBuilder.AddAzureServiceBusStreaming(options =>
        /// {
        ///     options.ConfigureServiceBusClient("connectionString");
        ///     options.TopologyType = ServiceBusTopologyType.Topic;
        ///     options.TopicName = "mytopic";
        ///     options.SubscriptionName = "mysubscription";
        /// });
        /// </code>
        /// </example>
        public static IClientBuilder AddAzureServiceBusStreaming(
            this IClientBuilder builder,
            Action<AzureServiceBusOptions> configureOptions)
        {
            return builder.AddAzureServiceBusStreaming("AzureServiceBus", configureOptions);
        }
    }
}