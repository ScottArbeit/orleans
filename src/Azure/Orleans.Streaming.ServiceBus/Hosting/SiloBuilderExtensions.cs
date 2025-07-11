using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using Orleans.Hosting;
using System;

namespace Orleans.Hosting
{
    /// <summary>
    /// Extension methods for configuring Azure Service Bus streaming providers on the silo host builder.
    /// </summary>
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use Azure Service Bus persistent stream provider.
        /// </summary>
        /// <param name="builder">The silo host builder.</param>
        /// <param name="name">The name of the provider.</param>
        /// <param name="configureOptions">The configuration delegate.</param>
        /// <returns>The silo host builder.</returns>
        /// <example>
        /// <code>
        /// hostBuilder.UseOrleans(siloBuilder =>
        /// {
        ///     siloBuilder.AddAzureServiceBusStreaming("MyProvider", options =>
        ///     {
        ///         options.ConfigureServiceBusClient("connectionString");
        ///         options.TopologyType = ServiceBusTopologyType.Queue;
        ///         options.QueueName = "myqueue";
        ///     });
        /// });
        /// </code>
        /// </example>
        public static ISiloBuilder AddAzureServiceBusStreaming(
            this ISiloBuilder builder,
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
        /// Configure silo to use Azure Service Bus persistent stream provider with default name.
        /// </summary>
        /// <param name="builder">The silo host builder.</param>
        /// <param name="configureOptions">The configuration delegate.</param>
        /// <returns>The silo host builder.</returns>
        /// <example>
        /// <code>
        /// hostBuilder.UseOrleans(siloBuilder =>
        /// {
        ///     siloBuilder.AddAzureServiceBusStreaming(options =>
        ///     {
        ///         options.ConfigureServiceBusClient("connectionString");
        ///         options.TopologyType = ServiceBusTopologyType.Topic;
        ///         options.TopicName = "mytopic";
        ///         options.SubscriptionName = "mysubscription";
        ///     });
        /// });
        /// </code>
        /// </example>
        public static ISiloBuilder AddAzureServiceBusStreaming(
            this ISiloBuilder builder,
            Action<AzureServiceBusOptions> configureOptions)
        {
            return builder.AddAzureServiceBusStreaming("AzureServiceBus", configureOptions);
        }
    }
}