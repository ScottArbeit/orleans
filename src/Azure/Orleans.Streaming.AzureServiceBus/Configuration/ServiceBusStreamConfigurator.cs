using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;

namespace Orleans.Streaming.AzureServiceBus.Configuration;

/// <summary>
/// Configurator for Service Bus streaming on the silo.
/// </summary>
public class SiloServiceBusStreamConfigurator : SiloPersistentStreamConfigurator
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SiloServiceBusStreamConfigurator"/> class.
    /// </summary>
    /// <param name="name">The stream provider name.</param>
    /// <param name="configureServicesDelegate">The configure services delegate.</param>
    public SiloServiceBusStreamConfigurator(string name, Action<Action<IServiceCollection>> configureServicesDelegate)
        : base(name, configureServicesDelegate, ServiceBusAdapterFactory.Create)
    {
        this.ConfigureDelegate(services =>
        {
            services.ConfigureNamedOptionForLogging<ServiceBusStreamOptions>(name)
                .ConfigureNamedOptionForLogging<SimpleQueueCacheOptions>(name)
                .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);

            // Register the options validator
            services.AddSingleton<IValidateOptions<ServiceBusStreamOptions>, ServiceBusStreamOptionsValidator>();
        });
    }

    /// <summary>
    /// Configures the Service Bus stream options.
    /// </summary>
    /// <param name="configureOptions">The configuration action.</param>
    /// <returns>The configurator for method chaining.</returns>
    public SiloServiceBusStreamConfigurator ConfigureServiceBus(Action<OptionsBuilder<ServiceBusStreamOptions>> configureOptions)
    {
        this.Configure(configureOptions);
        return this;
    }

    /// <summary>
    /// Configures the cache options.
    /// </summary>
    /// <param name="cacheSize">The cache size. Defaults to 4096.</param>
    /// <returns>The configurator for method chaining.</returns>
    public SiloServiceBusStreamConfigurator ConfigureCache(int cacheSize = 4096)
    {
        this.Configure<SimpleQueueCacheOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
        return this;
    }

    /// <summary>
    /// Configures the partitioning options.
    /// </summary>
    /// <param name="numOfPartitions">The number of partitions. Defaults to 1.</param>
    /// <returns>The configurator for method chaining.</returns>
    public SiloServiceBusStreamConfigurator ConfigurePartitioning(int numOfPartitions = 1)
    {
        this.Configure<HashRingStreamQueueMapperOptions>(ob =>
            ob.Configure(options => options.TotalQueueCount = numOfPartitions));
        return this;
    }
}

/// <summary>
/// Configurator for Service Bus streaming on the client.
/// </summary>
public class ClientServiceBusStreamConfigurator : ClusterClientPersistentStreamConfigurator
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ClientServiceBusStreamConfigurator"/> class.
    /// </summary>
    /// <param name="name">The stream provider name.</param>
    /// <param name="builder">The client builder.</param>
    public ClientServiceBusStreamConfigurator(string name, IClientBuilder builder)
        : base(name, builder, ServiceBusAdapterFactory.Create)
    {
        builder.ConfigureServices(services =>
        {
            services.ConfigureNamedOptionForLogging<ServiceBusStreamOptions>(name)
                .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);

            // Register the options validator
            services.AddSingleton<IValidateOptions<ServiceBusStreamOptions>, ServiceBusStreamOptionsValidator>();
        });
    }

    /// <summary>
    /// Configures the Service Bus stream options.
    /// </summary>
    /// <param name="configureOptions">The configuration action.</param>
    /// <returns>The configurator for method chaining.</returns>
    public ClientServiceBusStreamConfigurator ConfigureServiceBus(Action<OptionsBuilder<ServiceBusStreamOptions>> configureOptions)
    {
        this.Configure(configureOptions);
        return this;
    }

    /// <summary>
    /// Configures the partitioning options.
    /// </summary>
    /// <param name="numOfPartitions">The number of partitions. Defaults to 1.</param>
    /// <returns>The configurator for method chaining.</returns>
    public ClientServiceBusStreamConfigurator ConfigurePartitioning(int numOfPartitions = 1)
    {
        this.Configure<HashRingStreamQueueMapperOptions>(ob =>
            ob.Configure(options => options.TotalQueueCount = numOfPartitions));
        return this;
    }
}

/// <summary>
/// Placeholder adapter factory for Service Bus streaming.
/// This will be implemented in later steps.
/// </summary>
internal static class ServiceBusAdapterFactory
{
    /// <summary>
    /// Creates a Service Bus adapter factory.
    /// </summary>
    /// <param name="services">The service provider.</param>
    /// <param name="name">The provider name.</param>
    /// <returns>A placeholder adapter factory.</returns>
    public static Orleans.Streams.IQueueAdapterFactory Create(IServiceProvider services, string name)
    {
        // Placeholder implementation - will be implemented in Step 5
        throw new NotImplementedException("Service Bus adapter factory will be implemented in Step 5");
    }
}