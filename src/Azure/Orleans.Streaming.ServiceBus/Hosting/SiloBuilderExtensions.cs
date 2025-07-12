using System;
using Orleans.Configuration;

namespace Orleans.Hosting;

public static class SiloBuilderExtensions
{
    /// <summary>
    /// Configure silo to use Azure Service Bus persistent streams with advanced configuration options.
    /// </summary>
    /// <param name="builder">The silo builder instance.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configure">An action to configure the Service Bus stream provider.</param>
    /// <returns>The silo builder instance for method chaining.</returns>
    /// <example>
    /// <code>
    /// // Advanced configuration with C# 13 primary constructor
    /// public class ServiceBusStreamingConfig(IConfiguration config, ILogger&lt;ServiceBusStreamingConfig&gt; logger)
    /// {
    ///     public void ConfigureSilo(ISiloBuilder siloBuilder)
    ///     {
    ///         siloBuilder.AddServiceBusStreams("ServiceBusProvider", configurator =&gt;
    ///         {
    ///             configurator.ConfigureServiceBus(optionsBuilder =&gt;
    ///             {
    ///                 optionsBuilder.Configure(options =&gt;
    ///                 {
    ///                     options.ConfigureServiceBusClient(config.GetConnectionString("ServiceBus")!);
    ///                     options.EntityType = ServiceBusEntityType.Topic;
    ///                     options.PartitionCount = 16;
    ///                     logger.LogInformation("Configured Service Bus with {PartitionCount} partitions", options.PartitionCount);
    ///                 });
    ///             });
    ///             configurator.ConfigureCacheSize(1000);
    ///         });
    ///     }
    /// }
    /// </code>
    /// </example>
    public static ISiloBuilder AddServiceBusStreams(this ISiloBuilder builder, string name,
        Action<SiloServiceBusStreamConfigurator> configure)
    {
        var configurator = new SiloServiceBusStreamConfigurator(name,
            configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate));
        configure?.Invoke(configurator);
        return builder;
    }

    /// <summary>
    /// Configure silo to use Azure Service Bus persistent streams with simple configuration.
    /// </summary>
    /// <param name="builder">The silo builder instance.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configureOptions">An action to configure the Service Bus options.</param>
    /// <returns>The silo builder instance for method chaining.</returns>
    /// <example>
    /// <code>
    /// // Simple configuration example
    /// siloBuilder.AddServiceBusStreams("ServiceBusProvider", options =&gt;
    /// {
    ///     options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;...";
    ///     options.EntityType = ServiceBusEntityType.Queue;
    ///     options.PartitionCount = 8;
    /// });
    /// 
    /// // Using managed identity with C# 13 primary constructor  
    /// public class ServiceBusConfig(string serviceBusNamespace)
    /// {
    ///     public void Configure(ServiceBusOptions options)
    ///     {
    ///         options.ConfigureServiceBusClient(serviceBusNamespace, new DefaultAzureCredential());
    ///         options.EntityType = ServiceBusEntityType.Topic;
    ///     }
    /// }
    /// </code>
    /// </example>
    public static ISiloBuilder AddServiceBusStreams(this ISiloBuilder builder, string name, Action<ServiceBusOptions> configureOptions)
    {
        builder.AddServiceBusStreams(name, b =>
             b.ConfigureServiceBus(ob => ob.Configure(configureOptions)));
        return builder;
    }
}