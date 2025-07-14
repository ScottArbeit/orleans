using System;
using Orleans.Configuration;

namespace Orleans.Hosting;

public static class ClientBuilderExtensions
{
    /// <summary>
    /// Configure cluster client to use Azure Service Bus persistent streams with simple configuration.
    /// </summary>
    /// <param name="builder">The client builder instance.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configureOptions">An action to configure the Service Bus options.</param>
    /// <returns>The client builder instance for method chaining.</returns>
    /// <example>
    /// <code>
    /// // Simple client configuration
    /// clientBuilder.AddServiceBusStreams("ServiceBusProvider", options =&gt;
    /// {
    ///     options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;...";
    /// });
    /// 
    /// // Using managed identity with C# 13 primary constructor
    /// public class ClientServiceBusConfig(ITokenCredential credential, string serviceBusNamespace)
    /// {
    ///     public void Configure(ServiceBusOptions options)
    ///     {
    ///         options.ConfigureServiceBusClient(serviceBusNamespace, credential);
    ///     }
    /// }
    /// </code>
    /// </example>
    public static IClientBuilder AddServiceBusStreams(this IClientBuilder builder, string name, Action<ServiceBusOptions> configureOptions)
    {
        builder.AddServiceBusStreams(name, b =>
            b.ConfigureServiceBus(ob => ob.Configure(configureOptions)));
        return builder;
    }

    /// <summary>
    /// Configure cluster client to use Azure Service Bus persistent streams with advanced configuration options.
    /// </summary>
    /// <param name="builder">The client builder instance.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configure">An action to configure the Service Bus stream provider.</param>
    /// <returns>The client builder instance for method chaining.</returns>
    /// <example>
    /// <code>
    /// // Advanced client configuration with C# 13 primary constructor
    /// public class ClientStreamingConfig(IConfiguration config, ILogger&lt;ClientStreamingConfig&gt; logger)
    /// {
    ///     public void ConfigureClient(IClientBuilder clientBuilder)
    ///     {
    ///         clientBuilder.AddServiceBusStreams("ServiceBusProvider", configurator =&gt;
    ///         {
    ///             configurator.ConfigureServiceBus(optionsBuilder =&gt;
    ///             {
    ///                 optionsBuilder.Configure(options =&gt;
    ///                 {
    ///                     var connectionString = config.GetConnectionString("ServiceBus");
    ///                     if (connectionString is not null)
    ///                     {
    ///                         options.ConfigureServiceBusClient(connectionString);
    ///                         logger.LogInformation("Configured Service Bus client streaming");
    ///                     }
    ///                 });
    ///             });
    ///         });
    ///     }
    /// }
    /// </code>
    /// </example>
    public static IClientBuilder AddServiceBusStreams(this IClientBuilder builder, string name, Action<ClusterClientServiceBusStreamConfigurator> configure)
    {
        var configurator = new ClusterClientServiceBusStreamConfigurator(name, builder);
        configure?.Invoke(configurator);
        return builder;
    }
}