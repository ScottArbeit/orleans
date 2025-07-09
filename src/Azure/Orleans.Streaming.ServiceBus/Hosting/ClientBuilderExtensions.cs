using System;
using Orleans.Configuration;

namespace Orleans.Hosting;

public static class ClientBuilderExtensions
{
    /// <summary>
    /// Configure cluster client to use Azure Service Bus persistent streams with default settings
    /// </summary>
    public static IClientBuilder AddServiceBusStreams(this IClientBuilder builder, string name, Action<ServiceBusOptions> configureOptions)
    {
        builder.AddServiceBusStreams(name, b =>
            b.ConfigureServiceBus(ob => ob.Configure(configureOptions)));
        return builder;
    }

    /// <summary>
    /// Configure cluster client to use Azure Service Bus persistent streams.
    /// </summary>
    public static IClientBuilder AddServiceBusStreams(this IClientBuilder builder, string name, Action<ClusterClientServiceBusStreamConfigurator> configure)
    {
        var configurator = new ClusterClientServiceBusStreamConfigurator(name, builder);
        configure?.Invoke(configurator);
        return builder;
    }
}