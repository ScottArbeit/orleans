using System;
using Orleans.Configuration;

namespace Orleans.Hosting;

public static class SiloBuilderExtensions
{
    /// <summary>
    /// Configure silo to use Azure Service Bus persistent streams.
    /// </summary>
    public static ISiloBuilder AddServiceBusStreams(this ISiloBuilder builder, string name,
        Action<SiloServiceBusStreamConfigurator> configure)
    {
        var configurator = new SiloServiceBusStreamConfigurator(name,
            configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate));
        configure?.Invoke(configurator);
        return builder;
    }

    /// <summary>
    /// Configure silo to use Azure Service Bus persistent streams with default settings
    /// </summary>
    public static ISiloBuilder AddServiceBusStreams(this ISiloBuilder builder, string name, Action<ServiceBusOptions> configureOptions)
    {
        builder.AddServiceBusStreams(name, b =>
             b.ConfigureServiceBus(ob => ob.Configure(configureOptions)));
        return builder;
    }
}