using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Streaming.AzureServiceBus;
using Orleans.Streaming.AzureServiceBus.Configuration;

namespace Orleans.Hosting;

/// <summary>
/// Extension methods for configuring Azure Service Bus streaming on the silo.
/// </summary>
public static class SiloBuilderExtensions
{
    /// <summary>
    /// Configure silo to use Azure Service Bus persistent streams with provided options.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configureOptions">The configuration action for Service Bus options.</param>
    /// <returns>The silo builder.</returns>
    public static ISiloBuilder AddServiceBusStreams(
        this ISiloBuilder builder,
        string name,
        Action<ServiceBusStreamOptions> configureOptions)
    {
        return builder.AddServiceBusStreams(name, configure =>
            configure.ConfigureServiceBus(ob => ob.Configure(configureOptions)));
    }

    /// <summary>
    /// Configure silo to use Azure Service Bus persistent streams.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configure">The configuration action.</param>
    /// <returns>The silo builder.</returns>
    public static ISiloBuilder AddServiceBusStreams(
        this ISiloBuilder builder,
        string name,
        Action<SiloServiceBusStreamConfigurator> configure)
    {
        var configurator = new SiloServiceBusStreamConfigurator(name,
            configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate));
        configure?.Invoke(configurator);
        return builder;
    }


}

/// <summary>
/// Extension methods for configuring Azure Service Bus streaming on the client.
/// </summary>
public static class ClientBuilderExtensions
{
    /// <summary>
    /// Configure client to use Azure Service Bus persistent streams with provided options.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configureOptions">The configuration action for Service Bus options.</param>
    /// <returns>The client builder.</returns>
    public static IClientBuilder AddServiceBusStreams(
        this IClientBuilder builder,
        string name,
        Action<ServiceBusStreamOptions> configureOptions)
    {
        return builder.AddServiceBusStreams(name, configure =>
            configure.ConfigureServiceBus(ob => ob.Configure(configureOptions)));
    }

    /// <summary>
    /// Configure client to use Azure Service Bus persistent streams.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="configure">The configuration action.</param>
    /// <returns>The client builder.</returns>
    public static IClientBuilder AddServiceBusStreams(
        this IClientBuilder builder,
        string name,
        Action<ClientServiceBusStreamConfigurator> configure)
    {
        var configurator = new ClientServiceBusStreamConfigurator(name, builder);
        configure?.Invoke(configurator);
        return builder;
    }
}