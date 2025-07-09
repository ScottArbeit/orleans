using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Providers.Streams.ServiceBus;
using Orleans.Configuration;
using Orleans.Streams;

namespace Orleans.Hosting;

public interface IServiceBusStreamConfigurator : INamedServiceConfigurator { }

public static class ServiceBusStreamConfiguratorExtensions
{
    public static void ConfigureServiceBus(this IServiceBusStreamConfigurator configurator, Action<OptionsBuilder<ServiceBusOptions>> configureOptions)
    {
        configurator.Configure(configureOptions);
    }

    public static void ConfigureQueueDataAdapter(this IServiceBusStreamConfigurator configurator, Func<IServiceProvider, string, IQueueDataAdapter<string, IBatchContainer>> factory)
    {
        configurator.ConfigureComponent(factory);
    }

    public static void ConfigureQueueDataAdapter<TQueueDataAdapter>(this IServiceBusStreamConfigurator configurator)
        where TQueueDataAdapter : IQueueDataAdapter<string, IBatchContainer>
    {
        configurator.ConfigureComponent<IQueueDataAdapter<string, IBatchContainer>>((sp, n) => ActivatorUtilities.CreateInstance<TQueueDataAdapter>(sp));
    }
}

public interface ISiloServiceBusStreamConfigurator : IServiceBusStreamConfigurator, ISiloPersistentStreamConfigurator { }

public static class SiloServiceBusStreamConfiguratorExtensions
{
    public static void ConfigureCacheSize(this ISiloServiceBusStreamConfigurator configurator, int cacheSize = SimpleQueueCacheOptions.DEFAULT_CACHE_SIZE)
    {
        configurator.Configure<SimpleQueueCacheOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
    }
}

public class SiloServiceBusStreamConfigurator : SiloPersistentStreamConfigurator, ISiloServiceBusStreamConfigurator
{
    public SiloServiceBusStreamConfigurator(string name, Action<Action<IServiceCollection>> configureServicesDelegate)
        : base(name, configureServicesDelegate, ServiceBusAdapterFactory.Create)
    {
        this.ConfigureDelegate(services =>
        {
            services.ConfigureNamedOptionForLogging<ServiceBusOptions>(name)
                .ConfigureNamedOptionForLogging<SimpleQueueCacheOptions>(name)
                .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);
        });
    }
}

public interface IClusterClientServiceBusStreamConfigurator : IServiceBusStreamConfigurator, IClusterClientPersistentStreamConfigurator { }

public class ClusterClientServiceBusStreamConfigurator : ClusterClientPersistentStreamConfigurator, IClusterClientServiceBusStreamConfigurator
{
    public ClusterClientServiceBusStreamConfigurator(string name, IClientBuilder builder)
        : base(name, builder, ServiceBusAdapterFactory.Create)
    {
        builder.ConfigureServices(services =>
        {
            services.ConfigureNamedOptionForLogging<ServiceBusOptions>(name)
                .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);
        });
    }
}