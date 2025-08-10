using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Streams;

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
/// Service Bus adapter factory for Orleans streaming.
/// Provides the infrastructure for Service Bus streaming with support for single entity mapping (MVP).
/// </summary>
public static class ServiceBusAdapterFactory
{
    /// <summary>
    /// Creates a Service Bus adapter factory.
    /// </summary>
    /// <param name="services">The service provider.</param>
    /// <param name="name">The provider name.</param>
    /// <returns>A Service Bus adapter factory instance.</returns>
    public static IQueueAdapterFactory Create(IServiceProvider services, string name)
    {
        // Create and return the ServiceBusQueueAdapterFactory with our custom mapper
        return new ServiceBusQueueAdapterFactory(services, name);
    }
}

/// <summary>
/// Implementation of <see cref="Orleans.Streams.IQueueAdapterFactory"/> for Azure Service Bus streaming.
/// This factory creates and manages the components needed for Service Bus streaming with single entity support.
/// </summary>
internal class ServiceBusQueueAdapterFactory : IQueueAdapterFactory
{
    private readonly IServiceProvider _services;
    private readonly string _providerName;
    private readonly ServiceBusQueueMapper _queueMapper;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusQueueAdapterFactory"/> class.
    /// </summary>
    /// <param name="services">The service provider.</param>
    /// <param name="providerName">The stream provider name.</param>
    public ServiceBusQueueAdapterFactory(IServiceProvider services, string providerName)
    {
        _services = services ?? throw new ArgumentNullException(nameof(services));
        _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));

        // Get the Service Bus stream options and create the queue mapper
        var optionsMonitor = services.GetRequiredService<IOptionsMonitor<ServiceBusStreamOptions>>();
        var options = optionsMonitor.Get(providerName);
        _queueMapper = new ServiceBusQueueMapper(options, providerName);
    }

    /// <summary>
    /// Gets the configured queue identifier for this factory.
    /// This represents the single Service Bus entity (queue or topic/subscription) used in the MVP.
    /// </summary>
    public QueueId QueueId => _queueMapper.QueueId;

    /// <summary>
    /// Creates a queue adapter. (Not implemented in this step)
    /// </summary>
    /// <returns>The queue adapter</returns>
    public Task<IQueueAdapter> CreateAdapter()
    {
        // Will be implemented in later steps
        throw new NotImplementedException("Service Bus adapter will be implemented in a later step");
    }

    /// <summary>
    /// Creates queue message cache adapter. (Not implemented in this step)
    /// </summary>
    /// <returns>The queue adapter cache.</returns>
    public IQueueAdapterCache GetQueueAdapterCache()
    {
        // Will be implemented in later steps
        throw new NotImplementedException("Service Bus adapter cache will be implemented in a later step");
    }

    /// <summary>
    /// Creates a queue mapper.
    /// </summary>
    /// <returns>The queue mapper that maps all streams to the single configured entity.</returns>
    public IStreamQueueMapper GetStreamQueueMapper()
    {
        return _queueMapper;
    }

    /// <summary>
    /// Acquire delivery failure handler for a queue. (Not implemented in this step)
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>The stream failure handler.</returns>
    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
    {
        // Will be implemented in later steps
        throw new NotImplementedException("Service Bus delivery failure handler will be implemented in a later step");
    }
}