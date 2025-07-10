using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streaming.ServiceBus;
using Orleans.Streams;

namespace Orleans.Providers.Streams.ServiceBus;

/// <summary>
/// Factory class for Azure Service Bus based stream provider.
/// </summary>
public class ServiceBusAdapterFactory : IQueueAdapterFactory
{
    private readonly string providerName;
    private readonly ServiceBusOptions options;
    private readonly ILoggerFactory loggerFactory;
    private readonly HashRingBasedPartitionedStreamQueueMapper streamQueueMapper;
    private readonly SimpleQueueAdapterCache adapterCache;

    /// <summary>
    /// Application level failure handler override.
    /// </summary>
    protected Func<QueueId, Task<IStreamFailureHandler>>? StreamFailureHandlerFactory { private get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusAdapterFactory"/> class.
    /// </summary>
    /// <param name="name">The name of the stream provider.</param>
    /// <param name="options">The Service Bus configuration options.</param>
    /// <param name="cacheOptions">The queue cache configuration options.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public ServiceBusAdapterFactory(
        string name,
        ServiceBusOptions options,
        SimpleQueueCacheOptions cacheOptions,
        ILoggerFactory loggerFactory)
    {
        this.providerName = name;
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        
        // Generate queue names based on options
        var queueNames = GetEntityNames(options);
        
        this.streamQueueMapper = new(queueNames, providerName);
        this.adapterCache = new SimpleQueueAdapterCache(cacheOptions, this.providerName, this.loggerFactory);
    }

    /// <summary>
    /// Initializes the factory with default stream failure handler.
    /// </summary>
    public virtual void Init()
    {
        this.StreamFailureHandlerFactory = this.StreamFailureHandlerFactory ??
                ((qid) => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler()));
    }

    /// <summary>
    /// Creates the appropriate Azure Service Bus adapter based on the configured entity type.
    /// </summary>
    /// <returns>A queue adapter instance (either queue or topic based).</returns>
    public virtual Task<IQueueAdapter> CreateAdapter()
    {
        var optionsMonitor = new OptionsWrapper<ServiceBusOptions>(options);
        var clientFactory = new ServiceBusClientFactory(optionsMonitor);
        
        IQueueAdapter adapter = options.EntityType switch
        {
            ServiceBusEntityType.Queue => new ServiceBusQueueAdapter(providerName, options, streamQueueMapper, clientFactory, loggerFactory),
            ServiceBusEntityType.Topic => new ServiceBusTopicAdapter(providerName, options, streamQueueMapper, clientFactory, loggerFactory),
            _ => throw new ArgumentOutOfRangeException(nameof(options.EntityType), options.EntityType, "Unsupported Service Bus entity type")
        };
        
        return Task.FromResult(adapter);
    }

    /// <summary>
    /// Gets the queue adapter cache for caching stream messages.
    /// </summary>
    /// <returns>The queue adapter cache instance.</returns>
    public virtual IQueueAdapterCache GetQueueAdapterCache()
    {
        return adapterCache;
    }

    /// <summary>
    /// Gets the stream queue mapper for mapping streams to queue partitions.
    /// </summary>
    /// <returns>The stream queue mapper instance.</returns>
    public IStreamQueueMapper GetStreamQueueMapper()
    {
        return streamQueueMapper;
    }

    /// <summary>
    /// Gets a delivery failure handler for the specified queue.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>A stream failure handler instance.</returns>
    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
    {
        return StreamFailureHandlerFactory?.Invoke(queueId) ?? Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
    }

    private static List<string> GetEntityNames(ServiceBusOptions options)
    {
        if (options.EntityType == ServiceBusEntityType.Topic)
        {
            // For topics, return subscription names for partitioning
            return GetSubscriptionNames(options);
        }

        // For queues, return queue names
        // If specific entity names are provided, use them
        if (options.EntityNames is not null && options.EntityNames.Count > 0)
        {
            return options.EntityNames;
        }

        // If a single entity name is provided, use it for all partitions
        if (!string.IsNullOrWhiteSpace(options.EntityName))
        {
            return [options.EntityName];
        }

        // Generate entity names based on partition count and prefix
        return ServiceBusStreamProviderUtils.GenerateDefaultServiceBusQueueNames(
            options.QueueNamePrefix, 
            options.PartitionCount);
    }

    private static List<string> GetSubscriptionNames(ServiceBusOptions options)
    {
        // If specific subscription names are provided, use them
        if (options.SubscriptionNames is not null && options.SubscriptionNames.Count > 0)
        {
            return options.SubscriptionNames;
        }

        // If a single subscription name is provided, use it for all partitions
        if (!string.IsNullOrWhiteSpace(options.SubscriptionName))
        {
            return [options.SubscriptionName];
        }

        // Generate subscription names based on partition count and prefix
        return ServiceBusStreamProviderUtils.GenerateDefaultServiceBusSubscriptionNames(
            options.SubscriptionNamePrefix, 
            options.PartitionCount);
    }

    /// <summary>
    /// Creates and initializes a new instance of the <see cref="ServiceBusAdapterFactory"/> class.
    /// </summary>
    /// <param name="services">The service provider.</param>
    /// <param name="name">The name of the stream provider.</param>
    /// <returns>A configured and initialized ServiceBusAdapterFactory instance.</returns>
    public static ServiceBusAdapterFactory Create(IServiceProvider services, string name)
    {
        var serviceBusOptions = services.GetOptionsByName<ServiceBusOptions>(name);
        var cacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(name);
        var factory = ActivatorUtilities.CreateInstance<ServiceBusAdapterFactory>(services, name, serviceBusOptions, cacheOptions);
        factory.Init();
        return factory;
    }
}

/// <summary>
/// A simple wrapper to convert IOptions to IOptionsMonitor for Service Bus client factory.
/// </summary>
/// <typeparam name="T">The type of options.</typeparam>
internal class OptionsWrapper<T> : IOptionsMonitor<T>
{
    private readonly T _value;

    /// <summary>
    /// Initializes a new instance of the <see cref="OptionsWrapper{T}"/> class.
    /// </summary>
    /// <param name="value">The options value to wrap.</param>
    public OptionsWrapper(T value)
    {
        _value = value;
    }

    /// <summary>
    /// Gets the current value of the options.
    /// </summary>
    public T CurrentValue => _value;

    /// <summary>
    /// Gets the options value for the specified name.
    /// </summary>
    /// <param name="name">The name of the options (ignored in this implementation).</param>
    /// <returns>The options value.</returns>
    public T Get(string? name) => _value;

    /// <summary>
    /// Registers a listener for option changes (no-op in this implementation).
    /// </summary>
    /// <param name="listener">The change listener.</param>
    /// <returns>Always returns null as changes are not supported.</returns>
    public IDisposable? OnChange(Action<T, string?> listener) => null;
}