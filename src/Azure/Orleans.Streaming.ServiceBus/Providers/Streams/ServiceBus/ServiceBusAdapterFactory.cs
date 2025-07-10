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
    /// Init the factory.
    /// </summary>
    public virtual void Init()
    {
        this.StreamFailureHandlerFactory = this.StreamFailureHandlerFactory ??
                ((qid) => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler()));
    }

    /// <summary>
    /// Creates the Azure Service Bus based adapter.
    /// </summary>
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
    /// Creates the adapter cache.
    /// </summary>
    public virtual IQueueAdapterCache GetQueueAdapterCache()
    {
        return adapterCache;
    }

    /// <summary>
    /// Creates the factory stream queue mapper.
    /// </summary>
    public IStreamQueueMapper GetStreamQueueMapper()
    {
        return streamQueueMapper;
    }

    /// <summary>
    /// Creates a delivery failure handler for the specified queue.
    /// </summary>
    /// <param name="queueId"></param>
    /// <returns></returns>
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
/// A simple wrapper to convert IOptions to IOptionsMonitor.
/// </summary>
internal class OptionsWrapper<T> : IOptionsMonitor<T>
{
    private readonly T _value;

    public OptionsWrapper(T value)
    {
        _value = value;
    }

    public T CurrentValue => _value;

    public T Get(string? name) => _value;

    public IDisposable? OnChange(Action<T, string?> listener) => null;
}