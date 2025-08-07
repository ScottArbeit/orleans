using System;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus.Providers;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Cache;

/// <summary>
/// Factory for creating Azure Service Bus queue cache adapters.
/// Provides configuration and dependency injection for cache components.
/// </summary>
public class AzureServiceBusQueueCacheFactory
{
    private readonly string _providerName;
    private readonly ILoggerFactory _loggerFactory;
    private readonly AzureServiceBusCacheOptions _cacheOptions;
    private readonly ICacheMonitor? _cacheMonitor;

    /// <summary>
    /// Initializes a new instance of the <see cref="AzureServiceBusQueueCacheFactory"/> class.
    /// </summary>
    /// <param name="providerName">The provider name.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="cacheOptions">The cache options.</param>
    /// <param name="cacheMonitor">The cache monitor.</param>
    public AzureServiceBusQueueCacheFactory(
        string providerName,
        ILoggerFactory loggerFactory,
        AzureServiceBusCacheOptions? cacheOptions = null,
        ICacheMonitor? cacheMonitor = null)
    {
        _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _cacheOptions = cacheOptions ?? new AzureServiceBusCacheOptions();
        _cacheMonitor = cacheMonitor;
    }

    /// <summary>
    /// Creates a new Azure Service Bus queue cache adapter.
    /// </summary>
    /// <returns>The queue cache adapter.</returns>
    public AzureServiceBusQueueCache CreateQueueCacheAdapter()
    {
        return new AzureServiceBusQueueCache(
            _providerName,
            _loggerFactory,
            _cacheOptions,
            _cacheMonitor);
    }

    /// <summary>
    /// Creates a pressure monitor with the configured settings.
    /// </summary>
    /// <returns>The pressure monitor.</returns>
    public ServiceBusCachePressureMonitor CreatePressureMonitor()
    {
        var logger = _loggerFactory.CreateLogger<ServiceBusCachePressureMonitor>();
        
        return new ServiceBusCachePressureMonitor(
            flowControlThreshold: _cacheOptions.PressureThreshold,
            logger: logger,
            cacheMonitor: _cacheMonitor);
    }

    /// <summary>
    /// Creates an eviction policy with the configured settings.
    /// </summary>
    /// <returns>The eviction policy.</returns>
    public CacheEvictionPolicy CreateEvictionPolicy()
    {
        return new CacheEvictionPolicy(
            strategy: _cacheOptions.EvictionStrategy,
            messageTTL: _cacheOptions.MessageTTL,
            maxIdleTime: _cacheOptions.MaxIdleTime,
            maxCacheSize: _cacheOptions.MaxCacheSize,
            maxMemorySizeBytes: _cacheOptions.MaxMemorySizeBytes,
            pressureThreshold: _cacheOptions.PressureThreshold);
    }
}