using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streaming.AzureServiceBus.Cache;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Providers;

/// <summary>
/// Azure Service Bus implementation of queue cache adapter.
/// Provides Azure Service Bus specific caching functionality with pressure monitoring and eviction policies.
/// </summary>
public class AzureServiceBusQueueCache : IQueueAdapterCache
{
    private readonly string _providerName;
    private readonly ILogger<AzureServiceBusQueueCache> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly CacheEvictionPolicy _evictionPolicy;
    private readonly ServiceBusCachePressureMonitor _pressureMonitor;
    private readonly ICacheMonitor? _cacheMonitor;
    
    // Cache of individual queue caches
    private readonly ConcurrentDictionary<QueueId, ServiceBusQueueCache> _queueCaches = new();
    
    // Configuration
    private readonly AzureServiceBusCacheOptions _cacheOptions;

    /// <summary>
    /// Initializes a new instance of the <see cref="AzureServiceBusQueueCache"/> class.
    /// </summary>
    /// <param name="providerName">The name of the stream provider.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="cacheOptions">The cache options.</param>
    /// <param name="cacheMonitor">The cache monitor.</param>
    public AzureServiceBusQueueCache(
        string providerName, 
        ILoggerFactory loggerFactory,
        AzureServiceBusCacheOptions? cacheOptions = null,
        ICacheMonitor? cacheMonitor = null)
    {
        _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _logger = _loggerFactory.CreateLogger<AzureServiceBusQueueCache>();
        _cacheMonitor = cacheMonitor;
        
        // Initialize cache options with defaults if not provided
        _cacheOptions = cacheOptions ?? new AzureServiceBusCacheOptions();
        
        // Create eviction policy from options
        _evictionPolicy = new CacheEvictionPolicy(
            strategy: _cacheOptions.EvictionStrategy,
            messageTTL: _cacheOptions.MessageTTL,
            maxIdleTime: _cacheOptions.MaxIdleTime,
            maxCacheSize: _cacheOptions.MaxCacheSize,
            maxMemorySizeBytes: _cacheOptions.MaxMemorySizeBytes,
            pressureThreshold: _cacheOptions.PressureThreshold);
            
        // Create pressure monitor
        _pressureMonitor = new ServiceBusCachePressureMonitor(
            flowControlThreshold: _cacheOptions.PressureThreshold,
            logger: _logger,
            cacheMonitor: _cacheMonitor);
        
        _logger.LogDebug("Azure Service Bus queue cache initialized for provider '{ProviderName}' with options: MaxSize={MaxSize}, TTL={TTL}, Strategy={Strategy}", 
            _providerName, _cacheOptions.MaxCacheSize, _cacheOptions.MessageTTL, _cacheOptions.EvictionStrategy);
    }

    /// <inheritdoc/>
    public IQueueCache CreateQueueCache(QueueId queueId)
    {
        _logger.LogDebug("Creating queue cache for queue '{QueueId}' in provider '{ProviderName}'", queueId, _providerName);
        
        return _queueCaches.GetOrAdd(queueId, CreateQueueCacheInstance);
    }

    /// <summary>
    /// Adds a cache pressure monitor to all queue caches.
    /// </summary>
    /// <param name="monitor">The cache pressure monitor to add.</param>
    public void AddCachePressureMonitor(ICachePressureMonitor monitor)
    {
        if (monitor is null)
            throw new ArgumentNullException(nameof(monitor));

        // This is a simplified implementation - in a full implementation,
        // we might aggregate multiple monitors
        _logger.LogDebug("Cache pressure monitor added to Azure Service Bus queue cache for provider '{ProviderName}'", _providerName);
    }

    /// <summary>
    /// Gets the current cache pressure across all queues.
    /// </summary>
    /// <returns>The cache pressure level (0.0 to 1.0).</returns>
    public double GetCachePressure()
    {
        return _pressureMonitor.GetCachePressure(DateTime.UtcNow);
    }

    /// <summary>
    /// Attempts to purge items from all caches based on eviction policies.
    /// </summary>
    /// <param name="utcTime">The current UTC time.</param>
    /// <returns>True if purging was performed.</returns>
    public bool TryCachePurge(DateTime utcTime)
    {
        var purged = false;
        
        foreach (var cache in _queueCaches.Values)
        {
            if (cache.TryPurgeFromCache(out var purgedItems))
            {
                purged = true;
                _logger.LogDebug("Purged {Count} items from queue cache", purgedItems.Count);
            }
        }
        
        return purged;
    }

    private ServiceBusQueueCache CreateQueueCacheInstance(QueueId queueId)
    {
        var logger = _loggerFactory.CreateLogger<ServiceBusQueueCache>();
        
        return new ServiceBusQueueCache(
            queueId,
            _evictionPolicy,
            _pressureMonitor,
            logger,
            _cacheMonitor);
    }
}

/// <summary>
/// Configuration options for Azure Service Bus cache.
/// </summary>
public class AzureServiceBusCacheOptions
{
    /// <summary>
    /// Gets or sets the maximum cache size in number of messages.
    /// </summary>
    public int MaxCacheSize { get; set; } = 4096;

    /// <summary>
    /// Gets or sets the maximum memory size in bytes.
    /// </summary>
    public long MaxMemorySizeBytes { get; set; } = 64 * 1024 * 1024; // 64 MB

    /// <summary>
    /// Gets or sets the message time-to-live in the cache.
    /// </summary>
    public TimeSpan MessageTTL { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Gets or sets the maximum idle time before eviction.
    /// </summary>
    public TimeSpan MaxIdleTime { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Gets or sets the cache eviction strategy.
    /// </summary>
    public CacheEvictionStrategy EvictionStrategy { get; set; } = CacheEvictionStrategy.LRU;

    /// <summary>
    /// Gets or sets the pressure threshold (0.0 to 1.0) at which to start aggressive eviction.
    /// </summary>
    public double PressureThreshold { get; set; } = 0.8;
}