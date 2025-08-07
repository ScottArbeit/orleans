using System;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Cache;

/// <summary>
/// Defines the eviction strategy for the cache.
/// </summary>
public enum CacheEvictionStrategy
{
    /// <summary>
    /// Least Recently Used eviction strategy.
    /// </summary>
    LRU,

    /// <summary>
    /// First In First Out eviction strategy.
    /// </summary>
    FIFO,

    /// <summary>
    /// Time-based eviction strategy.
    /// </summary>
    TimeBasedTTL
}

/// <summary>
/// Defines the cache eviction policy for Azure Service Bus message caching.
/// </summary>
public sealed class CacheEvictionPolicy
{
    /// <summary>
    /// Gets the eviction strategy.
    /// </summary>
    public CacheEvictionStrategy Strategy { get; }

    /// <summary>
    /// Gets the maximum message time-to-live in the cache.
    /// </summary>
    public TimeSpan MessageTTL { get; }

    /// <summary>
    /// Gets the maximum idle time before eviction.
    /// </summary>
    public TimeSpan MaxIdleTime { get; }

    /// <summary>
    /// Gets the maximum cache size in number of messages.
    /// </summary>
    public int MaxCacheSize { get; }

    /// <summary>
    /// Gets the maximum memory size in bytes.
    /// </summary>
    public long MaxMemorySizeBytes { get; }

    /// <summary>
    /// Gets the pressure threshold (0.0 to 1.0) at which to start aggressive eviction.
    /// </summary>
    public double PressureThreshold { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="CacheEvictionPolicy"/> class.
    /// </summary>
    /// <param name="strategy">The eviction strategy.</param>
    /// <param name="messageTTL">The message time-to-live.</param>
    /// <param name="maxIdleTime">The maximum idle time.</param>
    /// <param name="maxCacheSize">The maximum cache size.</param>
    /// <param name="maxMemorySizeBytes">The maximum memory size in bytes.</param>
    /// <param name="pressureThreshold">The pressure threshold.</param>
    public CacheEvictionPolicy(
        CacheEvictionStrategy strategy = CacheEvictionStrategy.LRU,
        TimeSpan? messageTTL = null,
        TimeSpan? maxIdleTime = null,
        int maxCacheSize = 4096,
        long maxMemorySizeBytes = 64 * 1024 * 1024, // 64 MB
        double pressureThreshold = 0.8)
    {
        Strategy = strategy;
        MessageTTL = messageTTL ?? TimeSpan.FromMinutes(30);
        MaxIdleTime = maxIdleTime ?? TimeSpan.FromMinutes(10);
        MaxCacheSize = maxCacheSize > 0 ? maxCacheSize : throw new ArgumentOutOfRangeException(nameof(maxCacheSize));
        MaxMemorySizeBytes = maxMemorySizeBytes > 0 ? maxMemorySizeBytes : throw new ArgumentOutOfRangeException(nameof(maxMemorySizeBytes));
        PressureThreshold = pressureThreshold >= 0.0 && pressureThreshold <= 1.0 ? pressureThreshold : throw new ArgumentOutOfRangeException(nameof(pressureThreshold));
    }

    /// <summary>
    /// Determines if a message should be evicted based on age.
    /// </summary>
    /// <param name="messageAge">The message age.</param>
    /// <returns>True if the message should be evicted.</returns>
    public bool ShouldEvictByAge(TimeSpan messageAge)
    {
        return messageAge > MessageTTL;
    }

    /// <summary>
    /// Determines if a message should be evicted based on idle time.
    /// </summary>
    /// <param name="idleTime">The idle time.</param>
    /// <returns>True if the message should be evicted.</returns>
    public bool ShouldEvictByIdleTime(TimeSpan idleTime)
    {
        return idleTime > MaxIdleTime;
    }

    /// <summary>
    /// Determines if cache size limits have been exceeded.
    /// </summary>
    /// <param name="currentCacheSize">The current cache size.</param>
    /// <returns>True if size limits are exceeded.</returns>
    public bool IsCacheSizeExceeded(int currentCacheSize)
    {
        return currentCacheSize >= MaxCacheSize;
    }

    /// <summary>
    /// Determines if memory size limits have been exceeded.
    /// </summary>
    /// <param name="currentMemorySize">The current memory size in bytes.</param>
    /// <returns>True if memory limits are exceeded.</returns>
    public bool IsMemorySizeExceeded(long currentMemorySize)
    {
        return currentMemorySize >= MaxMemorySizeBytes;
    }

    /// <summary>
    /// Determines if the cache is under pressure.
    /// </summary>
    /// <param name="currentPressure">The current pressure (0.0 to 1.0).</param>
    /// <returns>True if the cache is under pressure.</returns>
    public bool IsUnderPressure(double currentPressure)
    {
        return currentPressure > PressureThreshold;
    }
}