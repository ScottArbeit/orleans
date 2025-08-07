using System;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Cache;

/// <summary>
/// Cache pressure monitor for Azure Service Bus queue cache.
/// Monitors memory usage, message count, and processing rate to determine cache pressure.
/// </summary>
public sealed partial class ServiceBusCachePressureMonitor : ICachePressureMonitor
{
    private static readonly TimeSpan CheckPeriod = TimeSpan.FromSeconds(2);
    
    private readonly ILogger _logger;
    private readonly double _flowControlThreshold;
    private readonly object _lockObject = new();
    
    private double _accumulatedCachePressure;
    private double _cachePressureContributionCount;
    private DateTime _nextCheckedTime = DateTime.MinValue;
    private bool _isUnderPressure;
    private long _currentMemoryUsage;
    private int _currentMessageCount;

    /// <inheritdoc />
    public ICacheMonitor? CacheMonitor { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusCachePressureMonitor"/> class.
    /// </summary>
    /// <param name="flowControlThreshold">The flow control threshold (0.0 to 1.0).</param>
    /// <param name="logger">The logger.</param>
    /// <param name="cacheMonitor">The cache monitor.</param>
    public ServiceBusCachePressureMonitor(
        double flowControlThreshold = 0.8,
        ILogger? logger = null,
        ICacheMonitor? cacheMonitor = null)
    {
        _flowControlThreshold = flowControlThreshold >= 0.0 && flowControlThreshold <= 1.0 
            ? flowControlThreshold 
            : throw new ArgumentOutOfRangeException(nameof(flowControlThreshold));
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
        CacheMonitor = cacheMonitor;
    }

    /// <inheritdoc />
    public void RecordCachePressureContribution(double cachePressureContribution)
    {
        lock (_lockObject)
        {
            // Weight unhealthy contributions more heavily than healthy ones
            // This compensates for the fact that healthy consumers consume more often
            double weight = cachePressureContribution < _flowControlThreshold ? 1.0 : 3.0;
            _accumulatedCachePressure += cachePressureContribution * weight;
            _cachePressureContributionCount += weight;
        }
    }

    /// <inheritdoc />
    public bool IsUnderPressure(DateTime utcNow)
    {
        lock (_lockObject)
        {
            if (_nextCheckedTime < utcNow)
            {
                CalculatePressure();
                _nextCheckedTime = utcNow + CheckPeriod;
            }
            return _isUnderPressure;
        }
    }

    /// <summary>
    /// Records the current memory usage of the cache.
    /// </summary>
    /// <param name="memoryUsage">The current memory usage in bytes.</param>
    public void RecordMemoryUsage(long memoryUsage)
    {
        lock (_lockObject)
        {
            _currentMemoryUsage = memoryUsage;
        }
    }

    /// <summary>
    /// Records the current message count in the cache.
    /// </summary>
    /// <param name="messageCount">The current message count.</param>
    public void RecordMessageCount(int messageCount)
    {
        lock (_lockObject)
        {
            _currentMessageCount = messageCount;
        }
    }

    /// <summary>
    /// Gets the current cache pressure level (0.0 to 1.0).
    /// </summary>
    /// <param name="utcNow">The current UTC time.</param>
    /// <returns>The cache pressure level.</returns>
    public double GetCachePressure(DateTime utcNow)
    {
        lock (_lockObject)
        {
            if (_cachePressureContributionCount < 0.5)
            {
                return 0.0;
            }
            return _accumulatedCachePressure / _cachePressureContributionCount;
        }
    }

    private void CalculatePressure()
    {
        // If we don't have any contributions, don't change status
        if (_cachePressureContributionCount < 0.5)
        {
            // After 5 checks with no contributions, increment anyway
            _cachePressureContributionCount += 0.1;
            return;
        }

        double pressure = _accumulatedCachePressure / _cachePressureContributionCount;
        bool wasUnderPressure = _isUnderPressure;
        _isUnderPressure = pressure > _flowControlThreshold;

        // If state changed, log and notify monitor
        if (_isUnderPressure != wasUnderPressure)
        {
            CacheMonitor?.TrackCachePressureMonitorStatusChange(
                GetType().Name, 
                _isUnderPressure, 
                _cachePressureContributionCount, 
                pressure, 
                _flowControlThreshold);

            if (_isUnderPressure)
            {
                LogDebugCacheUnderPressure(
                    _accumulatedCachePressure,
                    _cachePressureContributionCount,
                    pressure,
                    _flowControlThreshold,
                    _currentMemoryUsage,
                    _currentMessageCount);
            }
            else
            {
                LogDebugCachePressureHealthy(
                    _accumulatedCachePressure,
                    _cachePressureContributionCount,
                    pressure,
                    _flowControlThreshold,
                    _currentMemoryUsage,
                    _currentMessageCount);
            }
        }

        // Reset counters
        _cachePressureContributionCount = 0.0;
        _accumulatedCachePressure = 0.0;
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Azure Service Bus cache under pressure. Throttling message reading. AccumulatedPressure: {AccumulatedPressure}, Contributions: {Contributions}, AveragePressure: {AveragePressure}, Threshold: {Threshold}, MemoryUsage: {MemoryUsage} bytes, MessageCount: {MessageCount}"
    )]
    private partial void LogDebugCacheUnderPressure(
        double accumulatedPressure,
        double contributions,
        double averagePressure,
        double threshold,
        long memoryUsage,
        int messageCount);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Azure Service Bus cache pressure healthy. AccumulatedPressure: {AccumulatedPressure}, Contributions: {Contributions}, AveragePressure: {AveragePressure}, Threshold: {Threshold}, MemoryUsage: {MemoryUsage} bytes, MessageCount: {MessageCount}"
    )]
    private partial void LogDebugCachePressureHealthy(
        double accumulatedPressure,
        double contributions,
        double averagePressure,
        double threshold,
        long memoryUsage,
        int messageCount);
}