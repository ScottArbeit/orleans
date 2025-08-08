using System;
using Orleans.Providers.Streams.Common;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Cache;

/// <summary>
/// Cache pressure monitor records pressure contribution to the cache, and determines if the cache is under pressure based on its 
/// back pressure algorithm.
/// </summary>
public interface ICachePressureMonitor
{
    /// <summary>
    /// Record cache pressure contribution to the monitor.
    /// </summary>
    /// <param name="cachePressureContribution">The cache pressure contribution value.</param>
    void RecordCachePressureContribution(double cachePressureContribution);

    /// <summary>
    /// Determine if the monitor is under pressure.
    /// </summary>
    /// <param name="utcNow">Current UTC time.</param>
    /// <returns>True if under pressure, false otherwise.</returns>
    bool IsUnderPressure(DateTime utcNow);

    /// <summary>
    /// Cache monitor which is used to report cache related metrics.
    /// </summary>
    ICacheMonitor? CacheMonitor { set; }
}