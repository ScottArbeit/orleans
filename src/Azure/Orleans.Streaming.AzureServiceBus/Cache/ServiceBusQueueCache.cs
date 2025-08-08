using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Cache;

/// <summary>
/// Azure Service Bus specific implementation of IQueueCache.
/// Provides efficient caching with configurable eviction policies and pressure monitoring.
/// </summary>
public sealed partial class ServiceBusQueueCache : IQueueCache, IDisposable
{
    private readonly QueueId _queueId;
    private readonly CacheEvictionPolicy _evictionPolicy;
    private readonly ServiceBusCachePressureMonitor _pressureMonitor;
    private readonly ILogger _logger;
    private readonly ICacheMonitor? _cacheMonitor;
    private readonly object _lockObject = new();
    
    // Use a simpler data structure compatible with Orleans patterns
    private readonly Dictionary<string, MessageCacheEntry> _messageCache = new();
    private readonly LinkedList<string> _accessOrder = new(); // For LRU tracking
    private readonly Dictionary<string, LinkedListNode<string>> _accessNodes = new();
    
    // Index for quick sequence token lookups
    private readonly ConcurrentDictionary<StreamSequenceToken, string> _sequenceTokenIndex = new();
    
    // Tracking for cache metrics
    private long _currentMemoryUsage;
    private int _messageCount;
    private DateTime _lastPurgeTime = DateTime.UtcNow;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusQueueCache"/> class.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <param name="evictionPolicy">The eviction policy.</param>
    /// <param name="pressureMonitor">The pressure monitor.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="cacheMonitor">The cache monitor.</param>
    public ServiceBusQueueCache(
        QueueId queueId,
        CacheEvictionPolicy evictionPolicy,
        ServiceBusCachePressureMonitor pressureMonitor,
        ILogger logger,
        ICacheMonitor? cacheMonitor = null)
    {
        _queueId = queueId ?? throw new ArgumentNullException(nameof(queueId));
        _evictionPolicy = evictionPolicy ?? throw new ArgumentNullException(nameof(evictionPolicy));
        _pressureMonitor = pressureMonitor ?? throw new ArgumentNullException(nameof(pressureMonitor));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _cacheMonitor = cacheMonitor;

        LogDebugCacheInitialized(_queueId, _evictionPolicy.MaxCacheSize, _evictionPolicy.Strategy);
    }

    /// <inheritdoc />
    public void AddToCache(IList<IBatchContainer> messages)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ServiceBusQueueCache));

        if (messages is null || messages.Count == 0)
            return;

        lock (_lockObject)
        {
            var currentTime = DateTime.UtcNow;
            var addedCount = 0;
            var addedMemory = 0L;

            foreach (var message in messages)
            {
                if (TryAddMessageToCache(message, currentTime))
                {
                    addedCount++;
                    addedMemory += EstimateMessageSize(message);
                }
            }

            if (addedCount > 0)
            {
                _messageCount += addedCount;
                _currentMemoryUsage += addedMemory;
                
                // Update pressure monitor
                _pressureMonitor.RecordMessageCount(_messageCount);
                _pressureMonitor.RecordMemoryUsage(_currentMemoryUsage);
                
                // Report to cache monitor
                _cacheMonitor?.TrackMessagesAdded(addedCount);
                _cacheMonitor?.TrackMemoryAllocated((int)addedMemory);

                LogDebugMessagesAdded(addedCount, _messageCount, _currentMemoryUsage);

                // Check if we need to evict due to size constraints
                if (_evictionPolicy.IsCacheSizeExceeded(_messageCount) || 
                    _evictionPolicy.IsMemorySizeExceeded(_currentMemoryUsage))
                {
                    PerformEviction(currentTime, force: true);
                }
            }
        }
    }

    /// <inheritdoc />
    public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
    {
        purgedItems = new List<IBatchContainer>();
        
        if (_disposed)
            return false;

        lock (_lockObject)
        {
            var currentTime = DateTime.UtcNow;
            var purged = PerformEviction(currentTime, force: false);
            
            if (purged.Count > 0)
            {
                purgedItems = purged.Select(entry => entry.BatchContainer).ToList();
                _lastPurgeTime = currentTime;
                
                LogDebugMessagesPurged(purged.Count, _messageCount, _currentMemoryUsage);
                return true;
            }
        }
        
        return false;
    }

    /// <inheritdoc />
    public IQueueCacheCursor GetCacheCursor(StreamId streamId, StreamSequenceToken? token)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ServiceBusQueueCache));

        return new ServiceBusQueueCacheCursor(this, streamId, token, _logger);
    }

    /// <inheritdoc />
    public bool IsUnderPressure()
    {
        if (_disposed)
            return false;

        var currentTime = DateTime.UtcNow;
        return _pressureMonitor.IsUnderPressure(currentTime) ||
               _evictionPolicy.IsCacheSizeExceeded(_messageCount) ||
               _evictionPolicy.IsMemorySizeExceeded(_currentMemoryUsage);
    }

    /// <inheritdoc />
    public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        // For Azure Service Bus, message delivery is handled by the adapter
        // No additional action needed here
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets messages from the cache starting from the specified token.
    /// </summary>
    /// <param name="fromToken">The starting token.</param>
    /// <param name="maxCount">The maximum number of messages to retrieve.</param>
    /// <returns>The messages.</returns>
    internal IEnumerable<MessageCacheEntry> GetMessages(StreamSequenceToken? fromToken, int maxCount = int.MaxValue)
    {
        if (_disposed || maxCount <= 0)
            yield break;

        var count = 0;
        var currentTime = DateTime.UtcNow;
        
        lock (_lockObject)
        {
            // For simplicity, iterate through all messages and filter
            // In a production system, we might want more efficient indexing
            var sortedEntries = _messageCache.Values
                .OrderBy(entry => entry.SequenceToken.SequenceNumber)
                .ToList();

            bool foundStart = fromToken is null;
            
            foreach (var entry in sortedEntries)
            {
                if (count >= maxCount)
                    yield break;

                // If we haven't found our starting point yet, keep looking
                if (!foundStart)
                {
                    if (entry.SequenceToken.Equals(fromToken))
                        foundStart = true;
                    continue;
                }

                // Update access time and record pressure contribution
                entry.UpdateLastAccessTime();
                UpdateAccessOrder(GenerateCacheKey(entry.SequenceToken));
                RecordPressureContribution(entry, currentTime);
                
                yield return entry;
                count++;
            }
        }
    }

    private bool TryAddMessageToCache(IBatchContainer message, DateTime currentTime)
    {
        try
        {
            var cacheKey = GenerateCacheKey(message.SequenceToken);
            
            // Check if already exists
            if (_messageCache.ContainsKey(cacheKey))
                return false;

            var entry = new MessageCacheEntry(
                message,
                message.SequenceToken,
                currentTime, // Assume enqueue time is current time if not available
                currentTime);

            _messageCache[cacheKey] = entry;
            _sequenceTokenIndex[message.SequenceToken] = cacheKey;
            AddToAccessOrder(cacheKey);
            
            return true;
        }
        catch (Exception ex)
        {
            LogWarningFailedToAddMessage(ex, message.SequenceToken);
            return false;
        }
    }

    private List<MessageCacheEntry> PerformEviction(DateTime currentTime, bool force)
    {
        var evictedEntries = new List<MessageCacheEntry>();
        var evictedMemory = 0L;

        // Collect entries to evict based on policy
        var entriesToEvict = new List<string>();

        // Time-based eviction
        foreach (var kvp in _messageCache)
        {
            var entry = kvp.Value;
            var shouldEvict = force || ShouldEvictEntry(entry, currentTime);
            
            if (shouldEvict)
            {
                entriesToEvict.Add(kvp.Key);
            }
        }

        // Size-based eviction (LRU)
        if (_evictionPolicy.Strategy == CacheEvictionStrategy.LRU && 
            (_evictionPolicy.IsCacheSizeExceeded(_messageCount) || force))
        {
            // Evict oldest accessed items first
            var node = _accessOrder.First;
            while (node != null && (_evictionPolicy.IsCacheSizeExceeded(_messageCount) || force))
            {
                var key = node.Value;
                if (!entriesToEvict.Contains(key))
                {
                    entriesToEvict.Add(key);
                }
                node = node.Next;
                
                if (entriesToEvict.Count >= _messageCount / 4) // Don't evict more than 25% at once
                    break;
            }
        }

        // Remove evicted entries
        foreach (var key in entriesToEvict)
        {
            if (_messageCache.TryGetValue(key, out var entry))
            {
                _messageCache.Remove(key);
                _sequenceTokenIndex.TryRemove(entry.SequenceToken, out _);
                RemoveFromAccessOrder(key);
                evictedEntries.Add(entry);
                evictedMemory += EstimateMessageSize(entry.BatchContainer);
            }
        }

        if (evictedEntries.Count > 0)
        {
            _messageCount -= evictedEntries.Count;
            _currentMemoryUsage -= evictedMemory;
            
            _pressureMonitor.RecordMessageCount(_messageCount);
            _pressureMonitor.RecordMemoryUsage(_currentMemoryUsage);
            
            _cacheMonitor?.TrackMessagesPurged(evictedEntries.Count);
            _cacheMonitor?.TrackMemoryReleased((int)evictedMemory);
        }

        return evictedEntries;
    }

    private bool ShouldEvictEntry(MessageCacheEntry entry, DateTime currentTime)
    {
        // Check time-based eviction policies
        if (_evictionPolicy.ShouldEvictByAge(entry.GetMessageAge(currentTime)) ||
            _evictionPolicy.ShouldEvictByIdleTime(entry.GetIdleTime(currentTime)))
        {
            return true;
        }

        // Check if under pressure
        var pressure = _pressureMonitor.GetCachePressure(currentTime);
        if (_evictionPolicy.IsUnderPressure(pressure))
        {
            // Under pressure, more aggressive eviction
            return true;
        }

        return false;
    }

    private void RecordPressureContribution(MessageCacheEntry entry, DateTime currentTime)
    {
        // Calculate pressure contribution based on message age and cache position
        var messageAge = entry.GetMessageAge(currentTime);
        var normalizedAge = Math.Min(1.0, messageAge.TotalSeconds / _evictionPolicy.MessageTTL.TotalSeconds);
        
        _pressureMonitor.RecordCachePressureContribution(normalizedAge);
    }

    private void AddToAccessOrder(string key)
    {
        var node = _accessOrder.AddLast(key);
        _accessNodes[key] = node;
    }

    private void UpdateAccessOrder(string key)
    {
        if (_accessNodes.TryGetValue(key, out var node))
        {
            _accessOrder.Remove(node);
            var newNode = _accessOrder.AddLast(key);
            _accessNodes[key] = newNode;
        }
    }

    private void RemoveFromAccessOrder(string key)
    {
        if (_accessNodes.TryGetValue(key, out var node))
        {
            _accessOrder.Remove(node);
            _accessNodes.Remove(key);
        }
    }

    private static long EstimateMessageSize(IBatchContainer message)
    {
        // Rough estimate - could be made more accurate if needed
        return 1024; // Assume 1KB per message as baseline
    }

    private static string GenerateCacheKey(StreamSequenceToken token)
    {
        return $"{token.EventIndex}_{token.SequenceNumber}_{token.GetHashCode()}";
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;

        lock (_lockObject)
        {
            if (_disposed)
                return;

            _disposed = true;
            _messageCache.Clear();
            _sequenceTokenIndex.Clear();
            _accessOrder.Clear();
            _accessNodes.Clear();
            
            LogDebugCacheDisposed(_queueId);
        }
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Azure Service Bus queue cache initialized for queue '{QueueId}' with capacity {Capacity} and strategy {Strategy}"
    )]
    private partial void LogDebugCacheInitialized(QueueId queueId, int capacity, CacheEvictionStrategy strategy);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Added {AddedCount} messages to cache. Total: {TotalCount}, Memory: {MemoryUsage} bytes"
    )]
    private partial void LogDebugMessagesAdded(int addedCount, int totalCount, long memoryUsage);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Purged {PurgedCount} messages from cache. Remaining: {RemainingCount}, Memory: {MemoryUsage} bytes"
    )]
    private partial void LogDebugMessagesPurged(int purgedCount, int remainingCount, long memoryUsage);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Failed to add message to cache"
    )]
    private partial void LogWarningFailedToAddMessage(Exception exception, StreamSequenceToken sequenceToken);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Azure Service Bus queue cache disposed for queue '{QueueId}'"
    )]
    private partial void LogDebugCacheDisposed(QueueId queueId);
    /// <inheritdoc />
    public int GetMaxAddCount()
    {
        // Return the maximum number of messages that can be added to the cache at once,
        // based on the eviction policy's MaxCacheSize and current message count.
        if (_disposed)
        {
            return 0;
        }

        lock (_lockObject)
        {
            var remaining = _evictionPolicy.MaxCacheSize - _messageCount;
            return remaining > 0 ? remaining : 0;
        }
    }
}
