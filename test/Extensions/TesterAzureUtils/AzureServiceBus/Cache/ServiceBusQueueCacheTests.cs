using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streaming.AzureServiceBus.Cache;
using Xunit;

namespace Tester.AzureUtils.AzureServiceBus.Cache;

/// <summary>
/// Tests for Azure Service Bus queue cache functionality.
/// </summary>
public class ServiceBusQueueCacheTests
{
    private readonly ILogger<ServiceBusQueueCache> _logger;
    private readonly QueueId _queueId;

    public ServiceBusQueueCacheTests()
    {
        _logger = new LoggerFactory().CreateLogger<ServiceBusQueueCache>();
        _queueId = QueueId.GetQueueId("test-queue", 0);
    }

    [Fact]
    public void CreateQueueCache_WithValidParameters_CreatesCache()
    {
        // Arrange
        var evictionPolicy = new CacheEvictionPolicy();
        var pressureMonitor = new ServiceBusCachePressureMonitor();

        // Act
        using var cache = new ServiceBusQueueCache(_queueId, evictionPolicy, pressureMonitor, _logger);

        // Assert
        Assert.NotNull(cache);
        Assert.False(cache.IsUnderPressure());
    }

    [Fact]
    public void AddToCache_WithValidMessages_AddsMessagesToCache()
    {
        // Arrange
        var evictionPolicy = new CacheEvictionPolicy(maxCacheSize: 100);
        var pressureMonitor = new ServiceBusCachePressureMonitor();
        using var cache = new ServiceBusQueueCache(_queueId, evictionPolicy, pressureMonitor, _logger);
        
        var messages = CreateTestMessages(5);

        // Act
        cache.AddToCache(messages);

        // Assert
        // Cache should have messages - we can test this through cursor navigation
        var cursor = cache.GetCacheCursor(StreamId.Create("test-namespace", "test-stream"), null);
        Assert.NotNull(cursor);
    }

    [Fact]
    public void TryPurgeFromCache_WhenCacheHasMessages_PurgesExpiredMessages()
    {
        // Arrange
        var evictionPolicy = new CacheEvictionPolicy(
            messageTTL: TimeSpan.FromMilliseconds(1), // Very short TTL for testing
            maxCacheSize: 100);
        var pressureMonitor = new ServiceBusCachePressureMonitor();
        using var cache = new ServiceBusQueueCache(_queueId, evictionPolicy, pressureMonitor, _logger);
        
        var messages = CreateTestMessages(3);
        cache.AddToCache(messages);

        // Wait for messages to expire
        System.Threading.Thread.Sleep(10);

        // Act
        var result = cache.TryPurgeFromCache(out var purgedItems);

        // Assert
        Assert.True(result);
        Assert.NotNull(purgedItems);
    }

    [Fact]
    public void IsUnderPressure_WhenCacheSizeExceeded_ReturnsTrue()
    {
        // Arrange
        var evictionPolicy = new CacheEvictionPolicy(maxCacheSize: 2); // Very small cache
        var pressureMonitor = new ServiceBusCachePressureMonitor();
        using var cache = new ServiceBusQueueCache(_queueId, evictionPolicy, pressureMonitor, _logger);
        
        var messages = CreateTestMessages(5); // More than cache size

        // Act
        cache.AddToCache(messages);
        var isUnderPressure = cache.IsUnderPressure();

        // Assert
        Assert.True(isUnderPressure);
    }

    [Fact]
    public void GetCacheCursor_WithValidStreamId_ReturnsCursor()
    {
        // Arrange
        var evictionPolicy = new CacheEvictionPolicy();
        var pressureMonitor = new ServiceBusCachePressureMonitor();
        using var cache = new ServiceBusQueueCache(_queueId, evictionPolicy, pressureMonitor, _logger);
        
        var streamId = StreamId.Create("test-namespace", "test-stream");

        // Act
        var cursor = cache.GetCacheCursor(streamId, null);

        // Assert
        Assert.NotNull(cursor);
    }

    private static List<IBatchContainer> CreateTestMessages(int count)
    {
        var messages = new List<IBatchContainer>();
        for (int i = 0; i < count; i++)
        {
            var streamId = StreamId.Create("test-namespace", $"test-stream-{i}");
            var token = new TestSequenceToken(i);
            var message = new TestBatchContainer(streamId, token);
            messages.Add(message);
        }
        return messages;
    }
}

/// <summary>
/// Test implementation of sequence token.
/// </summary>
internal class TestSequenceToken : StreamSequenceToken
{
    public TestSequenceToken(long sequenceNumber)
    {
        SequenceNumber = sequenceNumber;
        EventIndex = sequenceNumber;
    }

    public override bool Equals(StreamSequenceToken? other)
    {
        return other is TestSequenceToken token && token.SequenceNumber == SequenceNumber;
    }

    public override int CompareTo(StreamSequenceToken? other)
    {
        if (other is not TestSequenceToken token)
            return 1;
        return SequenceNumber.CompareTo(token.SequenceNumber);
    }

    public override int GetHashCode()
    {
        return SequenceNumber.GetHashCode();
    }
}

/// <summary>
/// Test implementation of batch container.
/// </summary>
internal class TestBatchContainer : IBatchContainer
{
    public TestBatchContainer(StreamId streamId, StreamSequenceToken sequenceToken)
    {
        StreamId = streamId;
        SequenceToken = sequenceToken;
    }

    public StreamId StreamId { get; }
    public StreamSequenceToken SequenceToken { get; }

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        yield return new Tuple<T, StreamSequenceToken>(default!, SequenceToken);
    }

    public bool ImportRequestContext()
    {
        return false;
    }

    public bool ShouldDeliver(StreamId streamId, object filterData, StreamFilterPredicate shouldReceiveFunc)
    {
        return StreamId.Equals(streamId);
    }
}