# Azure Service Bus Cache Components

This directory contains the cache implementation for Azure Service Bus streaming in Orleans.

## Overview

The Azure Service Bus cache implementation provides efficient message caching with configurable eviction policies and pressure monitoring, specifically designed for Azure Service Bus delivery guarantees and characteristics.

## Key Components

### Core Classes

- **`AzureServiceBusQueueCache`**: Main adapter cache implementing `IQueueAdapterCache`
- **`ServiceBusQueueCache`**: Individual queue cache implementing `IQueueCache`
- **`ServiceBusQueueCacheCursor`**: Cursor for stream navigation implementing `IQueueCacheCursor`

### Cache Management

- **`MessageCacheEntry`**: Represents cached messages with metadata and access tracking
- **`CacheEvictionPolicy`**: Configurable eviction strategies (LRU, FIFO, TTL)
- **`ServiceBusCachePressureMonitor`**: Monitors cache pressure and provides backpressure signals

### Factory and Configuration

- **`AzureServiceBusQueueCacheFactory`**: Factory for creating cache components
- **`AzureServiceBusCacheOptions`**: Configuration options for cache behavior

## Features

### Eviction Strategies

1. **Time-Based Eviction**
   - Message age-based eviction (TTL)
   - Idle time-based eviction
   - Configurable time thresholds

2. **Size-Based Eviction**
   - Maximum message count limits
   - Memory usage limits
   - LRU and FIFO strategies

3. **Pressure-Based Eviction**
   - Adaptive cache sizing based on system pressure
   - Memory pressure monitoring
   - Processing rate adaptation

### Cache Pressure Monitoring

The pressure monitoring system tracks:
- Memory usage and message count
- Processing rate and backlog
- System pressure indicators
- Adaptive threshold management

### Configuration Options

```csharp
var options = new AzureServiceBusCacheOptions
{
    MaxCacheSize = 4096,                    // Maximum number of messages
    MaxMemorySizeBytes = 64 * 1024 * 1024,  // 64 MB memory limit
    MessageTTL = TimeSpan.FromMinutes(30),   // Message time-to-live
    MaxIdleTime = TimeSpan.FromMinutes(10),  // Maximum idle time
    EvictionStrategy = CacheEvictionStrategy.LRU,
    PressureThreshold = 0.8                  // Pressure threshold (0.0-1.0)
};
```

## Usage

### Creating a Cache Adapter

```csharp
var factory = new AzureServiceBusQueueCacheFactory(
    "my-provider",
    loggerFactory,
    cacheOptions);

var adapter = factory.CreateQueueCacheAdapter();
```

### Using the Cache

```csharp
// Create cache for a queue
var cache = adapter.CreateQueueCache(queueId);

// Add messages
cache.AddToCache(messages);

// Get cursor for stream navigation
var cursor = cache.GetCacheCursor(streamId, startToken);

// Check pressure
if (cache.IsUnderPressure())
{
    // Handle backpressure
}

// Purge expired messages
cache.TryPurgeFromCache(out var purgedItems);
```

## Integration with Orleans Streaming

The cache implementation integrates seamlessly with Orleans streaming:

- Implements standard Orleans cache interfaces (`IQueueAdapterCache`, `IQueueCache`)
- Supports Orleans sequence tokens and stream identification
- Provides cursor-based stream navigation
- Reports metrics through Orleans cache monitoring interfaces

## Performance Considerations

- Uses efficient data structures (Dictionary + LinkedList for LRU)
- Minimizes memory allocations in hot paths
- Thread-safe operations with appropriate locking
- Configurable cache sizes and eviction policies
- Pressure-based adaptive behavior

## Testing

The implementation includes comprehensive unit tests:

- Cache creation and initialization
- Message addition, retrieval, and eviction
- Pressure monitoring and adaptive behavior
- Configuration validation
- Integration scenarios

## Azure Service Bus Specific Considerations

- Designed for Azure Service Bus delivery guarantees (at-least-once)
- Handles Service Bus message ordering within sessions
- Optimized for Service Bus batch message patterns
- Considers Service Bus connection and receiver limits
- Supports Service Bus dead letter queue scenarios