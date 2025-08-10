# Azure Service Bus Streaming Provider - Configuration Guide

This guide covers the comprehensive configuration options available for the Azure Service Bus streaming provider in Orleans.

## Basic Usage

### Silo Configuration

```csharp
siloBuilder.AddServiceBusStreams("ServiceBusProvider", options =>
{
    options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
    options.EntityKind = EntityKind.Queue;
    options.QueueName = "orleans-streaming-queue";
});
```

### Client Configuration

```csharp
clientBuilder.AddServiceBusStreams("ServiceBusProvider", options =>
{
    options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
    options.EntityKind = EntityKind.Queue;
    options.QueueName = "orleans-streaming-queue";
});
```

## Configuration Options

### Connection Configuration

Two options are available for connecting to Azure Service Bus:

#### Option 1: Connection String
```csharp
options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
```

#### Option 2: Fully Qualified Namespace + Credential
```csharp
options.FullyQualifiedNamespace = "your-namespace.servicebus.windows.net";
options.Credential = new DefaultAzureCredential(); // or any TokenCredential
```

### Entity Configuration

#### Queue-based Streaming
```csharp
options.EntityKind = EntityKind.Queue;
options.QueueName = "my-streaming-queue";
```

#### Topic/Subscription-based Streaming
```csharp
options.EntityKind = EntityKind.TopicSubscription;
options.TopicName = "my-streaming-topic";
options.SubscriptionName = "my-subscription";
```

### Publisher Settings

```csharp
options.Publisher.BatchSize = 100;                                    // Number of messages per batch
options.Publisher.MessageTimeToLive = TimeSpan.FromDays(7);          // Message TTL
options.Publisher.SessionIdStrategy = SessionIdStrategy.UseStreamId;  // Session strategy
options.Publisher.PropertiesPrefix = "orleans_";                     // Custom properties prefix
```

### Receiver Settings

```csharp
options.Receiver.PrefetchCount = 50;                           // Number of messages to prefetch
options.Receiver.ReceiveBatchSize = 32;                        // Messages received per batch
options.Receiver.MaxConcurrentHandlers = 1;                    // Must be 1 in MVP for order preservation
options.Receiver.LockAutoRenew = true;                         // Auto-renew message locks
options.Receiver.LockRenewalDuration = TimeSpan.FromMinutes(5); // Lock renewal duration
options.Receiver.MaxDeliveryCount = 10;                        // Max delivery attempts (documentation)
```

### Cache Settings

```csharp
options.Cache.MaxCacheSize = 4096;                         // Maximum cache size
options.Cache.CacheEvictionAge = TimeSpan.FromMinutes(10); // Cache item eviction age
options.Cache.CachePressureSoft = 0.7;                     // Soft pressure threshold (70%)
options.Cache.CachePressureHard = 0.9;                     // Hard pressure threshold (90%)
```

### Entity Management

```csharp
options.AutoCreateEntities = true;        // Automatically create queues/topics if they don't exist
options.EntityCount = 1;                  // Number of entities for partitioning
options.EntityNamePrefix = "orleans-";    // Prefix for entity names when using multiple entities
```

### Dead Letter Handling

```csharp
options.DeadLetterHandling.LogDeadLetters = true;        // Log dead letter events
options.DeadLetterHandling.SurfaceMetrics = true;        // Surface metrics for dead letters
options.DeadLetterHandling.ForwardAddress = "";          // Optional forward address (real Service Bus only)
```

## Advanced Configuration

### Using the Configurator Pattern

```csharp
siloBuilder.AddServiceBusStreams("ServiceBusProvider", configurator =>
{
    configurator.ConfigureServiceBus(ob => ob.Configure(options =>
    {
        options.ConnectionString = "...";
        options.EntityKind = EntityKind.Queue;
        options.QueueName = "my-queue";
    }));
    
    configurator.ConfigureCache(2048);          // Set cache size
    configurator.ConfigurePartitioning(4);      // Set number of partitions
});
```

## Environment Variable Overrides

For testing and configuration flexibility, you can override settings using environment variables following this pattern:

```
Orleans__Streaming__ServiceBus__{ProviderName}__{PropertyPath}
```

Examples:
- `Orleans__Streaming__ServiceBus__MyProvider__ConnectionString`
- `Orleans__Streaming__ServiceBus__MyProvider__EntityKind`
- `Orleans__Streaming__ServiceBus__MyProvider__Publisher__BatchSize`
- `Orleans__Streaming__ServiceBus__MyProvider__Receiver__MaxConcurrentHandlers`
- `Orleans__Streaming__ServiceBus__MyProvider__Cache__MaxCacheSize`

For nested configuration objects, use double underscores to separate levels.

## Important Notes

- **AllowRewind**: Always `false` for Service Bus provider (no rewind capability)
- **MaxConcurrentHandlers**: Capped to 1 in MVP to preserve message order
- **Entity Creation**: When `AutoCreateEntities` is true, the provider will attempt to create queues/topics if they don't exist
- **Dead Letter Handling**: Some features may be no-op in the Service Bus emulator
- **Credential vs Connection String**: Use only one authentication method, not both

## Example: Production Configuration

```csharp
siloBuilder.AddServiceBusStreams("ProductionServiceBus", options =>
{
    // Use managed identity for authentication
    options.FullyQualifiedNamespace = "mycompany-prod.servicebus.windows.net";
    options.Credential = new ManagedIdentityCredential();
    
    // Use topic/subscription for better scalability
    options.EntityKind = EntityKind.TopicSubscription;
    options.TopicName = "orleans-events";
    options.SubscriptionName = "silo-subscription";
    
    // Optimize for production throughput
    options.Publisher.BatchSize = 200;
    options.Receiver.PrefetchCount = 100;
    options.Receiver.ReceiveBatchSize = 50;
    
    // Enhanced caching
    options.Cache.MaxCacheSize = 8192;
    options.Cache.CacheEvictionAge = TimeSpan.FromMinutes(30);
    
    // Don't auto-create in production
    options.AutoCreateEntities = false;
    
    // Enable comprehensive monitoring
    options.DeadLetterHandling.LogDeadLetters = true;
    options.DeadLetterHandling.SurfaceMetrics = true;
});
```