# Microsoft Orleans Streaming for Azure Service Bus

A streaming provider for Microsoft Orleans that uses Azure Service Bus queues as the underlying transport.

## About Orleans

Microsoft Orleans is a cross-platform framework for building robust, scalable distributed applications. Orleans builds on the .NET platform and provides a powerful and flexible programming model for building distributed, high-throughput, low-latency applications.

For more information, visit the [Orleans documentation](https://learn.microsoft.com/dotnet/orleans/).

## About Azure Service Bus

Azure Service Bus is a fully managed enterprise message broker with message queues and publish-subscribe topics. It provides reliable message queuing and durable pub/sub messaging.

## Getting Started

This package provides streaming capabilities for Orleans applications using Azure Service Bus as the message transport.

### Installation

```bash
dotnet add package Microsoft.Orleans.Streaming.AzureServiceBus
```

### Basic Configuration

Configuration will be documented here once the implementation is complete.

### Topic/Subscription Fan-out Semantics

### Provider Stance: Orleans Logical Fan-out vs Service Bus Physical Fan-out

The Azure Service Bus streaming provider follows a specific approach for handling multiple Orleans stream subscribers:

**One Service Bus subscription per service instance (entity)** - Orleans performs logical fan-out to stream subscribers. The provider does **NOT** create one Service Bus subscription per Orleans subscriber.

#### How It Works

1. **Service Bus Level**: Each service instance uses one Azure Service Bus subscription per configured entity
2. **Orleans Level**: Multiple Orleans grains can subscribe to the same stream
3. **Message Flow**: 
   - Messages are sent to the Service Bus topic
   - The single Service Bus subscription receives each message once
   - Orleans streaming infrastructure receives the message and fans it out to all registered Orleans grain subscribers
   - Each Orleans grain subscriber receives the message

#### Example Configuration

For multiple entities (Step 10):
```csharp
services.AddOrleans(builder =>
{
    builder.UseServiceBusStreaming("ConnectionString", options =>
    {
        options.EntityKind = EntityKind.TopicSubscription;
        options.TopicName = "my-topic";
        options.EntityCount = 3; // Creates 3 subscriptions: my-topic:prefix-sub-0, my-topic:prefix-sub-1, my-topic:prefix-sub-2
        options.EntityNamePrefix = "prefix";
    });
});
```

#### Implications and Recommended Patterns

**Benefits:**
- **Efficiency**: Reduces Service Bus subscription management overhead
- **Cost**: Lower Service Bus costs (fewer subscriptions to manage)
- **Simplicity**: Orleans handles the complexity of fan-out internally
- **Compatibility**: Works seamlessly with Orleans' existing streaming abstractions

**Considerations:**
- **Throughput**: All Orleans subscribers on the same stream share the throughput of the single Service Bus subscription
- **Message Ordering**: Per-session message ordering is maintained within each Service Bus subscription
- **Scaling**: Scale out by increasing `EntityCount` to create more Service Bus subscriptions for load distribution

**Recommended Patterns:**
- Use multiple entities (`EntityCount > 1`) for high-throughput scenarios
- Group related streams that can share throughput characteristics
- Consider subscription-level configuration (MaxDeliveryCount, TTL) affects all Orleans subscribers
- Design message handlers to be idempotent as Service Bus provides at-least-once delivery semantics

For information about configuring failure handling, retry behavior, and dead letter queue management, see [FAILURE_HANDLING.md](FAILURE_HANDLING.md).

### Graceful Shutdown and At-Least-Once Semantics

The Azure Service Bus streaming provider implements graceful shutdown with deterministic behavior:

#### Shutdown Process
1. **Stop Fetching**: The background message pump stops receiving new messages from Service Bus
2. **Cache Draining**: Messages already in the internal cache are processed up to a configurable deadline
3. **Message Abandonment**: After the deadline, remaining unprocessed messages are abandoned for redelivery
4. **Resource Cleanup**: Service Bus clients and resources are disposed cleanly

#### Configuration
You can configure the cache drain timeout during shutdown:

```csharp
services.AddOrleans(builder =>
{
    builder.UseServiceBusStreaming("ConnectionString", options =>
    {
        options.Receiver.CacheDrainTimeout = TimeSpan.FromSeconds(30); // Default: 30 seconds
    });
});
```

#### At-Least-Once Guarantees
- Messages are delivered **at least once** but may be delivered multiple times
- During shutdown, unprocessed messages are abandoned and will be redelivered after restart
- This ensures no message loss beyond the inherent at-least-once semantics of Service Bus
- Applications should design message handlers to be idempotent to handle potential duplicates

#### Cancellation Support
- The streaming provider respects `CancellationToken` for clean shutdown
- Lock renewal tasks are cancelled gracefully during shutdown
- Background operations stop promptly when cancellation is requested

## Requirements

- .NET 8.0 or later
- Azure Service Bus namespace

## Feedback & Contributing

- If you have any issues or would like to provide feedback, please [open an issue on GitHub](https://github.com/dotnet/orleans/issues)
- Join our community on [Discord](https://aka.ms/orleans-discord)
- Follow the [@msftorleans](https://twitter.com/msftorleans) Twitter account for Orleans announcements
- Contributions are welcome! Please review our [contribution guidelines](https://github.com/dotnet/orleans/blob/main/CONTRIBUTING.md)
- This project is licensed under the [MIT license](https://github.com/dotnet/orleans/blob/main/LICENSE)