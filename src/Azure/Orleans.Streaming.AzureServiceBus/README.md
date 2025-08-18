# Microsoft Orleans Streaming for Azure Service Bus

> **⚠️ Non-Rewindable Provider**  
> This Azure Service Bus streaming provider is **non-rewindable** and follows ASQ/SQS semantics. It does **not** support rewind, offsets, or Event Hubs-style replay functionality. Messages are processed once and cannot be replayed from previous positions in the stream.

A streaming provider for Microsoft Orleans that uses Azure Service Bus queues and topics as the underlying transport.

## About Orleans

Microsoft Orleans is a cross-platform framework for building robust, scalable distributed applications. Orleans builds on the .NET platform and provides a powerful and flexible programming model for building distributed, high-throughput, low-latency applications.

For more information, visit the [Orleans documentation](https://learn.microsoft.com/dotnet/orleans/).

## About Azure Service Bus

Azure Service Bus is a fully managed enterprise message broker with message queues and publish-subscribe topics. It provides reliable message queuing and durable pub/sub messaging.

## Differences vs Event Hubs Provider

| Feature | Azure Service Bus Provider | Event Hubs Provider |
|---------|---------------------------|-------------------|
| **Rewind Support** | ❌ No rewind capability | ✅ Full rewind and offset support |
| **Message Semantics** | At-least-once delivery | At-least-once delivery |
| **Ordering** | Per-queue/subscription ordering | Per-partition ordering |
| **Use Case** | Traditional messaging, fire-and-forget | Event sourcing, replay scenarios |
| **Positioning** | Like ASQ/SQS providers | Event streaming with history |

**Choose Service Bus when:** You need traditional messaging patterns without replay requirements.  
**Choose Event Hubs when:** You need event streaming with rewind and historical data access.

## Quick Start

### Installation

```bash
dotnet add package Microsoft.Orleans.Streaming.AzureServiceBus
```

### Queue-based Streaming (Simple)

**Silo Configuration:**
```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.UseOrleans(silo =>
{
    silo.UseLocalhostClustering()
        .AddServiceBusStreams("ServiceBusProvider", options =>
        {
            options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
            options.EntityKind = EntityKind.Queue;
            options.QueueName = "orleans-streaming-queue";
        });
});

var host = builder.Build();
```

**Client Configuration:**
```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.UseOrleansClient(client =>
{
    client.UseLocalhostClustering()
          .AddServiceBusStreams("ServiceBusProvider", options =>
          {
              options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
              options.EntityKind = EntityKind.Queue;
              options.QueueName = "orleans-streaming-queue";
          });
});

var host = builder.Build();
```

### Topic/Subscription Streaming (Scalable)

**Silo Configuration:**
```csharp
silo.AddServiceBusStreams("ServiceBusProvider", options =>
{
    options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
    options.EntityKind = EntityKind.TopicSubscription;
    options.TopicName = "orleans-events";
    options.SubscriptionName = "silo-subscription";
});
```

**Client Configuration:**
```csharp
client.AddServiceBusStreams("ServiceBusProvider", options =>
{
    options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
    options.EntityKind = EntityKind.TopicSubscription;
    options.TopicName = "orleans-events";
    options.SubscriptionName = "client-subscription";
});
```

## Configuration Options

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ConnectionString` | `string` | `""` | Service Bus connection string (Option 1) |
| `FullyQualifiedNamespace` | `string` | `""` | Service Bus namespace (Option 2, use with Credential) |
| `Credential` | `TokenCredential` | `null` | Azure credential for authentication (Option 2) |

### Entity Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `EntityKind` | `EntityKind` | `Queue` | Whether to use Queue or TopicSubscription |
| `QueueName` | `string` | `"orleans-stream"` | Queue name (when EntityKind = Queue) |
| `TopicName` | `string` | `""` | Topic name (when EntityKind = TopicSubscription) |
| `SubscriptionName` | `string` | `""` | Subscription name (when EntityKind = TopicSubscription) |
| `EntityCount` | `int` | `1` | Number of entities for partitioning/scaling |
| `EntityNamePrefix` | `string` | `""` | Prefix for entity names when EntityCount > 1 |
| `AutoCreateEntities` | `bool` | `true` | Whether to auto-create queues/topics if they don't exist |

### Publisher Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Publisher.BatchSize` | `int` | `100` | Number of messages to batch when publishing |
| `Publisher.MessageTimeToLive` | `TimeSpan` | `14 days` | TTL for published messages |
| `Publisher.SessionIdStrategy` | `SessionIdStrategy` | `None` | Whether to use session IDs for ordering |
| `Publisher.PropertiesPrefix` | `string` | `"orleans_"` | Prefix for custom message properties |

### Receiver Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Receiver.PrefetchCount` | `int` | `0` | Number of messages to prefetch |
| `Receiver.ReceiveBatchSize` | `int` | `32` | Number of messages to receive in each batch |
| `Receiver.MaxConcurrentHandlers` | `int` | `1` | Max concurrent message handlers (affects ordering) |
| `Receiver.LockAutoRenew` | `bool` | `true` | Whether to automatically renew message locks |
| `Receiver.LockRenewalDuration` | `TimeSpan` | `5 minutes` | Duration for lock renewal |
| `Receiver.MaxDeliveryCount` | `int` | `10` | Max delivery attempts (documentation only) |
| `Receiver.CacheDrainTimeout` | `TimeSpan` | `30 seconds` | Graceful shutdown timeout for cache drain |

### Cache Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Cache.MaxCacheSize` | `int` | `4096` | Maximum number of items in cache |
| `Cache.CacheEvictionAge` | `TimeSpan` | `10 minutes` | Age at which cache items are evicted |
| `Cache.CachePressureSoft` | `double` | `0.7` | Soft pressure threshold for eviction |
| `Cache.CachePressureHard` | `double` | `0.9` | Hard pressure threshold for eviction |

### Dead Letter Handling

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `DeadLetterHandling.LogDeadLetters` | `bool` | `true` | Whether to log dead letter events |
| `DeadLetterHandling.SurfaceMetrics` | `bool` | `true` | Whether to emit metrics for dead letters |
| `DeadLetterHandling.ForwardAddress` | `string` | `""` | Forward address for dead letters (Service Bus only) |

### Read-Only Properties

| Property | Type | Value | Description |
|----------|------|-------|-------------|
| `AllowRewind` | `bool` | `false` | Always false - this provider doesn't support rewind |

## Message Ordering & Concurrency

### Default Behavior (Ordered)
- **MaxConcurrentHandlers = 1**: Ensures strict message ordering within each queue/subscription
- **Trade-off**: Lower throughput but guaranteed order
- **Use when**: Message order is critical for your application logic

### High Throughput (Unordered)
```csharp
options.Receiver.MaxConcurrentHandlers = 16; // or higher
```
- **Result**: Higher throughput but no ordering guarantees
- **Trade-off**: Better performance but messages may be processed out of order
- **Use when**: Throughput is more important than ordering

### Per-Stream Ordering (Advanced)
```csharp
options.Publisher.SessionIdStrategy = SessionIdStrategy.UseStreamId;
options.Receiver.MaxConcurrentHandlers = 16;
// Note: Requires session-enabled Service Bus entities
```
- **Result**: Per-stream ordering with higher overall throughput
- **Trade-off**: More complex setup but best of both worlds
- **Limitation**: Full session support is not included in MVP

## Auto-Provisioning

When `AutoCreateEntities = true` (default), the provider will automatically create Service Bus entities if they don't exist:

### Queue Mode
```csharp
options.EntityKind = EntityKind.Queue;
options.QueueName = "my-queue";
options.AutoCreateEntities = true; // Will create "my-queue" if it doesn't exist
```

### Topic/Subscription Mode
```csharp
options.EntityKind = EntityKind.TopicSubscription;
options.TopicName = "my-topic";
options.SubscriptionName = "my-subscription";
options.AutoCreateEntities = true; // Will create both topic and subscription
```

### Multiple Entities (Scaling)
```csharp
options.EntityCount = 3;
options.EntityNamePrefix = "region-east";
// Creates: region-east-0, region-east-1, region-east-2
```

### Production Considerations
- **Disable in production**: Set `AutoCreateEntities = false` for production environments
- **Pre-create entities**: Use Azure CLI, ARM templates, or Terraform for production deployment
- **Permissions**: Ensure the service principal has appropriate permissions when auto-provisioning is enabled

## Dead Letter Queue (DLQ) Guidance

Service Bus automatically handles dead letter queues for failed message processing:

### Automatic DLQ Behavior
- Messages that exceed `MaxDeliveryCount` are moved to the dead letter queue
- Messages that expire (exceed TTL) are moved to the dead letter queue
- Messages that cause processing exceptions may be dead lettered

### Monitoring Dead Letters
```csharp
options.DeadLetterHandling.LogDeadLetters = true; // Logs DLQ events
options.DeadLetterHandling.SurfaceMetrics = true; // Emits metrics
```

### Accessing Dead Letter Messages
```csharp
// Use Azure Service Bus SDK directly to access DLQ
var serviceBusClient = new ServiceBusClient(connectionString);
var deadLetterReceiver = serviceBusClient.CreateReceiver(queueName, 
    new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
```

### Best Practices
- **Monitor DLQ size**: Set up alerts for dead letter queue depth
- **Investigate patterns**: Regularly check DLQ for recurring issues
- **Remediation**: Fix issues and replay messages from DLQ when appropriate
- **Retention**: Configure appropriate TTL for dead letter messages

## Azure Service Bus Emulator

For development and testing, you can use the Azure Service Bus emulator. The Orleans test suite includes a `ServiceBusEmulatorFixture` that demonstrates how to set up the emulator with Docker.

### Prerequisites
- Docker Desktop or Docker Engine
- .NET 8.0 or later

### Using the Emulator Fixture

The `ServiceBusEmulatorFixture` provides a complete setup with both SQL Server (required dependency) and Service Bus emulator:

```csharp
public class MyServiceBusTest : IClassFixture<ServiceBusEmulatorFixture>
{
    private readonly ServiceBusEmulatorFixture _fixture;

    public MyServiceBusTest(ServiceBusEmulatorFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task TestStreamingWithEmulator()
    {
        // Use the emulator connection string
        var connectionString = _fixture.ServiceBusConnectionString;
        
        // Configure Orleans with emulator
        var silo = new HostBuilder()
            .UseOrleans(builder =>
            {
                builder.UseLocalhostClustering()
                       .AddServiceBusStreams("ServiceBusProvider", options =>
                       {
                           options.ConnectionString = connectionString;
                           options.EntityKind = EntityKind.Queue;
                           options.QueueName = ServiceBusEmulatorFixture.QueueName; // "test-queue"
                       });
            })
            .Build();
    }
}
```

### Manual Emulator Setup

If you prefer to run the emulator manually:

```bash
# Start SQL Server (required dependency)
docker run -d --name sqlserver \
  -e "ACCEPT_EULA=Y" \
  -e "SA_PASSWORD=YourPassword123!" \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest

# Start Service Bus Emulator
docker run -d --name servicebus-emulator \
  -e "ACCEPT_EULA=Y" \
  -e "SQL_SERVER=sqlserver" \
  -e "SQL_CONNECTION_STRING=Server=sqlserver;Initial Catalog=SBEmulator;Persist Security Info=False;User ID=sa;Password=YourPassword123!;TrustServerCertificate=true;" \
  -p 5671:5671 \
  -p 5672:5672 \
  --link sqlserver \
  mcr.microsoft.com/azure-messaging/servicebus-emulator:latest
```

### Emulator Connection String

When using the emulator, use this connection string format:
```
Endpoint=sb://localhost:5671;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
```

### Emulator Limitations
- Some dead letter features may be no-op in the emulator
- Production Service Bus features may not be fully supported
- Performance characteristics differ from the actual service

## Topic/Subscription Fan-out Semantics

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