# Microsoft Orleans Azure Service Bus Streaming Provider

This package provides Azure Service Bus streaming support for Microsoft Orleans, enabling reliable, scalable message streaming using Azure Service Bus queues and topics.

## Features

- **Persistent stream provider** backed by Azure Service Bus
- **Support for both queues and topics** for different messaging patterns
- **Multiple authentication methods** including connection strings, managed identity, and custom credentials
- **Built-in retry and error handling** capabilities
- **Configurable partitioning** for horizontal scaling
- **Telemetry integration** with OpenTelemetry and Azure Monitor
- **Full integration** with Orleans streaming infrastructure

## Installation

```bash
dotnet add package Microsoft.Orleans.Streaming.ServiceBus
```

## Quick Start

### Basic Silo Configuration

```csharp
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.UseOrleans(siloBuilder =>
{
    siloBuilder
        .UseLocalhostClustering()
        .AddServiceBusStreams("ServiceBusProvider", options =>
        {
            options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...";
            options.EntityType = ServiceBusEntityType.Queue;
            options.PartitionCount = 8; // Number of queues/topics to create
        });
});

await builder.Build().RunAsync();
```

### Basic Client Configuration

```csharp
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.UseOrleansClient(clientBuilder =>
{
    clientBuilder
        .UseLocalhostClustering()
        .AddServiceBusStreams("ServiceBusProvider", options =>
        {
            options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...";
        });
});

await builder.Build().RunAsync();
```

### Example Producer Grain (C# 13 Primary Constructor)

```csharp
using Orleans.Streams;

public interface IProducerGrain : IGrainWithGuidKey
{
    Task ProduceMessage(string message);
}

public class ProducerGrain(ILogger<ProducerGrain> logger) : Grain, IProducerGrain
{
    private IAsyncStream<string>? _stream;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var streamProvider = this.GetStreamProvider("ServiceBusProvider");
        _stream = streamProvider.GetStream<string>(this.GetPrimaryKey(), "MyStreamNamespace");
        return Task.CompletedTask;
    }

    public async Task ProduceMessage(string message)
    {
        logger.LogInformation("Producing message: {Message}", message);
        await _stream!.OnNextAsync(message);
    }
}
```

### Example Consumer Grain (C# 13 Primary Constructor)

```csharp
using Orleans.Streams;

public interface IConsumerGrain : IGrainWithGuidKey
{
    Task StartConsuming();
    Task StopConsuming();
}

public class ConsumerGrain(ILogger<ConsumerGrain> logger) : Grain, IConsumerGrain, IAsyncObserver<string>
{
    private StreamSubscriptionHandle<string>? _subscription;

    public async Task StartConsuming()
    {
        var streamProvider = this.GetStreamProvider("ServiceBusProvider");
        var stream = streamProvider.GetStream<string>(this.GetPrimaryKey(), "MyStreamNamespace");
        _subscription = await stream.SubscribeAsync(this);
        logger.LogInformation("Started consuming messages");
    }

    public async Task StopConsuming()
    {
        if (_subscription is not null)
        {
            await _subscription.UnsubscribeAsync();
            _subscription = null;
            logger.LogInformation("Stopped consuming messages");
        }
    }

    public Task OnNextAsync(string item, StreamSequenceToken? token = null)
    {
        logger.LogInformation("Received message: {Message} with token: {Token}", item, token);
        return Task.CompletedTask;
    }

    public Task OnCompletedAsync()
    {
        logger.LogInformation("Stream completed");
        return Task.CompletedTask;
    }

    public Task OnErrorAsync(Exception ex)
    {
        logger.LogError(ex, "Stream error occurred");
        return Task.CompletedTask;
    }
}
```

## Configuration Matrix

### Authentication Options

| Method | Configuration | Use Case |
|--------|---------------|----------|
| **Connection String** | `options.ConnectionString = "..."` | Development, testing with shared access keys |
| **Managed Identity** | `options.ConfigureServiceBusClient(namespace, credential)` | Production with Azure AD authentication |
| **Custom Credential** | `options.ConfigureServiceBusClient(createClientCallback)` | Custom authentication scenarios |
| **Existing Client** | `options.ServiceBusClient = existingClient` | Reuse existing configured client |

### Entity Configuration

| Entity Type | Configuration | Best For |
|-------------|---------------|----------|
| **Queue** | `options.EntityType = ServiceBusEntityType.Queue` | Point-to-point messaging, guaranteed delivery |
| **Topic** | `options.EntityType = ServiceBusEntityType.Topic` | Publish-subscribe patterns, multiple consumers |

### Advanced Configuration Options

```csharp
siloBuilder.AddServiceBusStreams("ServiceBusProvider", configurator =>
{
    configurator.ConfigureServiceBus(optionsBuilder =>
    {
        optionsBuilder.Configure(options =>
        {
            // Authentication
            options.ConfigureServiceBusClient("your-connection-string");
            
            // Entity configuration
            options.EntityType = ServiceBusEntityType.Topic;
            options.PartitionCount = 16;
            options.QueueNamePrefix = "orleans-streams";
            options.SubscriptionNamePrefix = "orleans-consumer";
            
            // Performance tuning
            options.PrefetchCount = 100;
            options.ReceiveBatchSize = 10;
            options.MaxConcurrentCalls = Environment.ProcessorCount * 2;
            options.MaxWaitTime = TimeSpan.FromSeconds(10);
            
            // Reliability
            options.MaxDeliveryAttempts = 5;
        });
    });
    
    // Configure caching (silo only)
    configurator.ConfigureCacheSize(1000);
});
```

## Telemetry Hooks

The Service Bus provider includes built-in telemetry through OpenTelemetry:

### Activity Source Configuration

```csharp
using System.Diagnostics;
using Orleans.Configuration;

// The provider exposes telemetry through this ActivitySource
var activitySource = ServiceBusOptions.ActivitySource;

// Configure OpenTelemetry to capture Service Bus activities
builder.Services.AddOpenTelemetry()
    .WithTracing(tracingBuilder =>
    {
        tracingBuilder
            .AddSource(ServiceBusOptions.ActivitySource.Name)
            .AddAzureMonitorTraceExporter(); // Or your preferred exporter
    });
```

### Custom Telemetry Integration

```csharp
// Access telemetry in your grains
public class TelemetryAwareGrain(ILogger<TelemetryAwareGrain> logger) : Grain
{
    public async Task ProcessWithTelemetry(string data)
    {
        using var activity = ServiceBusOptions.ActivitySource.StartActivity("ProcessMessage");
        activity?.SetTag("grain.type", nameof(TelemetryAwareGrain));
        activity?.SetTag("data.length", data.Length);
        
        try
        {
            // Your processing logic here
            await Task.Delay(100);
            logger.LogInformation("Processed data with telemetry");
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }
}
```

## Migration from Azure Storage Queues (ASQ)

If you're currently using Azure Storage Queues (`Orleans.Streaming.AzureStorage`), here's how to migrate:

### 1. Update Package References

```xml
<!-- Remove -->
<PackageReference Include="Microsoft.Orleans.Streaming.AzureStorage" Version="x.x.x" />

<!-- Add -->
<PackageReference Include="Microsoft.Orleans.Streaming.ServiceBus" Version="x.x.x" />
```

### 2. Update Silo Configuration

**Before (ASQ):**
```csharp
siloBuilder.AddAzureQueueStreams("AzureQueueProvider", optionsBuilder =>
{
    optionsBuilder.Configure(options =>
    {
        options.ConnectionString = "DefaultEndpointsProtocol=https;AccountName=...";
        options.QueueNames = ["queue0", "queue1", "queue2", "queue3"];
    });
});
```

**After (ASB):**
```csharp
siloBuilder.AddServiceBusStreams("ServiceBusProvider", options =>
{
    options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;...";
    options.EntityType = ServiceBusEntityType.Queue;
    options.PartitionCount = 4; // Equivalent to 4 queues
    options.QueueNamePrefix = "orleans-stream"; // Will create: orleans-stream-0, orleans-stream-1, etc.
});
```

### 3. Update Client Configuration

**Before (ASQ):**
```csharp
clientBuilder.AddAzureQueueStreams("AzureQueueProvider", optionsBuilder =>
{
    optionsBuilder.Configure(options =>
    {
        options.ConnectionString = "DefaultEndpointsProtocol=https;AccountName=...";
    });
});
```

**After (ASB):**
```csharp
clientBuilder.AddServiceBusStreams("ServiceBusProvider", options =>
{
    options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;...";
});
```

### 4. Update Grain Code

The grain code remains largely the same, just update the provider name:

```csharp
// Change the provider name from "AzureQueueProvider" to "ServiceBusProvider"
var streamProvider = this.GetStreamProvider("ServiceBusProvider");
```

### 5. Key Differences

| Aspect | Azure Storage Queues | Azure Service Bus |
|--------|----------------------|-------------------|
| **Message Size** | Up to 64KB | Up to 1MB (256KB default) |
| **Message TTL** | 7 days maximum | Configurable, up to TimeSpan.MaxValue |
| **Throughput** | ~2,000 msg/sec per queue | ~20,000+ msg/sec per queue |
| **Ordering** | No guarantees | FIFO available with sessions |
| **Dead Letter** | Manual implementation | Built-in dead letter queues |
| **Cost** | Lower cost per message | Higher cost but better features |

### 6. Testing Migration

Create a simple test to verify the migration:

```csharp
public class MigrationTestGrain(ILogger<MigrationTestGrain> logger) : Grain
{
    public async Task TestServiceBusStreams()
    {
        var streamProvider = this.GetStreamProvider("ServiceBusProvider");
        var stream = streamProvider.GetStream<string>(Guid.NewGuid(), "test-namespace");
        
        // Test producer
        await stream.OnNextAsync("Migration test message");
        logger.LogInformation("Successfully sent message to Service Bus stream");
        
        // Test consumer
        await stream.SubscribeAsync((msg, token) =>
        {
            logger.LogInformation("Successfully received message: {Message}", msg);
            return Task.CompletedTask;
        });
    }
}
```

## Advanced Topics

### Using Topics with Multiple Subscriptions

```csharp
siloBuilder.AddServiceBusStreams("ServiceBusProvider", configurator =>
{
    configurator.ConfigureServiceBus(optionsBuilder =>
    {
        optionsBuilder.Configure(options =>
        {
            options.EntityType = ServiceBusEntityType.Topic;
            options.EntityName = "orleans-events";
            options.SubscriptionNames = ["consumer-1", "consumer-2", "consumer-3"];
        });
    });
});
```

### Custom Message Serialization

```csharp
siloBuilder.AddServiceBusStreams("ServiceBusProvider", configurator =>
{
    // Configure custom data adapter for message serialization
    configurator.ConfigureQueueDataAdapter<CustomServiceBusDataAdapter>();
});
```

## Documentation

For more information, see:
- [Orleans Streaming Documentation](https://learn.microsoft.com/en-us/dotnet/orleans/streaming/)
- [Azure Service Bus Documentation](https://learn.microsoft.com/en-us/azure/service-bus-messaging/)
- [Orleans Samples Repository](https://github.com/dotnet/orleans/tree/main/samples)