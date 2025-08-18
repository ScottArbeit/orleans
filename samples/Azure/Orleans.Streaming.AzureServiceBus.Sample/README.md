# Orleans Azure Service Bus Streaming Sample

This sample demonstrates how to use the Orleans Azure Service Bus streaming provider for both queue-based and topic/subscription streaming scenarios.

## Overview

The sample includes:
- **Producer grain**: Sends messages to Azure Service Bus streams
- **Consumer grain**: Receives and processes messages from streams  
- **Two streaming modes**: Queue-based (simple) and Topic/Subscription (scalable)
- **Service Bus emulator support**: Uses the Azure Service Bus emulator for local development

## Prerequisites

- .NET 8.0 or later
- Docker Desktop (for Service Bus emulator)

## Running the Sample

### 1. Start the Azure Service Bus Emulator

The sample is configured to use the Service Bus emulator. Start it using Docker:

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
  -e "SQL_SERVER=localhost" \
  -e "SQL_CONNECTION_STRING=Server=localhost;Initial Catalog=SBEmulator;Persist Security Info=False;User ID=sa;Password=YourPassword123!;TrustServerCertificate=true;" \
  -p 5671:5671 \
  -p 5672:5672 \
  mcr.microsoft.com/azure-messaging/servicebus-emulator:latest
```

### 2. Run the Sample

```bash
cd samples/Azure/Orleans.Streaming.AzureServiceBus.Sample
dotnet run
```

### 3. Choose Streaming Mode

The sample will prompt you to choose between:
1. **Queue-based streaming**: Simple messaging with a single queue
2. **Topic/Subscription streaming**: Pub/sub messaging with topics and subscriptions

## Sample Flow

1. **Silo starts** with Service Bus streaming configured
2. **Client connects** to the silo
3. **Consumer grain** subscribes to a stream
4. **Producer grain** sends sample messages to the stream
5. **Consumer grain** receives and processes the messages
6. **Results are displayed** showing message flow

## Key Features Demonstrated

### Configuration
- Connection to Service Bus emulator
- Entity auto-provisioning (queues/topics created automatically)
- Different entity kinds (Queue vs TopicSubscription)

### Message Flow
- Producer grain using `stream.OnNextAsync()` to send messages
- Consumer grain using `stream.SubscribeAsync()` to receive messages
- Proper subscription lifecycle management

### Orleans Integration
- Stream providers and grain-to-stream communication
- Graceful cleanup on grain deactivation
- Error handling in stream processing

## Configuration Details

The sample uses these Service Bus streaming settings:

```csharp
options.ConnectionString = "Endpoint=sb://localhost:5671;..."; // Emulator
options.AutoCreateEntities = true; // Auto-create queues/topics
options.Receiver.MaxConcurrentHandlers = 1; // Preserve message ordering
options.Publisher.BatchSize = 10; // Small batches for demo
```

## Using with Real Azure Service Bus

To use with a real Azure Service Bus namespace:

1. Replace the connection string in `Program.cs`:
```csharp
private const string ServiceBusConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
```

2. Optionally disable auto-creation for production:
```csharp
options.AutoCreateEntities = false;
```

## Sample Output

```
Orleans Azure Service Bus Streaming Sample
==========================================

Choose streaming mode:
1. Queue-based streaming (simple)
2. Topic/Subscription streaming (scalable)
Enter choice (1 or 2): 1

=== Queue-based Streaming Sample ===
Using EntityKind.Queue for simple messaging

✓ Silo started with queue-based streaming
✓ Client connected with queue-based streaming
✓ Consumer grain started, listening to stream: sample-namespace/12345678-...

Producing messages...
  → Sent: Hello from Orleans!
  → Sent: Azure Service Bus streaming works!
  → Sent: This is message #3
  → Sent: Final message in the sample

Waiting for messages to be processed...

✓ Consumer processed 4 messages:
  ← Received: Hello from Orleans! (at 14:23:15)
  ← Received: Azure Service Bus streaming works! (at 14:23:16)
  ← Received: This is message #3 (at 14:23:17)
  ← Received: Final message in the sample (at 14:23:18)

--- Queue Sample Completed ---
```

## Learn More

- [Orleans Streaming Documentation](https://learn.microsoft.com/dotnet/orleans/host/configuration/distributed-tracing)
- [Azure Service Bus Documentation](https://docs.microsoft.com/azure/service-bus/)
- [Orleans Documentation](https://learn.microsoft.com/dotnet/orleans/)