# Microsoft Orleans Azure Service Bus Streaming Provider

This package provides Azure Service Bus streaming support for Microsoft Orleans.

## Configuration

### Queue Topology

```csharp
hostBuilder.UseOrleans(siloBuilder =>
{
    siloBuilder.AddAzureServiceBusStreaming("MyProvider", options =>
    {
        options.ConfigureServiceBusClient("connectionString");
        options.TopologyType = ServiceBusTopologyType.Queue;
        options.QueueName = "myqueue";
        options.MaxConcurrentCalls = 10;
        options.PrefetchCount = 100;
    });
});
```

### Topic/Subscription Topology

```csharp
hostBuilder.UseOrleans(siloBuilder =>
{
    siloBuilder.AddAzureServiceBusStreaming("MyProvider", options =>
    {
        options.ConfigureServiceBusClient("connectionString");
        options.TopologyType = ServiceBusTopologyType.Topic;
        options.TopicName = "mytopic";
        options.SubscriptionName = "mysubscription";
        options.MaxConcurrentCalls = 10;
        options.PrefetchCount = 100;
    });
});
```

### Authentication Options

You can configure authentication using various credential types:

```csharp
// Using connection string
options.ConfigureServiceBusClient("connectionString");

// Using managed identity
options.ConfigureServiceBusClient("namespace.servicebus.windows.net", new DefaultAzureCredential());

// Using shared access signature
options.ConfigureServiceBusClient("namespace.servicebus.windows.net", new AzureSasCredential("sas"));

// Using shared access key
options.ConfigureServiceBusClient("namespace.servicebus.windows.net", new AzureNamedKeyCredential("keyName", "keyValue"));
```

## Features

- Support for both Queue and Topic/Subscription topologies
- Configurable message processing options (prefetch, concurrent calls, etc.)
- Support for all Azure Service Bus authentication methods
- Automatic message completion or manual control
- Dead letter queue support
- Session support
- Duplicate detection