# Microsoft Orleans Azure Service Bus Streaming Provider

This package provides Azure Service Bus streaming support for Microsoft Orleans.

## Features

- Persistent stream provider backed by Azure Service Bus
- Support for both queues and topics
- Built-in retry and error handling capabilities
- Integration with Orleans streaming infrastructure

## Installation

```bash
dotnet add package Microsoft.Orleans.Streaming.ServiceBus
```

## Usage

### Silo Configuration

```csharp
siloBuilder.AddServiceBusStreams("StreamProvider", options =>
{
    options.ConnectionString = "your-service-bus-connection-string";
});
```

### Client Configuration

```csharp
clientBuilder.AddServiceBusStreams("StreamProvider", options =>
{
    options.ConnectionString = "your-service-bus-connection-string";
});
```

## Documentation

For more information, see the [Orleans documentation](https://orleans.docs.microsoft.com/).