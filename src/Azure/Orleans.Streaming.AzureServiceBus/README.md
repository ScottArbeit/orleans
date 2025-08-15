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

### Failure Handling and Dead Letter Queues

For information about configuring failure handling, retry behavior, and dead letter queue management, see [FAILURE_HANDLING.md](FAILURE_HANDLING.md).

## Requirements

- .NET 8.0 or later
- Azure Service Bus namespace

## Feedback & Contributing

- If you have any issues or would like to provide feedback, please [open an issue on GitHub](https://github.com/dotnet/orleans/issues)
- Join our community on [Discord](https://aka.ms/orleans-discord)
- Follow the [@msftorleans](https://twitter.com/msftorleans) Twitter account for Orleans announcements
- Contributions are welcome! Please review our [contribution guidelines](https://github.com/dotnet/orleans/blob/main/CONTRIBUTING.md)
- This project is licensed under the [MIT license](https://github.com/dotnet/orleans/blob/main/LICENSE)