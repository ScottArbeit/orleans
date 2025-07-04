# Microsoft Orleans Streaming for Azure Service Bus

## Introduction
Microsoft Orleans Streaming for Azure Service Bus provides a stream provider implementation for Orleans using Azure Service Bus. This allows for publishing and subscribing to streams of events with Azure Service Bus queues and topics/subscriptions as the underlying messaging infrastructure, leveraging Service Bus's enterprise-grade features like sessions, dead letter queues, and message scheduling.

## Getting Started
To use this package, install it via NuGet:

```shell
dotnet add package Microsoft.Orleans.Streaming.AzureServiceBus
```

## Example - Configuring Azure Service Bus Streaming
```csharp
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Streams;

var builder = Host.CreateApplicationBuilder(args)
    .UseOrleans(siloBuilder =>
    {
        siloBuilder
            .UseLocalhostClustering()
            // Configure Azure Service Bus as a stream provider
            .AddAzureServiceBusStreams(
                name: "AzureServiceBusStreamProvider", 
                b => b.ConfigureAzureServiceBus(ob => ob.Configure((options, dep) =>
                {
                    options.ConfigureTestDefaults();
                    options.ConnectionString = "YOUR_SERVICE_BUS_CONNECTION_STRING";
                    options.QueueNames = Enumerable.Range(0, 8).Select(num => $"{dep.Value.ClusterId}-queue-{num}").ToList();
                })));
    });

// Run the host
await builder.RunAsync();
```

## Example - Using Azure Service Bus Streams in a Grain
```csharp
using Orleans.Streams;

public interface IMyGrain : IGrainWithGuidKey
{
    Task PublishMessage(string message);
    Task SubscribeToStream();
}

public class MyGrain : Grain, IMyGrain
{
    public async Task PublishMessage(string message)
    {
        var streamProvider = this.GetStreamProvider("AzureServiceBusStreamProvider");
        var stream = streamProvider.GetStream<string>("MyStreamNamespace", this.GetPrimaryKey());
        await stream.OnNextAsync(message);
    }

    public async Task SubscribeToStream()
    {
        var streamProvider = this.GetStreamProvider("AzureServiceBusStreamProvider");
        var stream = streamProvider.GetStream<string>("MyStreamNamespace", this.GetPrimaryKey());
        
        await stream.SubscribeAsync(async (message, token) =>
        {
            // Process the received message
            Console.WriteLine($"Received message: {message}");
        });
    }
}
```

## Features
- **Point-to-Point Messaging**: Support for Azure Service Bus queues for reliable message delivery
- **Publish-Subscribe Messaging**: Support for Azure Service Bus topics and subscriptions
- **Enterprise Features**: Leverage Service Bus sessions, dead letter queues, message scheduling, and duplicate detection
- **Scalable**: Automatic scaling based on message load and queue depth
- **Reliable**: Built-in retry policies and dead letter handling
- **Secure**: Support for Azure Active Directory authentication and Managed Identity

## Configuration Options
- **ConnectionString**: Azure Service Bus connection string
- **QueueNames**: List of queue names to use for point-to-point messaging
- **TopicName**: Topic name for publish-subscribe scenarios
- **SubscriptionNames**: List of subscription names for topic-based messaging
- **SessionsEnabled**: Enable session-based message processing
- **MaxConcurrentCalls**: Maximum number of concurrent message processing calls
- **AutoCompleteMessages**: Whether to auto-complete messages after processing

## Documentation
For more comprehensive documentation, please refer to:
- [Microsoft Orleans Documentation](https://learn.microsoft.com/dotnet/orleans/)
- [Orleans Streams](https://learn.microsoft.com/en-us/dotnet/orleans/streaming/index)
- [Stream Providers](https://learn.microsoft.com/en-us/dotnet/orleans/streaming/stream-providers)
- [Azure Service Bus Documentation](https://learn.microsoft.com/en-us/azure/service-bus-messaging/)

## Feedback & Contributing
- If you have any issues or would like to provide feedback, please [open an issue on GitHub](https://github.com/dotnet/orleans/issues)
- Join our community on [Discord](https://aka.ms/orleans-discord)
- Follow the [@msftorleans](https://twitter.com/msftorleans) Twitter account for Orleans announcements
- Contributions are welcome! Please review our [contribution guidelines](https://github.com/dotnet/orleans/blob/main/CONTRIBUTING.md)
- This project is licensed under the [MIT license](https://github.com/dotnet/orleans/blob/main/LICENSE)