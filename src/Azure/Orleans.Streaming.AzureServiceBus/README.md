# Microsoft Orleans Streaming for Azure Service Bus

## Introduction
Microsoft Orleans Streaming for Azure Service Bus provides a stream provider implementation for Orleans using Azure Service Bus. This allows for publishing and subscribing to streams of events with Azure Service Bus topics and subscriptions as the underlying messaging infrastructure.

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
                configureOptions: options =>
                {
                    options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
                    options.TopicName = "orleans-streaming";
                });
    });

// Run the host
await builder.RunAsync();
```

## Example - Using Azure Service Bus Streams in a Grain
```csharp
using Orleans;
using Orleans.Streams;

public interface IMyGrain : IGrainWithIntegerKey
{
    Task PublishEvent(string message);
    Task StartConsuming();
}

public class MyGrain : Grain, IMyGrain
{
    private IAsyncStream<string> _stream;

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var streamProvider = this.GetStreamProvider("AzureServiceBusStreamProvider");
        _stream = streamProvider.GetStream<string>("my-namespace", this.GetPrimaryKeyLong());
        await base.OnActivateAsync(cancellationToken);
    }

    public async Task PublishEvent(string message)
    {
        await _stream.OnNextAsync(message);
    }

    public async Task StartConsuming()
    {
        await _stream.SubscribeAsync(OnMessageReceived);
    }

    private Task OnMessageReceived(string message, StreamSequenceToken token)
    {
        // Process the message
        return Task.CompletedTask;
    }
}
```

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