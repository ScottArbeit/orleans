using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus;
using Orleans.Streaming.AzureServiceBus.Sample.Grains;
using Orleans.Streams;
using System.Text.Json;

namespace Orleans.Streaming.AzureServiceBus.Sample;

/// <summary>
/// Sample application demonstrating Azure Service Bus streaming with Orleans.
/// This sample shows both queue-based and topic/subscription streaming patterns.
/// </summary>
class Program
{
    private const string ServiceBusConnectionString = "Endpoint=sb://localhost:5671;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
    private const string StreamProviderName = "ServiceBusProvider";
    private const string StreamNamespace = "sample-namespace";

    static async Task Main(string[] args)
    {
        Console.WriteLine("Orleans Azure Service Bus Streaming Sample");
        Console.WriteLine("==========================================");
        Console.WriteLine();

        try
        {
            Console.WriteLine("Choose streaming mode:");
            Console.WriteLine("1. Queue-based streaming (simple)");
            Console.WriteLine("2. Topic/Subscription streaming (scalable)");
            Console.Write("Enter choice (1 or 2): ");
            
            var choice = Console.ReadLine();
            var useTopicSubscription = choice == "2";

            if (useTopicSubscription)
            {
                await RunTopicSubscriptionSampleAsync();
            }
            else
            {
                await RunQueueSampleAsync();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Sample failed: {ex.Message}");
            Console.WriteLine(ex.ToString());
        }

        Console.WriteLine("\nPress any key to exit...");
        Console.ReadKey();
    }

    /// <summary>
    /// Demonstrates queue-based streaming - simpler setup, single queue.
    /// </summary>
    static async Task RunQueueSampleAsync()
    {
        Console.WriteLine("\n=== Queue-based Streaming Sample ===");
        Console.WriteLine("Using EntityKind.Queue for simple messaging");
        Console.WriteLine();

        // Start silo
        using var silo = await StartSiloAsync(EntityKind.Queue);
        Console.WriteLine("✓ Silo started with queue-based streaming");

        // Start client
        using var client = await StartClientAsync(EntityKind.Queue);
        Console.WriteLine("✓ Client connected with queue-based streaming");

        // Run the sample
        await RunStreamingSampleAsync(client, "Queue Sample");
    }

    /// <summary>
    /// Demonstrates topic/subscription streaming - more scalable, supports multiple subscribers.
    /// </summary>
    static async Task RunTopicSubscriptionSampleAsync()
    {
        Console.WriteLine("\n=== Topic/Subscription Streaming Sample ===");
        Console.WriteLine("Using EntityKind.TopicSubscription for pub/sub messaging");
        Console.WriteLine();

        // Start silo
        using var silo = await StartSiloAsync(EntityKind.TopicSubscription);
        Console.WriteLine("✓ Silo started with topic/subscription streaming");

        // Start client  
        using var client = await StartClientAsync(EntityKind.TopicSubscription);
        Console.WriteLine("✓ Client connected with topic/subscription streaming");

        // Run the sample
        await RunStreamingSampleAsync(client, "Topic/Subscription Sample");
    }

    /// <summary>
    /// Starts an Orleans silo with Azure Service Bus streaming configured.
    /// </summary>
    static async Task<IHost> StartSiloAsync(EntityKind entityKind)
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Logging.ClearProviders()
                       .AddConsole()
                       .SetMinimumLevel(LogLevel.Warning); // Reduce noise for sample

        builder.UseOrleans(silo =>
        {
            silo.UseLocalhostClustering(siloPort: 11111, gatewayPort: 30000, primarySiloEndpoint: null)
                .AddServiceBusStreams(StreamProviderName, options =>
                {
                    ConfigureServiceBusOptions(options, entityKind);
                });
                //.UseDashboard(options => { }); // Optional: enable Orleans dashboard when available
        });

        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    /// <summary>
    /// Starts an Orleans client with Azure Service Bus streaming configured.
    /// </summary>
    static async Task<IHost> StartClientAsync(EntityKind entityKind)
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Logging.ClearProviders()
                       .AddConsole()
                       .SetMinimumLevel(LogLevel.Warning); // Reduce noise for sample

        builder.UseOrleansClient(client =>
        {
            client.UseLocalhostClustering(gatewayPort: 30000)
                  .AddServiceBusStreams(StreamProviderName, options =>
                  {
                      ConfigureServiceBusOptions(options, entityKind);
                  });
        });

        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    /// <summary>
    /// Configures Service Bus streaming options based on the entity kind.
    /// </summary>
    static void ConfigureServiceBusOptions(ServiceBusStreamOptions options, EntityKind entityKind)
    {
        // Connection - using emulator for this sample
        options.ConnectionString = ServiceBusConnectionString;

        // Entity configuration
        options.EntityKind = entityKind;
        
        if (entityKind == EntityKind.Queue)
        {
            options.QueueName = "orleans-sample-queue";
        }
        else
        {
            options.TopicName = "orleans-sample-topic";
            options.SubscriptionName = "sample-subscription";
        }

        // Auto-create entities for sample (disable in production)
        options.AutoCreateEntities = true;

        // Performance tuning for sample
        options.Receiver.MaxConcurrentHandlers = 1; // Preserve ordering
        options.Publisher.BatchSize = 10; // Smaller batches for demo
        options.Cache.MaxCacheSize = 1000; // Smaller cache for demo
    }

    /// <summary>
    /// Runs the actual streaming sample with producer and consumer grains.
    /// </summary>
    static async Task RunStreamingSampleAsync(IHost client, string sampleName)
    {
        var grainFactory = client.Services.GetRequiredService<IGrainFactory>();
        var streamProvider = client.Services.GetRequiredService<IStreamProvider>();

        Console.WriteLine($"\n--- {sampleName} Started ---");

        // Get grains
        var producer = grainFactory.GetGrain<IProducerGrain>(0);
        var consumer = grainFactory.GetGrain<IConsumerGrain>(0);

        // Configure the consumer to listen to the stream
        var streamId = StreamId.Create(StreamNamespace, Guid.NewGuid());
        await consumer.StartConsuming(StreamProviderName, streamId);

        Console.WriteLine($"✓ Consumer grain started, listening to stream: {streamId}");

        // Produce some messages
        Console.WriteLine("\nProducing messages...");
        var messages = new[]
        {
            new SampleMessage("Hello from Orleans!", DateTime.UtcNow),
            new SampleMessage("Azure Service Bus streaming works!", DateTime.UtcNow),
            new SampleMessage("This is message #3", DateTime.UtcNow),
            new SampleMessage("Final message in the sample", DateTime.UtcNow)
        };

        foreach (var message in messages)
        {
            await producer.ProduceMessage(StreamProviderName, streamId, message);
            Console.WriteLine($"  → Sent: {message.Content}");
            await Task.Delay(1000); // Small delay to see ordering
        }

        // Give time for processing
        Console.WriteLine("\nWaiting for messages to be processed...");
        await Task.Delay(3000);

        // Get consumption results
        var consumedMessages = await consumer.GetConsumedMessages();
        Console.WriteLine($"\n✓ Consumer processed {consumedMessages.Count} messages:");
        
        foreach (var msg in consumedMessages)
        {
            Console.WriteLine($"  ← Received: {msg.Content} (at {msg.Timestamp:HH:mm:ss})");
        }

        // Stop the consumer
        await consumer.StopConsuming();
        Console.WriteLine($"\n--- {sampleName} Completed ---");
    }
}

/// <summary>
/// Sample message type for the streaming demonstration.
/// </summary>
[Serializable]
[GenerateSerializer]
public record SampleMessage(
    [property: Id(0)] string Content,
    [property: Id(1)] DateTime Timestamp
);