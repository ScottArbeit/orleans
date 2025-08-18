using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus.Sample.Grains;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Orleans.Streams;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Streaming.AzureServiceBus.Sample.Tests;

/// <summary>
/// Smoke tests for the Azure Service Bus streaming sample using the emulator.
/// These tests verify that the sample components work correctly together.
/// </summary>
public class SampleSmokeTests : IClassFixture<ServiceBusEmulatorFixture>
{
    private readonly ServiceBusEmulatorFixture _fixture;
    private readonly ITestOutputHelper _output;

    public SampleSmokeTests(ServiceBusEmulatorFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact, TestCategory("Functional"), TestCategory("AzureServiceBus")]
    public async Task QueueStreaming_ProducerAndConsumer_ShouldWorkCorrectly()
    {
        // Arrange
        const string streamProviderName = "TestServiceBusProvider";
        const string streamNamespace = "test-namespace";
        var streamId = StreamId.Create(streamNamespace, Guid.NewGuid());

        using var silo = await StartTestSiloAsync(streamProviderName, EntityKind.Queue);
        using var client = await StartTestClientAsync(streamProviderName, EntityKind.Queue);

        var grainFactory = client.Services.GetRequiredService<IGrainFactory>();
        var producer = grainFactory.GetGrain<IProducerGrain>(0);
        var consumer = grainFactory.GetGrain<IConsumerGrain>(0);

        // Act - Start consuming
        await consumer.StartConsuming(streamProviderName, streamId);

        // Act - Produce messages
        var testMessages = new[]
        {
            new SampleMessage("Test message 1", DateTime.UtcNow),
            new SampleMessage("Test message 2", DateTime.UtcNow),
            new SampleMessage("Test message 3", DateTime.UtcNow)
        };

        foreach (var message in testMessages)
        {
            await producer.ProduceMessage(streamProviderName, streamId, message);
        }

        // Give time for processing
        await Task.Delay(2000);

        // Assert
        var consumedMessages = await consumer.GetConsumedMessages();
        var sentCount = await producer.GetMessagesSentCount();
        var consumedCount = await consumer.GetMessagesConsumedCount();

        Assert.Equal(testMessages.Length, sentCount);
        Assert.Equal(testMessages.Length, consumedCount);
        Assert.Equal(testMessages.Length, consumedMessages.Count);

        // Verify message content
        for (int i = 0; i < testMessages.Length; i++)
        {
            Assert.Equal(testMessages[i].Content, consumedMessages[i].Content);
        }

        _output.WriteLine($"✓ Successfully processed {consumedMessages.Count} messages via queue streaming");

        // Cleanup
        await consumer.StopConsuming();
    }

    [Fact, TestCategory("Functional"), TestCategory("AzureServiceBus")]
    public async Task TopicSubscriptionStreaming_ProducerAndConsumer_ShouldWorkCorrectly()
    {
        // Arrange
        const string streamProviderName = "TestServiceBusTopicProvider";
        const string streamNamespace = "test-topic-namespace";
        var streamId = StreamId.Create(streamNamespace, Guid.NewGuid());

        using var silo = await StartTestSiloAsync(streamProviderName, EntityKind.TopicSubscription);
        using var client = await StartTestClientAsync(streamProviderName, EntityKind.TopicSubscription);

        var grainFactory = client.Services.GetRequiredService<IGrainFactory>();
        var producer = grainFactory.GetGrain<IProducerGrain>(1); // Different grain instance
        var consumer = grainFactory.GetGrain<IConsumerGrain>(1);

        // Act - Start consuming
        await consumer.StartConsuming(streamProviderName, streamId);

        // Act - Produce messages
        var testMessages = new[]
        {
            new SampleMessage("Topic message 1", DateTime.UtcNow),
            new SampleMessage("Topic message 2", DateTime.UtcNow)
        };

        foreach (var message in testMessages)
        {
            await producer.ProduceMessage(streamProviderName, streamId, message);
        }

        // Give time for processing
        await Task.Delay(2000);

        // Assert
        var consumedMessages = await consumer.GetConsumedMessages();
        var sentCount = await producer.GetMessagesSentCount();
        var consumedCount = await consumer.GetMessagesConsumedCount();

        Assert.Equal(testMessages.Length, sentCount);
        Assert.Equal(testMessages.Length, consumedCount);
        Assert.Equal(testMessages.Length, consumedMessages.Count);

        // Verify message content
        for (int i = 0; i < testMessages.Length; i++)
        {
            Assert.Equal(testMessages[i].Content, consumedMessages[i].Content);
        }

        _output.WriteLine($"✓ Successfully processed {consumedMessages.Count} messages via topic/subscription streaming");

        // Cleanup
        await consumer.StopConsuming();
    }

    private async Task<IHost> StartTestSiloAsync(string streamProviderName, EntityKind entityKind)
    {
        var builder = Host.CreateApplicationBuilder();

        // Minimize logging for tests
        builder.Logging.ClearProviders()
                       .AddConsole()
                       .SetMinimumLevel(LogLevel.Error);

        builder.UseOrleans(silo =>
        {
            silo.UseLocalhostClustering()
                .AddServiceBusStreams(streamProviderName, options =>
                {
                    options.ConnectionString = _fixture.ServiceBusConnectionString;
                    options.EntityKind = entityKind;

                    if (entityKind == EntityKind.Queue)
                    {
                        options.QueueName = ServiceBusEmulatorFixture.QueueName;
                    }
                    else
                    {
                        options.TopicName = ServiceBusEmulatorFixture.TopicName;
                        options.SubscriptionName = ServiceBusEmulatorFixture.SubscriptionName;
                    }

                    options.AutoCreateEntities = true;
                    options.Receiver.MaxConcurrentHandlers = 1; // Preserve ordering for tests
                });
        });

        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    private async Task<IHost> StartTestClientAsync(string streamProviderName, EntityKind entityKind)
    {
        var builder = Host.CreateApplicationBuilder();

        // Minimize logging for tests
        builder.Logging.ClearProviders()
                       .AddConsole()
                       .SetMinimumLevel(LogLevel.Error);

        builder.UseOrleansClient(client =>
        {
            client.UseLocalhostClustering()
                  .AddServiceBusStreams(streamProviderName, options =>
                  {
                      options.ConnectionString = _fixture.ServiceBusConnectionString;
                      options.EntityKind = entityKind;

                      if (entityKind == EntityKind.Queue)
                      {
                          options.QueueName = ServiceBusEmulatorFixture.QueueName;
                      }
                      else
                      {
                          options.TopicName = ServiceBusEmulatorFixture.TopicName;
                          options.SubscriptionName = ServiceBusEmulatorFixture.SubscriptionName;
                      }

                      options.AutoCreateEntities = true;
                      options.Receiver.MaxConcurrentHandlers = 1; // Preserve ordering for tests
                  });
        });

        var host = builder.Build();
        await host.StartAsync();
        return host;
    }
}