namespace Orleans.Streaming.AzureServiceBus.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

/// <summary>
/// Integration tests for ordering guarantees in Azure Service Bus streaming.
/// Tests demonstrate the trade-offs between ordering and concurrency.
/// </summary>
[Collection(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusStreamOrderingTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly ServiceBusEmulatorFixture _fixture;
    private TestCluster? _cluster;
    private ILogger<ServiceBusStreamOrderingTests> _logger = null!;

    public ServiceBusStreamOrderingTests(ServiceBusEmulatorFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<ServiceBusStreamOrderingTests>();

        await _fixture.InitializeAsync();
    }

    public async Task DisposeAsync()
    {
        if (_cluster is not null)
        {
            await _cluster.DisposeAsync();
        }
    }

    [Fact]
    public async Task Messages_WithMaxConcurrentHandlers1_AreReceivedInOrder()
    {
        // Arrange
        const int messageCount = 10;
        const string streamNamespace = "test-namespace";
        const string streamKey = "test-stream";

        _cluster = CreateTestCluster(maxConcurrentHandlers: 1);
        await _cluster.DeployAsync();

        var client = _cluster.Client;
        var streamProvider = client.GetStreamProvider("ServiceBusProvider");
        var stream = streamProvider.GetStream<int>(streamNamespace, streamKey);

        var receivedMessages = new List<int>();
        var receivedAllMessages = new ManualResetEventSlim(false);

        // Subscribe to receive messages
        await stream.SubscribeAsync((message, token) =>
        {
            _logger.LogInformation("Received message: {Message}", message);
            receivedMessages.Add(message);
            
            if (receivedMessages.Count == messageCount)
            {
                receivedAllMessages.Set();
            }
            
            return Task.CompletedTask;
        });

        // Allow some time for subscription to be established
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Act - Publish messages in order
        _logger.LogInformation("Publishing {MessageCount} messages", messageCount);
        for (int i = 1; i <= messageCount; i++)
        {
            await stream.OnNextAsync(i);
            _logger.LogInformation("Published message: {Message}", i);
        }

        // Wait for all messages to be received
        var received = receivedAllMessages.Wait(TimeSpan.FromSeconds(30));

        // Assert
        Assert.True(received, "Not all messages were received within timeout");
        Assert.Equal(messageCount, receivedMessages.Count);
        
        // With MaxConcurrentHandlers = 1, messages should be received in order
        for (int i = 0; i < messageCount; i++)
        {
            Assert.Equal(i + 1, receivedMessages[i]);
        }

        _logger.LogInformation("All messages received in correct order: [{Messages}]", string.Join(", ", receivedMessages));
    }

    [Fact]
    public async Task Messages_WithMaxConcurrentHandlers4_MayNotBeReceivedInOrder()
    {
        // Arrange
        const int messageCount = 20;
        const string streamNamespace = "test-namespace";
        const string streamKey = "test-stream-concurrent";

        _cluster = CreateTestCluster(maxConcurrentHandlers: 4);
        await _cluster.DeployAsync();

        var client = _cluster.Client;
        var streamProvider = client.GetStreamProvider("ServiceBusProvider");
        var stream = streamProvider.GetStream<int>(streamNamespace, streamKey);

        var receivedMessages = new List<int>();
        var receivedAllMessages = new ManualResetEventSlim(false);
        var lockObject = new object();

        // Subscribe to receive messages with artificial delay to encourage reordering
        await stream.SubscribeAsync(async (message, token) =>
        {
            // Add random delay to encourage reordering with concurrent handlers
            var delay = Random.Shared.Next(1, 50);
            await Task.Delay(delay);
            
            _logger.LogInformation("Received message: {Message} after {Delay}ms delay", message, delay);
            
            lock (lockObject)
            {
                receivedMessages.Add(message);
                if (receivedMessages.Count == messageCount)
                {
                    receivedAllMessages.Set();
                }
            }
            
            return;
        });

        // Allow some time for subscription to be established
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Act - Publish messages in order
        _logger.LogInformation("Publishing {MessageCount} messages with MaxConcurrentHandlers = 4", messageCount);
        for (int i = 1; i <= messageCount; i++)
        {
            await stream.OnNextAsync(i);
            _logger.LogInformation("Published message: {Message}", i);
        }

        // Wait for all messages to be received
        var received = receivedAllMessages.Wait(TimeSpan.FromSeconds(60));

        // Assert
        Assert.True(received, "Not all messages were received within timeout");
        Assert.Equal(messageCount, receivedMessages.Count);
        
        // With MaxConcurrentHandlers > 1, we can't guarantee order
        // This test documents the behavior - messages may arrive out of order
        var isInOrder = receivedMessages.SequenceEqual(Enumerable.Range(1, messageCount));
        
        _logger.LogInformation("Messages received: [{Messages}]", string.Join(", ", receivedMessages));
        _logger.LogInformation("Messages were received in order: {InOrder}", isInOrder);
        
        // We don't assert order here since concurrent processing may reorder them
        // This test serves as documentation of the behavior
        Assert.True(receivedMessages.All(m => m >= 1 && m <= messageCount), "All published messages should be received");
    }

    private TestCluster CreateTestCluster(int maxConcurrentHandlers)
    {
        var builder = new TestClusterBuilder();
        
        builder.AddSiloBuilderConfigurator<TestClusterConfigurator>();
        builder.ConfigureHostConfiguration(config =>
        {
            config.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionString"] = _fixture.ServiceBusConnectionString,
                ["MaxConcurrentHandlers"] = maxConcurrentHandlers.ToString()
            });
        });

        return builder.Build();
    }

    private class TestClusterConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddServiceBusStreams("ServiceBusProvider", configurator =>
            {
                configurator.ConfigureServiceBus(optionsBuilder =>
                {
                    optionsBuilder.Configure<IConfiguration>((options, config) =>
                    {
                        options.ConnectionString = config["ConnectionString"]!;
                        options.EntityKind = EntityKind.Queue;
                        options.QueueName = ServiceBusEmulatorFixture.QueueName;
                        
                        // Configure concurrency from test configuration
                        if (int.TryParse(config["MaxConcurrentHandlers"], out var maxConcurrentHandlers))
                        {
                            options.Receiver.MaxConcurrentHandlers = maxConcurrentHandlers;
                        }
                    });
                });
            });
        }
    }
}