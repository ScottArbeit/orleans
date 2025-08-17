using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Streaming.AzureServiceBus.Tests;

/// <summary>
/// End-to-end tests that verify auto-provisioned entities can successfully send and receive messages.
/// </summary>
[Collection(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusAutoProvisioningE2ETests
{
    private readonly ServiceBusEmulatorFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServiceBusAutoProvisioningE2ETests(ServiceBusEmulatorFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task E2E_AutoProvisioned_Queue_Should_Send_And_Receive_Messages()
    {
        // Arrange
        var services = new ServiceCollection();
        var randomQueueName = $"test-e2e-queue-{Guid.NewGuid():N}";
        
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = _fixture.ServiceBusConnectionString,
            AutoCreateEntities = true,
            EntityKind = EntityKind.Queue,
            QueueName = randomQueueName,
            EntityCount = 1
        };

        // Add Orleans serialization services
        services.AddSerializer();
        services.AddSingleton<ILogger<ServiceBusQueueAdapterFactory>>(new TestLogger<ServiceBusQueueAdapterFactory>(_output));
        services.AddSingleton<ILoggerFactory>(new TestLoggerFactory(_output));
        services.AddSingleton<IOptionsMonitor<ServiceBusStreamOptions>>(new TestOptionsMonitor<ServiceBusStreamOptions>("testProvider", options));

        var serviceProvider = services.BuildServiceProvider();
        var factory = new ServiceBusQueueAdapterFactory(serviceProvider, "testProvider");

        // Act - Create adapter which should auto-provision the queue
        var adapter = await factory.CreateAdapter();
        Assert.NotNull(adapter);

        // Send a test message to the auto-provisioned queue
        var streamId = StreamId.Create("test-namespace", $"test-stream-{Guid.NewGuid()}");
        var events = new List<string> { "Hello", "World", "From", "Auto-Provisioned", "Queue" };
        var requestContext = new Dictionary<string, object> { { "test", "e2e" } };

        await adapter.QueueMessageBatchAsync(streamId, events, null, requestContext);

        _output.WriteLine($"Successfully sent {events.Count} messages to auto-provisioned queue: {randomQueueName}");

        // Verify the message was actually sent by receiving it directly from Service Bus
        await using var client = new ServiceBusClient(_fixture.ServiceBusConnectionString);
        var receiver = client.CreateReceiver(randomQueueName);

        var receivedMessage = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(receivedMessage);

        _output.WriteLine($"Successfully received message from auto-provisioned queue. Body length: {receivedMessage.Body.Length}");

        // Clean up
        await receiver.CompleteMessageAsync(receivedMessage);
        await receiver.DisposeAsync();
    }

    [Fact]
    public async Task E2E_AutoProvisioned_Topic_Should_Send_And_Receive_Messages()
    {
        // Arrange
        var services = new ServiceCollection();
        var randomTopicName = $"test-e2e-topic-{Guid.NewGuid():N}";
        var randomSubscriptionName = $"test-e2e-sub-{Guid.NewGuid():N}";
        
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = _fixture.ServiceBusConnectionString,
            AutoCreateEntities = true,
            EntityKind = EntityKind.TopicSubscription,
            TopicName = randomTopicName,
            SubscriptionName = randomSubscriptionName,
            EntityCount = 1
        };

        // Add Orleans serialization services
        services.AddSerializer();
        services.AddSingleton<ILogger<ServiceBusQueueAdapterFactory>>(new TestLogger<ServiceBusQueueAdapterFactory>(_output));
        services.AddSingleton<ILoggerFactory>(new TestLoggerFactory(_output));
        services.AddSingleton<IOptionsMonitor<ServiceBusStreamOptions>>(new TestOptionsMonitor<ServiceBusStreamOptions>("testProvider", options));

        var serviceProvider = services.BuildServiceProvider();
        var factory = new ServiceBusQueueAdapterFactory(serviceProvider, "testProvider");

        // Act - Create adapter which should auto-provision the topic and subscription
        var adapter = await factory.CreateAdapter();
        Assert.NotNull(adapter);

        // Send a test message to the auto-provisioned topic
        var streamId = StreamId.Create("test-namespace", $"test-stream-{Guid.NewGuid()}");
        var events = new List<string> { "Hello", "World", "From", "Auto-Provisioned", "Topic" };
        var requestContext = new Dictionary<string, object> { { "test", "e2e-topic" } };

        await adapter.QueueMessageBatchAsync(streamId, events, null, requestContext);

        _output.WriteLine($"Successfully sent {events.Count} messages to auto-provisioned topic: {randomTopicName}");

        // Verify the message was actually sent by receiving it directly from Service Bus
        await using var client = new ServiceBusClient(_fixture.ServiceBusConnectionString);
        var receiver = client.CreateReceiver(randomTopicName, randomSubscriptionName);

        var receivedMessage = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(receivedMessage);

        _output.WriteLine($"Successfully received message from auto-provisioned subscription. Body length: {receivedMessage.Body.Length}");

        // Clean up
        await receiver.CompleteMessageAsync(receivedMessage);
        await receiver.DisposeAsync();
    }
}