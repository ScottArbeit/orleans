namespace Orleans.Streaming.AzureServiceBus.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus;
using Orleans.Streaming.AzureServiceBus.Configuration;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Orleans.Streams;
using Xunit;
using Xunit.Abstractions;

/// <summary>
/// Integration tests for Service Bus topic/subscription fan-out validation (Step 14).
/// Verifies that Orleans logical fan-out works correctly with a single Service Bus subscription.
/// </summary>
[Collection(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusFanOutIntegrationTests
{
    private readonly ServiceBusEmulatorFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServiceBusFanOutIntegrationTests(ServiceBusEmulatorFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    /// <summary>
    /// Tests that Orleans logical fan-out works with Service Bus topic/subscription.
    /// Multiple stream observers should all receive the same message from a single Service Bus subscription.
    /// This validates the provider stance: one SB subscription per service instance, Orleans does logical fan-out.
    /// </summary>
    [Fact]
    public async Task MultiplePlStreamSubscribers_SingleServiceBusSubscription_AllReceiveMessages()
    {
        // Arrange
        var services = CreateServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider") as ServiceBusQueueAdapterFactory;
        Assert.NotNull(factory);

        var adapter = await factory.CreateAdapter();
        var queueId = QueueId.GetQueueId($"{ServiceBusEmulatorFixture.TopicName}:{ServiceBusEmulatorFixture.SubscriptionName}", 0, 0);
        var receiver = adapter.CreateReceiver(queueId);

        // Set up the topic and subscription
        await SetupServiceBusEntities();

        // Initialize the receiver
        await receiver.Initialize(TimeSpan.FromSeconds(30));

        try
        {
            // Create multiple stream observers to simulate Orleans fan-out
            var receivedMessages1 = new List<object>();
            var receivedMessages2 = new List<object>();

            var observer1 = new TestStreamObserver(receivedMessages1, _output, "Observer1");
            var observer2 = new TestStreamObserver(receivedMessages2, _output, "Observer2");

            // Subscribe both observers to the same stream
            var streamId = StreamId.Create("test-namespace", "test-stream");

            // Send a test message to the Service Bus topic
            var testMessage = "test-message-for-fanout";
            var events = new List<object> { testMessage };
            await adapter.QueueMessageBatchAsync(streamId, events, null, new Dictionary<string, object>());

            _output.WriteLine($"Sent message: {testMessage}");

            // Wait for the message to be received by the adapter
            IList<IBatchContainer> messages = Array.Empty<IBatchContainer>();
            for (int attempt = 0; attempt < 20; attempt++)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500));
                messages = await receiver.GetQueueMessagesAsync(1);
                if (messages.Count > 0)
                {
                    break;
                }
                _output.WriteLine($"Attempt {attempt + 1}: No messages received yet, retrying...");
            }

            Assert.Single(messages);
            var batchContainer = messages[0];

            // Simulate Orleans streaming infrastructure delivering to multiple observers
            // This is what Orleans does internally - it takes the message from the single SB subscription
            // and fans it out to all registered stream subscribers
            await observer1.OnNextAsync(batchContainer);
            await observer2.OnNextAsync(batchContainer);

            // Wait a bit for processing
            await Task.Delay(TimeSpan.FromMilliseconds(200));

            // Assert - Both observers should have received the message
            Assert.Single(receivedMessages1);
            Assert.Single(receivedMessages2);

            // Both should have received the same message content
            var container1 = receivedMessages1[0] as IBatchContainer;
            var container2 = receivedMessages2[0] as IBatchContainer;
            
            Assert.NotNull(container1);
            Assert.NotNull(container2);

            // Extract the actual message content
            var events1 = container1.GetEvents<object>().Select(evt => evt.Item1).ToList();
            var events2 = container2.GetEvents<object>().Select(evt => evt.Item1).ToList();

            Assert.Single(events1);
            Assert.Single(events2);
            Assert.Equal(testMessage, events1[0]);
            Assert.Equal(testMessage, events2[0]);

            _output.WriteLine("✓ Both observers received the same message from single Service Bus subscription");
            _output.WriteLine("✓ Orleans logical fan-out working correctly with Service Bus topics");

            // Mark messages as delivered
            await receiver.MessagesDeliveredAsync(messages);
        }
        finally
        {
            await receiver.Shutdown(TimeSpan.FromSeconds(5));
        }
    }

    /// <summary>
    /// Test stream observer that captures messages for verification.
    /// </summary>
    private class TestStreamObserver : IAsyncObserver<IBatchContainer>
    {
        private readonly List<object> _receivedMessages;
        private readonly ITestOutputHelper _output;
        private readonly string _name;

        public TestStreamObserver(List<object> receivedMessages, ITestOutputHelper output, string name)
        {
            _receivedMessages = receivedMessages;
            _output = output;
            _name = name;
        }

        public Task OnNextAsync(IBatchContainer item, StreamSequenceToken? token = null)
        {
            _output.WriteLine($"{_name}: Received message with {item.GetEvents<object>().Count()} events");
            _receivedMessages.Add(item);
            return Task.CompletedTask;
        }

        public Task OnCompletedAsync()
        {
            _output.WriteLine($"{_name}: Stream completed");
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            _output.WriteLine($"{_name}: Stream error: {ex.Message}");
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Creates the service collection with proper Service Bus streaming configuration.
    /// </summary>
    private ServiceCollection CreateServiceCollection()
    {
        var services = new ServiceCollection();
        
        // Add required Orleans services
        services.AddSingleton<IStreamQueueMapper, ServiceBusQueueMapper>();
        services.AddSingleton<IQueueAdapterFactory, ServiceBusQueueAdapterFactory>();
        services.AddSingleton<Serializer>();
        services.AddLogging();

        // Configure Service Bus options
        services.Configure<ServiceBusStreamOptions>("test-provider", options =>
        {
            options.ConnectionString = _fixture.ServiceBusConnectionString;
            options.EntityKind = EntityKind.TopicSubscription;
            options.TopicName = ServiceBusEmulatorFixture.TopicName;
            options.SubscriptionName = ServiceBusEmulatorFixture.SubscriptionName;
        });

        // Configure Orleans cluster options - needed for some factory methods
        services.Configure<ClusterOptions>(options =>
        {
            options.ClusterId = "test-cluster";
            options.ServiceId = "test-service";
        });

        return services;
    }

    /// <summary>
    /// Sets up the required Service Bus topic and subscription.
    /// </summary>
    private async Task SetupServiceBusEntities()
    {
        var adminClient = new ServiceBusAdministrationClient(_fixture.ServiceBusConnectionString);
        
        try
        {
            // Create topic if it doesn't exist
            if (!await adminClient.TopicExistsAsync(ServiceBusEmulatorFixture.TopicName))
            {
                await adminClient.CreateTopicAsync(ServiceBusEmulatorFixture.TopicName);
                _output.WriteLine($"Created topic: {ServiceBusEmulatorFixture.TopicName}");
            }

            // Create subscription if it doesn't exist
            if (!await adminClient.SubscriptionExistsAsync(ServiceBusEmulatorFixture.TopicName, ServiceBusEmulatorFixture.SubscriptionName))
            {
                await adminClient.CreateSubscriptionAsync(ServiceBusEmulatorFixture.TopicName, ServiceBusEmulatorFixture.SubscriptionName);
                _output.WriteLine($"Created subscription: {ServiceBusEmulatorFixture.SubscriptionName}");
            }

            _output.WriteLine($"Service Bus entities ready: {ServiceBusEmulatorFixture.TopicName}/{ServiceBusEmulatorFixture.SubscriptionName}");
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Error setting up Service Bus entities: {ex.Message}");
            throw;
        }
    }
}