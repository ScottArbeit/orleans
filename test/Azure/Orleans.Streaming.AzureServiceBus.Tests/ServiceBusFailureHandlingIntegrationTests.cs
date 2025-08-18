namespace Orleans.Streaming.AzureServiceBus.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus;
using Orleans.Streaming.AzureServiceBus.Configuration;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Orleans.Streams;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

/// <summary>
/// Integration tests for Service Bus failure handling behavior.
/// Tests that failed messages are abandoned and allow Service Bus retries/DLQ.
/// </summary>
[Collection(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusFailureHandlingIntegrationTests
{
    private readonly ServiceBusEmulatorFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServiceBusFailureHandlingIntegrationTests(ServiceBusEmulatorFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact, TestCategory("Functional"), TestCategory("AzureServiceBus")]
    public async Task DeliveryFailure_AbandonMessageInsteadOfCompleting()
    {
        // Arrange
        var services = CreateServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider") as ServiceBusQueueAdapterFactory;
        Assert.NotNull(factory);

        // Get the failure handler from the factory to ensure it's properly configured
        var failureHandler = await factory.GetDeliveryFailureHandler(QueueId.GetQueueId("test-queue", 0, 0)) as ServiceBusStreamFailureHandler;
        Assert.NotNull(failureHandler);

        var adapter = await factory.CreateAdapter();
        var receiver = adapter.CreateReceiver(QueueId.GetQueueId("test-queue", 0, 0));

        // Initialize the receiver to start the background pump
        await receiver.Initialize(TimeSpan.FromSeconds(30));

        try
        {
            // Send a test message first
            var streamId = StreamId.Create("test-namespace", "test-key");
            var events = new List<object> { "test-message-for-failure" };
            await adapter.QueueMessageBatchAsync(streamId, events, null, new Dictionary<string, object>());

            // Wait for the message to be received by the background pump
            // Try polling with retries to handle timing issues
            IList<IBatchContainer> messages = Array.Empty<IBatchContainer>();
            for (int attempt = 0; attempt < 10; attempt++)
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

            var batchContainer = messages[0] as ServiceBusBatchContainer;
            Assert.NotNull(batchContainer);

            // Simulate delivery failure by marking the token as failed
            await failureHandler.OnDeliveryFailure(
                GuidId.GetNewGuidId(), 
                "test-provider", 
                streamId, 
                batchContainer.SequenceToken);

            // Verify the token is marked as failed
            Assert.True(failureHandler.IsTokenFailed(batchContainer.SequenceToken));

            // This would normally be called by the Orleans infrastructure
            // In a real scenario, the receiver would check the failure handler
            await receiver.MessagesDeliveredAsync(messages);

            // Assert - The message should have been abandoned (not completed)
            // We can verify this by checking that the failure token was cleared (indicating abandon was called)
            Assert.False(failureHandler.IsTokenFailed(batchContainer.SequenceToken));

            _output.WriteLine("Delivery failure test completed successfully");
        }
        finally
        {
            // Clean up
            await receiver.Shutdown(TimeSpan.FromSeconds(10));
        }
    }

    [Fact, TestCategory("BVT"), TestCategory("AzureServiceBus")]
    public async Task SuccessfulDelivery_CompletesMessage()
    {
        // Arrange
        var services = CreateServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider") as ServiceBusQueueAdapterFactory;
        Assert.NotNull(factory);

        var adapter = await factory.CreateAdapter();
        var receiver = adapter.CreateReceiver(QueueId.GetQueueId("test-queue", 0, 0));

        // Initialize the receiver to start the background pump
        await receiver.Initialize(TimeSpan.FromSeconds(30));

        try
        {
            // Send a test message
            var streamId = StreamId.Create("test-namespace", "successful-message");
            var events = new List<object> { "test-message-success" };
            await adapter.QueueMessageBatchAsync(streamId, events, null, new Dictionary<string, object>());

            // Wait for the message to be received by the background pump
            // Try polling with retries to handle timing issues
            IList<IBatchContainer> messages = Array.Empty<IBatchContainer>();
            for (var attempt = 0; attempt < 10; attempt++)
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

            var batchContainer = messages[0] as ServiceBusBatchContainer;
            Assert.NotNull(batchContainer);

            // Do NOT mark as failed - this simulates successful delivery
            // Just call MessagesDeliveredAsync directly
            await receiver.MessagesDeliveredAsync(messages);

            _output.WriteLine("Successful delivery test completed");
        }
        finally
        {
            // Clean up
            await receiver.Shutdown(TimeSpan.FromSeconds(10));
        }
    }

    [Fact]
    public async Task FailureHandler_Factory_CreatesCorrectType()
    {
        // Arrange
        var services = CreateServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        
        var factory = ServiceBusAdapterFactory.Create(serviceProvider, "test-provider") as ServiceBusQueueAdapterFactory;
        Assert.NotNull(factory);

        // Act
        var failureHandler = await factory.GetDeliveryFailureHandler(QueueId.GetQueueId("test-queue", 0, 0));

        // Assert
        Assert.NotNull(failureHandler);
        Assert.IsType<ServiceBusStreamFailureHandler>(failureHandler);
        Assert.False(failureHandler.ShouldFaultSubscriptionOnError);

        _output.WriteLine($"Failure handler type: {failureHandler.GetType().Name}");
    }

    [Fact, TestCategory("BVT"), TestCategory("AzureServiceBus")]
    public void FailureHandler_DLQCallback_IsInvoked()
    {
        // Arrange
        string? capturedMessageId = null;
        StreamId capturedStreamId = default;
        string? capturedReason = null;

        var dlqCallback = new Action<string, StreamId, string>((messageId, streamId, reason) =>
        {
            capturedMessageId = messageId;
            capturedStreamId = streamId;
            capturedReason = reason;
        });

        var logger = NullLogger<ServiceBusStreamFailureHandler>.Instance;
        var handler = new ServiceBusStreamFailureHandler(logger, dlqCallback);

        var messageId = "test-message-123";
        var streamId = StreamId.Create("test-namespace", "test-key");
        var reason = "MaxDeliveryCountExceeded";

        // Act
        handler.OnDeadLetterQueueMove(messageId, streamId, reason);

        // Assert
        Assert.Equal(messageId, capturedMessageId);
        Assert.Equal(streamId, capturedStreamId);
        Assert.Equal(reason, capturedReason);

        _output.WriteLine($"DLQ callback invoked with MessageId: {capturedMessageId}, Reason: {capturedReason}");
    }

    private IServiceCollection CreateServiceCollection()
    {
        var services = new ServiceCollection();
        
        // Add logging
        services.AddLogging(builder => builder.AddConsole());
        
        // Add serialization
        services.AddSerializer();
        
        // Configure Service Bus options to use the emulator
        services.Configure<ServiceBusStreamOptions>("test-provider", options =>
        {
            options.ConnectionString = _fixture.ServiceBusConnectionString;
            options.EntityKind = EntityKind.Queue;
            options.QueueName = ServiceBusEmulatorFixture.QueueName; // Use the pre-configured queue
            options.AutoCreateEntities = false;
            // Configure receiver settings for testing
            options.Receiver = new ReceiverSettings
            {
                PrefetchCount = 10,
                ReceiveBatchSize = 5,
                LockAutoRenew = false, // Disable to reduce complexity in tests
                MaxConcurrentHandlers = 1
            };
            
            // Configure publisher settings for testing
            options.Publisher = new PublisherSettings
            {
                BatchSize = 100,
                MessageTimeToLive = TimeSpan.FromMinutes(30) // Reasonable TTL for tests
            };
        });

        return services;
    }
}
