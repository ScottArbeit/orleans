using Orleans.Streaming.AzureServiceBus.Tests.Infrastructure;
using Orleans.Streams;
using Orleans.TestingHost;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Streaming.AzureServiceBus.Tests.Streaming;

/// <summary>
/// Tests for Azure Service Bus subscription management and multiplicity scenarios.
/// Validates subscription lifecycle, multiple subscribers, and subscription isolation.
/// </summary>
[TestCategory("ServiceBus"), TestCategory("Streaming"), TestCategory("Functional")]
[Collection("ServiceBusEmulator")]
public class ServiceBusSubscriptionTests : TestClusterPerTest
{
    private const string StreamProviderName = "ServiceBusSubscriptionProvider";
    private const string StreamNamespace = "SBSubscriptionTestNamespace";

    private readonly ITestOutputHelper _output;
    private readonly ServiceBusEmulatorFixture _emulatorFixture;

    public ServiceBusSubscriptionTests(ITestOutputHelper output, ServiceBusEmulatorFixture emulatorFixture)
    {
        _output = output;
        _emulatorFixture = emulatorFixture;
    }

    protected override void ConfigureTestCluster(TestClusterBuilder builder)
    {
        builder.AddSiloBuilderConfigurator<MySiloBuilderConfigurator>();
        builder.AddClientBuilderConfigurator<MyClientBuilderConfigurator>();
    }

    /// <summary>
    /// Tests basic subscription creation and management.
    /// </summary>
    [Fact]
    public async Task ServiceBusSubscription_BasicSubscriptionManagement()
    {
        _output.WriteLine("Testing basic subscription creation and management");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var handle = await stream.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        Assert.NotNull(handle);

        // Test that subscription is active
        await Task.Delay(1000);
        var testEvent = ServiceBusTestUtils.GenerateTestEvents(1).First();
        await stream.OnNextAsync(testEvent);
        await Task.Delay(2000);

        Assert.Single(receivedEvents);

        // Test unsubscription
        await handle.UnsubscribeAsync();
        
        // Send another event after unsubscribe
        var secondEvent = ServiceBusTestUtils.GenerateTestEvents(1).First();
        await stream.OnNextAsync(secondEvent);
        await Task.Delay(2000);

        // Should still have only one event
        Assert.Single(receivedEvents);
    }

    /// <summary>
    /// Tests multiple subscriptions to the same stream.
    /// </summary>
    [Fact]
    public async Task ServiceBusSubscription_MultipleSubscriptionsToSameStream()
    {
        _output.WriteLine("Testing multiple subscriptions to the same stream");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedEvents1 = new List<TestEvent>();
        var receivedEvents2 = new List<TestEvent>();
        var receivedEvents3 = new List<TestEvent>();

        // Create multiple subscriptions to the same stream
        var handle1 = await stream.SubscribeAsync((evt, token) =>
        {
            receivedEvents1.Add(evt);
            return Task.CompletedTask;
        });

        var handle2 = await stream.SubscribeAsync((evt, token) =>
        {
            receivedEvents2.Add(evt);
            return Task.CompletedTask;
        });

        var handle3 = await stream.SubscribeAsync((evt, token) =>
        {
            receivedEvents3.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscriptions to initialize
        await Task.Delay(2000);

        // Send test events
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(3).ToList();
        foreach (var evt in testEvents)
        {
            await stream.OnNextAsync(evt);
        }

        await Task.Delay(5000);

        // All subscriptions should receive all events
        Assert.Equal(testEvents.Count, receivedEvents1.Count);
        Assert.Equal(testEvents.Count, receivedEvents2.Count);
        Assert.Equal(testEvents.Count, receivedEvents3.Count);

        await handle1.UnsubscribeAsync();
        await handle2.UnsubscribeAsync();
        await handle3.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests subscription isolation - events sent to one stream don't affect other streams.
    /// </summary>
    [Fact]
    public async Task ServiceBusSubscription_StreamIsolation()
    {
        _output.WriteLine("Testing subscription isolation between different streams");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        
        var streamId1 = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace, "stream1");
        var streamId2 = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace, "stream2");
        
        var stream1 = provider.GetStream<TestEvent>(streamId1);
        var stream2 = provider.GetStream<TestEvent>(streamId2);

        var receivedEvents1 = new List<TestEvent>();
        var receivedEvents2 = new List<TestEvent>();

        var handle1 = await stream1.SubscribeAsync((evt, token) =>
        {
            receivedEvents1.Add(evt);
            return Task.CompletedTask;
        });

        var handle2 = await stream2.SubscribeAsync((evt, token) =>
        {
            receivedEvents2.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscriptions to initialize
        await Task.Delay(1000);

        // Send events to stream1 only
        var events1 = ServiceBusTestUtils.GenerateTestEvents(3, "stream1").ToList();
        foreach (var evt in events1)
        {
            await stream1.OnNextAsync(evt);
        }

        await Task.Delay(3000);

        // Only stream1 subscriber should receive events
        Assert.Equal(events1.Count, receivedEvents1.Count);
        Assert.Empty(receivedEvents2);

        // Now send events to stream2
        var events2 = ServiceBusTestUtils.GenerateTestEvents(2, "stream2").ToList();
        foreach (var evt in events2)
        {
            await stream2.OnNextAsync(evt);
        }

        await Task.Delay(3000);

        // Stream1 should still have the same events, stream2 should have new events
        Assert.Equal(events1.Count, receivedEvents1.Count);
        Assert.Equal(events2.Count, receivedEvents2.Count);

        await handle1.UnsubscribeAsync();
        await handle2.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests subscription resume after temporary disconnection.
    /// </summary>
    [Fact]
    public async Task ServiceBusSubscription_ResumeAfterDisconnection()
    {
        _output.WriteLine("Testing subscription resume after temporary disconnection");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var handle = await stream.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Send first batch of events
        var firstBatch = ServiceBusTestUtils.GenerateTestEvents(2).ToList();
        foreach (var evt in firstBatch)
        {
            await stream.OnNextAsync(evt);
        }

        await Task.Delay(2000);
        Assert.Equal(firstBatch.Count, receivedEvents.Count);

        // Simulate disconnection by unsubscribing
        await handle.UnsubscribeAsync();
        await Task.Delay(500);

        // Send events while disconnected
        var disconnectedBatch = ServiceBusTestUtils.GenerateTestEvents(2).ToList();
        foreach (var evt in disconnectedBatch)
        {
            await stream.OnNextAsync(evt);
        }

        await Task.Delay(1000);

        // Resubscribe (resume)
        handle = await stream.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        await Task.Delay(2000);

        // Send new events after resume
        var resumeBatch = ServiceBusTestUtils.GenerateTestEvents(2).ToList();
        foreach (var evt in resumeBatch)
        {
            await stream.OnNextAsync(evt);
        }

        await Task.Delay(3000);

        // Should have received at least the first batch and resume batch
        Assert.True(receivedEvents.Count >= firstBatch.Count + resumeBatch.Count,
            $"Expected at least {firstBatch.Count + resumeBatch.Count} events, received {receivedEvents.Count}");

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests subscription error handling and recovery.
    /// </summary>
    [Fact]
    public async Task ServiceBusSubscription_ErrorHandlingAndRecovery()
    {
        _output.WriteLine("Testing subscription error handling and recovery");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var errorCount = 0;

        var handle = await stream.SubscribeAsync(
            onNext: (evt, token) =>
            {
                // Simulate processing error for first few events
                if (errorCount < 2)
                {
                    errorCount++;
                    throw new InvalidOperationException($"Simulated processing error {errorCount}");
                }

                receivedEvents.Add(evt);
                return Task.CompletedTask;
            });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Send test events (some will cause errors, some will succeed)
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(5).ToList();
        foreach (var evt in testEvents)
        {
            await stream.OnNextAsync(evt);
            await Task.Delay(200); // Small delay between events
        }

        await Task.Delay(5000);

        // Should have processed events after the error recovery
        Assert.True(receivedEvents.Count > 0, "Should have received some events after error recovery");
        Assert.Equal(2, errorCount); // Should have encountered exactly 2 errors

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests subscription with different message types.
    /// </summary>
    [Fact]
    public async Task ServiceBusSubscription_DifferentMessageTypes()
    {
        _output.WriteLine("Testing subscription with different message types");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);

        // Test with TestEvent type
        var testEventStream = provider.GetStream<TestEvent>(streamId);
        var receivedTestEvents = new List<TestEvent>();

        var testEventHandle = await testEventStream.SubscribeAsync((evt, token) =>
        {
            receivedTestEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Test with string type
        var stringStream = provider.GetStream<string>(streamId);
        var receivedStrings = new List<string>();

        var stringHandle = await stringStream.SubscribeAsync((str, token) =>
        {
            receivedStrings.Add(str);
            return Task.CompletedTask;
        });

        // Allow subscriptions to initialize
        await Task.Delay(1000);

        // Send different types of events
        var testEvent = ServiceBusTestUtils.GenerateTestEvents(1).First();
        await testEventStream.OnNextAsync(testEvent);

        await stringStream.OnNextAsync("Test string message");

        await Task.Delay(3000);

        // Each subscription should only receive events of its type
        Assert.Single(receivedTestEvents);
        Assert.Single(receivedStrings);
        Assert.Equal("Test string message", receivedStrings[0]);
        Assert.Equal(testEvent.Id, receivedTestEvents[0].Id);

        await testEventHandle.UnsubscribeAsync();
        await stringHandle.UnsubscribeAsync();
    }

    private class MySiloBuilderConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder hostBuilder)
        {
            hostBuilder.AddServiceBusTopicStreams(StreamProviderName, configure =>
            {
                configure.ConfigureTopicTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "subscription-test-topic",
                    "subscription-test-sub");
                configure.BatchSize = 5;
            });
        }
    }

    private class MyClientBuilderConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IClientBuilder clientBuilder)
        {
            clientBuilder.AddServiceBusTopicStreams(StreamProviderName, configure =>
            {
                configure.ConfigureTopicTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "subscription-test-topic",
                    "subscription-test-sub");
            });
        }
    }
}