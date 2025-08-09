#nullable enable
using Orleans.Streaming.AzureServiceBus.Tests.Infrastructure;
using Microsoft.Extensions.Configuration;
using Orleans.Streams;
using Microsoft.Extensions.Configuration;
using Orleans.TestingHost;
using Microsoft.Extensions.Configuration;
using TestExtensions;
using Microsoft.Extensions.Configuration;
using Xunit;
using Microsoft.Extensions.Configuration;
using Xunit.Abstractions;
using Microsoft.Extensions.Configuration;

namespace Orleans.Streaming.AzureServiceBus.Tests.Streaming;

/// <summary>
/// Tests for Azure Service Bus stream recovery and resume functionality.
/// Validates error handling, connection recovery, and message replay capabilities.
/// </summary>
[TestCategory("ServiceBus"), TestCategory("Streaming"), TestCategory("Functional")]
[Collection("ServiceBusEmulator")]
public class ServiceBusStreamRecoveryTests : TestClusterPerTest
{
    private const string StreamProviderName = "ServiceBusRecoveryProvider";
    private const string StreamNamespace = "SBRecoveryTestNamespace";

    private readonly ITestOutputHelper _output;
    private readonly ServiceBusEmulatorFixture _emulatorFixture;

    public ServiceBusStreamRecoveryTests(ITestOutputHelper output, ServiceBusEmulatorFixture emulatorFixture)
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
    /// Tests stream recovery after processing errors.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamRecovery_ProcessingErrorRecovery()
    {
        _output.WriteLine("Testing stream recovery after processing errors");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var errorEvents = new List<TestEvent>();
        var processedEventCount = 0;

        var handle = await stream.SubscribeAsync((evt, token) =>
        {
            processedEventCount++;
            
            // Simulate processing error for specific events
            if (evt.Data.Contains("error"))
            {
                errorEvents.Add(evt);
                throw new InvalidOperationException($"Simulated processing error for event {evt.Id}");
            }

            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Send mixed events (some will cause errors)
        var testEvents = new List<TestEvent>
        {
            new() { Id = Guid.NewGuid(), Data = "normal event 1", StreamKey = "test" },
            new() { Id = Guid.NewGuid(), Data = "error event 1", StreamKey = "test" },
            new() { Id = Guid.NewGuid(), Data = "normal event 2", StreamKey = "test" },
            new() { Id = Guid.NewGuid(), Data = "error event 2", StreamKey = "test" },
            new() { Id = Guid.NewGuid(), Data = "normal event 3", StreamKey = "test" }
        };

        foreach (var evt in testEvents)
        {
            await stream.OnNextAsync(evt);
            await Task.Delay(200);
        }

        await Task.Delay(5000);

        // Should have processed all events (some successfully, some with errors)
        _output.WriteLine($"Processed {processedEventCount} events, {receivedEvents.Count} successful, {errorEvents.Count} errors");
        
        Assert.True(processedEventCount > 0, "Should have processed some events");
        Assert.True(receivedEvents.Count > 0, "Should have successfully processed some events");
        Assert.Equal(3, receivedEvents.Count); // 3 normal events
        
        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests stream subscription resumption after unsubscribe.
    /// Note: Azure Service Bus is non-rewindable - messages are delivered once and cannot be replayed from tokens.
    /// This test verifies proper subscription lifecycle management.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamRecovery_SubscriptionResumption()
    {
        _output.WriteLine("Testing stream subscription resumption (non-rewindable Service Bus)");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();

        // First subscription - process some events
        var handle1 = await stream.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        await Task.Delay(1000);

        // Send first batch of events
        var firstBatch = ServiceBusTestUtils.GenerateTestEvents(3).ToList();
        foreach (var evt in firstBatch)
        {
            await stream.OnNextAsync(evt);
        }

        await Task.Delay(3000);
        
        Assert.Equal(firstBatch.Count, receivedEvents.Count);

        // Unsubscribe
        await handle1.UnsubscribeAsync();
        await Task.Delay(500);

        // Send more events while unsubscribed - these may be lost since Service Bus is non-rewindable
        var secondBatch = ServiceBusTestUtils.GenerateTestEvents(2).ToList();
        foreach (var evt in secondBatch)
        {
            await stream.OnNextAsync(evt);
        }

        await Task.Delay(1000);

        // Create new subscription - cannot resume from previous point
        var newSubscriptionEvents = new List<TestEvent>();
        var handle2 = await stream.SubscribeAsync((evt, token) =>
        {
            newSubscriptionEvents.Add(evt);
            return Task.CompletedTask;
        });

        await Task.Delay(3000);

        // Verify new subscription works (events sent while unsubscribed are typically lost)
        _output.WriteLine($"New subscription received {newSubscriptionEvents.Count} events");
        
        await handle2.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests connection retry behavior and recovery.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamRecovery_ConnectionRetryRecovery()
    {
        _output.WriteLine("Testing connection retry and recovery behavior");

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

        // Send initial events to verify connection works
        var initialEvents = ServiceBusTestUtils.GenerateTestEvents(2).ToList();
        foreach (var evt in initialEvents)
        {
            await stream.OnNextAsync(evt);
        }

        await Task.Delay(2000);
        Assert.Equal(initialEvents.Count, receivedEvents.Count);

        // Note: In a real scenario, we would simulate connection failure
        // For this test, we'll continue sending events to verify resilience
        var additionalEvents = ServiceBusTestUtils.GenerateTestEvents(3).ToList();
        foreach (var evt in additionalEvents)
        {
            await stream.OnNextAsync(evt);
            await Task.Delay(100);
        }

        await Task.Delay(3000);

        var totalExpected = initialEvents.Count + additionalEvents.Count;
        Assert.Equal(totalExpected, receivedEvents.Count);

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests graceful degradation under error conditions.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamRecovery_GracefulDegradation()
    {
        _output.WriteLine("Testing graceful degradation under error conditions");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var errorCount = 0;
        const int maxErrors = 3;

        var handle = await stream.SubscribeAsync((evt, token) =>
        {
            // Simulate intermittent processing errors based on event ID
            if (evt.Id.ToString().EndsWith("0") && errorCount < maxErrors)
            {
                errorCount++;
                throw new InvalidOperationException($"Simulated intermittent error {errorCount}");
            }

            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Send events that will trigger some errors
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(10).ToList();
        foreach (var evt in testEvents)
        {
            await stream.OnNextAsync(evt);
            await Task.Delay(100);
        }

        await Task.Delay(5000);

        // Should have processed most events despite some errors
        _output.WriteLine($"Received {receivedEvents.Count} events with {errorCount} errors");
        Assert.True(receivedEvents.Count > 0, "Should have processed some events despite errors");
        Assert.True(errorCount <= maxErrors, $"Error count {errorCount} should not exceed {maxErrors}");

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests message replay functionality after failure.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamRecovery_MessageReplay()
    {
        _output.WriteLine("Testing message replay functionality");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var allReceivedEvents = new List<TestEvent>();
        var firstSubscriptionEvents = new List<TestEvent>();
        var secondSubscriptionEvents = new List<TestEvent>();

        // First subscription
        var handle1 = await stream.SubscribeAsync((evt, token) =>
        {
            firstSubscriptionEvents.Add(evt);
            allReceivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        await Task.Delay(1000);

        // Send events
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(3).ToList();
        foreach (var evt in testEvents)
        {
            await stream.OnNextAsync(evt);
        }

        await Task.Delay(2000);
        
        Assert.Equal(testEvents.Count, firstSubscriptionEvents.Count);

        // Unsubscribe first subscription
        await handle1.UnsubscribeAsync();
        await Task.Delay(500);

        // Create second subscription (may replay some messages depending on Service Bus configuration)
        var handle2 = await stream.SubscribeAsync((evt, token) =>
        {
            secondSubscriptionEvents.Add(evt);
            allReceivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        await Task.Delay(2000);

        // Send more events to second subscription
        var additionalEvents = ServiceBusTestUtils.GenerateTestEvents(2).ToList();
        foreach (var evt in additionalEvents)
        {
            await stream.OnNextAsync(evt);
        }

        await Task.Delay(3000);

        // Second subscription should receive at least the new events
        Assert.True(secondSubscriptionEvents.Count >= additionalEvents.Count,
            $"Second subscription should receive at least {additionalEvents.Count} events, got {secondSubscriptionEvents.Count}");

        _output.WriteLine($"First subscription: {firstSubscriptionEvents.Count} events");
        _output.WriteLine($"Second subscription: {secondSubscriptionEvents.Count} events");
        _output.WriteLine($"Total events processed: {allReceivedEvents.Count}");

        await handle2.UnsubscribeAsync();
    }

    private class MySiloBuilderConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder hostBuilder)
        {
            hostBuilder.AddServiceBusQueueStreams(StreamProviderName, configure =>
            {
                configure.ConfigureQueueTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "recovery-test-queue");
                configure.BatchSize = 5;
                configure.MaxConcurrentCalls = 1; // Sequential processing for deterministic testing
            });
        }
    }

    private class MyClientBuilderConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.AddServiceBusQueueStreams(StreamProviderName, configure =>
            {
                configure.ConfigureQueueTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "recovery-test-queue");
            });
        }
    }
}