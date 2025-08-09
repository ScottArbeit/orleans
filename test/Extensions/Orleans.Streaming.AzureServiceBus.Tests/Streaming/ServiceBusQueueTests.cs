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
/// Tests specific to Azure Service Bus Queue scenarios.
/// Validates queue-specific messaging patterns, FIFO behavior, and queue management.
/// </summary>
[TestCategory("ServiceBus"), TestCategory("Streaming"), TestCategory("Functional")]
[Collection("ServiceBusEmulator")]
public class ServiceBusQueueTests : TestClusterPerTest
{
    private const string StreamProviderName = "ServiceBusQueueProvider";
    private const string StreamNamespace = "SBQueueTestNamespace";

    private readonly ITestOutputHelper _output;
    private readonly ServiceBusEmulatorFixture _emulatorFixture;

    public ServiceBusQueueTests(ITestOutputHelper output, ServiceBusEmulatorFixture emulatorFixture)
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
    /// Tests basic queue message sending and receiving.
    /// </summary>
    [Fact]
    public async Task ServiceBusQueue_BasicSendReceive()
    {
        _output.WriteLine("Testing basic Service Bus queue send/receive");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var handle = await stream.SubscribeAsync((evt, token) =>
        {
            _output.WriteLine($"Received event: {evt.Id}");
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Send test events
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(5).ToList();
        foreach (var evt in testEvents)
        {
            await stream.OnNextAsync(evt);
            _output.WriteLine($"Sent event: {evt.Id}");
        }

        // Wait for all events to be processed
        await Task.Delay(5000);

        Assert.Equal(testEvents.Count, receivedEvents.Count);

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests queue message ordering (FIFO behavior).
    /// Note: Service Bus queues provide FIFO ordering within a session.
    /// </summary>
    [Fact]
    public async Task ServiceBusQueue_MessageOrdering()
    {
        _output.WriteLine("Testing Service Bus queue message ordering");

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

        // Send events in order (Service Bus queues preserve FIFO within a session)
        var testEvents = new List<TestEvent>();
        for (int i = 0; i < 10; i++)
        {
            var evt = new TestEvent
            {
                Id = Guid.NewGuid(),
                StreamKey = streamId.Key,
                Timestamp = DateTimeOffset.UtcNow,
                Data = $"Ordered message {i}",
                Properties = new Dictionary<string, object> { { "Index", i } }
            };
            testEvents.Add(evt);
            await stream.OnNextAsync(evt);
            
            // Small delay to ensure ordering
            await Task.Delay(100);
        }

        // Wait for all events to be processed
        await Task.Delay(5000);

        Assert.Equal(testEvents.Count, receivedEvents.Count);

        // Check if events are received in the same order they were sent (by checking Index property)
        for (int i = 0; i < testEvents.Count; i++)
        {
            var expectedIndex = (int)testEvents[i].Properties["Index"];
            var receivedIndex = (int)receivedEvents[i].Properties["Index"];
            Assert.Equal(expectedIndex, receivedIndex);
        }

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests queue behavior with single consumer (no duplication).
    /// </summary>
    [Fact]
    public async Task ServiceBusQueue_SingleConsumerNoDuplication()
    {
        _output.WriteLine("Testing Service Bus queue single consumer behavior");

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

        // Send the same event multiple times
        var testEvent = ServiceBusTestUtils.GenerateTestEvents(1).First();
        for (int i = 0; i < 3; i++)
        {
            await stream.OnNextAsync(testEvent);
            await Task.Delay(100);
        }

        // Wait for processing
        await Task.Delay(3000);

        // Should receive the event multiple times (queue doesn't deduplicate)
        Assert.Equal(3, receivedEvents.Count);
        Assert.All(receivedEvents, evt => Assert.Equal(testEvent.Id, evt.Id));

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests queue behavior with batch processing.
    /// </summary>
    [Fact]
    public async Task ServiceBusQueue_BatchProcessing()
    {
        _output.WriteLine("Testing Service Bus queue batch processing");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedBatches = new List<int>();
        var receivedEvents = new List<TestEvent>();

        var handle = await stream.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            
            // Track batch sizes (simplified - just count events received in quick succession)
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Send a large batch of events quickly
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(20).ToList();
        var tasks = testEvents.Select(evt => stream.OnNextAsync(evt));
        await Task.WhenAll(tasks);

        // Wait for all events to be processed
        await Task.Delay(8000);

        Assert.Equal(testEvents.Count, receivedEvents.Count);

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests queue behavior with multiple producers to single queue.
    /// </summary>
    [Fact]
    public async Task ServiceBusQueue_MultipleProducersSingleQueue()
    {
        _output.WriteLine("Testing multiple producers sending to single queue");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        
        var receivedEvents = new List<TestEvent>();
        var consumer = provider.GetStream<TestEvent>(streamId);
        var handle = await consumer.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Create multiple producer streams (same stream ID, different instances)
        var producer1 = provider.GetStream<TestEvent>(streamId);
        var producer2 = provider.GetStream<TestEvent>(streamId);
        var producer3 = provider.GetStream<TestEvent>(streamId);

        // Send events from multiple producers concurrently
        var events1 = ServiceBusTestUtils.GenerateTestEvents(3, "producer1").ToList();
        var events2 = ServiceBusTestUtils.GenerateTestEvents(3, "producer2").ToList();
        var events3 = ServiceBusTestUtils.GenerateTestEvents(3, "producer3").ToList();

        var tasks = new List<Task>();
        tasks.AddRange(events1.Select(evt => producer1.OnNextAsync(evt)));
        tasks.AddRange(events2.Select(evt => producer2.OnNextAsync(evt)));
        tasks.AddRange(events3.Select(evt => producer3.OnNextAsync(evt)));

        await Task.WhenAll(tasks);

        // Wait for all events to be processed
        await Task.Delay(5000);

        var totalExpectedEvents = events1.Count + events2.Count + events3.Count;
        Assert.Equal(totalExpectedEvents, receivedEvents.Count);

        // Verify we received events from all producers
        Assert.Contains(receivedEvents, evt => evt.StreamKey == "producer1");
        Assert.Contains(receivedEvents, evt => evt.StreamKey == "producer2");
        Assert.Contains(receivedEvents, evt => evt.StreamKey == "producer3");

        await handle.UnsubscribeAsync();
    }

    private class MySiloBuilderConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder hostBuilder)
        {
            hostBuilder.AddServiceBusQueueStreams(StreamProviderName, configure =>
            {
                configure.ConfigureQueueTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "queue-specific-test");
                configure.BatchSize = 10; // Enable batching for batch tests
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
                    "queue-specific-test");
            });
        }
    }
}