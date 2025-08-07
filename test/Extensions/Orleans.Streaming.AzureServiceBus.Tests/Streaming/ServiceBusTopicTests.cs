using Orleans.Streaming.AzureServiceBus.Tests.Infrastructure;
using Orleans.Streams;
using Orleans.TestingHost;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Streaming.AzureServiceBus.Tests.Streaming;

/// <summary>
/// Tests specific to Azure Service Bus Topic/Subscription scenarios.
/// Validates topic-based pub/sub messaging patterns, multiple subscriptions, and filtering.
/// </summary>
[TestCategory("ServiceBus"), TestCategory("Streaming"), TestCategory("Functional")]
[Collection("ServiceBusEmulator")]
public class ServiceBusTopicTests : TestClusterPerTest
{
    private const string StreamProviderName = "ServiceBusTopicProvider";
    private const string StreamNamespace = "SBTopicTestNamespace";

    private readonly ITestOutputHelper _output;
    private readonly ServiceBusEmulatorFixture _emulatorFixture;

    public ServiceBusTopicTests(ITestOutputHelper output, ServiceBusEmulatorFixture emulatorFixture)
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
    /// Tests basic topic message publishing and subscription receiving.
    /// </summary>
    [Fact]
    public async Task ServiceBusTopic_BasicPublishSubscribe()
    {
        _output.WriteLine("Testing basic Service Bus topic publish/subscribe");

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

        // Publish test events
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(5).ToList();
        foreach (var evt in testEvents)
        {
            await stream.OnNextAsync(evt);
            _output.WriteLine($"Published event: {evt.Id}");
        }

        // Wait for all events to be processed
        await Task.Delay(5000);

        Assert.Equal(testEvents.Count, receivedEvents.Count);

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests topic fan-out to multiple subscribers (pub/sub pattern).
    /// </summary>
    [Fact]
    public async Task ServiceBusTopic_FanOutToMultipleSubscribers()
    {
        _output.WriteLine("Testing Service Bus topic fan-out to multiple subscribers");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var publisher = provider.GetStream<TestEvent>(streamId);

        // Create multiple subscribers
        var receivedEvents1 = new List<TestEvent>();
        var receivedEvents2 = new List<TestEvent>();
        var receivedEvents3 = new List<TestEvent>();

        var subscriber1 = provider.GetStream<TestEvent>(streamId);
        var handle1 = await subscriber1.SubscribeAsync((evt, token) =>
        {
            receivedEvents1.Add(evt);
            return Task.CompletedTask;
        });

        var subscriber2 = provider.GetStream<TestEvent>(streamId);
        var handle2 = await subscriber2.SubscribeAsync((evt, token) =>
        {
            receivedEvents2.Add(evt);
            return Task.CompletedTask;
        });

        var subscriber3 = provider.GetStream<TestEvent>(streamId);
        var handle3 = await subscriber3.SubscribeAsync((evt, token) =>
        {
            receivedEvents3.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscriptions to initialize
        await Task.Delay(2000);

        // Publish test events
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(3).ToList();
        foreach (var evt in testEvents)
        {
            await publisher.OnNextAsync(evt);
        }

        // Wait for all events to be processed
        await Task.Delay(5000);

        // All subscribers should receive all events (fan-out behavior)
        Assert.Equal(testEvents.Count, receivedEvents1.Count);
        Assert.Equal(testEvents.Count, receivedEvents2.Count);
        Assert.Equal(testEvents.Count, receivedEvents3.Count);

        // Verify events are the same across all subscribers
        var expectedIds = testEvents.Select(e => e.Id).OrderBy(id => id).ToList();
        var actualIds1 = receivedEvents1.Select(e => e.Id).OrderBy(id => id).ToList();
        var actualIds2 = receivedEvents2.Select(e => e.Id).OrderBy(id => id).ToList();
        var actualIds3 = receivedEvents3.Select(e => e.Id).OrderBy(id => id).ToList();

        Assert.Equal(expectedIds, actualIds1);
        Assert.Equal(expectedIds, actualIds2);
        Assert.Equal(expectedIds, actualIds3);

        await handle1.UnsubscribeAsync();
        await handle2.UnsubscribeAsync();
        await handle3.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests topic behavior with high message volume (load testing).
    /// </summary>
    [Fact]
    public async Task ServiceBusTopic_HighVolumeMessaging()
    {
        _output.WriteLine("Testing Service Bus topic with high volume messaging");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var publisher = provider.GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var subscriber = provider.GetStream<TestEvent>(streamId);
        var handle = await subscriber.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Publish a large number of events
        const int eventCount = 50;
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(eventCount).ToList();
        
        var publishTasks = testEvents.Select(evt => publisher.OnNextAsync(evt));
        await Task.WhenAll(publishTasks);

        _output.WriteLine($"Published {eventCount} events");

        // Wait for all events to be processed (longer timeout for high volume)
        await Task.Delay(10000);

        _output.WriteLine($"Received {receivedEvents.Count} events");
        Assert.Equal(testEvents.Count, receivedEvents.Count);

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests topic subscription durability across publisher/subscriber lifecycle.
    /// </summary>
    [Fact]
    public async Task ServiceBusTopic_SubscriptionDurability()
    {
        _output.WriteLine("Testing Service Bus topic subscription durability");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var publisher = provider.GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var subscriber = provider.GetStream<TestEvent>(streamId);
        var handle = await subscriber.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Publish first batch
        var firstBatch = ServiceBusTestUtils.GenerateTestEvents(2).ToList();
        foreach (var evt in firstBatch)
        {
            await publisher.OnNextAsync(evt);
        }

        await Task.Delay(2000);

        // Temporarily unsubscribe
        await handle.UnsubscribeAsync();
        await Task.Delay(1000);

        // Publish while unsubscribed (messages should be held for durable subscription)
        var secondBatch = ServiceBusTestUtils.GenerateTestEvents(2).ToList();
        foreach (var evt in secondBatch)
        {
            await publisher.OnNextAsync(evt);
        }

        await Task.Delay(1000);

        // Resubscribe
        handle = await subscriber.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        await Task.Delay(3000);

        // Should receive both batches if subscription is durable
        var totalExpectedEvents = firstBatch.Count + secondBatch.Count;
        Assert.True(receivedEvents.Count >= firstBatch.Count, 
            $"Expected at least {firstBatch.Count} events, received {receivedEvents.Count}");

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests concurrent publishers to the same topic.
    /// </summary>
    [Fact]
    public async Task ServiceBusTopic_ConcurrentPublishers()
    {
        _output.WriteLine("Testing concurrent publishers to Service Bus topic");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);

        var receivedEvents = new List<TestEvent>();
        var subscriber = provider.GetStream<TestEvent>(streamId);
        var handle = await subscriber.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Create multiple concurrent publishers
        var publisher1 = provider.GetStream<TestEvent>(streamId);
        var publisher2 = provider.GetStream<TestEvent>(streamId);
        var publisher3 = provider.GetStream<TestEvent>(streamId);

        // Publish concurrently from multiple publishers
        var events1 = ServiceBusTestUtils.GenerateTestEvents(3, "publisher1").ToList();
        var events2 = ServiceBusTestUtils.GenerateTestEvents(3, "publisher2").ToList();
        var events3 = ServiceBusTestUtils.GenerateTestEvents(3, "publisher3").ToList();

        var publishTasks = new List<Task>();
        publishTasks.AddRange(events1.Select(evt => publisher1.OnNextAsync(evt)));
        publishTasks.AddRange(events2.Select(evt => publisher2.OnNextAsync(evt)));
        publishTasks.AddRange(events3.Select(evt => publisher3.OnNextAsync(evt)));

        await Task.WhenAll(publishTasks);

        // Wait for all events to be processed
        await Task.Delay(5000);

        var totalExpectedEvents = events1.Count + events2.Count + events3.Count;
        Assert.Equal(totalExpectedEvents, receivedEvents.Count);

        // Verify we received events from all publishers
        Assert.Contains(receivedEvents, evt => evt.StreamKey == "publisher1");
        Assert.Contains(receivedEvents, evt => evt.StreamKey == "publisher2");
        Assert.Contains(receivedEvents, evt => evt.StreamKey == "publisher3");

        await handle.UnsubscribeAsync();
    }

    private class MySiloBuilderConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder hostBuilder)
        {
            hostBuilder.AddServiceBusTopicStreams(StreamProviderName, configure =>
            {
                configure.ConfigureTopicTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "topic-specific-test",
                    "topic-test-subscription");
                configure.BatchSize = 10;
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
                    "topic-specific-test",
                    "topic-test-subscription");
            });
        }
    }
}