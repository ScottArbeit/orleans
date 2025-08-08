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
/// Tests for Azure Service Bus stream provider functionality.
/// Validates provider initialization, configuration, and stream lifecycle management.
/// </summary>
[TestCategory("ServiceBus"), TestCategory("Streaming"), TestCategory("Functional")]
[Collection("ServiceBusEmulator")]
public class ServiceBusStreamProviderTests : TestClusterPerTest
{
    private const string QueueStreamProviderName = "ServiceBusQueueProvider";
    private const string TopicStreamProviderName = "ServiceBusTopicProvider";
    private const string StreamNamespace = "SBProviderTestNamespace";

    private readonly ITestOutputHelper _output;
    private readonly ServiceBusEmulatorFixture _emulatorFixture;

    public ServiceBusStreamProviderTests(ITestOutputHelper output, ServiceBusEmulatorFixture emulatorFixture)
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
    /// Tests that the Service Bus queue stream provider is properly initialized and accessible.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamProvider_QueueProviderIsInitialized()
    {
        _output.WriteLine("Testing Service Bus queue stream provider initialization");

        var provider = this.Client.GetStreamProvider(QueueStreamProviderName);
        Assert.NotNull(provider);

        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);
        Assert.NotNull(stream);
        Assert.Equal(streamId, stream.StreamId);
    }

    /// <summary>
    /// Tests that the Service Bus topic stream provider is properly initialized and accessible.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamProvider_TopicProviderIsInitialized()
    {
        _output.WriteLine("Testing Service Bus topic stream provider initialization");

        var provider = this.Client.GetStreamProvider(TopicStreamProviderName);
        Assert.NotNull(provider);

        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);
        Assert.NotNull(stream);
        Assert.Equal(streamId, stream.StreamId);
    }

    /// <summary>
    /// Tests basic stream creation and event publishing through the queue provider.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamProvider_QueueCanPublishEvents()
    {
        _output.WriteLine("Testing event publishing through queue stream provider");

        var provider = this.Client.GetStreamProvider(QueueStreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var testEvent = ServiceBusTestUtils.GenerateTestEvents(1).First();
        
        // Should not throw
        await stream.OnNextAsync(testEvent);
        _output.WriteLine($"Successfully published event {testEvent.Id}");
    }

    /// <summary>
    /// Tests basic stream creation and event publishing through the topic provider.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamProvider_TopicCanPublishEvents()
    {
        _output.WriteLine("Testing event publishing through topic stream provider");

        var provider = this.Client.GetStreamProvider(TopicStreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var testEvent = ServiceBusTestUtils.GenerateTestEvents(1).First();
        
        // Should not throw
        await stream.OnNextAsync(testEvent);
        _output.WriteLine($"Successfully published event {testEvent.Id}");
    }

    /// <summary>
    /// Tests stream subscription creation and basic event flow.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamProvider_CanCreateSubscription()
    {
        _output.WriteLine("Testing stream subscription creation");

        var provider = this.Client.GetStreamProvider(QueueStreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var handle = await stream.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        Assert.NotNull(handle);
        
        // Allow subscription to initialize
        await Task.Delay(1000);

        // Publish an event
        var testEvent = ServiceBusTestUtils.GenerateTestEvents(1).First();
        await stream.OnNextAsync(testEvent);

        // Wait for processing
        await Task.Delay(3000);

        Assert.Single(receivedEvents);
        Assert.Equal(testEvent.Id, receivedEvents[0].Id);

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests that multiple streams can be created with different stream IDs.
    /// </summary>
    [Fact]
    public async Task ServiceBusStreamProvider_SupportsMultipleStreams()
    {
        _output.WriteLine("Testing multiple stream support");

        var provider = this.Client.GetStreamProvider(QueueStreamProviderName);
        
        var streamId1 = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace, "stream1");
        var streamId2 = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace, "stream2");
        
        var stream1 = provider.GetStream<TestEvent>(streamId1);
        var stream2 = provider.GetStream<TestEvent>(streamId2);

        Assert.NotEqual(stream1.StreamId, stream2.StreamId);

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

        // Send events to different streams
        var event1 = ServiceBusTestUtils.GenerateTestEvents(1, "stream1").First();
        var event2 = ServiceBusTestUtils.GenerateTestEvents(1, "stream2").First();

        await stream1.OnNextAsync(event1);
        await stream2.OnNextAsync(event2);

        // Wait for processing
        await Task.Delay(3000);

        // Each stream should only receive its own events
        Assert.Single(receivedEvents1);
        Assert.Single(receivedEvents2);
        
        Assert.Equal(event1.Id, receivedEvents1[0].Id);
        Assert.Equal(event2.Id, receivedEvents2[0].Id);

        await handle1.UnsubscribeAsync();
        await handle2.UnsubscribeAsync();
    }

    private class MySiloBuilderConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder hostBuilder)
        {
            hostBuilder.AddServiceBusQueueStreams(QueueStreamProviderName, configure =>
            {
                configure.ConfigureQueueTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "provider-queue-test");
            });

            hostBuilder.AddServiceBusTopicStreams(TopicStreamProviderName, configure =>
            {
                configure.ConfigureTopicTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "provider-topic-test",
                    "provider-subscription-test");
            });
        }
    }

    private class MyClientBuilderConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.AddServiceBusQueueStreams(QueueStreamProviderName, configure =>
            {
                configure.ConfigureQueueTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "provider-queue-test");
            });

            clientBuilder.AddServiceBusTopicStreams(TopicStreamProviderName, configure =>
            {
                configure.ConfigureTopicTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "provider-topic-test",
                    "provider-subscription-test");
            });
        }
    }
}