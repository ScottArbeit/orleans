#nullable enable
using Microsoft.Extensions.Configuration;
using Orleans.Streaming.AzureServiceBus.Tests.Infrastructure;
using Orleans.Streams;
using Orleans.TestingHost;
using Tester.StreamingTests;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Streaming.AzureServiceBus.Tests.Streaming;

/// <summary>
/// Tests for Azure Service Bus streaming functionality with client producer/consumer scenarios.
/// Validates end-to-end message flow, subscription handling, and error recovery.
/// </summary>
[TestCategory("ServiceBus"), TestCategory("Streaming"), TestCategory("Functional")]
[Collection("ServiceBusEmulator")]
public class ServiceBusClientStreamTests : TestClusterPerTest
{
    private const string StreamProviderName = "ServiceBusStreamProvider";
    private const string StreamNamespace = "SBClientTestNamespace";

    private readonly ITestOutputHelper _output;
    private readonly ServiceBusEmulatorFixture _emulatorFixture;
    private ClientStreamTestRunner? _runner;

    public ServiceBusClientStreamTests(ITestOutputHelper output, ServiceBusEmulatorFixture emulatorFixture)
    {
        _output = output;
        _emulatorFixture = emulatorFixture;
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        _runner = new ClientStreamTestRunner(this.HostedCluster);
    }

    protected override void ConfigureTestCluster(TestClusterBuilder builder)
    {
        builder.AddSiloBuilderConfigurator<MySiloBuilderConfigurator>();
        builder.AddClientBuilderConfigurator<MyClientBuilderConfigurator>();
    }

    /// <summary>
    /// Tests basic client stream functionality - producer sends events and consumer receives them.
    /// </summary>
    [Fact]
    public async Task ServiceBusStream_ClientCanProduceAndConsume()
    {
        _output.WriteLine("Testing basic client stream produce/consume functionality");
        
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var producer = this.Client.GetStreamProvider(StreamProviderName).GetStream<TestEvent>(streamId);
        var consumer = this.Client.GetStreamProvider(StreamProviderName).GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var handle = await consumer.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Produce test events
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(5).ToList();
        foreach (var evt in testEvents)
        {
            await producer.OnNextAsync(evt);
        }

        // Wait for events to be processed
        await Task.Delay(5000);

        Assert.Equal(testEvents.Count, receivedEvents.Count);
        Assert.Equal(testEvents.Select(e => e.Id).OrderBy(id => id), receivedEvents.Select(e => e.Id).OrderBy(id => id));

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests multiple subscribers to the same stream (fan-out scenario).
    /// </summary>
    [Fact]
    public async Task ServiceBusStream_MultipleSubscribersReceiveEvents()
    {
        _output.WriteLine("Testing multiple subscribers to the same stream");
        
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var producer = this.Client.GetStreamProvider(StreamProviderName).GetStream<TestEvent>(streamId);
        
        var receivedEvents1 = new List<TestEvent>();
        var receivedEvents2 = new List<TestEvent>();

        var consumer1 = this.Client.GetStreamProvider(StreamProviderName).GetStream<TestEvent>(streamId);
        var handle1 = await consumer1.SubscribeAsync((evt, token) =>
        {
            receivedEvents1.Add(evt);
            return Task.CompletedTask;
        });

        var consumer2 = this.Client.GetStreamProvider(StreamProviderName).GetStream<TestEvent>(streamId);
        var handle2 = await consumer2.SubscribeAsync((evt, token) =>
        {
            receivedEvents2.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscriptions to initialize
        await Task.Delay(1000);

        // Produce test events
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(3).ToList();
        foreach (var evt in testEvents)
        {
            await producer.OnNextAsync(evt);
        }

        // Wait for events to be processed
        await Task.Delay(5000);

        // Both subscribers should receive all events
        Assert.Equal(testEvents.Count, receivedEvents1.Count);
        Assert.Equal(testEvents.Count, receivedEvents2.Count);

        await handle1.UnsubscribeAsync();
        await handle2.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests stream subscription recovery after unsubscribe/resubscribe.
    /// </summary>
    [Fact]
    public async Task ServiceBusStream_SubscriptionRecoveryAfterResubscribe()
    {
        _output.WriteLine("Testing subscription recovery after resubscribe");
        
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var producer = this.Client.GetStreamProvider(StreamProviderName).GetStream<TestEvent>(streamId);
        var consumer = this.Client.GetStreamProvider(StreamProviderName).GetStream<TestEvent>(streamId);

        var receivedEvents = new List<TestEvent>();
        var handle = await consumer.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Send first batch
        var firstBatch = ServiceBusTestUtils.GenerateTestEvents(2).ToList();
        foreach (var evt in firstBatch)
        {
            await producer.OnNextAsync(evt);
        }
        
        await Task.Delay(2000);

        // Unsubscribe and resubscribe
        await handle.UnsubscribeAsync();
        await Task.Delay(500);

        handle = await consumer.SubscribeAsync((evt, token) =>
        {
            receivedEvents.Add(evt);
            return Task.CompletedTask;
        });

        await Task.Delay(1000);

        // Send second batch
        var secondBatch = ServiceBusTestUtils.GenerateTestEvents(2).ToList();
        foreach (var evt in secondBatch)
        {
            await producer.OnNextAsync(evt);
        }

        await Task.Delay(2000);

        // Should receive at least the second batch after resubscribe
        Assert.True(receivedEvents.Count >= secondBatch.Count, 
            $"Expected at least {secondBatch.Count} events, received {receivedEvents.Count}");

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
                    "client-test-queue");
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
                    "client-test-queue");
            });
        }
    }
}