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

namespace Orleans.Streaming.AzureServiceBus.Tests.Performance;

/// <summary>
/// Performance tests for Azure Service Bus streaming functionality.
/// Validates throughput, latency, and resource utilization under various load conditions.
/// </summary>
[TestCategory("ServiceBus"), TestCategory("Streaming"), TestCategory("Performance")]
[Collection("ServiceBusEmulator")]
public class ServiceBusStreamPerformanceTests : TestClusterPerTest
{
    private const string StreamProviderName = "ServiceBusPerformanceProvider";
    private const string StreamNamespace = "SBPerformanceTestNamespace";

    private readonly ITestOutputHelper _output;
    private readonly ServiceBusEmulatorFixture _emulatorFixture;

    public ServiceBusStreamPerformanceTests(ITestOutputHelper output, ServiceBusEmulatorFixture emulatorFixture)
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
    /// Tests basic throughput performance with moderate message volume.
    /// </summary>
    [Fact]
    public async Task ServiceBusPerformance_BasicThroughput()
    {
        _output.WriteLine("Testing basic throughput performance");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var metrics = new ServiceBusTestUtils.PerformanceMetrics();
        var receivedEvents = new List<TestEvent>();

        var handle = await stream.SubscribeAsync((evt, token) =>
        {
            var receiveTime = DateTime.UtcNow;
            var latency = receiveTime - evt.Timestamp.DateTime;
            
            lock (receivedEvents)
            {
                receivedEvents.Add(evt);
                metrics.RecordMessageLatency(latency);
            }
            
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        metrics.StartTime = DateTime.UtcNow;

        // Send moderate volume of messages
        const int messageCount = 100;
        var testEvents = ServiceBusTestUtils.GenerateTestEvents(messageCount).ToList();

        var sendTasks = testEvents.Select(async evt =>
        {
            await stream.OnNextAsync(evt);
            Interlocked.Increment(ref metrics.MessagesSent);
        });

        await Task.WhenAll(sendTasks);
        
        // Wait for all messages to be processed
        var timeout = DateTime.UtcNow.AddMinutes(2);
        while (receivedEvents.Count < messageCount && DateTime.UtcNow < timeout)
        {
            await Task.Delay(500);
        }

        metrics.EndTime = DateTime.UtcNow;
        metrics.MessagesReceived = receivedEvents.Count;

        // Report performance metrics
        _output.WriteLine($"Messages sent: {metrics.MessagesSent}");
        _output.WriteLine($"Messages received: {metrics.MessagesReceived}");
        _output.WriteLine($"Duration: {metrics.Duration.TotalSeconds:F2} seconds");
        _output.WriteLine($"Throughput: {metrics.ThroughputPerSecond:F2} messages/second");
        _output.WriteLine($"Average latency: {metrics.AverageLatencyMs:F2} ms");
        _output.WriteLine($"P95 latency: {metrics.P95LatencyMs:F2} ms");

        // Basic assertions
        Assert.Equal(messageCount, metrics.MessagesReceived);
        Assert.True(metrics.ThroughputPerSecond > 0, "Throughput should be greater than 0");
        Assert.True(metrics.AverageLatencyMs >= 0, "Average latency should be non-negative");

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests concurrent producer performance.
    /// </summary>
    [Fact]
    public async Task ServiceBusPerformance_ConcurrentProducers()
    {
        _output.WriteLine("Testing concurrent producer performance");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);

        var receivedEvents = new List<TestEvent>();
        var consumer = provider.GetStream<TestEvent>(streamId);
        
        var handle = await consumer.SubscribeAsync((evt, token) =>
        {
            lock (receivedEvents)
            {
                receivedEvents.Add(evt);
            }
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        var startTime = DateTime.UtcNow;

        // Create multiple concurrent producers
        const int producerCount = 5;
        const int messagesPerProducer = 20;
        
        var producerTasks = Enumerable.Range(0, producerCount).Select(async producerId =>
        {
            var producer = provider.GetStream<TestEvent>(streamId);
            var events = ServiceBusTestUtils.GenerateTestEvents(messagesPerProducer, $"producer{producerId}").ToList();
            
            foreach (var evt in events)
            {
                await producer.OnNextAsync(evt);
            }
        });

        await Task.WhenAll(producerTasks);

        // Wait for all messages to be processed
        var expectedMessages = producerCount * messagesPerProducer;
        var timeout = DateTime.UtcNow.AddMinutes(2);
        
        while (receivedEvents.Count < expectedMessages && DateTime.UtcNow < timeout)
        {
            await Task.Delay(500);
        }

        var endTime = DateTime.UtcNow;
        var duration = endTime - startTime;
        var throughput = receivedEvents.Count / duration.TotalSeconds;

        _output.WriteLine($"Concurrent producers: {producerCount}");
        _output.WriteLine($"Messages per producer: {messagesPerProducer}");
        _output.WriteLine($"Total expected messages: {expectedMessages}");
        _output.WriteLine($"Messages received: {receivedEvents.Count}");
        _output.WriteLine($"Duration: {duration.TotalSeconds:F2} seconds");
        _output.WriteLine($"Throughput: {throughput:F2} messages/second");

        // Verify all producers contributed
        for (int i = 0; i < producerCount; i++)
        {
            var producerKey = $"producer{i}";
            var messagesFromProducer = receivedEvents.Count(e => e.StreamKey == producerKey);
            _output.WriteLine($"Messages from {producerKey}: {messagesFromProducer}");
        }

        Assert.Equal(expectedMessages, receivedEvents.Count);

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests large message handling performance.
    /// </summary>
    [Fact]
    public async Task ServiceBusPerformance_LargeMessages()
    {
        _output.WriteLine("Testing large message handling performance");

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

        var startTime = DateTime.UtcNow;

        // Create large messages (simulate with larger data payload)
        const int messageCount = 10;
        var largeDataPayload = new string('X', 50000); // 50KB payload
        
        var largeEvents = Enumerable.Range(0, messageCount).Select(i => new TestEvent
        {
            Id = Guid.NewGuid(),
            StreamKey = "large-message-test",
            Timestamp = DateTimeOffset.UtcNow,
            Data = $"Large message {i}: {largeDataPayload}",
            Properties = new Dictionary<string, object>
            {
                { "Index", i },
                { "MessageSize", largeDataPayload.Length },
                { "Index", i }
            }
        }).ToList();

        foreach (var evt in largeEvents)
        {
            await stream.OnNextAsync(evt);
        }

        // Wait for all large messages to be processed (longer timeout for large messages)
        var timeout = DateTime.UtcNow.AddMinutes(3);
        while (receivedEvents.Count < messageCount && DateTime.UtcNow < timeout)
        {
            await Task.Delay(1000);
        }

        var endTime = DateTime.UtcNow;
        var duration = endTime - startTime;
        var throughput = receivedEvents.Count / duration.TotalSeconds;

        _output.WriteLine($"Large messages sent: {messageCount}");
        _output.WriteLine($"Message payload size: {largeDataPayload.Length} bytes");
        _output.WriteLine($"Messages received: {receivedEvents.Count}");
        _output.WriteLine($"Duration: {duration.TotalSeconds:F2} seconds");
        _output.WriteLine($"Throughput: {throughput:F2} messages/second");

        Assert.Equal(messageCount, receivedEvents.Count);
        
        // Verify message integrity
        foreach (var evt in receivedEvents)
        {
            Assert.Contains("Large message", evt.Data);
            Assert.True(evt.Data.Length > 50000, "Message should contain large payload");
        }

        await handle.UnsubscribeAsync();
    }

    /// <summary>
    /// Tests memory usage and resource cleanup during high-volume streaming.
    /// </summary>
    [Fact]
    public async Task ServiceBusPerformance_ResourceCleanup()
    {
        _output.WriteLine("Testing resource cleanup during high-volume streaming");

        var provider = this.Client.GetStreamProvider(StreamProviderName);
        var streamId = ServiceBusTestUtils.CreateTestStreamId(StreamNamespace);
        var stream = provider.GetStream<TestEvent>(streamId);

        var receivedCount = 0;
        var handle = await stream.SubscribeAsync((evt, token) =>
        {
            Interlocked.Increment(ref receivedCount);
            return Task.CompletedTask;
        });

        // Allow subscription to initialize
        await Task.Delay(1000);

        // Get initial memory usage
        var initialMemory = GC.GetTotalMemory(true);
        _output.WriteLine($"Initial memory: {initialMemory / 1024 / 1024:F2} MB");

        // Send multiple batches of messages
        const int batchCount = 5;
        const int messagesPerBatch = 50;

        for (int batch = 0; batch < batchCount; batch++)
        {
            _output.WriteLine($"Processing batch {batch + 1}/{batchCount}");
            
            var batchEvents = ServiceBusTestUtils.GenerateTestEvents(messagesPerBatch, $"batch{batch}").ToList();
            
            foreach (var evt in batchEvents)
            {
                await stream.OnNextAsync(evt);
            }

            // Wait for batch to be processed
            var expectedCount = (batch + 1) * messagesPerBatch;
            var timeout = DateTime.UtcNow.AddSeconds(30);
            
            while (receivedCount < expectedCount && DateTime.UtcNow < timeout)
            {
                await Task.Delay(200);
            }

            // Force garbage collection and check memory
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            
            var currentMemory = GC.GetTotalMemory(false);
            _output.WriteLine($"Memory after batch {batch + 1}: {currentMemory / 1024 / 1024:F2} MB");
        }

        var finalMemory = GC.GetTotalMemory(true);
        var memoryIncrease = finalMemory - initialMemory;
        
        _output.WriteLine($"Final memory: {finalMemory / 1024 / 1024:F2} MB");
        _output.WriteLine($"Memory increase: {memoryIncrease / 1024 / 1024:F2} MB");
        _output.WriteLine($"Total messages processed: {receivedCount}");

        var expectedTotal = batchCount * messagesPerBatch;
        Assert.Equal(expectedTotal, receivedCount);

        // Memory increase should be reasonable (less than 100MB for this test)
        Assert.True(memoryIncrease < 100 * 1024 * 1024, 
            $"Memory increase of {memoryIncrease / 1024 / 1024:F2} MB seems excessive");

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
                    "performance-test-queue");
                configure.BatchSize = 20; // Larger batch size for performance
                configure.MaxConcurrentCalls = 5; // More concurrent processing
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
                    "performance-test-queue");
            });
        }
    }
}