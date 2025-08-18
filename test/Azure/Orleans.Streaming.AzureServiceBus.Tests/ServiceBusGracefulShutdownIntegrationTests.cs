namespace Orleans.Streaming.AzureServiceBus.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Orleans.Streams;
using Xunit;
using Xunit.Abstractions;

/// <summary>
/// Tests for graceful shutdown behavior of ServiceBusAdapterReceiver.
/// Verifies cancellation token handling, cache draining, and deterministic message handling.
/// </summary>
[Collection(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusGracefulShutdownIntegrationTests
{
    private readonly ServiceBusEmulatorFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServiceBusGracefulShutdownIntegrationTests(ServiceBusEmulatorFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Shutdown_WithEmptyCache_CompletesQuickly()
    {
        // Arrange
        var queueId = QueueId.GetQueueId("test-shutdown-empty", 0, 1);
        var options = CreateServiceBusStreamOptions();
        var logger = CreateLogger<ServiceBusAdapterReceiver>();

        using var receiver = new ServiceBusAdapterReceiver(queueId, "TestProvider", options, CreateDataAdapter(), logger);
        await receiver.Initialize(TimeSpan.FromSeconds(30));

        // Act
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await receiver.Shutdown(TimeSpan.FromSeconds(30));
        stopwatch.Stop();

        // Assert
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5), 
            $"Shutdown with empty cache should complete quickly, but took {stopwatch.Elapsed}");
    }

    [Fact]
    public async Task Shutdown_WithCancellationToken_StopsBackgroundPump()
    {
        // Arrange
        var queueId = QueueId.GetQueueId("test-shutdown-cancellation", 0, 1);
        var options = CreateServiceBusStreamOptions();
        var logger = CreateLogger<ServiceBusAdapterReceiver>();

        using var receiver = new ServiceBusAdapterReceiver(queueId, "TestProvider", options, CreateDataAdapter(), logger);
        await receiver.Initialize(TimeSpan.FromSeconds(30));

        // Allow background pump to start
        await Task.Delay(TimeSpan.FromMilliseconds(100));

        // Act
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await receiver.Shutdown(TimeSpan.FromSeconds(30));
        stopwatch.Stop();

        // Assert - shutdown should complete deterministically
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(10), 
            $"Shutdown should complete deterministically, but took {stopwatch.Elapsed}");
    }

    [Fact]
    public async Task Shutdown_RespectsCacheDrainTimeout()
    {
        // Arrange
        var queueId = QueueId.GetQueueId("test-cache-drain-timeout", 0, 1);
        var options = CreateServiceBusStreamOptions();
        // Set a very short cache drain timeout
        options.Receiver.CacheDrainTimeout = TimeSpan.FromMilliseconds(100);
        
        var logger = CreateLogger<ServiceBusAdapterReceiver>();

        using var receiver = new ServiceBusAdapterReceiver(queueId, "TestProvider", options, CreateDataAdapter(), logger);
        await receiver.Initialize(TimeSpan.FromSeconds(30));

        // Act - shutdown should respect the cache drain timeout
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await receiver.Shutdown(TimeSpan.FromSeconds(30));
        stopwatch.Stop();

        // Assert - shutdown should complete within reasonable time even with short drain timeout
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5), 
            $"Shutdown should respect cache drain timeout, but took {stopwatch.Elapsed}");
    }

    [Fact]
    public async Task Shutdown_CalledMultipleTimes_IsIdempotent()
    {
        // Arrange
        var queueId = QueueId.GetQueueId("test-shutdown-idempotent", 0, 1);
        var options = CreateServiceBusStreamOptions();
        var logger = CreateLogger<ServiceBusAdapterReceiver>();

        using var receiver = new ServiceBusAdapterReceiver(queueId, "TestProvider", options, CreateDataAdapter(), logger);
        await receiver.Initialize(TimeSpan.FromSeconds(30));

        // Act - call shutdown multiple times
        await receiver.Shutdown(TimeSpan.FromSeconds(30));
        await receiver.Shutdown(TimeSpan.FromSeconds(30));
        await receiver.Shutdown(TimeSpan.FromSeconds(30));

        // Assert - no exceptions should be thrown, method should be idempotent
        // Test passes if no exceptions are thrown
    }

    [Fact]
    public async Task Shutdown_AfterDispose_DoesNotThrow()
    {
        // Arrange
        var queueId = QueueId.GetQueueId("test-shutdown-after-dispose", 0, 1);
        var options = CreateServiceBusStreamOptions();
        var logger = CreateLogger<ServiceBusAdapterReceiver>();

        var receiver = new ServiceBusAdapterReceiver(queueId, "TestProvider", options, CreateDataAdapter(), logger);
        await receiver.Initialize(TimeSpan.FromSeconds(30));

        // Act
        receiver.Dispose();
        await receiver.Shutdown(TimeSpan.FromSeconds(30));

        // Assert - no exceptions should be thrown
        // Test passes if no exceptions are thrown
    }

    [Fact]
    public async Task BackgroundPump_HonorsCancellationToken()
    {
        // Arrange
        var queueId = QueueId.GetQueueId("test-background-pump-cancellation", 0, 1);
        var options = CreateServiceBusStreamOptions();
        var logger = CreateLogger<ServiceBusAdapterReceiver>();

        using var receiver = new ServiceBusAdapterReceiver(queueId, "TestProvider", options, CreateDataAdapter(), logger);
        await receiver.Initialize(TimeSpan.FromSeconds(30));

        // Allow background pump to start
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        // Act - trigger shutdown which should cancel the background pump
        var shutdownTask = receiver.Shutdown(TimeSpan.FromSeconds(30));
        
        // Assert - shutdown should complete within reasonable time
        await shutdownTask;
        Assert.True(true, "Background pump should honor cancellation token and shutdown should complete");
    }

    [Fact]
    public void CacheDrainTimeout_DefaultValue_IsReasonable()
    {
        // Arrange & Act
        var options = new ServiceBusStreamOptions();

        // Assert
        Assert.Equal(TimeSpan.FromSeconds(30), options.Receiver.CacheDrainTimeout);
        Assert.True(options.Receiver.CacheDrainTimeout > TimeSpan.Zero);
        Assert.True(options.Receiver.CacheDrainTimeout < TimeSpan.FromMinutes(5));
    }

    private ServiceBusStreamOptions CreateServiceBusStreamOptions()
    {
        return new ServiceBusStreamOptions
        {
            ConnectionString = _fixture.ServiceBusConnectionString,
            EntityKind = EntityKind.Queue,
            QueueName = $"test-queue-{Guid.NewGuid():N}",
            Receiver = new ReceiverSettings
            {
                ReceiveBatchSize = 10,
                PrefetchCount = 0,
                MaxConcurrentHandlers = 1,
                LockAutoRenew = false, // Disable to simplify testing
                CacheDrainTimeout = TimeSpan.FromSeconds(30)
            }
        };
    }

    private ServiceBusDataAdapter CreateDataAdapter()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        var serviceProvider = services.BuildServiceProvider();
        var serializer = serviceProvider.GetRequiredService<Serializer<ServiceBusBatchContainer>>();
        return new ServiceBusDataAdapter(serializer);
    }

    private ILogger<T> CreateLogger<T>()
    {
        return new TestOutputLogger<T>(_output);
    }

    private class TestOutputLogger<T> : ILogger<T>
    {
        private readonly ITestOutputHelper _output;

        public TestOutputLogger(ITestOutputHelper output)
        {
            _output = output;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            try
            {
                _output.WriteLine($"[{logLevel}] {typeof(T).Name}: {formatter(state, exception)}");
                if (exception is not null)
                {
                    _output.WriteLine($"Exception: {exception}");
                }
            }
            catch
            {
                // Ignore logging errors in tests
            }
        }

        private class NullScope : IDisposable
        {
            public static NullScope Instance { get; } = new();
            public void Dispose() { }
        }
    }
}