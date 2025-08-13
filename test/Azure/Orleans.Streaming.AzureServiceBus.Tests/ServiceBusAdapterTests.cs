namespace Orleans.Streaming.AzureServiceBus.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Orleans.Streams;
using Xunit;

/// <summary>
/// Tests for ServiceBusAdapter publishing functionality.
/// </summary>
[Collection(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusAdapterTests
{
    private readonly ServiceBusEmulatorFixture _fixture;

    public ServiceBusAdapterTests(ServiceBusEmulatorFixture fixture)
    {
        _fixture = fixture;
    }
    [Fact]
    public void Constructor_ValidParameters_CreatesAdapter()
    {
        // Arrange
        var options = CreateValidOptions();
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(options);
        var logger = CreateLogger();

        // Act
        var adapter = new ServiceBusAdapter("test-provider", options, dataAdapter, queueMapper, logger);

        // Assert
        Assert.NotNull(adapter);
        Assert.Equal("test-provider", adapter.Name);
        Assert.Equal(StreamProviderDirection.WriteOnly, adapter.Direction);
        Assert.False(adapter.IsRewindable);
        
        // Cleanup
        adapter.Dispose();
    }

    [Fact]
    public void Constructor_NullProviderName_ThrowsArgumentNullException()
    {
        // Arrange
        var options = CreateValidOptions();
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(options);
        var logger = CreateLogger();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            new ServiceBusAdapter(null!, options, dataAdapter, queueMapper, logger));
    }

    [Fact]
    public void Constructor_EmptyProviderName_ThrowsArgumentException()
    {
        // Arrange
        var options = CreateValidOptions();
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(options);
        var logger = CreateLogger();

        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            new ServiceBusAdapter("", options, dataAdapter, queueMapper, logger));
    }

    [Fact]
    public void Constructor_NullOptions_ThrowsArgumentNullException()
    {
        // Arrange
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(CreateValidOptions());
        var logger = CreateLogger();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            new ServiceBusAdapter("test-provider", null!, dataAdapter, queueMapper, logger));
    }

    [Fact]
    public void Constructor_InvalidConnectionConfiguration_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
            // No connection string or credential
        };
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(options);
        var logger = CreateLogger();

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => 
            new ServiceBusAdapter("test-provider", options, dataAdapter, queueMapper, logger));
    }

    [Fact]
    public async Task QueueMessageBatchAsync_EmptyEvents_CompletesWithoutError()
    {
        // Arrange
        var options = CreateValidOptionsWithFixture();
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(options);
        var logger = CreateLogger();

        using var adapter = new ServiceBusAdapter("test-provider", options, dataAdapter, queueMapper, logger);
        var uniqueKey = $"adapter-test-{Guid.NewGuid():N}";
        var streamId = StreamId.Create("test-namespace", uniqueKey);
        var emptyEvents = new List<string>();
        var requestContext = new Dictionary<string, object>();

        // Act & Assert - should not throw and should complete successfully with real Service Bus
        await adapter.QueueMessageBatchAsync(streamId, emptyEvents, null, requestContext);
        
        // Verify we can also send a non-empty batch to confirm Service Bus connectivity
        var events = new List<string> { "test-event" };
        await adapter.QueueMessageBatchAsync(streamId, events, null, requestContext);
        
        // Clean up: drain messages we sent to avoid interference with other tests
        await using var client = _fixture.CreateServiceBusClient();
        await using var receiver = client.CreateReceiver(ServiceBusEmulatorFixture.QueueName);
        
        while (true)
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(100));
            if (message is null) break;
            await receiver.CompleteMessageAsync(message);
        }
    }

    [Fact]
    public async Task QueueMessageBatchAsync_NullEvents_ThrowsArgumentNullException()
    {
        // Arrange
        var options = CreateValidOptions();
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(options);
        var logger = CreateLogger();

        using var adapter = new ServiceBusAdapter("test-provider", options, dataAdapter, queueMapper, logger);
        var streamId = StreamId.Create("test-namespace", "test-key");
        var requestContext = new Dictionary<string, object>();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => 
            adapter.QueueMessageBatchAsync<string>(streamId, null!, null, requestContext));
    }

    [Fact]
    public async Task QueueMessageBatchAsync_NullRequestContext_ThrowsArgumentNullException()
    {
        // Arrange
        var options = CreateValidOptions();
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(options);
        var logger = CreateLogger();

        using var adapter = new ServiceBusAdapter("test-provider", options, dataAdapter, queueMapper, logger);
        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = new List<string> { "event1" };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => 
            adapter.QueueMessageBatchAsync(streamId, events, null, null!));
    }

    [Fact]
    public void CreateReceiver_NotImplemented_ThrowsNotImplementedException()
    {
        // Arrange
        var options = CreateValidOptions();
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(options);
        var logger = CreateLogger();

        using var adapter = new ServiceBusAdapter("test-provider", options, dataAdapter, queueMapper, logger);
        var queueId = QueueId.GetQueueId("test", 0, 0);

        // Act & Assert
        Assert.Throws<NotImplementedException>(() => adapter.CreateReceiver(queueId));
    }

    [Fact]
    public void Dispose_MultipleCallsSafe()
    {
        // Arrange
        var options = CreateValidOptions();
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(options);
        var logger = CreateLogger();

        var adapter = new ServiceBusAdapter("test-provider", options, dataAdapter, queueMapper, logger);

        // Act & Assert - should not throw
        adapter.Dispose();
        adapter.Dispose(); // Second dispose should be safe
    }

    [Fact]
    public async Task QueueMessageBatchAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var options = CreateValidOptions();
        var dataAdapter = CreateDataAdapter();
        var queueMapper = CreateQueueMapper(options);
        var logger = CreateLogger();

        var adapter = new ServiceBusAdapter("test-provider", options, dataAdapter, queueMapper, logger);
        adapter.Dispose();

        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = new List<string> { "event1" };
        var requestContext = new Dictionary<string, object>();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(() => 
            adapter.QueueMessageBatchAsync(streamId, events, null, requestContext));
    }

    private static ServiceBusStreamOptions CreateValidOptions()
    {
        return new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue",
            ConnectionString = "Endpoint=sb://localhost:5672/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
            Publisher = new PublisherSettings
            {
                BatchSize = 10,
                SessionIdStrategy = SessionIdStrategy.None
            }
        };
    }

    private ServiceBusStreamOptions CreateValidOptionsWithFixture()
    {
        return new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = ServiceBusEmulatorFixture.QueueName,
            ConnectionString = _fixture.ServiceBusConnectionString,
            Publisher = new PublisherSettings
            {
                BatchSize = 10,
                SessionIdStrategy = SessionIdStrategy.None
            }
        };
    }

    private static ServiceBusDataAdapter CreateDataAdapter()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        var serviceProvider = services.BuildServiceProvider();
        var serializer = serviceProvider.GetRequiredService<Serializer<ServiceBusBatchContainer>>();
        return new ServiceBusDataAdapter(serializer);
    }

    private static ServiceBusQueueMapper CreateQueueMapper(ServiceBusStreamOptions options)
    {
        return new ServiceBusQueueMapper(options, "test-provider");
    }

    private static ILogger<ServiceBusAdapter> CreateLogger()
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        return loggerFactory.CreateLogger<ServiceBusAdapter>();
    }
}