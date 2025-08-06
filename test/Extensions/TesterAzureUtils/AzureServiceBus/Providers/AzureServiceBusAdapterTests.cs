using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Serialization;
using Orleans.Streams;
using Orleans.Streaming.AzureServiceBus.Messages;
using Orleans.Streaming.AzureServiceBus.Providers;
using Orleans.TestingHost.Utils;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests.Providers;

[Collection(TestEnvironmentFixture.DefaultCollection)]
public class AzureServiceBusAdapterTests
{
    private readonly AzureServiceBusOptions _options;
    private readonly ILogger<AzureServiceBusAdapter> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly AzureServiceBusConnectionManager _connectionManager;
    private readonly AzureServiceBusMessageFactory _messageFactory;

    public AzureServiceBusAdapterTests()
    {
        _options = new AzureServiceBusOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==",
            EntityName = "test-queue",
            EntityMode = ServiceBusEntityMode.Queue
        };
        
        _logger = Substitute.For<ILogger<AzureServiceBusAdapter>>();
        _loggerFactory = Substitute.For<ILoggerFactory>();
        _loggerFactory.CreateLogger<ServiceBusAdapterReceiver>().Returns(Substitute.For<ILogger<ServiceBusAdapterReceiver>>());
        
        var connectionLogger = Substitute.For<ILogger<AzureServiceBusConnectionManager>>();
        _connectionManager = Substitute.For<AzureServiceBusConnectionManager>(_options, connectionLogger);
        
        var serializer = Substitute.For<Serializer>();
        _messageFactory = new AzureServiceBusMessageFactory(serializer);
    }

    [Fact]
    public void Constructor_WithValidParameters_InitializesCorrectly()
    {
        // Arrange & Act
        var adapter = new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            _logger,
            _loggerFactory,
            "test-provider");

        // Assert
        Assert.Equal("test-provider", adapter.Name);
        Assert.True(adapter.IsRewindable);
        Assert.Equal(StreamProviderDirection.ReadWrite, adapter.Direction);
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AzureServiceBusAdapter(
            null!,
            _connectionManager,
            _messageFactory,
            _logger,
            _loggerFactory,
            "test-provider"));
    }

    [Fact]
    public void Constructor_WithNullConnectionManager_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AzureServiceBusAdapter(
            _options,
            null!,
            _messageFactory,
            _logger,
            _loggerFactory,
            "test-provider"));
    }

    [Fact]
    public void Constructor_WithNullMessageFactory_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            null!,
            _logger,
            _loggerFactory,
            "test-provider"));
    }

    [Fact]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            null!,
            _loggerFactory,
            "test-provider"));
    }

    [Fact]
    public void Constructor_WithNullLoggerFactory_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            _logger,
            null!,
            "test-provider"));
    }

    [Fact]
    public void Constructor_WithNullProviderName_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            _logger,
            _loggerFactory,
            null!));
    }

    [Fact]
    public void CreateReceiver_WithValidQueueId_ReturnsReceiver()
    {
        // Arrange
        var adapter = new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            _logger,
            _loggerFactory,
            "test-provider");

        var queueId = QueueId.GetQueueId("test-queue", 0, 1);

        // Act
        var receiver = adapter.CreateReceiver(queueId);

        // Assert
        Assert.NotNull(receiver);
        Assert.IsType<ServiceBusAdapterReceiver>(receiver);
    }

    [Fact]
    public async Task QueueMessageBatchAsync_WithNullEvents_ThrowsArgumentNullException()
    {
        // Arrange
        var adapter = new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            _logger,
            _loggerFactory,
            "test-provider");

        var streamId = StreamId.Create("test-namespace", "test-key");
        var requestContext = new Dictionary<string, object>();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => 
            adapter.QueueMessageBatchAsync<object>(streamId, null!, null!, requestContext));
    }

    [Fact]
    public async Task QueueMessageBatchAsync_WithNullRequestContext_ThrowsArgumentNullException()
    {
        // Arrange
        var adapter = new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            _logger,
            _loggerFactory,
            "test-provider");

        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = new[] { "test-event" };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => 
            adapter.QueueMessageBatchAsync(streamId, events, null!, null!));
    }

    [Fact]
    public async Task QueueMessageBatchAsync_WithEmptyEvents_DoesNotThrow()
    {
        // Arrange
        var adapter = new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            _logger,
            _loggerFactory,
            "test-provider");

        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = Array.Empty<string>();
        var requestContext = new Dictionary<string, object>();

        // Act
        await adapter.QueueMessageBatchAsync(streamId, events, null!, requestContext);

        // Assert - No exception should be thrown
    }

    [Fact]
    public void Dispose_WhenCalled_DisposesCorrectly()
    {
        // Arrange
        var adapter = new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            _logger,
            _loggerFactory,
            "test-provider");

        // Act
        adapter.Dispose();

        // Assert - Should not throw and should dispose connection manager
        _connectionManager.Received(1).Dispose();
    }

    [Fact]
    public void Dispose_WhenCalledMultipleTimes_DoesNotThrow()
    {
        // Arrange
        var adapter = new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            _logger,
            _loggerFactory,
            "test-provider");

        // Act & Assert
        adapter.Dispose();
        adapter.Dispose(); // Should not throw
    }

    [Fact]
    public async Task QueueMessageBatchAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var adapter = new AzureServiceBusAdapter(
            _options,
            _connectionManager,
            _messageFactory,
            _logger,
            _loggerFactory,
            "test-provider");

        var streamId = StreamId.Create("test-namespace", "test-key");
        var events = new[] { "test-event" };
        var requestContext = new Dictionary<string, object>();

        adapter.Dispose();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(() => 
            adapter.QueueMessageBatchAsync(streamId, events, null!, requestContext));
    }
}