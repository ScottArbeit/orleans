using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Streaming.AzureServiceBus.Providers;
using Orleans.TestingHost.Utils;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests.Providers;

[Collection(TestEnvironmentFixture.DefaultCollection)]
public class AzureServiceBusConnectionManagerTests
{
    private readonly AzureServiceBusOptions _options;
    private readonly ILogger<AzureServiceBusConnectionManager> _logger;

    public AzureServiceBusConnectionManagerTests()
    {
        _options = new AzureServiceBusOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==",
            EntityName = "test-queue",
            EntityMode = ServiceBusEntityMode.Queue
        };
        
        _logger = Substitute.For<ILogger<AzureServiceBusConnectionManager>>();
    }

    [Fact]
    public void Constructor_WithValidParameters_InitializesCorrectly()
    {
        // Arrange & Act
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Assert
        Assert.True(connectionManager.IsHealthy);
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AzureServiceBusConnectionManager(null!, _logger));
    }

    [Fact]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AzureServiceBusConnectionManager(_options, null!));
    }

    [Fact]
    public async Task GetSenderAsync_WithValidEntityName_ReturnsSender()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Act
        var sender = await connectionManager.GetSenderAsync("test-queue");

        // Assert
        Assert.NotNull(sender);
    }

    [Fact]
    public async Task GetSenderAsync_WithNullEntityName_ThrowsArgumentException()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => connectionManager.GetSenderAsync(null!));
    }

    [Fact]
    public async Task GetSenderAsync_WithEmptyEntityName_ThrowsArgumentException()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => connectionManager.GetSenderAsync(string.Empty));
    }

    [Fact]
    public async Task GetReceiverAsync_WithValidEntityName_ReturnsReceiver()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Act
        var receiver = await connectionManager.GetReceiverAsync("test-queue");

        // Assert
        Assert.NotNull(receiver);
    }

    [Fact]
    public async Task GetReceiverAsync_WithNullEntityName_ThrowsArgumentException()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => connectionManager.GetReceiverAsync(null!));
    }

    [Fact]
    public async Task GetReceiverAsync_WithEmptyEntityName_ThrowsArgumentException()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => connectionManager.GetReceiverAsync(string.Empty));
    }

    [Fact]
    public async Task GetReceiverAsync_WithTopicModeButNoSubscription_ThrowsInvalidOperationException()
    {
        // Arrange
        var topicOptions = new AzureServiceBusOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==",
            EntityName = "test-topic",
            EntityMode = ServiceBusEntityMode.Topic,
            SubscriptionName = "" // Empty subscription name
        };
        var connectionManager = new AzureServiceBusConnectionManager(topicOptions, _logger);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => connectionManager.GetReceiverAsync("test-topic"));
    }

    [Fact]
    public async Task CheckHealthAsync_WithValidConnection_ReturnsTrue()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Act
        var isHealthy = await connectionManager.CheckHealthAsync();

        // Assert
        Assert.True(isHealthy);
    }

    [Fact]
    public async Task RecoverConnectionAsync_WhenCalled_CompletesSuccessfully()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Act & Assert - Should not throw
        await connectionManager.RecoverConnectionAsync();
    }

    [Fact]
    public void Dispose_WhenCalled_DisposesCorrectly()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Act
        connectionManager.Dispose();

        // Assert
        Assert.False(connectionManager.IsHealthy);
    }

    [Fact]
    public void Dispose_WhenCalledMultipleTimes_DoesNotThrow()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);

        // Act & Assert
        connectionManager.Dispose();
        connectionManager.Dispose(); // Should not throw
    }

    [Fact]
    public async Task GetSenderAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);
        connectionManager.Dispose();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(() => connectionManager.GetSenderAsync("test-queue"));
    }

    [Fact]
    public async Task GetReceiverAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var connectionManager = new AzureServiceBusConnectionManager(_options, _logger);
        connectionManager.Dispose();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(() => connectionManager.GetReceiverAsync("test-queue"));
    }
}