#nullable enable
using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Configuration;
using Orleans.Streaming.ServiceBus.Storage;
using Xunit;

namespace Orleans.Tests.ServiceBus;

/// <summary>
/// Tests for <see cref="AzureServiceBusDataManager"/>.
/// </summary>
public class AzureServiceBusDataManagerTests
{
    private readonly ILoggerFactory _loggerFactory = new NullLoggerFactory();

    [Fact]
    public void Constructor_WithValidQueueOptions_ShouldSucceed()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue,
            QueueName = "test-queue"
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        // Act
        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "test-queue", options);

        // Assert
        Assert.Equal("test-queue", dataManager.EntityName);
        Assert.Equal(ServiceBusTopologyType.Queue, dataManager.TopologyType);
    }

    [Fact]
    public void Constructor_WithValidTopicOptions_ShouldSucceed()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Topic,
            TopicName = "test-topic",
            SubscriptionName = "test-subscription"
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        // Act
        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "test-topic", options);

        // Assert
        Assert.Equal("test-topic", dataManager.EntityName);
        Assert.Equal(ServiceBusTopologyType.Topic, dataManager.TopologyType);
    }

    [Fact]
    public void Constructor_WithNullLoggerFactory_ShouldThrow()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue,
            QueueName = "test-queue"
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => 
            new AzureServiceBusDataManager(null!, "test-queue", options));
        Assert.Equal("loggerFactory", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithNullEntityName_ShouldThrow()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue,
            QueueName = "test-queue"
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => 
            new AzureServiceBusDataManager(_loggerFactory, null!, options));
        Assert.Equal("entityName", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithEmptyEntityName_ShouldThrow()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue,
            QueueName = "test-queue"
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => 
            new AzureServiceBusDataManager(_loggerFactory, "", options));
        Assert.Equal("entityName", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithNullOptions_ShouldThrow()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => 
            new AzureServiceBusDataManager(_loggerFactory, "test-queue", null!));
        Assert.Equal("options", exception.ParamName);
    }

    [Fact]
    public void Constructor_QueueTopologyWithoutQueueName_ShouldUseEntityName()
    {
        // Arrange - options don't have QueueName, but still provide entity name to pass first validation
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue
            // Missing QueueName - will use entityName from parameter
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        // Act - create with empty options but provide entityName - should validate and use entityName
        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "test-queue", options);
        
        // Assert - should use the entity name from parameter since options don't specify it
        Assert.Equal("test-queue", dataManager.EntityName);
    }

    [Fact]
    public void Constructor_TopicTopologyWithoutTopicName_ShouldUseEntityName()
    {
        // Arrange - options don't have TopicName, but still provide entity name to pass first validation
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Topic,
            SubscriptionName = "test-subscription"
            // Missing TopicName - will use entityName from parameter
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        // Act - create with empty options but provide entityName - should validate and use entityName
        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "test-topic", options);
        
        // Assert - should use the entity name from parameter since options don't specify it
        Assert.Equal("test-topic", dataManager.EntityName);
    }

    [Fact]
    public void Constructor_UsesEntityNameFromOptions()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue,
            QueueName = "queue-from-options"
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        // Act
        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "queue-from-param", options);

        // Assert - should use QueueName from options, not from parameter
        Assert.Equal("queue-from-options", dataManager.EntityName);
    }

    [Fact]
    public void Constructor_TopicTopology_UsesTopicNameFromOptions()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Topic,
            TopicName = "topic-from-options",
            SubscriptionName = "test-subscription"
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        // Act
        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "topic-from-param", options);

        // Assert - should use TopicName from options, not from parameter
        Assert.Equal("topic-from-options", dataManager.EntityName);
    }

    [Fact]
    public async Task SendMessageAsync_WithNullMessage_ShouldThrow()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue,
            QueueName = "test-queue"
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "test-queue", options);

        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentNullException>(() => 
            dataManager.SendMessageAsync(null!));
        Assert.Equal("message", exception.ParamName);
    }

    [Fact]
    public void CreateProcessor_BeforeInit_ShouldThrow()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue,
            QueueName = "test-queue"
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "test-queue", options);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => 
            dataManager.CreateProcessor());
        Assert.Contains("Service Bus client is not initialized", exception.Message);
    }

    [Fact]
    public void CreateSessionProcessor_BeforeInit_ShouldThrow()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue,
            QueueName = "test-queue"
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "test-queue", options);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => 
            dataManager.CreateSessionProcessor());
        Assert.Contains("Service Bus client is not initialized", exception.Message);
    }

    [Fact]
    public async Task DisposeAsync_ShouldMarkAsDisposed()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue,
            QueueName = "test-queue"
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "test-queue", options);

        // Act
        await dataManager.DisposeAsync();

        // Assert
        var exception = Assert.Throws<ObjectDisposedException>(() => 
            dataManager.CreateProcessor());
        Assert.Equal(nameof(AzureServiceBusDataManager), exception.ObjectName);
    }

    [Fact]
    public async Task DisposeAsync_MultipleCallsSafe()
    {
        // Arrange
        var options = new AzureServiceBusOptions
        {
            TopologyType = ServiceBusTopologyType.Queue,
            QueueName = "test-queue"
        };
        options.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=testkey;SharedAccessKey=testvalue");

        var dataManager = new AzureServiceBusDataManager(_loggerFactory, "test-queue", options);

        // Act & Assert - should not throw
        await dataManager.DisposeAsync();
        await dataManager.DisposeAsync(); // Second call should be safe
    }
}