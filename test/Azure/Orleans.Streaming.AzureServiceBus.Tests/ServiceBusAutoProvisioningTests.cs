using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Streaming.AzureServiceBus.Tests;

/// <summary>
/// Tests for Service Bus auto-provisioning functionality.
/// These tests validate that entities are created automatically when AutoCreateEntities=true.
/// </summary>
[Collection(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusAutoProvisioningTests
{
    private readonly ServiceBusEmulatorFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServiceBusAutoProvisioningTests(ServiceBusEmulatorFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task Provisioner_Should_Create_Queue_When_AutoCreate_Enabled()
    {
        // Arrange
        var randomQueueName = $"test-queue-{Guid.NewGuid():N}";
        var adminClient = new ServiceBusAdministrationClient(_fixture.ServiceBusConnectionString);
        var logger = new TestLogger<ServiceBusProvisioner>(_output);
        var provisioner = new ServiceBusProvisioner(adminClient, logger);
        
        var options = new ServiceBusStreamOptions
        {
            AutoCreateEntities = true,
            EntityKind = EntityKind.Queue,
            QueueName = randomQueueName,
            EntityCount = 1
        };

        // Ensure queue doesn't exist initially
        var queueExists = await adminClient.QueueExistsAsync(randomQueueName);
        Assert.False(queueExists, "Queue should not exist initially");

        // Act
        await provisioner.ProvisionEntitiesAsync(options);

        // Assert
        queueExists = await adminClient.QueueExistsAsync(randomQueueName);
        Assert.True(queueExists, "Queue should exist after provisioning");

        // Verify queue properties
        var queueProperties = await adminClient.GetQueueAsync(randomQueueName);
        Assert.Equal(options.Receiver.MaxDeliveryCount, queueProperties.Value.MaxDeliveryCount);
        Assert.True(queueProperties.Value.DeadLetteringOnMessageExpiration);
        Assert.Equal(options.Publisher.MessageTimeToLive, queueProperties.Value.DefaultMessageTimeToLive);

        _output.WriteLine($"Successfully created queue: {randomQueueName}");
    }

    [Fact]
    public async Task Provisioner_Should_Create_Topic_And_Subscription_When_AutoCreate_Enabled()
    {
        // Arrange
        var randomTopicName = $"test-topic-{Guid.NewGuid():N}";
        var randomSubscriptionName = $"test-subscription-{Guid.NewGuid():N}";
        var adminClient = new ServiceBusAdministrationClient(_fixture.ServiceBusConnectionString);
        var logger = new TestLogger<ServiceBusProvisioner>(_output);
        var provisioner = new ServiceBusProvisioner(adminClient, logger);
        
        var options = new ServiceBusStreamOptions
        {
            AutoCreateEntities = true,
            EntityKind = EntityKind.TopicSubscription,
            TopicName = randomTopicName,
            SubscriptionName = randomSubscriptionName,
            EntityCount = 1
        };

        // Ensure entities don't exist initially
        var topicExists = await adminClient.TopicExistsAsync(randomTopicName);
        Assert.False(topicExists, "Topic should not exist initially");

        // Act
        await provisioner.ProvisionEntitiesAsync(options);

        // Assert
        topicExists = await adminClient.TopicExistsAsync(randomTopicName);
        Assert.True(topicExists, "Topic should exist after provisioning");

        var subscriptionExists = await adminClient.SubscriptionExistsAsync(randomTopicName, randomSubscriptionName);
        Assert.True(subscriptionExists, "Subscription should exist after provisioning");

        // Verify properties
        var topicProperties = await adminClient.GetTopicAsync(randomTopicName);
        Assert.Equal(options.Publisher.MessageTimeToLive, topicProperties.Value.DefaultMessageTimeToLive);

        var subscriptionProperties = await adminClient.GetSubscriptionAsync(randomTopicName, randomSubscriptionName);
        Assert.Equal(options.Receiver.MaxDeliveryCount, subscriptionProperties.Value.MaxDeliveryCount);
        Assert.True(subscriptionProperties.Value.DeadLetteringOnMessageExpiration);

        _output.WriteLine($"Successfully created topic: {randomTopicName} and subscription: {randomSubscriptionName}");
    }

    [Fact]
    public async Task Provisioner_Should_Handle_Multiple_Queues_When_EntityCount_Greater_Than_One()
    {
        // Arrange
        var entityPrefix = $"test-multi-queue-{Guid.NewGuid():N}";
        var adminClient = new ServiceBusAdministrationClient(_fixture.ServiceBusConnectionString);
        var logger = new TestLogger<ServiceBusProvisioner>(_output);
        var provisioner = new ServiceBusProvisioner(adminClient, logger);
        
        var options = new ServiceBusStreamOptions
        {
            AutoCreateEntities = true,
            EntityKind = EntityKind.Queue,
            EntityNamePrefix = entityPrefix,
            EntityCount = 3
        };

        // Act
        await provisioner.ProvisionEntitiesAsync(options);

        // Assert - verify all queues were created
        for (int i = 0; i < options.EntityCount; i++)
        {
            string expectedQueueName = $"{entityPrefix}-q-{i}";
            var queueExists = await adminClient.QueueExistsAsync(expectedQueueName);
            Assert.True(queueExists, $"Queue {expectedQueueName} should exist after provisioning");
            _output.WriteLine($"Verified queue exists: {expectedQueueName}");
        }
    }

    [Fact]
    public async Task Provisioner_Should_Handle_Multiple_Subscriptions_When_EntityCount_Greater_Than_One()
    {
        // Arrange
        var topicName = $"test-multi-topic-{Guid.NewGuid():N}";
        var entityPrefix = $"test-multi-sub-{Guid.NewGuid():N}";
        var adminClient = new ServiceBusAdministrationClient(_fixture.ServiceBusConnectionString);
        var logger = new TestLogger<ServiceBusProvisioner>(_output);
        var provisioner = new ServiceBusProvisioner(adminClient, logger);
        
        var options = new ServiceBusStreamOptions
        {
            AutoCreateEntities = true,
            EntityKind = EntityKind.TopicSubscription,
            TopicName = topicName,
            EntityNamePrefix = entityPrefix,
            EntityCount = 3
        };

        // Act
        await provisioner.ProvisionEntitiesAsync(options);

        // Assert - verify topic and all subscriptions were created
        var topicExists = await adminClient.TopicExistsAsync(topicName);
        Assert.True(topicExists, $"Topic {topicName} should exist after provisioning");

        for (int i = 0; i < options.EntityCount; i++)
        {
            string expectedSubscriptionName = $"{entityPrefix}-sub-{i}";
            var subscriptionExists = await adminClient.SubscriptionExistsAsync(topicName, expectedSubscriptionName);
            Assert.True(subscriptionExists, $"Subscription {expectedSubscriptionName} should exist after provisioning");
            _output.WriteLine($"Verified subscription exists: {expectedSubscriptionName}");
        }
    }

    [Fact]
    public async Task Provisioner_Should_Be_Idempotent_Queue_Already_Exists()
    {
        // Arrange
        var randomQueueName = $"test-idempotent-queue-{Guid.NewGuid():N}";
        var adminClient = new ServiceBusAdministrationClient(_fixture.ServiceBusConnectionString);
        var logger = new TestLogger<ServiceBusProvisioner>(_output);
        var provisioner = new ServiceBusProvisioner(adminClient, logger);
        
        var options = new ServiceBusStreamOptions
        {
            AutoCreateEntities = true,
            EntityKind = EntityKind.Queue,
            QueueName = randomQueueName,
            EntityCount = 1
        };

        // Pre-create the queue
        await adminClient.CreateQueueAsync(randomQueueName);
        var initialQueueExists = await adminClient.QueueExistsAsync(randomQueueName);
        Assert.True(initialQueueExists, "Queue should exist after manual creation");

        // Act - should not throw exception
        await provisioner.ProvisionEntitiesAsync(options);

        // Assert - queue should still exist
        var finalQueueExists = await adminClient.QueueExistsAsync(randomQueueName);
        Assert.True(finalQueueExists, "Queue should still exist after idempotent provisioning");

        _output.WriteLine($"Idempotent provisioning succeeded for queue: {randomQueueName}");
    }

    [Fact]
    public async Task Provisioner_Should_Be_Idempotent_Topic_And_Subscription_Already_Exist()
    {
        // Arrange
        var randomTopicName = $"test-idempotent-topic-{Guid.NewGuid():N}";
        var randomSubscriptionName = $"test-idempotent-subscription-{Guid.NewGuid():N}";
        var adminClient = new ServiceBusAdministrationClient(_fixture.ServiceBusConnectionString);
        var logger = new TestLogger<ServiceBusProvisioner>(_output);
        var provisioner = new ServiceBusProvisioner(adminClient, logger);
        
        var options = new ServiceBusStreamOptions
        {
            AutoCreateEntities = true,
            EntityKind = EntityKind.TopicSubscription,
            TopicName = randomTopicName,
            SubscriptionName = randomSubscriptionName,
            EntityCount = 1
        };

        // Pre-create the topic and subscription
        await adminClient.CreateTopicAsync(randomTopicName);
        await adminClient.CreateSubscriptionAsync(randomTopicName, randomSubscriptionName);
        
        var initialTopicExists = await adminClient.TopicExistsAsync(randomTopicName);
        var initialSubscriptionExists = await adminClient.SubscriptionExistsAsync(randomTopicName, randomSubscriptionName);
        Assert.True(initialTopicExists, "Topic should exist after manual creation");
        Assert.True(initialSubscriptionExists, "Subscription should exist after manual creation");

        // Act - should not throw exception
        await provisioner.ProvisionEntitiesAsync(options);

        // Assert - entities should still exist
        var finalTopicExists = await adminClient.TopicExistsAsync(randomTopicName);
        var finalSubscriptionExists = await adminClient.SubscriptionExistsAsync(randomTopicName, randomSubscriptionName);
        Assert.True(finalTopicExists, "Topic should still exist after idempotent provisioning");
        Assert.True(finalSubscriptionExists, "Subscription should still exist after idempotent provisioning");

        _output.WriteLine($"Idempotent provisioning succeeded for topic: {randomTopicName} and subscription: {randomSubscriptionName}");
    }

    [Fact]
    public async Task Provisioner_Should_Skip_When_AutoCreate_Disabled()
    {
        // Arrange
        var randomQueueName = $"test-skip-queue-{Guid.NewGuid():N}";
        var adminClient = new ServiceBusAdministrationClient(_fixture.ServiceBusConnectionString);
        var logger = new TestLogger<ServiceBusProvisioner>(_output);
        var provisioner = new ServiceBusProvisioner(adminClient, logger);
        
        var options = new ServiceBusStreamOptions
        {
            AutoCreateEntities = false, // Disabled
            EntityKind = EntityKind.Queue,
            QueueName = randomQueueName,
            EntityCount = 1
        };

        // Ensure queue doesn't exist initially
        var queueExists = await adminClient.QueueExistsAsync(randomQueueName);
        Assert.False(queueExists, "Queue should not exist initially");

        // Act
        await provisioner.ProvisionEntitiesAsync(options);

        // Assert - queue should still not exist
        queueExists = await adminClient.QueueExistsAsync(randomQueueName);
        Assert.False(queueExists, "Queue should not exist after provisioning when AutoCreateEntities is false");

        _output.WriteLine($"Correctly skipped creating queue when AutoCreateEntities=false: {randomQueueName}");
    }
}

/// <summary>
/// Test logger that writes to test output.
/// </summary>
internal class TestLogger<T> : ILogger<T>
{
    private readonly ITestOutputHelper _output;

    public TestLogger(ITestOutputHelper output)
    {
        _output = output;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        var message = formatter(state, exception);
        _output.WriteLine($"[{logLevel}] {message}");
        if (exception is not null)
        {
            _output.WriteLine($"Exception: {exception}");
        }
    }
}