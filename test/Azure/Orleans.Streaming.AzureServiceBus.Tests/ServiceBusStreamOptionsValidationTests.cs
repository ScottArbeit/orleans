namespace Orleans.Streaming.AzureServiceBus.Tests;

using System;
using Microsoft.Extensions.Options;
using Orleans.Streaming.AzureServiceBus;
using Xunit;

/// <summary>
/// Tests for ServiceBusStreamOptions validation.
/// </summary>
public class ServiceBusStreamOptionsValidationTests
{
    private readonly ServiceBusStreamOptionsValidator _validator = new();

    [Fact]
    public void ValidateOptions_ValidConnectionString_ReturnsSuccess()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateOptions_ValidFullyQualifiedNamespace_ReturnsSuccess()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            FullyQualifiedNamespace = "test.servicebus.windows.net",
            Credential = new TestTokenCredential(), // Mock credential
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateOptions_ValidTopicSubscription_ReturnsSuccess()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            EntityKind = EntityKind.TopicSubscription,
            TopicName = "test-topic",
            SubscriptionName = "test-subscription"
        };

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateOptions_NoConnectionInfo_ReturnsFailure()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Contains("Either ConnectionString or FullyQualifiedNamespace must be provided", result.FailureMessage);
    }

    [Fact]
    public void ValidateOptions_BothConnectionStringAndNamespace_ReturnsFailure()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            FullyQualifiedNamespace = "test.servicebus.windows.net",
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Contains("Only one of ConnectionString or FullyQualifiedNamespace should be provided", result.FailureMessage);
    }

    [Fact]
    public void ValidateOptions_NamespaceWithoutCredential_ReturnsFailure()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            FullyQualifiedNamespace = "test.servicebus.windows.net",
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Contains("Credential must be provided when using FullyQualifiedNamespace", result.FailureMessage);
    }

    [Fact]
    public void ValidateOptions_QueueWithoutQueueName_ReturnsFailure()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            EntityKind = EntityKind.Queue,
            QueueName = ""
        };

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Contains("QueueName must be provided when EntityKind is Queue", result.FailureMessage);
    }

    [Fact]
    public void ValidateOptions_TopicWithoutTopicName_ReturnsFailure()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            EntityKind = EntityKind.TopicSubscription,
            TopicName = "",
            SubscriptionName = "test-subscription"
        };

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Contains("TopicName must be provided when EntityKind is TopicSubscription", result.FailureMessage);
    }

    [Fact]
    public void ValidateOptions_TopicWithoutSubscriptionName_ReturnsFailure()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            EntityKind = EntityKind.TopicSubscription,
            TopicName = "test-topic",
            SubscriptionName = ""
        };

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Contains("SubscriptionName must be provided when EntityKind is TopicSubscription", result.FailureMessage);
    }

    [Fact]
    public void ValidateOptions_InvalidBatchSize_ReturnsFailure()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };
        options.Publisher.BatchSize = 0;

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Contains("Publisher.BatchSize must be greater than zero", result.FailureMessage);
    }

    [Fact]
    public void ValidateOptions_MaxConcurrentHandlersGreaterThanOne_ReturnsSuccess()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };
        options.Receiver.MaxConcurrentHandlers = 2;

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateOptions_InvalidCachePressure_ReturnsFailure()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };
        options.Cache.CachePressureSoft = 0.9;
        options.Cache.CachePressureHard = 0.8;

        // Act
        var result = _validator.Validate("test", options);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Contains("Cache.CachePressureSoft must be less than Cache.CachePressureHard", result.FailureMessage);
    }

    [Fact]
    public void ValidateOptions_AllowRewindIsAlwaysFalse()
    {
        // Arrange
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            EntityKind = EntityKind.Queue,
            QueueName = "test-queue"
        };

        // Act & Assert
        Assert.False(options.AllowRewind);
    }
}

/// <summary>
/// Mock token credential for testing.
/// </summary>
internal class TestTokenCredential : Azure.Core.TokenCredential
{
    public override Azure.Core.AccessToken GetToken(Azure.Core.TokenRequestContext requestContext, System.Threading.CancellationToken cancellationToken)
    {
        return new Azure.Core.AccessToken("test-token", DateTimeOffset.UtcNow.AddHours(1));
    }

    public override System.Threading.Tasks.ValueTask<Azure.Core.AccessToken> GetTokenAsync(Azure.Core.TokenRequestContext requestContext, System.Threading.CancellationToken cancellationToken)
    {
        return new System.Threading.Tasks.ValueTask<Azure.Core.AccessToken>(GetToken(requestContext, cancellationToken));
    }
}
