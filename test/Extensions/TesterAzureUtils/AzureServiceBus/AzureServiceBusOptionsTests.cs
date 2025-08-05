using System;
using Orleans.Configuration;
using Orleans.Runtime;
using Xunit;

namespace Tester.AzureUtils.AzureServiceBus;

/// <summary>
/// Tests for Azure Service Bus configuration options and validation.
/// </summary>
public class AzureServiceBusOptionsTests
{
    [Fact]
    public void AzureServiceBusOptions_DefaultValues_AreCorrect()
    {
        var options = new AzureServiceBusOptions();

        Assert.Equal(string.Empty, options.ConnectionString);
        Assert.Equal(string.Empty, options.EntityName);
        Assert.Equal(string.Empty, options.SubscriptionName);
        Assert.Equal(ServiceBusEntityMode.Queue, options.EntityMode);
        Assert.Equal(32, options.BatchSize);
        Assert.Equal(0, options.PrefetchCount);
        Assert.Equal(TimeSpan.FromSeconds(60), options.OperationTimeout);
        Assert.Equal(1, options.MaxConcurrentCalls);
        Assert.Equal(TimeSpan.FromMinutes(5), options.MaxAutoLockRenewalDuration);
        Assert.True(options.AutoCompleteMessages);
        Assert.Equal("PeekLock", options.ReceiveMode);
    }

    [Fact]
    public void AzureServiceBusQueueOptions_DefaultValues_AreCorrect()
    {
        var options = new AzureServiceBusQueueOptions();

        // Base class defaults
        Assert.Equal(ServiceBusEntityMode.Queue, options.EntityMode);
        Assert.Equal(32, options.BatchSize);

        // Queue-specific defaults
        Assert.Null(options.MessageTimeToLive);
        Assert.True(options.EnableBatchedOperations);
        Assert.False(options.RequiresSession);
        Assert.Equal(string.Empty, options.SessionId);
        Assert.False(options.RequiresDuplicateDetection);
        Assert.Equal(TimeSpan.FromMinutes(10), options.DuplicateDetectionHistoryTimeWindow);
        Assert.Equal(10, options.MaxDeliveryCount);
    }

    [Fact]
    public void AzureServiceBusTopicOptions_DefaultValues_AreCorrect()
    {
        var options = new AzureServiceBusTopicOptions();

        // Base class defaults
        Assert.Equal(ServiceBusEntityMode.Topic, options.EntityMode);
        Assert.Equal(32, options.BatchSize);

        // Topic-specific defaults
        Assert.False(options.EnableSubscriptionRuleEvaluation);
        Assert.Equal(string.Empty, options.ForwardTo);
        Assert.Equal(string.Empty, options.ForwardDeadLetteredMessagesTo);
        Assert.True(options.EnableBatchedOperations);
        Assert.False(options.RequiresSession);
        Assert.Equal(string.Empty, options.SessionId);
        Assert.False(options.RequiresDuplicateDetection);
        Assert.Equal(TimeSpan.FromMinutes(10), options.DuplicateDetectionHistoryTimeWindow);
        Assert.Equal(10, options.MaxDeliveryCount);
        Assert.Null(options.AutoDeleteOnIdle);
        Assert.Null(options.DefaultMessageTimeToLive);
        Assert.Equal(string.Empty, options.SubscriptionFilter);
    }

    [Theory]
    [InlineData(ServiceBusEntityMode.Queue)]
    [InlineData(ServiceBusEntityMode.Topic)]
    public void ServiceBusEntityMode_ValidValues_AreAccepted(ServiceBusEntityMode mode)
    {
        var options = new AzureServiceBusOptions { EntityMode = mode };

        Assert.Equal(mode, options.EntityMode);
    }

    [Fact]
    public void AzureServiceBusOptions_PropertyAssignment_WorksCorrectly()
    {
        var connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=key";
        var entityName = "testqueue";
        var subscriptionName = "testsubscription";
        var batchSize = 64;
        var prefetchCount = 100;
        var operationTimeout = TimeSpan.FromSeconds(120);
        var maxConcurrentCalls = 5;
        var maxAutoLockRenewalDuration = TimeSpan.FromMinutes(10);

        var options = new AzureServiceBusOptions
        {
            ConnectionString = connectionString,
            EntityName = entityName,
            SubscriptionName = subscriptionName,
            EntityMode = ServiceBusEntityMode.Topic,
            BatchSize = batchSize,
            PrefetchCount = prefetchCount,
            OperationTimeout = operationTimeout,
            MaxConcurrentCalls = maxConcurrentCalls,
            MaxAutoLockRenewalDuration = maxAutoLockRenewalDuration,
            AutoCompleteMessages = false,
            ReceiveMode = "ReceiveAndDelete"
        };

        Assert.Equal(connectionString, options.ConnectionString);
        Assert.Equal(entityName, options.EntityName);
        Assert.Equal(subscriptionName, options.SubscriptionName);
        Assert.Equal(ServiceBusEntityMode.Topic, options.EntityMode);
        Assert.Equal(batchSize, options.BatchSize);
        Assert.Equal(prefetchCount, options.PrefetchCount);
        Assert.Equal(operationTimeout, options.OperationTimeout);
        Assert.Equal(maxConcurrentCalls, options.MaxConcurrentCalls);
        Assert.Equal(maxAutoLockRenewalDuration, options.MaxAutoLockRenewalDuration);
        Assert.False(options.AutoCompleteMessages);
        Assert.Equal("ReceiveAndDelete", options.ReceiveMode);
    }

    [Fact]
    public void AzureServiceBusQueueOptions_PropertyAssignment_WorksCorrectly()
    {
        var messageTimeToLive = TimeSpan.FromHours(24);
        var sessionId = "session123";
        var duplicateDetectionWindow = TimeSpan.FromMinutes(30);
        var maxDeliveryCount = 5;

        var options = new AzureServiceBusQueueOptions
        {
            MessageTimeToLive = messageTimeToLive,
            EnableBatchedOperations = false,
            RequiresSession = true,
            SessionId = sessionId,
            RequiresDuplicateDetection = true,
            DuplicateDetectionHistoryTimeWindow = duplicateDetectionWindow,
            MaxDeliveryCount = maxDeliveryCount
        };

        Assert.Equal(messageTimeToLive, options.MessageTimeToLive);
        Assert.False(options.EnableBatchedOperations);
        Assert.True(options.RequiresSession);
        Assert.Equal(sessionId, options.SessionId);
        Assert.True(options.RequiresDuplicateDetection);
        Assert.Equal(duplicateDetectionWindow, options.DuplicateDetectionHistoryTimeWindow);
        Assert.Equal(maxDeliveryCount, options.MaxDeliveryCount);
    }

    [Fact]
    public void AzureServiceBusTopicOptions_PropertyAssignment_WorksCorrectly()
    {
        var forwardTo = "deadletterqueue";
        var forwardDeadLetteredTo = "errorqueue";
        var sessionId = "session123";
        var autoDeleteOnIdle = TimeSpan.FromHours(48);
        var defaultMessageTimeToLive = TimeSpan.FromDays(7);
        var subscriptionFilter = "MessageType = 'Order'";

        var options = new AzureServiceBusTopicOptions
        {
            EnableSubscriptionRuleEvaluation = true,
            ForwardTo = forwardTo,
            ForwardDeadLetteredMessagesTo = forwardDeadLetteredTo,
            EnableBatchedOperations = false,
            RequiresSession = true,
            SessionId = sessionId,
            RequiresDuplicateDetection = true,
            AutoDeleteOnIdle = autoDeleteOnIdle,
            DefaultMessageTimeToLive = defaultMessageTimeToLive,
            SubscriptionFilter = subscriptionFilter
        };

        Assert.True(options.EnableSubscriptionRuleEvaluation);
        Assert.Equal(forwardTo, options.ForwardTo);
        Assert.Equal(forwardDeadLetteredTo, options.ForwardDeadLetteredMessagesTo);
        Assert.False(options.EnableBatchedOperations);
        Assert.True(options.RequiresSession);
        Assert.Equal(sessionId, options.SessionId);
        Assert.True(options.RequiresDuplicateDetection);
        Assert.Equal(autoDeleteOnIdle, options.AutoDeleteOnIdle);
        Assert.Equal(defaultMessageTimeToLive, options.DefaultMessageTimeToLive);
        Assert.Equal(subscriptionFilter, options.SubscriptionFilter);
    }
}