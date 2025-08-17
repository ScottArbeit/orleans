using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;

namespace Orleans.Streaming.AzureServiceBus.Telemetry;

/// <summary>
/// Service Bus streaming instrument names for metrics.
/// </summary>
internal static class ServiceBusInstrumentNames
{
    // Azure Service Bus Streaming
    public const string SERVICEBUS_PUBLISH_BATCHES = "orleans-streams-servicebus-publish-batches";
    public const string SERVICEBUS_PUBLISH_MESSAGES = "orleans-streams-servicebus-publish-messages";
    public const string SERVICEBUS_PUBLISH_FAILURES = "orleans-streams-servicebus-publish-failures";
    public const string SERVICEBUS_RECEIVE_BATCHES = "orleans-streams-servicebus-receive-batches";
    public const string SERVICEBUS_RECEIVE_MESSAGES = "orleans-streams-servicebus-receive-messages";
    public const string SERVICEBUS_RECEIVE_FAILURES = "orleans-streams-servicebus-receive-failures";
    public const string SERVICEBUS_MESSAGES_COMPLETED = "orleans-streams-servicebus-messages-completed";
    public const string SERVICEBUS_MESSAGES_ABANDONED = "orleans-streams-servicebus-messages-abandoned";
    public const string SERVICEBUS_CACHE_SIZE = "orleans-streams-servicebus-cache-size";
    public const string SERVICEBUS_CACHE_PRESSURE_TRIGGERS = "orleans-streams-servicebus-cache-pressure-triggers";
    public const string SERVICEBUS_DLQ_SUSPECTED_COUNT = "orleans-streams-servicebus-dlq-suspected-count";
    public const string SERVICEBUS_HEALTH_CHECKS = "orleans-streams-servicebus-health-checks";
    public const string SERVICEBUS_HEALTH_CHECK_FAILURES = "orleans-streams-servicebus-health-check-failures";
}

/// <summary>
/// Provides metrics instrumentation for Azure Service Bus streaming operations.
/// Exposes counters for publish/receive operations, message completion status,
/// cache metrics, and health monitoring.
/// </summary>
internal static class ServiceBusStreamingMetrics
{
    private static readonly Meter Meter = new("Orleans.Streaming.AzureServiceBus");

    // Publisher metrics
    public static Counter<int> PublishBatches = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_PUBLISH_BATCHES);
    public static Counter<int> PublishMessages = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_PUBLISH_MESSAGES);
    public static Counter<int> PublishFailures = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_PUBLISH_FAILURES);

    // Receiver metrics
    public static Counter<int> ReceiveBatches = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_RECEIVE_BATCHES);
    public static Counter<int> ReceiveMessages = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_RECEIVE_MESSAGES);
    public static Counter<int> ReceiveFailures = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_RECEIVE_FAILURES);

    // Message completion metrics
    public static Counter<int> MessagesCompleted = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_MESSAGES_COMPLETED);
    public static Counter<int> MessagesAbandoned = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_MESSAGES_ABANDONED);

    // Cache metrics
    public static ObservableGauge<int> CacheSize = null!;
    public static Counter<int> CachePressureTriggers = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_CACHE_PRESSURE_TRIGGERS);

    // DLQ metrics
    public static Counter<int> DlqSuspectedCount = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_DLQ_SUSPECTED_COUNT);

    // Health check metrics
    public static Counter<int> HealthChecks = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_HEALTH_CHECKS);
    public static Counter<int> HealthCheckFailures = Meter.CreateCounter<int>(ServiceBusInstrumentNames.SERVICEBUS_HEALTH_CHECK_FAILURES);

    /// <summary>
    /// Registers an observable gauge for cache size monitoring.
    /// </summary>
    /// <param name="observeValue">Function to observe the current cache size.</param>
    public static void RegisterCacheSizeObserver(Func<Measurement<int>> observeValue)
    {
        CacheSize = Meter.CreateObservableGauge<int>(ServiceBusInstrumentNames.SERVICEBUS_CACHE_SIZE, observeValue);
    }

    /// <summary>
    /// Records a successful batch publish operation.
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <param name="messageCount">The number of messages in the batch.</param>
    public static void RecordPublishBatch(string providerName, string entityName, int messageCount)
    {
        var tags = CreateProviderTags(providerName, entityName);
        PublishBatches.Add(1, tags);
        PublishMessages.Add(messageCount, tags);
    }

    /// <summary>
    /// Records a failed batch publish operation.
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <param name="messageCount">The number of messages that failed to publish.</param>
    public static void RecordPublishFailure(string providerName, string entityName, int messageCount)
    {
        var tags = CreateProviderTags(providerName, entityName);
        PublishFailures.Add(1, tags);
        // Don't increment PublishMessages on failure - only successful publishes
    }

    /// <summary>
    /// Records a successful batch receive operation.
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <param name="queueId">The queue identifier.</param>
    /// <param name="messageCount">The number of messages received in the batch.</param>
    public static void RecordReceiveBatch(string providerName, string entityName, string queueId, int messageCount)
    {
        var tags = CreateQueueTags(providerName, entityName, queueId);
        ReceiveBatches.Add(1, tags);
        ReceiveMessages.Add(messageCount, tags);
    }

    /// <summary>
    /// Records a failed receive operation.
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <param name="queueId">The queue identifier.</param>
    public static void RecordReceiveFailure(string providerName, string entityName, string queueId)
    {
        var tags = CreateQueueTags(providerName, entityName, queueId);
        ReceiveFailures.Add(1, tags);
    }

    /// <summary>
    /// Records message completion (successful processing).
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <param name="queueId">The queue identifier.</param>
    /// <param name="messageCount">The number of messages completed.</param>
    public static void RecordMessagesCompleted(string providerName, string entityName, string queueId, int messageCount)
    {
        var tags = CreateQueueTags(providerName, entityName, queueId);
        MessagesCompleted.Add(messageCount, tags);
    }

    /// <summary>
    /// Records message abandonment (failed processing or explicit abandon).
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <param name="queueId">The queue identifier.</param>
    /// <param name="messageCount">The number of messages abandoned.</param>
    public static void RecordMessagesAbandoned(string providerName, string entityName, string queueId, int messageCount)
    {
        var tags = CreateQueueTags(providerName, entityName, queueId);
        MessagesAbandoned.Add(messageCount, tags);
    }

    /// <summary>
    /// Records cache pressure trigger events.
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="queueId">The queue identifier.</param>
    public static void RecordCachePressureTrigger(string providerName, string queueId)
    {
        var tags = new KeyValuePair<string, object?>[]
        {
            new("provider", providerName),
            new("queue_id", queueId)
        };
        CachePressureTriggers.Add(1, tags);
    }

    /// <summary>
    /// Records suspected DLQ messages (best-effort tracking).
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <param name="queueId">The queue identifier.</param>
    /// <param name="messageCount">The number of messages suspected to be sent to DLQ.</param>
    public static void RecordDlqSuspected(string providerName, string entityName, string queueId, int messageCount)
    {
        var tags = CreateQueueTags(providerName, entityName, queueId);
        DlqSuspectedCount.Add(messageCount, tags);
    }

    /// <summary>
    /// Records a health check execution.
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <param name="checkType">The type of health check (e.g., "sender", "receiver").</param>
    public static void RecordHealthCheck(string providerName, string entityName, string checkType)
    {
        var tags = new KeyValuePair<string, object?>[]
        {
            new("provider", providerName),
            new("entity", entityName),
            new("check_type", checkType)
        };
        HealthChecks.Add(1, tags);
    }

    /// <summary>
    /// Records a failed health check.
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <param name="checkType">The type of health check (e.g., "sender", "receiver").</param>
    public static void RecordHealthCheckFailure(string providerName, string entityName, string checkType)
    {
        var tags = new KeyValuePair<string, object?>[]
        {
            new("provider", providerName),
            new("entity", entityName),
            new("check_type", checkType)
        };
        HealthCheckFailures.Add(1, tags);
    }

    /// <summary>
    /// Creates standard tags for provider-level metrics.
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <returns>Tag array for metrics instrumentation.</returns>
    private static KeyValuePair<string, object?>[] CreateProviderTags(string providerName, string entityName)
    {
        return new KeyValuePair<string, object?>[]
        {
            new("provider", providerName),
            new("entity", entityName)
        };
    }

    /// <summary>
    /// Creates standard tags for queue-level metrics.
    /// </summary>
    /// <param name="providerName">The stream provider name.</param>
    /// <param name="entityName">The Service Bus entity name.</param>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>Tag array for metrics instrumentation.</returns>
    private static KeyValuePair<string, object?>[] CreateQueueTags(string providerName, string entityName, string queueId)
    {
        return new KeyValuePair<string, object?>[]
        {
            new("provider", providerName),
            new("entity", entityName),
            new("queue_id", queueId)
        };
    }
}