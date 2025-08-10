using System;
using Azure.Core;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Configuration options for Azure Service Bus streaming provider.
/// </summary>
public class ServiceBusStreamOptions
{
    /// <summary>
    /// The Service Bus connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// The fully qualified namespace for Service Bus connection (alternative to connection string).
    /// </summary>
    public string FullyQualifiedNamespace { get; set; } = string.Empty;

    /// <summary>
    /// The credential to use for authentication (when using FullyQualifiedNamespace).
    /// </summary>
    public TokenCredential? Credential { get; set; }

    /// <summary>
    /// The type of Service Bus entity to use.
    /// </summary>
    public EntityKind EntityKind { get; set; } = EntityKind.Queue;

    /// <summary>
    /// The queue name to use when EntityKind is Queue.
    /// </summary>
    public string QueueName { get; set; } = "orleans-stream";

    /// <summary>
    /// The topic name to use when EntityKind is TopicSubscription.
    /// </summary>
    public string TopicName { get; set; } = string.Empty;

    /// <summary>
    /// The subscription name to use when EntityKind is TopicSubscription.
    /// </summary>
    public string SubscriptionName { get; set; } = string.Empty;

    /// <summary>
    /// Publisher settings for the streaming provider.
    /// </summary>
    public PublisherSettings Publisher { get; set; } = new();

    /// <summary>
    /// Receiver settings for the streaming provider.
    /// </summary>
    public ReceiverSettings Receiver { get; set; } = new();

    /// <summary>
    /// Cache settings for the streaming provider.
    /// </summary>
    public CacheSettings Cache { get; set; } = new();

    /// <summary>
    /// Whether to automatically create Service Bus entities if they don't exist.
    /// </summary>
    public bool AutoCreateEntities { get; set; } = true;

    /// <summary>
    /// The number of entities to create for partitioning (for Step 10).
    /// </summary>
    public int EntityCount { get; set; } = 1;

    /// <summary>
    /// The prefix to use for entity names when creating multiple entities.
    /// </summary>
    public string EntityNamePrefix { get; set; } = string.Empty;

    /// <summary>
    /// Dead letter handling configuration.
    /// </summary>
    public DeadLetterHandling DeadLetterHandling { get; set; } = new();

    /// <summary>
    /// Whether rewinding is allowed. Always false for Service Bus provider.
    /// </summary>
    public bool AllowRewind { get; } = false;
}

/// <summary>
/// The type of Service Bus entity to use for streaming.
/// </summary>
public enum EntityKind
{
    /// <summary>
    /// Use Service Bus queues for streaming.
    /// </summary>
    Queue,

    /// <summary>
    /// Use Service Bus topics and subscriptions for streaming.
    /// </summary>
    TopicSubscription
}

/// <summary>
/// Publisher settings for Service Bus streaming.
/// </summary>
public class PublisherSettings
{
    /// <summary>
    /// The batch size for publishing messages.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// The time-to-live for messages.
    /// </summary>
    public TimeSpan MessageTimeToLive { get; set; } = TimeSpan.FromDays(14);

    /// <summary>
    /// The session ID strategy to use for messages.
    /// </summary>
    public SessionIdStrategy SessionIdStrategy { get; set; } = SessionIdStrategy.None;

    /// <summary>
    /// The prefix to use for custom properties on messages.
    /// </summary>
    public string PropertiesPrefix { get; set; } = "orleans_";
}

/// <summary>
/// Session ID strategy for Service Bus messages.
/// </summary>
public enum SessionIdStrategy
{
    /// <summary>
    /// Do not use session IDs.
    /// </summary>
    None,

    /// <summary>
    /// Use the stream ID as the session ID.
    /// </summary>
    UseStreamId
}

/// <summary>
/// Receiver settings for Service Bus streaming.
/// </summary>
public class ReceiverSettings
{
    /// <summary>
    /// The number of messages to prefetch.
    /// </summary>
    public int PrefetchCount { get; set; } = 0;

    /// <summary>
    /// The batch size for receiving messages.
    /// </summary>
    public int ReceiveBatchSize { get; set; } = 32;

    /// <summary>
    /// The maximum number of concurrent message handlers (capped to 1 in MVP to preserve order).
    /// </summary>
    public int MaxConcurrentHandlers { get; set; } = 1;

    /// <summary>
    /// Whether to automatically renew message locks.
    /// </summary>
    public bool LockAutoRenew { get; set; } = true;

    /// <summary>
    /// The duration for automatic lock renewal.
    /// </summary>
    public TimeSpan LockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The maximum delivery count for messages (documentation only; actual is entity-level).
    /// </summary>
    public int MaxDeliveryCount { get; set; } = 10;
}

/// <summary>
/// Cache settings for Service Bus streaming.
/// </summary>
public class CacheSettings
{
    /// <summary>
    /// The maximum cache size.
    /// </summary>
    public int MaxCacheSize { get; set; } = 4096;

    /// <summary>
    /// The cache eviction age.
    /// </summary>
    public TimeSpan CacheEvictionAge { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// The soft pressure threshold for cache eviction.
    /// </summary>
    public double CachePressureSoft { get; set; } = 0.7;

    /// <summary>
    /// The hard pressure threshold for cache eviction.
    /// </summary>
    public double CachePressureHard { get; set; } = 0.9;
}

/// <summary>
/// Dead letter handling configuration.
/// </summary>
public class DeadLetterHandling
{
    /// <summary>
    /// Whether to log dead letter events.
    /// </summary>
    public bool LogDeadLetters { get; set; } = true;

    /// <summary>
    /// Whether to surface metrics for dead letter events.
    /// </summary>
    public bool SurfaceMetrics { get; set; } = true;

    /// <summary>
    /// Optional forward address for dead letter messages (documented for real Service Bus only; emulator may no-op).
    /// </summary>
    public string ForwardAddress { get; set; } = string.Empty;
}