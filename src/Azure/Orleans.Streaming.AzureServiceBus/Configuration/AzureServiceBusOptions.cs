using System;

namespace Orleans.Configuration;

/// <summary>
/// Base configuration options for Azure Service Bus streaming providers.
/// </summary>
public class AzureServiceBusOptions
{
    /// <summary>
    /// The Azure Service Bus connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// The name of the Azure Service Bus entity (queue or topic).
    /// </summary>
    public string EntityName { get; set; } = string.Empty;

    /// <summary>
    /// The subscription name when using topics. Required for Topic mode.
    /// </summary>
    public string SubscriptionName { get; set; } = string.Empty;

    /// <summary>
    /// The Azure Service Bus entity mode (Queue or Topic).
    /// </summary>
    public ServiceBusEntityMode EntityMode { get; set; } = ServiceBusEntityMode.Queue;

    /// <summary>
    /// The maximum number of messages to retrieve in a batch. Default is 32.
    /// </summary>
    public int BatchSize { get; set; } = 32;

    /// <summary>
    /// The number of messages to prefetch. Default is 0 (no prefetch).
    /// </summary>
    public int PrefetchCount { get; set; } = 0;

    /// <summary>
    /// The operation timeout for Service Bus operations. Default is 60 seconds.
    /// </summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// The maximum number of concurrent calls to process messages. Default is 1.
    /// </summary>
    public int MaxConcurrentCalls { get; set; } = 1;

    /// <summary>
    /// The maximum duration to wait for a lock renewal before abandoning the message. Default is 5 minutes.
    /// </summary>
    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Whether to enable auto-completion of messages after successful processing. Default is true.
    /// </summary>
    public bool AutoCompleteMessages { get; set; } = true;

    /// <summary>
    /// The receive mode for messages. Default is PeekLock.
    /// </summary>
    public string ReceiveMode { get; set; } = "PeekLock";
}