using System;

namespace Orleans.Configuration;

/// <summary>
/// Configuration options specific to Azure Service Bus Queue streaming.
/// </summary>
public class AzureServiceBusQueueOptions : AzureServiceBusOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AzureServiceBusQueueOptions"/> class.
    /// </summary>
    public AzureServiceBusQueueOptions()
    {
        EntityMode = ServiceBusEntityMode.Queue;
    }

    /// <summary>
    /// The time-to-live for messages sent to the queue. If not set, uses queue default.
    /// </summary>
    public TimeSpan? MessageTimeToLive { get; set; }

    /// <summary>
    /// Whether to enable batched operations for improved throughput. Default is true.
    /// </summary>
    public bool EnableBatchedOperations { get; set; } = true;

    /// <summary>
    /// Whether the queue requires session handling. Default is false.
    /// </summary>
    public bool RequiresSession { get; set; } = false;

    /// <summary>
    /// The session ID to use when sessions are required. Only applicable when RequiresSession is true.
    /// </summary>
    public string SessionId { get; set; } = string.Empty;

    /// <summary>
    /// Whether to enable duplicate detection. Default is false.
    /// </summary>
    public bool RequiresDuplicateDetection { get; set; } = false;

    /// <summary>
    /// The duration of the duplicate detection history window. Only applicable when RequiresDuplicateDetection is true.
    /// </summary>
    public TimeSpan DuplicateDetectionHistoryTimeWindow { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// The maximum delivery count before a message is moved to the dead letter queue. Default is 10.
    /// </summary>
    public int MaxDeliveryCount { get; set; } = 10;
}