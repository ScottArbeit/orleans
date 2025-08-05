using System;

namespace Orleans.Configuration;

/// <summary>
/// Configuration options specific to Azure Service Bus Topic/Subscription streaming.
/// </summary>
public class AzureServiceBusTopicOptions : AzureServiceBusOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AzureServiceBusTopicOptions"/> class.
    /// </summary>
    public AzureServiceBusTopicOptions()
    {
        EntityMode = ServiceBusEntityMode.Topic;
    }

    /// <summary>
    /// Whether to enable subscription rule evaluation on the server. Default is false.
    /// </summary>
    public bool EnableSubscriptionRuleEvaluation { get; set; } = false;

    /// <summary>
    /// The name of the queue or topic to forward messages to. If set, messages will be forwarded after processing.
    /// </summary>
    public string ForwardTo { get; set; } = string.Empty;

    /// <summary>
    /// The name of the queue or topic to forward dead letter messages to.
    /// </summary>
    public string ForwardDeadLetteredMessagesTo { get; set; } = string.Empty;

    /// <summary>
    /// Whether to enable batched operations for improved throughput. Default is true.
    /// </summary>
    public bool EnableBatchedOperations { get; set; } = true;

    /// <summary>
    /// Whether the subscription requires session handling. Default is false.
    /// </summary>
    public bool RequiresSession { get; set; } = false;

    /// <summary>
    /// The session ID to use when sessions are required. Only applicable when RequiresSession is true.
    /// </summary>
    public string SessionId { get; set; } = string.Empty;

    /// <summary>
    /// Whether to enable duplicate detection on the subscription. Default is false.
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

    /// <summary>
    /// The auto-delete timeout for the subscription. If set, the subscription will be automatically deleted after this period of inactivity.
    /// </summary>
    public TimeSpan? AutoDeleteOnIdle { get; set; }

    /// <summary>
    /// The default message time-to-live for the subscription. If not set, uses topic default.
    /// </summary>
    public TimeSpan? DefaultMessageTimeToLive { get; set; }

    /// <summary>
    /// The subscription filter SQL expression. Only messages matching this filter will be delivered.
    /// </summary>
    public string SubscriptionFilter { get; set; } = string.Empty;
}