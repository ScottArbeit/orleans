using System;
using Orleans.Streaming;

namespace Orleans.Configuration;

/// <summary>
/// Configuration options for Azure Service Bus streaming provider.
/// </summary>
public class ServiceBusOptions
{
    /// <summary>
    /// Gets or sets the Azure Service Bus connection string.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Gets or sets the number of queues/topics to use for partitioning.
    /// </summary>
    public int PartitionCount { get; set; } = 8;

    /// <summary>
    /// Gets or sets the maximum time to wait for messages when receiving.
    /// </summary>
    public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets whether to use topics instead of queues.
    /// </summary>
    public bool UseTopics { get; set; } = false;

    /// <summary>
    /// Gets or sets the prefix for queue/topic names.
    /// </summary>
    public string QueueNamePrefix { get; set; } = "orleans-stream";
}