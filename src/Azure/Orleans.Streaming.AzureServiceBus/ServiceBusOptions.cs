using System;
using Microsoft.Extensions.Options;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Configuration options for Azure Service Bus streaming provider.
/// </summary>
public class ServiceBusOptions
{
    /// <summary>
    /// The Service Bus connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// The queue name to use for streaming.
    /// </summary>
    public string QueueName { get; set; } = "orleans-stream";

    /// <summary>
    /// The receive mode for messages. Default is PeekLock.
    /// </summary>
    public ReceiveMode ReceiveMode { get; set; } = ReceiveMode.PeekLock;

    /// <summary>
    /// Maximum number of concurrent calls to the message handler.
    /// </summary>
    public int MaxConcurrentCalls { get; set; } = 32;

    /// <summary>
    /// Maximum time to wait for a message to be received.
    /// </summary>
    public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(60);
}

/// <summary>
/// Receive mode for Service Bus messages.
/// </summary>
public enum ReceiveMode
{
    /// <summary>
    /// Peek and lock mode (messages must be explicitly completed).
    /// </summary>
    PeekLock,

    /// <summary>
    /// Receive and delete mode (messages are automatically completed).
    /// </summary>
    ReceiveAndDelete
}