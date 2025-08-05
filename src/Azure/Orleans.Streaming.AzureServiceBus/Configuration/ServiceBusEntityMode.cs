namespace Orleans.Configuration;

/// <summary>
/// Specifies the Azure Service Bus entity mode for streaming operations.
/// </summary>
public enum ServiceBusEntityMode
{
    /// <summary>
    /// Azure Service Bus Queue mode for point-to-point messaging.
    /// </summary>
    Queue,

    /// <summary>
    /// Azure Service Bus Topic/Subscription mode for publish-subscribe messaging.
    /// </summary>
    Topic
}