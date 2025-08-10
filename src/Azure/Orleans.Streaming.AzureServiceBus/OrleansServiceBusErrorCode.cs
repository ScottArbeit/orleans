namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Error codes specific to Orleans Azure Service Bus streaming provider.
/// </summary>
internal static class OrleansServiceBusErrorCode
{
    /// <summary>
    /// Base error code for Service Bus streaming provider.
    /// </summary>
    private const int ServiceBusStreamProviderBase = 400000;

    /// <summary>
    /// Service Bus queue adapter error.
    /// </summary>
    public const int ServiceBusQueueAdapter = ServiceBusStreamProviderBase + 1;

    /// <summary>
    /// Service Bus receiver error.
    /// </summary>
    public const int ServiceBusReceiver = ServiceBusStreamProviderBase + 2;

    /// <summary>
    /// Service Bus sender error.
    /// </summary>
    public const int ServiceBusSender = ServiceBusStreamProviderBase + 3;

    /// <summary>
    /// Service Bus configuration error.
    /// </summary>
    public const int ServiceBusConfiguration = ServiceBusStreamProviderBase + 4;
}