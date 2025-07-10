using System;
using System.Collections.Generic;

namespace Orleans.Providers.Streams.ServiceBus;

/// <summary>
/// Utility class for Azure Service Bus stream provider.
/// </summary>
public static class ServiceBusStreamProviderUtils
{
    /// <summary>
    /// Generate default Azure Service Bus queue names for Orleans streaming.
    /// </summary>
    /// <param name="serviceId">The service ID for unique naming.</param>
    /// <param name="providerName">The stream provider name.</param>
    /// <returns>List of queue names.</returns>
    public static List<string> GenerateDefaultServiceBusQueueNames(string serviceId, string providerName)
    {
        return GenerateDefaultServiceBusQueueNames($"{serviceId}-{providerName}", 8);
    }

    /// <summary>
    /// Generate default Azure Service Bus queue names with specified count.
    /// </summary>
    /// <param name="queueNamePrefix">The prefix for queue names.</param>
    /// <param name="queueCount">The number of queues to create.</param>
    /// <returns>List of queue names.</returns>
    public static List<string> GenerateDefaultServiceBusQueueNames(string queueNamePrefix, int queueCount)
    {
        var queueNames = new List<string>();
        for (int i = 0; i < queueCount; i++)
        {
            queueNames.Add($"{queueNamePrefix}-{i:00}");
        }
        return queueNames;
    }

    /// <summary>
    /// Generate default Azure Service Bus subscription names for Orleans streaming.
    /// </summary>
    /// <param name="serviceId">The service ID for unique naming.</param>
    /// <param name="providerName">The stream provider name.</param>
    /// <returns>List of subscription names.</returns>
    public static List<string> GenerateDefaultServiceBusSubscriptionNames(string serviceId, string providerName)
    {
        return GenerateDefaultServiceBusSubscriptionNames($"{serviceId}-{providerName}", 8);
    }

    /// <summary>
    /// Generate default Azure Service Bus subscription names with specified count.
    /// </summary>
    /// <param name="subscriptionNamePrefix">The prefix for subscription names.</param>
    /// <param name="subscriptionCount">The number of subscriptions to create.</param>
    /// <returns>List of subscription names.</returns>
    public static List<string> GenerateDefaultServiceBusSubscriptionNames(string subscriptionNamePrefix, int subscriptionCount)
    {
        var subscriptionNames = new List<string>();
        for (int i = 0; i < subscriptionCount; i++)
        {
            subscriptionNames.Add($"{subscriptionNamePrefix}-{i:00}");
        }
        return subscriptionNames;
    }
}