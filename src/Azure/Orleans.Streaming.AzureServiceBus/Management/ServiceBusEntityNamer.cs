using System;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Provides canonical entity naming for Azure Service Bus streaming entities.
/// </summary>
public static class ServiceBusEntityNamer
{
    /// <summary>
    /// Gets the canonical entity name based on the configured entity kind and options.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <returns>The canonical entity name for the configured entity kind.</returns>
    /// <exception cref="ArgumentNullException">Thrown when options is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when entity configuration is invalid.</exception>
    public static string GetEntityName(ServiceBusStreamOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return options.EntityKind switch
        {
            EntityKind.Queue => GetQueueName(options),
            EntityKind.TopicSubscription => GetTopicSubscriptionName(options),
            _ => throw new InvalidOperationException($"Unsupported entity kind: {options.EntityKind}")
        };
    }

    /// <summary>
    /// Gets the queue name for Queue entity kind.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <returns>The canonical queue name.</returns>
    /// <exception cref="InvalidOperationException">Thrown when queue name is not configured.</exception>
    private static string GetQueueName(ServiceBusStreamOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.QueueName))
        {
            throw new InvalidOperationException("QueueName must be configured when EntityKind is Queue.");
        }

        return options.QueueName;
    }

    /// <summary>
    /// Gets the topic/subscription identifier for TopicSubscription entity kind.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <returns>The canonical topic/subscription identifier in the format "topic:subscription".</returns>
    /// <exception cref="InvalidOperationException">Thrown when topic or subscription name is not configured.</exception>
    private static string GetTopicSubscriptionName(ServiceBusStreamOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.TopicName))
        {
            throw new InvalidOperationException("TopicName must be configured when EntityKind is TopicSubscription.");
        }

        if (string.IsNullOrWhiteSpace(options.SubscriptionName))
        {
            throw new InvalidOperationException("SubscriptionName must be configured when EntityKind is TopicSubscription.");
        }

        return $"{options.TopicName}:{options.SubscriptionName}";
    }
}