using System;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Provides canonical entity naming for Azure Service Bus streaming entities.
/// </summary>
public static class ServiceBusEntityNamer
{
    /// <summary>
    /// Gets the canonical entity name based on the configured entity kind and options.
    /// For single entity (EntityCount = 1), returns the base entity name.
    /// For multiple entities, this method is for backward compatibility and returns the first entity name.
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
            EntityKind.Queue => GetQueueName(options, 0),
            EntityKind.TopicSubscription => GetTopicSubscriptionName(options, 0),
            _ => throw new InvalidOperationException($"Unsupported entity kind: {options.EntityKind}")
        };
    }

    /// <summary>
    /// Gets the entity name for a specific index when using multiple entities.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <param name="entityIndex">The zero-based index of the entity (0 to EntityCount-1).</param>
    /// <returns>The entity name for the specified index.</returns>
    /// <exception cref="ArgumentNullException">Thrown when options is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when entityIndex is invalid.</exception>
    /// <exception cref="InvalidOperationException">Thrown when entity configuration is invalid.</exception>
    public static string GetEntityName(ServiceBusStreamOptions options, int entityIndex)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (entityIndex < 0 || entityIndex >= options.EntityCount)
        {
            throw new ArgumentOutOfRangeException(nameof(entityIndex), 
                $"Entity index {entityIndex} is out of range. Valid range is 0 to {options.EntityCount - 1}.");
        }

        return options.EntityKind switch
        {
            EntityKind.Queue => GetQueueName(options, entityIndex),
            EntityKind.TopicSubscription => GetTopicSubscriptionName(options, entityIndex),
            _ => throw new InvalidOperationException($"Unsupported entity kind: {options.EntityKind}")
        };
    }

    /// <summary>
    /// Gets the queue name for Queue entity kind at the specified index.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <param name="entityIndex">The zero-based index of the entity.</param>
    /// <returns>The canonical queue name for the specified index.</returns>
    /// <exception cref="InvalidOperationException">Thrown when queue name is not configured properly.</exception>
    private static string GetQueueName(ServiceBusStreamOptions options, int entityIndex)
    {
        if (options.EntityCount == 1)
        {
            // Single entity: use the configured QueueName directly
            if (string.IsNullOrWhiteSpace(options.QueueName))
            {
                throw new InvalidOperationException("QueueName must be configured when EntityKind is Queue.");
            }
            return options.QueueName;
        }
        else
        {
            // Multiple entities: use EntityNamePrefix with pattern <prefix>-q-0..N-1
            if (string.IsNullOrWhiteSpace(options.EntityNamePrefix))
            {
                throw new InvalidOperationException("EntityNamePrefix must be configured when EntityCount > 1.");
            }
            return $"{options.EntityNamePrefix}-q-{entityIndex}";
        }
    }

    /// <summary>
    /// Gets the topic/subscription identifier for TopicSubscription entity kind at the specified index.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <param name="entityIndex">The zero-based index of the entity.</param>
    /// <returns>The canonical topic/subscription identifier in the format "topic:subscription" for the specified index.</returns>
    /// <exception cref="InvalidOperationException">Thrown when topic or subscription name is not configured properly.</exception>
    private static string GetTopicSubscriptionName(ServiceBusStreamOptions options, int entityIndex)
    {
        if (options.EntityCount == 1)
        {
            // Single entity: use the configured TopicName and SubscriptionName directly
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
        else
        {
            // Multiple entities: one topic; N subscriptions <prefix>-sub-0..N-1
            if (string.IsNullOrWhiteSpace(options.EntityNamePrefix))
            {
                throw new InvalidOperationException("EntityNamePrefix must be configured when EntityCount > 1.");
            }

            if (string.IsNullOrWhiteSpace(options.TopicName))
            {
                throw new InvalidOperationException("TopicName must be configured when EntityKind is TopicSubscription.");
            }

            return $"{options.TopicName}:{options.EntityNamePrefix}-sub-{entityIndex}";
        }
    }
}