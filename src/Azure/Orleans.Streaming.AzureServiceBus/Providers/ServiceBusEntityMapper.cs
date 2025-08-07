using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streams;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Providers;

/// <summary>
/// Helper class for mapping between Orleans stream identifiers and Azure Service Bus entity information.
/// Provides utilities for entity name generation, partition selection, and queue ID construction.
/// </summary>
public static class ServiceBusEntityMapper
{
    /// <summary>
    /// Maximum length for Azure Service Bus entity names.
    /// </summary>
    public const int MaxEntityNameLength = 260;

    /// <summary>
    /// Creates a QueueId for a Service Bus queue.
    /// </summary>
    /// <param name="queueName">The queue name.</param>
    /// <param name="providerName">The provider name.</param>
    /// <param name="partition">Optional partition number for partitioned queues.</param>
    /// <returns>A QueueId representing the queue.</returns>
    public static QueueId ForQueue(string queueName, string providerName, int partition = 0)
    {
        ArgumentNullException.ThrowIfNull(queueName);
        ArgumentNullException.ThrowIfNull(providerName);
        
        var entityName = partition > 0 ? $"{queueName}-{partition}" : queueName;
        ValidateEntityName(entityName);
        
        return QueueId.GetQueueId($"{providerName}-{entityName}", (uint)partition, (uint)queueName.GetHashCode());
    }

    /// <summary>
    /// Creates a QueueId for a Service Bus topic/subscription combination.
    /// </summary>
    /// <param name="topicName">The topic name.</param>
    /// <param name="subscriptionName">The subscription name.</param>
    /// <param name="providerName">The provider name.</param>
    /// <param name="partition">Optional partition number for partitioned topics.</param>
    /// <returns>A QueueId representing the topic/subscription.</returns>
    public static QueueId ForTopicSubscription(string topicName, string subscriptionName, string providerName, int partition = 0)
    {
        ArgumentNullException.ThrowIfNull(topicName);
        ArgumentNullException.ThrowIfNull(subscriptionName);
        ArgumentNullException.ThrowIfNull(providerName);
        
        var entityName = partition > 0 ? $"{topicName}-{partition}" : topicName;
        ValidateEntityName(entityName);
        ValidateEntityName(subscriptionName);
        
        var queueName = $"{topicName}/{subscriptionName}";
        if (partition > 0)
        {
            queueName = $"{topicName}-{partition}/{subscriptionName}";
        }
        
        return QueueId.GetQueueId($"{providerName}-{queueName}", (uint)partition, (uint)queueName.GetHashCode());
    }

    /// <summary>
    /// Extracts the entity name from a QueueId.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <param name="providerName">The provider name to remove from the prefix.</param>
    /// <returns>The Service Bus entity name.</returns>
    public static string GetEntityNameFromQueueId(QueueId queueId, string providerName)
    {
        ArgumentNullException.ThrowIfNull(providerName);
        
        var queueNamePrefix = queueId.GetStringNamePrefix();
        var expectedPrefix = $"{providerName}-";
        
        if (queueNamePrefix.StartsWith(expectedPrefix))
        {
            var entityInfo = queueNamePrefix.Substring(expectedPrefix.Length);
            
            // For topic/subscription format, extract just the topic name
            var slashIndex = entityInfo.IndexOf('/');
            if (slashIndex > 0)
            {
                return entityInfo.Substring(0, slashIndex);
            }
            
            return entityInfo;
        }
        
        return queueNamePrefix;
    }

    /// <summary>
    /// Extracts the subscription name from a QueueId (for topic mode).
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <param name="providerName">The provider name to remove from the prefix.</param>
    /// <returns>The subscription name, or null if not in topic/subscription format.</returns>
    public static string? GetSubscriptionNameFromQueueId(QueueId queueId, string providerName)
    {
        ArgumentNullException.ThrowIfNull(providerName);
        
        var queueNamePrefix = queueId.GetStringNamePrefix();
        var expectedPrefix = $"{providerName}-";
        
        if (queueNamePrefix.StartsWith(expectedPrefix))
        {
            var entityInfo = queueNamePrefix.Substring(expectedPrefix.Length);
            
            // For topic/subscription format, extract the subscription name
            var slashIndex = entityInfo.IndexOf('/');
            if (slashIndex > 0 && slashIndex < entityInfo.Length - 1)
            {
                return entityInfo.Substring(slashIndex + 1);
            }
        }
        
        return null;
    }

    /// <summary>
    /// Generates a list of QueueIds for partitioned entities.
    /// </summary>
    /// <param name="entityName">The base entity name.</param>
    /// <param name="subscriptionName">The subscription name (for topics only).</param>
    /// <param name="providerName">The provider name.</param>
    /// <param name="partitionCount">The number of partitions.</param>
    /// <param name="entityMode">The entity mode (Queue or Topic).</param>
    /// <returns>An array of QueueIds representing the partitioned entities.</returns>
    public static QueueId[] CreatePartitionedQueues(
        string entityName, 
        string? subscriptionName, 
        string providerName, 
        int partitionCount, 
        ServiceBusEntityMode entityMode)
    {
        ArgumentNullException.ThrowIfNull(entityName);
        ArgumentNullException.ThrowIfNull(providerName);
        
        if (partitionCount < 1)
            throw new ArgumentException("Partition count must be at least 1", nameof(partitionCount));

        var queueIds = new QueueId[partitionCount];
        
        for (int i = 0; i < partitionCount; i++)
        {
            queueIds[i] = entityMode == ServiceBusEntityMode.Topic && !string.IsNullOrEmpty(subscriptionName)
                ? ForTopicSubscription(entityName, subscriptionName, providerName, i)
                : ForQueue(entityName, providerName, i);
        }
        
        return queueIds;
    }

    /// <summary>
    /// Selects a partition for a stream using consistent hashing.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="partitionCount">The number of partitions.</param>
    /// <returns>The partition number (0-based).</returns>
    public static int SelectPartitionForStream(StreamId streamId, int partitionCount)
    {
        if (partitionCount <= 1) return 0;
        
        // Use the stream's hash code for consistent distribution
        var hash = Math.Abs(streamId.GetHashCode());
        return hash % partitionCount;
    }

    /// <summary>
    /// Selects a partition based on the stream's namespace.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="namespaceToPartitionMap">Mapping from namespace to partition.</param>
    /// <param name="defaultPartition">Default partition for unmapped namespaces.</param>
    /// <returns>The partition number.</returns>
    public static int SelectPartitionForNamespace(
        StreamId streamId, 
        IReadOnlyDictionary<string, int> namespaceToPartitionMap, 
        int defaultPartition = 0)
    {
        ArgumentNullException.ThrowIfNull(namespaceToPartitionMap);
        
        // Convert the namespace bytes to string
        var namespaceString = Encoding.UTF8.GetString(streamId.Namespace.Span);
        
        if (namespaceToPartitionMap.TryGetValue(namespaceString, out var partition))
        {
            return partition;
        }
        
        return defaultPartition;
    }

    /// <summary>
    /// Validates an Azure Service Bus entity name according to Azure rules.
    /// </summary>
    /// <param name="entityName">The entity name to validate.</param>
    /// <exception cref="ArgumentException">Thrown if the name is invalid.</exception>
    public static void ValidateEntityName(string entityName)
    {
        ArgumentNullException.ThrowIfNull(entityName);
        
        if (string.IsNullOrWhiteSpace(entityName))
            throw new ArgumentException("Entity name cannot be empty or whitespace", nameof(entityName));
            
        if (entityName.Length > MaxEntityNameLength)
            throw new ArgumentException($"Entity name cannot exceed {MaxEntityNameLength} characters", nameof(entityName));
            
        // Azure Service Bus entity names have specific character restrictions
        // They cannot contain: / \ ? # and some other special characters
        var invalidChars = new[] { '/', '\\', '?', '#' };
        if (entityName.IndexOfAny(invalidChars) >= 0)
            throw new ArgumentException($"Entity name contains invalid characters. Avoid: {string.Join(", ", invalidChars)}", nameof(entityName));
    }
}