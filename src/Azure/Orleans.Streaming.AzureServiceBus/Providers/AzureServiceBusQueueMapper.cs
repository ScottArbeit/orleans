using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streams;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Providers;

/// <summary>
/// Stream queue mapper for Azure Service Bus that supports multiple mapping strategies.
/// Maps streams to Service Bus queues or topics based on configuration and strategy.
/// Supports single entity, hash-based partitioning, round-robin, namespace-based, and custom mapping.
/// </summary>
public class AzureServiceBusQueueMapper : IStreamQueueMapper
{
    private readonly string _providerName;
    private readonly AzureServiceBusOptions _options;
    private readonly QueueId[] _allQueues;
    private readonly Dictionary<string, int> _namespaceToPartitionMap;
    private int _roundRobinCounter;
    private readonly Func<StreamId, QueueId>? _customMappingFunction;

    /// <summary>
    /// Initializes a new instance of the <see cref="AzureServiceBusQueueMapper"/> class.
    /// </summary>
    /// <param name="providerName">The name of the stream provider.</param>
    /// <param name="options">The Azure Service Bus configuration options.</param>
    /// <param name="customMappingFunction">Optional custom mapping function for Custom strategy.</param>
    public AzureServiceBusQueueMapper(
        string providerName, 
        AzureServiceBusOptions options,
        Func<StreamId, QueueId>? customMappingFunction = null)
    {
        _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _customMappingFunction = customMappingFunction;
        _namespaceToPartitionMap = new Dictionary<string, int>(_options.NamespaceToPartitionMap);
        _roundRobinCounter = 0;

        _allQueues = CreateQueueIds();
    }

    /// <inheritdoc/>
    public IEnumerable<QueueId> GetAllQueues()
    {
        return _allQueues;
    }

    /// <inheritdoc/>
    public QueueId GetQueueForStream(StreamId streamId)
    {
        return _options.MappingStrategy switch
        {
            QueueMappingStrategy.Single => GetQueueForSingleStrategy(),
            QueueMappingStrategy.HashBased => GetQueueForHashBasedStrategy(streamId),
            QueueMappingStrategy.RoundRobin => GetQueueForRoundRobinStrategy(),
            QueueMappingStrategy.NamespaceBased => GetQueueForNamespaceBasedStrategy(streamId),
            QueueMappingStrategy.Custom => GetQueueForCustomStrategy(streamId),
            _ => throw new ArgumentException($"Unsupported mapping strategy: {_options.MappingStrategy}")
        };
    }

    /// <summary>
    /// Gets the Service Bus entity name for the specified queue.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>The Service Bus entity name.</returns>
    public string GetEntityNameForQueue(QueueId queueId)
    {
        return ServiceBusEntityMapper.GetEntityNameFromQueueId(queueId, _providerName);
    }

    /// <summary>
    /// Gets the Service Bus subscription name for the specified queue (topic mode only).
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>The Service Bus subscription name, or null if not in topic mode.</returns>
    public string? GetSubscriptionNameForQueue(QueueId queueId)
    {
        return _options.EntityMode == ServiceBusEntityMode.Topic 
            ? ServiceBusEntityMapper.GetSubscriptionNameFromQueueId(queueId, _providerName)
            : null;
    }

    /// <summary>
    /// Gets the partition number for the specified stream when using partitioned entities.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The partition number.</returns>
    public int GetPartitionForStream(StreamId streamId)
    {
        if (_options.PartitionCount <= 1)
            return 0;

        return _options.MappingStrategy switch
        {
            QueueMappingStrategy.HashBased => ServiceBusEntityMapper.SelectPartitionForStream(streamId, _options.PartitionCount),
            QueueMappingStrategy.NamespaceBased => ServiceBusEntityMapper.SelectPartitionForNamespace(streamId, _namespaceToPartitionMap, _options.DefaultPartition),
            QueueMappingStrategy.RoundRobin => Interlocked.Increment(ref _roundRobinCounter) % _options.PartitionCount,
            _ => 0
        };
    }

    private QueueId[] CreateQueueIds()
    {
        return _options.MappingStrategy switch
        {
            QueueMappingStrategy.Single => CreateSingleEntityQueues(),
            QueueMappingStrategy.HashBased => CreateHashBasedQueues(),
            QueueMappingStrategy.RoundRobin => CreateRoundRobinQueues(),
            QueueMappingStrategy.NamespaceBased => CreateNamespaceBasedQueues(),
            QueueMappingStrategy.Custom => CreateCustomMappingQueues(),
            _ => throw new ArgumentException($"Unsupported mapping strategy: {_options.MappingStrategy}")
        };
    }

    private QueueId[] CreateSingleEntityQueues()
    {
        var entityName = string.IsNullOrEmpty(_options.EntityName) ? "orleans-streaming" : _options.EntityName;
        var subscriptionName = _options.EntityMode == ServiceBusEntityMode.Topic ? _options.SubscriptionName : null;

        if (_options.EntityMode == ServiceBusEntityMode.Topic && string.IsNullOrEmpty(subscriptionName))
            throw new InvalidOperationException("SubscriptionName is required when EntityMode is Topic");

        return new[]
        {
            _options.EntityMode == ServiceBusEntityMode.Topic
                ? ServiceBusEntityMapper.ForTopicSubscription(entityName, subscriptionName!, _providerName)
                : ServiceBusEntityMapper.ForQueue(entityName, _providerName)
        };
    }

    private QueueId[] CreateHashBasedQueues()
    {
        var entityName = string.IsNullOrEmpty(_options.EntityName) ? "orleans-streaming" : _options.EntityName;
        var subscriptionName = _options.EntityMode == ServiceBusEntityMode.Topic ? _options.SubscriptionName : null;

        if (_options.EntityMode == ServiceBusEntityMode.Topic && string.IsNullOrEmpty(subscriptionName))
            throw new InvalidOperationException("SubscriptionName is required when EntityMode is Topic");

        return ServiceBusEntityMapper.CreatePartitionedQueues(
            entityName, subscriptionName, _providerName, _options.PartitionCount, _options.EntityMode);
    }

    private QueueId[] CreateRoundRobinQueues()
    {
        // Same as hash-based but used differently in mapping
        return CreateHashBasedQueues();
    }

    private QueueId[] CreateNamespaceBasedQueues()
    {
        if (_options.NamespaceToPartitionMap.Count == 0)
        {
            // If no namespace mapping provided, fall back to single entity
            return CreateSingleEntityQueues();
        }

        var maxPartition = Math.Max(_options.NamespaceToPartitionMap.Values.Max(), _options.DefaultPartition);
        var partitionCount = maxPartition + 1; // 0-based indexing

        var entityName = string.IsNullOrEmpty(_options.EntityName) ? "orleans-streaming" : _options.EntityName;
        var subscriptionName = _options.EntityMode == ServiceBusEntityMode.Topic ? _options.SubscriptionName : null;

        if (_options.EntityMode == ServiceBusEntityMode.Topic && string.IsNullOrEmpty(subscriptionName))
            throw new InvalidOperationException("SubscriptionName is required when EntityMode is Topic");

        return ServiceBusEntityMapper.CreatePartitionedQueues(
            entityName, subscriptionName, _providerName, partitionCount, _options.EntityMode);
    }

    private QueueId[] CreateCustomMappingQueues()
    {
        // For custom mapping, we need to provide a reasonable set of queues
        // This could be configured or we can fall back to single entity
        if (_options.EntityNames.Count > 0)
        {
            return CreateMultiEntityQueues();
        }

        // Fall back to single entity for custom mapping
        return CreateSingleEntityQueues();
    }

    private QueueId[] CreateMultiEntityQueues()
    {
        var queues = new List<QueueId>();

        if (_options.EntityMode == ServiceBusEntityMode.Topic)
        {
            // For topics, we need subscriptions
            var subscriptions = _options.SubscriptionNames.Count > 0 ? _options.SubscriptionNames : new List<string> { _options.SubscriptionName };
            
            if (subscriptions.All(string.IsNullOrEmpty))
                throw new InvalidOperationException("At least one subscription name is required for Topic mode");

            foreach (var entityName in _options.EntityNames)
            {
                foreach (var subscription in subscriptions.Where(s => !string.IsNullOrEmpty(s)))
                {
                    queues.Add(ServiceBusEntityMapper.ForTopicSubscription(entityName, subscription, _providerName));
                }
            }
        }
        else
        {
            // For queues, just create one queue per entity name
            foreach (var entityName in _options.EntityNames)
            {
                queues.Add(ServiceBusEntityMapper.ForQueue(entityName, _providerName));
            }
        }

        return queues.ToArray();
    }

    private QueueId GetQueueForSingleStrategy()
    {
        return _allQueues[0];
    }

    private QueueId GetQueueForHashBasedStrategy(StreamId streamId)
    {
        if (_allQueues.Length == 1)
            return _allQueues[0];

        var partition = ServiceBusEntityMapper.SelectPartitionForStream(streamId, _allQueues.Length);
        return _allQueues[partition];
    }

    private QueueId GetQueueForRoundRobinStrategy()
    {
        if (_allQueues.Length == 1)
            return _allQueues[0];

        var index = Math.Abs(Interlocked.Increment(ref _roundRobinCounter)) % _allQueues.Length;
        return _allQueues[index];
    }

    private QueueId GetQueueForNamespaceBasedStrategy(StreamId streamId)
    {
        if (_allQueues.Length == 1)
            return _allQueues[0];

        var partition = ServiceBusEntityMapper.SelectPartitionForNamespace(streamId, _namespaceToPartitionMap, _options.DefaultPartition);
        partition = Math.Min(partition, _allQueues.Length - 1); // Ensure we don't exceed available queues
        return _allQueues[partition];
    }

    private QueueId GetQueueForCustomStrategy(StreamId streamId)
    {
        if (_customMappingFunction is not null)
        {
            var customQueue = _customMappingFunction(streamId);
            
            // Validate that the custom queue is in our available queues
            if (_allQueues.Contains(customQueue))
                return customQueue;
        }

        // Fall back to hash-based strategy if custom mapping fails
        return GetQueueForHashBasedStrategy(streamId);
    }
}