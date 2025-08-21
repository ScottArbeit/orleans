using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Implementation of <see cref="IStreamQueueMapper"/> and <see cref="IConsistentRingStreamQueueMapper"/> for Azure Service Bus streaming provider.
/// Supports both single entity mapping (MVP) and multiple entity mapping with consistent hashing for scale-out scenarios.
/// </summary>
public class ServiceBusQueueMapper : IConsistentRingStreamQueueMapper
{
    /// <summary>
    /// The queue identifier(s) representing the configured Service Bus entity/entities.
    /// For single entity (EntityCount = 1), this contains one queue.
    /// For multiple entities, this contains multiple queues arranged in a hash ring.
    /// </summary>
    private readonly QueueId[] _queueIds;

    /// <summary>
    /// The hash ring based mapper for consistent stream-to-queue mapping when using multiple entities.
    /// Null when using single entity mapping.
    /// </summary>
    private readonly HashRingBasedStreamQueueMapper? _hashRingMapper;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusQueueMapper"/> class.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <param name="providerName">The stream provider name.</param>
    /// <exception cref="ArgumentNullException">Thrown when options or providerName is null.</exception>
    /// <exception cref="ArgumentException">Thrown when providerName is empty or whitespace.</exception>
    public ServiceBusQueueMapper(ServiceBusStreamOptions options, string providerName)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);

        if (options.EntityCount == 1)
        {
            // Single entity: create one QueueId
            var entityName = ServiceBusEntityNamer.GetEntityName(options);
            _queueIds = [QueueId.GetQueueId(entityName, 0, 0)];
            _hashRingMapper = null;
        }
        else
        {
            // Multiple entities: use HashRingBasedStreamQueueMapper for consistent hashing
            // Create entity names for all indices and use a consistent prefix
            var entityPrefix = GetEntityPrefix(options);
            var mapperOptions = new HashRingStreamQueueMapperOptions { TotalQueueCount = options.EntityCount };
            _hashRingMapper = new HashRingBasedStreamQueueMapper(mapperOptions, entityPrefix);
            _queueIds = _hashRingMapper.GetAllQueues().ToArray();
        }
    }

    /// <summary>
    /// Gets the entity prefix to use for multiple entities based on the configuration.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <returns>The entity prefix for consistent naming.</returns>
    private static string GetEntityPrefix(ServiceBusStreamOptions options)
    {
        return options.EntityKind switch
        {
            EntityKind.Queue => $"{options.EntityNamePrefix}-q",
            EntityKind.TopicSubscription => $"{options.TopicName}:{options.EntityNamePrefix}-sub",
            _ => throw new InvalidOperationException($"Unsupported entity kind: {options.EntityKind}")
        };
    }

    /// <summary>
    /// Gets all queues. Returns all configured entities.
    /// </summary>
    /// <returns>A collection containing all queue identifiers.</returns>
    public IEnumerable<QueueId> GetAllQueues()
    {
        return _queueIds;
    }

    /// <summary>
    /// Gets the queue for the specified stream. 
    /// For single entity, all streams map to the same queue.
    /// For multiple entities, uses consistent hashing to map streams to queues.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The queue identifier responsible for the specified stream.</returns>
    public QueueId GetQueueForStream(StreamId streamId)
    {
        if (_hashRingMapper is null)
        {
            // Single entity: all streams map to the single queue
            return _queueIds[0];
        }
        else
        {
            // Multiple entities: use consistent hashing
            return _hashRingMapper.GetQueueForStream(streamId);
        }
    }

    /// <summary>
    /// Gets the queues which map to the specified range.
    /// This is required for the IConsistentRingStreamQueueMapper interface.
    /// </summary>
    /// <param name="range">The range.</param>
    /// <returns>The queues which map to the specified range.</returns>
    public IEnumerable<QueueId> GetQueuesForRange(IRingRange range)
    {
        if (_hashRingMapper is null)
        {
            // Single entity: if the range includes our single queue, return it
            if (range.InRange(_queueIds[0].GetUniformHashCode()))
            {
                yield return _queueIds[0];
            }
        }
        else
        {
            // Multiple entities: delegate to the hash ring mapper
            foreach (var queueId in _hashRingMapper.GetQueuesForRange(range))
            {
                yield return queueId;
            }
        }
    }

    /// <summary>
    /// Gets the first configured queue identifier for backward compatibility.
    /// For single entity, returns the single queue.
    /// For multiple entities, returns the first queue (index 0).
    /// </summary>
    public QueueId QueueId => _queueIds[0];

    /// <summary>
    /// Returns a string representation of this mapper.
    /// </summary>
    /// <returns>A string describing the mapper configuration.</returns>
    public override string ToString()
    {
        if (_hashRingMapper is null)
        {
            return $"ServiceBusQueueMapper(SingleEntity: {_queueIds[0]})";
        }
        else
        {
            return $"ServiceBusQueueMapper(MultipleEntities: {_queueIds.Length} entities, HashRing: {_hashRingMapper})";
        }
    }
}