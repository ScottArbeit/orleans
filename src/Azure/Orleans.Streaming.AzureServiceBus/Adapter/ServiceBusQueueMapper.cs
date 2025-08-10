using System;
using System.Collections.Generic;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Implementation of <see cref="IStreamQueueMapper"/> for Azure Service Bus streaming provider.
/// Maps all streams to a single queue representing the configured Service Bus entity.
/// This is the MVP implementation that supports only a single entity.
/// </summary>
public class ServiceBusQueueMapper : IStreamQueueMapper
{
    /// <summary>
    /// The single queue identifier representing the configured Service Bus entity.
    /// </summary>
    private readonly QueueId _queueId;

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

        // Get the canonical entity name from the entity namer
        var entityName = ServiceBusEntityNamer.GetEntityName(options);
        
        // Create a single QueueId with the entity name as the queue prefix
        // Use queue number 0 and hash 0 since we only have one entity in the MVP
        _queueId = QueueId.GetQueueId(entityName, 0, 0);
    }

    /// <summary>
    /// Gets all queues. For MVP, this returns a single queue representing the configured entity.
    /// </summary>
    /// <returns>A collection containing the single queue identifier.</returns>
    public IEnumerable<QueueId> GetAllQueues()
    {
        yield return _queueId;
    }

    /// <summary>
    /// Gets the queue for the specified stream. For MVP, all streams map to the same single queue.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The single queue identifier for all streams.</returns>
    public QueueId GetQueueForStream(StreamId streamId)
    {
        // In the MVP, all streams map to the single configured entity
        return _queueId;
    }

    /// <summary>
    /// Gets the configured queue identifier for this mapper.
    /// </summary>
    public QueueId QueueId => _queueId;
}