#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streaming.ServiceBus.Storage;
using Orleans.Streams;

namespace Orleans.Providers.Streams.AzureServiceBus;

/// <summary>
/// Queue adapter for Azure Service Bus streaming provider.
/// Integrates with Orleans streaming infrastructure and coordinates message sending,
/// handling stream-to-queue mapping and message dispatch.
/// </summary>
internal sealed class AzureServiceBusAdapter : IQueueAdapter
{
    private readonly AzureServiceBusOptions _options;
    private readonly HashRingBasedPartitionedStreamQueueMapper _streamQueueMapper;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ConcurrentDictionary<QueueId, AzureServiceBusDataManager> _managers = new();
    private readonly IQueueDataAdapter<ServiceBusMessage, IBatchContainer> _dataAdapter;

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public bool IsRewindable => false;

    /// <inheritdoc/>
    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    /// <summary>
    /// Initializes a new instance of the <see cref="AzureServiceBusAdapter"/> class.
    /// </summary>
    /// <param name="dataAdapter">The data adapter for converting between Orleans and Service Bus messages.</param>
    /// <param name="streamQueueMapper">The stream queue mapper for mapping streams to queues.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="options">The Azure Service Bus options.</param>
    /// <param name="providerName">The stream provider name.</param>
    public AzureServiceBusAdapter(
        IQueueDataAdapter<ServiceBusMessage, IBatchContainer> dataAdapter,
        HashRingBasedPartitionedStreamQueueMapper streamQueueMapper,
        ILoggerFactory loggerFactory,
        AzureServiceBusOptions options,
        string providerName)
    {
        _dataAdapter = dataAdapter ?? throw new ArgumentNullException(nameof(dataAdapter));
        _streamQueueMapper = streamQueueMapper ?? throw new ArgumentNullException(nameof(streamQueueMapper));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        Name = providerName ?? throw new ArgumentNullException(nameof(providerName));
    }

    /// <inheritdoc/>
    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        // For now, throw NotImplementedException as this will be implemented in issue #7
        throw new NotImplementedException("AzureServiceBusAdapterReceiver creation will be implemented in a separate issue.");
    }

    /// <inheritdoc/>
    public async Task QueueMessageBatchAsync<T>(
        StreamId streamId, 
        IEnumerable<T> events, 
        StreamSequenceToken? token, 
        Dictionary<string, object> requestContext)
    {
        if (token is not null)
        {
            throw new ArgumentException("Azure Service Bus stream provider currently does not support non-null StreamSequenceToken.", nameof(token));
        }

        var queueId = _streamQueueMapper.GetQueueForStream(streamId);
        var dataManager = await GetOrCreateDataManager(queueId);
        var serviceBusMessage = _dataAdapter.ToQueueMessage(streamId, events, null, requestContext);
        
        await dataManager.SendMessageAsync(serviceBusMessage);
    }

    /// <summary>
    /// Gets or creates a data manager for the specified queue ID.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>The data manager for the queue.</returns>
    private async Task<AzureServiceBusDataManager> GetOrCreateDataManager(QueueId queueId)
    {
        if (_managers.TryGetValue(queueId, out var existingManager))
        {
            return existingManager;
        }

        var entityName = GetEntityName(queueId);
        var newManager = new AzureServiceBusDataManager(_loggerFactory, entityName, _options);
        await newManager.InitAsync();
        
        // Use GetOrAdd to handle race conditions - if another thread added a manager
        // between our TryGetValue and GetOrAdd, we'll use the existing one and dispose ours
        var actualManager = _managers.GetOrAdd(queueId, newManager);
        if (actualManager != newManager)
        {
            // Another thread won the race, dispose our manager and use theirs
            await newManager.DisposeAsync();
        }
        
        return actualManager;
    }

    /// <summary>
    /// Gets the Service Bus entity name for the specified queue ID.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>The entity name to use for Service Bus operations.</returns>
    private string GetEntityName(QueueId queueId)
    {
        var partitionName = _streamQueueMapper.QueueToPartition(queueId);
        
        return _options.TopologyType switch
        {
            ServiceBusTopologyType.Queue => _options.QueueName ?? partitionName,
            ServiceBusTopologyType.Topic => _options.TopicName ?? partitionName,
            _ => throw new InvalidOperationException($"Unknown topology type: {_options.TopologyType}")
        };
    }
}