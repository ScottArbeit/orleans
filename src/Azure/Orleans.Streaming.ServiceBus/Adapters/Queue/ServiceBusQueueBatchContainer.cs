using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.ServiceBus;

/// <summary>
/// Batch container for Azure Service Bus queue messages.
/// </summary>
[Serializable]
[GenerateSerializer]
internal class ServiceBusQueueBatchContainer : IBatchContainer
{
    [JsonProperty]
    [Id(0)]
    private EventSequenceToken sequenceToken;

    [JsonProperty]
    [Id(1)]
    private readonly List<object> events;

    [JsonProperty]
    [Id(2)]
    private readonly Dictionary<string, object> requestContext;

    [Id(3)]
    public StreamId StreamId { get; private set; }

    public StreamSequenceToken SequenceToken => sequenceToken;

    internal EventSequenceToken RealSequenceToken
    {
        set => sequenceToken = value;
    }

    [JsonConstructor]
    public ServiceBusQueueBatchContainer(
        StreamId streamId,
        List<object> events,
        Dictionary<string, object> requestContext,
        EventSequenceToken sequenceToken)
        : this(streamId, events, requestContext)
    {
        this.sequenceToken = sequenceToken;
    }

    public ServiceBusQueueBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object> requestContext)
    {
        ArgumentNullException.ThrowIfNull(events);

        StreamId = streamId;
        this.events = events;
        this.requestContext = requestContext;
    }

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        return events.OfType<T>().Select(e => Tuple.Create(e, SequenceToken));
    }

    public bool ImportRequestContext()
    {
        if (requestContext is not null)
        {
            RequestContextExtensions.Import(requestContext);
            return true;
        }
        return false;
    }

    public override string ToString()
    {
        return $"[ServiceBusQueueBatchContainer: StreamId={StreamId}, #Events={events.Count}, SequenceToken={SequenceToken}]";
    }
}