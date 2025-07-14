using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Providers.Streams.ServiceBus;

/// <summary>
/// Batch container for Azure Service Bus messages.
/// </summary>
[Serializable]
[GenerateSerializer]
public class ServiceBusBatchContainer : IBatchContainer
{
    [Id(0)]
    private EventSequenceTokenV2 sequenceToken = null!;

    [Id(1)]
    private readonly List<object> events;

    [Id(2)]
    private readonly Dictionary<string, object> requestContext;

    [Id(3)]
    public StreamId StreamId { get; private set; }

    public StreamSequenceToken SequenceToken => sequenceToken;

    internal EventSequenceTokenV2 RealSequenceToken
    {
        set => sequenceToken = value;
    }

    public ServiceBusBatchContainer(
        StreamId streamId,
        List<object> events,
        Dictionary<string, object> requestContext,
        EventSequenceTokenV2 sequenceToken)
        : this(streamId, events, requestContext)
    {
        this.sequenceToken = sequenceToken;
    }

    public ServiceBusBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object> requestContext)
    {
        ArgumentNullException.ThrowIfNull(events, nameof(events));

        StreamId = streamId;
        this.events = events;
        this.requestContext = requestContext ?? [];
        sequenceToken = new EventSequenceTokenV2(DateTime.UtcNow.Ticks);
    }

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        return events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, sequenceToken.CreateSequenceTokenForEvent(i)));
    }

    public bool ImportRequestContext()
    {
        if (requestContext.Count > 0)
        {
            RequestContextExtensions.Import(requestContext);
            return true;
        }
        return false;
    }

    public override string ToString()
    {
        return $"[ServiceBusBatchContainer:Stream={StreamId},#Items={events.Count}]";
    }
}