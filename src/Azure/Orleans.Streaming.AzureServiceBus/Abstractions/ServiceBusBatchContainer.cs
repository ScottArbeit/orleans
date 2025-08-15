using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Orleans batch container for Azure Service Bus streaming provider.
/// This implementation is modeled after the SQS provider and is non-rewindable with ephemeral sequence tokens.
/// 
/// <para>
/// The sequence token used by this batch container is ephemeral and non-rewindable. It only provides
/// a queue-local monotonic counter for ordering within a single Service Bus processing session.
/// Unlike Event Hubs, this does not support rewind semantics or persistent offset tracking.
/// </para>
/// </summary>
/// <remarks>
/// This implementation follows the SQS pattern for non-rewindable streaming semantics as specified
/// in the requirements. The sequence tokens are ephemeral and only valid within the current
/// processing context.
/// </remarks>
[Serializable]
[GenerateSerializer]
public class ServiceBusBatchContainer : IBatchContainer
{
    [Id(0)]
    private EventSequenceTokenV2? sequenceToken;

    [Id(1)]
    private readonly List<object> events;

    [Id(2)]
    private readonly Dictionary<string, object> requestContext;

    /// <summary>
    /// Gets the stream identifier for the stream this batch is part of.
    /// </summary>
    [Id(3)]
    public StreamId StreamId { get; private set; }

    [NonSerialized]
    // Need to store reference to the original Service Bus received message to be able to complete/abandon it later.
    // Don't need to serialize it, since we are never interested in sending it to stream consumers.
    internal ServiceBusAdapterReceiver.ReceivedMessage? ReceivedMessage;

    [NonSerialized]
    // Track delivery status to determine whether to complete or abandon the message
    internal bool DeliveryFailed;

    /// <summary>
    /// Gets the sequence token for this batch.
    /// This is an ephemeral, non-rewindable token that only tracks queue-local monotonic position.
    /// </summary>
    public StreamSequenceToken SequenceToken => sequenceToken ?? throw new InvalidOperationException("Sequence token has not been set");

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusBatchContainer"/> class.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">The list of events in this batch.</param>
    /// <param name="requestContext">The request context dictionary.</param>
    /// <param name="sequenceToken">The ephemeral sequence token.</param>
    private ServiceBusBatchContainer(
        StreamId streamId,
        List<object> events,
        Dictionary<string, object> requestContext,
        EventSequenceTokenV2 sequenceToken)
        : this(streamId, events, requestContext)
    {
        this.sequenceToken = sequenceToken;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusBatchContainer"/> class.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">The list of events in this batch.</param>
    /// <param name="requestContext">The request context dictionary.</param>
    private ServiceBusBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object> requestContext)
    {
        ArgumentNullException.ThrowIfNull(events);

        StreamId = streamId;
        this.events = events;
        this.requestContext = requestContext ?? new Dictionary<string, object>();
    }

    /// <summary>
    /// Parameterless constructor for serialization.
    /// </summary>
    public ServiceBusBatchContainer()
    {
        events = new List<object>();
        requestContext = new Dictionary<string, object>();
    }

    /// <summary>
    /// Gets the list of events in this batch (internal access for adapters).
    /// </summary>
    internal List<object> Events => events;

    /// <summary>
    /// Gets the request context dictionary (internal access for adapters).
    /// </summary>
    internal Dictionary<string, object> RequestContext => requestContext;

    /// <summary>
    /// Gets the events in this batch with their individual sequence tokens.
    /// </summary>
    /// <typeparam name="T">The event type.</typeparam>
    /// <returns>An enumerable of tuples containing events and their sequence tokens.</returns>
    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        if (sequenceToken is null)
            throw new InvalidOperationException("Sequence token has not been set");

        return events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, sequenceToken.CreateSequenceTokenForEvent(i)));
    }

    /// <summary>
    /// Imports the request context from this batch container into the current request context.
    /// </summary>
    /// <returns><c>true</c> if request context was imported; otherwise, <c>false</c>.</returns>
    public bool ImportRequestContext()
    {
        if (requestContext is not null && requestContext.Count > 0)
        {
            RequestContextExtensions.Import(requestContext);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Creates a new <see cref="ServiceBusBatchContainer"/> for the given stream and events.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">The events to include in the batch.</param>
    /// <param name="requestContext">The request context.</param>
    /// <returns>A new batch container.</returns>
    public static ServiceBusBatchContainer CreateContainer(StreamId streamId, List<object> events, Dictionary<string, object> requestContext)
    {
        return new ServiceBusBatchContainer(streamId, events, requestContext);
    }

    /// <summary>
    /// Creates a new <see cref="ServiceBusBatchContainer"/> from a deserialized batch with the given sequence ID.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">The events in the batch.</param>
    /// <param name="requestContext">The request context.</param>
    /// <param name="sequenceId">The sequence ID for creating the ephemeral token.</param>
    /// <returns>A new batch container with the sequence token set.</returns>
    public static ServiceBusBatchContainer CreateWithSequenceId(
        StreamId streamId, 
        List<object> events, 
        Dictionary<string, object> requestContext, 
        long sequenceId)
    {
        var sequenceToken = new EventSequenceTokenV2(sequenceId);
        return new ServiceBusBatchContainer(streamId, events, requestContext, sequenceToken);
    }

    /// <summary>
    /// Gets the Service Bus MessageId if available.
    /// </summary>
    /// <returns>The MessageId or null if not available.</returns>
    internal string? GetServiceBusMessageId()
    {
        return ReceivedMessage?.ServiceBusReceivedMessage.MessageId;
    }

    /// <summary>
    /// Marks this batch as having failed delivery to consumers.
    /// This prevents the message from being completed, allowing Service Bus to handle retries.
    /// </summary>
    internal void MarkDeliveryFailed()
    {
        DeliveryFailed = true;
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        return $"[ServiceBusBatchContainer: Stream={StreamId}, #Items={events.Count}, Token={sequenceToken}]";
    }
}