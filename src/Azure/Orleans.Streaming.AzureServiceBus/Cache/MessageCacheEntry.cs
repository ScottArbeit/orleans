using System;
using Orleans.Streams;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Cache;

/// <summary>
/// Represents a cached message entry for Azure Service Bus streaming.
/// </summary>
internal sealed class MessageCacheEntry
{
    /// <summary>
    /// Gets the cached batch container.
    /// </summary>
    public IBatchContainer BatchContainer { get; }

    /// <summary>
    /// Gets the sequence token for the message.
    /// </summary>
    public StreamSequenceToken SequenceToken { get; }

    /// <summary>
    /// Gets the time when the message was enqueued to Azure Service Bus.
    /// </summary>
    public DateTime EnqueueTimeUtc { get; }

    /// <summary>
    /// Gets the time when the message was dequeued from Azure Service Bus and added to cache.
    /// </summary>
    public DateTime DequeueTimeUtc { get; }

    /// <summary>
    /// Gets the time when the message was last accessed from the cache.
    /// </summary>
    public DateTime LastAccessTimeUtc { get; private set; }

    /// <summary>
    /// Gets a value indicating whether this message has delivery failure.
    /// </summary>
    public bool DeliveryFailure { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="MessageCacheEntry"/> class.
    /// </summary>
    /// <param name="batchContainer">The batch container.</param>
    /// <param name="sequenceToken">The sequence token.</param>
    /// <param name="enqueueTimeUtc">The enqueue time in UTC.</param>
    /// <param name="dequeueTimeUtc">The dequeue time in UTC.</param>
    /// <param name="deliveryFailure">Whether this message has delivery failure.</param>
    public MessageCacheEntry(
        IBatchContainer batchContainer,
        StreamSequenceToken sequenceToken,
        DateTime enqueueTimeUtc,
        DateTime dequeueTimeUtc,
        bool deliveryFailure = false)
    {
        BatchContainer = batchContainer ?? throw new ArgumentNullException(nameof(batchContainer));
        SequenceToken = sequenceToken ?? throw new ArgumentNullException(nameof(sequenceToken));
        EnqueueTimeUtc = enqueueTimeUtc;
        DequeueTimeUtc = dequeueTimeUtc;
        LastAccessTimeUtc = dequeueTimeUtc;
        DeliveryFailure = deliveryFailure;
    }

    /// <summary>
    /// Updates the last access time to the current UTC time.
    /// </summary>
    public void UpdateLastAccessTime()
    {
        LastAccessTimeUtc = DateTime.UtcNow;
    }

    /// <summary>
    /// Gets the age of the message since it was enqueued.
    /// </summary>
    /// <param name="currentTime">The current time to calculate age against.</param>
    /// <returns>The message age.</returns>
    public TimeSpan GetMessageAge(DateTime currentTime)
    {
        return currentTime - EnqueueTimeUtc;
    }

    /// <summary>
    /// Gets the idle time since the message was last accessed.
    /// </summary>
    /// <param name="currentTime">The current time to calculate idle time against.</param>
    /// <returns>The idle time.</returns>
    public TimeSpan GetIdleTime(DateTime currentTime)
    {
        return currentTime - LastAccessTimeUtc;
    }
}