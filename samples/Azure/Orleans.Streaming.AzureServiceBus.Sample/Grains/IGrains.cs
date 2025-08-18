using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streaming.AzureServiceBus.Sample;

namespace Orleans.Streaming.AzureServiceBus.Sample.Grains;

/// <summary>
/// Grain interface for producing messages to Azure Service Bus streams.
/// </summary>
public interface IProducerGrain : IGrainWithIntegerKey
{
    /// <summary>
    /// Produces a message to the specified stream.
    /// </summary>
    /// <param name="streamProviderName">The name of the stream provider.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="message">The message to send.</param>
    /// <returns>A task representing the async operation.</returns>
    Task ProduceMessage(string streamProviderName, StreamId streamId, SampleMessage message);

    /// <summary>
    /// Gets the count of messages this producer has sent.
    /// </summary>
    /// <returns>The number of messages sent.</returns>
    Task<int> GetMessagesSentCount();
}

/// <summary>
/// Grain interface for consuming messages from Azure Service Bus streams.
/// </summary>
public interface IConsumerGrain : IGrainWithIntegerKey
{
    /// <summary>
    /// Starts consuming messages from the specified stream.
    /// </summary>
    /// <param name="streamProviderName">The name of the stream provider.</param>
    /// <param name="streamId">The stream identifier to consume from.</param>
    /// <returns>A task representing the async operation.</returns>
    Task StartConsuming(string streamProviderName, StreamId streamId);

    /// <summary>
    /// Stops consuming messages and cleans up the stream subscription.
    /// </summary>
    /// <returns>A task representing the async operation.</returns>
    Task StopConsuming();

    /// <summary>
    /// Gets all messages that have been consumed by this grain.
    /// </summary>
    /// <returns>A list of consumed messages.</returns>
    Task<List<SampleMessage>> GetConsumedMessages();

    /// <summary>
    /// Gets the count of messages this consumer has processed.
    /// </summary>
    /// <returns>The number of messages consumed.</returns>
    Task<int> GetMessagesConsumedCount();
}