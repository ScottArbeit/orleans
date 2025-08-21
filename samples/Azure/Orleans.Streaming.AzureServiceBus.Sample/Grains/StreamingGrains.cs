using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streams.Core;
using Orleans.Streaming.AzureServiceBus.Sample;

namespace Orleans.Streaming.AzureServiceBus.Sample.Grains;

/// <summary>
/// Producer grain that sends messages to Azure Service Bus streams.
/// </summary>
public class ProducerGrain : Grain, IProducerGrain
{
    private readonly ILogger<ProducerGrain> _logger;
    private int _messagesSent = 0;

    public ProducerGrain(ILogger<ProducerGrain> logger)
    {
        _logger = logger;
    }

    public async Task ProduceMessage(string streamProviderName, StreamId streamId, SampleMessage message)
    {
        try
        {
            var streamProvider = this.GetStreamProvider(streamProviderName);
            var stream = streamProvider.GetStream<SampleMessage>(streamId);

            await stream.OnNextAsync(message);
            _messagesSent++;

            _logger.LogInformation("Producer {GrainId} sent message: {Content}", 
                this.GetPrimaryKeyLong(), message.Content);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to produce message: {Content}", message.Content);
            throw;
        }
    }

    public Task<int> GetMessagesSentCount()
    {
        return Task.FromResult(_messagesSent);
    }
}

/// <summary>
/// Consumer grain that receives messages from Azure Service Bus streams.
/// </summary>
public class ConsumerGrain : Grain, IConsumerGrain, IStreamSubscriptionObserver
{
    private readonly ILogger<ConsumerGrain> _logger;
    private readonly List<SampleMessage> _consumedMessages = new();
    private StreamSubscriptionHandle<SampleMessage>? _subscription;

    public ConsumerGrain(ILogger<ConsumerGrain> logger)
    {
        _logger = logger;
    }

    public async Task StartConsuming(string streamProviderName, StreamId streamId)
    {
        try
        {
            if (_subscription != null)
            {
                _logger.LogWarning("Consumer {GrainId} is already consuming. Stopping previous subscription.", 
                    this.GetPrimaryKeyLong());
                await StopConsuming();
            }

            var streamProvider = this.GetStreamProvider(streamProviderName);
            var stream = streamProvider.GetStream<SampleMessage>(streamId);

            _subscription = await stream.SubscribeAsync(OnNextAsync, OnErrorAsync, OnCompletedAsync);

            _logger.LogInformation("Consumer {GrainId} started consuming from stream {StreamId}", 
                this.GetPrimaryKeyLong(), streamId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start consuming from stream {StreamId}", streamId);
            throw;
        }
    }

    public async Task StopConsuming()
    {
        if (_subscription != null)
        {
            try
            {
                await _subscription.UnsubscribeAsync();
                _subscription = null;

                _logger.LogInformation("Consumer {GrainId} stopped consuming", this.GetPrimaryKeyLong());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while stopping consumption");
                throw;
            }
        }
    }

    public Task<List<SampleMessage>> GetConsumedMessages()
    {
        return Task.FromResult(new List<SampleMessage>(_consumedMessages));
    }

    public Task<int> GetMessagesConsumedCount()
    {
        return Task.FromResult(_consumedMessages.Count);
    }

    /// <summary>
    /// Called when a new message is received from the stream.
    /// </summary>
    private async Task OnNextAsync(SampleMessage message, StreamSequenceToken? token)
    {
        try
        {
            _consumedMessages.Add(message);

            _logger.LogInformation("Consumer {GrainId} received message: {Content} (Total: {Count})", 
                this.GetPrimaryKeyLong(), message.Content, _consumedMessages.Count);

            // Simulate some processing time
            await Task.Delay(100);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message: {Content}", message.Content);
            throw;
        }
    }

    /// <summary>
    /// Called when an error occurs in the stream.
    /// </summary>
    private Task OnErrorAsync(Exception ex)
    {
        _logger.LogError(ex, "Stream error in consumer {GrainId}", this.GetPrimaryKeyLong());
        return Task.CompletedTask;
    }

    /// <summary>
    /// Called when the stream is completed.
    /// </summary>
    private Task OnCompletedAsync()
    {
        _logger.LogInformation("Stream completed for consumer {GrainId}", this.GetPrimaryKeyLong());
        return Task.CompletedTask;
    }

    /// <summary>
    /// IStreamSubscriptionObserver implementation - called when a subscription is removed.
    /// </summary>
    public Task OnSubscriptionRecoveryError(StreamSubscriptionHandle<SampleMessage> subscription, Exception exception)
    {
        _logger.LogError(exception, "Subscription recovery error for consumer {GrainId}", this.GetPrimaryKeyLong());
        return Task.CompletedTask;
    }

    /// <summary>
    /// IStreamSubscriptionObserver implementation - called when the grain is first subscribed to a stream.
    /// </summary>
    public Task OnSubscribed(IStreamSubscriptionHandleFactory handleFactory)
    {
        _logger.LogInformation("Consumer {GrainId} has been subscribed to a stream via factory", this.GetPrimaryKeyLong());
        return Task.CompletedTask;
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        // Clean up subscription when grain is deactivated
        await StopConsuming();
        await base.OnDeactivateAsync(reason, cancellationToken);
    }
}