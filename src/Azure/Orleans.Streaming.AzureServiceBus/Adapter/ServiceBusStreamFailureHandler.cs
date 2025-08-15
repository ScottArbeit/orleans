using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Stream failure handler for Azure Service Bus that provides proper failure handling,
/// logging with Service Bus MessageId, and optional DLQ detection callback support.
/// 
/// <para>
/// This handler ensures that failed messages are NOT completed, allowing Service Bus
/// to handle retries and eventual dead letter queue routing based on MaxDeliveryCount configuration.
/// </para>
/// <para>
/// The failure tracking uses a concurrent dictionary to track failed sequence tokens,
/// which the Service Bus adapter receiver uses to determine message completion behavior.
/// </para>
/// </summary>
public partial class ServiceBusStreamFailureHandler : IStreamFailureHandler
{
    private readonly ILogger<ServiceBusStreamFailureHandler> _logger;
    private readonly Action<string, StreamId, string>? _dlqCallback;
    
    // Thread-safe dictionary to track failed sequence tokens  
    private readonly ConcurrentDictionary<StreamSequenceToken, bool> _failedTokens = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusStreamFailureHandler"/> class.
    /// </summary>
    /// <param name="logger">The logger for failure events.</param>
    /// <param name="dlqCallback">Optional callback for DLQ move detection (messageId, streamId, reason).</param>
    public ServiceBusStreamFailureHandler(
        ILogger<ServiceBusStreamFailureHandler> logger,
        Action<string, StreamId, string>? dlqCallback = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dlqCallback = dlqCallback;
    }

    /// <summary>
    /// Gets a value indicating whether the subscription should fault when there is an error.
    /// For Service Bus, we don't fault subscriptions on delivery failures as the message will be retried.
    /// </summary>
    public bool ShouldFaultSubsriptionOnError => false;

    /// <summary>
    /// Called once all measures to deliver an event to a consumer have been exhausted.
    /// This logs the failure and marks the sequence token as failed to prevent message completion.
    /// </summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="streamProviderName">Name of the stream provider.</param>
    /// <param name="streamIdentity">The stream identity.</param>
    /// <param name="sequenceToken">The sequence token.</param>
    /// <returns>A <see cref="Task"/> representing the operation.</returns>
    public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, StreamId streamIdentity, StreamSequenceToken sequenceToken)
    {
        // Mark this sequence token as failed
        _failedTokens.TryAdd(sequenceToken, true);
        
        LogDeliveryFailure(
            subscriptionId,
            streamProviderName,
            streamIdentity.GetNamespace() ?? string.Empty,
            streamIdentity.GetKeyAsString() ?? string.Empty,
            sequenceToken);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Should be called when establishing a subscription failed.
    /// </summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="streamProviderName">Name of the stream provider.</param>
    /// <param name="streamIdentity">The stream identity.</param>
    /// <param name="sequenceToken">The sequence token.</param>
    /// <returns>A <see cref="Task"/> representing the operation.</returns>
    public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, StreamId streamIdentity, StreamSequenceToken sequenceToken)
    {
        LogSubscriptionFailure(
            subscriptionId,
            streamProviderName,
            streamIdentity.GetNamespace() ?? string.Empty,
            streamIdentity.GetKeyAsString() ?? string.Empty,
            sequenceToken);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Checks if a sequence token is marked as failed.
    /// </summary>
    /// <param name="sequenceToken">The sequence token to check.</param>
    /// <returns>True if the token is marked as failed, otherwise false.</returns>
    internal bool IsTokenFailed(StreamSequenceToken sequenceToken)
    {
        return _failedTokens.ContainsKey(sequenceToken);
    }

    /// <summary>
    /// Removes a sequence token from the failed tokens collection.
    /// This should be called after the message has been abandoned.
    /// </summary>
    /// <param name="sequenceToken">The sequence token to remove.</param>
    internal void ClearFailedToken(StreamSequenceToken sequenceToken)
    {
        _failedTokens.TryRemove(sequenceToken, out _);
    }

    /// <summary>
    /// Called when a message move to dead letter queue is detected.
    /// This can be used for monitoring and alerting on poison messages.
    /// </summary>
    /// <param name="messageId">The Service Bus message identifier.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="reason">The reason for DLQ move.</param>
    public void OnDeadLetterQueueMove(string messageId, StreamId streamId, string reason)
    {
        LogDeadLetterQueueMove(messageId, streamId.GetNamespace() ?? string.Empty, streamId.GetKeyAsString() ?? string.Empty, reason);
        
        // Invoke optional callback for external monitoring/metrics
        _dlqCallback?.Invoke(messageId, streamId, reason);
    }

    #region Logging

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Stream delivery failure for subscription {SubscriptionId} on provider {StreamProviderName}, " +
                 "stream {StreamNamespace}:{StreamKey}, sequence token {SequenceToken}. " +
                 "Message will NOT be completed to allow Service Bus retry and DLQ handling.")]
    private partial void LogDeliveryFailure(
        GuidId subscriptionId, 
        string streamProviderName, 
        string streamNamespace, 
        string streamKey, 
        StreamSequenceToken sequenceToken);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Stream subscription failure for subscription {SubscriptionId} on provider {StreamProviderName}, " +
                 "stream {StreamNamespace}:{StreamKey}, sequence token {SequenceToken}.")]
    private partial void LogSubscriptionFailure(
        GuidId subscriptionId, 
        string streamProviderName, 
        string streamNamespace, 
        string streamKey, 
        StreamSequenceToken sequenceToken);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Service Bus message {MessageId} moved to dead letter queue for stream {StreamNamespace}:{StreamKey}. Reason: {Reason}")]
    private partial void LogDeadLetterQueueMove(
        string messageId, 
        string streamNamespace, 
        string streamKey, 
        string reason);

    #endregion
}