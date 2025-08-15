namespace Orleans.Streaming.AzureServiceBus.Tests;

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using Xunit;

/// <summary>
/// Tests for ServiceBusStreamFailureHandler functionality.
/// </summary>
public class ServiceBusStreamFailureHandlerTests
{
    [Fact]
    public void Constructor_WithValidLogger_SetsProperties()
    {
        // Arrange
        var logger = NullLogger<ServiceBusStreamFailureHandler>.Instance;

        // Act
        var handler = new ServiceBusStreamFailureHandler(logger);

        // Assert
        Assert.False(handler.ShouldFaultSubsriptionOnError);
    }

    [Fact]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new ServiceBusStreamFailureHandler(null!));
    }

    [Fact]
    public void Constructor_WithCallback_StoresCallback()
    {
        // Arrange
        var logger = NullLogger<ServiceBusStreamFailureHandler>.Instance;
        var callbackInvoked = false;
        Action<string, StreamId, string> dlqCallback = (messageId, streamId, reason) => callbackInvoked = true;

        // Act
        var handler = new ServiceBusStreamFailureHandler(logger, dlqCallback);

        // Assert
        Assert.False(callbackInvoked); // Should not be invoked during construction
    }

    [Fact]
    public async Task OnDeliveryFailure_MarksTokenAsFailed()
    {
        // Arrange
        var logger = NullLogger<ServiceBusStreamFailureHandler>.Instance;
        var handler = new ServiceBusStreamFailureHandler(logger);
        
        var subscriptionId = GuidId.GetNewGuidId();
        var streamProviderName = "test-provider";
        var streamId = StreamId.Create("test-namespace", "test-key");
        var sequenceToken = new EventSequenceTokenV2(123);

        // Act
        await handler.OnDeliveryFailure(subscriptionId, streamProviderName, streamId, sequenceToken);

        // Assert
        Assert.True(handler.IsTokenFailed(sequenceToken));
    }

    [Fact]
    public async Task OnSubscriptionFailure_DoesNotMarkTokenAsFailed()
    {
        // Arrange
        var logger = NullLogger<ServiceBusStreamFailureHandler>.Instance;
        var handler = new ServiceBusStreamFailureHandler(logger);
        
        var subscriptionId = GuidId.GetNewGuidId();
        var streamProviderName = "test-provider";
        var streamId = StreamId.Create("test-namespace", "test-key");
        var sequenceToken = new EventSequenceTokenV2(123);

        // Act
        await handler.OnSubscriptionFailure(subscriptionId, streamProviderName, streamId, sequenceToken);

        // Assert
        Assert.False(handler.IsTokenFailed(sequenceToken));
    }

    [Fact]
    public void IsTokenFailed_WithUnfailedToken_ReturnsFalse()
    {
        // Arrange
        var logger = NullLogger<ServiceBusStreamFailureHandler>.Instance;
        var handler = new ServiceBusStreamFailureHandler(logger);
        var sequenceToken = new EventSequenceTokenV2(123);

        // Act
        var result = handler.IsTokenFailed(sequenceToken);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ClearFailedToken_RemovesTokenFromFailedSet()
    {
        // Arrange
        var logger = NullLogger<ServiceBusStreamFailureHandler>.Instance;
        var handler = new ServiceBusStreamFailureHandler(logger);
        
        var subscriptionId = GuidId.GetNewGuidId();
        var streamProviderName = "test-provider";
        var streamId = StreamId.Create("test-namespace", "test-key");
        var sequenceToken = new EventSequenceTokenV2(123);

        // Mark as failed first
        await handler.OnDeliveryFailure(subscriptionId, streamProviderName, streamId, sequenceToken);
        Assert.True(handler.IsTokenFailed(sequenceToken));

        // Act
        handler.ClearFailedToken(sequenceToken);

        // Assert
        Assert.False(handler.IsTokenFailed(sequenceToken));
    }

    [Fact]
    public void OnDeadLetterQueueMove_InvokesCallback()
    {
        // Arrange
        var logger = NullLogger<ServiceBusStreamFailureHandler>.Instance;
        string? callbackMessageId = null;
        StreamId callbackStreamId = default;
        string? callbackReason = null;

        Action<string, StreamId, string> dlqCallback = (messageId, streamId, reason) =>
        {
            callbackMessageId = messageId;
            callbackStreamId = streamId;
            callbackReason = reason;
        };

        var handler = new ServiceBusStreamFailureHandler(logger, dlqCallback);
        var messageId = "test-message-123";
        var streamId = StreamId.Create("test-namespace", "test-key");
        var reason = "MaxDeliveryCountExceeded";

        // Act
        handler.OnDeadLetterQueueMove(messageId, streamId, reason);

        // Assert
        Assert.Equal(messageId, callbackMessageId);
        Assert.Equal(streamId, callbackStreamId);
        Assert.Equal(reason, callbackReason);
    }

    [Fact]
    public void OnDeadLetterQueueMove_WithNullCallback_DoesNotThrow()
    {
        // Arrange
        var logger = NullLogger<ServiceBusStreamFailureHandler>.Instance;
        var handler = new ServiceBusStreamFailureHandler(logger, null);
        var messageId = "test-message-123";
        var streamId = StreamId.Create("test-namespace", "test-key");
        var reason = "MaxDeliveryCountExceeded";

        // Act & Assert (should not throw)
        handler.OnDeadLetterQueueMove(messageId, streamId, reason);
    }

    [Fact]
    public async Task MultipleFailures_TracksSeparateTokens()
    {
        // Arrange
        var logger = NullLogger<ServiceBusStreamFailureHandler>.Instance;
        var handler = new ServiceBusStreamFailureHandler(logger);
        
        var subscriptionId = GuidId.GetNewGuidId();
        var streamProviderName = "test-provider";
        var streamId = StreamId.Create("test-namespace", "test-key");
        var token1 = new EventSequenceTokenV2(123);
        var token2 = new EventSequenceTokenV2(456);
        var token3 = new EventSequenceTokenV2(789);

        // Act
        await handler.OnDeliveryFailure(subscriptionId, streamProviderName, streamId, token1);
        await handler.OnDeliveryFailure(subscriptionId, streamProviderName, streamId, token2);

        // Assert
        Assert.True(handler.IsTokenFailed(token1));
        Assert.True(handler.IsTokenFailed(token2));
        Assert.False(handler.IsTokenFailed(token3));

        // Clear one token
        handler.ClearFailedToken(token1);
        
        Assert.False(handler.IsTokenFailed(token1));
        Assert.True(handler.IsTokenFailed(token2));
        Assert.False(handler.IsTokenFailed(token3));
    }
}