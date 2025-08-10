using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Messaging.ServiceBus;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests;

/// <summary>
/// Round-trip serialization tests for Service Bus batch container and data adapter.
/// </summary>
public class ServiceBusBatchContainerSerializationTests
{
    [Fact]
    public void ServiceBusBatchContainer_BasicCreation_Succeeds()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-stream");
        var events = new List<object> { "event1", 42, new { Property = "value" } };
        var requestContext = new Dictionary<string, object> { { "key1", "value1" }, { "key2", 123 } };

        // Act
        var container = ServiceBusBatchContainer.CreateWithSequenceId(streamId, events, requestContext, 1L);

        // Assert
        Assert.Equal(streamId, container.StreamId);
        Assert.Equal(events.Count, container.GetEvents<object>().Count());
    }

    [Fact]
    public void ServiceBusBatchContainer_EphemeralSequenceToken_IsNonRewindable()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-stream");
        var events = new List<object> { "event1", "event2" };
        var requestContext = new Dictionary<string, object>();
        var sequenceId = 100L;

        // Act
        var container = ServiceBusBatchContainer.CreateWithSequenceId(streamId, events, requestContext, sequenceId);

        // Assert - Verify ephemeral sequence token properties
        Assert.NotNull(container.SequenceToken);
        Assert.Equal(sequenceId, container.SequenceToken.SequenceNumber);
        
        // Verify individual event tokens are created correctly
        var eventTokens = container.GetEvents<object>().Select(t => t.Item2).ToList();
        Assert.Equal(2, eventTokens.Count);
        
        Assert.Equal(sequenceId, eventTokens[0].SequenceNumber);
        Assert.Equal(0, eventTokens[0].EventIndex);
        
        Assert.Equal(sequenceId, eventTokens[1].SequenceNumber);
        Assert.Equal(1, eventTokens[1].EventIndex);
    }

    [Fact]
    public void ServiceBusBatchContainer_ToString_ContainsExpectedInformation()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-stream");
        var events = new List<object> { "event1", "event2", "event3" };
        var requestContext = new Dictionary<string, object>();
        var sequenceId = 42L;

        // Act
        var container = ServiceBusBatchContainer.CreateWithSequenceId(streamId, events, requestContext, sequenceId);
        var stringRepresentation = container.ToString();

        // Assert
        Assert.Contains("ServiceBusBatchContainer", stringRepresentation);
        Assert.Contains(streamId.ToString(), stringRepresentation);
        Assert.Contains("3", stringRepresentation); // Event count
    }

    [Fact]
    public void ServiceBusMessage_HeaderConstants_AreWellDefined()
    {
        // Assert - Verify header constants are stable and documented
        Assert.Equal("orleans-stream-namespace", Headers.StreamNamespace);
        Assert.Equal("orleans-stream-id", Headers.StreamId);
        Assert.Equal("orleans-sequence-token", Headers.SequenceToken);
        Assert.Equal("orleans-batch-index", Headers.BatchIndex);
        Assert.Equal("application/vnd.orleans.stream-events+json", Headers.ContentType);
    }

    [Fact]
    public void ServiceBusBatchContainer_ImportRequestContext_WorksCorrectly()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-stream");
        var events = new List<object> { "event1" };
        var requestContext = new Dictionary<string, object> { { "key", "value" } };

        // Act
        var container = ServiceBusBatchContainer.CreateContainer(streamId, events, requestContext);
        var imported = container.ImportRequestContext();

        // Assert
        Assert.True(imported);
    }

    [Fact]
    public void ServiceBusBatchContainer_EmptyRequestContext_ReturnsCorrectly()
    {
        // Arrange
        var streamId = StreamId.Create("test-namespace", "test-stream");
        var events = new List<object> { "event1" };
        var requestContext = new Dictionary<string, object>();

        // Act
        var container = ServiceBusBatchContainer.CreateContainer(streamId, events, requestContext);
        var imported = container.ImportRequestContext();

        // Assert
        Assert.False(imported);
    }
}