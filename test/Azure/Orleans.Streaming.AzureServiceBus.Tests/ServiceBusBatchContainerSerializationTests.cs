using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
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
    public void ServiceBusDataAdapter_RoundTripWithServiceBusMessage_PreservesData()
    {
        // This test validates the round-trip requirement from the issue description
        
        // Arrange
        var serviceProvider = TestServiceProvider.Create();
        var serializer = serviceProvider.GetRequiredService<Serializer<ServiceBusBatchContainer>>();
        var dataAdapter = new ServiceBusDataAdapter(serializer);
        
        var streamId = StreamId.Create("test-namespace", "my-stream-key");
        var events = new List<string> { "event1", "event2", "event3" };
        var requestContext = new Dictionary<string, object> 
        { 
            { "user-id", "12345" }, 
            { "correlation-id", Guid.NewGuid().ToString() } 
        };

        // Act - Convert to ServiceBusMessage
        var serviceBusMessage = dataAdapter.ToQueueMessage(streamId, events, null, requestContext);

        // Verify ServiceBusMessage structure
        Assert.Equal(Headers.ContentType, serviceBusMessage.ContentType);
        Assert.Equal("test-namespace", serviceBusMessage.ApplicationProperties[Headers.StreamNamespace]);
        Assert.Equal("my-stream-key", serviceBusMessage.ApplicationProperties[Headers.StreamId]);

        // Act - Convert back to ServiceBusBatchContainer
        var sequenceId = 98765L;
        var batchContainer = dataAdapter.FromQueueMessage(serviceBusMessage, sequenceId);

        // Assert - Verify round-trip equality
        Assert.Equal(streamId, batchContainer.StreamId);
        Assert.Equal(sequenceId, batchContainer.SequenceToken.SequenceNumber);
        
        var recoveredEvents = batchContainer.GetEvents<string>().Select(t => t.Item1).ToList();
        Assert.Equal(events.Count, recoveredEvents.Count);
        for (int i = 0; i < events.Count; i++)
        {
            Assert.Equal(events[i], recoveredEvents[i]);
        }
        
        // Verify request context was imported correctly
        Assert.True(batchContainer.ImportRequestContext()); // Should return true since we have context
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

/// <summary>
/// Helper class to create a test service provider with the required serialization services.
/// </summary>
internal static class TestServiceProvider
{
    public static IServiceProvider Create()
    {
        var services = new ServiceCollection();
        
        // Add Orleans serialization services
        services.AddSerializer();
        
        return services.BuildServiceProvider();
    }
}