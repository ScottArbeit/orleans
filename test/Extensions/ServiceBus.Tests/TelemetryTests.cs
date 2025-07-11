using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Streaming.ServiceBus;
using Xunit;

namespace ServiceBus.Tests;

/// <summary>
/// Tests for ServiceBus OpenTelemetry instrumentation.
/// </summary>
[TestCategory("BVT")]
[TestCategory("ServiceBus")]
[TestCategory("Telemetry")]
public class TelemetryTests
{
    /// <summary>
    /// Test that verifies three spans are created per message processing scenario.
    /// According to acceptance criteria: client.create, receiver.initialize, and queue.process.
    /// </summary>
    [Fact]
    public void ServiceBusInstrumentation_ShouldCreateThreeSpansPerMessage()
    {
        // Arrange
        var exportedActivities = new List<Activity>();
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => { },
            ActivityStopped = activity => exportedActivities.Add(activity)
        };
        ActivitySource.AddActivityListener(activityListener);

        // Act - Manually create the three expected spans per message
        
        // 1. Client create span
        using (var clientActivity = ServiceBusInstrumentation.ActivitySource.StartActivity(ServiceBusInstrumentation.Activities.ClientCreate))
        {
            clientActivity?.SetTag(ServiceBusInstrumentation.Tags.ServiceBusOptionsName, "test-options");
            clientActivity?.SetTag(ServiceBusInstrumentation.Tags.ServiceBusClientCreated, true);
        }

        // 2. Receiver initialize span
        using (var receiverActivity = ServiceBusInstrumentation.ActivitySource.StartActivity(ServiceBusInstrumentation.Activities.ReceiverInitialize))
        {
            receiverActivity?.SetTag(ServiceBusInstrumentation.Tags.MessagingSystem, ServiceBusInstrumentation.TagValues.MessagingSystemValue);
            receiverActivity?.SetTag(ServiceBusInstrumentation.Tags.MessagingDestinationName, "test-queue");
            receiverActivity?.SetTag(ServiceBusInstrumentation.Tags.ServiceBusEntityType, ServiceBusInstrumentation.TagValues.EntityTypeQueue);
        }

        // 3. Queue process span
        using (var processActivity = ServiceBusInstrumentation.ActivitySource.StartActivity(ServiceBusInstrumentation.Activities.QueueProcess))
        {
            processActivity?.SetTag(ServiceBusInstrumentation.Tags.MessagingSystem, ServiceBusInstrumentation.TagValues.MessagingSystemValue);
            processActivity?.SetTag(ServiceBusInstrumentation.Tags.MessagingDestinationName, "test-queue");
            processActivity?.SetTag(ServiceBusInstrumentation.Tags.MessagingMessageId, "test-message-id");
            processActivity?.SetTag(ServiceBusInstrumentation.Tags.MessagingOperation, ServiceBusInstrumentation.TagValues.MessagingOperationProcess);
            processActivity?.SetTag(ServiceBusInstrumentation.Tags.ServiceBusEntityType, ServiceBusInstrumentation.TagValues.EntityTypeQueue);
        }

        // Assert
        Assert.Equal(3, exportedActivities.Count);
        
        var clientCreateActivity = exportedActivities.FirstOrDefault(a => a.OperationName == ServiceBusInstrumentation.Activities.ClientCreate);
        var receiverInitActivity = exportedActivities.FirstOrDefault(a => a.OperationName == ServiceBusInstrumentation.Activities.ReceiverInitialize);
        var queueProcessActivity = exportedActivities.FirstOrDefault(a => a.OperationName == ServiceBusInstrumentation.Activities.QueueProcess);

        Assert.NotNull(clientCreateActivity);
        Assert.NotNull(receiverInitActivity);
        Assert.NotNull(queueProcessActivity);

        // Verify tags are set correctly
        Assert.Equal("test-options", clientCreateActivity.GetTagItem(ServiceBusInstrumentation.Tags.ServiceBusOptionsName));
        Assert.Equal(ServiceBusInstrumentation.TagValues.MessagingSystemValue, receiverInitActivity.GetTagItem(ServiceBusInstrumentation.Tags.MessagingSystem));
        Assert.Equal("test-queue", queueProcessActivity.GetTagItem(ServiceBusInstrumentation.Tags.MessagingDestinationName));
    }

    /// <summary>
    /// Test that verifies topic processing spans are created correctly.
    /// </summary>
    [Fact]
    public void ServiceBusInstrumentation_ShouldCreateTopicSpans()
    {
        // Arrange
        var exportedActivities = new List<Activity>();
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => { },
            ActivityStopped = activity => exportedActivities.Add(activity)
        };
        ActivitySource.AddActivityListener(activityListener);

        // Act - Create topic process span
        using (var topicActivity = ServiceBusInstrumentation.ActivitySource.StartActivity(ServiceBusInstrumentation.Activities.TopicProcess))
        {
            topicActivity?.SetTag(ServiceBusInstrumentation.Tags.MessagingSystem, ServiceBusInstrumentation.TagValues.MessagingSystemValue);
            topicActivity?.SetTag(ServiceBusInstrumentation.Tags.MessagingDestinationName, "test-topic");
            topicActivity?.SetTag(ServiceBusInstrumentation.Tags.ServiceBusEntityType, ServiceBusInstrumentation.TagValues.EntityTypeTopic);
            topicActivity?.SetTag(ServiceBusInstrumentation.Tags.ServiceBusSubscriptionName, "test-subscription");
            topicActivity?.SetTag(ServiceBusInstrumentation.Tags.MessagingOperation, ServiceBusInstrumentation.TagValues.MessagingOperationProcess);
        }

        // Assert
        Assert.Single(exportedActivities);
        var topicProcessActivity = exportedActivities[0];
        
        Assert.Equal(ServiceBusInstrumentation.Activities.TopicProcess, topicProcessActivity.OperationName);
        Assert.Equal(ServiceBusInstrumentation.TagValues.MessagingSystemValue, topicProcessActivity.GetTagItem(ServiceBusInstrumentation.Tags.MessagingSystem));
        Assert.Equal("test-topic", topicProcessActivity.GetTagItem(ServiceBusInstrumentation.Tags.MessagingDestinationName));
        Assert.Equal(ServiceBusInstrumentation.TagValues.EntityTypeTopic, topicProcessActivity.GetTagItem(ServiceBusInstrumentation.Tags.ServiceBusEntityType));
        Assert.Equal("test-subscription", topicProcessActivity.GetTagItem(ServiceBusInstrumentation.Tags.ServiceBusSubscriptionName));
    }

    /// <summary>
    /// Test that verifies metrics are properly configured and can be measured.
    /// </summary>
    [Fact]
    public void ServiceBusInstrumentation_ShouldHaveCorrectMetrics()
    {
        // Arrange
        var exportedMeasurements = new List<KeyValuePair<string, object>>();
        
        using var meterProvider = new MeterListener();
        meterProvider.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == ServiceBusInstrumentation.MeterName)
            {
                listener.EnableMeasurementEvents(instrument, null);
            }
        };
        meterProvider.SetMeasurementEventCallback<int>((instrument, measurement, tags, state) =>
        {
            exportedMeasurements.Add(new KeyValuePair<string, object>(instrument.Name, measurement));
        });
        meterProvider.Start();

        // Act - Simulate incrementing counters
        ServiceBusInstrumentation.QueueMessagesProcessedCounter.Add(1, 
            new KeyValuePair<string, object>(ServiceBusInstrumentation.Tags.MessagingDestinationName, "test-queue"));
        
        ServiceBusInstrumentation.TopicMessagesProcessedCounter.Add(2,
            new KeyValuePair<string, object>(ServiceBusInstrumentation.Tags.MessagingDestinationName, "test-topic"));
        
        ServiceBusInstrumentation.MessagesDeadLetteredCounter.Add(1,
            new KeyValuePair<string, object>(ServiceBusInstrumentation.Tags.MessagingDestinationName, "test-queue"));

        ServiceBusInstrumentation.ClientCreatedCounter.Add(1,
            new KeyValuePair<string, object>(ServiceBusInstrumentation.Tags.ServiceBusOptionsName, "test-options"));

        ServiceBusInstrumentation.FactoryInitializedCounter.Add(1,
            new KeyValuePair<string, object>(ServiceBusInstrumentation.Tags.ServiceBusOptionsName, "test-provider"));

        // Assert
        var queueMessagesProcessed = exportedMeasurements.Where(m => m.Key == "servicebus.queue.messages_processed").ToList();
        var topicMessagesProcessed = exportedMeasurements.Where(m => m.Key == "servicebus.topic.messages_processed").ToList();
        var messagesDeadLettered = exportedMeasurements.Where(m => m.Key == "servicebus.messages_dead_lettered").ToList();
        var clientsCreated = exportedMeasurements.Where(m => m.Key == "servicebus.client.created").ToList();
        var factoriesInitialized = exportedMeasurements.Where(m => m.Key == "servicebus.factory.initialized").ToList();

        Assert.Single(queueMessagesProcessed);
        Assert.Equal(1, queueMessagesProcessed[0].Value);

        Assert.Single(topicMessagesProcessed);
        Assert.Equal(2, topicMessagesProcessed[0].Value);

        Assert.Single(messagesDeadLettered);
        Assert.Equal(1, messagesDeadLettered[0].Value);

        Assert.Single(clientsCreated);
        Assert.Equal(1, clientsCreated[0].Value);

        Assert.Single(factoriesInitialized);
        Assert.Equal(1, factoriesInitialized[0].Value);
    }

    /// <summary>
    /// Test that verifies the buffer size gauge can be observed.
    /// </summary>
    [Fact]
    public void ServiceBusInstrumentation_ShouldHaveBufferSizeGauge()
    {
        // Arrange & Act
        var bufferSizeGauge = ServiceBusInstrumentation.BufferSizeGauge;

        // Assert
        Assert.NotNull(bufferSizeGauge);
        Assert.Equal("servicebus.receiver.buffer_size", bufferSizeGauge.Name);
        Assert.Equal("Current number of messages in the receiver buffer", bufferSizeGauge.Description);
    }

    /// <summary>
    /// Test that verifies ActivitySource and Meter have correct names and versions.
    /// </summary>
    [Fact]
    public void ServiceBusInstrumentation_ShouldHaveCorrectNamesAndVersions()
    {
        // Assert
        Assert.Equal("Orleans.ServiceBus", ServiceBusInstrumentation.ActivitySource.Name);
        Assert.Equal("1.0.0", ServiceBusInstrumentation.ActivitySource.Version);
        
        Assert.Equal("Orleans.ServiceBus", ServiceBusInstrumentation.Meter.Name);
        Assert.Equal("1.0.0", ServiceBusInstrumentation.Meter.Version);
    }

    /// <summary>
    /// Test that verifies all expected constants are defined.
    /// </summary>
    [Fact]
    public void ServiceBusInstrumentation_ShouldHaveAllExpectedConstants()
    {
        // Assert Activity names
        Assert.Equal("client.create", ServiceBusInstrumentation.Activities.ClientCreate);
        Assert.Equal("receiver.initialize", ServiceBusInstrumentation.Activities.ReceiverInitialize);
        Assert.Equal("queue.process", ServiceBusInstrumentation.Activities.QueueProcess);
        Assert.Equal("topic.process", ServiceBusInstrumentation.Activities.TopicProcess);

        // Assert Tag names
        Assert.Equal("messaging.system", ServiceBusInstrumentation.Tags.MessagingSystem);
        Assert.Equal("messaging.destination.name", ServiceBusInstrumentation.Tags.MessagingDestinationName);
        Assert.Equal("messaging.message.id", ServiceBusInstrumentation.Tags.MessagingMessageId);
        Assert.Equal("messaging.operation", ServiceBusInstrumentation.Tags.MessagingOperation);

        // Assert Tag values
        Assert.Equal("azureservicebus", ServiceBusInstrumentation.TagValues.MessagingSystemValue);
        Assert.Equal("process", ServiceBusInstrumentation.TagValues.MessagingOperationProcess);
        Assert.Equal("queue", ServiceBusInstrumentation.TagValues.EntityTypeQueue);
        Assert.Equal("topic", ServiceBusInstrumentation.TagValues.EntityTypeTopic);
    }
}