using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;

namespace Orleans.Streaming.ServiceBus;

/// <summary>
/// Contains ServiceBus instrumentation infrastructure for OpenTelemetry support.
/// Provides metrics, traces, and logs aligned with semantic conventions.
/// </summary>
public static class ServiceBusInstrumentation
{
    /// <summary>
    /// The name of the ActivitySource for ServiceBus tracing.
    /// </summary>
    public const string ActivitySourceName = "Orleans.ServiceBus";

    /// <summary>
    /// The name of the Meter for ServiceBus metrics.
    /// </summary>
    public const string MeterName = "Orleans.ServiceBus";

    /// <summary>
    /// The version of the instrumentation.
    /// </summary>
    public const string Version = "1.0.0";

    /// <summary>
    /// Gets the ActivitySource for ServiceBus telemetry.
    /// </summary>
    public static ActivitySource ActivitySource { get; } = new(ActivitySourceName, Version);

    /// <summary>
    /// Gets the Meter for ServiceBus metrics.
    /// </summary>
    public static Meter Meter { get; } = new(MeterName, Version);

    /// <summary>
    /// Registry of active receivers for buffer size monitoring.
    /// </summary>
    private static readonly ConcurrentDictionary<string, IBufferSizeProvider> _activeReceivers = new();

    // Counters
    /// <summary>
    /// Counter for the number of Service Bus queue messages processed.
    /// </summary>
    public static readonly Counter<int> QueueMessagesProcessedCounter = Meter.CreateCounter<int>(
        "servicebus.queue.messages_processed",
        description: "Number of Service Bus queue messages processed");

    /// <summary>
    /// Counter for the number of Service Bus topic messages processed.
    /// </summary>
    public static readonly Counter<int> TopicMessagesProcessedCounter = Meter.CreateCounter<int>(
        "servicebus.topic.messages_processed", 
        description: "Number of Service Bus topic messages processed");

    /// <summary>
    /// Counter for the number of Service Bus messages dead-lettered.
    /// </summary>
    public static readonly Counter<int> MessagesDeadLetteredCounter = Meter.CreateCounter<int>(
        "servicebus.messages_dead_lettered",
        description: "Number of Service Bus messages dead-lettered");

    /// <summary>
    /// Counter for the number of ServiceBus clients created.
    /// </summary>
    public static readonly Counter<int> ClientCreatedCounter = Meter.CreateCounter<int>(
        "servicebus.client.created",
        description: "Number of ServiceBus clients created");

    /// <summary>
    /// Counter for the number of ServiceBus adapter factories initialized.
    /// </summary>
    public static readonly Counter<int> FactoryInitializedCounter = Meter.CreateCounter<int>(
        "servicebus.factory.initialized",
        description: "Number of ServiceBus adapter factories initialized");

    // Gauges
    /// <summary>
    /// Gauge for the current buffer size of pending messages in receivers.
    /// </summary>
    public static readonly ObservableGauge<int> BufferSizeGauge = Meter.CreateObservableGauge<int>(
        "servicebus.receiver.buffer_size",
        description: "Current number of messages in the receiver buffer",
        observeValues: () => _activeReceivers.Select(kvp => new Measurement<int>(
            kvp.Value.GetBufferSize(),
            new KeyValuePair<string, object?>("receiver.name", kvp.Key))));

    /// <summary>
    /// Interface for objects that can provide buffer size information.
    /// </summary>
    public interface IBufferSizeProvider
    {
        /// <summary>
        /// Gets the current buffer size.
        /// </summary>
        /// <returns>The current buffer size.</returns>
        int GetBufferSize();
    }

    /// <summary>
    /// Registers a receiver for buffer size monitoring.
    /// </summary>
    /// <param name="receiverName">The unique name of the receiver.</param>
    /// <param name="receiver">The receiver that implements IBufferSizeProvider.</param>
    public static void RegisterReceiver(string receiverName, IBufferSizeProvider receiver)
    {
        _activeReceivers[receiverName] = receiver;
    }

    /// <summary>
    /// Unregisters a receiver from buffer size monitoring.
    /// </summary>
    /// <param name="receiverName">The unique name of the receiver.</param>
    public static void UnregisterReceiver(string receiverName)
    {
        _activeReceivers.TryRemove(receiverName, out _);
    }

    /// <summary>
    /// Activity names for ServiceBus operations.
    /// </summary>
    public static class Activities
    {
        /// <summary>
        /// Activity name for ServiceBus client creation.
        /// </summary>
        public const string ClientCreate = "client.create";

        /// <summary>
        /// Activity name for ServiceBus receiver initialization.
        /// </summary>
        public const string ReceiverInitialize = "receiver.initialize";

        /// <summary>
        /// Activity name for ServiceBus queue message processing.
        /// </summary>
        public const string QueueProcess = "queue.process";

        /// <summary>
        /// Activity name for ServiceBus topic message processing.
        /// </summary>
        public const string TopicProcess = "topic.process";
    }

    /// <summary>
    /// Standard tags for ServiceBus telemetry aligned with OpenTelemetry semantic conventions.
    /// </summary>
    public static class Tags
    {
        /// <summary>
        /// The messaging system identifier (azureservicebus).
        /// </summary>
        public const string MessagingSystem = "messaging.system";

        /// <summary>
        /// The destination name (queue or topic name).
        /// </summary>
        public const string MessagingDestinationName = "messaging.destination.name";

        /// <summary>
        /// The message ID.
        /// </summary>
        public const string MessagingMessageId = "messaging.message.id";

        /// <summary>
        /// The operation name.
        /// </summary>
        public const string MessagingOperation = "messaging.operation";

        /// <summary>
        /// The ServiceBus options name.
        /// </summary>
        public const string ServiceBusOptionsName = "servicebus.options_name";

        /// <summary>
        /// The ServiceBus entity type (queue or topic).
        /// </summary>
        public const string ServiceBusEntityType = "servicebus.entity_type";

        /// <summary>
        /// The ServiceBus subscription name (for topics).
        /// </summary>
        public const string ServiceBusSubscriptionName = "servicebus.subscription_name";

        /// <summary>
        /// Whether the client was created successfully.
        /// </summary>
        public const string ServiceBusClientCreated = "servicebus.client.created";
    }

    /// <summary>
    /// Standard tag values for ServiceBus telemetry.
    /// </summary>
    public static class TagValues
    {
        /// <summary>
        /// The messaging system value for Azure Service Bus.
        /// </summary>
        public const string MessagingSystemValue = "azureservicebus";

        /// <summary>
        /// The operation value for message processing.
        /// </summary>
        public const string MessagingOperationProcess = "process";

        /// <summary>
        /// The entity type value for queues.
        /// </summary>
        public const string EntityTypeQueue = "queue";

        /// <summary>
        /// The entity type value for topics.
        /// </summary>
        public const string EntityTypeTopic = "topic";
    }
}