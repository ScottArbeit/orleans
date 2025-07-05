using Azure;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using System;
using System.Threading.Tasks;

namespace Orleans.Configuration
{
    /// <summary>
    /// Options for configuring Azure Service Bus streaming provider.
    /// </summary>
    public class AzureServiceBusOptions
    {
        private ServiceBusClient _serviceBusClient;

        /// <summary>
        /// Gets or sets the Service Bus topology type (Queue or Topic).
        /// </summary>
        public ServiceBusTopologyType TopologyType { get; set; } = ServiceBusTopologyType.Queue;

        /// <summary>
        /// Gets or sets the Service Bus queue name when using Queue topology.
        /// </summary>
        public string QueueName { get; set; }

        /// <summary>
        /// Gets or sets the Service Bus topic name when using Topic topology.
        /// </summary>
        public string TopicName { get; set; }

        /// <summary>
        /// Gets or sets the Service Bus subscription name when using Topic topology.
        /// </summary>
        public string SubscriptionName { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of concurrent calls to process messages.
        /// Default is 1.
        /// </summary>
        public int MaxConcurrentCalls { get; set; } = 1;

        /// <summary>
        /// Gets or sets the maximum wait time for receiving messages.
        /// Default is 1 minute.
        /// </summary>
        public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Gets or sets the prefetch count for the message receiver.
        /// Default is 0 (no prefetch).
        /// </summary>
        public int PrefetchCount { get; set; } = 0;

        /// <summary>
        /// Gets or sets whether to auto-complete messages after processing.
        /// Default is true.
        /// </summary>
        public bool AutoCompleteMessages { get; set; } = true;

        /// <summary>
        /// Gets or sets the maximum delivery count before a message is dead-lettered.
        /// This is only applicable when creating new queues/subscriptions.
        /// </summary>
        public int? MaxDeliveryCount { get; set; }

        /// <summary>
        /// Gets or sets the lock duration for messages.
        /// This is only applicable when creating new queues/subscriptions.
        /// </summary>
        public TimeSpan? LockDuration { get; set; }

        /// <summary>
        /// Gets or sets the time-to-live for messages.
        /// This is only applicable when creating new queues/subscriptions.
        /// </summary>
        public TimeSpan? DefaultMessageTimeToLive { get; set; }

        /// <summary>
        /// Gets or sets whether to enable duplicate detection.
        /// This is only applicable when creating new queues/topics.
        /// </summary>
        public bool? RequiresDuplicateDetection { get; set; }

        /// <summary>
        /// Gets or sets the duration of duplicate detection history.
        /// This is only applicable when creating new queues/topics with duplicate detection enabled.
        /// </summary>
        public TimeSpan? DuplicateDetectionHistoryTimeWindow { get; set; }

        /// <summary>
        /// Gets or sets whether sessions are enabled.
        /// This is only applicable when creating new queues/subscriptions.
        /// </summary>
        public bool? RequiresSession { get; set; }

        /// <summary>
        /// Gets or sets whether dead lettering is enabled on message expiration.
        /// This is only applicable when creating new queues/subscriptions.
        /// </summary>
        public bool? DeadLetteringOnMessageExpiration { get; set; }

        /// <summary>
        /// Gets or sets the optional delegate used to create a <see cref="ServiceBusClient"/> instance.
        /// </summary>
        internal Func<Task<ServiceBusClient>> CreateClient { get; private set; }

        /// <summary>
        /// Gets or sets the client used to access the Azure Service Bus.
        /// </summary>
        public ServiceBusClient ServiceBusClient
        {
            get => _serviceBusClient;
            set
            {
                _serviceBusClient = value;
                CreateClient = () => Task.FromResult(value);
            }
        }

        /// <summary>
        /// Configures the <see cref="ServiceBusClient"/> using a connection string.
        /// </summary>
        /// <param name="connectionString">The connection string to the Service Bus namespace.</param>
        /// <param name="options">Optional client options.</param>
        public void ConfigureServiceBusClient(string connectionString, ServiceBusClientOptions options = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("A non-null, non-empty value must be provided.", nameof(connectionString));
            }

            ServiceBusClient = new ServiceBusClient(connectionString, options);
        }

        /// <summary>
        /// Configures the <see cref="ServiceBusClient"/> using a fully qualified namespace and token credential.
        /// </summary>
        /// <param name="fullyQualifiedNamespace">The fully qualified Service Bus namespace.</param>
        /// <param name="credential">The token credential to use for authentication.</param>
        /// <param name="options">Optional client options.</param>
        public void ConfigureServiceBusClient(string fullyQualifiedNamespace, TokenCredential credential, ServiceBusClientOptions options = null)
        {
            if (string.IsNullOrWhiteSpace(fullyQualifiedNamespace))
            {
                throw new ArgumentException("A non-null, non-empty value must be provided.", nameof(fullyQualifiedNamespace));
            }

            if (credential is null)
            {
                throw new ArgumentNullException(nameof(credential));
            }

            ServiceBusClient = new ServiceBusClient(fullyQualifiedNamespace, credential, options);
        }

        /// <summary>
        /// Configures the <see cref="ServiceBusClient"/> using a fully qualified namespace and Azure SAS credential.
        /// </summary>
        /// <param name="fullyQualifiedNamespace">The fully qualified Service Bus namespace.</param>
        /// <param name="credential">The Azure SAS credential to use for authentication.</param>
        /// <param name="options">Optional client options.</param>
        public void ConfigureServiceBusClient(string fullyQualifiedNamespace, AzureSasCredential credential, ServiceBusClientOptions options = null)
        {
            if (string.IsNullOrWhiteSpace(fullyQualifiedNamespace))
            {
                throw new ArgumentException("A non-null, non-empty value must be provided.", nameof(fullyQualifiedNamespace));
            }

            if (credential is null)
            {
                throw new ArgumentNullException(nameof(credential));
            }

            ServiceBusClient = new ServiceBusClient(fullyQualifiedNamespace, credential, options);
        }

        /// <summary>
        /// Configures the <see cref="ServiceBusClient"/> using a fully qualified namespace and Azure named key credential.
        /// </summary>
        /// <param name="fullyQualifiedNamespace">The fully qualified Service Bus namespace.</param>
        /// <param name="credential">The Azure named key credential to use for authentication.</param>
        /// <param name="options">Optional client options.</param>
        public void ConfigureServiceBusClient(string fullyQualifiedNamespace, AzureNamedKeyCredential credential, ServiceBusClientOptions options = null)
        {
            if (string.IsNullOrWhiteSpace(fullyQualifiedNamespace))
            {
                throw new ArgumentException("A non-null, non-empty value must be provided.", nameof(fullyQualifiedNamespace));
            }

            if (credential is null)
            {
                throw new ArgumentNullException(nameof(credential));
            }

            ServiceBusClient = new ServiceBusClient(fullyQualifiedNamespace, credential, options);
        }

        /// <summary>
        /// Configures the <see cref="ServiceBusClient"/> using the provided callback.
        /// </summary>
        /// <param name="createClientCallback">The callback to create the Service Bus client.</param>
        public void ConfigureServiceBusClient(Func<Task<ServiceBusClient>> createClientCallback)
        {
            CreateClient = createClientCallback ?? throw new ArgumentNullException(nameof(createClientCallback));
        }
    }
}