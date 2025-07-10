using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using Orleans.Runtime;
using Orleans.Streaming;

namespace Orleans.Configuration;

/// <summary>
/// Defines the type of Service Bus entity to use.
/// </summary>
public enum ServiceBusEntityType
{
    /// <summary>
    /// Use Service Bus queues.
    /// </summary>
    Queue,
    
    /// <summary>
    /// Use Service Bus topics.
    /// </summary>
    Topic
}

/// <summary>
/// Configuration options for Azure Service Bus streaming provider.
/// </summary>
public class ServiceBusOptions
{
    private ServiceBusClient? _serviceBusClient;

    /// <summary>
    /// Gets or sets the Azure Service Bus connection string.
    /// </summary>
    [Redact]
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Gets or sets the fully qualified namespace for the Service Bus.
    /// Used with TokenCredential for authentication.
    /// </summary>
    public string? FullyQualifiedNamespace { get; set; }

    /// <summary>
    /// Gets or sets the token credential for authenticating with Service Bus.
    /// Used with FullyQualifiedNamespace.
    /// </summary>
    public TokenCredential? TokenCredential { get; set; }

    /// <summary>
    /// Gets or sets the type of Service Bus entity to use (Queue or Topic).
    /// </summary>
    public ServiceBusEntityType EntityType { get; set; } = ServiceBusEntityType.Queue;

    /// <summary>
    /// Gets or sets the specific entity name to use.
    /// If not specified, entity names will be generated based on QueueNamePrefix and PartitionCount.
    /// </summary>
    public string? EntityName { get; set; }

    /// <summary>
    /// Gets or sets the list of entity names to use for partitioning.
    /// If not specified, entity names will be generated based on QueueNamePrefix and PartitionCount.
    /// </summary>
    public List<string>? EntityNames { get; set; }

    /// <summary>
    /// Gets or sets the subscription name to use when EntityType is Topic.
    /// If not specified, subscription names will be generated based on SubscriptionNamePrefix and PartitionCount.
    /// </summary>
    public string? SubscriptionName { get; set; }

    /// <summary>
    /// Gets or sets the list of subscription names to use for partitioning when EntityType is Topic.
    /// If not specified, subscription names will be generated based on SubscriptionNamePrefix and PartitionCount.
    /// </summary>
    public List<string>? SubscriptionNames { get; set; }

    /// <summary>
    /// Gets or sets the prefix for subscription names when auto-generating subscription names for topics.
    /// </summary>
    public string SubscriptionNamePrefix { get; set; } = "orleans-subscription";

    /// <summary>
    /// Gets or sets the number of messages to prefetch from the Service Bus.
    /// </summary>
    public int PrefetchCount { get; set; } = 0;

    /// <summary>
    /// Gets or sets the maximum number of concurrent calls for message processing.
    /// </summary>
    public int MaxConcurrentCalls { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Gets or sets the maximum number of delivery attempts for a message.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 10;

    /// <summary>
    /// Gets or sets the number of queues/topics to use for partitioning.
    /// </summary>
    public int PartitionCount { get; set; } = 8;

    /// <summary>
    /// Gets or sets the maximum time to wait for messages when receiving.
    /// </summary>
    public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the prefix for queue/topic names when auto-generating entity names.
    /// </summary>
    public string QueueNamePrefix { get; set; } = "orleans-stream";

    /// <summary>
    /// Gets or sets the ServiceBusClient used to access Azure Service Bus.
    /// </summary>
    public ServiceBusClient? ServiceBusClient
    {
        get => _serviceBusClient;
        set
        {
            _serviceBusClient = value;
            CreateClient = value is not null ? () => Task.FromResult(value) : null;
        }
    }

    /// <summary>
    /// The optional delegate used to create a ServiceBusClient instance.
    /// </summary>
    internal Func<Task<ServiceBusClient>>? CreateClient { get; private set; }

    /// <summary>
    /// Gets the ActivitySource for Service Bus telemetry.
    /// </summary>
    public static ActivitySource ActivitySource { get; } = new("Orleans.ServiceBus");

    /// <summary>
    /// Configures the ServiceBusClient using a connection string.
    /// </summary>
    /// <param name="connectionString">The Service Bus connection string.</param>
    /// <param name="options">Optional Service Bus client options.</param>
    public void ConfigureServiceBusClient(string connectionString, ServiceBusClientOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) 
            throw new ArgumentNullException(nameof(connectionString));
        
        ConnectionString = connectionString;
        CreateClient = () => Task.FromResult(new ServiceBusClient(connectionString, options));
    }

    /// <summary>
    /// Configures the ServiceBusClient using a fully qualified namespace and token credential.
    /// </summary>
    /// <param name="fullyQualifiedNamespace">The fully qualified Service Bus namespace.</param>
    /// <param name="credential">The token credential for authentication.</param>
    /// <param name="options">Optional Service Bus client options.</param>
    public void ConfigureServiceBusClient(string fullyQualifiedNamespace, TokenCredential credential, ServiceBusClientOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(fullyQualifiedNamespace)) 
            throw new ArgumentNullException(nameof(fullyQualifiedNamespace));
        if (credential is null) 
            throw new ArgumentNullException(nameof(credential));

        FullyQualifiedNamespace = fullyQualifiedNamespace;
        TokenCredential = credential;
        CreateClient = () => Task.FromResult(new ServiceBusClient(fullyQualifiedNamespace, credential, options));
    }

    /// <summary>
    /// Configures the ServiceBusClient using a custom factory function.
    /// </summary>
    /// <param name="createClientCallback">The callback to create the ServiceBusClient.</param>
    public void ConfigureServiceBusClient(Func<Task<ServiceBusClient>> createClientCallback)
    {
        CreateClient = createClientCallback ?? throw new ArgumentNullException(nameof(createClientCallback));
    }
}

/// <summary>
/// Validates ServiceBusOptions configuration.
/// </summary>
public class ServiceBusOptionsValidator : IConfigurationValidator
{
    private readonly ServiceBusOptions options;
    private readonly string name;

    private ServiceBusOptionsValidator(ServiceBusOptions options, string name)
    {
        this.options = options;
        this.name = name;
    }

    /// <summary>
    /// Validates the configuration.
    /// </summary>
    public void ValidateConfiguration()
    {
        if (options.CreateClient is null && options.ServiceBusClient is null)
        {
            throw new OrleansConfigurationException(
                $"No credentials specified for ServiceBus stream provider '{name}'. " +
                $"Use the {nameof(ServiceBusOptions)}.{nameof(ServiceBusOptions.ConfigureServiceBusClient)} method to configure the Azure Service Bus client.");
        }

        if (options.PartitionCount <= 0)
        {
            throw new OrleansConfigurationException(
                $"{nameof(ServiceBusOptions.PartitionCount)} on ServiceBus stream provider '{name}' must be greater than 0.");
        }

        if (options.PrefetchCount < 0)
        {
            throw new OrleansConfigurationException(
                $"{nameof(ServiceBusOptions.PrefetchCount)} on ServiceBus stream provider '{name}' must be greater than or equal to 0.");
        }

        if (options.MaxConcurrentCalls <= 0)
        {
            throw new OrleansConfigurationException(
                $"{nameof(ServiceBusOptions.MaxConcurrentCalls)} on ServiceBus stream provider '{name}' must be greater than 0.");
        }

        if (options.MaxDeliveryAttempts <= 0)
        {
            throw new OrleansConfigurationException(
                $"{nameof(ServiceBusOptions.MaxDeliveryAttempts)} on ServiceBus stream provider '{name}' must be greater than 0.");
        }

        if (string.IsNullOrWhiteSpace(options.QueueNamePrefix))
        {
            throw new OrleansConfigurationException(
                $"{nameof(ServiceBusOptions.QueueNamePrefix)} on ServiceBus stream provider '{name}' cannot be null or empty.");
        }

        // If EntityNames is specified, it should have at least one entry
        if (options.EntityNames is not null && options.EntityNames.Count == 0)
        {
            throw new OrleansConfigurationException(
                $"{nameof(ServiceBusOptions.EntityNames)} on ServiceBus stream provider '{name}' cannot be empty when specified.");
        }

        // Validate specific entity name if provided
        if (!string.IsNullOrWhiteSpace(options.EntityName) && options.EntityNames is not null && options.EntityNames.Count > 0)
        {
            throw new OrleansConfigurationException(
                $"Cannot specify both {nameof(ServiceBusOptions.EntityName)} and {nameof(ServiceBusOptions.EntityNames)} on ServiceBus stream provider '{name}'.");
        }

        // Validate subscription name configuration for topics
        if (options.EntityType == ServiceBusEntityType.Topic)
        {
            if (!string.IsNullOrWhiteSpace(options.SubscriptionName) && options.SubscriptionNames is not null && options.SubscriptionNames.Count > 0)
            {
                throw new OrleansConfigurationException(
                    $"Cannot specify both {nameof(ServiceBusOptions.SubscriptionName)} and {nameof(ServiceBusOptions.SubscriptionNames)} on ServiceBus stream provider '{name}'.");
            }

            if (string.IsNullOrWhiteSpace(options.SubscriptionNamePrefix))
            {
                throw new OrleansConfigurationException(
                    $"{nameof(ServiceBusOptions.SubscriptionNamePrefix)} on ServiceBus stream provider '{name}' cannot be null or empty when using topics.");
            }

            // If SubscriptionNames is specified, it should have at least one entry
            if (options.SubscriptionNames is not null && options.SubscriptionNames.Count == 0)
            {
                throw new OrleansConfigurationException(
                    $"{nameof(ServiceBusOptions.SubscriptionNames)} on ServiceBus stream provider '{name}' cannot be empty when specified.");
            }
        }
    }

    /// <summary>
    /// Creates a validator instance.
    /// </summary>
    public static IConfigurationValidator Create(IServiceProvider services, string name)
    {
        var serviceBusOptions = services.GetOptionsByName<ServiceBusOptions>(name);
        return new ServiceBusOptionsValidator(serviceBusOptions, name);
    }
}