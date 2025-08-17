using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Handles auto-provisioning of Service Bus entities (queues, topics, subscriptions) when enabled.
/// </summary>
internal class ServiceBusProvisioner
{
    private readonly ServiceBusAdministrationClient _adminClient;
    private readonly ILogger<ServiceBusProvisioner> _logger;

    /// <summary>
    /// Initializes a new instance of the ServiceBusProvisioner.
    /// </summary>
    /// <param name="adminClient">The Service Bus administration client.</param>
    /// <param name="logger">The logger.</param>
    public ServiceBusProvisioner(ServiceBusAdministrationClient adminClient, ILogger<ServiceBusProvisioner> logger)
    {
        _adminClient = adminClient ?? throw new ArgumentNullException(nameof(adminClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Provisions the Service Bus entities based on the configured options.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <returns>A task that represents the asynchronous provision operation.</returns>
    public async Task ProvisionEntitiesAsync(ServiceBusStreamOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (!options.AutoCreateEntities)
        {
            _logger.LogDebug("Auto-create entities is disabled, skipping provisioning");
            return;
        }

        try
        {
            switch (options.EntityKind)
            {
                case EntityKind.Queue:
                    await ProvisionQueuesAsync(options);
                    break;
                case EntityKind.TopicSubscription:
                    await ProvisionTopicSubscriptionsAsync(options);
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported entity kind: {options.EntityKind}");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to provision Service Bus entities");
            throw;
        }
    }

    /// <summary>
    /// Provisions queues based on the configured options.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    private async Task ProvisionQueuesAsync(ServiceBusStreamOptions options)
    {
        for (int i = 0; i < options.EntityCount; i++)
        {
            string queueName = ServiceBusEntityNamer.GetEntityName(options, i);
            await CreateQueueIfNotExistsAsync(queueName, options);
        }
    }

    /// <summary>
    /// Provisions topics and subscriptions based on the configured options.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    private async Task ProvisionTopicSubscriptionsAsync(ServiceBusStreamOptions options)
    {
        if (options.EntityCount == 1)
        {
            // Single entity: create one topic and one subscription
            await CreateTopicIfNotExistsAsync(options.TopicName, options);
            await CreateSubscriptionIfNotExistsAsync(options.TopicName, options.SubscriptionName, options);
        }
        else
        {
            // Multiple entities: create one topic with multiple subscriptions
            await CreateTopicIfNotExistsAsync(options.TopicName, options);
            
            for (int i = 0; i < options.EntityCount; i++)
            {
                string entityName = ServiceBusEntityNamer.GetEntityName(options, i);
                // Extract subscription name from "topic:subscription" format
                var parts = entityName.Split(':');
                if (parts.Length < 2)
                {
                    _logger.LogError("Invalid entity name format: '{EntityName}'. Expected format 'topic:subscription'. Skipping.", entityName);
                    continue;
                }
                string subscriptionName = parts[1];
                await CreateSubscriptionIfNotExistsAsync(options.TopicName, subscriptionName, options);
            }
        }
    }

    /// <summary>
    /// Creates a queue if it doesn't already exist.
    /// </summary>
    /// <param name="queueName">The name of the queue to create.</param>
    /// <param name="options">The Service Bus streaming options.</param>
    private async Task CreateQueueIfNotExistsAsync(string queueName, ServiceBusStreamOptions options)
    {
        try
        {
            bool exists = await _adminClient.QueueExistsAsync(queueName);
            if (exists)
            {
                _logger.LogDebug("Queue '{QueueName}' already exists", queueName);
                return;
            }

            var queueOptions = new CreateQueueOptions(queueName)
            {
                MaxDeliveryCount = options.Receiver.MaxDeliveryCount,
                DeadLetteringOnMessageExpiration = true,
                DefaultMessageTimeToLive = options.Publisher.MessageTimeToLive
            };

            await _adminClient.CreateQueueAsync(queueOptions);
            _logger.LogInformation("Successfully created queue '{QueueName}' with MaxDeliveryCount={MaxDeliveryCount}", 
                queueName, options.Receiver.MaxDeliveryCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create queue '{QueueName}'", queueName);
            throw;
        }
    }

    /// <summary>
    /// Creates a topic if it doesn't already exist.
    /// </summary>
    /// <param name="topicName">The name of the topic to create.</param>
    /// <param name="options">The Service Bus streaming options.</param>
    private async Task CreateTopicIfNotExistsAsync(string topicName, ServiceBusStreamOptions options)
    {
        try
        {
            bool exists = await _adminClient.TopicExistsAsync(topicName);
            if (exists)
            {
                _logger.LogDebug("Topic '{TopicName}' already exists", topicName);
                return;
            }

            var topicOptions = new CreateTopicOptions(topicName)
            {
                DefaultMessageTimeToLive = options.Publisher.MessageTimeToLive
            };

            await _adminClient.CreateTopicAsync(topicOptions);
            _logger.LogInformation("Successfully created topic '{TopicName}'", topicName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create topic '{TopicName}'", topicName);
            throw;
        }
    }

    /// <summary>
    /// Creates a subscription if it doesn't already exist.
    /// </summary>
    /// <param name="topicName">The name of the topic.</param>
    /// <param name="subscriptionName">The name of the subscription to create.</param>
    /// <param name="options">The Service Bus streaming options.</param>
    private async Task CreateSubscriptionIfNotExistsAsync(string topicName, string subscriptionName, ServiceBusStreamOptions options)
    {
        try
        {
            bool exists = await _adminClient.SubscriptionExistsAsync(topicName, subscriptionName);
            if (exists)
            {
                _logger.LogDebug("Subscription '{SubscriptionName}' on topic '{TopicName}' already exists", 
                    subscriptionName, topicName);
                return;
            }

            var subscriptionOptions = new CreateSubscriptionOptions(topicName, subscriptionName)
            {
                MaxDeliveryCount = options.Receiver.MaxDeliveryCount,
                DeadLetteringOnMessageExpiration = true,
                DefaultMessageTimeToLive = options.Publisher.MessageTimeToLive
            };

            await _adminClient.CreateSubscriptionAsync(subscriptionOptions);
            _logger.LogInformation("Successfully created subscription '{SubscriptionName}' on topic '{TopicName}' with MaxDeliveryCount={MaxDeliveryCount}", 
                subscriptionName, topicName, options.Receiver.MaxDeliveryCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create subscription '{SubscriptionName}' on topic '{TopicName}'", 
                subscriptionName, topicName);
            throw;
        }
    }
}