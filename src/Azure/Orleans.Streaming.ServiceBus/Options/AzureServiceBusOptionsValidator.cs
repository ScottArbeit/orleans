using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using System;

namespace Orleans.Configuration
{
    /// <summary>
    /// Configuration validator for <see cref="AzureServiceBusOptions"/>.
    /// </summary>
    public class AzureServiceBusOptionsValidator : IConfigurationValidator
    {
        private readonly AzureServiceBusOptions options;
        private readonly string name;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusOptionsValidator"/> class.
        /// </summary>
        /// <param name="options">The options to validate.</param>
        /// <param name="name">The name of the options instance.</param>
        public AzureServiceBusOptionsValidator(AzureServiceBusOptions options, string name)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));
            this.name = name ?? string.Empty;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusOptionsValidator"/> class.
        /// </summary>
        /// <param name="options">The options to validate.</param>
        public AzureServiceBusOptionsValidator(IOptions<AzureServiceBusOptions> options)
            : this(options?.Value, string.Empty)
        {
        }

        /// <summary>
        /// Creates a validator for a named instance of <see cref="AzureServiceBusOptions"/>.
        /// </summary>
        /// <param name="services">The service provider.</param>
        /// <param name="name">The name of the options instance.</param>
        /// <returns>A configuration validator.</returns>
        public static IConfigurationValidator Create(IServiceProvider services, string name)
        {
            var options = services.GetOptionsByName<AzureServiceBusOptions>(name);
            return new AzureServiceBusOptionsValidator(options, name);
        }

        /// <inheritdoc />
        public void ValidateConfiguration()
        {
            // Validate name
            if (name is not null && string.IsNullOrWhiteSpace(name))
            {
                throw new OrleansConfigurationException($"Named option {nameof(AzureServiceBusOptions)} of name '{name}' is invalid. Name cannot be empty or whitespace.");
            }

            // Validate Service Bus client configuration
            if (options.CreateClient is null)
            {
                throw new OrleansConfigurationException($"No credentials specified for Azure Service Bus provider \"{name}\". Use the {options.GetType().Name}.{nameof(AzureServiceBusOptions.ConfigureServiceBusClient)} method to configure the Azure Service Bus client.");
            }

            // Validate topology-specific configuration
            switch (options.TopologyType)
            {
                case ServiceBusTopologyType.Queue:
                    ValidateQueueConfiguration();
                    break;
                case ServiceBusTopologyType.Topic:
                    ValidateTopicConfiguration();
                    break;
                default:
                    throw new OrleansConfigurationException($"Invalid {nameof(ServiceBusTopologyType)} '{options.TopologyType}' for Azure Service Bus provider \"{name}\".");
            }

            // Validate general configuration
            ValidateGeneralConfiguration();
        }

        private void ValidateQueueConfiguration()
        {
            if (string.IsNullOrWhiteSpace(options.QueueName))
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.QueueName)} must be specified when using {nameof(ServiceBusTopologyType.Queue)} topology.");
            }

            if (!string.IsNullOrWhiteSpace(options.TopicName))
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.TopicName)} should not be specified when using {nameof(ServiceBusTopologyType.Queue)} topology.");
            }

            if (!string.IsNullOrWhiteSpace(options.SubscriptionName))
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.SubscriptionName)} should not be specified when using {nameof(ServiceBusTopologyType.Queue)} topology.");
            }
        }

        private void ValidateTopicConfiguration()
        {
            if (string.IsNullOrWhiteSpace(options.TopicName))
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.TopicName)} must be specified when using {nameof(ServiceBusTopologyType.Topic)} topology.");
            }

            if (string.IsNullOrWhiteSpace(options.SubscriptionName))
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.SubscriptionName)} must be specified when using {nameof(ServiceBusTopologyType.Topic)} topology.");
            }

            if (!string.IsNullOrWhiteSpace(options.QueueName))
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.QueueName)} should not be specified when using {nameof(ServiceBusTopologyType.Topic)} topology.");
            }
        }

        private void ValidateGeneralConfiguration()
        {
            if (options.MaxConcurrentCalls <= 0)
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.MaxConcurrentCalls)} must be greater than 0.");
            }

            if (options.MaxWaitTime <= TimeSpan.Zero)
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.MaxWaitTime)} must be greater than zero.");
            }

            if (options.PrefetchCount < 0)
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.PrefetchCount)} must be greater than or equal to 0.");
            }

            // Validate optional properties if they are set
            if (options.MaxDeliveryCount.HasValue && options.MaxDeliveryCount.Value <= 0)
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.MaxDeliveryCount)} must be greater than 0 when specified.");
            }

            if (options.LockDuration.HasValue && options.LockDuration.Value <= TimeSpan.Zero)
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.LockDuration)} must be greater than zero when specified.");
            }

            if (options.DefaultMessageTimeToLive.HasValue && options.DefaultMessageTimeToLive.Value <= TimeSpan.Zero)
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.DefaultMessageTimeToLive)} must be greater than zero when specified.");
            }

            if (options.DuplicateDetectionHistoryTimeWindow.HasValue && options.DuplicateDetectionHistoryTimeWindow.Value <= TimeSpan.Zero)
            {
                throw new OrleansConfigurationException($"{nameof(AzureServiceBusOptions)} for provider \"{name}\" is invalid. {nameof(options.DuplicateDetectionHistoryTimeWindow)} must be greater than zero when specified.");
            }
        }
    }
}