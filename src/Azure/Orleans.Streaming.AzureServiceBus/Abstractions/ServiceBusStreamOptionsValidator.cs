using System;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Validates instances of <see cref="ServiceBusStreamOptions"/>.
/// </summary>
public class ServiceBusStreamOptionsValidator : IValidateOptions<ServiceBusStreamOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, ServiceBusStreamOptions options)
    {
        // Validate connection configuration
        if (string.IsNullOrWhiteSpace(options.ConnectionString) && string.IsNullOrWhiteSpace(options.FullyQualifiedNamespace))
        {
            return ValidateOptionsResult.Fail(
                "Either ConnectionString or FullyQualifiedNamespace must be provided for Azure Service Bus streaming provider.");
        }

        if (!string.IsNullOrWhiteSpace(options.ConnectionString) && !string.IsNullOrWhiteSpace(options.FullyQualifiedNamespace))
        {
            return ValidateOptionsResult.Fail(
                "Only one of ConnectionString or FullyQualifiedNamespace should be provided, not both.");
        }

        if (!string.IsNullOrWhiteSpace(options.FullyQualifiedNamespace) && options.Credential is null)
        {
            return ValidateOptionsResult.Fail(
                "Credential must be provided when using FullyQualifiedNamespace for Azure Service Bus streaming provider.");
        }

        // Validate entity configuration
        switch (options.EntityKind)
        {
            case EntityKind.Queue:
                if (options.EntityCount == 1)
                {
                    if (string.IsNullOrWhiteSpace(options.QueueName))
                    {
                        return ValidateOptionsResult.Fail(
                            "QueueName must be provided when EntityKind is Queue and EntityCount is 1.");
                    }
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(options.EntityNamePrefix))
                    {
                        return ValidateOptionsResult.Fail(
                            "EntityNamePrefix must be provided when EntityCount > 1.");
                    }
                }
                break;

            case EntityKind.TopicSubscription:
                if (string.IsNullOrWhiteSpace(options.TopicName))
                {
                    return ValidateOptionsResult.Fail(
                        "TopicName must be provided when EntityKind is TopicSubscription.");
                }
                
                if (options.EntityCount == 1)
                {
                    if (string.IsNullOrWhiteSpace(options.SubscriptionName))
                    {
                        return ValidateOptionsResult.Fail(
                            "SubscriptionName must be provided when EntityKind is TopicSubscription and EntityCount is 1.");
                    }
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(options.EntityNamePrefix))
                    {
                        return ValidateOptionsResult.Fail(
                            "EntityNamePrefix must be provided when EntityCount > 1.");
                    }
                }
                break;

            default:
                return ValidateOptionsResult.Fail($"Invalid EntityKind: {options.EntityKind}");
        }

        // Validate EntityCount
        if (options.EntityCount <= 0)
        {
            return ValidateOptionsResult.Fail(
                "EntityCount must be greater than zero.");
        }

        // Validate publisher settings
        if (options.Publisher.BatchSize <= 0)
        {
            return ValidateOptionsResult.Fail(
                "Publisher.BatchSize must be greater than zero.");
        }

        if (options.Publisher.MessageTimeToLive <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                "Publisher.MessageTimeToLive must be greater than zero.");
        }

        // Validate receiver settings
        if (options.Receiver.PrefetchCount < 0)
        {
            return ValidateOptionsResult.Fail(
                "Receiver.PrefetchCount must be greater than or equal to zero.");
        }

        if (options.Receiver.ReceiveBatchSize <= 0)
        {
            return ValidateOptionsResult.Fail(
                "Receiver.ReceiveBatchSize must be greater than zero.");
        }

        if (options.Receiver.MaxConcurrentHandlers <= 0)
        {
            return ValidateOptionsResult.Fail(
                "Receiver.MaxConcurrentHandlers must be greater than zero.");
        }

        // Note: We allow MaxConcurrentHandlers > 1 but will emit warnings at runtime
        // since increasing concurrency breaks message ordering guarantees

        if (options.Receiver.LockRenewalDuration <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                "Receiver.LockRenewalDuration must be greater than zero.");
        }

        if (options.Receiver.MaxDeliveryCount <= 0)
        {
            return ValidateOptionsResult.Fail(
                "Receiver.MaxDeliveryCount must be greater than zero.");
        }

        // Validate cache settings
        if (options.Cache.MaxCacheSize <= 0)
        {
            return ValidateOptionsResult.Fail(
                "Cache.MaxCacheSize must be greater than zero.");
        }

        if (options.Cache.CacheEvictionAge <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                "Cache.CacheEvictionAge must be greater than zero.");
        }

        if (options.Cache.CachePressureSoft <= 0.0 || options.Cache.CachePressureSoft >= 1.0)
        {
            return ValidateOptionsResult.Fail(
                "Cache.CachePressureSoft must be between 0.0 and 1.0 (exclusive).");
        }

        if (options.Cache.CachePressureHard <= 0.0 || options.Cache.CachePressureHard >= 1.0)
        {
            return ValidateOptionsResult.Fail(
                "Cache.CachePressureHard must be between 0.0 and 1.0 (exclusive).");
        }

        if (options.Cache.CachePressureSoft >= options.Cache.CachePressureHard)
        {
            return ValidateOptionsResult.Fail(
                "Cache.CachePressureSoft must be less than Cache.CachePressureHard.");
        }

        // Validate entity count and naming
        if (options.EntityCount <= 0)
        {
            return ValidateOptionsResult.Fail(
                "EntityCount must be greater than zero.");
        }

        return ValidateOptionsResult.Success;
    }
}