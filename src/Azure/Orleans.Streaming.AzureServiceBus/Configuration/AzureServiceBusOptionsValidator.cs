using System;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Configuration;

/// <summary>
/// Validates <see cref="AzureServiceBusOptions"/> configuration.
/// </summary>
public class AzureServiceBusOptionsValidator : IConfigurationValidator
{
    private readonly AzureServiceBusOptions _options;
    private readonly string _name;

    /// <summary>
    /// Initializes a new instance of the <see cref="AzureServiceBusOptionsValidator"/> class.
    /// </summary>
    /// <param name="options">The options to validate.</param>
    /// <param name="name">The name of the configuration.</param>
    public AzureServiceBusOptionsValidator(AzureServiceBusOptions options, string name)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _name = name;
    }

    /// <summary>
    /// Creates a new validator instance using the service provider.
    /// </summary>
    /// <param name="services">The service provider.</param>
    /// <param name="name">The name of the configuration.</param>
    /// <returns>A new configuration validator instance.</returns>
    public static IConfigurationValidator Create(IServiceProvider services, string name)
    {
        var options = services.GetOptionsByName<AzureServiceBusOptions>(name);
        return new AzureServiceBusOptionsValidator(options, name);
    }

    /// <inheritdoc />
    public void ValidateConfiguration()
    {
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            throw new OrleansConfigurationException(
                $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                $"{nameof(AzureServiceBusOptions.ConnectionString)} is required and cannot be null or empty.");
        }

        if (string.IsNullOrWhiteSpace(_options.EntityName))
        {
            throw new OrleansConfigurationException(
                $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                $"{nameof(AzureServiceBusOptions.EntityName)} is required and cannot be null or empty.");
        }

        if (_options.EntityMode == ServiceBusEntityMode.Topic && string.IsNullOrWhiteSpace(_options.SubscriptionName))
        {
            throw new OrleansConfigurationException(
                $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                $"{nameof(AzureServiceBusOptions.SubscriptionName)} is required when {nameof(AzureServiceBusOptions.EntityMode)} is Topic.");
        }

        if (_options.BatchSize <= 0 || _options.BatchSize > 1000)
        {
            throw new OrleansConfigurationException(
                $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                $"{nameof(AzureServiceBusOptions.BatchSize)} must be between 1 and 1000. Current value: {_options.BatchSize}");
        }

        if (_options.PrefetchCount < 0 || _options.PrefetchCount > 1000)
        {
            throw new OrleansConfigurationException(
                $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                $"{nameof(AzureServiceBusOptions.PrefetchCount)} must be between 0 and 1000. Current value: {_options.PrefetchCount}");
        }

        if (_options.MaxConcurrentCalls <= 0)
        {
            throw new OrleansConfigurationException(
                $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                $"{nameof(AzureServiceBusOptions.MaxConcurrentCalls)} must be greater than 0. Current value: {_options.MaxConcurrentCalls}");
        }

        if (_options.OperationTimeout <= TimeSpan.Zero)
        {
            throw new OrleansConfigurationException(
                $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                $"{nameof(AzureServiceBusOptions.OperationTimeout)} must be positive. Current value: {_options.OperationTimeout}");
        }

        if (_options.MaxAutoLockRenewalDuration <= TimeSpan.Zero)
        {
            throw new OrleansConfigurationException(
                $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                $"{nameof(AzureServiceBusOptions.MaxAutoLockRenewalDuration)} must be positive. Current value: {_options.MaxAutoLockRenewalDuration}");
        }

        ValidateConnectionString(_options.ConnectionString);
    }

    private void ValidateConnectionString(string connectionString)
    {
        try
        {
            // Split the connection string into parts
            var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);
            
            // Look for an endpoint part that starts with "Endpoint="
            var endpointPart = parts.FirstOrDefault(p => p.StartsWith("Endpoint=", StringComparison.OrdinalIgnoreCase));
            
            if (endpointPart == null)
            {
                throw new OrleansConfigurationException(
                    $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                    "Connection string must contain 'Endpoint='");
            }

            // Extract the endpoint value and ensure it's not empty
            var endpointValue = endpointPart.Substring("Endpoint=".Length);
            if (string.IsNullOrWhiteSpace(endpointValue))
            {
                throw new OrleansConfigurationException(
                    $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                    "Connection string Endpoint value cannot be empty");
            }

            // Check for either SharedAccessKey or managed identity
            bool hasSharedAccessKey = parts.Any(p => p.StartsWith("SharedAccessKeyName=", StringComparison.OrdinalIgnoreCase)) &&
                                     parts.Any(p => p.StartsWith("SharedAccessKey=", StringComparison.OrdinalIgnoreCase));
            bool hasSharedAccessSignature = parts.Any(p => p.StartsWith("SharedAccessSignature=", StringComparison.OrdinalIgnoreCase));

            if (!hasSharedAccessKey && !hasSharedAccessSignature)
            {
                // Allow for managed identity scenarios where no key is provided
                // The actual Azure SDK will validate this at runtime
            }
        }
        catch (Exception ex) when (!(ex is OrleansConfigurationException))
        {
            throw new OrleansConfigurationException(
                $"{nameof(AzureServiceBusOptions)} on stream provider \"{_name}\" is invalid. " +
                $"Connection string format is invalid: {ex.Message}");
        }
    }
}