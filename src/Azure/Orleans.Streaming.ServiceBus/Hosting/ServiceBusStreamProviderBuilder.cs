using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Providers;

[assembly: RegisterProvider("ServiceBus", "Streaming", "Silo", typeof(Orleans.Hosting.ServiceBusStreamProviderBuilder))]
[assembly: RegisterProvider("ServiceBus", "Streaming", "Client", typeof(Orleans.Hosting.ServiceBusStreamProviderBuilder))]

namespace Orleans.Hosting;

public sealed class ServiceBusStreamProviderBuilder : IProviderBuilder<ISiloBuilder>, IProviderBuilder<IClientBuilder>
{
    public void Configure(ISiloBuilder builder, string? name, IConfigurationSection configurationSection)
    {
        builder.AddServiceBusStreams(name!, GetServiceBusOptionsAction(configurationSection));
    }

    public void Configure(IClientBuilder builder, string? name, IConfigurationSection configurationSection)
    {
        builder.AddServiceBusStreams(name!, GetServiceBusOptionsAction(configurationSection));
    }

    private static Action<ServiceBusOptions> GetServiceBusOptionsAction(IConfigurationSection configurationSection)
    {
        return (ServiceBusOptions options) =>
        {
            // Configure connection string
            var connectionString = configurationSection["ConnectionString"];
            if (!string.IsNullOrEmpty(connectionString))
            {
                options.ConnectionString = connectionString;
            }

            // Configure fully qualified namespace
            var fullyQualifiedNamespace = configurationSection["FullyQualifiedNamespace"];
            if (!string.IsNullOrEmpty(fullyQualifiedNamespace))
            {
                options.FullyQualifiedNamespace = fullyQualifiedNamespace;
            }

            // Configure entity type
            var entityType = configurationSection["EntityType"];
            if (!string.IsNullOrEmpty(entityType) && Enum.TryParse<ServiceBusEntityType>(entityType, true, out var entityTypeValue))
            {
                options.EntityType = entityTypeValue;
            }

            // Configure entity name
            var entityName = configurationSection["EntityName"];
            if (!string.IsNullOrEmpty(entityName))
            {
                options.EntityName = entityName;
            }

            // Configure partition count
            if (int.TryParse(configurationSection["PartitionCount"], out var partitionCount))
            {
                options.PartitionCount = partitionCount;
            }

            // Configure prefetch count
            if (int.TryParse(configurationSection["PrefetchCount"], out var prefetchCount))
            {
                options.PrefetchCount = prefetchCount;
            }

            // Configure max delivery attempts
            if (int.TryParse(configurationSection["MaxDeliveryAttempts"], out var maxDeliveryAttempts))
            {
                options.MaxDeliveryAttempts = maxDeliveryAttempts;
            }

            // Configure queue name prefix
            var queueNamePrefix = configurationSection["QueueNamePrefix"];
            if (!string.IsNullOrEmpty(queueNamePrefix))
            {
                options.QueueNamePrefix = queueNamePrefix;
            }

            // Configure max wait time
            var maxWaitTime = configurationSection["MaxWaitTime"];
            if (TimeSpan.TryParse(maxWaitTime, out var maxWaitTimeSpan))
            {
                options.MaxWaitTime = maxWaitTimeSpan;
            }
        };
    }
}