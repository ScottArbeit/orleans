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

            // Configure partition count
            if (int.TryParse(configurationSection["PartitionCount"], out var partitionCount))
            {
                options.PartitionCount = partitionCount;
            }

            // Configure queue name prefix
            var queueNamePrefix = configurationSection["QueueNamePrefix"];
            if (!string.IsNullOrEmpty(queueNamePrefix))
            {
                options.QueueNamePrefix = queueNamePrefix;
            }

            // Configure whether to use topics
            if (bool.TryParse(configurationSection["UseTopics"], out var useTopics))
            {
                options.UseTopics = useTopics;
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