using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Service Bus adapter factory for Orleans streaming.
/// Provides the infrastructure for Service Bus streaming with support for single entity mapping (MVP).
/// </summary>
public static class ServiceBusAdapterFactory
{
    /// <summary>
    /// Creates a Service Bus adapter factory.
    /// </summary>
    /// <param name="services">The service provider.</param>
    /// <param name="name">The provider name.</param>
    /// <returns>A Service Bus adapter factory instance.</returns>
    public static IQueueAdapterFactory Create(IServiceProvider services, string name)
    {
        // Create and return the ServiceBusQueueAdapterFactory with our custom mapper
        return new ServiceBusQueueAdapterFactory(services, name);
    }
}

/// <summary>
/// Implementation of <see cref="Orleans.Streams.IQueueAdapterFactory"/> for Azure Service Bus streaming.
/// This factory creates and manages the components needed for Service Bus streaming with single entity support.
/// </summary>
internal class ServiceBusQueueAdapterFactory : IQueueAdapterFactory
{
    private readonly IServiceProvider _services;
    private readonly string _providerName;
    private readonly ServiceBusQueueMapper _queueMapper;
    private IQueueAdapterCache? _adapterCache;
    private IStreamFailureHandler? _streamFailureHandler;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusQueueAdapterFactory"/> class.
    /// </summary>
    /// <param name="services">The service provider.</param>
    /// <param name="providerName">The stream provider name.</param>
    public ServiceBusQueueAdapterFactory(IServiceProvider services, string providerName)
    {
        _services = services ?? throw new ArgumentNullException(nameof(services));
        _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));

        // Get the Service Bus stream options and create the queue mapper
        var optionsMonitor = services.GetRequiredService<IOptionsMonitor<ServiceBusStreamOptions>>();
        var options = optionsMonitor.Get(providerName);
        _queueMapper = new ServiceBusQueueMapper(options, providerName);
    }

    /// <summary>
    /// Gets the configured queue identifier for this factory.
    /// This represents the single Service Bus entity (queue or topic/subscription) used in the MVP.
    /// </summary>
    public QueueId QueueId => _queueMapper.QueueId;

    /// <summary>
    /// Creates a queue adapter for Service Bus streaming.
    /// </summary>
    /// <returns>The queue adapter</returns>
    public async Task<IQueueAdapter> CreateAdapter()
    {
        // Get the Service Bus stream options
        var optionsMonitor = _services.GetRequiredService<IOptionsMonitor<ServiceBusStreamOptions>>();
        var options = optionsMonitor.Get(_providerName);

        // Provision entities if auto-create is enabled
        if (options.AutoCreateEntities)
        {
            await ProvisionEntitiesAsync(options);
        }

        // Get the data adapter from DI or create one
        var serializer = _services.GetRequiredService<Serializer<ServiceBusBatchContainer>>();
        var dataAdapter = new ServiceBusDataAdapter(serializer);

        // Get logger
        var logger = _services.GetService<ILogger<ServiceBusAdapter>>() ?? 
                    Microsoft.Extensions.Logging.Abstractions.NullLogger<ServiceBusAdapter>.Instance;

        // Ensure the failure handler is initialized
        if (_streamFailureHandler is null)
        {
            var loggerFactory = _services.GetService<ILoggerFactory>() ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
            var failureLogger = loggerFactory.CreateLogger<ServiceBusStreamFailureHandler>();
            _streamFailureHandler = new ServiceBusStreamFailureHandler(failureLogger);
        }

        var failureHandler = _streamFailureHandler as ServiceBusStreamFailureHandler;

        // Create the adapter
        var adapter = new ServiceBusAdapter(_providerName, options, dataAdapter, _queueMapper, logger, failureHandler);
        await Task.CompletedTask;
        return adapter;
    }

    /// <summary>
    /// Creates queue message cache adapter using Orleans' SimpleQueueAdapterCache.
    /// The cache is non-rewindable as required for Service Bus streaming.
    /// </summary>
    /// <returns>The queue adapter cache.</returns>
    public IQueueAdapterCache GetQueueAdapterCache()
    {
        if (_adapterCache is null)
        {
            // Lazy initialize the adapter cache
            var cacheOptionsMonitor = _services.GetService<IOptionsMonitor<SimpleQueueCacheOptions>>();
            var cacheOptions = cacheOptionsMonitor?.Get(_providerName) ?? new SimpleQueueCacheOptions();
            
            var loggerFactory = _services.GetService<ILoggerFactory>() ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
            _adapterCache = new SimpleQueueAdapterCache(cacheOptions, _providerName, loggerFactory);
        }
        
        return _adapterCache;
    }

    /// <summary>
    /// Creates a queue mapper.
    /// </summary>
    /// <returns>The queue mapper that maps all streams to the single configured entity.</returns>
    public IStreamQueueMapper GetStreamQueueMapper()
    {
        return _queueMapper;
    }

    /// <summary>
    /// Acquire delivery failure handler for a queue.
    /// Returns the Service Bus stream failure handler that logs failures and prevents completion
    /// of failed messages to allow Service Bus retries and dead letter queue handling.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>The stream failure handler.</returns>
    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
    {
        if (_streamFailureHandler is null)
        {
            // Lazy initialize the failure handler
            var loggerFactory = _services.GetService<ILoggerFactory>() ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
            var logger = loggerFactory.CreateLogger<ServiceBusStreamFailureHandler>();
            _streamFailureHandler = new ServiceBusStreamFailureHandler(logger);
        }
        
        return Task.FromResult(_streamFailureHandler);
    }

    /// <summary>
    /// Provisions Service Bus entities if auto-create is enabled.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    private async Task ProvisionEntitiesAsync(ServiceBusStreamOptions options)
    {
        try
        {
            var adminClient = CreateServiceBusAdministrationClient(options);
            var loggerFactory = _services.GetService<ILoggerFactory>() ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
            var logger = loggerFactory.CreateLogger<ServiceBusProvisioner>();
            var provisioner = new ServiceBusProvisioner(adminClient, logger);
            
            await provisioner.ProvisionEntitiesAsync(options);
        }
        catch (Exception ex)
        {
            var logger = _services.GetService<ILogger<ServiceBusQueueAdapterFactory>>() ?? 
                        Microsoft.Extensions.Logging.Abstractions.NullLogger<ServiceBusQueueAdapterFactory>.Instance;
            logger.LogError(ex, "Failed to provision Service Bus entities for provider '{ProviderName}'", _providerName);
            throw;
        }
    }

    /// <summary>
    /// Creates a Service Bus administration client based on the configured connection options.
    /// </summary>
    /// <param name="options">The Service Bus streaming options.</param>
    /// <returns>A configured Service Bus administration client.</returns>
    /// <exception cref="InvalidOperationException">Thrown when connection configuration is invalid.</exception>
    private static ServiceBusAdministrationClient CreateServiceBusAdministrationClient(ServiceBusStreamOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return new ServiceBusAdministrationClient(options.ConnectionString);
        }

        if (!string.IsNullOrWhiteSpace(options.FullyQualifiedNamespace) && options.Credential is not null)
        {
            return new ServiceBusAdministrationClient(options.FullyQualifiedNamespace, options.Credential);
        }

        throw new InvalidOperationException(
            "Either ConnectionString or both FullyQualifiedNamespace and Credential must be configured for Service Bus streaming.");
    }
}
