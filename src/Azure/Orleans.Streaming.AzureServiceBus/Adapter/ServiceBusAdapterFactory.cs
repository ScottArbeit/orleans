using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
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
    /// Creates a queue adapter. (Not implemented in this step)
    /// </summary>
    /// <returns>The queue adapter</returns>
    public Task<IQueueAdapter> CreateAdapter()
    {
        // Will be implemented in later steps
        throw new NotImplementedException("Service Bus adapter will be implemented in a later step");
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
    /// Returns a default handler that logs failures and allows Service Bus to handle retries and dead letter queuing.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>The stream failure handler.</returns>
    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
    {
        if (_streamFailureHandler is null)
        {
            // Lazy initialize the failure handler
            var loggerFactory = _services.GetService<ILoggerFactory>() ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
            _streamFailureHandler = new ServiceBusStreamDeliveryFailureHandler(loggerFactory.CreateLogger<ServiceBusStreamDeliveryFailureHandler>());
        }
        
        return Task.FromResult(_streamFailureHandler);
    }
}

/// <summary>
/// Default stream failure handler for Service Bus that logs delivery failures
/// and lets Service Bus handle retries and eventual dead letter queuing.
/// </summary>
internal class ServiceBusStreamDeliveryFailureHandler : IStreamFailureHandler
{
    private readonly ILogger<ServiceBusStreamDeliveryFailureHandler> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusStreamDeliveryFailureHandler"/> class.
    /// </summary>
    /// <param name="logger">The logger.</param>
    public ServiceBusStreamDeliveryFailureHandler(ILogger<ServiceBusStreamDeliveryFailureHandler> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Gets a value indicating whether the subscription should fault when there is an error.
    /// For Service Bus, we don't fault subscriptions on delivery failures as the message will be retried.
    /// </summary>
    public bool ShouldFaultSubscriptionOnError => false;

    /// <summary>
    /// Called once all measures to deliver an event to a consumer have been exhausted.
    /// Logs the failure and lets Service Bus handle retry logic and eventual dead letter queue routing.
    /// </summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="streamProviderName">Name of the stream provider.</param>
    /// <param name="streamIdentity">The stream identity.</param>
    /// <param name="sequenceToken">The sequence token.</param>
    /// <returns>A <see cref="Task"/> representing the operation.</returns>
    public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, StreamId streamIdentity, StreamSequenceToken sequenceToken)
    {
        _logger.LogWarning(
            "Stream delivery failure for subscription {SubscriptionId} on provider {StreamProviderName}, " +
            "stream {StreamNamespace}:{StreamKey}, sequence token {SequenceToken}. " +
            "Message will be retried by Service Bus and eventually moved to dead letter queue if max delivery count is exceeded.",
            subscriptionId,
            streamProviderName,
            streamIdentity.GetNamespace(),
            streamIdentity.GetKeyAsString(),
            sequenceToken);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Should be called when establishing a subscription failed.
    /// </summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="streamProviderName">Name of the stream provider.</param>
    /// <param name="streamIdentity">The stream identity.</param>
    /// <param name="sequenceToken">The sequence token.</param>
    /// <returns>A <see cref="Task"/> representing the operation.</returns>
    public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, StreamId streamIdentity, StreamSequenceToken sequenceToken)
    {
        _logger.LogError(
            "Stream subscription failure for subscription {SubscriptionId} on provider {StreamProviderName}, " +
            "stream {StreamNamespace}:{StreamKey}, sequence token {SequenceToken}.",
            subscriptionId,
            streamProviderName,
            streamIdentity.GetNamespace(),
            streamIdentity.GetKeyAsString(),
            sequenceToken);

        return Task.CompletedTask;
    }
}