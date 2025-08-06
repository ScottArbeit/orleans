using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streaming.AzureServiceBus.Messages;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Providers;

/// <summary>
/// Queue adapter factory for Azure Service Bus that creates and manages adapters, caches, and queue mappers.
/// Implements the Orleans streaming provider patterns for Azure Service Bus integration.
/// </summary>
public class AzureServiceBusAdapterFactory : IQueueAdapterFactory, IDisposable
{
    private readonly string _name;
    private readonly AzureServiceBusOptions _options;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<AzureServiceBusAdapterFactory> _logger;
    private readonly object _lockObject = new();
    
    private IStreamQueueMapper? _streamQueueMapper;
    private IQueueAdapterCache? _queueAdapterCache;
    private AzureServiceBusConnectionManager? _connectionManager;
    private AzureServiceBusMessageFactory? _messageFactory;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="AzureServiceBusAdapterFactory"/> class.
    /// </summary>
    /// <param name="name">The name of this adapter factory instance.</param>
    /// <param name="options">The Azure Service Bus configuration options.</param>
    /// <param name="cacheOptions">The cache configuration options.</param>
    /// <param name="serviceProvider">The service provider for dependency injection.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public AzureServiceBusAdapterFactory(
        string name,
        AzureServiceBusOptions options,
        SimpleQueueCacheOptions cacheOptions,
        IServiceProvider serviceProvider,
        ILoggerFactory loggerFactory)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _logger = _loggerFactory.CreateLogger<AzureServiceBusAdapterFactory>();
        
        // Validate configuration
        ValidateConfiguration();
        
        _logger.LogInformation("Azure Service Bus adapter factory '{FactoryName}' initialized for entity '{EntityName}' in {EntityMode} mode",
            _name, _options.EntityName, _options.EntityMode);
    }

    /// <summary>
    /// Creates the Azure Service Bus adapter.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation and contains the queue adapter.</returns>
    public Task<IQueueAdapter> CreateAdapter()
    {
        ThrowIfDisposed();
        
        _logger.LogDebug("Creating Azure Service Bus adapter for factory '{FactoryName}'", _name);
        
        try
        {
            // Ensure connection manager is initialized
            var connectionManager = GetOrCreateConnectionManager();
            var messageFactory = GetOrCreateMessageFactory();
            
            var adapter = new AzureServiceBusAdapter(
                _options,
                connectionManager,
                messageFactory,
                _loggerFactory.CreateLogger<AzureServiceBusAdapter>(),
                _loggerFactory,
                _name);
            
            _logger.LogInformation("Successfully created Azure Service Bus adapter for factory '{FactoryName}'", _name);
            return Task.FromResult<IQueueAdapter>(adapter);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create Azure Service Bus adapter for factory '{FactoryName}'", _name);
            throw;
        }
    }

    /// <summary>
    /// Gets the queue adapter cache for caching received messages.
    /// </summary>
    /// <returns>The queue adapter cache.</returns>
    public IQueueAdapterCache GetQueueAdapterCache()
    {
        ThrowIfDisposed();
        
        if (_queueAdapterCache is null)
        {
            lock (_lockObject)
            {
                if (_queueAdapterCache is null)
                {
                    _logger.LogDebug("Creating queue adapter cache for factory '{FactoryName}'", _name);
                    
                    // For now, use placeholder until Step 9 implements the actual Azure Service Bus cache
                    _queueAdapterCache = new AzureServiceBusQueueCache(_name, _loggerFactory);
                    
                    _logger.LogInformation("Successfully created queue adapter cache for factory '{FactoryName}'", _name);
                }
            }
        }
        
        return _queueAdapterCache;
    }

    /// <summary>
    /// Gets the stream queue mapper for mapping streams to Service Bus queues/topics.
    /// </summary>
    /// <returns>The stream queue mapper.</returns>
    public IStreamQueueMapper GetStreamQueueMapper()
    {
        ThrowIfDisposed();
        
        if (_streamQueueMapper is null)
        {
            lock (_lockObject)
            {
                if (_streamQueueMapper is null)
                {
                    _logger.LogDebug("Creating stream queue mapper for factory '{FactoryName}'", _name);
                    
                    // For now, use placeholder until Step 6 implements the actual Azure Service Bus mapper
                    _streamQueueMapper = new AzureServiceBusQueueMapper(_name, _options);
                    
                    _logger.LogInformation("Successfully created stream queue mapper for factory '{FactoryName}'", _name);
                }
            }
        }
        
        return _streamQueueMapper;
    }

    /// <summary>
    /// Gets the delivery failure handler for handling message delivery failures.
    /// </summary>
    /// <param name="queueId">The queue identifier.</param>
    /// <returns>A task that represents the asynchronous operation and contains the failure handler.</returns>
    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
    {
        ThrowIfDisposed();
        
        _logger.LogDebug("Creating delivery failure handler for queue '{QueueId}' in factory '{FactoryName}'", queueId, _name);
        
        // Create a Service Bus specific failure handler
        var failureHandler = new ServiceBusStreamFailureHandler(_name, queueId, _logger);
        
        return Task.FromResult<IStreamFailureHandler>(failureHandler);
    }

    /// <summary>
    /// Creates an adapter factory instance from the service provider.
    /// </summary>
    /// <param name="services">The service provider.</param>
    /// <param name="name">The name of the adapter factory.</param>
    /// <returns>The created adapter factory.</returns>
    public static AzureServiceBusAdapterFactory Create(IServiceProvider services, string name)
    {
        var options = services.GetOptionsByName<AzureServiceBusOptions>(name);
        var cacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(name);
        var loggerFactory = services.GetRequiredService<ILoggerFactory>();
        
        var factory = new AzureServiceBusAdapterFactory(name, options, cacheOptions, services, loggerFactory);
        
        return factory;
    }

    /// <summary>
    /// Validates the Azure Service Bus configuration.
    /// </summary>
    private void ValidateConfiguration()
    {
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            throw new ArgumentException("Azure Service Bus connection string cannot be null or empty", nameof(_options.ConnectionString));
        }
        
        if (string.IsNullOrWhiteSpace(_options.EntityName))
        {
            throw new ArgumentException("Azure Service Bus entity name cannot be null or empty", nameof(_options.EntityName));
        }
        
        if (_options.EntityMode == ServiceBusEntityMode.Topic && string.IsNullOrWhiteSpace(_options.SubscriptionName))
        {
            throw new ArgumentException("Subscription name is required when using Topic mode", nameof(_options.SubscriptionName));
        }
        
        if (_options.BatchSize <= 0)
        {
            throw new ArgumentException("Batch size must be greater than zero", nameof(_options.BatchSize));
        }
        
        if (_options.MaxConcurrentCalls <= 0)
        {
            throw new ArgumentException("Max concurrent calls must be greater than zero", nameof(_options.MaxConcurrentCalls));
        }
        
        _logger.LogDebug("Configuration validation passed for factory '{FactoryName}'", _name);
    }

    /// <summary>
    /// Gets or creates the connection manager for Azure Service Bus operations.
    /// </summary>
    /// <returns>The connection manager.</returns>
    private AzureServiceBusConnectionManager GetOrCreateConnectionManager()
    {
        if (_connectionManager is null)
        {
            lock (_lockObject)
            {
                if (_connectionManager is null)
                {
                    _logger.LogDebug("Creating connection manager for factory '{FactoryName}'", _name);
                    _connectionManager = new AzureServiceBusConnectionManager(_options, _loggerFactory.CreateLogger<AzureServiceBusConnectionManager>());
                }
            }
        }
        
        return _connectionManager;
    }

    /// <summary>
    /// Gets or creates the message factory for creating Service Bus messages.
    /// </summary>
    /// <returns>The message factory.</returns>
    private AzureServiceBusMessageFactory GetOrCreateMessageFactory()
    {
        if (_messageFactory is null)
        {
            lock (_lockObject)
            {
                if (_messageFactory is null)
                {
                    _logger.LogDebug("Creating message factory for factory '{FactoryName}'", _name);
                    var serializer = _serviceProvider.GetRequiredService<Serializer>();
                    _messageFactory = new AzureServiceBusMessageFactory(serializer);
                }
            }
        }
        
        return _messageFactory;
    }

    /// <summary>
    /// Throws an exception if the factory has been disposed.
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(AzureServiceBusAdapterFactory));
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes the adapter factory and releases resources.
    /// </summary>
    /// <param name="disposing">True if disposing from Dispose method, false if from finalizer.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            _logger.LogInformation("Disposing Azure Service Bus adapter factory '{FactoryName}'", _name);
            
            try
            {
                _connectionManager?.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error disposing connection manager for factory '{FactoryName}'", _name);
            }
            
            _disposed = true;
        }
    }
}