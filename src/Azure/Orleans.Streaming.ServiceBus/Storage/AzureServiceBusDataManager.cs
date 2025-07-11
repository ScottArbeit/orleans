#nullable enable
using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;

namespace Orleans.Streaming.ServiceBus.Storage;

/// <summary>
/// Utility class to encapsulate access to Azure Service Bus.
/// Manages ServiceBus client lifecycle, sender/receiver management, and connection handling.
/// </summary>
/// <remarks>
/// Used by Azure Service Bus streaming provider.
/// </remarks>
public partial class AzureServiceBusDataManager : IAsyncDisposable
{
    private readonly ILogger _logger;
    private readonly AzureServiceBusOptions _options;
    private readonly string _entityName;
    private readonly ServiceBusTopologyType _topologyType;
    
    private ServiceBusClient? _serviceBusClient;
    private ServiceBusSender? _sender;
    private ServiceBusProcessor? _processor;
    private ServiceBusSessionProcessor? _sessionProcessor;
    private bool _disposed;

    /// <summary>
    /// Gets the entity name (queue or topic name) this data manager is connected to.
    /// </summary>
    public string EntityName => _entityName;

    /// <summary>
    /// Gets the topology type this data manager is configured for.
    /// </summary>
    public ServiceBusTopologyType TopologyType => _topologyType;

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="loggerFactory">Logger factory to use.</param>
    /// <param name="entityName">Name of the queue or topic to connect to.</param>
    /// <param name="options">Azure Service Bus connection options.</param>
    public AzureServiceBusDataManager(
        ILoggerFactory loggerFactory, 
        string entityName, 
        AzureServiceBusOptions options)
    {
        if (loggerFactory is null)
        {
            throw new ArgumentNullException(nameof(loggerFactory));
        }

        if (string.IsNullOrWhiteSpace(entityName))
        {
            throw new ArgumentException("A non-null, non-empty value must be provided.", nameof(entityName));
        }

        if (options is null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        _logger = loggerFactory.CreateLogger<AzureServiceBusDataManager>();
        _options = options;
        _topologyType = options.TopologyType;
        _entityName = GetEntityName(entityName, options);
        
        ValidateConfiguration();
    }

    /// <summary>
    /// Initializes the connection to Azure Service Bus.
    /// </summary>
    public async Task InitAsync()
    {
        ThrowIfDisposed();
        
        if (_serviceBusClient is not null)
        {
            return;
        }

        try
        {
            _serviceBusClient = await GetServiceBusClient();
            LogServiceBusInitialized(_topologyType, _entityName);
        }
        catch (Exception ex)
        {
            LogServiceBusInitializationFailed(ex, _entityName);
            throw;
        }
    }

    /// <summary>
    /// Sends a message to the configured Service Bus entity.
    /// </summary>
    /// <param name="message">The message to send.</param>
    public async Task SendMessageAsync(ServiceBusMessage message)
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        ThrowIfDisposed();
        await EnsureInitialized();

        try
        {
            var sender = GetOrCreateSender();
            await sender.SendMessageAsync(message);
            LogMessageSent(_entityName);
        }
        catch (Exception ex)
        {
            LogMessageSendFailed(ex, _entityName);
            throw;
        }
    }

    /// <summary>
    /// Creates a ServiceBusProcessor for receiving messages.
    /// </summary>
    /// <param name="subscriptionName">Optional subscription name for topic topology.</param>
    /// <returns>A configured ServiceBusProcessor.</returns>
    public ServiceBusProcessor CreateProcessor(string? subscriptionName = null)
    {
        ThrowIfDisposed();
        
        if (_serviceBusClient is null)
        {
            throw new InvalidOperationException("Service Bus client is not initialized. Call InitAsync first.");
        }

        try
        {
            var processor = _topologyType switch
            {
                ServiceBusTopologyType.Queue => _serviceBusClient.CreateProcessor(_entityName, CreateProcessorOptions()),
                ServiceBusTopologyType.Topic => _serviceBusClient.CreateProcessor(_entityName, GetSubscriptionName(subscriptionName), CreateProcessorOptions()),
                _ => throw new InvalidOperationException($"Unsupported topology type: {_topologyType}")
            };

            LogProcessorCreated(_topologyType, _entityName, subscriptionName);
            return processor;
        }
        catch (Exception ex)
        {
            LogProcessorCreationFailed(ex, _topologyType, _entityName);
            throw;
        }
    }

    /// <summary>
    /// Creates a ServiceBusSessionProcessor for receiving messages from session-enabled entities.
    /// </summary>
    /// <param name="subscriptionName">Optional subscription name for topic topology.</param>
    /// <returns>A configured ServiceBusSessionProcessor.</returns>
    public ServiceBusSessionProcessor CreateSessionProcessor(string? subscriptionName = null)
    {
        ThrowIfDisposed();
        
        if (_serviceBusClient is null)
        {
            throw new InvalidOperationException("Service Bus client is not initialized. Call InitAsync first.");
        }

        try
        {
            var sessionProcessor = _topologyType switch
            {
                ServiceBusTopologyType.Queue => _serviceBusClient.CreateSessionProcessor(_entityName, CreateSessionProcessorOptions()),
                ServiceBusTopologyType.Topic => _serviceBusClient.CreateSessionProcessor(_entityName, GetSubscriptionName(subscriptionName), CreateSessionProcessorOptions()),
                _ => throw new InvalidOperationException($"Unsupported topology type: {_topologyType}")
            };

            LogSessionProcessorCreated(_topologyType, _entityName, subscriptionName);
            return sessionProcessor;
        }
        catch (Exception ex)
        {
            LogSessionProcessorCreationFailed(ex, _topologyType, _entityName);
            throw;
        }
    }

    /// <summary>
    /// Closes all connections and releases resources.
    /// </summary>
    public async Task CloseAsync()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            if (_sender is not null)
            {
                await _sender.CloseAsync();
            }

            if (_processor is not null)
            {
                await _processor.CloseAsync();
            }

            if (_sessionProcessor is not null)
            {
                await _sessionProcessor.CloseAsync();
            }

            if (_serviceBusClient is not null)
            {
                await _serviceBusClient.DisposeAsync();
            }

            LogServiceBusClosed(_entityName);
        }
        catch (Exception ex)
        {
            LogServiceBusCloseFailed(ex, _entityName);
            throw;
        }
    }

    /// <summary>
    /// Disposes the data manager and releases all resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await CloseAsync();
        _disposed = true;
        GC.SuppressFinalize(this);
    }

    private async Task<ServiceBusClient> GetServiceBusClient()
    {
        if (_options.CreateClient is not null)
        {
            return await _options.CreateClient();
        }

        if (_options.ServiceBusClient is not null)
        {
            return _options.ServiceBusClient;
        }

        throw new InvalidOperationException("No Service Bus client configuration found. Use one of the ConfigureServiceBusClient methods.");
    }

    private ServiceBusSender GetOrCreateSender()
    {
        if (_sender is not null)
        {
            return _sender;
        }

        if (_serviceBusClient is null)
        {
            throw new InvalidOperationException("Service Bus client is not initialized.");
        }

        _sender = _serviceBusClient.CreateSender(_entityName);
        return _sender;
    }

    private async Task EnsureInitialized()
    {
        if (_serviceBusClient is null)
        {
            await InitAsync();
        }
    }

    private string GetEntityName(string providedName, AzureServiceBusOptions options)
    {
        var entityName = _topologyType switch
        {
            ServiceBusTopologyType.Queue => options.QueueName ?? providedName,
            ServiceBusTopologyType.Topic => options.TopicName ?? providedName,
            _ => throw new InvalidOperationException($"Unsupported topology type: {_topologyType}")
        };

        // Return empty string if both sources are null/empty - will be caught by validation
        return entityName ?? string.Empty;
    }

    private string GetSubscriptionName(string? providedSubscriptionName)
    {
        if (_topologyType != ServiceBusTopologyType.Topic)
        {
            throw new InvalidOperationException("Subscription name is only applicable for Topic topology.");
        }

        return providedSubscriptionName ?? _options.SubscriptionName ?? 
               throw new InvalidOperationException("Subscription name must be provided for Topic topology.");
    }

    private void ValidateConfiguration()
    {
        switch (_topologyType)
        {
            case ServiceBusTopologyType.Queue:
                if (string.IsNullOrWhiteSpace(_entityName))
                {
                    throw new InvalidOperationException("Queue name must be provided for Queue topology.");
                }
                break;

            case ServiceBusTopologyType.Topic:
                if (string.IsNullOrWhiteSpace(_entityName))
                {
                    throw new InvalidOperationException("Topic name must be provided for Topic topology.");
                }
                break;

            default:
                throw new InvalidOperationException($"Unsupported topology type: {_topologyType}");
        }
    }

    private ServiceBusProcessorOptions CreateProcessorOptions()
    {
        return new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = _options.MaxConcurrentCalls,
            PrefetchCount = _options.PrefetchCount,
            AutoCompleteMessages = _options.AutoCompleteMessages,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5),
            ReceiveMode = _options.AutoCompleteMessages ? ServiceBusReceiveMode.ReceiveAndDelete : ServiceBusReceiveMode.PeekLock
        };
    }

    private ServiceBusSessionProcessorOptions CreateSessionProcessorOptions()
    {
        return new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = _options.MaxConcurrentCalls,
            PrefetchCount = _options.PrefetchCount,
            AutoCompleteMessages = _options.AutoCompleteMessages,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5),
            ReceiveMode = _options.AutoCompleteMessages ? ServiceBusReceiveMode.ReceiveAndDelete : ServiceBusReceiveMode.PeekLock
        };
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(AzureServiceBusDataManager));
        }
    }

    #region Logging

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Initialized Azure Service Bus data manager for {TopologyType}: {EntityName}"
    )]
    private partial void LogServiceBusInitialized(ServiceBusTopologyType topologyType, string entityName);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Failed to initialize Azure Service Bus data manager for entity: {EntityName}"
    )]
    private partial void LogServiceBusInitializationFailed(Exception exception, string entityName);

    [LoggerMessage(
        Level = LogLevel.Trace,
        Message = "Message sent to Azure Service Bus entity: {EntityName}"
    )]
    private partial void LogMessageSent(string entityName);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Failed to send message to Azure Service Bus entity: {EntityName}"
    )]
    private partial void LogMessageSendFailed(Exception exception, string entityName);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Created {TopologyType} processor for entity: {EntityName}, subscription: {SubscriptionName}"
    )]
    private partial void LogProcessorCreated(ServiceBusTopologyType topologyType, string entityName, string? subscriptionName);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Failed to create {TopologyType} processor for entity: {EntityName}"
    )]
    private partial void LogProcessorCreationFailed(Exception exception, ServiceBusTopologyType topologyType, string entityName);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Created {TopologyType} session processor for entity: {EntityName}, subscription: {SubscriptionName}"
    )]
    private partial void LogSessionProcessorCreated(ServiceBusTopologyType topologyType, string entityName, string? subscriptionName);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Failed to create {TopologyType} session processor for entity: {EntityName}"
    )]
    private partial void LogSessionProcessorCreationFailed(Exception exception, ServiceBusTopologyType topologyType, string entityName);

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "Closed Azure Service Bus data manager for entity: {EntityName}"
    )]
    private partial void LogServiceBusClosed(string entityName);

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Failed to close Azure Service Bus data manager for entity: {EntityName}"
    )]
    private partial void LogServiceBusCloseFailed(Exception exception, string entityName);

    #endregion
}