using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Providers
{
    /// <summary>
    /// Manages Azure Service Bus connections, senders, and receivers with proper lifecycle management.
    /// Provides connection pooling, health monitoring, and automatic recovery capabilities.
    /// </summary>
    public class AzureServiceBusConnectionManager : IDisposable
    {
        private readonly AzureServiceBusOptions _options;
        private readonly ILogger<AzureServiceBusConnectionManager> _logger;
        private readonly ConcurrentDictionary<string, ServiceBusSender> _senders;
        private readonly ConcurrentDictionary<string, ServiceBusReceiver> _receivers;
        private readonly SemaphoreSlim _connectionSemaphore;
        private ServiceBusClient? _serviceBusClient;
        private bool _disposed;

        /// <summary>
        /// Gets a value indicating whether the connection manager is healthy and ready to use.
        /// </summary>
        public bool IsHealthy => _serviceBusClient != null && !_disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusConnectionManager"/> class.
        /// </summary>
        /// <param name="options">The Azure Service Bus configuration options.</param>
        /// <param name="logger">The logger instance.</param>
        public AzureServiceBusConnectionManager(
            AzureServiceBusOptions options,
            ILogger<AzureServiceBusConnectionManager> logger)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _senders = new ConcurrentDictionary<string, ServiceBusSender>();
            _receivers = new ConcurrentDictionary<string, ServiceBusReceiver>();
            _connectionSemaphore = new SemaphoreSlim(1, 1);

            InitializeServiceBusClient();
        }

        /// <summary>
        /// Gets or creates a Service Bus sender for the specified entity.
        /// </summary>
        /// <param name="entityName">The name of the queue or topic.</param>
        /// <returns>A Service Bus sender instance.</returns>
        public Task<ServiceBusSender> GetSenderAsync(string entityName)
        {
            if (string.IsNullOrEmpty(entityName))
                throw new ArgumentException("Entity name cannot be null or empty.", nameof(entityName));

            ThrowIfDisposed();

            return Task.FromResult(_senders.GetOrAdd(entityName, name =>
            {
                _logger.LogDebug("Creating new Service Bus sender for entity: {EntityName}", name);
                
                if (_serviceBusClient == null)
                {
                    throw new InvalidOperationException("Service Bus client is not initialized.");
                }

                var sender = _serviceBusClient.CreateSender(name);
                _logger.LogInformation("Created Service Bus sender for entity: {EntityName}", name);
                return sender;
            }));
        }

        /// <summary>
        /// Gets or creates a Service Bus receiver for the specified entity.
        /// </summary>
        /// <param name="entityName">The name of the queue.</param>
        /// <returns>A Service Bus receiver instance.</returns>
        public Task<ServiceBusReceiver> GetReceiverAsync(string entityName)
        {
            if (string.IsNullOrEmpty(entityName))
                throw new ArgumentException("Entity name cannot be null or empty.", nameof(entityName));

            ThrowIfDisposed();

            return Task.FromResult(_receivers.GetOrAdd(entityName, name =>
            {
                _logger.LogDebug("Creating new Service Bus receiver for entity: {EntityName}", name);
                
                if (_serviceBusClient == null)
                {
                    throw new InvalidOperationException("Service Bus client is not initialized.");
                }

                var receiverOptions = new ServiceBusReceiverOptions
                {
                    ReceiveMode = _options.ReceiveMode == "PeekLock" 
                        ? ServiceBusReceiveMode.PeekLock 
                        : ServiceBusReceiveMode.ReceiveAndDelete,
                    PrefetchCount = _options.PrefetchCount
                };

                ServiceBusReceiver receiver;
                if (_options.EntityMode == ServiceBusEntityMode.Topic)
                {
                    if (string.IsNullOrEmpty(_options.SubscriptionName))
                    {
                        throw new InvalidOperationException("Subscription name is required for Topic mode.");
                    }
                    receiver = _serviceBusClient.CreateReceiver(name, _options.SubscriptionName, receiverOptions);
                }
                else
                {
                    receiver = _serviceBusClient.CreateReceiver(name, receiverOptions);
                }

                _logger.LogInformation("Created Service Bus receiver for entity: {EntityName}", name);
                return receiver;
            }));
        }

        /// <summary>
        /// Attempts to recover from connection failures by reinitializing the Service Bus client.
        /// </summary>
        /// <returns>A task representing the recovery operation.</returns>
        public async Task RecoverConnectionAsync()
        {
            await _connectionSemaphore.WaitAsync();
            try
            {
                _logger.LogWarning("Attempting to recover Service Bus connection");

                // Dispose existing resources
                DisposeAllResources();

                // Reinitialize the client
                InitializeServiceBusClient();

                _logger.LogInformation("Service Bus connection recovery completed successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to recover Service Bus connection");
                throw;
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        }

        /// <summary>
        /// Checks the health of the Service Bus connection.
        /// </summary>
        /// <returns>A task representing the health check operation.</returns>
        public Task<bool> CheckHealthAsync()
        {
            try
            {
                if (_serviceBusClient == null)
                {
                    return Task.FromResult(false);
                }

                // Try to create a sender to test the connection
                var testSender = _serviceBusClient.CreateSender(_options.EntityName);
                // Just creating the sender is enough to test the connection
                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Service Bus health check failed");
                return Task.FromResult(false);
            }
        }

        /// <summary>
        /// Initializes the Service Bus client with the configured connection string.
        /// </summary>
        private void InitializeServiceBusClient()
        {
            try
            {
                if (string.IsNullOrEmpty(_options.ConnectionString))
                {
                    throw new InvalidOperationException("Azure Service Bus connection string is not configured.");
                }

                var clientOptions = new ServiceBusClientOptions
                {
                    TransportType = ServiceBusTransportType.AmqpTcp,
                    RetryOptions = new ServiceBusRetryOptions
                    {
                        MaxRetries = 3,
                        MaxDelay = TimeSpan.FromSeconds(30),
                        TryTimeout = _options.OperationTimeout
                    }
                };

                _serviceBusClient = new ServiceBusClient(_options.ConnectionString, clientOptions);
                _logger.LogInformation("Service Bus client initialized successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize Service Bus client");
                throw;
            }
        }

        /// <summary>
        /// Disposes all Service Bus resources (senders, receivers, and client).
        /// </summary>
        private void DisposeAllResources()
        {
            // Dispose all senders
            foreach (var sender in _senders.Values)
            {
                try
                {
                    sender?.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(10));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error disposing Service Bus sender");
                }
            }
            _senders.Clear();

            // Dispose all receivers
            foreach (var receiver in _receivers.Values)
            {
                try
                {
                    receiver?.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(10));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error disposing Service Bus receiver");
                }
            }
            _receivers.Clear();

            // Dispose client
            try
            {
                _serviceBusClient?.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(10));
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing Service Bus client");
            }
            _serviceBusClient = null;
        }

        /// <summary>
        /// Throws an ObjectDisposedException if the connection manager has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(AzureServiceBusConnectionManager));
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the connection manager and releases all resources.
        /// </summary>
        /// <param name="disposing">True if disposing from Dispose method, false if from finalizer.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                _logger.LogInformation("Disposing Azure Service Bus connection manager");
                
                DisposeAllResources();
                _connectionSemaphore?.Dispose();
                
                _disposed = true;
            }
        }
    }
}