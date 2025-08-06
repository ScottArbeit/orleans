using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streaming.AzureServiceBus.Messages;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Providers
{
    /// <summary>
    /// Queue adapter for Azure Service Bus that implements Orleans streaming interface.
    /// Supports both Queue and Topic/Subscription modes based on configuration.
    /// </summary>
    public class AzureServiceBusAdapter : IQueueAdapter, IDisposable
    {
        private readonly AzureServiceBusOptions _options;
        private readonly ILogger<AzureServiceBusAdapter> _logger;
        private readonly ILoggerFactory _loggerFactory;
        private readonly AzureServiceBusConnectionManager _connectionManager;
        private readonly AzureServiceBusMessageFactory _messageFactory;
        private readonly string _providerName;
        private bool _disposed;

        /// <inheritdoc/>
        public string Name => _providerName;

        /// <inheritdoc/>
        public bool IsRewindable => true;

        /// <inheritdoc/>
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureServiceBusAdapter"/> class.
        /// </summary>
        /// <param name="options">The Azure Service Bus configuration options.</param>
        /// <param name="connectionManager">The connection manager for Service Bus operations.</param>
        /// <param name="messageFactory">The message factory for creating Service Bus messages.</param>
        /// <param name="logger">The logger instance.</param>
        /// <param name="loggerFactory">The logger factory.</param>
        /// <param name="providerName">The name of this adapter instance.</param>
        public AzureServiceBusAdapter(
            AzureServiceBusOptions options,
            AzureServiceBusConnectionManager connectionManager,
            AzureServiceBusMessageFactory messageFactory,
            ILogger<AzureServiceBusAdapter> logger,
            ILoggerFactory loggerFactory,
            string providerName)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            _messageFactory = messageFactory ?? throw new ArgumentNullException(nameof(messageFactory));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));

            _logger.LogInformation("Azure Service Bus adapter '{AdapterName}' initialized for entity '{EntityName}' in {EntityMode} mode",
                _providerName, _options.EntityName, _options.EntityMode);
        }

        /// <inheritdoc/>
        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            _logger.LogDebug("Creating receiver for queue ID: {QueueId}", queueId);
            
            // For now, return a placeholder receiver that will be implemented in Step 7
            return new ServiceBusAdapterReceiver(
                queueId,
                _options,
                _connectionManager,
                _messageFactory,
                _loggerFactory.CreateLogger<ServiceBusAdapterReceiver>());
        }

        /// <inheritdoc/>
        public async Task QueueMessageBatchAsync<T>(
            StreamId streamId, 
            IEnumerable<T> events, 
            StreamSequenceToken token, 
            Dictionary<string, object> requestContext)
        {
            if (events == null)
                throw new ArgumentNullException(nameof(events));

            if (requestContext == null)
                throw new ArgumentNullException(nameof(requestContext));

            ThrowIfDisposed();

            var eventList = new List<T>(events);
            if (eventList.Count == 0)
            {
                _logger.LogDebug("No events to queue for stream {StreamId}", streamId);
                return;
            }

            _logger.LogDebug("Queuing batch of {EventCount} events for stream {StreamId}", eventList.Count, streamId);

            try
            {
                // Use retry logic for sending messages to handle transient failures
                await RetryHelper.ExecuteWithRetryAsync(
                    async () =>
                    {
                        // Get the appropriate sender based on entity mode
                        var sender = await _connectionManager.GetSenderAsync(_options.EntityName);

                        // Create Service Bus messages for each event
                        var serviceBusMessages = new List<ServiceBusMessage>();
                        foreach (var evt in eventList)
                        {
                            var serviceBusMessage = _messageFactory.CreateServiceBusMessage(
                                streamId,
                                evt!,
                                requestContext);

                            serviceBusMessages.Add(serviceBusMessage);
                        }

                        // Send messages based on batch size
                        if (serviceBusMessages.Count == 1)
                        {
                            await sender.SendMessageAsync(serviceBusMessages[0]);
                            _logger.LogDebug("Sent single message for stream {StreamId}", streamId);
                        }
                        else
                        {
                            await sender.SendMessagesAsync(serviceBusMessages);
                            _logger.LogDebug("Sent batch of {MessageCount} messages for stream {StreamId}", serviceBusMessages.Count, streamId);
                        }
                    },
                    maxRetries: 3,
                    baseDelay: TimeSpan.FromSeconds(1),
                    maxDelay: TimeSpan.FromSeconds(30),
                    logger: _logger,
                    operationName: $"SendMessages for stream {streamId}");
            }
            catch (ServiceBusException ex) when (IsTransientError(ex))
            {
                _logger.LogWarning(ex, "Transient error occurred while sending messages for stream {StreamId}. Error: {Error}", 
                    streamId, ex.Reason);
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send messages for stream {StreamId}", streamId);
                throw;
            }
        }

        /// <summary>
        /// Determines if a Service Bus exception represents a transient error that can be retried.
        /// </summary>
        /// <param name="ex">The Service Bus exception.</param>
        /// <returns>True if the error is transient, false otherwise.</returns>
        private static bool IsTransientError(ServiceBusException ex)
        {
            return ex.Reason switch
            {
                ServiceBusFailureReason.ServiceTimeout => true,
                ServiceBusFailureReason.ServiceBusy => true,
                ServiceBusFailureReason.ServiceCommunicationProblem => true,
                ServiceBusFailureReason.GeneralError => true,
                _ => false
            };
        }

        /// <summary>
        /// Throws an ObjectDisposedException if the adapter has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(AzureServiceBusAdapter));
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the adapter and releases resources.
        /// </summary>
        /// <param name="disposing">True if disposing from Dispose method, false if from finalizer.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                _logger.LogInformation("Disposing Azure Service Bus adapter '{AdapterName}'", _providerName);
                
                try
                {
                    _connectionManager?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error disposing connection manager for adapter '{AdapterName}'", _providerName);
                }

                _disposed = true;
            }
        }
    }
}