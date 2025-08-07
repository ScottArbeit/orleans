using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
    /// Implementation of IQueueAdapterReceiver for Azure Service Bus.
    /// Supports both Queue and Topic/Subscription consumption modes with proper message batching,
    /// acknowledgment, error handling, and session management.
    /// </summary>
    internal class ServiceBusAdapterReceiver : IQueueAdapterReceiver, IDisposable
    {
        private readonly QueueId _queueId;
        private readonly AzureServiceBusOptions _options;
        private readonly AzureServiceBusConnectionManager _connectionManager;
        private readonly AzureServiceBusMessageFactory _messageFactory;
        private readonly ILogger<ServiceBusAdapterReceiver> _logger;
        
        private ServiceBusReceiver? _receiver;
        private ServiceBusSessionReceiver? _sessionReceiver;
        private CancellationTokenSource? _shutdownCts;
        private Task? _outstandingTask;
        private volatile bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceBusAdapterReceiver"/> class.
        /// </summary>
        /// <param name="queueId">The queue identifier.</param>
        /// <param name="options">The Azure Service Bus options.</param>
        /// <param name="connectionManager">The connection manager.</param>
        /// <param name="messageFactory">The message factory.</param>
        /// <param name="logger">The logger instance.</param>
        public ServiceBusAdapterReceiver(
            QueueId queueId,
            AzureServiceBusOptions options,
            AzureServiceBusConnectionManager connectionManager,
            AzureServiceBusMessageFactory messageFactory,
            ILogger<ServiceBusAdapterReceiver> logger)
        {
            _queueId = queueId;
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            _messageFactory = messageFactory ?? throw new ArgumentNullException(nameof(messageFactory));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _shutdownCts = new CancellationTokenSource();
        }

        /// <inheritdoc/>
        public async Task Initialize(TimeSpan timeout)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ServiceBusAdapterReceiver));

            _logger.LogInformation("Initializing Service Bus adapter receiver for queue {QueueId}", _queueId);
            
            try
            {
                await RetryHelper.ExecuteWithRetryAsync(
                    async () =>
                    {
                        // Get the entity name from queue ID or options
                        var entityName = GetEntityName();
                        
                        // Check if we need session-enabled receiver
                        var requiresSession = GetRequiresSession();
                        
                        if (requiresSession)
                        {
                            _sessionReceiver = await CreateSessionReceiverAsync(entityName);
                            if (_sessionReceiver != null)
                            {
                                var sessionId = _sessionReceiver.SessionId ?? "unknown";
                                _logger.LogInformation("Created session receiver for entity {EntityName} with session {SessionId}", 
                                    entityName, sessionId);
                            }
                        }
                        else
                        {
                            _receiver = await _connectionManager.GetReceiverAsync(entityName);
                            _logger.LogInformation("Created regular receiver for entity {EntityName}", entityName);
                        }
                    },
                    maxRetries: 3,
                    baseDelay: TimeSpan.FromSeconds(1),
                    maxDelay: TimeSpan.FromSeconds(30),
                    logger: _logger,
                    operationName: "Initialize Receiver",
                    cancellationToken: _shutdownCts?.Token ?? CancellationToken.None);
                    
                _logger.LogInformation("Successfully initialized Service Bus adapter receiver for queue {QueueId}", _queueId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize Service Bus adapter receiver for queue {QueueId}", _queueId);
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            if (_disposed || _shutdownCts?.Token.IsCancellationRequested == true)
                return new List<IBatchContainer>();

            var actualMaxCount = maxCount <= 0 ? _options.BatchSize : Math.Min(maxCount, _options.BatchSize);
            _logger.LogDebug("Getting up to {MaxCount} messages from queue {QueueId}", actualMaxCount, _queueId);
            
            try
            {
                var task = RetryHelper.ExecuteWithRetryAsync(
                    async () =>
                    {
                        var messages = new List<ServiceBusReceivedMessage>();
                        
                        if (_sessionReceiver != null)
                        {
                            // Session-enabled receiver
                            messages.AddRange(await _sessionReceiver.ReceiveMessagesAsync(
                                actualMaxCount, _options.OperationTimeout, _shutdownCts?.Token ?? CancellationToken.None));
                        }
                        else if (_receiver != null)
                        {
                            // Regular receiver
                            messages.AddRange(await _receiver.ReceiveMessagesAsync(
                                actualMaxCount, _options.OperationTimeout, _shutdownCts?.Token ?? CancellationToken.None));
                        }
                        else
                        {
                            _logger.LogWarning("No receiver available for queue {QueueId}", _queueId);
                            return new List<IBatchContainer>();
                        }

                        return await ConvertMessagesToBatchContainers(messages);
                    },
                    maxRetries: 3,
                    baseDelay: TimeSpan.FromSeconds(1),
                    maxDelay: TimeSpan.FromSeconds(10),
                    logger: _logger,
                    operationName: "Get Queue Messages",
                    cancellationToken: _shutdownCts?.Token ?? CancellationToken.None);

                _outstandingTask = task;
                var result = await task;
                
                _logger.LogDebug("Retrieved {MessageCount} messages from queue {QueueId}", 
                    result.Count, _queueId);
                
                return result;
            }
            catch (OperationCanceledException) when (_shutdownCts?.Token.IsCancellationRequested == true)
            {
                _logger.LogInformation("Message retrieval cancelled for queue {QueueId} due to shutdown", _queueId);
                return new List<IBatchContainer>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to retrieve messages from queue {QueueId}", _queueId);
                return new List<IBatchContainer>();
            }
            finally
            {
                _outstandingTask = null;
            }
        }

        /// <inheritdoc/>
        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            if (messages.Count == 0 || _disposed)
                return;

            _logger.LogDebug("Acknowledging delivery of {MessageCount} messages for queue {QueueId}", 
                messages.Count, _queueId);
            
            try
            {
                var task = RetryHelper.ExecuteWithRetryAsync(
                    async () =>
                    {
                        var serviceBusMessages = ExtractServiceBusMessages(messages);
                        
                        if (serviceBusMessages.Count == 0)
                        {
                            _logger.LogWarning("No Service Bus messages found to acknowledge for queue {QueueId}", _queueId);
                            return;
                        }

                        // Acknowledge messages based on receiver type and options
                        if (_options.AutoCompleteMessages)
                        {
                            await AcknowledgeMessages(serviceBusMessages);
                        }
                        else
                        {
                            _logger.LogDebug("AutoComplete disabled - messages will be auto-acknowledged by Service Bus for queue {QueueId}", _queueId);
                        }
                    },
                    maxRetries: 3,
                    baseDelay: TimeSpan.FromSeconds(1),
                    maxDelay: TimeSpan.FromSeconds(10),
                    logger: _logger,
                    operationName: "Acknowledge Messages",
                    cancellationToken: _shutdownCts?.Token ?? CancellationToken.None);

                _outstandingTask = task;
                await task;
                
                _logger.LogDebug("Successfully acknowledged {MessageCount} messages for queue {QueueId}", 
                    messages.Count, _queueId);
            }
            catch (OperationCanceledException) when (_shutdownCts?.Token.IsCancellationRequested == true)
            {
                _logger.LogInformation("Message acknowledgment cancelled for queue {QueueId} due to shutdown", _queueId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to acknowledge messages for queue {QueueId}. Messages may be reprocessed.", _queueId);
                // Don't rethrow here as it would stop the streaming pipeline
            }
            finally
            {
                _outstandingTask = null;
            }
        }

        /// <inheritdoc/>
        public async Task Shutdown(TimeSpan timeout)
        {
            _logger.LogInformation("Shutting down Service Bus adapter receiver for queue {QueueId}", _queueId);
            
            try
            {
                // Signal shutdown to cancel any ongoing operations
                _shutdownCts?.Cancel();

                // Wait for any outstanding operations to complete
                var outstandingTask = _outstandingTask;
                if (outstandingTask != null)
                {
                    try
                    {
                        await outstandingTask.WaitAsync(timeout);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Outstanding task did not complete gracefully during shutdown for queue {QueueId}", _queueId);
                    }
                }

                // Dispose receivers
                await DisposeReceiversAsync();
                
                _logger.LogInformation("Successfully shut down Service Bus adapter receiver for queue {QueueId}", _queueId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during shutdown of Service Bus adapter receiver for queue {QueueId}", _queueId);
                throw;
            }
        }

        /// <summary>
        /// Gets the entity name from the queue ID or options.
        /// </summary>
        /// <returns>The entity name.</returns>
        private string GetEntityName()
        {
            // Use entity name from options if specified, otherwise derive from queue ID
            return !string.IsNullOrEmpty(_options.EntityName) 
                ? _options.EntityName 
                : _queueId.ToString();
        }

        /// <summary>
        /// Determines if the entity requires session handling.
        /// </summary>
        /// <returns>True if sessions are required, false otherwise.</returns>
        private bool GetRequiresSession()
        {
            return _options switch
            {
                AzureServiceBusQueueOptions queueOptions => queueOptions.RequiresSession,
                AzureServiceBusTopicOptions topicOptions => topicOptions.RequiresSession,
                _ => false
            };
        }

        /// <summary>
        /// Creates a session receiver for session-enabled entities.
        /// </summary>
        /// <param name="entityName">The entity name.</param>
        /// <returns>A session receiver.</returns>
        private async Task<ServiceBusSessionReceiver> CreateSessionReceiverAsync(string entityName)
        {
            var sessionId = GetSessionId();
            
            var sessionReceiverOptions = new ServiceBusSessionReceiverOptions
            {
                ReceiveMode = _options.ReceiveMode == "PeekLock" 
                    ? ServiceBusReceiveMode.PeekLock 
                    : ServiceBusReceiveMode.ReceiveAndDelete,
                PrefetchCount = _options.PrefetchCount
            };

            // Get the Service Bus client from connection manager (we need to access the client directly for sessions)
            var client = await GetServiceBusClientAsync();
            
            ServiceBusSessionReceiver sessionReceiver;
            if (_options.EntityMode == ServiceBusEntityMode.Topic)
            {
                if (string.IsNullOrEmpty(_options.SubscriptionName))
                {
                    throw new InvalidOperationException("Subscription name is required for Topic mode.");
                }
                
                sessionReceiver = string.IsNullOrEmpty(sessionId)
                    ? await client.AcceptNextSessionAsync(entityName, _options.SubscriptionName, sessionReceiverOptions)
                    : await client.AcceptSessionAsync(entityName, _options.SubscriptionName, sessionId, sessionReceiverOptions);
            }
            else
            {
                sessionReceiver = string.IsNullOrEmpty(sessionId)
                    ? await client.AcceptNextSessionAsync(entityName, sessionReceiverOptions)
                    : await client.AcceptSessionAsync(entityName, sessionId, sessionReceiverOptions);
            }

            return sessionReceiver;
        }

        /// <summary>
        /// Gets the session ID from options.
        /// </summary>
        /// <returns>The session ID or null for next available session.</returns>
        private string? GetSessionId()
        {
            return _options switch
            {
                AzureServiceBusQueueOptions queueOptions => 
                    string.IsNullOrEmpty(queueOptions.SessionId) ? null : queueOptions.SessionId,
                AzureServiceBusTopicOptions topicOptions => 
                    string.IsNullOrEmpty(topicOptions.SessionId) ? null : topicOptions.SessionId,
                _ => null
            };
        }

        /// <summary>
        /// Gets the Service Bus client from the connection manager.
        /// We need reflection here as the client is private in the connection manager.
        /// </summary>
        /// <returns>The Service Bus client.</returns>
        private Task<ServiceBusClient> GetServiceBusClientAsync()
        {
            // This is a bit of a hack, but we need access to the ServiceBusClient for session operations
            // In a real implementation, we might want to expose this through the connection manager
            var clientField = typeof(AzureServiceBusConnectionManager)
                .GetField("_serviceBusClient", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            
            if (clientField?.GetValue(_connectionManager) is ServiceBusClient client)
            {
                return Task.FromResult(client);
            }
            
            throw new InvalidOperationException("Unable to access Service Bus client for session operations.");
        }

        /// <summary>
        /// Converts Service Bus messages to Orleans batch containers.
        /// </summary>
        /// <param name="messages">The Service Bus messages.</param>
        /// <returns>A list of batch containers.</returns>
        private async Task<List<IBatchContainer>> ConvertMessagesToBatchContainers(IReadOnlyList<ServiceBusReceivedMessage> messages)
        {
            var batchContainers = new List<IBatchContainer>();

            foreach (var message in messages)
            {
                try
                {
                    // Extract stream ID from message properties
                    var streamId = _messageFactory.ExtractStreamId(message, 
                        StreamId.Create(_queueId.ToString(), Guid.NewGuid().ToString()));
                    
                    // Convert to Orleans message
                    var orleansMessage = _messageFactory.CreateFromServiceBusMessage(message, streamId);
                    batchContainers.Add(orleansMessage);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to convert Service Bus message {MessageId} to batch container for queue {QueueId}", 
                        message.MessageId, _queueId);
                    
                    // Try to abandon the message if using PeekLock mode
                    if (_options.ReceiveMode == "PeekLock")
                    {
                        try
                        {
                            if (_sessionReceiver != null)
                            {
                                await _sessionReceiver.AbandonMessageAsync(message);
                            }
                            else if (_receiver != null)
                            {
                                await _receiver.AbandonMessageAsync(message);
                            }
                        }
                        catch (Exception abandonEx)
                        {
                            _logger.LogWarning(abandonEx, "Failed to abandon message {MessageId} after conversion error", message.MessageId);
                        }
                    }
                }
            }

            return batchContainers;
        }

        /// <summary>
        /// Extracts Service Bus messages from batch containers for acknowledgment.
        /// </summary>
        /// <param name="messages">The batch containers.</param>
        /// <returns>A list of Service Bus messages to acknowledge.</returns>
        private List<ServiceBusReceivedMessage> ExtractServiceBusMessages(IList<IBatchContainer> messages)
        {
            var serviceBusMessages = new List<ServiceBusReceivedMessage>();

            foreach (var message in messages)
            {
                if (message is AzureServiceBusMessage azureMessage && 
                    azureMessage.Metadata.OriginalServiceBusMessage != null)
                {
                    serviceBusMessages.Add(azureMessage.Metadata.OriginalServiceBusMessage);
                }
            }

            return serviceBusMessages;
        }

        /// <summary>
        /// Acknowledges (completes) Service Bus messages.
        /// </summary>
        /// <param name="messages">The messages to acknowledge.</param>
        private async Task AcknowledgeMessages(List<ServiceBusReceivedMessage> messages)
        {
            var acknowledgeTasks = new List<Task>();

            foreach (var message in messages)
            {
                Task ackTask;
                
                if (_sessionReceiver != null)
                {
                    ackTask = _sessionReceiver.CompleteMessageAsync(message, _shutdownCts?.Token ?? CancellationToken.None);
                }
                else if (_receiver != null)
                {
                    ackTask = _receiver.CompleteMessageAsync(message, _shutdownCts?.Token ?? CancellationToken.None);
                }
                else
                {
                    _logger.LogWarning("No receiver available to acknowledge message {MessageId}", message.MessageId);
                    continue;
                }

                acknowledgeTasks.Add(ackTask);
            }

            if (acknowledgeTasks.Count > 0)
            {
                await Task.WhenAll(acknowledgeTasks);
            }
        }

        /// <summary>
        /// Disposes the Service Bus receivers.
        /// </summary>
        private async Task DisposeReceiversAsync()
        {
            if (_sessionReceiver != null)
            {
                try
                {
                    await _sessionReceiver.DisposeAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error disposing session receiver for queue {QueueId}", _queueId);
                }
                finally
                {
                    _sessionReceiver = null;
                }
            }

            // Note: Regular receivers are managed by the connection manager, so we don't dispose them directly
            _receiver = null;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the receiver and releases all resources.
        /// </summary>
        /// <param name="disposing">True if disposing from Dispose method, false if from finalizer.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                _logger.LogDebug("Disposing Service Bus adapter receiver for queue {QueueId}", _queueId);
                
                try
                {
                    _shutdownCts?.Cancel();
                    DisposeReceiversAsync().GetAwaiter().GetResult();
                    _shutdownCts?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error during disposal of Service Bus adapter receiver for queue {QueueId}", _queueId);
                }
                
                _disposed = true;
            }
        }
    }
}