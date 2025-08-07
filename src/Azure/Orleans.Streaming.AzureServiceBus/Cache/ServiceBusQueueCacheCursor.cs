using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Cache;

/// <summary>
/// Cache cursor for Azure Service Bus queue cache.
/// Provides efficient navigation through cached messages for a specific stream.
/// </summary>
internal sealed partial class ServiceBusQueueCacheCursor : IQueueCacheCursor
{
    private readonly ServiceBusQueueCache _cache;
    private readonly StreamId _streamId;
    private readonly ILogger _logger;
    private StreamSequenceToken? _currentToken;
    private readonly object _lockObject = new();
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusQueueCacheCursor"/> class.
    /// </summary>
    /// <param name="cache">The cache instance.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="startToken">The starting token.</param>
    /// <param name="logger">The logger.</param>
    public ServiceBusQueueCacheCursor(
        ServiceBusQueueCache cache,
        StreamId streamId,
        StreamSequenceToken? startToken,
        ILogger logger)
    {
        _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        _streamId = streamId;
        _currentToken = startToken;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        LogDebugCursorCreated(_streamId, _currentToken);
    }

    /// <inheritdoc />
    public bool MoveNext()
    {
        if (_disposed)
            return false;

        lock (_lockObject)
        {
            try
            {
                // Get messages from cache starting from current position
                var messages = _cache.GetMessages(_currentToken, maxCount: 1)
                    .Where(entry => IsMessageForStream(entry))
                    .Take(1)
                    .ToList();

                if (messages.Count > 0)
                {
                    var nextEntry = messages[0];
                    _currentToken = nextEntry.SequenceToken;
                    
                    LogDebugCursorMoved(_streamId, _currentToken);
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                LogWarningCursorMoveNextFailed(ex, _streamId, _currentToken);
                return false;
            }
        }
    }

    /// <inheritdoc />
    public IBatchContainer GetCurrent(out Exception? exception)
    {
        exception = null;

        if (_disposed)
        {
            exception = new ObjectDisposedException(nameof(ServiceBusQueueCacheCursor));
            return null!;
        }

        lock (_lockObject)
        {
            try
            {
                if (_currentToken is null)
                {
                    exception = new InvalidOperationException("Cursor is not positioned on a valid message. Call MoveNext() first.");
                    return null!;
                }

                // Find the current message in cache
                var currentMessage = _cache.GetMessages(_currentToken, maxCount: 1)
                    .Where(entry => IsMessageForStream(entry) && 
                                  entry.SequenceToken.Equals(_currentToken))
                    .FirstOrDefault();

                if (currentMessage is not null)
                {
                    return currentMessage.BatchContainer;
                }

                exception = new InvalidOperationException($"Current message not found in cache for token {_currentToken}");
                return null!;
            }
            catch (Exception ex)
            {
                LogWarningCursorGetCurrentFailed(ex, _streamId, _currentToken);
                exception = ex;
                return null!;
            }
        }
    }

    /// <inheritdoc />
    public void Refresh(StreamSequenceToken? token)
    {
        if (_disposed)
            return;

        lock (_lockObject)
        {
            _currentToken = token;
            LogDebugCursorRefreshed(_streamId, _currentToken);
        }
    }

    /// <inheritdoc />
    public void RecordDeliveryFailure()
    {
        if (_disposed)
            return;

        lock (_lockObject)
        {
            LogDebugDeliveryFailureRecorded(_streamId, _currentToken);
            
            // Mark current message as having delivery failure
            // This could be used for dead letter queue scenarios
            // For now, we just log it
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;

        lock (_lockObject)
        {
            if (_disposed)
                return;

            _disposed = true;
            LogDebugCursorDisposed(_streamId);
        }
    }

    private bool IsMessageForStream(MessageCacheEntry entry)
    {
        // Check if this message belongs to our stream
        // This is a simplified implementation - in a real scenario,
        // we might need to examine the batch container's stream data
        try
        {
            return entry.BatchContainer.StreamId.Equals(_streamId);
        }
        catch
        {
            // If we can't determine the stream, assume it's not ours
            return false;
        }
    }

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Created cache cursor for stream '{StreamId}' starting at token '{StartToken}'"
    )]
    private partial void LogDebugCursorCreated(StreamId streamId, StreamSequenceToken? startToken);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Moved cursor for stream '{StreamId}' to token '{Token}'"
    )]
    private partial void LogDebugCursorMoved(StreamId streamId, StreamSequenceToken? token);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Refreshed cursor for stream '{StreamId}' to token '{Token}'"
    )]
    private partial void LogDebugCursorRefreshed(StreamId streamId, StreamSequenceToken? token);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Recorded delivery failure for stream '{StreamId}' at token '{Token}'"
    )]
    private partial void LogDebugDeliveryFailureRecorded(StreamId streamId, StreamSequenceToken? token);

    [LoggerMessage(
        Level = LogLevel.Debug,
        Message = "Disposed cache cursor for stream '{StreamId}'"
    )]
    private partial void LogDebugCursorDisposed(StreamId streamId);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Failed to move cursor for stream '{StreamId}' from token '{Token}'"
    )]
    private partial void LogWarningCursorMoveNextFailed(Exception exception, StreamId streamId, StreamSequenceToken? token);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Failed to get current message for stream '{StreamId}' at token '{Token}'"
    )]
    private partial void LogWarningCursorGetCurrentFailed(Exception exception, StreamId streamId, StreamSequenceToken? token);
}