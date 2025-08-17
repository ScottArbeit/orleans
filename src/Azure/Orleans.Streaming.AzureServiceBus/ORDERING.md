# Azure Service Bus Streaming - Ordering Guarantees

This document describes the ordering guarantees and concurrency controls in the Orleans Azure Service Bus streaming provider.

## Default Behavior

By default, the Service Bus streaming provider is configured to **preserve message ordering**:

- `MaxConcurrentHandlers` = 1 (default)
- Messages are processed sequentially within each stream
- Ordering is maintained from publisher to subscriber

## Concurrency vs. Ordering Trade-offs

### MaxConcurrentHandlers = 1 (Recommended for Ordering)

**Pros:**
- ✅ **Message ordering is preserved** 
- ✅ Predictable behavior
- ✅ Suitable for scenarios requiring strict ordering (e.g., financial transactions, audit logs)

**Cons:**
- ⚠️ Lower throughput
- ⚠️ Potential backpressure under high load

### MaxConcurrentHandlers > 1 (Higher Throughput)

**Pros:**
- ✅ **Higher throughput** 
- ✅ Better resource utilization
- ✅ Reduced backpressure

**Cons:**
- ⚠️ **Message ordering is NOT guaranteed**
- ⚠️ Messages may be processed out of order
- ⚠️ Complex error handling scenarios

## Session-Based Ordering (Future Enhancement)

The `SessionIdStrategy.UseStreamId` option provides a path for maintaining ordering with higher concurrency:

**How it works:**
- Each stream ID becomes a Service Bus session ID
- Service Bus ensures per-session ordering automatically
- Multiple sessions can be processed concurrently

**Trade-offs:**
- ✅ Per-stream ordering maintained even with higher concurrency
- ⚠️ Requires session-enabled Service Bus entities (queues/subscriptions)
- ⚠️ Additional complexity in message processing
- ⚠️ May reduce throughput for high-volume scenarios

**Current Status:** 
Session support is documented but deferred in the MVP implementation.

## Configuration Examples

### Strict Ordering (Default)
```csharp
builder.AddServiceBusStreams("MyProvider", options =>
{
    options.ConnectionString = connectionString;
    options.QueueName = "my-queue";
    options.Receiver.MaxConcurrentHandlers = 1; // Default
});
```

### High Throughput (Ordering Not Guaranteed)
```csharp
builder.AddServiceBusStreams("MyProvider", options =>
{
    options.ConnectionString = connectionString;
    options.QueueName = "my-queue";
    options.Receiver.MaxConcurrentHandlers = 8; // Emits warning
});
```

### Session-Based Ordering (Future)
```csharp
builder.AddServiceBusStreams("MyProvider", options =>
{
    options.ConnectionString = connectionString;
    options.QueueName = "my-session-queue"; // Must support sessions
    options.Publisher.SessionIdStrategy = SessionIdStrategy.UseStreamId;
    options.Receiver.MaxConcurrentHandlers = 4; // Safe with sessions
});
```

## Warnings and Logging

When `MaxConcurrentHandlers` is set to a value greater than 1, the provider will log a warning:

```
ServiceBus adapter receiver for queue [QueueId] is configured with MaxConcurrentHandlers = 4. 
This breaks message ordering guarantees. For strict ordering, use MaxConcurrentHandlers = 1.
```

## Recommendations

1. **Use MaxConcurrentHandlers = 1** when message ordering is critical
2. **Use MaxConcurrentHandlers > 1** when throughput is more important than ordering
3. **Consider SessionIdStrategy.UseStreamId** for future implementations requiring both ordering and throughput
4. **Monitor logs** for ordering-related warnings
5. **Test thoroughly** when changing concurrency settings

## Testing Ordering Behavior

The test suite includes validation for:
- Default ordering behavior with `MaxConcurrentHandlers = 1`
- Validator acceptance of higher concurrency values
- Warning messages for ordering trade-offs
- Documentation of session strategy trade-offs