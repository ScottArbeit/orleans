# Service Bus Failure Handling and Dead Letter Queue Configuration

This document explains how to configure Azure Service Bus failure handling, retry behavior, and dead letter queue (DLQ) management for Orleans streaming.

## Overview

The Orleans Service Bus streaming provider implements failure handling that allows Service Bus to manage retries and dead letter queue routing according to its built-in mechanisms. When a consumer exception occurs during message processing, the message is **not completed**, allowing Service Bus to redeliver the message based on the configured `MaxDeliveryCount`.

## Key Behavior

- **Consumer Exceptions**: Messages are abandoned (not completed) when delivery fails
- **Service Bus Retries**: Service Bus automatically retries abandoned messages
- **Dead Letter Queue**: Messages exceeding `MaxDeliveryCount` are moved to DLQ by Service Bus
- **No Subscription Faulting**: Stream subscriptions remain active during delivery failures

## Configuring MaxDeliveryCount

The `MaxDeliveryCount` property controls how many times Service Bus will attempt to deliver a message before moving it to the dead letter queue.

### For Service Bus Queues

Configure `MaxDeliveryCount` when creating the queue:

```csharp
using Azure.Messaging.ServiceBus.Administration;

var adminClient = new ServiceBusAdministrationClient(connectionString);

var queueOptions = new CreateQueueOptions("my-stream-queue")
{
    MaxDeliveryCount = 5, // Retry up to 5 times before DLQ
    LockDuration = TimeSpan.FromMinutes(5), // How long to hold message lock
    DefaultMessageTimeToLive = TimeSpan.FromDays(14) // Message expiration
};

await adminClient.CreateQueueAsync(queueOptions);
```

### For Service Bus Topics/Subscriptions

Configure `MaxDeliveryCount` on the subscription:

```csharp
var subscriptionOptions = new CreateSubscriptionOptions("my-topic", "my-subscription")
{
    MaxDeliveryCount = 3, // Retry up to 3 times before DLQ
    LockDuration = TimeSpan.FromMinutes(2),
    DefaultMessageTimeToLive = TimeSpan.FromDays(7)
};

await adminClient.CreateSubscriptionAsync(subscriptionOptions);
```

### Using Azure Portal

1. Navigate to your Service Bus namespace
2. Select the queue or subscription
3. Go to **Settings** > **Properties**
4. Set **Max delivery count** to desired value (1-2000)
5. Save changes

### Using Azure CLI

```bash
# For queues
az servicebus queue update \
    --resource-group myresourcegroup \
    --namespace-name mynamespace \
    --name myqueue \
    --max-delivery-count 5

# For subscriptions  
az servicebus topic subscription update \
    --resource-group myresourcegroup \
    --namespace-name mynamespace \
    --topic-name mytopic \
    --name mysubscription \
    --max-delivery-count 3
```

## Monitoring Dead Letter Queues

### Checking DLQ Message Count

#### Using Azure Portal
1. Navigate to Service Bus queue/subscription
2. The **Dead letter messages** count appears in the overview
3. Click on **Dead letter messages** to view details

#### Using Service Bus Explorer
```csharp
var adminClient = new ServiceBusAdministrationClient(connectionString);

// For queue DLQ
var queueRuntimeInfo = await adminClient.GetQueueRuntimeInfoAsync("my-queue");
Console.WriteLine($"Dead letter messages: {queueRuntimeInfo.Value.DeadLetterMessageCount}");

// For subscription DLQ  
var subscriptionRuntimeInfo = await adminClient.GetSubscriptionRuntimeInfoAsync("my-topic", "my-subscription");
Console.WriteLine($"Dead letter messages: {subscriptionRuntimeInfo.Value.DeadLetterMessageCount}");
```

### Reading Messages from Dead Letter Queue

```csharp
// Create receiver for dead letter queue
var receiver = serviceBusClient.CreateReceiver("my-queue", new ServiceBusReceiverOptions
{
    SubQueue = SubQueue.DeadLetter
});

// Peek at dead letter messages without removing them
var dlqMessages = await receiver.PeekMessagesAsync(maxMessages: 10);

foreach (var message in dlqMessages)
{
    Console.WriteLine($"Dead letter reason: {message.DeadLetterReason}");
    Console.WriteLine($"Dead letter description: {message.DeadLetterErrorDescription}");
    Console.WriteLine($"Original message ID: {message.MessageId}");
    Console.WriteLine($"Delivery count: {message.DeliveryCount}");
}

// Or receive and process dead letter messages
var receivedMessages = await receiver.ReceiveMessagesAsync(maxMessages: 10, maxWaitTime: TimeSpan.FromSeconds(30));

foreach (var message in receivedMessages)
{
    // Process the dead letter message
    // Complete when done processing
    await receiver.CompleteMessageAsync(message);
}
```

### Dead Letter Queue Management

#### Resubmit Messages from DLQ
```csharp
// Read from DLQ and resubmit to main queue
var dlqReceiver = client.CreateReceiver("my-queue", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
var sender = client.CreateSender("my-queue");

var dlqMessage = await dlqReceiver.ReceiveMessageAsync();
if (dlqMessage != null)
{
    // Create new message with original content
    var newMessage = new ServiceBusMessage(dlqMessage.Body)
    {
        ContentType = dlqMessage.ContentType,
        Subject = dlqMessage.Subject
    };
    
    // Copy application properties
    foreach (var prop in dlqMessage.ApplicationProperties)
    {
        newMessage.ApplicationProperties[prop.Key] = prop.Value;
    }
    
    // Send to main queue and complete DLQ message
    await sender.SendMessageAsync(newMessage);
    await dlqReceiver.CompleteMessageAsync(dlqMessage);
}
```

#### Purge Dead Letter Queue
```csharp
// Warning: This permanently deletes all DLQ messages
var dlqReceiver = client.CreateReceiver("my-queue", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });

while (true)
{
    var messages = await dlqReceiver.ReceiveMessagesAsync(100, TimeSpan.FromSeconds(5));
    if (!messages.Any()) break;
    
    foreach (var message in messages)
    {
        await dlqReceiver.CompleteMessageAsync(message);
    }
}
```

## Orleans Configuration

Configure the Orleans Service Bus streaming provider:

```csharp
siloBuilder.AddServiceBusStreaming("ServiceBusProvider", options =>
{
    options.ConnectionString = "Endpoint=sb://...";
    options.EntityKind = EntityKind.Queue;
    options.QueueName = "my-stream-queue";
    
    // Configure receiver options
    options.Receiver.PrefetchCount = 100;
    options.Receiver.LockAutoRenew = true;
    options.Receiver.LockRenewalDuration = TimeSpan.FromMinutes(10);
});
```

## Failure Handler Customization

The default `ServiceBusStreamFailureHandler` can be customized with DLQ detection callbacks:

```csharp
// In your DI configuration
services.AddSingleton<IStreamFailureHandler>(serviceProvider =>
{
    var logger = serviceProvider.GetRequiredService<ILogger<ServiceBusStreamFailureHandler>>();
    
    // Optional callback for DLQ move detection
    Action<string, StreamId, string> dlqCallback = (messageId, streamId, reason) =>
    {
        // Log or emit metrics when messages move to DLQ
        Console.WriteLine($"Message {messageId} moved to DLQ: {reason}");
        
        // Could emit to monitoring systems, etc.
    };
    
    return new ServiceBusStreamFailureHandler(logger, dlqCallback);
});
```

## Best Practices

1. **Set Appropriate MaxDeliveryCount**: 
   - Start with 3-5 retries for most scenarios
   - Consider message processing time and temporary failure frequency

2. **Monitor DLQ Regularly**:
   - Set up alerts when DLQ message count exceeds thresholds
   - Review DLQ messages to identify systemic issues

3. **Handle Poison Messages**:
   - Implement DLQ processing workflows
   - Consider message content validation
   - Log failed message details for debugging

4. **Lock Duration Configuration**:
   - Set lock duration longer than expected processing time
   - Enable auto-renew for long-running operations

5. **Message TTL**:
   - Set reasonable time-to-live to prevent indefinite message retention
   - Balance between retry opportunities and storage costs

## Troubleshooting

### High DLQ Message Count
- Review consumer code for exceptions
- Check lock duration vs. processing time
- Verify message format compatibility
- Monitor downstream service availability

### Messages Not Retrying
- Verify MaxDeliveryCount > 1
- Check if messages are being completed instead of abandoned
- Review failure handler implementation

### Missing Messages
- Check message TTL settings
- Verify DLQ for expired or failed messages
- Review consumer subscription filters

For more information, see the [Azure Service Bus documentation](https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dead-letter-queues).