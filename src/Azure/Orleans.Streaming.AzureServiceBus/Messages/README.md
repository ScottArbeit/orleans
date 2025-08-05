# Azure Service Bus Messages

This directory contains the message structure implementation for Azure Service Bus streaming provider integration with Orleans.

## Overview

The message structure provides Orleans streaming compatibility while preserving Azure Service Bus message properties and supporting efficient batch operations.

## Key Classes

### ServiceBusMessageMetadata
Preserves Azure Service Bus message properties including:
- MessageId, CorrelationId, SessionId
- EnqueuedTime, ReplyTo, Subject
- ApplicationProperties, PartitionKey, TimeToLive

### AzureServiceBusMessage
Main IBatchContainer implementation featuring:
- Orleans serialization with `[GenerateSerializer]`
- ReadOnlyMemory<byte> for efficient payload handling
- Request context propagation
- Standard EventSequenceTokenV2 for sequence tracking
- Immutable design for thread safety

### AzureServiceBusBatchContainer
Implements IBatchContainerBatch for:
- Efficient batch processing
- Batch splitting capabilities
- Stream validation and consistency

### AzureServiceBusMessageFactory
Provides creation methods for:
- Converting Orleans stream events to messages
- Creating Service Bus messages for publishing
- Extracting data from received Service Bus messages
- Batch creation and validation

## Features

- **Orleans Compatibility**: Full IBatchContainer implementation
- **Serialization**: Orleans native serialization with proper attributes
- **Performance**: Zero-copy payload handling with ReadOnlyMemory<byte>
- **Batching**: Efficient batch operations with splitting support
- **Metadata**: Complete Azure Service Bus property preservation
- **Context**: Request context propagation for distributed tracing
- **Validation**: Message size and structure validation
- **Immutability**: Thread-safe immutable design

## Usage Example

```csharp
// Create message from stream event
var factory = new AzureServiceBusMessageFactory(serializer);
var message = factory.CreateFromStreamEvent(streamId, eventData, requestContext);

// Create Service Bus message for publishing
var serviceBusMessage = factory.CreateServiceBusMessage(streamId, eventData);

// Create from received Service Bus message
var receivedMessage = factory.CreateFromServiceBusMessage(serviceBusReceivedMessage, streamId);

// Create batch
var batch = factory.CreateBatch(streamEvents, "batch-id");

// Extract events
var events = message.GetEvents<MyEventType>();
```

## Testing

Comprehensive test suite covers:
- Serialization round-trip testing
- Batch container functionality
- Message factory operations
- Integration scenarios
- Error handling and validation

See the test files in the Messages directory for detailed examples.