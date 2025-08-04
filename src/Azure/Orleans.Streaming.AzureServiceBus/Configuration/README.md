# Azure Service Bus Configuration for Orleans Streaming

This document provides examples of how to configure Azure Service Bus streaming providers using the new configuration classes.

## Configuration Classes

### AzureServiceBusOptions
Base configuration class for common Azure Service Bus streaming settings.

### AzureServiceBusQueueOptions  
Configuration class for Azure Service Bus Queue streaming with queue-specific settings.

### AzureServiceBusTopicOptions
Configuration class for Azure Service Bus Topic/Subscription streaming with topic-specific settings.

## Configuration Examples

### appsettings.json Configuration

#### Queue Configuration
```json
{
  "Orleans": {
    "Streaming": {
      "AzureServiceBus": {
        "MyQueueProvider": {
          "ConnectionString": "Endpoint=sb://myservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mykey",
          "EntityName": "myqueue",
          "EntityMode": "Queue",
          "BatchSize": 64,
          "PrefetchCount": 10,
          "OperationTimeout": "00:02:00",
          "MaxConcurrentCalls": 4,
          "AutoCompleteMessages": true
        }
      }
    }
  }
}
```

#### Topic Configuration
```json
{
  "Orleans": {
    "Streaming": {
      "AzureServiceBus": {
        "MyTopicProvider": {
          "ConnectionString": "Endpoint=sb://myservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mykey",
          "EntityName": "mytopic",
          "SubscriptionName": "mysubscription",
          "EntityMode": "Topic",
          "BatchSize": 32,
          "EnableSubscriptionRuleEvaluation": true,
          "SubscriptionFilter": "MessageType = 'Order'"
        }
      }
    }
  }
}
```

### Environment Variables Configuration

```bash
# Queue configuration via environment variables
ORLEANS_STREAMING_AZURESERVICEBUS_CONNECTIONSTRING="Endpoint=sb://myservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mykey"
ORLEANS_STREAMING_AZURESERVICEBUS_ENTITYNAME="myqueue"
ORLEANS_STREAMING_AZURESERVICEBUS_ENTITYMODE="Queue"
ORLEANS_STREAMING_AZURESERVICEBUS_BATCHSIZE="64"
```

### Programmatic Configuration

#### Queue Configuration
```csharp
services.Configure<AzureServiceBusQueueOptions>("MyQueueProvider", options =>
{
    options.ConnectionString = "Endpoint=sb://myservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mykey";
    options.EntityName = "myqueue";
    options.BatchSize = 64;
    options.PrefetchCount = 10;
    options.RequiresSession = false;
    options.EnableBatchedOperations = true;
    options.MaxDeliveryCount = 10;
});
```

#### Topic Configuration
```csharp
services.Configure<AzureServiceBusTopicOptions>("MyTopicProvider", options =>
{
    options.ConnectionString = "Endpoint=sb://myservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mykey";
    options.EntityName = "mytopic";
    options.SubscriptionName = "mysubscription";
    options.BatchSize = 32;
    options.EnableSubscriptionRuleEvaluation = true;
    options.SubscriptionFilter = "MessageType = 'Order'";
    options.ForwardTo = "deadletterqueue";
});
```

## Configuration Validation

All configuration classes include comprehensive validation:

- **Connection String**: Must be valid Azure Service Bus format with Endpoint
- **Entity Name**: Required, cannot be null or empty
- **Subscription Name**: Required for Topic mode
- **Batch Size**: Must be between 1 and 1000
- **Prefetch Count**: Must be between 0 and 1000
- **Timeouts**: Must be positive values
- **Max Concurrent Calls**: Must be greater than 0

## Default Values

### Common Options (AzureServiceBusOptions)
- BatchSize: 32
- PrefetchCount: 0
- OperationTimeout: 60 seconds
- MaxConcurrentCalls: 1
- MaxAutoLockRenewalDuration: 5 minutes
- AutoCompleteMessages: true
- ReceiveMode: "PeekLock"

### Queue Options (AzureServiceBusQueueOptions)
- EnableBatchedOperations: true
- RequiresSession: false
- RequiresDuplicateDetection: false
- DuplicateDetectionHistoryTimeWindow: 10 minutes
- MaxDeliveryCount: 10

### Topic Options (AzureServiceBusTopicOptions)
- EnableSubscriptionRuleEvaluation: false
- EnableBatchedOperations: true
- RequiresSession: false
- RequiresDuplicateDetection: false
- DuplicateDetectionHistoryTimeWindow: 10 minutes
- MaxDeliveryCount: 10

## Azure Key Vault Integration

Configuration supports Azure Key Vault for connection strings:

```json
{
  "Orleans": {
    "Streaming": {
      "AzureServiceBus": {
        "MyProvider": {
          "ConnectionString": "@Microsoft.KeyVault(SecretUri=https://myvault.vault.azure.net/secrets/ServiceBusConnectionString/)",
          "EntityName": "myqueue",
          "EntityMode": "Queue"
        }
      }
    }
  }
}
```

## Managed Identity Support

For managed identity scenarios, connection strings with only endpoints are supported:

```json
{
  "ConnectionString": "Endpoint=sb://myservicebus.servicebus.windows.net/"
}
```

The Azure Service Bus SDK will handle authentication via managed identity when no access keys are provided.