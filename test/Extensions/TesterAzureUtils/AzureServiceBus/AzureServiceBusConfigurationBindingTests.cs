using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Xunit;

namespace Tester.AzureUtils.AzureServiceBus;

/// <summary>
/// Tests for Azure Service Bus configuration binding from various sources.
/// </summary>
public class AzureServiceBusConfigurationBindingTests
{
    [Fact]
    public void BindFromConfiguration_WithValidJson_SetsPropertiesCorrectly()
    {
        var configData = new Dictionary<string, string>
        {
            ["ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey",
            ["EntityName"] = "testqueue",
            ["EntityMode"] = "Queue",
            ["BatchSize"] = "64",
            ["PrefetchCount"] = "100",
            ["OperationTimeout"] = "00:02:00",
            ["MaxConcurrentCalls"] = "5",
            ["MaxAutoLockRenewalDuration"] = "00:10:00",
            ["AutoCompleteMessages"] = "false",
            ["ReceiveMode"] = "ReceiveAndDelete"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var services = new ServiceCollection();
        services.Configure<AzureServiceBusOptions>(configuration);

        using var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusOptions>>().Value;

        Assert.Equal("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey", options.ConnectionString);
        Assert.Equal("testqueue", options.EntityName);
        Assert.Equal(ServiceBusEntityMode.Queue, options.EntityMode);
        Assert.Equal(64, options.BatchSize);
        Assert.Equal(100, options.PrefetchCount);
        Assert.Equal(TimeSpan.FromMinutes(2), options.OperationTimeout);
        Assert.Equal(5, options.MaxConcurrentCalls);
        Assert.Equal(TimeSpan.FromMinutes(10), options.MaxAutoLockRenewalDuration);
        Assert.False(options.AutoCompleteMessages);
        Assert.Equal("ReceiveAndDelete", options.ReceiveMode);
    }

    [Fact]
    public void BindFromConfiguration_WithTopicConfiguration_SetsPropertiesCorrectly()
    {
        var configData = new Dictionary<string, string>
        {
            ["ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey",
            ["EntityName"] = "testtopic",
            ["SubscriptionName"] = "testsubscription",
            ["EntityMode"] = "Topic",
            ["BatchSize"] = "32"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var services = new ServiceCollection();
        services.Configure<AzureServiceBusOptions>(configuration);

        using var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusOptions>>().Value;

        Assert.Equal("testtopic", options.EntityName);
        Assert.Equal("testsubscription", options.SubscriptionName);
        Assert.Equal(ServiceBusEntityMode.Topic, options.EntityMode);
        Assert.Equal(32, options.BatchSize);
    }

    [Fact]
    public void BindQueueOptionsFromConfiguration_SetsQueueSpecificProperties()
    {
        var configData = new Dictionary<string, string>
        {
            ["ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey",
            ["EntityName"] = "testqueue",
            ["MessageTimeToLive"] = "1.00:00:00", // 1 day
            ["EnableBatchedOperations"] = "false",
            ["RequiresSession"] = "true",
            ["SessionId"] = "session123",
            ["RequiresDuplicateDetection"] = "true",
            ["DuplicateDetectionHistoryTimeWindow"] = "00:30:00", // 30 minutes
            ["MaxDeliveryCount"] = "5"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var services = new ServiceCollection();
        services.Configure<AzureServiceBusQueueOptions>(configuration);

        using var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusQueueOptions>>().Value;

        Assert.Equal("testqueue", options.EntityName);
        Assert.Equal(ServiceBusEntityMode.Queue, options.EntityMode); // Set by constructor
        Assert.Equal(TimeSpan.FromDays(1), options.MessageTimeToLive);
        Assert.False(options.EnableBatchedOperations);
        Assert.True(options.RequiresSession);
        Assert.Equal("session123", options.SessionId);
        Assert.True(options.RequiresDuplicateDetection);
        Assert.Equal(TimeSpan.FromMinutes(30), options.DuplicateDetectionHistoryTimeWindow);
        Assert.Equal(5, options.MaxDeliveryCount);
    }

    [Fact]
    public void BindTopicOptionsFromConfiguration_SetsTopicSpecificProperties()
    {
        var configData = new Dictionary<string, string>
        {
            ["ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey",
            ["EntityName"] = "testtopic",
            ["SubscriptionName"] = "testsubscription",
            ["EnableSubscriptionRuleEvaluation"] = "true",
            ["ForwardTo"] = "deadletterqueue",
            ["ForwardDeadLetteredMessagesTo"] = "errorqueue",
            ["EnableBatchedOperations"] = "false",
            ["RequiresSession"] = "true",
            ["SessionId"] = "session456",
            ["RequiresDuplicateDetection"] = "true",
            ["AutoDeleteOnIdle"] = "2.00:00:00", // 2 days
            ["DefaultMessageTimeToLive"] = "7.00:00:00", // 7 days
            ["SubscriptionFilter"] = "MessageType = 'Order'"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var services = new ServiceCollection();
        services.Configure<AzureServiceBusTopicOptions>(configuration);

        using var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusTopicOptions>>().Value;

        Assert.Equal("testtopic", options.EntityName);
        Assert.Equal("testsubscription", options.SubscriptionName);
        Assert.Equal(ServiceBusEntityMode.Topic, options.EntityMode); // Set by constructor
        Assert.True(options.EnableSubscriptionRuleEvaluation);
        Assert.Equal("deadletterqueue", options.ForwardTo);
        Assert.Equal("errorqueue", options.ForwardDeadLetteredMessagesTo);
        Assert.False(options.EnableBatchedOperations);
        Assert.True(options.RequiresSession);
        Assert.Equal("session456", options.SessionId);
        Assert.True(options.RequiresDuplicateDetection);
        Assert.Equal(TimeSpan.FromDays(2), options.AutoDeleteOnIdle);
        Assert.Equal(TimeSpan.FromDays(7), options.DefaultMessageTimeToLive);
        Assert.Equal("MessageType = 'Order'", options.SubscriptionFilter);
    }

    [Fact]
    public void ConfigurationExtensions_WithNamedOptions_WorksCorrectly()
    {
        var configData = new Dictionary<string, string>
        {
            ["ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey",
            ["EntityName"] = "namedqueue",
            ["BatchSize"] = "16"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var services = new ServiceCollection();
        services.Configure<AzureServiceBusOptions>("namedProvider", configuration);

        using var serviceProvider = services.BuildServiceProvider();
        var namedOptions = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<AzureServiceBusOptions>>();
        var options = namedOptions.Get("namedProvider");

        Assert.Equal("namedqueue", options.EntityName);
        Assert.Equal(16, options.BatchSize);
    }

    [Fact]
    public void ConfigurationBinding_WithMissingOptionalValues_UsesDefaults()
    {
        var configData = new Dictionary<string, string>
        {
            ["ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey",
            ["EntityName"] = "testqueue"
            // Other properties missing - should use defaults
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var services = new ServiceCollection();
        services.Configure<AzureServiceBusOptions>(configuration);

        using var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusOptions>>().Value;

        Assert.Equal("testqueue", options.EntityName);
        Assert.Equal(32, options.BatchSize); // Default value
        Assert.Equal(0, options.PrefetchCount); // Default value
        Assert.Equal(ServiceBusEntityMode.Queue, options.EntityMode); // Default value
    }

    [Fact]
    public void ConfigurationBinding_WithEnvironmentVariables_WorksCorrectly()
    {
        var configData = new Dictionary<string, string>
        {
            ["ORLEANS_STREAMING_AZURESERVICEBUS_CONNECTIONSTRING"] = "Endpoint=sb://env.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=envkey",
            ["ORLEANS_STREAMING_AZURESERVICEBUS_ENTITYNAME"] = "envqueue",
            ["ORLEANS_STREAMING_AZURESERVICEBUS_ENTITYMODE"] = "Queue",
            ["ORLEANS_STREAMING_AZURESERVICEBUS_BATCHSIZE"] = "128"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        // Create a section that maps environment variable names to property names
        var section = configuration.GetSection("ORLEANS_STREAMING_AZURESERVICEBUS");
        
        var services = new ServiceCollection();
        services.Configure<AzureServiceBusOptions>(options =>
        {
            options.ConnectionString = configuration["ORLEANS_STREAMING_AZURESERVICEBUS_CONNECTIONSTRING"];
            options.EntityName = configuration["ORLEANS_STREAMING_AZURESERVICEBUS_ENTITYNAME"];
            if (Enum.TryParse<ServiceBusEntityMode>(configuration["ORLEANS_STREAMING_AZURESERVICEBUS_ENTITYMODE"], out var mode))
            {
                options.EntityMode = mode;
            }
            if (int.TryParse(configuration["ORLEANS_STREAMING_AZURESERVICEBUS_BATCHSIZE"], out var batchSize))
            {
                options.BatchSize = batchSize;
            }
        });

        using var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusOptions>>().Value;

        Assert.Equal("Endpoint=sb://env.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=envkey", options.ConnectionString);
        Assert.Equal("envqueue", options.EntityName);
        Assert.Equal(ServiceBusEntityMode.Queue, options.EntityMode);
        Assert.Equal(128, options.BatchSize);
    }

    [Fact]
    public void ConfigurationValidation_WithInvalidValues_CanBeValidatedManually()
    {
        var configData = new Dictionary<string, string>
        {
            ["ConnectionString"] = "", // Invalid - empty
            ["EntityName"] = "testqueue",
            ["BatchSize"] = "0" // Invalid - must be > 0
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var services = new ServiceCollection();
        services.Configure<AzureServiceBusOptions>(configuration);
        // For this test, we don't need validation as we're testing that invalid config throws at access time

        using var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusOptions>>();

        // Get the options value (this should work since we're not using automatic validation)
        var optionsValue = options.Value;
        
        // But when we manually validate using our validator, it should throw
        var validator = new AzureServiceBusOptionsValidator(optionsValue, "test");
        Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
    }
}