using System;
using Orleans.Configuration;
using Orleans.Runtime;
using Xunit;

namespace Tester.AzureUtils.AzureServiceBus;

/// <summary>
/// Tests for Azure Service Bus configuration validation.
/// </summary>
public class AzureServiceBusValidationTests
{
    private const string ValidConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey";
    private const string ValidEntityName = "testqueue";
    private const string ValidSubscriptionName = "testsubscription";

    [Fact]
    public void ValidConfiguration_PassesValidation()
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            EntityMode = ServiceBusEntityMode.Queue
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        validator.ValidateConfiguration();
    }

    [Fact]
    public void ValidTopicConfiguration_PassesValidation()
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            SubscriptionName = ValidSubscriptionName,
            EntityMode = ServiceBusEntityMode.Topic
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        validator.ValidateConfiguration();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void EmptyConnectionString_ThrowsConfigurationException(string connectionString)
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = connectionString,
            EntityName = ValidEntityName
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("ConnectionString", exception.Message);
        Assert.Contains("required", exception.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void EmptyEntityName_ThrowsConfigurationException(string entityName)
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = entityName
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("EntityName", exception.Message);
        Assert.Contains("required", exception.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void TopicModeWithEmptySubscriptionName_ThrowsConfigurationException(string subscriptionName)
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            SubscriptionName = subscriptionName,
            EntityMode = ServiceBusEntityMode.Topic
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("SubscriptionName", exception.Message);
        Assert.Contains("required when", exception.Message);
        Assert.Contains("Topic", exception.Message);
    }

    [Fact]
    public void QueueModeWithEmptySubscriptionName_PassesValidation()
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            SubscriptionName = "", // Empty is OK for Queue mode
            EntityMode = ServiceBusEntityMode.Queue
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        validator.ValidateConfiguration();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1001)]
    public void InvalidBatchSize_ThrowsConfigurationException(int batchSize)
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            BatchSize = batchSize
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("BatchSize", exception.Message);
        Assert.Contains("between 1 and 1000", exception.Message);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(32)]
    [InlineData(1000)]
    public void ValidBatchSize_PassesValidation(int batchSize)
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            BatchSize = batchSize
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        validator.ValidateConfiguration();
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(1001)]
    public void InvalidPrefetchCount_ThrowsConfigurationException(int prefetchCount)
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            PrefetchCount = prefetchCount
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("PrefetchCount", exception.Message);
        Assert.Contains("between 0 and 1000", exception.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(10)]
    [InlineData(1000)]
    public void ValidPrefetchCount_PassesValidation(int prefetchCount)
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            PrefetchCount = prefetchCount
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        validator.ValidateConfiguration();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void InvalidMaxConcurrentCalls_ThrowsConfigurationException(int maxConcurrentCalls)
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            MaxConcurrentCalls = maxConcurrentCalls
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("MaxConcurrentCalls", exception.Message);
        Assert.Contains("greater than 0", exception.Message);
    }

    [Fact]
    public void NegativeOperationTimeout_ThrowsConfigurationException()
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            OperationTimeout = TimeSpan.FromSeconds(-1)
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("OperationTimeout", exception.Message);
        Assert.Contains("positive", exception.Message);
    }

    [Fact]
    public void ZeroOperationTimeout_ThrowsConfigurationException()
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            OperationTimeout = TimeSpan.Zero
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("OperationTimeout", exception.Message);
        Assert.Contains("positive", exception.Message);
    }

    [Fact]
    public void NegativeMaxAutoLockRenewalDuration_ThrowsConfigurationException()
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = ValidConnectionString,
            EntityName = ValidEntityName,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(-1)
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("MaxAutoLockRenewalDuration", exception.Message);
        Assert.Contains("positive", exception.Message);
    }

    [Theory]
    [InlineData("InvalidConnectionString")]
    [InlineData("NoEndpoint=value")]
    [InlineData("Endpoint=")]
    public void InvalidConnectionStringFormat_ThrowsConfigurationException(string connectionString)
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = connectionString,
            EntityName = ValidEntityName
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("Connection string", exception.Message);
    }

    [Theory]
    [InlineData("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey")]
    [InlineData("Endpoint=sb://test.servicebus.windows.net/;SharedAccessSignature=SharedAccessSignature=value")]
    [InlineData("Endpoint=sb://test.servicebus.windows.net/")] // Managed identity scenario
    public void ValidConnectionStringFormats_PassValidation(string connectionString)
    {
        var options = new AzureServiceBusOptions
        {
            ConnectionString = connectionString,
            EntityName = ValidEntityName
        };

        var validator = new AzureServiceBusOptionsValidator(options, "test");

        validator.ValidateConfiguration();
    }

    [Fact]
    public void ValidatorConstructor_WithNullOptions_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new AzureServiceBusOptionsValidator(null, "test"));
    }

    [Fact]
    public void ValidatorConstructor_WithNullName_DoesNotThrow()
    {
        var options = new AzureServiceBusOptions();
        
        var validator = new AzureServiceBusOptionsValidator(options, null);
        Assert.NotNull(validator);
    }
}
