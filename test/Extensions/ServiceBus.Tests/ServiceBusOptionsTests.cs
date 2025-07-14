using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Runtime;
using Xunit;

namespace ServiceBus.Tests;

[TestCategory("BVT")]
[TestCategory("ServiceBus")]
public class ServiceBusOptionsTests
{
    [Fact]
    public void DefaultValues_ShouldBeSet()
    {
        var options = new ServiceBusOptions();

        Assert.Equal(ServiceBusEntityType.Queue, options.EntityType);
        Assert.Equal(8, options.PartitionCount);
        Assert.Equal(TimeSpan.FromSeconds(5), options.MaxWaitTime);
        Assert.Equal("orleans-stream", options.QueueNamePrefix);
        Assert.Equal(0, options.PrefetchCount);
        Assert.Equal(1, options.ReceiveBatchSize);
        Assert.Equal(Environment.ProcessorCount, options.MaxConcurrentCalls);
        Assert.Equal(10, options.MaxDeliveryAttempts);
        Assert.Null(options.ConnectionString);
        Assert.Null(options.FullyQualifiedNamespace);
        Assert.Null(options.TokenCredential);
        Assert.Null(options.EntityName);
        Assert.Null(options.EntityNames);
        Assert.Null(options.ServiceBusClient);
    }

    [Fact]
    public void ConfigureServiceBusClient_WithConnectionString_ShouldSetProperties()
    {
        var options = new ServiceBusOptions();
        var connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test";

        options.ConfigureServiceBusClient(connectionString);

        Assert.Equal(connectionString, options.ConnectionString);
        // CreateClient is internal, so we can't directly test it, but we can test that configuration doesn't throw
    }

    [Fact]
    public void ConfigureServiceBusClient_WithNullConnectionString_ShouldThrow()
    {
        var options = new ServiceBusOptions();

        Assert.Throws<ArgumentNullException>(() => options.ConfigureServiceBusClient(null!));
        Assert.Throws<ArgumentNullException>(() => options.ConfigureServiceBusClient(""));
        Assert.Throws<ArgumentNullException>(() => options.ConfigureServiceBusClient("   "));
    }

    [Fact]
    public void ConfigureServiceBusClient_WithNamespaceAndCredential_ShouldSetProperties()
    {
        var options = new ServiceBusOptions();
        var fullyQualifiedNamespace = "test.servicebus.windows.net";
        var credential = new DefaultAzureCredential();

        options.ConfigureServiceBusClient(fullyQualifiedNamespace, credential);

        Assert.Equal(fullyQualifiedNamespace, options.FullyQualifiedNamespace);
        Assert.Equal(credential, options.TokenCredential);
    }

    [Fact]
    public void ConfigureServiceBusClient_WithNullNamespace_ShouldThrow()
    {
        var options = new ServiceBusOptions();
        var credential = new DefaultAzureCredential();

        Assert.Throws<ArgumentNullException>(() => options.ConfigureServiceBusClient(null!, credential));
        Assert.Throws<ArgumentNullException>(() => options.ConfigureServiceBusClient("", credential));
        Assert.Throws<ArgumentNullException>(() => options.ConfigureServiceBusClient("   ", credential));
    }

    [Fact]
    public void ConfigureServiceBusClient_WithNullCredential_ShouldThrow()
    {
        var options = new ServiceBusOptions();
        var fullyQualifiedNamespace = "test.servicebus.windows.net";
        TokenCredential credential = null;

        Assert.Throws<ArgumentNullException>(() => options.ConfigureServiceBusClient(fullyQualifiedNamespace, credential));
    }

    [Fact]
    public void ConfigureServiceBusClient_WithCallback_ShouldNotThrow()
    {
        var options = new ServiceBusOptions();
        var client = new ServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
        
        // Should not throw
        options.ConfigureServiceBusClient(() => Task.FromResult(client));
    }

    [Fact]
    public void ConfigureServiceBusClient_WithNullCallback_ShouldThrow()
    {
        var options = new ServiceBusOptions();

        Assert.Throws<ArgumentNullException>(() => options.ConfigureServiceBusClient(null!));
    }

    [Fact]
    public void ServiceBusClient_PropertySetter_ShouldSetValue()
    {
        var options = new ServiceBusOptions();
        var client = new ServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");

        options.ServiceBusClient = client;

        Assert.Equal(client, options.ServiceBusClient);
    }

    [Fact]
    public void ActivitySource_ShouldBeConfigured()
    {
        Assert.NotNull(ServiceBusOptions.ActivitySource);
        Assert.Equal("Orleans.ServiceBus", ServiceBusOptions.ActivitySource.Name);
    }

    [Fact]
    public void EntityType_Values_ShouldBeValid()
    {
        Assert.Equal(0, (int)ServiceBusEntityType.Queue);
        Assert.Equal(1, (int)ServiceBusEntityType.Topic);
    }
}

[TestCategory("BVT")]
[TestCategory("ServiceBus")]
public class ServiceBusOptionsValidatorTests
{
    [Fact]
    public void ValidateConfiguration_WithValidOptions_ShouldNotThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        // Should not throw
        validator.ValidateConfiguration();
    }

    [Fact]
    public void ValidateConfiguration_WithoutCredentials_ShouldThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt => { });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("No credentials specified", exception.Message);
        Assert.Contains("test", exception.Message);
    }

    [Fact]
    public void ValidateConfiguration_WithInvalidPartitionCount_ShouldThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.PartitionCount = 0;
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("PartitionCount", exception.Message);
        Assert.Contains("must be greater than 0", exception.Message);
    }

    [Fact]
    public void ValidateConfiguration_WithInvalidPrefetchCount_ShouldThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.PrefetchCount = -1;
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("PrefetchCount", exception.Message);
        Assert.Contains("must be greater than or equal to 0", exception.Message);
    }

    [Fact]
    public void ValidateConfiguration_WithInvalidMaxDeliveryAttempts_ShouldThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.MaxDeliveryAttempts = 0;
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("MaxDeliveryAttempts", exception.Message);
        Assert.Contains("must be greater than 0", exception.Message);
    }

    [Fact]
    public void ValidateConfiguration_WithEmptyQueueNamePrefix_ShouldThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.QueueNamePrefix = "";
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("QueueNamePrefix", exception.Message);
        Assert.Contains("cannot be null or empty", exception.Message);
    }

    [Fact]
    public void ValidateConfiguration_WithEmptyEntityNames_ShouldThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.EntityNames = new List<string>();
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("EntityNames", exception.Message);
        Assert.Contains("cannot be empty when specified", exception.Message);
    }

    [Fact]
    public void ValidateConfiguration_WithBothEntityNameAndEntityNames_ShouldThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.EntityName = "test-entity";
            opt.EntityNames = new List<string> { "entity1", "entity2" };
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("Cannot specify both EntityName and EntityNames", exception.Message);
    }
}