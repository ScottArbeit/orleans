using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Runtime;
using Xunit;

namespace ServiceBus.Tests;

/// <summary>
/// Tests for advanced ServiceBus options validation.
/// </summary>
[TestCategory("BVT")]
[TestCategory("ServiceBus")]
public class AdvancedOptionsTests
{
    [Fact]
    public void ValidateConfiguration_WithValidReceiveBatchSize_ShouldNotThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.PrefetchCount = 10;
            opt.ReceiveBatchSize = 5;
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        // Should not throw
        validator.ValidateConfiguration();
    }

    [Fact]
    public void ValidateConfiguration_WithReceiveBatchSizeEqualToPrefetchCount_ShouldNotThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.PrefetchCount = 10;
            opt.ReceiveBatchSize = 10;
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        // Should not throw
        validator.ValidateConfiguration();
    }

    [Fact]
    public void ValidateConfiguration_WithPrefetchCountZero_ShouldNotThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.PrefetchCount = 0;
            opt.ReceiveBatchSize = 10;
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        // Should not throw when PrefetchCount is 0
        validator.ValidateConfiguration();
    }

    [Fact]
    public void ValidateConfiguration_WithReceiveBatchSizeGreaterThanPrefetchCount_ShouldThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.PrefetchCount = 5;
            opt.ReceiveBatchSize = 10;
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("ReceiveBatchSize", exception.Message);
        Assert.Contains("must be less than or equal to PrefetchCount", exception.Message);
        Assert.Contains("ReceiveBatchSize=10", exception.Message);
        Assert.Contains("PrefetchCount=5", exception.Message);
    }

    [Fact]
    public void ValidateConfiguration_WithInvalidReceiveBatchSize_ShouldThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.ReceiveBatchSize = 0;
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("ReceiveBatchSize", exception.Message);
        Assert.Contains("must be greater than 0", exception.Message);
    }

    [Fact]
    public void ValidateConfiguration_WithNegativeReceiveBatchSize_ShouldThrow()
    {
        var services = new ServiceCollection();
        services.Configure<ServiceBusOptions>("test", opt =>
        {
            opt.ConfigureServiceBusClient("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test");
            opt.ReceiveBatchSize = -1;
        });
        var serviceProvider = services.BuildServiceProvider();
        
        var validator = ServiceBusOptionsValidator.Create(serviceProvider, "test");

        var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
        Assert.Contains("ReceiveBatchSize", exception.Message);
        Assert.Contains("must be greater than 0", exception.Message);
    }

    [Fact]
    public void ServiceBusOptions_DefaultValues_ShouldBeValid()
    {
        var options = new ServiceBusOptions();

        Assert.Equal(0, options.PrefetchCount);
        Assert.Equal(1, options.ReceiveBatchSize);
    }
}