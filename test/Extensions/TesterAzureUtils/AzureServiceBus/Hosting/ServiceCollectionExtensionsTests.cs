using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus.Providers;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests.Hosting;

/// <summary>
/// Tests for Azure Service Bus ServiceCollection extensions.
/// </summary>
public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddAzureServiceBusStreaming_ValidConfiguration_RegistersServices()
    {
        // Arrange
        var services = new ServiceCollection();
        const string providerName = "test-provider";
        
        // Act
        services.AddAzureServiceBusStreaming(providerName, options =>
        {
            options.ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=value";
            options.EntityName = "testqueue";
            options.EntityMode = ServiceBusEntityMode.Queue;
        });
        
        // Assert
        Assert.Contains(services, service => service.ServiceType == typeof(IConfigurationValidator));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_NullServices_ThrowsArgumentNullException()
    {
        // Arrange
        IServiceCollection services = null;
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            services.AddAzureServiceBusStreaming("test", options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_NullName_ThrowsArgumentException()
    {
        // Arrange
        var services = new ServiceCollection();
        
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            services.AddAzureServiceBusStreaming(null, options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_EmptyName_ThrowsArgumentException()
    {
        // Arrange
        var services = new ServiceCollection();
        
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            services.AddAzureServiceBusStreaming("", options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_WhitespaceName_ThrowsArgumentException()
    {
        // Arrange
        var services = new ServiceCollection();
        
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            services.AddAzureServiceBusStreaming("   ", options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_NullConfigureAction_ThrowsArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();
        Action<AzureServiceBusOptions> configure = null;
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            services.AddAzureServiceBusStreaming("test", configure));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_WithIConfiguration_RegistersServices()
    {
        // Arrange
        var services = new ServiceCollection();
        var configData = new Dictionary<string, string>
        {
            {"ConnectionString", "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=value"},
            {"EntityName", "testqueue"},
            {"EntityMode", "Queue"}
        };
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();
        
        // Act
        services.AddAzureServiceBusStreaming("test-provider", configuration);
        
        // Assert
        Assert.Contains(services, service => service.ServiceType == typeof(IConfigurationValidator));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_NullConfiguration_ThrowsArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();
        IConfiguration configuration = null;
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            services.AddAzureServiceBusStreaming("test", configuration));
    }

    [Fact]
    public void AddAzureServiceBusQueueStreaming_ValidParameters_RegistersServices()
    {
        // Arrange
        var services = new ServiceCollection();
        const string providerName = "test-provider";
        const string connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=value";
        const string queueName = "testqueue";
        
        // Act
        services.AddAzureServiceBusQueueStreaming(providerName, connectionString, queueName);
        
        // Assert
        Assert.Contains(services, service => service.ServiceType == typeof(IConfigurationValidator));
    }

    [Theory]
    [InlineData(null, "conn", "queue")]
    [InlineData("", "conn", "queue")]
    [InlineData("   ", "conn", "queue")]
    [InlineData("name", null, "queue")]
    [InlineData("name", "", "queue")]
    [InlineData("name", "   ", "queue")]
    [InlineData("name", "conn", null)]
    [InlineData("name", "conn", "")]
    [InlineData("name", "conn", "   ")]
    public void AddAzureServiceBusQueueStreaming_InvalidParameters_ThrowsArgumentException(
        string name, string connectionString, string queueName)
    {
        // Arrange
        var services = new ServiceCollection();
        
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            services.AddAzureServiceBusQueueStreaming(name, connectionString, queueName));
    }

    [Fact]
    public void AddAzureServiceBusTopicStreaming_ValidParameters_RegistersServices()
    {
        // Arrange
        var services = new ServiceCollection();
        const string providerName = "test-provider";
        const string connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=value";
        const string topicName = "testtopic";
        const string subscriptionName = "testsubscription";
        
        // Act
        services.AddAzureServiceBusTopicStreaming(providerName, connectionString, topicName, subscriptionName);
        
        // Assert
        Assert.Contains(services, service => service.ServiceType == typeof(IConfigurationValidator));
    }

    [Theory]
    [InlineData(null, "conn", "topic", "sub")]
    [InlineData("", "conn", "topic", "sub")]
    [InlineData("   ", "conn", "topic", "sub")]
    [InlineData("name", null, "topic", "sub")]
    [InlineData("name", "", "topic", "sub")]
    [InlineData("name", "   ", "topic", "sub")]
    [InlineData("name", "conn", null, "sub")]
    [InlineData("name", "conn", "", "sub")]
    [InlineData("name", "conn", "   ", "sub")]
    [InlineData("name", "conn", "topic", null)]
    [InlineData("name", "conn", "topic", "")]
    [InlineData("name", "conn", "topic", "   ")]
    public void AddAzureServiceBusTopicStreaming_InvalidParameters_ThrowsArgumentException(
        string name, string connectionString, string topicName, string subscriptionName)
    {
        // Arrange
        var services = new ServiceCollection();
        
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            services.AddAzureServiceBusTopicStreaming(name, connectionString, topicName, subscriptionName));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_MultipleNamedProviders_IsolatesConfigurations()
    {
        // Arrange
        var services = new ServiceCollection();
        
        // Act
        services.AddAzureServiceBusQueueStreaming("provider1", "conn1", "queue1");
        services.AddAzureServiceBusQueueStreaming("provider2", "conn2", "queue2");
        
        // Assert - Both should register their own validators
        var validators = services.Where(s => s.ServiceType == typeof(IConfigurationValidator)).ToList();
        Assert.Equal(2, validators.Count);
    }
}