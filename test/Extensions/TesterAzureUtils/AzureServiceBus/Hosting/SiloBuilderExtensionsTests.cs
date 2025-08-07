using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus.Providers;
using Orleans.TestingHost;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests.Hosting;

/// <summary>
/// Tests for Azure Service Bus SiloBuilder extensions.
/// </summary>
public class SiloBuilderExtensionsTests
{
    [Fact]
    public void AddAzureServiceBusStreaming_NullBuilder_ThrowsArgumentNullException()
    {
        // Arrange
        ISiloBuilder builder = null;
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            builder.AddAzureServiceBusStreaming("test", options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_NullName_ThrowsArgumentException()
    {
        // Arrange
        var services = new ServiceCollection();
        var siloBuilder = new SiloHostBuilder().ConfigureServices(s => { });
        
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            siloBuilder.AddAzureServiceBusStreaming(null, options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_EmptyName_ThrowsArgumentException()
    {
        // Arrange  
        var services = new ServiceCollection();
        var siloBuilder = new SiloHostBuilder().ConfigureServices(s => { });
        
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            siloBuilder.AddAzureServiceBusStreaming("", options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_WhitespaceName_ThrowsArgumentException()
    {
        // Arrange
        var services = new ServiceCollection();
        var siloBuilder = new SiloHostBuilder().ConfigureServices(s => { });
        
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            siloBuilder.AddAzureServiceBusStreaming("   ", options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_NullConfigureAction_ThrowsArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();
        var siloBuilder = new SiloHostBuilder().ConfigureServices(s => { });
        Action<AzureServiceBusOptions> configure = null;
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            siloBuilder.AddAzureServiceBusStreaming("test", configure));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_WithIConfiguration_ReturnsBuilder()
    {
        // Arrange
        var services = new ServiceCollection();
        var siloBuilder = new SiloHostBuilder().ConfigureServices(s => { });
        
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
        var result = siloBuilder.AddAzureServiceBusStreaming("test-provider", configuration);
        
        // Assert
        Assert.NotNull(result);
        Assert.Same(siloBuilder, result);
    }

    [Fact]
    public void AddAzureServiceBusStreaming_NullConfiguration_ThrowsArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();
        var siloBuilder = new SiloHostBuilder().ConfigureServices(s => { });
        IConfiguration configuration = null;
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => 
            siloBuilder.AddAzureServiceBusStreaming("test", configuration));
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
        var siloBuilder = new SiloHostBuilder().ConfigureServices(s => { });
        
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            siloBuilder.AddAzureServiceBusQueueStreaming(name, connectionString, queueName));
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
        var siloBuilder = new SiloHostBuilder().ConfigureServices(s => { });
        
        // Act & Assert
        Assert.Throws<ArgumentException>(() => 
            siloBuilder.AddAzureServiceBusTopicStreaming(name, connectionString, topicName, subscriptionName));
    }
}