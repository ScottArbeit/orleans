using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streaming.AzureServiceBus.Providers;
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
        ISiloBuilder builder = null;
        
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddAzureServiceBusQueueStreaming(name, connectionString, queueName));
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
}