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
        ISiloBuilder builder = null;
        
        Assert.Throws<ArgumentNullException>(() => 
            builder.AddAzureServiceBusStreaming("test", options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_NullName_ThrowsArgumentException()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().Build();
        var builder = new SiloBuilder(services, configuration);
        
        Assert.Throws<ArgumentException>(() => 
            builder.AddAzureServiceBusStreaming(null, options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_EmptyName_ThrowsArgumentException()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().Build();
        var builder = new SiloBuilder(services, configuration);
        
        Assert.Throws<ArgumentException>(() => 
            builder.AddAzureServiceBusStreaming("", options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_WhitespaceName_ThrowsArgumentException()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().Build();
        var builder = new SiloBuilder(services, configuration);
        
        Assert.Throws<ArgumentException>(() => 
            builder.AddAzureServiceBusStreaming("   ", options => { }));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_NullConfigureAction_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().Build();
        var builder = new SiloBuilder(services, configuration);
        Action<AzureServiceBusOptions> configure = null;
        
        Assert.Throws<ArgumentNullException>(() => 
            builder.AddAzureServiceBusStreaming("test", configure));
    }

    [Fact]
    public void AddAzureServiceBusStreaming_NullConfiguration_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder().Build();
        var builder = new SiloBuilder(services, configuration);
        IConfiguration config = null;
        
        Assert.Throws<ArgumentNullException>(() => 
            builder.AddAzureServiceBusStreaming("test", config));
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
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() =>
        {
            var configuration = new ConfigurationBuilder().Build();
            var builder = new SiloBuilder(services, configuration);
            builder.AddAzureServiceBusQueueStreaming(name, connectionString, queueName);
        });
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
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() =>
        {
            var configuration = new ConfigurationBuilder().Build();
            var builder = new SiloBuilder(services, configuration);
            builder.AddAzureServiceBusTopicStreaming(name, connectionString, topicName, subscriptionName);
        });
    }


}
