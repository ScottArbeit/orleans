using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Streaming.AzureServiceBus.Tests;

/// <summary>
/// Integration tests for Service Bus adapter factory with auto-provisioning configuration.
/// These tests validate that the adapter factory works correctly when AutoCreateEntities=true without
/// actually performing management operations (which fail on Service Bus Emulator).
/// </summary>
[Collection(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusAdapterFactoryProvisioningTests
{
    private readonly ServiceBusEmulatorFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServiceBusAdapterFactoryProvisioningTests(ServiceBusEmulatorFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task AdapterFactory_Should_Create_Adapter_With_Queue_AutoProvisioning()
    {
        // Arrange
        var services = new ServiceCollection();
        var randomQueueName = $"test-adapter-queue-{Guid.NewGuid():N}";
        
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = _fixture.ServiceBusConnectionString,
            AutoCreateEntities = true,
            EntityKind = EntityKind.Queue,
            QueueName = randomQueueName,
            EntityCount = 1
        };

        // Add Orleans serialization services
        services.AddSerializer();
        services.AddSingleton<ILogger<ServiceBusQueueAdapterFactory>>(new TestLogger<ServiceBusQueueAdapterFactory>(_output));
        services.AddSingleton<ILoggerFactory>(new TestLoggerFactory(_output));
        services.AddSingleton<IOptionsMonitor<ServiceBusStreamOptions>>(new TestOptionsMonitor<ServiceBusStreamOptions>("testProvider", options));

        var serviceProvider = services.BuildServiceProvider();
        var factory = new ServiceBusQueueAdapterFactory(serviceProvider, "testProvider");

        // Act - This should not crash even though provisioning will fail in emulator
        var adapter = await factory.CreateAdapter();

        // Assert - Just verify adapter was created despite provisioning limitations
        Assert.NotNull(adapter);
        Assert.Equal("testProvider", adapter.Name);
        Assert.False(adapter.IsRewindable);

        _output.WriteLine($"Successfully created adapter with AutoCreateEntities=true for queue: {randomQueueName}");
    }

    [Fact]
    public async Task AdapterFactory_Should_Create_Adapter_With_Topic_AutoProvisioning()
    {
        // Arrange
        var services = new ServiceCollection();
        var randomTopicName = $"test-adapter-topic-{Guid.NewGuid():N}";
        var randomSubscriptionName = $"test-adapter-sub-{Guid.NewGuid():N}";
        
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = _fixture.ServiceBusConnectionString,
            AutoCreateEntities = true,
            EntityKind = EntityKind.TopicSubscription,
            TopicName = randomTopicName,
            SubscriptionName = randomSubscriptionName,
            EntityCount = 1
        };

        // Add Orleans serialization services
        services.AddSerializer();
        services.AddSingleton<ILogger<ServiceBusQueueAdapterFactory>>(new TestLogger<ServiceBusQueueAdapterFactory>(_output));
        services.AddSingleton<ILoggerFactory>(new TestLoggerFactory(_output));
        services.AddSingleton<IOptionsMonitor<ServiceBusStreamOptions>>(new TestOptionsMonitor<ServiceBusStreamOptions>("testProvider", options));

        var serviceProvider = services.BuildServiceProvider();
        var factory = new ServiceBusQueueAdapterFactory(serviceProvider, "testProvider");

        // Act - This should not crash even though provisioning will fail in emulator
        var adapter = await factory.CreateAdapter();

        // Assert - Just verify adapter was created despite provisioning limitations
        Assert.NotNull(adapter);
        Assert.Equal("testProvider", adapter.Name);
        Assert.False(adapter.IsRewindable);

        _output.WriteLine($"Successfully created adapter with AutoCreateEntities=true for topic: {randomTopicName} and subscription: {randomSubscriptionName}");
    }

    [Fact]
    public async Task AdapterFactory_Should_Create_Adapter_When_AutoCreate_Disabled()
    {
        // Arrange
        var services = new ServiceCollection();
        var randomQueueName = $"test-adapter-queue-{Guid.NewGuid():N}";
        
        var options = new ServiceBusStreamOptions
        {
            ConnectionString = _fixture.ServiceBusConnectionString,
            AutoCreateEntities = false, // Disabled
            EntityKind = EntityKind.Queue,
            QueueName = randomQueueName,
            EntityCount = 1
        };

        // Add Orleans serialization services
        services.AddSerializer();
        services.AddSingleton<ILogger<ServiceBusQueueAdapterFactory>>(new TestLogger<ServiceBusQueueAdapterFactory>(_output));
        services.AddSingleton<ILoggerFactory>(new TestLoggerFactory(_output));
        services.AddSingleton<IOptionsMonitor<ServiceBusStreamOptions>>(new TestOptionsMonitor<ServiceBusStreamOptions>("testProvider", options));

        var serviceProvider = services.BuildServiceProvider();
        var factory = new ServiceBusQueueAdapterFactory(serviceProvider, "testProvider");

        // Act - This should skip auto-provisioning
        var adapter = await factory.CreateAdapter();

        // Assert
        Assert.NotNull(adapter);
        Assert.Equal("testProvider", adapter.Name);
        Assert.False(adapter.IsRewindable);

        _output.WriteLine($"Successfully created adapter without auto-provisioning: {randomQueueName}");
    }
}

/// <summary>
/// Simple test implementation of IOptionsMonitor.
/// </summary>
internal class TestOptionsMonitor<T> : IOptionsMonitor<T>
{
    private readonly string _name;
    private readonly T _value;

    public TestOptionsMonitor(string name, T value)
    {
        _name = name;
        _value = value;
    }

    public T CurrentValue => _value;

    public T Get(string? name)
    {
        if (string.IsNullOrEmpty(name) || name == _name)
        {
            return _value;
        }
        throw new ArgumentException($"Unrecognized options name: {name}", nameof(name));
    }

    public IDisposable? OnChange(Action<T, string?> listener) => null;
}

/// <summary>
/// Simple test implementation of ILoggerFactory.
/// </summary>
internal class TestLoggerFactory : ILoggerFactory
{
    private readonly ITestOutputHelper _output;

    public TestLoggerFactory(ITestOutputHelper output)
    {
        _output = output;
    }

    public ILogger CreateLogger(string categoryName) => new TestLogger(_output, categoryName);

    public void AddProvider(ILoggerProvider provider) { }

    public void Dispose() { }
}

/// <summary>
/// Simple test logger implementation.
/// </summary>
internal class TestLogger : ILogger
{
    private readonly ITestOutputHelper _output;
    private readonly string _categoryName;

    public TestLogger(ITestOutputHelper output, string categoryName)
    {
        _output = output;
        _categoryName = categoryName;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        var message = formatter(state, exception);
        _output.WriteLine($"[{logLevel}] {_categoryName}: {message}");
        if (exception is not null)
        {
            _output.WriteLine($"Exception: {exception}");
        }
    }
}

/// <summary>
/// Generic test logger implementation.
/// </summary>
internal class TestLogger<T> : ILogger<T>
{
    private readonly ITestOutputHelper _output;

    public TestLogger(ITestOutputHelper output)
    {
        _output = output;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        var message = formatter(state, exception);
        _output.WriteLine($"[{logLevel}] {typeof(T).Name}: {message}");
        if (exception is not null)
        {
            _output.WriteLine($"Exception: {exception}");
        }
    }
}