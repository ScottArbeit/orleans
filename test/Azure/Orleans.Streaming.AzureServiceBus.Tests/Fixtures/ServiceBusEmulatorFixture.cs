using System.Net;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.Logging;
using Xunit;
using Azure.Messaging.ServiceBus;

namespace Orleans.Streaming.AzureServiceBus.Tests.Fixtures;

/// <summary>
/// Test fixture for Azure Service Bus emulator using Docker containers.
/// This fixture manages the lifecycle of a Service Bus emulator container for testing.
/// </summary>
public class ServiceBusEmulatorFixture : IAsyncLifetime
{
    private const int DefaultServiceBusPort = 5672; // AMQP port
    private const int DefaultManagementPort = 15672; // Management UI port

    private IContainer? _container;
    private readonly ILogger<ServiceBusEmulatorFixture> _logger;

    public ServiceBusEmulatorFixture()
    {
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<ServiceBusEmulatorFixture>();
    }

    /// <summary>
    /// The connection string for connecting to the Service Bus emulator.
    /// </summary>
    public string ConnectionString { get; private set; } = string.Empty;

    /// <summary>
    /// The exposed port for AMQP connections.
    /// </summary>
    public ushort ExposedServiceBusPort { get; private set; }

    /// <summary>
    /// The exposed port for management UI.
    /// </summary>
    public ushort ExposedManagementPort { get; private set; }

    /// <summary>
    /// Gets a Service Bus client configured for the emulator.
    /// </summary>
    public ServiceBusClient CreateServiceBusClient()
    {
        if (string.IsNullOrEmpty(ConnectionString))
        {
            throw new InvalidOperationException("Service Bus emulator is not initialized. Ensure InitializeAsync has been called.");
        }

        return new ServiceBusClient(ConnectionString);
    }

    /// <summary>
    /// Initializes the Service Bus emulator container.
    /// </summary>
    public async Task InitializeAsync()
    {
        try
        {
            _logger.LogInformation("Starting Azure Service Bus emulator container...");

            // Using RabbitMQ as a Service Bus emulator since there's no official Azure Service Bus emulator
            // RabbitMQ supports AMQP 1.0 which is compatible with Service Bus protocol
            _container = new ContainerBuilder()
                .WithImage("rabbitmq:3-management")
                .WithPortBinding(DefaultServiceBusPort, true)
                .WithPortBinding(DefaultManagementPort, true)
                .WithEnvironment("RABBITMQ_DEFAULT_USER", "guest")
                .WithEnvironment("RABBITMQ_DEFAULT_PASS", "guest")
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilPortIsAvailable(DefaultServiceBusPort)
                    .UntilPortIsAvailable(DefaultManagementPort))
                .WithStartupCallback((container, ct) =>
                {
                    _logger.LogInformation("Service Bus emulator container started successfully.");
                    return Task.CompletedTask;
                })
                .Build();

            await _container.StartAsync();

            ExposedServiceBusPort = _container.GetMappedPublicPort(DefaultServiceBusPort);
            ExposedManagementPort = _container.GetMappedPublicPort(DefaultManagementPort);

            // For testing purposes, we'll use a mock connection string format
            // In a real emulator, this would be the actual Service Bus connection string
            ConnectionString = $"Endpoint=sb://localhost:{ExposedServiceBusPort}/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE";

            _logger.LogInformation("Service Bus emulator is ready. AMQP Port: {AmqpPort}, Management Port: {ManagementPort}",
                ExposedServiceBusPort, ExposedManagementPort);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start Service Bus emulator container");
            throw;
        }
    }

    /// <summary>
    /// Disposes the Service Bus emulator container.
    /// </summary>
    public async Task DisposeAsync()
    {
        try
        {
            if (_container is not null)
            {
                _logger.LogInformation("Stopping Service Bus emulator container...");
                await _container.StopAsync();
                await _container.DisposeAsync();
                _logger.LogInformation("Service Bus emulator container stopped successfully.");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred while stopping Service Bus emulator container");
        }
    }

    /// <summary>
    /// Creates a queue in the Service Bus emulator for testing.
    /// </summary>
    /// <param name="queueName">The name of the queue to create.</param>
    public async Task CreateQueueAsync(string queueName)
    {
        // For this skeleton, we'll just log the queue creation
        // In a real implementation, this would use Service Bus management APIs
        _logger.LogInformation("Creating queue: {QueueName}", queueName);
        await Task.Delay(100); // Simulate queue creation delay
    }

    /// <summary>
    /// Deletes a queue from the Service Bus emulator.
    /// </summary>
    /// <param name="queueName">The name of the queue to delete.</param>
    public async Task DeleteQueueAsync(string queueName)
    {
        // For this skeleton, we'll just log the queue deletion
        // In a real implementation, this would use Service Bus management APIs
        _logger.LogInformation("Deleting queue: {QueueName}", queueName);
        await Task.Delay(100); // Simulate queue deletion delay
    }
}