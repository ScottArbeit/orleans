#nullable enable
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using System.Net;
using System.Net.Sockets;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests.Infrastructure;

/// <summary>
/// Fixture for managing Azure Service Bus emulator container lifecycle.
/// Uses the Microsoft Azure Service Bus emulator Docker image.
/// </summary>
public class ServiceBusEmulatorFixture : IAsyncLifetime
{
    private IContainer? _container;
    private ushort _exposedPort;
    private const int ServiceBusPort = 5671; // AMQP port
    private const string EmulatorImage = "mcr.microsoft.com/azure-messaging/servicebus-emulator:latest";
    
    /// <summary>
    /// Gets the connection string for the Service Bus emulator.
    /// </summary>
    public string ConnectionString => $"Endpoint=sb://localhost:{_exposedPort}/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true";
    
    /// <summary>
    /// Gets the exposed port for the Service Bus emulator.
    /// </summary>
    public ushort ExposedPort => _exposedPort;

    public async Task InitializeAsync()
    {
        try
        {
            // Find an available port
            _exposedPort = GetAvailablePort();
            
            // Create and start the container
            _container = new ContainerBuilder()
                .WithImage(EmulatorImage)
                .WithPortBinding(_exposedPort, ServiceBusPort)
                .WithEnvironment("ACCEPT_EULA", "Y")
                .WithEnvironment("SERVICEBUS_EMULATOR_ENABLE_LOGGING", "true")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(ServiceBusPort))
                .Build();

            await _container.StartAsync();

            // Wait for the emulator to be ready
            await WaitForEmulatorReadiness();
        }
        catch (Exception ex)
        {
            await DisposeAsync();
            throw new InvalidOperationException($"Failed to start Service Bus emulator: {ex.Message}", ex);
        }
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            try
            {
                await _container.StopAsync();
                await _container.DisposeAsync();
            }
            catch
            {
                // Best effort cleanup
            }
        }
    }

    private static ushort GetAvailablePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return (ushort)port;
    }

    private async Task WaitForEmulatorReadiness()
    {
        var timeout = TimeSpan.FromMinutes(2);
        var startTime = DateTime.UtcNow;
        
        while (DateTime.UtcNow - startTime < timeout)
        {
            try
            {
                using var tcpClient = new TcpClient();
                await tcpClient.ConnectAsync(IPAddress.Loopback, _exposedPort);
                return; // Connection successful
            }
            catch
            {
                await Task.Delay(1000);
            }
        }
        
        throw new TimeoutException($"Service Bus emulator did not start within {timeout.TotalMinutes} minutes");
    }
}

/// <summary>
/// Collection definition for Service Bus emulator tests.
/// Ensures all tests in this collection share the same emulator instance.
/// </summary>
[CollectionDefinition("ServiceBusEmulator")]
public class ServiceBusEmulatorCollection : ICollectionFixture<ServiceBusEmulatorFixture>
{
}