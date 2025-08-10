using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Streaming.AzureServiceBus.Tests;

/// <summary>
/// Basic tests to verify the Service Bus emulator fixture is working correctly.
/// </summary>
public class ServiceBusEmulatorFixtureTests : IClassFixture<ServiceBusEmulatorFixture>
{
    private readonly ServiceBusEmulatorFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServiceBusEmulatorFixtureTests(ServiceBusEmulatorFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public void Fixture_Should_Have_Valid_Connection_String()
    {
        // Arrange & Act
        var connectionString = _fixture.ConnectionString;

        // Assert
        Assert.NotNull(connectionString);
        Assert.NotEmpty(connectionString);
        Assert.Contains("Endpoint=sb://localhost:", connectionString);
        
        _output.WriteLine($"Connection String: {connectionString}");
    }

    [Fact]
    public void Fixture_Should_Have_Valid_Ports()
    {
        // Arrange & Act
        var serviceBusPort = _fixture.ExposedServiceBusPort;
        var managementPort = _fixture.ExposedManagementPort;

        // Assert
        Assert.True(serviceBusPort > 0, "Service Bus port should be greater than 0");
        Assert.True(managementPort > 0, "Management port should be greater than 0");
        Assert.NotEqual(serviceBusPort, managementPort);
        
        _output.WriteLine($"Service Bus Port: {serviceBusPort}");
        _output.WriteLine($"Management Port: {managementPort}");
    }

    [Fact]
    public async Task Fixture_Should_Allow_Queue_Creation_And_Deletion()
    {
        // Arrange
        const string testQueueName = "test-queue";

        // Act & Assert - should not throw
        await _fixture.CreateQueueAsync(testQueueName);
        await _fixture.DeleteQueueAsync(testQueueName);
        
        _output.WriteLine($"Successfully created and deleted queue: {testQueueName}");
    }

    [Fact]
    public async Task Fixture_Should_Create_Service_Bus_Client()
    {
        // Act
        await using var client = _fixture.CreateServiceBusClient();

        // Assert
        Assert.NotNull(client);
        Assert.NotNull(client.FullyQualifiedNamespace);
        
        _output.WriteLine($"Service Bus Client Namespace: {client.FullyQualifiedNamespace}");
    }
}