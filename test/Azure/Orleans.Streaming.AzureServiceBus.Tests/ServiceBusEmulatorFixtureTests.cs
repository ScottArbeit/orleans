namespace Orleans.Streaming.AzureServiceBus.Tests;

using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

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
        var connectionString = _fixture.ServiceBusConnectionString;

        Assert.NotNull(connectionString);
        Assert.NotEmpty(connectionString);
        Assert.Contains("Endpoint=sb://localhost", connectionString);
        Assert.Contains("UseDevelopmentEmulator=true", connectionString);
        
        _output.WriteLine($"Connection String: {connectionString}");
    }

    [Fact]
    public void Fixture_Should_Have_Valid_Ports()
    {
        var serviceBusPort = _fixture.ExposedServiceBusPort;
        var dataApiPort = _fixture.ExposedDataApiPort;

        Assert.True(serviceBusPort > 0, "Service Bus port should be greater than 0");
        Assert.True(dataApiPort > 0, "Data API port should be greater than 0");
        Assert.NotEqual(serviceBusPort, dataApiPort);
        
        _output.WriteLine($"Service Bus Port: {serviceBusPort}");
        _output.WriteLine($"Data API Port: {dataApiPort}");
    }

    [Fact]
    public async Task Fixture_Should_Create_Service_Bus_Client()
    {
        await using var client = _fixture.CreateServiceBusClient();

        Assert.NotNull(client);
        Assert.NotNull(client.FullyQualifiedNamespace);
        
        _output.WriteLine($"Service Bus Client Namespace: {client.FullyQualifiedNamespace}");
    }

    [Fact]
    public async Task Fixture_Should_Support_Real_Message_Operations()
    {
        // Create sender and receiver
        await using var client = _fixture.CreateServiceBusClient();
        await using var sender = client.CreateSender(ServiceBusEmulatorFixture.QueueName);
        await using var receiver = client.CreateReceiver(ServiceBusEmulatorFixture.QueueName);

        // Send a test message
        var message = new Azure.Messaging.ServiceBus.ServiceBusMessage("Hello from emulator with SQL Server!");
        await sender.SendMessageAsync(message);

        // Receive the message with a longer timeout for emulator
        var receivedMessage = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5.0));
        Assert.NotNull(receivedMessage);
        Assert.Equal("Hello from emulator with SQL Server!", receivedMessage.Body.ToString());

        // Complete the message
        await receiver.CompleteMessageAsync(receivedMessage);
            
        _output.WriteLine("Successfully sent and received message through emulator with SQL Server backend");
    }

    [Fact]
    public async Task Fixture_Should_Support_Topic_Subscription_Message_Operations()
    {
        // Create sender and receiver
        await using var client = _fixture.CreateServiceBusClient();
        await using var sender = client.CreateSender(ServiceBusEmulatorFixture.TopicName);
        await using var receiver = client.CreateReceiver(ServiceBusEmulatorFixture.TopicName, ServiceBusEmulatorFixture.SubscriptionName);

        // Send a test message
        var message = new Azure.Messaging.ServiceBus.ServiceBusMessage("Hello from topic/subscription test!");
        await sender.SendMessageAsync(message);

        // Receive the message with a longer timeout for emulator
        var receivedMessage = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5.0));
        Assert.NotNull(receivedMessage);
        Assert.Equal("Hello from topic/subscription test!", receivedMessage.Body.ToString());

        // Complete the message
        await receiver.CompleteMessageAsync(receivedMessage);
            
        _output.WriteLine("Successfully sent and received message through topic/subscription with SQL Server backend");
    }
}
