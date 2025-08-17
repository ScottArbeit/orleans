namespace Orleans.Streaming.AzureServiceBus.Tests;

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Orleans.Streams;
using Xunit;
using Xunit.Abstractions;

/// <summary>
/// Tests for ServiceBusAdapterReceiver functionality.
/// </summary>
[Collection(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusAdapterReceiverTests
{
    private readonly ServiceBusEmulatorFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServiceBusAdapterReceiverTests(ServiceBusEmulatorFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public void ServiceBusAdapterReceiver_Constructor_RequiresDataAdapter()
    {
        // Arrange
        var queueId = QueueId.GetQueueId("test", 0, 1);
        var options = CreateServiceBusStreamOptions();
        var logger = NullLogger<ServiceBusAdapterReceiver>.Instance;

        // Act & Assert - Creating with null data adapter should throw
        Assert.Throws<ArgumentNullException>(() => new ServiceBusAdapterReceiver(queueId, "TestProvider", options, null!, logger));
    }

    [Fact]
    public async Task ServiceBusAdapterReceiver_GetQueueMessages_Returns_Empty_When_Shutdown()
    {
        // Arrange  
        var queueId = QueueId.GetQueueId("test", 0, 1);
        var options = CreateServiceBusStreamOptions();
        var logger = NullLogger<ServiceBusAdapterReceiver>.Instance;

        using var receiver = new ServiceBusAdapterReceiver(queueId, "TestProvider", options, CreateDataAdapter(), logger);

        // Act - Shutdown before getting messages
        await receiver.Shutdown(TimeSpan.FromSeconds(30));
        var messages = await receiver.GetQueueMessagesAsync(10);

        // Assert
        Assert.NotNull(messages);
        Assert.Empty(messages);
    }

    [Fact]
    public async Task ServiceBusAdapterReceiver_MessagesDelivered_Handles_Empty_List()
    {
        // Arrange
        var queueId = QueueId.GetQueueId("test", 0, 1);
        var options = CreateServiceBusStreamOptions();
        var logger = NullLogger<ServiceBusAdapterReceiver>.Instance;

        using var receiver = new ServiceBusAdapterReceiver(queueId, "TestProvider", options, CreateDataAdapter(), logger);

        // Act & Assert - Should not throw on empty list
        await receiver.MessagesDeliveredAsync(Array.Empty<IBatchContainer>());
    }

    private ServiceBusStreamOptions CreateServiceBusStreamOptions()
    {
        return new ServiceBusStreamOptions
        {
            ConnectionString = _fixture.ServiceBusConnectionString,
            EntityKind = EntityKind.Queue,
            QueueName = ServiceBusEmulatorFixture.QueueName,
            Receiver = new ReceiverSettings
            {
                PrefetchCount = 10,
                ReceiveBatchSize = 5,
                LockAutoRenew = false // Disable for tests to avoid complexity
            }
        };
    }

    private static ServiceBusDataAdapter CreateDataAdapter()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        var serviceProvider = services.BuildServiceProvider();
        var serializer = serviceProvider.GetRequiredService<Serializer<ServiceBusBatchContainer>>();
        return new ServiceBusDataAdapter(serializer);
    }
}