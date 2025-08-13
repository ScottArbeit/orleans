namespace Orleans.Streaming.AzureServiceBus.Tests;

using Orleans.Streaming.AzureServiceBus.Tests.Fixtures;
using Xunit;

/// <summary>
/// Collection definition for ServiceBusEmulatorFixture to share it across multiple test classes.
/// This ensures only one Service Bus emulator instance is created and shared.
/// </summary>
[CollectionDefinition(ServiceBusEmulatorCollection.CollectionName)]
public class ServiceBusEmulatorCollection : ICollectionFixture<ServiceBusEmulatorFixture>
{
    public const string CollectionName = "ServiceBusEmulator";
}