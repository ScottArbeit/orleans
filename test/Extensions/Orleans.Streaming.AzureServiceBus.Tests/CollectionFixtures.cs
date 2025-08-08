#nullable enable
using Orleans.Streaming.AzureServiceBus.Tests.Infrastructure;
using Microsoft.Extensions.Configuration;
using Orleans.TestingHost;
using Microsoft.Extensions.Configuration;
using TestExtensions;
using Microsoft.Extensions.Configuration;
using Xunit;
using Microsoft.Extensions.Configuration;

namespace Orleans.Streaming.AzureServiceBus.Tests;

/// <summary>
/// Collection fixtures for Azure Service Bus streaming integration tests.
/// Organizes test execution and resource sharing across test classes.
/// </summary>

/// <summary>
/// Collection definition for tests that require the Service Bus emulator.
/// All tests in this collection share the same emulator instance.
/// </summary>
[CollectionDefinition("ServiceBusEmulator")]
public class ServiceBusEmulatorTestCollection : ICollectionFixture<ServiceBusEmulatorFixture>
{
}

/// <summary>
/// Collection definition for tests that require a Service Bus queue test cluster.
/// Provides a shared Orleans cluster configured with Service Bus queue streaming.
/// </summary>
[CollectionDefinition("ServiceBusQueueCluster")]
public class ServiceBusQueueClusterCollection : ICollectionFixture<ServiceBusQueueClusterFixture>
{
}

/// <summary>
/// Collection definition for tests that require a Service Bus topic test cluster.
/// Provides a shared Orleans cluster configured with Service Bus topic streaming.
/// </summary>
[CollectionDefinition("ServiceBusTopicCluster")]
public class ServiceBusTopicClusterCollection : ICollectionFixture<ServiceBusTopicClusterFixture>
{
}

/// <summary>
/// Collection definition for tests that require the default test environment.
/// Uses the shared test environment fixture for Azure-related configurations.
/// </summary>
[CollectionDefinition("ServiceBusTestEnvironment")]
public class ServiceBusTestEnvironmentCollection : ICollectionFixture<TestEnvironmentFixture>
{
}

/// <summary>
/// Test cluster fixture specifically configured for Service Bus queue streaming scenarios.
/// Provides an Orleans cluster with Service Bus queue stream provider configured.
/// </summary>
public class ServiceBusQueueClusterFixture : BaseServiceBusTestClusterFixture
{
    public const string StreamProviderName = "ServiceBusQueueClusterProvider";
    public const string StreamNamespace = "SBQueueClusterNamespace";

    protected override void ConfigureTestCluster(TestClusterBuilder builder)
    {
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
    }

    private class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder hostBuilder)
        {
            hostBuilder.AddServiceBusQueueStreams(StreamProviderName, configure =>
            {
                configure.ConfigureQueueTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "cluster-queue-test");
                configure.BatchSize = 10;
                configure.MaxConcurrentCalls = 2;
            });
        }
    }

    private class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.AddServiceBusQueueStreams(StreamProviderName, configure =>
            {
                configure.ConfigureQueueTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "cluster-queue-test");
            });
        }
    }
}

/// <summary>
/// Test cluster fixture specifically configured for Service Bus topic streaming scenarios.
/// Provides an Orleans cluster with Service Bus topic stream provider configured.
/// </summary>
public class ServiceBusTopicClusterFixture : BaseServiceBusTestClusterFixture
{
    public const string StreamProviderName = "ServiceBusTopicClusterProvider";
    public const string StreamNamespace = "SBTopicClusterNamespace";

    protected override void ConfigureTestCluster(TestClusterBuilder builder)
    {
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
    }

    private class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder hostBuilder)
        {
            hostBuilder.AddServiceBusTopicStreams(StreamProviderName, configure =>
            {
                configure.ConfigureTopicTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "cluster-topic-test",
                    "cluster-subscription-test");
                configure.BatchSize = 10;
                configure.MaxConcurrentCalls = 2;
            });
        }
    }

    private class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.AddServiceBusTopicStreams(StreamProviderName, configure =>
            {
                configure.ConfigureTopicTestDefaults(
                    "ServiceBus=UseDevelopmentEmulator=true;",
                    "cluster-topic-test",
                    "cluster-subscription-test");
            });
        }
    }
}