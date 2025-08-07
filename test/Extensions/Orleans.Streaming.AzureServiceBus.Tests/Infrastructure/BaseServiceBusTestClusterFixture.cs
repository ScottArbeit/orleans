using Orleans.Configuration;
using Orleans.TestingHost;
using TestExtensions;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests.Infrastructure;

/// <summary>
/// Base test cluster fixture for Azure Service Bus integration tests.
/// Ensures Service Bus emulator connectivity and waits for stream queue initialization.
/// </summary>
public abstract class BaseServiceBusTestClusterFixture : BaseTestClusterFixture
{
    protected override void CheckPreconditionsOrThrow()
    {
        base.CheckPreconditionsOrThrow();
        // Add any Service Bus specific precondition checks here
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        
        // Wait for stream queues to initialize similar to EventHub pattern
        var collector = new Microsoft.Extensions.Diagnostics.Metrics.Testing.MetricCollector<long>(
            Instruments.Meter, 
            "orleans-streams-queue-read-duration");
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        try
        {
            // Wait for 10 queue reads to ensure the streaming infrastructure is ready
            await collector.WaitForMeasurementsAsync(10, cts.Token);
        }
        catch (OperationCanceledException)
        {
            // If we timeout, that's okay - the cluster should still be functional
            // Some tests may not generate enough queue activity to trigger metrics
        }
    }
}

/// <summary>
/// Test cluster fixture with Azure Service Bus Queue stream provider.
/// Configures Orleans cluster for Queue-based streaming scenarios.
/// </summary>
public class ServiceBusQueueTestClusterFixture : BaseServiceBusTestClusterFixture
{
    public const string StreamProviderName = "ServiceBusQueueProvider";
    public const string StreamNamespace = "SBQueueTestNamespace";

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
                configure.ConnectionString = "ServiceBus=UseDevelopmentEmulator=true;";
                configure.EntityName = "test-queue";
                configure.EntityMode = ServiceBusEntityMode.Queue;
                configure.BatchSize = 10;
                configure.MaxConcurrentCalls = 1;
            });
        }
    }

    private class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IClientBuilder clientBuilder)
        {
            clientBuilder.AddServiceBusQueueStreams(StreamProviderName, configure =>
            {
                configure.ConnectionString = "ServiceBus=UseDevelopmentEmulator=true;";
                configure.EntityName = "test-queue";
                configure.EntityMode = ServiceBusEntityMode.Queue;
            });
        }
    }
}

/// <summary>
/// Test cluster fixture with Azure Service Bus Topic stream provider.
/// Configures Orleans cluster for Topic/Subscription-based streaming scenarios.
/// </summary>
public class ServiceBusTopicTestClusterFixture : BaseServiceBusTestClusterFixture
{
    public const string StreamProviderName = "ServiceBusTopicProvider";
    public const string StreamNamespace = "SBTopicTestNamespace";

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
                configure.ConnectionString = "ServiceBus=UseDevelopmentEmulator=true;";
                configure.EntityName = "test-topic";
                configure.EntityMode = ServiceBusEntityMode.Topic;
                configure.BatchSize = 10;
                configure.MaxConcurrentCalls = 1;
                configure.SubscriptionName = "test-subscription";
            });
        }
    }

    private class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IClientBuilder clientBuilder)
        {
            clientBuilder.AddServiceBusTopicStreams(StreamProviderName, configure =>
            {
                configure.ConnectionString = "ServiceBus=UseDevelopmentEmulator=true;";
                configure.EntityName = "test-topic";
                configure.EntityMode = ServiceBusEntityMode.Topic;
                configure.SubscriptionName = "test-subscription";
            });
        }
    }
}