using System.Net;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Microsoft.Extensions.Logging;
using Xunit;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using DotNet.Testcontainers.Configurations;

namespace Orleans.Streaming.AzureServiceBus.Tests.Fixtures;

/// <summary>
/// Test fixture for Azure Service Bus emulator using Docker containers.
/// This fixture manages the lifecycle of both SQL Server and Service Bus emulator containers for testing.
/// </summary>
public class ServiceBusEmulatorFixture : IAsyncLifetime
{
    private const int DefaultServiceBusPort = 5671; // AMQPS port for Service Bus Emulator
    private const int DefaultDataApiPort = 5672; // Data API port for Service Bus Emulator
    private const int DefaultSqlServerPort = 1433; // SQL Server port
    private readonly string DefaultSqlServerPassword = Guid.NewGuid().ToString();
    private static readonly TimeSpan SqlServerStartupTimeout = TimeSpan.FromSeconds(30);

    public const string QueueName = "test-queue";
    public const string TopicName = "test-topic";
    public const string SubscriptionName = "test-subscription";

    private INetwork? _network;
    private IContainer? _sqlServerContainer;
    private IContainer? _serviceBusContainer;
    private readonly ILogger<ServiceBusEmulatorFixture> _logger;
    private string _sqlServerContainerName = String.Empty;
    private string _configDirectory = String.Empty; // Temp directory holding emulator config

    public ServiceBusEmulatorFixture()
    {
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<ServiceBusEmulatorFixture>();
    }

    /// <summary>
    /// The connection string for connecting to the Service Bus emulator.
    /// </summary>
    public string ServiceBusConnectionString { get; private set; } = string.Empty;

    /// <summary>
    /// The exposed port for AMQPS connections.
    /// </summary>
    public ushort ExposedServiceBusPort { get; private set; }

    /// <summary>
    /// The exposed port for Data API.
    /// </summary>
    public ushort ExposedDataApiPort { get; private set; }

    /// <summary>
    /// The exposed port for management UI (legacy property for backward compatibility).
    /// </summary>
    public ushort ExposedManagementPort => ExposedDataApiPort;

    /// <summary>
    /// Gets a Service Bus client configured for the emulator.
    /// </summary>
    public ServiceBusClient CreateServiceBusClient()
    {
        if (string.IsNullOrEmpty(ServiceBusConnectionString))
        {
            throw new InvalidOperationException("Service Bus emulator is not initialized. Ensure InitializeAsync has been called.");
        }

        ServiceBusClientOptions serviceBusClientOptions = new()
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions
            {
                MaxRetries = 3,
                Delay = TimeSpan.FromSeconds(2),
                Mode = ServiceBusRetryMode.Fixed
            },
        };

        return new ServiceBusClient(ServiceBusConnectionString, serviceBusClientOptions);
    }

    /// <summary>
    /// Initializes the Service Bus emulator container with its SQL Server dependency.
    /// </summary>
    public async Task InitializeAsync()
    {
        try
        {
            _logger.LogInformation("Starting Azure Service Bus emulator with SQL Server dependency...");

            // Create a new, unique network for this test run to avoid conflicts.
            _network = new NetworkBuilder()
                .WithName($"servicebus-network-{Guid.NewGuid():N}")
                .WithCleanUp(true)
                .WithLogger(_logger)
                .Build();
            await _network.CreateAsync();

            // Start the SQL Server and Service Bus emulator containers at the same time.
            await Task.WhenAll(StartSqlServerContainerAsync(), StartServiceBusContainerAsync());

            _logger.LogInformation("Service Bus emulator is ready. AMQPS Port: {AmqpsPort}, Data API Port: {DataApiPort}",
                ExposedServiceBusPort, ExposedDataApiPort);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start Service Bus emulator with dependencies");
            // Perform cleanup even if initialization fails
            await DisposeAsync();
            throw;
        }
    }

    private async Task StartSqlServerContainerAsync()
    {
        _sqlServerContainerName = $"sql-servicebus-{Guid.NewGuid():N}";
        _logger.LogInformation("Starting SQL Server container: {ContainerName}", _sqlServerContainerName);

        _sqlServerContainer = new ContainerBuilder()
            .WithCleanUp(true)
            .WithImage("mcr.microsoft.com/mssql/server:2025-latest")
            .WithName(_sqlServerContainerName)
            .WithNetwork(_network)
            .WithNetworkAliases(_sqlServerContainerName)
            // Port binding is optional for intra-network connectivity. Keep it for external troubleshooting.
            .WithPortBinding(DefaultSqlServerPort, true)
            .WithEnvironment("ACCEPT_EULA", "Y")
            .WithEnvironment("SQL_SERVER", _sqlServerContainerName)
            .WithEnvironment("MSSQL_SA_PASSWORD", DefaultSqlServerPassword)
            //.WithEnvironment("MSSQL_PID", "Developer")
            //.WithEnvironment("MSSQL_ENABLE_HADR", "0")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilPortIsAvailable(DefaultSqlServerPort)
                .UntilMessageIsLogged(".*SQL Server is now ready for client connections.*"))
            .Build();

        using var cts = new CancellationTokenSource(SqlServerStartupTimeout);

        try
        {
            await _sqlServerContainer.StartAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            var earlyLogs = await SafeGetContainerLogsAsync(_sqlServerContainer);
            _logger.LogError("SQL Server failed to become ready within {Timeout}s. Partial logs:\n{Logs}",
                SqlServerStartupTimeout.TotalSeconds, earlyLogs);
            throw new TimeoutException($"SQL Server startup exceeded {SqlServerStartupTimeout.TotalSeconds} seconds.");
        }

        if (!_sqlServerContainer.State.Equals(TestcontainersStates.Running))
        {
            var logs = await SafeGetContainerLogsAsync(_sqlServerContainer);
            _logger.LogError("SQL Server container not running after startup. State: {State}. Logs:\n{Logs}",
                _sqlServerContainer.State, logs);
            throw new InvalidOperationException($"SQL Server container failed to start. State: {_sqlServerContainer.State}");
        }

        _logger.LogInformation("SQL Server container is running. Network alias: {Alias}", _sqlServerContainerName);
    }

    private async Task StartServiceBusContainerAsync()
    {
        var serviceBusContainerName = $"emulator-servicebus-{Guid.NewGuid():N}";
        _logger.LogInformation("Starting Service Bus emulator container: {ContainerName}", serviceBusContainerName);

        if (string.IsNullOrEmpty(_sqlServerContainerName))
        {
            throw new InvalidOperationException("SQL Server container name not set.");
        }

        // Internal DNS resolution uses the network alias; do not use localhost here.
        var sqlConnectionString = $"Server={_sqlServerContainerName},{DefaultSqlServerPort};User Id=sa;Password={DefaultSqlServerPassword};Encrypt=False;TrustServerCertificate=True;Connection Timeout=30;";

        _logger.LogInformation("Service Bus will connect to SQL Server using: \"{ConnectionString}\"", sqlConnectionString);

        // Create config.json for the emulator.
        _configDirectory = Path.Combine(Path.GetTempPath(), "sbemulator", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDirectory);
        var hostConfigPath = Path.Combine(_configDirectory, "Config.json");
        var sasKeyValue = Guid.NewGuid().ToString();
        string serviceBusConfigJson = BuildServiceBusConfigJson();
        _logger.LogInformation("Writing Service Bus emulator config to: {ConfigPath}", hostConfigPath);
        _logger.LogDebug("Service Bus emulator config content:\n{ConfigContent}", serviceBusConfigJson);
        File.WriteAllText(hostConfigPath, serviceBusConfigJson);

        _serviceBusContainer = new ContainerBuilder()
            .WithCleanUp(true)
            .WithImage("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
            .WithName(serviceBusContainerName)
            .WithNetwork(_network)
            .WithNetworkAliases(serviceBusContainerName)
            .WithPortBinding(DefaultServiceBusPort, DefaultServiceBusPort)
            .WithPortBinding(DefaultDataApiPort, DefaultDataApiPort)
            .WithEnvironment("ACCEPT_EULA", "Y")
            .WithEnvironment("SQL_CONNECTION_STRING", sqlConnectionString)
            .WithEnvironment("SQL_WAIT_INTERVAL", "7")
            .WithEnvironment("SQL_SERVER", _sqlServerContainerName)
            .WithEnvironment("MSSQL_SA_PASSWORD", DefaultSqlServerPassword)
            .WithBindMount(_configDirectory, "/ServiceBus_Emulator/ConfigFiles")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilMessageIsLogged(".*Emulator Service is Successfully Up!.*"))
            .Build();

        await _serviceBusContainer.StartAsync();

        ExposedServiceBusPort = _serviceBusContainer.GetMappedPublicPort(DefaultServiceBusPort);
        ExposedDataApiPort = _serviceBusContainer.GetMappedPublicPort(DefaultDataApiPort);

        ServiceBusConnectionString =
            $"Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={sasKeyValue};UseDevelopmentEmulator=true;";

        _logger.LogInformation("Service Bus connection string: {ConnectionString}", ServiceBusConnectionString);
    }

    public async Task DisposeAsync()
    {
        try
        {
            if (!String.IsNullOrEmpty(_configDirectory) && Directory.Exists(_configDirectory))
            {
                _logger.LogInformation("Deleting temporary config directory: {Dir}", _configDirectory);
                Directory.Delete(_configDirectory, recursive: true);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete temp config directory {Dir}: {Message}", _configDirectory, ex.Message);
            await Task.Delay(1); // This is just here to make sure the method has an async Task to complete, otherwise the compiler will complain.
        }
    }

    private static async Task<string> SafeGetContainerLogsAsync(IContainer? container)
    {
        try
        {
            if (container is not null)
            {
                var logs = await container.GetLogsAsync();
                return logs.Stdout;
            }
            return string.Empty;
        }
        catch
        {
            return "<failed to retrieve logs>";
        }
    }

    private static string BuildServiceBusConfigJson()
    {
        // Entities listed here will be created when the container starts.
        // For more information about this schema, see: https://mcr.microsoft.com/en-us/artifact/mar/azure-messaging/servicebus-emulator/about.
        var config = new
        {
            UserConfig = new
            {
                Namespaces = new[]
                {
                    new
                    {
                        Name = "sbemulatorns", // Namespace name referenced by the emulator
                        Queues = new[]
                        {
                            new
                            {
                                Name = QueueName,
                                Properties = new[]
                                {
                                    new { Name = "RequiresDuplicateDetection", Value = false },
                                    new { Name = "RequiresSession", Value = false }
                                },
                            }
                        },
                        Topics = new[]
                        {
                            new
                            {
                                Name = TopicName,
                                Properties = new[]
                                {
                                    new { Name = "RequiresDuplicateDetection", Value = false }
                                },
                                Subscriptions = new[]
                                {
                                    new
                                    {
                                        Name = SubscriptionName
                                    }
                                }
                            }
                        }
                    }
                },
                Logging = new
                {
                    Type = "File"
                }
            }
        };

        return JsonSerializer.Serialize(config, new JsonSerializerOptions
        {
            WriteIndented = true
        });
    }
}
