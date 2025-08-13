# Azure Service Bus Emulator Tests

This test project uses the official Microsoft Azure Service Bus Emulator for integration testing.

## Architecture

The `ServiceBusEmulatorFixture` sets up a multi-container environment:

1. **SQL Server Container** (`mcr.microsoft.com/mssql/server:2025-latest`)
   - Provides the backend storage for the Service Bus emulator
   - Uses SA authentication with password: `Guid.NewGuid().ToString()`
   - Runs on default port 1433

2. **Service Bus Emulator Container** (`mcr.microsoft.com/azure-messaging/servicebus-emulator:latest`)
   - The actual Service Bus emulator
   - Connects to SQL Server for persistence
   - Exposes ports 5671 (AMQPS) and 5672 (Data API)

3. **Docker Network**
   - Both containers communicate via a custom Docker network
   - Allows the Service Bus emulator to connect to SQL Server by hostname

## Container Startup Sequence

1. Create Docker network
2. Start SQL Server container and wait for it to be ready
3. Start Service Bus emulator with SQL connection string
4. Wait for Service Bus emulator to initialize

## Test Categories

- **Unit Tests**: Configuration validation, serialization, mapping logic
- **Integration Tests**: Real message operations using the emulator

## Requirements

- Docker must be available and running
- Sufficient memory for both SQL Server and Service Bus containers
- Network connectivity between containers

## Troubleshooting

If tests fail with connection errors:
1. Check Docker is running
2. Verify container logs for startup issues
3. Ensure sufficient system resources
4. Check firewall settings for container networking
