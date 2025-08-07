# Orleans.Streaming.AzureServiceBus.Tests

Integration tests for the Orleans Azure Service Bus streaming provider.

## Overview

This test project provides comprehensive integration testing for the Orleans Azure Service Bus streaming implementation. It validates end-to-end functionality using the Azure Service Bus emulator and covers both Queue and Topic/Subscription messaging patterns.

## Test Structure

### Infrastructure
- **ServiceBusEmulatorFixture**: Manages Azure Service Bus emulator Docker container lifecycle
- **BaseServiceBusTestClusterFixture**: Base Orleans cluster fixture for Service Bus streaming tests
- **ServiceBusTestUtils**: Utility methods for test data generation and performance metrics

### Test Categories

#### Core Integration Tests
- **ServiceBusClientStreamTests**: Client producer/consumer scenarios
- **ServiceBusStreamProviderTests**: Stream provider functionality
- **ServiceBusSubscriptionTests**: Subscription management and multiplicity
- **ServiceBusQueueTests**: Queue-specific messaging patterns
- **ServiceBusTopicTests**: Topic/Subscription pub/sub patterns
- **ServiceBusStreamRecoveryTests**: Error handling and stream recovery

#### Performance Tests
- **ServiceBusStreamPerformanceTests**: Throughput, latency, and resource utilization tests

### Test Stream Providers
- **TestServiceBusQueueStreamProvider**: Test-specific queue stream provider
- **TestServiceBusTopicStreamProvider**: Test-specific topic stream provider
- **ServiceBusStreamProviderExtensions**: Configuration helpers

## Prerequisites

### Azure Service Bus Emulator
The tests use the Microsoft Azure Service Bus emulator running in Docker. Ensure Docker is available:

```bash
# Pull the Service Bus emulator image
docker pull mcr.microsoft.com/azure-messaging/servicebus-emulator:latest
```

### .NET 8
Tests target .NET 8 to match the Orleans framework requirements.

## Running Tests

### All Tests
```bash
dotnet test Orleans.Streaming.AzureServiceBus.Tests.csproj
```

### Specific Categories
```bash
# Run only functional tests
dotnet test --filter "TestCategory=Functional"

# Run only performance tests
dotnet test --filter "TestCategory=Performance"

# Run only Service Bus tests
dotnet test --filter "TestCategory=ServiceBus"
```

### Individual Test Classes
```bash
# Run client stream tests
dotnet test --filter "FullyQualifiedName~ServiceBusClientStreamTests"

# Run queue tests
dotnet test --filter "FullyQualifiedName~ServiceBusQueueTests"

# Run topic tests
dotnet test --filter "FullyQualifiedName~ServiceBusTopicTests"
```

## Test Configuration

### Emulator Settings
The tests automatically start and stop the Service Bus emulator using Docker. Configuration is handled through:
- Connection strings pointing to the emulator
- Automatic port allocation and health checking
- Container lifecycle management

### Orleans Configuration
Test clusters are configured with:
- Service Bus stream providers (Queue and Topic modes)
- Test-optimized batch sizes and timeouts
- Development emulator connection strings
- Simplified queue/topic names for testing

### Environment Variables
Optional environment variables for test customization:
- `SERVICEBUS_CONNECTION_STRING`: Override default emulator connection
- `SERVICEBUS_QUEUE_NAME`: Override default queue name
- `SERVICEBUS_TOPIC_NAME`: Override default topic name

## Test Patterns

### Collection Fixtures
Tests use xUnit collection fixtures for resource sharing:
- `ServiceBusEmulator`: Shared emulator instance across all tests
- `ServiceBusQueueCluster`: Shared Orleans cluster for queue tests
- `ServiceBusTopicCluster`: Shared Orleans cluster for topic tests

### Async Lifecycle
All fixtures implement `IAsyncLifetime` for proper async setup/teardown:
- Container startup with health checks
- Orleans cluster initialization
- Graceful resource cleanup

### Error Handling
Tests validate error scenarios:
- Connection failures and retry logic
- Message processing errors
- Subscription recovery
- Resource cleanup under failure conditions

## Performance Testing

Performance tests measure:
- **Throughput**: Messages per second under various loads
- **Latency**: End-to-end message processing time
- **Concurrency**: Multiple producers/consumers scenarios
- **Resource Usage**: Memory and CPU utilization
- **Large Messages**: Performance with larger payloads

## Debugging Tests

### Docker Issues
If emulator startup fails:
```bash
# Check Docker daemon
docker ps

# Pull latest emulator image
docker pull mcr.microsoft.com/azure-messaging/servicebus-emulator:latest

# Check container logs
docker logs <container-id>
```

### Orleans Cluster Issues
If cluster startup fails:
- Check silo logs for streaming provider initialization errors
- Verify Service Bus connection string format
- Ensure emulator is ready before cluster start

### Test Timeouts
Default timeouts are conservative. For debugging:
- Increase timeout values in test methods
- Add additional logging to trace message flow
- Use debugger-friendly test methods (single-threaded)

## Extending Tests

### Adding New Test Scenarios
1. Create test class inheriting from `TestClusterPerTest`
2. Use appropriate collection attribute for resource sharing
3. Follow existing patterns for setup/teardown
4. Add test categories for filtering

### Custom Stream Providers
1. Extend base provider classes in `TestStreamProviders`
2. Override configuration methods as needed
3. Register in cluster configurators

### Performance Metrics
1. Use `ServiceBusTestUtils.PerformanceMetrics` for measurement
2. Add custom metrics collection as needed
3. Report results through test output

## Known Limitations

1. **Emulator Fidelity**: The emulator may not perfectly match Azure Service Bus behavior
2. **Docker Dependency**: Tests require Docker for emulator
3. **Resource Cleanup**: Some test failures may leave containers running
4. **Platform Support**: Docker-based tests may have platform-specific issues

## Contributing

When adding new tests:
1. Follow existing naming conventions
2. Use appropriate test categories
3. Include comprehensive documentation
4. Add performance considerations
5. Ensure proper resource cleanup
6. Update this README as needed

## Troubleshooting

### Common Issues

**Port Conflicts**: Tests automatically find available ports, but conflicts may occur
```bash
# Check port usage
netstat -an | grep :5672
```

**Container Cleanup**: Failed tests may leave containers running
```bash
# List containers
docker ps -a

# Remove test containers
docker rm -f $(docker ps -a -q --filter "ancestor=mcr.microsoft.com/azure-messaging/servicebus-emulator")
```

**Slow Test Execution**: Large message or high-volume tests may be slow
- Adjust timeout values for CI environments
- Consider reducing message counts for faster feedback
- Use performance test category to separate long-running tests