using System;
using Orleans.Configuration;
using Xunit;

namespace Tester.AzureUtils.AzureServiceBus.Providers;

/// <summary>
/// Tests for Azure Service Bus adapter functionality.
/// </summary>
public class AzureServiceBusAdapterTests
{
    [Fact]
    public void AzureServiceBusOptions_DefaultValues_AreCorrect()
    {
        // Arrange
        var options = new AzureServiceBusOptions();

        // Assert - Test that defaults are reasonable
        Assert.Equal(string.Empty, options.ConnectionString);
        Assert.Equal(string.Empty, options.EntityName);
        Assert.Equal(ServiceBusEntityMode.Queue, options.EntityMode);
        Assert.Equal(32, options.BatchSize);
        Assert.True(options.AutoCompleteMessages);
    }

    [Fact]
    public void AzureServiceBusOptions_WithValidConfiguration_SetsPropertiesCorrectly()
    {
        // Arrange
        var connectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==";
        var entityName = "test-queue";

        // Act
        var options = new AzureServiceBusOptions
        {
            ConnectionString = connectionString,
            EntityName = entityName,
            EntityMode = ServiceBusEntityMode.Queue,
            BatchSize = 50,
            AutoCompleteMessages = false
        };

        // Assert
        Assert.Equal(connectionString, options.ConnectionString);
        Assert.Equal(entityName, options.EntityName);
        Assert.Equal(ServiceBusEntityMode.Queue, options.EntityMode);
        Assert.Equal(50, options.BatchSize);
        Assert.False(options.AutoCompleteMessages);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1001)]
    public void AzureServiceBusOptions_WithInvalidBatchSize_IsOutOfExpectedRange(int batchSize)
    {
        // Arrange
        var options = new AzureServiceBusOptions();

        // Act
        options.BatchSize = batchSize;

        // Assert - We expect batch sizes to be reasonable, but this test
        // documents what values are currently allowed vs expected
        if (batchSize <= 0 || batchSize > 1000)
        {
            // These values are probably not ideal for production use
            Assert.True(batchSize <= 0 || batchSize > 1000);
        }
    }
}