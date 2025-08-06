using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Streaming.AzureServiceBus.Providers;
using Orleans.TestingHost.Utils;
using Xunit;

namespace Orleans.Streaming.AzureServiceBus.Tests.Providers;

[Collection(TestEnvironmentFixture.DefaultCollection)]
public class RetryHelperTests
{
    private readonly ILogger _logger;

    public RetryHelperTests()
    {
        _logger = Substitute.For<ILogger>();
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_SuccessfulOperation_ReturnsResult()
    {
        // Arrange
        var expectedResult = "success";
        Func<Task<string>> operation = () => Task.FromResult(expectedResult);

        // Act
        var result = await RetryHelper.ExecuteWithRetryAsync(
            operation,
            maxRetries: 3,
            logger: _logger,
            operationName: "Test Operation");

        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_TransientFailureThenSuccess_RetriesAndSucceeds()
    {
        // Arrange
        var attemptCount = 0;
        var expectedResult = "success";
        
        Func<Task<string>> operation = () =>
        {
            attemptCount++;
            if (attemptCount == 1)
            {
                throw new ServiceBusException("Transient error", ServiceBusFailureReason.ServiceTimeout);
            }
            return Task.FromResult(expectedResult);
        };

        // Act
        var result = await RetryHelper.ExecuteWithRetryAsync(
            operation,
            maxRetries: 3,
            baseDelay: TimeSpan.FromMilliseconds(10),
            logger: _logger,
            operationName: "Test Operation");

        // Assert
        Assert.Equal(expectedResult, result);
        Assert.Equal(2, attemptCount);
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_NonRetriableException_ThrowsImmediately()
    {
        // Arrange
        var attemptCount = 0;
        Func<Task<string>> operation = () =>
        {
            attemptCount++;
            throw new ArgumentException("Non-retriable error");
        };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => 
            RetryHelper.ExecuteWithRetryAsync(
                operation,
                maxRetries: 3,
                logger: _logger,
                operationName: "Test Operation"));

        Assert.Equal(1, attemptCount);
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_ExceedsMaxRetries_ThrowsLastException()
    {
        // Arrange
        var attemptCount = 0;
        Func<Task<string>> operation = () =>
        {
            attemptCount++;
            throw new ServiceBusException("Persistent error", ServiceBusFailureReason.ServiceTimeout);
        };

        // Act & Assert
        await Assert.ThrowsAsync<ServiceBusException>(() => 
            RetryHelper.ExecuteWithRetryAsync(
                operation,
                maxRetries: 2,
                baseDelay: TimeSpan.FromMilliseconds(10),
                logger: _logger,
                operationName: "Test Operation"));

        Assert.Equal(3, attemptCount); // Initial attempt + 2 retries
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_RetriableServiceBusExceptions_AreRetried()
    {
        // Arrange
        var retriableReasons = new[]
        {
            ServiceBusFailureReason.ServiceTimeout,
            ServiceBusFailureReason.ServiceBusy,
            ServiceBusFailureReason.ServiceCommunicationProblem,
            ServiceBusFailureReason.GeneralError
        };

        foreach (var reason in retriableReasons)
        {
            var attemptCount = 0;
            Func<Task<string>> operation = () =>
            {
                attemptCount++;
                if (attemptCount == 1)
                {
                    throw new ServiceBusException($"Error {reason}", reason);
                }
                return Task.FromResult("success");
            };

            // Act
            var result = await RetryHelper.ExecuteWithRetryAsync(
                operation,
                maxRetries: 3,
                baseDelay: TimeSpan.FromMilliseconds(10),
                logger: _logger,
                operationName: "Test Operation");

            // Assert
            Assert.Equal("success", result);
            Assert.Equal(2, attemptCount);
        }
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_NonRetriableServiceBusExceptions_AreNotRetried()
    {
        // Arrange
        var nonRetriableReasons = new[]
        {
            ServiceBusFailureReason.MessageNotFound,
            ServiceBusFailureReason.MessagingEntityNotFound,
            ServiceBusFailureReason.Unauthorized,
            ServiceBusFailureReason.QuotaExceeded
        };

        foreach (var reason in nonRetriableReasons)
        {
            var attemptCount = 0;
            Func<Task<string>> operation = () =>
            {
                attemptCount++;
                throw new ServiceBusException($"Error {reason}", reason);
            };

            // Act & Assert
            await Assert.ThrowsAsync<ServiceBusException>(() => 
                RetryHelper.ExecuteWithRetryAsync(
                    operation,
                    maxRetries: 3,
                    baseDelay: TimeSpan.FromMilliseconds(10),
                    logger: _logger,
                    operationName: "Test Operation"));

            Assert.Equal(1, attemptCount);
        }
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_CancellationRequested_ThrowsOperationCanceledException()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        cts.Cancel();

        Func<Task<string>> operation = () =>
        {
            throw new ServiceBusException("Transient error", ServiceBusFailureReason.ServiceTimeout);
        };

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() => 
            RetryHelper.ExecuteWithRetryAsync(
                operation,
                maxRetries: 3,
                baseDelay: TimeSpan.FromMilliseconds(100),
                logger: _logger,
                operationName: "Test Operation",
                cancellationToken: cts.Token));
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_VoidOperation_CompletesSuccessfully()
    {
        // Arrange
        var executionCount = 0;
        Func<Task> operation = () =>
        {
            executionCount++;
            return Task.CompletedTask;
        };

        // Act
        await RetryHelper.ExecuteWithRetryAsync(
            operation,
            maxRetries: 3,
            logger: _logger,
            operationName: "Test Operation");

        // Assert
        Assert.Equal(1, executionCount);
    }

    [Fact]
    public async Task ExecuteWithRetryAsync_VoidOperationWithRetries_RetriesCorrectly()
    {
        // Arrange
        var attemptCount = 0;
        Func<Task> operation = () =>
        {
            attemptCount++;
            if (attemptCount == 1)
            {
                throw new ServiceBusException("Transient error", ServiceBusFailureReason.ServiceTimeout);
            }
            return Task.CompletedTask;
        };

        // Act
        await RetryHelper.ExecuteWithRetryAsync(
            operation,
            maxRetries: 3,
            baseDelay: TimeSpan.FromMilliseconds(10),
            logger: _logger,
            operationName: "Test Operation");

        // Assert
        Assert.Equal(2, attemptCount);
    }
}