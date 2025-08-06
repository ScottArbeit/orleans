using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

#nullable enable

namespace Orleans.Streaming.AzureServiceBus.Providers
{
    /// <summary>
    /// Implements retry logic with exponential backoff for Azure Service Bus operations.
    /// </summary>
    internal static class RetryHelper
    {
        /// <summary>
        /// Executes an operation with retry logic and exponential backoff.
        /// </summary>
        /// <typeparam name="T">The return type of the operation.</typeparam>
        /// <param name="operation">The operation to execute.</param>
        /// <param name="maxRetries">The maximum number of retry attempts.</param>
        /// <param name="baseDelay">The base delay between retries.</param>
        /// <param name="maxDelay">The maximum delay between retries.</param>
        /// <param name="logger">The logger instance.</param>
        /// <param name="operationName">The name of the operation for logging.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the operation.</returns>
        public static async Task<T> ExecuteWithRetryAsync<T>(
            Func<Task<T>> operation,
            int maxRetries = 3,
            TimeSpan? baseDelay = null,
            TimeSpan? maxDelay = null,
            ILogger? logger = null,
            string operationName = "Operation",
            CancellationToken cancellationToken = default)
        {
            baseDelay ??= TimeSpan.FromSeconds(1);
            maxDelay ??= TimeSpan.FromSeconds(30);

            var attempt = 0;
            while (true)
            {
                try
                {
                    return await operation();
                }
                catch (Exception ex) when (attempt < maxRetries && IsRetriableException(ex))
                {
                    attempt++;
                    var delay = CalculateDelay(attempt, baseDelay.Value, maxDelay.Value);
                    
                    logger?.LogWarning(ex, 
                        "{OperationName} failed on attempt {Attempt}/{MaxRetries}. Retrying after {Delay}ms. Error: {Error}",
                        operationName, attempt, maxRetries + 1, delay.TotalMilliseconds, ex.Message);

                    await Task.Delay(delay, cancellationToken);
                }
            }
        }

        /// <summary>
        /// Executes an operation with retry logic and exponential backoff.
        /// </summary>
        /// <param name="operation">The operation to execute.</param>
        /// <param name="maxRetries">The maximum number of retry attempts.</param>
        /// <param name="baseDelay">The base delay between retries.</param>
        /// <param name="maxDelay">The maximum delay between retries.</param>
        /// <param name="logger">The logger instance.</param>
        /// <param name="operationName">The name of the operation for logging.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the operation.</returns>
        public static async Task ExecuteWithRetryAsync(
            Func<Task> operation,
            int maxRetries = 3,
            TimeSpan? baseDelay = null,
            TimeSpan? maxDelay = null,
            ILogger? logger = null,
            string operationName = "Operation",
            CancellationToken cancellationToken = default)
        {
            await ExecuteWithRetryAsync(
                async () =>
                {
                    await operation();
                    return true;
                },
                maxRetries,
                baseDelay,
                maxDelay,
                logger,
                operationName,
                cancellationToken);
        }

        /// <summary>
        /// Determines if an exception is retriable.
        /// </summary>
        /// <param name="exception">The exception to check.</param>
        /// <returns>True if the exception is retriable, false otherwise.</returns>
        private static bool IsRetriableException(Exception exception)
        {
            return exception switch
            {
                Azure.Messaging.ServiceBus.ServiceBusException sbEx => IsRetriableServiceBusException(sbEx),
                TimeoutException => true,
                TaskCanceledException => false, // Don't retry cancellation
                OperationCanceledException => false, // Don't retry cancellation
                _ => false
            };
        }

        /// <summary>
        /// Determines if a Service Bus exception is retriable.
        /// </summary>
        /// <param name="exception">The Service Bus exception.</param>
        /// <returns>True if the exception is retriable, false otherwise.</returns>
        private static bool IsRetriableServiceBusException(Azure.Messaging.ServiceBus.ServiceBusException exception)
        {
            return exception.Reason switch
            {
                Azure.Messaging.ServiceBus.ServiceBusFailureReason.ServiceTimeout => true,
                Azure.Messaging.ServiceBus.ServiceBusFailureReason.ServiceBusy => true,
                Azure.Messaging.ServiceBus.ServiceBusFailureReason.ServiceCommunicationProblem => true,
                Azure.Messaging.ServiceBus.ServiceBusFailureReason.GeneralError => true,
                _ => false
            };
        }

        /// <summary>
        /// Calculates the delay for the specified retry attempt using exponential backoff with jitter.
        /// </summary>
        /// <param name="attempt">The retry attempt number (1-based).</param>
        /// <param name="baseDelay">The base delay.</param>
        /// <param name="maxDelay">The maximum delay.</param>
        /// <returns>The calculated delay.</returns>
        private static TimeSpan CalculateDelay(int attempt, TimeSpan baseDelay, TimeSpan maxDelay)
        {
            // Exponential backoff: baseDelay * 2^(attempt-1)
            var exponentialDelay = TimeSpan.FromMilliseconds(
                baseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));

            // Cap at maxDelay
            var cappedDelay = exponentialDelay > maxDelay ? maxDelay : exponentialDelay;

            // Add jitter (±25% random variation)
            var random = new Random();
            var jitterFactor = 0.75 + (random.NextDouble() * 0.5); // 0.75 to 1.25
            var delayWithJitter = TimeSpan.FromMilliseconds(cappedDelay.TotalMilliseconds * jitterFactor);

            return delayWithJitter;
        }
    }
}