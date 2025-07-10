using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Streaming.ServiceBus;

/// <summary>
/// A thread-safe factory for creating and caching Azure Service Bus clients.
/// Provides centralized client management with support for both connection string 
/// and TokenCredential authentication methods.
/// 
/// Note: This factory is not responsible for key rotation. Applications should
/// handle credential refresh scenarios according to their security requirements.
/// </summary>
public sealed class ServiceBusClientFactory : IAsyncDisposable
{
    private static readonly Counter<int> ClientCreatedCounter = ServiceBusInstrumentation.Meter.CreateCounter<int>(
        "servicebus.client.created",
        description: "Number of ServiceBus clients created");

    private readonly ConcurrentDictionary<string, ServiceBusClient> _clients = new();
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly IOptionsMonitor<ServiceBusOptions> _optionsMonitor;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceBusClientFactory"/> class.
    /// </summary>
    /// <param name="optionsMonitor">The options monitor for ServiceBus configuration.</param>
    public ServiceBusClientFactory(IOptionsMonitor<ServiceBusOptions> optionsMonitor)
    {
        _optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    }

    /// <summary>
    /// Gets or creates a ServiceBus client for the specified options name.
    /// Returns the same instance for identical configurations within the same silo.
    /// </summary>
    /// <param name="optionsName">The name of the ServiceBus options configuration.</param>
    /// <returns>A ServiceBus client instance.</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the factory has been disposed.</exception>
    /// <exception cref="InvalidOperationException">Thrown when no client configuration is available.</exception>
    public async Task<ServiceBusClient> GetClientAsync(string optionsName)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ServiceBusClientFactory));
        }

        if (string.IsNullOrEmpty(optionsName))
        {
            throw new ArgumentException("Options name cannot be null or empty.", nameof(optionsName));
        }

        // Try to get existing client first (fast path)
        if (_clients.TryGetValue(optionsName, out var existingClient))
        {
            return existingClient;
        }

        // Use semaphore to ensure only one client is created per options name
        await _semaphore.WaitAsync();
        try
        {
            // Double-check pattern - another thread might have created the client
            if (_clients.TryGetValue(optionsName, out existingClient))
            {
                return existingClient;
            }

            return await CreateClientAsync(optionsName);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task<ServiceBusClient> CreateClientAsync(string optionsName)
    {
        using var activity = ServiceBusOptions.ActivitySource.StartActivity("client.create");
        activity?.SetTag("servicebus.options_name", optionsName);

        var options = _optionsMonitor.Get(optionsName);

        ServiceBusClient client;

        // Use pre-configured client if available
        if (options.ServiceBusClient is not null)
        {
            client = options.ServiceBusClient;
        }
        // Use factory function if available
        else if (options.CreateClient is not null)
        {
            client = await options.CreateClient();
        }
        else
        {
            throw new InvalidOperationException(
                $"No ServiceBus client configuration found for options '{optionsName}'. " +
                $"Use {nameof(ServiceBusOptions)}.{nameof(ServiceBusOptions.ConfigureServiceBusClient)} to configure the client.");
        }

        // Cache the client
        _clients[optionsName] = client;

        // Increment the counter
        ClientCreatedCounter.Add(1, new KeyValuePair<string, object?>("servicebus.options_name", optionsName));

        activity?.SetTag("servicebus.client.created", true);

        return client;
    }

    /// <summary>
    /// Disposes all cached ServiceBus clients asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await _semaphore.WaitAsync();
        try
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            // Dispose all cached clients
            foreach (var client in _clients.Values)
            {
                await client.DisposeAsync();
            }

            _clients.Clear();
        }
        finally
        {
            _semaphore.Release();
            _semaphore.Dispose();
        }
    }
}

/// <summary>
/// Contains ServiceBus instrumentation infrastructure.
/// </summary>
internal static class ServiceBusInstrumentation
{
    /// <summary>
    /// Gets the Meter for ServiceBus metrics.
    /// </summary>
    public static Meter Meter { get; } = new("Orleans.ServiceBus");
}