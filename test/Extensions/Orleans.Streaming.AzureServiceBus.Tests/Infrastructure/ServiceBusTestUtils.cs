using Azure.Messaging.ServiceBus;
using Orleans.Streams;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace Orleans.Streaming.AzureServiceBus.Tests.Infrastructure;

/// <summary>
/// Utility methods for Service Bus integration tests.
/// </summary>
public static class ServiceBusTestUtils
{
    /// <summary>
    /// Checks if a TCP port is available on localhost.
    /// </summary>
    public static bool IsPortAvailable(int port)
    {
        try
        {
            using var tcpListener = new TcpListener(System.Net.IPAddress.Loopback, port);
            tcpListener.Start();
            tcpListener.Stop();
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Waits for a TCP port to become available with timeout.
    /// </summary>
    public static async Task WaitForPortAsync(string host, int port, TimeSpan timeout)
    {
        var startTime = DateTime.UtcNow;
        
        while (DateTime.UtcNow - startTime < timeout)
        {
            try
            {
                using var tcpClient = new TcpClient();
                await tcpClient.ConnectAsync(host, port);
                return; // Connection successful
            }
            catch
            {
                await Task.Delay(500);
            }
        }
        
        throw new TimeoutException($"Port {port} on {host} did not become available within {timeout.TotalSeconds} seconds");
    }

    /// <summary>
    /// Generates test event data for streaming tests.
    /// </summary>
    public static IEnumerable<TestEvent> GenerateTestEvents(int count, string streamKey = "test-stream")
    {
        for (int i = 0; i < count; i++)
        {
            yield return new TestEvent
            {
                Id = Guid.NewGuid(),
                StreamKey = streamKey,
                SequenceNumber = i,
                Timestamp = DateTimeOffset.UtcNow,
                Data = $"Test data {i}",
                Properties = new Dictionary<string, object>
                {
                    { "Index", i },
                    { "StreamKey", streamKey },
                    { "CreatedAt", DateTime.UtcNow }
                }
            };
        }
    }

    /// <summary>
    /// Creates a unique stream ID for testing.
    /// </summary>
    public static StreamId CreateTestStreamId(string nameSpace = "test", string? key = null)
    {
        key ??= Guid.NewGuid().ToString("N")[..8];
        return StreamId.Create(nameSpace, key);
    }

    /// <summary>
    /// Validates that a Service Bus connection string is properly formatted.
    /// </summary>
    public static bool IsValidConnectionString(string connectionString)
    {
        try
        {
            _ = new ServiceBusConnectionStringProperties(connectionString);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Creates test configuration defaults for Service Bus options.
    /// </summary>
    public static void ConfigureTestDefaults(Action<AzureServiceBusOptions> configure, string connectionString)
    {
        configure(new AzureServiceBusOptions
        {
            ConnectionString = connectionString,
            BatchSize = 10,
            MaxConcurrentCalls = 1,
            OperationTimeout = TimeSpan.FromSeconds(30)
        });
    }

    /// <summary>
    /// Performance metric collection helper for stream throughput tests.
    /// </summary>
    public class PerformanceMetrics
    {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public int MessagesSent { get; set; }
        public int MessagesReceived { get; set; }
        public TimeSpan Duration => EndTime - StartTime;
        public double ThroughputPerSecond => MessagesReceived / Duration.TotalSeconds;
        public List<TimeSpan> MessageLatencies { get; set; } = new();

        public void RecordMessageLatency(TimeSpan latency)
        {
            MessageLatencies.Add(latency);
        }

        public double AverageLatencyMs => MessageLatencies.Count > 0 
            ? MessageLatencies.Average(l => l.TotalMilliseconds) 
            : 0;

        public double P95LatencyMs => MessageLatencies.Count > 0 
            ? MessageLatencies.OrderBy(l => l.TotalMilliseconds).Skip((int)(MessageLatencies.Count * 0.95)).First().TotalMilliseconds 
            : 0;
    }
}

/// <summary>
/// Test event class for streaming integration tests.
/// </summary>
public class TestEvent
{
    public Guid Id { get; set; }
    public string StreamKey { get; set; } = string.Empty;
    public int SequenceNumber { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    public string Data { get; set; } = string.Empty;
    public Dictionary<string, object> Properties { get; set; } = new();
    
    public override bool Equals(object? obj)
    {
        return obj is TestEvent other && Id == other.Id;
    }
    
    public override int GetHashCode() => Id.GetHashCode();
}