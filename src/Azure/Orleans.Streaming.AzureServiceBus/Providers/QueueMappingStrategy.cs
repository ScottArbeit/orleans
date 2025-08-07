namespace Orleans.Configuration;

/// <summary>
/// Specifies the strategy for mapping streams to Azure Service Bus entities.
/// </summary>
public enum QueueMappingStrategy
{
    /// <summary>
    /// All streams map to a single entity.
    /// Simple strategy for low-throughput scenarios.
    /// </summary>
    Single,

    /// <summary>
    /// Streams are distributed across multiple entities using consistent hashing.
    /// Provides even distribution and consistent mapping across restarts.
    /// </summary>
    HashBased,

    /// <summary>
    /// Streams are distributed across entities using round-robin allocation.
    /// Provides even distribution but requires state management.
    /// </summary>
    RoundRobin,

    /// <summary>
    /// Streams are mapped based on their namespace to specific entities.
    /// Allows logical grouping of streams by namespace.
    /// </summary>
    NamespaceBased,

    /// <summary>
    /// Custom mapping function provided by user code.
    /// Allows for complex mapping scenarios not covered by built-in strategies.
    /// </summary>
    Custom
}