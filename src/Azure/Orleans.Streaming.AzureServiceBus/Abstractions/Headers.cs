namespace Orleans.Streaming.AzureServiceBus;

/// <summary>
/// Constants for Service Bus message header keys used in Orleans streaming.
/// 
/// <para>
/// These header keys are used in Azure Service Bus message application properties to store
/// Orleans streaming metadata. The headers are stable and documented for compatibility
/// across versions.
/// </para>
/// </summary>
/// <remarks>
/// All header constants are prefixed with "orleans-" to avoid conflicts with user-defined
/// application properties in Service Bus messages.
/// </remarks>
public static class Headers
{
    /// <summary>
    /// Header key for the stream namespace in Service Bus message application properties.
    /// </summary>
    public const string StreamNamespace = "orleans-stream-namespace";

    /// <summary>
    /// Header key for the stream identifier in Service Bus message application properties.
    /// </summary>
    public const string StreamId = "orleans-stream-id";

    /// <summary>
    /// Header key for the sequence token in Service Bus message application properties.
    /// </summary>
    public const string SequenceToken = "orleans-sequence-token";

    /// <summary>
    /// Header key for the batch index in Service Bus message application properties.
    /// </summary>
    public const string BatchIndex = "orleans-batch-index";

    /// <summary>
    /// Content type for Orleans stream events.
    /// </summary>
    public const string ContentType = "application/vnd.orleans.stream-events+json";
}