using System.Runtime.CompilerServices;

namespace Orleans.Streaming.ServiceBus;

/// <summary>
/// The <see cref="Orleans.Streaming.ServiceBus"/> namespace contains the core implementation of Azure Service Bus streaming for Orleans.
/// </summary>
/// <remarks>
/// <para>
/// This namespace contains the internal implementation classes that power the Azure Service Bus streaming
/// provider. While most applications will interact with the configuration and hosting APIs, advanced
/// users may need to understand these implementations for customization or troubleshooting.
/// </para>
/// <para>
/// Key implementation areas include:
/// <list type="bullet">
/// <item><description>Adapter factories and implementations for both queues and topics</description></item>
/// <item><description>Batch container implementations for message handling</description></item>
/// <item><description>Client factory for managing Service Bus connections</description></item>
/// <item><description>Telemetry integration with OpenTelemetry</description></item>
/// </list>
/// </para>
/// <example>
/// <code>
/// // Custom data adapter with C# 13 primary constructor
/// public class CustomServiceBusDataAdapter(IServiceProvider serviceProvider, ILogger&lt;CustomServiceBusDataAdapter&gt; logger) 
///     : IQueueDataAdapter&lt;string, IBatchContainer&gt;
/// {
///     public IBatchContainer FromQueueMessage(string message, long sequenceId)
///     {
///         logger.LogDebug("Converting message with sequence {SequenceId}", sequenceId);
///         // Custom implementation
///         return new ServiceBusBatchContainer(message, sequenceId);
///     }
/// 
///     public string ToQueueMessage&lt;T&gt;(StreamId streamId, IEnumerable&lt;T&gt; events, 
///         StreamSequenceToken? token, Dictionary&lt;string, object&gt;? requestContext)
///     {
///         // Custom serialization logic
///         return JsonSerializer.Serialize(new { streamId, events, token, requestContext });
///     }
/// }
/// </code>
/// </example>
/// </remarks>
[CompilerGenerated]
internal class NamespaceDoc;