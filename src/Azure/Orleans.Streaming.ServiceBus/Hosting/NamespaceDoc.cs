using System.Runtime.CompilerServices;

namespace Orleans.Hosting;

/// <summary>
/// The <see cref="Orleans.Hosting"/> namespace contains extension methods for configuring Azure Service Bus streaming.
/// </summary>
/// <remarks>
/// <para>
/// This namespace provides fluent extension methods for both silo and client builders to easily 
/// configure Azure Service Bus streaming providers. These methods integrate seamlessly with the
/// Orleans hosting model and dependency injection system.
/// </para>
/// <para>
/// Key extension classes include:
/// <list type="bullet">
/// <item><description><see cref="SiloBuilderExtensions"/> - Extensions for configuring Service Bus on silos</description></item>
/// <item><description><see cref="ClientBuilderExtensions"/> - Extensions for configuring Service Bus on clients</description></item>
/// <item><description><see cref="SiloServiceBusStreamConfigurator"/> - Fluent configurator for advanced silo scenarios</description></item>
/// <item><description><see cref="ClusterClientServiceBusStreamConfigurator"/> - Fluent configurator for client scenarios</description></item>
/// </list>
/// </para>
/// <example>
/// <code>
/// // Basic silo configuration
/// builder.UseOrleans(siloBuilder =&gt;
/// {
///     siloBuilder.AddServiceBusStreams("ServiceBusProvider", options =&gt;
///     {
///         options.ConnectionString = "Endpoint=sb://...";
///         options.EntityType = ServiceBusEntityType.Topic;
///     });
/// });
/// 
/// // Advanced configuration with C# 13 primary constructor
/// public class StreamingConfiguration(IConfiguration config, ILogger&lt;StreamingConfiguration&gt; logger)
/// {
///     public void ConfigureSilo(ISiloBuilder siloBuilder)
///     {
///         siloBuilder.AddServiceBusStreams("ServiceBusProvider", configurator =&gt;
///         {
///             configurator.ConfigureServiceBus(optionsBuilder =&gt;
///             {
///                 optionsBuilder.Configure(options =&gt;
///                 {
///                     options.ConfigureServiceBusClient(config.GetConnectionString("ServiceBus")!);
///                     options.PartitionCount = 16;
///                     logger.LogInformation("Configured Service Bus streaming with {PartitionCount} partitions", options.PartitionCount);
///                 });
///             });
///         });
///     }
/// }
/// </code>
/// </example>
/// </remarks>
[CompilerGenerated]
internal class NamespaceDoc;