using System.Runtime.CompilerServices;

namespace Orleans.Configuration;

/// <summary>
/// The <see cref="Orleans.Configuration"/> namespace contains configuration classes for Azure Service Bus streaming.
/// </summary>
/// <remarks>
/// <para>
/// This namespace provides configuration options and validators for setting up Azure Service Bus 
/// as a streaming provider in Orleans applications. The main configuration class is 
/// <see cref="ServiceBusOptions"/>, which allows you to configure authentication, entity types,
/// performance settings, and other aspects of the Service Bus integration.
/// </para>
/// <para>
/// Key classes include:
/// <list type="bullet">
/// <item><description><see cref="ServiceBusOptions"/> - Main configuration class for Service Bus streaming</description></item>
/// <item><description><see cref="ServiceBusEntityType"/> - Enumeration for choosing between queues and topics</description></item>
/// <item><description><see cref="ServiceBusOptionsValidator"/> - Validates configuration options</description></item>
/// </list>
/// </para>
/// <example>
/// <code>
/// // Configure with connection string
/// services.Configure&lt;ServiceBusOptions&gt;("ServiceBusProvider", options =&gt;
/// {
///     options.ConnectionString = "Endpoint=sb://...";
///     options.EntityType = ServiceBusEntityType.Queue;
///     options.PartitionCount = 8;
/// });
/// 
/// // Configure with managed identity (C# 13 primary constructor)
/// public class ServiceBusConfigurator(IConfiguration configuration)
/// {
///     public void Configure(ServiceBusOptions options)
///     {
///         options.ConfigureServiceBusClient(
///             configuration["ServiceBus:Namespace"]!, 
///             new DefaultAzureCredential());
///     }
/// }
/// </code>
/// </example>
/// </remarks>
[CompilerGenerated]
internal class NamespaceDoc;