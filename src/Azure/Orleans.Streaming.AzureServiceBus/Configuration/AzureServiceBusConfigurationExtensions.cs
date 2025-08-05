using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Configuration;

/// <summary>
/// Extension methods for configuring Azure Service Bus streaming options.
/// </summary>
public static class AzureServiceBusConfigurationExtensions
{
    /// <summary>
    /// Configures Azure Service Bus options from an <see cref="IConfiguration"/> instance.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration instance to bind.</param>
    /// <param name="name">The name of the options instance.</param>
    /// <returns>The configured options builder.</returns>
    public static OptionsBuilder<AzureServiceBusOptions> Configure(
        this IServiceCollection services,
        IConfiguration configuration,
        string name = null)
    {
        return services.AddOptions<AzureServiceBusOptions>(name).Bind(configuration);
    }

    /// <summary>
    /// Configures Azure Service Bus queue options from an <see cref="IConfiguration"/> instance.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration instance to bind.</param>
    /// <param name="name">The name of the options instance.</param>
    /// <returns>The configured options builder.</returns>
    public static OptionsBuilder<AzureServiceBusQueueOptions> ConfigureQueue(
        this IServiceCollection services,
        IConfiguration configuration,
        string name = null)
    {
        return services.AddOptions<AzureServiceBusQueueOptions>(name).Bind(configuration);
    }

    /// <summary>
    /// Configures Azure Service Bus topic options from an <see cref="IConfiguration"/> instance.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration instance to bind.</param>
    /// <param name="name">The name of the options instance.</param>
    /// <returns>The configured options builder.</returns>
    public static OptionsBuilder<AzureServiceBusTopicOptions> ConfigureTopic(
        this IServiceCollection services,
        IConfiguration configuration,
        string name = null)
    {
        return services.AddOptions<AzureServiceBusTopicOptions>(name).Bind(configuration);
    }

    /// <summary>
    /// Configures Azure Service Bus options with validation using the custom validator.
    /// </summary>
    /// <param name="builder">The options builder.</param>
    /// <param name="name">The name of the options instance.</param>
    /// <returns>The configured options builder with validation.</returns>
    public static OptionsBuilder<AzureServiceBusOptions> ValidateConfiguration(
        this OptionsBuilder<AzureServiceBusOptions> builder, string name = null)
    {
        return builder.Validate(options =>
        {
            var validator = new AzureServiceBusOptionsValidator(options, name ?? string.Empty);
            validator.ValidateConfiguration();
            return true;
        });
    }

    /// <summary>
    /// Configures Azure Service Bus queue options with validation using the custom validator.
    /// </summary>
    /// <param name="builder">The options builder.</param>
    /// <param name="name">The name of the options instance.</param>
    /// <returns>The configured options builder with validation.</returns>
    public static OptionsBuilder<AzureServiceBusQueueOptions> ValidateConfiguration(
        this OptionsBuilder<AzureServiceBusQueueOptions> builder, string name = null)
    {
        return builder.Validate(options =>
        {
            var validator = new AzureServiceBusOptionsValidator(options, name ?? string.Empty);
            validator.ValidateConfiguration();
            return true;
        });
    }

    /// <summary>
    /// Configures Azure Service Bus topic options with validation using the custom validator.
    /// </summary>
    /// <param name="builder">The options builder.</param>
    /// <param name="name">The name of the options instance.</param>
    /// <returns>The configured options builder with validation.</returns>
    public static OptionsBuilder<AzureServiceBusTopicOptions> ValidateConfiguration(
        this OptionsBuilder<AzureServiceBusTopicOptions> builder, string name = null)
    {
        return builder.Validate(options =>
        {
            var validator = new AzureServiceBusOptionsValidator(options, name ?? string.Empty);
            validator.ValidateConfiguration();
            return true;
        });
    }
}