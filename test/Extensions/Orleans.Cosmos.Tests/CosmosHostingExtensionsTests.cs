using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Persistence.Cosmos;
using Orleans.Runtime;

namespace Tester.Cosmos.Persistence;

public class CosmosHostingExtensionsTests
{
    [Fact]
    public async Task AddCosmosGrainStorage_NamedProvidersKeepIndependentConfigurationAndDocumentKeys()
    {
        using var host = new HostBuilder()
            .UseOrleans(builder =>
            {
                builder.AddCosmosGrainStorage("first", options =>
                {
                    options.ContainerName = "one-key-container";
                    options.PartitionKeyLevelCount = 1;
                }, typeof(FirstDocumentIdProvider));
                builder.AddCosmosGrainStorage("second", options =>
                {
                    options.ContainerName = "two-key-container";
                    options.PartitionKeyLevelCount = 2;
                }, typeof(SecondDocumentIdProvider));
                builder.AddCosmosGrainStorage("third", options =>
                {
                    options.ContainerName = "three-key-container";
                    options.PartitionKeyLevelCount = 3;
                }, typeof(ThirdDocumentIdProvider));
            })
            .Build();

        Assert.IsType<FirstDocumentIdProvider>(host.Services.GetRequiredKeyedService<IDocumentIdProvider>("first"));
        Assert.IsType<SecondDocumentIdProvider>(host.Services.GetRequiredKeyedService<IDocumentIdProvider>("second"));
        Assert.IsType<ThirdDocumentIdProvider>(host.Services.GetRequiredKeyedService<IDocumentIdProvider>("third"));

        var options = host.Services.GetRequiredService<IOptionsMonitor<CosmosGrainStorageOptions>>();
        Assert.Equal("one-key-container", options.Get("first").ContainerName);
        Assert.Equal(1, options.Get("first").PartitionKeyLevelCount);
        Assert.Equal("two-key-container", options.Get("second").ContainerName);
        Assert.Equal(2, options.Get("second").PartitionKeyLevelCount);
        Assert.Equal("three-key-container", options.Get("third").ContainerName);
        Assert.Equal(3, options.Get("third").PartitionKeyLevelCount);

        var grainId = GrainId.Create("type", "key");
        Assert.Equal(["one"], (await CosmosStorageFactory.Create(host.Services, "first").ResolveDocumentKey("grain", grainId)).PartitionKeyValues);
        Assert.Equal(["one", "two"], (await CosmosStorageFactory.Create(host.Services, "second").ResolveDocumentKey("grain", grainId)).PartitionKeyValues);
        Assert.Equal(["one", "two", "three"], (await CosmosStorageFactory.Create(host.Services, "third").ResolveDocumentKey("grain", grainId)).PartitionKeyValues);
    }

#pragma warning disable CS0618 // Type or member is obsolete
    [Fact]
    public void AddCosmosGrainStorage_LegacyPartitionKeyProvidersAreRegisteredByKey()
    {
        using var host = new HostBuilder()
            .UseOrleans(builder =>
            {
                builder.AddCosmosGrainStorage<FirstPartitionKeyProvider>(
                    "legacy-generic",
                    (CosmosGrainStorageOptions _) => { });
                builder.AddCosmosGrainStorage(
                    "legacy-type",
                    (CosmosGrainStorageOptions _) => { },
                    customPartitionKeyProviderType: typeof(SecondPartitionKeyProvider));
                builder.AddCosmosGrainStorage<FirstPartitionKeyProvider>("legacy-options-generic");
                builder.AddCosmosGrainStorage(
                    "legacy-options-type",
                    customPartitionKeyProviderType: typeof(SecondPartitionKeyProvider));
            })
            .Build();

        Assert.IsType<FirstPartitionKeyProvider>(host.Services.GetRequiredKeyedService<IPartitionKeyProvider>("legacy-generic"));
        Assert.IsType<SecondPartitionKeyProvider>(host.Services.GetRequiredKeyedService<IPartitionKeyProvider>("legacy-type"));
        Assert.IsType<FirstPartitionKeyProvider>(host.Services.GetRequiredKeyedService<IPartitionKeyProvider>("legacy-options-generic"));
        Assert.IsType<SecondPartitionKeyProvider>(host.Services.GetRequiredKeyedService<IPartitionKeyProvider>("legacy-options-type"));
    }

    [Fact]
    public async Task DefaultDocumentIdProvider_UsesLegacyPartitionKeyProvider()
    {
        var provider = new DefaultDocumentIdProvider(
            Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ServiceId = "service" }),
            new FirstPartitionKeyProvider());

        var identifiers = await provider.GetDocumentIdentifiers("grain-type", GrainId.Create("type", "key"));
        var documentKey = await ((IDocumentIdProvider)provider).GetDocumentKey("grain-type", GrainId.Create("type", "key"));

        Assert.Equal("first", identifiers.PartitionKey);
        Assert.Equal("service__type_key", identifiers.DocumentId);
        Assert.Equal("service__type_key", documentKey.DocumentId);
        Assert.Equal(["first"], documentKey.PartitionKeyValues);
    }

#pragma warning restore CS0618 // Type or member is obsolete

    private sealed class FirstDocumentIdProvider : IDocumentIdProvider
    {
        public ValueTask<(string DocumentId, string PartitionKey)> GetDocumentIdentifiers(string grainType, GrainId grainId) =>
            new(("first-document", "one"));
    }

    private sealed class SecondDocumentIdProvider : IDocumentIdProvider
    {
        public ValueTask<(string DocumentId, string PartitionKey)> GetDocumentIdentifiers(string grainType, GrainId grainId) =>
            new(("second-document", "one"));

        public ValueTask<CosmosDocumentKey> GetDocumentKey(string grainType, GrainId grainId) =>
            new(new CosmosDocumentKey("second-document", ["one", "two"]));
    }

    private sealed class ThirdDocumentIdProvider : IDocumentIdProvider
    {
        public ValueTask<(string DocumentId, string PartitionKey)> GetDocumentIdentifiers(string grainType, GrainId grainId) =>
            new(("third-document", "one"));

        public ValueTask<CosmosDocumentKey> GetDocumentKey(string grainType, GrainId grainId) =>
            new(new CosmosDocumentKey("third-document", ["one", "two", "three"]));
    }

#pragma warning disable CS0618 // Type or member is obsolete
    private sealed class FirstPartitionKeyProvider : IPartitionKeyProvider
    {
        public ValueTask<string> GetPartitionKey(string grainType, GrainId grainId) => new("first");
    }

    private sealed class SecondPartitionKeyProvider : IPartitionKeyProvider
    {
        public ValueTask<string> GetPartitionKey(string grainType, GrainId grainId) => new("second");
    }
#pragma warning restore CS0618 // Type or member is obsolete
}
