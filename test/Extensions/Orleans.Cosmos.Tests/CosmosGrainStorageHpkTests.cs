using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Persistence.Cosmos;
using Orleans.Runtime;
using Orleans.Serialization.Serializers;

namespace Tester.Cosmos.Persistence;

[TestCategory("Cosmos"), TestCategory("BVT")]
public class CosmosGrainStorageHpkTests
{
    private static readonly GrainId TestGrainId = GrainId.Create("test-type", "test-key");

    [Fact]
    public void PartitionKeyLevelCount_DefaultsToOne()
    {
        var options = new CosmosGrainStorageOptions();

        Assert.Equal(1, options.PartitionKeyLevelCount);
        Assert.Equal("/PartitionKey", options.PartitionKeyPath);
    }

    [Fact]
    public async Task ExistingDocumentIdProvider_AdaptsToSinglePartitionKeyValue()
    {
        IDocumentIdProvider provider = new ExistingDocumentIdProvider();

        var key = await provider.GetDocumentKey("grain-type", TestGrainId);

        Assert.Equal("document-id", key.DocumentId);
        Assert.Equal(["partition-key"], key.PartitionKeyValues);
    }

    [Fact]
    public void SupportsHierarchicalPartitionKeys_DistinguishesLegacyAndHpkProviders()
    {
        Assert.False(CosmosGrainStorage.SupportsHierarchicalPartitionKeys(new ExistingDocumentIdProvider()));
        Assert.True(CosmosGrainStorage.SupportsHierarchicalPartitionKeys(
            new HierarchicalDocumentIdProvider("one", "two")));
        Assert.True(CosmosGrainStorage.SupportsHierarchicalPartitionKeys(
            new ExplicitHierarchicalDocumentIdProvider()));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task ResolveDocumentKey_BuildsCompleteHierarchicalPartitionKey(int levelCount)
    {
        var values = Enumerable.Range(1, levelCount).Select(index => $"value-{index}").ToArray();
        var storage = CreateStorage(levelCount, new HierarchicalDocumentIdProvider(values));

        var key = await storage.ResolveDocumentKey("grain-type", TestGrainId);

        var expectedBuilder = new PartitionKeyBuilder();
        foreach (var value in values)
        {
            expectedBuilder.Add(value);
        }

        Assert.Equal("document-id", key.DocumentId);
        Assert.Equal(values, key.PartitionKeyValues);
        Assert.Equal(expectedBuilder.Build(), key.PartitionKey);
    }

    [Fact]
    public async Task ResolveDocumentKey_PreservesSpecialStringValues()
    {
        string[] values = ["", "租户|repository", "kind__segment:3"];
        var storage = CreateStorage(3, new HierarchicalDocumentIdProvider(values));

        var key = await storage.ResolveDocumentKey("grain-type", TestGrainId);

        Assert.Equal(values, key.PartitionKeyValues);
        Assert.Equal(BuildPartitionKey(values), key.PartitionKey);
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(2, 3)]
    [InlineData(3, 2)]
    public async Task ResolveDocumentKey_RejectsPartitionKeyValueCountMismatch(int configuredLevelCount, int providedValueCount)
    {
        var values = Enumerable.Range(1, providedValueCount).Select(index => $"value-{index}").ToArray();
        var storage = CreateStorage(configuredLevelCount, new HierarchicalDocumentIdProvider(values));

        var exception = await Assert.ThrowsAsync<OrleansConfigurationException>(
            async () => await storage.ResolveDocumentKey("grain-type", TestGrainId));

        Assert.Contains($"returned {providedValueCount} partition-key value(s)", exception.Message);
        Assert.Contains($"requires {configuredLevelCount}", exception.Message);
    }

    [Fact]
    public async Task ResolveDocumentKey_RejectsNullPartitionKeyValue()
    {
        var storage = CreateStorage(2, new HierarchicalDocumentIdProvider("tenant", null));

        var exception = await Assert.ThrowsAsync<OrleansConfigurationException>(
            async () => await storage.ResolveDocumentKey("grain-type", TestGrainId));

        Assert.Contains("returned a null partition-key value", exception.Message);
    }

    [Fact]
    public async Task ResolveDocumentKey_RejectsLegacyProviderForHierarchicalPartitioning()
    {
        var storage = CreateStorage(2, new ExistingDocumentIdProvider());

        var exception = await Assert.ThrowsAsync<OrleansConfigurationException>(
            async () => await storage.ResolveDocumentKey("grain-type", TestGrainId));

        Assert.Contains("returned 1 partition-key value(s)", exception.Message);
        Assert.Contains("requires 2", exception.Message);
    }

    [Fact]
    public async Task Startup_RejectsLegacyDocumentIdProviderForHierarchicalPartitioning()
    {
        var services = new ServiceCollection().AddLogging().BuildServiceProvider();
        var storage = CreateStorage(2, new ExistingDocumentIdProvider());
        var lifecycle = ActivatorUtilities.CreateInstance<SiloLifecycleSubject>(services);
        storage.Participate(lifecycle);

        var exception = await Assert.ThrowsAsync<WrappedException>(() => lifecycle.OnStart());

        Assert.Contains(nameof(IDocumentIdProvider.GetDocumentKey), exception.Message);
        Assert.Contains("legacy single-key contract", exception.Message);
    }

    [Fact]
    public async Task CreateEntity_PopulatesEveryHierarchicalPartitionKeyProperty()
    {
        var storage = CreateStorage(3, new HierarchicalDocumentIdProvider("tenant", "grain-type", "region"));
        var key = await storage.ResolveDocumentKey("grain-type", TestGrainId);

        var entity = CosmosGrainStorage.CreateEntity(key, "grain-type", new TestState { Value = 7 }, "etag");

        Assert.Equal("tenant", entity.PartitionKey);
        Assert.Equal("grain-type", entity.PartitionKey2);
        Assert.Equal("region", entity.PartitionKey3);
    }

    [Fact]
    public async Task CreateEntity_OmitsUnusedPartitionKeyPropertiesInLegacyMode()
    {
        var storage = CreateStorage(1, new ExistingDocumentIdProvider());
        var key = await storage.ResolveDocumentKey("grain-type", TestGrainId);
        var entity = CosmosGrainStorage.CreateEntity(key, "grain-type", new TestState { Value = 7 }, "etag");

        var newtonsoftJson = JsonConvert.SerializeObject(entity);
        var systemTextJson = System.Text.Json.JsonSerializer.Serialize(entity);

        Assert.DoesNotContain(nameof(GrainStateEntity<object>.PartitionKey2), newtonsoftJson);
        Assert.DoesNotContain(nameof(GrainStateEntity<object>.PartitionKey3), newtonsoftJson);
        Assert.DoesNotContain(nameof(GrainStateEntity<object>.PartitionKey2), systemTextJson);
        Assert.DoesNotContain(nameof(GrainStateEntity<object>.PartitionKey3), systemTextJson);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task CreateEntity_SerializesQueryableShapeWithBothJsonSerializers(int levelCount)
    {
        var values = Enumerable.Range(1, levelCount).Select(index => $"value-{index}").ToArray();
        var storage = CreateStorage(levelCount, new HierarchicalDocumentIdProvider(values));
        var key = await storage.ResolveDocumentKey("grain-type", TestGrainId);
        var entity = CosmosGrainStorage.CreateEntity(key, "grain-type", new TestState { Value = 7 }, "etag");

        var newtonsoftDocument = Newtonsoft.Json.Linq.JObject.Parse(JsonConvert.SerializeObject(entity));
        Assert.Equal("document-id", newtonsoftDocument.Value<string>("id"));
        Assert.Equal("grain-type", newtonsoftDocument.Value<string>("GrainType"));
        Assert.Equal(7, newtonsoftDocument["State"]!.Value<int>("Value"));
        AssertSerializedPartitionKeys(levelCount, values, propertyName => newtonsoftDocument.Value<string>(propertyName));

        using var systemTextDocument = System.Text.Json.JsonDocument.Parse(System.Text.Json.JsonSerializer.Serialize(entity));
        var root = systemTextDocument.RootElement;
        Assert.Equal("document-id", root.GetProperty("id").GetString());
        Assert.Equal("grain-type", root.GetProperty("GrainType").GetString());
        Assert.Equal(7, root.GetProperty("State").GetProperty("Value").GetInt32());
        AssertSerializedPartitionKeys(
            levelCount,
            values,
            propertyName => root.TryGetProperty(propertyName, out var value) ? value.GetString() : null);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(4)]
    public void Constructor_RejectsUnsupportedPartitionKeyLevelCount(int levelCount)
    {
        var options = new CosmosGrainStorageOptions { PartitionKeyLevelCount = levelCount };

        var exception = Assert.Throws<OrleansConfigurationException>(() => CreateStorage(options, new ExistingDocumentIdProvider()));

        Assert.Contains("Supported values are 1, 2, and 3", exception.Message);
    }

    [Fact]
    public void Constructor_PreservesCustomSinglePartitionKeyPath()
    {
        var options = new CosmosGrainStorageOptions { PartitionKeyPath = "/CustomPartitionKey" };

        var storage = CreateStorage(options, new ExistingDocumentIdProvider());

        CosmosGrainStorage.ValidateContainerPartitionKeyPaths(
            [options.PartitionKeyPath],
            ["/CustomPartitionKey"],
            "test",
            "container");
        Assert.NotNull(storage);
    }

    [Theory]
    [MemberData(nameof(MismatchedContainerPartitionKeyDefinitions))]
    public void ValidateContainerPartitionKeyPaths_RejectsMismatches(string[] configuredPaths, string[] containerPaths)
    {
        var exception = Assert.Throws<OrleansConfigurationException>(() =>
            CosmosGrainStorage.ValidateContainerPartitionKeyPaths(configuredPaths, containerPaths, "test", "container"));

        Assert.Contains("number, order, and name", exception.Message);
        Assert.Contains(string.Join(", ", configuredPaths), exception.Message);
        Assert.Contains(string.Join(", ", containerPaths), exception.Message);
    }

    [Theory]
    [MemberData(nameof(MatchingContainerPartitionKeyDefinitions))]
    public void ValidateContainerPartitionKeyPaths_AcceptsExactMatch(string[] configuredPaths, string[] containerPaths)
    {
        CosmosGrainStorage.ValidateContainerPartitionKeyPaths(configuredPaths, containerPaths, "test", "container");
    }

    public static TheoryData<string[], string[]> MismatchedContainerPartitionKeyDefinitions => new()
    {
        { ["/PartitionKey", "/PartitionKey2"], ["/PartitionKey"] },
        { ["/PartitionKey"], ["/PartitionKey", "/PartitionKey2"] },
        { ["/PartitionKey", "/PartitionKey2"], ["/PartitionKey", "/PartitionKey2", "/PartitionKey3"] },
        { ["/PartitionKey", "/PartitionKey2"], ["/PartitionKey2", "/PartitionKey"] },
        { ["/PartitionKey", "/PartitionKey2"], ["/PartitionKey", "/Different"] }
    };

    public static TheoryData<string[], string[]> MatchingContainerPartitionKeyDefinitions => new()
    {
        { ["/PartitionKey"], ["/PartitionKey"] },
        { ["/CustomPartitionKey"], ["/CustomPartitionKey"] },
        { ["/PartitionKey", "/PartitionKey2"], ["/PartitionKey", "/PartitionKey2"] },
        { ["/PartitionKey", "/PartitionKey2", "/PartitionKey3"], ["/PartitionKey", "/PartitionKey2", "/PartitionKey3"] }
    };

    private static CosmosGrainStorage CreateStorage(int partitionKeyLevelCount, IDocumentIdProvider documentIdProvider)
    {
        return CreateStorage(
            new CosmosGrainStorageOptions { PartitionKeyLevelCount = partitionKeyLevelCount },
            documentIdProvider);
    }

    private static CosmosGrainStorage CreateStorage(CosmosGrainStorageOptions options, IDocumentIdProvider documentIdProvider)
    {
        var services = new ServiceCollection().AddLogging().BuildServiceProvider();
        return new CosmosGrainStorage(
            "test",
            options,
            services.GetRequiredService<ILoggerFactory>(),
            services,
            Options.Create(new ClusterOptions { ServiceId = "service" }),
            documentIdProvider,
            activatorProvider: null!);
    }

    private static PartitionKey BuildPartitionKey(IEnumerable<string> values)
    {
        var builder = new PartitionKeyBuilder();
        foreach (var value in values)
        {
            builder.Add(value);
        }

        return builder.Build();
    }

    private static void AssertSerializedPartitionKeys(
        int levelCount,
        IReadOnlyList<string> values,
        Func<string, string> getValue)
    {
        Assert.Equal(values[0], getValue("PartitionKey"));
        Assert.Equal(levelCount > 1 ? values[1] : null, getValue("PartitionKey2"));
        Assert.Equal(levelCount > 2 ? values[2] : null, getValue("PartitionKey3"));
    }

    private sealed class ExistingDocumentIdProvider : IDocumentIdProvider
    {
        public ValueTask<(string DocumentId, string PartitionKey)> GetDocumentIdentifiers(string grainType, GrainId grainId) =>
            new(("document-id", "partition-key"));
    }

    private sealed class HierarchicalDocumentIdProvider(params string[] partitionKeyValues) : IDocumentIdProvider
    {
        public ValueTask<(string DocumentId, string PartitionKey)> GetDocumentIdentifiers(string grainType, GrainId grainId) =>
            new(("document-id", partitionKeyValues[0]));

        public ValueTask<CosmosDocumentKey> GetDocumentKey(string grainType, GrainId grainId) =>
            new(new CosmosDocumentKey("document-id", partitionKeyValues));
    }

    private sealed class ExplicitHierarchicalDocumentIdProvider : IDocumentIdProvider
    {
        public ValueTask<(string DocumentId, string PartitionKey)> GetDocumentIdentifiers(string grainType, GrainId grainId) =>
            new(("document-id", "one"));

        ValueTask<CosmosDocumentKey> IDocumentIdProvider.GetDocumentKey(string grainType, GrainId grainId) =>
            new(new CosmosDocumentKey("document-id", ["one", "two"]));
    }

    private sealed class TestState
    {
        public int Value { get; set; }
    }
}
