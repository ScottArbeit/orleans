# Microsoft Orleans Persistence for Azure Cosmos DB

## Introduction

Microsoft Orleans Persistence for Azure Cosmos DB provides grain persistence for Microsoft Orleans using Azure Cosmos DB. This allows your grains to persist their state in Azure Cosmos DB and reload it when they are reactivated, offering a globally distributed, multi-model database service for your Orleans applications.

## Getting Started

To use this package, install it via NuGet:

```shell
dotnet add package Microsoft.Orleans.Persistence.Cosmos
```

## Example - Configuring Azure Cosmos DB Persistence

```csharp
using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using Orleans.Hosting;

var builder = Host.CreateApplicationBuilder(args)
    .UseOrleans(siloBuilder =>
    {
        siloBuilder
            .UseLocalhostClustering()
            // Configure Azure Cosmos DB as grain storage
            .AddCosmosGrainStorage(
                name: "cosmosStore",
                configureOptions: options =>
                {
                    options.AccountEndpoint = "https://YOUR_COSMOS_ENDPOINT";
                    options.AccountKey = "YOUR_COSMOS_KEY";
                    options.DB = "YOUR_DATABASE_NAME";
                    options.CanCreateResources = true;
                });
    });

// Run the host
await builder.RunAsync();
```

## Hierarchical partition keys

Single-string partitioning remains the default and continues to use `PartitionKeyPath`, which defaults to `/PartitionKey`. Existing applications do not need code or configuration changes.

To opt into a two-level or three-level hierarchical partition key, set `PartitionKeyLevelCount` and register an `IDocumentIdProvider` that returns the same number of ordered values from `GetDocumentKey`. The provider must derive the document ID and every partition-key value from `grainType` and `GrainId`. Reads and deletes cannot depend on first loading the document.

HPK containers intentionally use these fixed top-level paths, in order:

1. `/PartitionKey`
1. `/PartitionKey2`
1. `/PartitionKey3`

Applications map their domain values to that order in `GetDocumentKey`. For example, an application can map tenant, record kind, and shard to the first, second, and third values. Single-key containers can still select a different `PartitionKeyPath`.

The following example configures three independent named providers in one silo. Each provider has its own container, key depth, and `IDocumentIdProvider` implementation.

```csharp
public abstract class ApplicationDocumentIdProvider(
    IOptions<ClusterOptions> clusterOptions) : IDocumentIdProvider
{
    private readonly DefaultDocumentIdProvider _defaultProvider = new(clusterOptions);

    public ValueTask<(string DocumentId, string PartitionKey)> GetDocumentIdentifiers(
        string grainType,
        GrainId grainId)
    {
        var values = GetPartitionKeyValues(grainType, grainId);
        return new((_defaultProvider.GetId(grainType, grainId), values[0]));
    }

    public ValueTask<CosmosDocumentKey> GetDocumentKey(string grainType, GrainId grainId)
    {
        return new(new CosmosDocumentKey(
            _defaultProvider.GetId(grainType, grainId),
            GetPartitionKeyValues(grainType, grainId)));
    }

    protected abstract string[] GetPartitionKeyValues(string grainType, GrainId grainId);
}

public sealed class OneKeyProvider(IOptions<ClusterOptions> options)
    : ApplicationDocumentIdProvider(options)
{
    // ApplicationKey.Decode is application code which reconstructs stable parts from GrainId.
    protected override string[] GetPartitionKeyValues(string grainType, GrainId grainId) =>
        [ApplicationKey.Decode(grainId).TenantId];
}

public sealed class TwoKeyProvider(IOptions<ClusterOptions> options)
    : ApplicationDocumentIdProvider(options)
{
    protected override string[] GetPartitionKeyValues(string grainType, GrainId grainId)
    {
        var key = ApplicationKey.Decode(grainId);
        return [key.TenantId, key.RecordKind];
    }
}

public sealed class ThreeKeyProvider(IOptions<ClusterOptions> options)
    : ApplicationDocumentIdProvider(options)
{
    protected override string[] GetPartitionKeyValues(string grainType, GrainId grainId)
    {
        var key = ApplicationKey.Decode(grainId);
        return [key.TenantId, key.RecordKind, key.ShardKey];
    }
}

siloBuilder.AddCosmosGrainStorage<OneKeyProvider>("control", options =>
{
    options.ConfigureCosmosClient(connectionString);
    options.ContainerName = "Control";
    options.PartitionKeyLevelCount = 1;
});
siloBuilder.AddCosmosGrainStorage<TwoKeyProvider>("current", options =>
{
    options.ConfigureCosmosClient(connectionString);
    options.ContainerName = "Current";
    options.PartitionKeyLevelCount = 2;
});
siloBuilder.AddCosmosGrainStorage<ThreeKeyProvider>("history", options =>
{
    options.ConfigureCosmosClient(connectionString);
    options.ContainerName = "History";
    options.PartitionKeyLevelCount = 3;
});
```

For a three-level provider, Orleans writes this envelope:

```json
{
  "id": "service__grain-type_grain-key",
  "GrainType": "application-grain-type",
  "State": {
    "Value": 7
  },
  "PartitionKey": "tenant-42",
  "PartitionKey2": "history",
  "PartitionKey3": "segment-3"
}
```

Cosmos DB adds system properties such as `_etag`. In one-key mode, Orleans omits `PartitionKey2` and `PartitionKey3`. The envelope property names are the same with Newtonsoft.Json and System.Text.Json. Grain state is always under `State`, so a read-only query can address `c.State.Value`. The shape inside `State` follows the application's state type and serializer settings.

Read-only application queries can use the first HPK value or the first two values as a routing prefix. Include matching equality filters in the SQL query and pass the same ordered prefix to `QueryRequestOptions`:

```csharp
var prefix = new PartitionKeyBuilder()
    .Add(tenantId)
    .Add(recordKind)
    .Build();

var query = new QueryDefinition(
        "SELECT * FROM c WHERE c.PartitionKey = @tenant AND c.PartitionKey2 = @kind")
    .WithParameter("@tenant", tenantId)
    .WithParameter("@kind", recordKind);

using var iterator = container.GetItemQueryIterator<JsonElement>(
    query,
    requestOptions: new QueryRequestOptions { PartitionKey = prefix });
```

During startup, Orleans verifies that the actual container has the configured path count, names, and order. The check runs even when resource creation is disabled. Cosmos DB cannot convert an existing single-key container to HPK in place. Create a new container and copy the data when migrating; Orleans does not perform that migration.

## Example - Using Grain Storage in a Grain

```csharp
// Define grain state class

public class MyGrainState
{
    public string Data { get; set; }
    public int Version { get; set; }
}

// Grain implementation that uses the Cosmos DB storage
public class MyGrain : Grain, IMyGrain, IGrainWithStringKey
{
    private readonly IPersistentState<MyGrainState> _state;

    public MyGrain([PersistentState("state", "cosmosStore")] IPersistentState<MyGrainState> state)
    {
        _state = state;
    }

    public async Task SetData(string data)
    {
        _state.State.Data = data;
        _state.State.Version++;
        await _state.WriteStateAsync();
    }

    public Task<string> GetData()
    {
        return Task.FromResult(_state.State.Data);
    }
}
```

## Documentation

For more comprehensive documentation, please refer to:

- [Microsoft Orleans Documentation](https://learn.microsoft.com/dotnet/orleans/)
- [Grain Persistence](https://learn.microsoft.com/en-us/dotnet/orleans/grains/grain-persistence)
- [Azure Storage Providers](https://learn.microsoft.com/en-us/dotnet/orleans/grains/grain-persistence/azure-storage)

## Feedback & Contributing

- If you have any issues or would like to provide feedback, please [open an issue on GitHub](https://github.com/dotnet/orleans/issues)
- Join our community on [Discord](https://aka.ms/orleans-discord)
- Follow the [@msftorleans](https://twitter.com/msftorleans) Twitter account for Orleans announcements
- Contributions are welcome! Please review our [contribution guidelines](https://github.com/dotnet/orleans/blob/main/CONTRIBUTING.md)
- This project is licensed under the [MIT license](https://github.com/dotnet/orleans/blob/main/LICENSE)
