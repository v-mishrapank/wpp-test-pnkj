using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

public record EntityType(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("tier")] int Tier,
    [property: JsonPropertyName("container")] string Container
);

public record EntityRegistryConfig(
    [property: JsonPropertyName("entities")] IReadOnlyList<EntityType> Entities
);
