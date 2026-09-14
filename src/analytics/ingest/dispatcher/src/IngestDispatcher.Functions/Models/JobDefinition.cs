using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

public record JobDefinition(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("description")] string? Description,
    [property: JsonPropertyName("cron")] string? Cron,
    [property: JsonPropertyName("enabled")] bool Enabled,
    [property: JsonPropertyName("entity_selector")] EntitySelector EntitySelector,
    [property: JsonPropertyName("tenant_selector")] TenantSelector TenantSelector,
    // Optional per-job timeout in seconds, tightens the dispatcher's default
    // (7d). Resolved against any ad-hoc override at dispatch time as
    // MIN(adhoc, this, default). Must be > 0 if set. Absent = use default.
    [property: JsonPropertyName("timeout_seconds")] int? TimeoutSeconds = null
);

public record EntitySelector(
    [property: JsonPropertyName("include_tiers")] IReadOnlyList<int>? IncludeTiers = null,
    [property: JsonPropertyName("include_entities")] IReadOnlyList<string>? IncludeEntities = null,
    [property: JsonPropertyName("exclude_entities")] IReadOnlyList<string>? ExcludeEntities = null
);

public record TenantSelector(
    [property: JsonPropertyName("mode")] string Mode,
    [property: JsonPropertyName("tenant_keys")] IReadOnlyList<string>? TenantKeys = null,
    [property: JsonPropertyName("exclude_keys")] IReadOnlyList<string>? ExcludeKeys = null
);

public record JobsConfig(
    [property: JsonPropertyName("jobs")] IReadOnlyList<JobDefinition> Jobs
);
