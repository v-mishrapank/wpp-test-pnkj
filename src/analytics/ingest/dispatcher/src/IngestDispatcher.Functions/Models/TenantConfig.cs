using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

public record TenantConfig(
    [property: JsonPropertyName("tenant_key")] string TenantKey,
    [property: JsonPropertyName("tenant_id")] string TenantId,
    [property: JsonPropertyName("organization")] string Organization,
    [property: JsonPropertyName("admin_url")] string? AdminUrl,
    [property: JsonPropertyName("analytics_enabled")] bool AnalyticsEnabled,
    [property: JsonPropertyName("analytics_max_concurrency")] int AnalyticsMaxConcurrency,
    // Optional per-tenant entity filter. Intersected with the job's entity_selector
    // at dispatch (RunExecutor) so an entity is only ingested for a tenant when both
    // selectors agree. Null = no constraint (tenant gets whatever the job resolves).
    // Semantically distinct from job-level EntitySelector, where omitting fields
    // resolves to an empty set; here, omitting the whole property is pass-through.
    [property: JsonPropertyName("entity_selector")] EntitySelector? EntitySelector = null,
    // When true, scoped entity root stages (entra_users, exo_mailboxes, spo_sites)
    // read a UPN set from _scope/<tenant_key>/users/ on the landing container
    // and filter their work to that set. Dispatcher signals consumers via the
    // SCOPE_ROOT env var. See #260.
    [property: JsonPropertyName("scoped")] bool Scoped = false
);

public record TenantsConfig(
    [property: JsonPropertyName("tenants")] IReadOnlyList<TenantConfig> Tenants
);
