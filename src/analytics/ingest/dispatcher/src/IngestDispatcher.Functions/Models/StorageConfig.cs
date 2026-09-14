using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

public record StorageConfig(
    [property: JsonPropertyName("account_url")] string AccountUrl,
    [property: JsonPropertyName("container")] string Container,
    [property: JsonPropertyName("auth")] StorageAuthConfig Auth,
    [property: JsonPropertyName("orphan_sweep_interval_minutes")] int OrphanSweepIntervalMinutes = 60,
    // 30d default matches RunsFunction.MaxWindow — blobs older than the
    // listing horizon are unreachable via the API and safe to age out.
    // Pre-#385 default was 24h when run-state was deleted at finalization
    // and the sweeper only had to reclaim leakage from that delete.
    [property: JsonPropertyName("orphan_age_threshold_hours")] int OrphanAgeThresholdHours = 720
);

public record StorageAuthConfig(
    [property: JsonPropertyName("method")] string Method,
    [property: JsonPropertyName("tenant_id")] string? TenantId = null,
    [property: JsonPropertyName("client_id")] string? ClientId = null,
    [property: JsonPropertyName("cert_name")] string? CertName = null,
    [property: JsonPropertyName("secret_name")] string? SecretName = null
);
