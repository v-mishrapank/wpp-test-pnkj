using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

// Container-written run-state projection. Path:
// _dispatcher/run_state/{runId}/{tenantKey}/{containerType}.json
//
// Schema v4: entities are flat (no rollup/stages nesting). Each entity
// carries its own progress counters alongside the rollup fields.
// run_error captures container-level early-failure errors.
// prerequisite_entities replaces the former prerequisite_stages.
public record HeartbeatPayload(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("run_id")] string RunId,
    [property: JsonPropertyName("tenant_key")] string TenantKey,
    [property: JsonPropertyName("container_type")] string ContainerType,
    [property: JsonPropertyName("container_started_at")] DateTimeOffset ContainerStartedAt,
    [property: JsonPropertyName("last_heartbeat_at")] DateTimeOffset LastHeartbeatAt,
    [property: JsonPropertyName("run_status")] string RunStatus,
    [property: JsonPropertyName("entities")] IReadOnlyList<HeartbeatEntity> Entities,
    [property: JsonPropertyName("run_error")] string? RunError = null,
    [property: JsonPropertyName("prerequisite_entities")] IReadOnlyList<HeartbeatEntity>? PrerequisiteEntities = null
);

// Flat entity shape — carries both rollup fields (name, status,
// record_count, timestamps, errors) and progress counters (input_count,
// items_*, records_so_far, duration_ms) at the same level.
public record HeartbeatEntity(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("record_count")] int? RecordCount = null,
    [property: JsonPropertyName("input_count")] int? InputCount = null,
    [property: JsonPropertyName("items_processed")] int? ItemsProcessed = null,
    [property: JsonPropertyName("items_failed")] int? ItemsFailed = null,
    [property: JsonPropertyName("items_skipped")] int? ItemsSkipped = null,
    [property: JsonPropertyName("records_so_far")] int? RecordsSoFar = null,
    [property: JsonPropertyName("started_at")] DateTimeOffset? StartedAt = null,
    [property: JsonPropertyName("completed_at")] DateTimeOffset? CompletedAt = null,
    [property: JsonPropertyName("duration_ms")] long? DurationMs = null,
    [property: JsonPropertyName("errors")] IReadOnlyList<string>? Errors = null
);

