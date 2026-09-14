using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

public record RunStatusResponse(
    [property: JsonPropertyName("run_id")] string RunId,
    [property: JsonPropertyName("job_name")] string JobName,
    [property: JsonPropertyName("tenant_key")] string TenantKey,
    [property: JsonPropertyName("trigger_type")] string TriggerType,
    [property: JsonPropertyName("run_type")] string RunType,
    [property: JsonPropertyName("triggered_by")] string? TriggeredBy,
    [property: JsonPropertyName("started_at")] DateTimeOffset StartedAt,
    [property: JsonPropertyName("completed_at")] DateTimeOffset? CompletedAt,
    [property: JsonPropertyName("elapsed_seconds")] long ElapsedSeconds,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("resolved_entities")] IReadOnlyList<string> ResolvedEntities,
    [property: JsonPropertyName("tasks")] IReadOnlyList<TaskStatusResponse> Tasks
);

public record TaskStatusResponse(
    [property: JsonPropertyName("container_type")] string ContainerType,
    [property: JsonPropertyName("aca_execution_name")] string? AcaExecutionName,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("started_at")] DateTimeOffset? StartedAt,
    [property: JsonPropertyName("completed_at")] DateTimeOffset? CompletedAt,
    [property: JsonPropertyName("error_message")] string? ErrorMessage,
    [property: JsonPropertyName("entities")] IReadOnlyList<TaskEntityProjection> Entities,
    [property: JsonPropertyName("heartbeat")] HeartbeatBlock? Heartbeat,
    [property: JsonPropertyName("prerequisite_entities")] IReadOnlyList<TaskEntityProjection>? PrerequisiteEntities = null
);

// Flat entity projection — progress counters promoted from the former
// per-stage level. No stages[] — entity progress is the only granularity.
public record TaskEntityProjection(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("record_count")] int? RecordCount,
    [property: JsonPropertyName("input_count")] int? InputCount,
    [property: JsonPropertyName("items_processed")] int? ItemsProcessed,
    [property: JsonPropertyName("items_failed")] int? ItemsFailed,
    [property: JsonPropertyName("items_skipped")] int? ItemsSkipped,
    [property: JsonPropertyName("records_so_far")] int? RecordsSoFar,
    [property: JsonPropertyName("started_at")] DateTimeOffset? StartedAt,
    [property: JsonPropertyName("completed_at")] DateTimeOffset? CompletedAt,
    [property: JsonPropertyName("duration_ms")] long? DurationMs,
    [property: JsonPropertyName("errors")] IReadOnlyList<string> Errors
);

public record HeartbeatBlock(
    [property: JsonPropertyName("last_heartbeat_at")] DateTimeOffset LastHeartbeatAt,
    [property: JsonPropertyName("last_heartbeat_age_seconds")] long LastHeartbeatAgeSeconds,
    [property: JsonPropertyName("stale")] bool Stale
);

public static class TaskStatusValues
{
    public const string Pending = "pending";
    public const string Running = "running";
}
