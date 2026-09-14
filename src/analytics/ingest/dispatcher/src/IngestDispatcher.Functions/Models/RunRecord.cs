using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

// task_count is the number of (containerType, entity-group) tasks the run
// dispatched — distinct from entity_count (the count of distinct entities
// the run resolved). For a run that fans 5 entities across 2 container
// types, task_count=2 and entity_count=5. Persisted on the run history
// row so /runs listing can return it without re-reading the tasks JSONL
// per row. Pre-#314 history blobs decode with TaskCount=0 (we wiped
// branch + dev at cutover).
public record RunRecord(
    [property: JsonPropertyName("run_id")] string RunId,
    [property: JsonPropertyName("job_name")] string JobName,
    [property: JsonPropertyName("tenant_key")] string TenantKey,
    [property: JsonPropertyName("trigger_type")] string TriggerType,
    [property: JsonPropertyName("run_type")] string RunType,
    [property: JsonPropertyName("triggered_by")] string? TriggeredBy,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("started_at")] DateTimeOffset StartedAt,
    [property: JsonPropertyName("completed_at")] DateTimeOffset? CompletedAt,
    [property: JsonPropertyName("task_count")] int TaskCount,
    [property: JsonPropertyName("entity_count")] int EntityCount,
    [property: JsonPropertyName("resolved_entities")] IReadOnlyList<string> ResolvedEntities
);
