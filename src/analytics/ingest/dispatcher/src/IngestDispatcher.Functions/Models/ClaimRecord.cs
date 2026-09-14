using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

// Replaces TrackedRun in the new derivation-based model. Written exactly
// twice during dispatch:
//   1. Atomic If-None-Match: * upload (ClaimWriter.TryCreateAsync) — overlap
//      protection. Same 409 BlobAlreadyExists shape RunTracker.TryClaimAsync
//      previously used.
//   2. Overwrite (ClaimWriter.UpdateExecutionNamesAsync) once StartJobAsync
//      has populated aca_execution_name on every expected task.
// Immutable thereafter until finalization deletes it.
//
// Per-task state (status, completed_at, error_message, next_check_at) is
// gone — derived at API time from claim + heartbeat + manifest + ACA fallback.
//
// Path: _dispatcher/tracking/{job_name}/{tenant_key}/{run_type}.json — kept
// the same name during cutover so existing ADLS Gen2 placeholder filtering
// continues to work; rename to _dispatcher/claims/ is a future cleanup.
public record ClaimRecord
{
    // Transport metadata: the blob's ETag at the time this record was last
    // read or written. Populated by ClaimReader (read path) and ClaimWriter's
    // TryCreateResult (create path); consumed by UpdateExecutionNamesAsync
    // for If-Match concurrency control on the second write. JsonIgnore: the
    // ETag is the blob's, not part of the payload — round-tripping it would
    // serialize a stale value into the next blob version.
    [JsonIgnore]
    public string? ETag { get; init; }

    [JsonPropertyName("schema_version")]
    public int SchemaVersion { get; init; } = 1;

    [JsonPropertyName("run_id")]
    public required string RunId { get; init; }

    [JsonPropertyName("job_name")]
    public required string JobName { get; init; }

    [JsonPropertyName("tenant_key")]
    public required string TenantKey { get; init; }

    [JsonPropertyName("run_type")]
    public required string RunType { get; init; }

    [JsonPropertyName("trigger_type")]
    public required string TriggerType { get; init; }

    [JsonPropertyName("triggered_by")]
    public string? TriggeredBy { get; init; }

    [JsonPropertyName("started_at")]
    public required DateTimeOffset StartedAt { get; init; }

    [JsonPropertyName("resolved_entities")]
    public required IReadOnlyList<string> ResolvedEntities { get; init; }

    [JsonPropertyName("expected_tasks")]
    public required IReadOnlyList<ExpectedTask> ExpectedTasks { get; init; }

    // Wall-clock deadline for this run, set at dispatch. Beyond this, the
    // timeout reconciler creates a cancel-intent (trigger=timeout) and the
    // dispatcher actively stops the run's ACA executions. Resolved as
    // MIN(adhoc_override, jobs_json_override, dispatcher_default_7d).
    // Nullable for backwards-compatible reads of pre-#263 claim blobs;
    // the reconciler treats null as "use the dispatcher default."
    [JsonPropertyName("resolved_timeout_seconds")]
    public int? ResolvedTimeoutSeconds { get; init; }
}

// One row per (containerType, entities) dispatch unit. ContainerType is the
// ACA Container App Job name (e.g. caj-ma-toolkit-branch-graph-001). The
// legacy TrackedTask carried both ContainerJobName and ContainerType
// separately, but for ingest jobs in this codebase they're always equal —
// collapsed here.
//
// AcaExecutionName is null on the first claim write (before StartJobAsync)
// AND on tasks where dispatch failed before StartJobAsync returned. The two
// are distinguished at finalization: the second claim write only populates
// names for tasks that actually started; tasks still null after that are
// rendered as task.status = dispatch_failed by TaskStateDeriver.
public record ExpectedTask
{
    [JsonPropertyName("container_type")]
    public required string ContainerType { get; init; }

    [JsonPropertyName("aca_execution_name")]
    public string? AcaExecutionName { get; set; }

    [JsonPropertyName("entities")]
    public required IReadOnlyList<string> Entities { get; init; }

    // Populated when dispatch failed before StartJobAsync returned. Null on
    // the success path. TaskStateDeriver renders this as task.error_message
    // when task.status is dispatch_failed.
    [JsonPropertyName("dispatch_error")]
    public string? DispatchError { get; set; }
}
