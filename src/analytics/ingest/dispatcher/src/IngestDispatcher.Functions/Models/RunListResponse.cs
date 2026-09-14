using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

// GET /runs response shape. Listing of in-flight + recently-completed runs
// in the requested window (default 24h, cap 30d).
public record RunListResponse(
    [property: JsonPropertyName("runs")] IReadOnlyList<RunListEntry> Runs
);

// Per-run summary. status is the only field that distinguishes in-flight
// (status == "running") from completed (everything else); completed_at is
// a second confirmation (null vs. populated).
public record RunListEntry(
    [property: JsonPropertyName("run_id")] string RunId,
    [property: JsonPropertyName("job_name")] string JobName,
    [property: JsonPropertyName("tenant_key")] string TenantKey,
    [property: JsonPropertyName("run_type")] string RunType,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("started_at")] DateTimeOffset StartedAt,
    [property: JsonPropertyName("completed_at")] DateTimeOffset? CompletedAt,
    [property: JsonPropertyName("elapsed_seconds")] long ElapsedSeconds,
    [property: JsonPropertyName("task_count")] int TaskCount,
    [property: JsonPropertyName("tasks_completed")] int TasksCompleted
);

// Top-level run status vocabulary for /runs listing:
// - In-flight: "running" (the only in-flight value).
// - Completed: completed | completed_with_errors | failed | cancelled |
//   timed_out | cancellation_failed. Five of six come from RunStatuses
//   constants via RunTracker.ComputeRunStatus; cancellation_failed is
//   written directly by ForceReleaseRunFunction. Operator-intent values
//   (cancelled/timed_out) dominate over success-mix rollup — see
//   ComputeRunStatus for the precedence table (#382).
public static class RunListStatusValues
{
    public const string Running = "running";
    // Completed values come from RunStatuses constants.
}
