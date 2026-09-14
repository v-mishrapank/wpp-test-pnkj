using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

// Cancel-intent blob: a separate mutable record that drives the cancel state
// machine for an active claim. Kept out of ClaimRecord because the claim is
// immutable after dispatch (written exactly twice).
//
// Path: _dispatcher/cancellations/{run_id}.json
//
// Lifecycle (split between RunCanceller and RunTracker):
//   1. Trigger (timeout fire OR manual endpoint) creates with If-None-Match: *.
//      Initial state = cancel_requested.
//   2. Trigger inline-submits ACA stop for each task. On success → state
//      flips to cancel_submitted. On failure → stays cancel_requested
//      (RunCanceller.ReconcileAsync will re-submit on subsequent ticks).
//   3. ACA polling/terminal detection happens in RunTracker.CheckActiveRunsAsync
//      (which already polls GetExecutionStatusAsync on every active claim).
//      Once every task reports terminal, that method finalizes the run —
//      writes JSONL, deletes the claim, deletes this blob.
//   4. RunCanceller.ReconcileAsync (in the timer tick) drives the cancel
//      state machine independent of finalization: re-submits cancels for
//      cancel_requested intents, and flips cancel_submitted →
//      cancel_stalled after CancelStallThreshold reconciler passes.
//   5. Force-release endpoint can write JSONL with CancellationFailed and
//      delete both the claim and this blob, breaking out of cancel_stalled.
public record CancelIntent
{
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

    [JsonPropertyName("cancel_state")]
    public required string CancelState { get; init; }

    [JsonPropertyName("cancel_trigger")]
    public required string CancelTrigger { get; init; }

    [JsonPropertyName("cancel_reason")]
    public string? CancelReason { get; init; }

    [JsonPropertyName("cancel_requested_at")]
    public required DateTimeOffset CancelRequestedAt { get; init; }

    [JsonPropertyName("cancel_submitted_at")]
    public DateTimeOffset? CancelSubmittedAt { get; init; }

    // Reconciler-tick counter. Semantics depend on cancel_state:
    //   - cancel_requested: number of ARM stop submissions that have
    //     failed (caller-side or inline-submit + each reconciler retry).
    //   - cancel_submitted: number of reconciler passes since entering this
    //     state. Counts toward CancelStallThreshold; once reached, the
    //     intent flips to cancel_stalled.
    //   - cancel_stalled: no longer load-bearing; reconciler keeps nudging
    //     ACA but doesn't change state on its own.
    // Reset to 0 on the cancel_requested → cancel_submitted transition so
    // submit retries don't accidentally trip stall detection.
    [JsonPropertyName("cancel_attempt_count")]
    public int CancelAttemptCount { get; init; }
}

public static class CancelStates
{
    public const string CancelRequested = "cancel_requested";
    public const string CancelSubmitted = "cancel_submitted";
    public const string CancelStalled = "cancel_stalled";
}

public static class CancelTriggers
{
    public const string Timeout = "timeout";
    public const string Manual = "manual";
}
