namespace IngestDispatcher.Functions.Services;

public static class TaskStatuses
{
    public const string Dispatched = "dispatched";
    public const string Succeeded = "succeeded";
    public const string Partial = "partial";
    public const string Skipped = "skipped";
    public const string Failed = "failed";
    public const string TimedOut = "timed_out";
    public const string DispatchFailed = "dispatch_failed";

    // Operator cancelled the run via the cancel endpoint, ACA confirmed
    // terminal. Data may be partial.
    public const string Cancelled = "cancelled";

    // Operator force-released a stalled cancellation without ACA terminal
    // confirmation. Downstream must treat data integrity as uncertain — the
    // replica may have written after the force-release.
    public const string CancellationFailed = "cancellation_failed";
}

// Container-side entity rollup vocabulary, carried on the run-state blob's
// per-entity Rollup.Status field (HeartbeatPayload.cs). Distinct from
// TaskStatuses (which is past tense) because the container's words are the
// ground truth we map from — keeping them separate avoids string-equality
// bugs if either side ever drifts. Pre-#385 these were ManifestStatuses,
// named for the now-removed _dispatcher/manifests/ summary blob.
public static class EntityRollupStatuses
{
    public const string Success = "success";
    public const string Partial = "partial";
    public const string Skipped = "skipped";
    public const string Failed = "failed";
}

// Run-level rollup vocabulary, written to the run JSONL history blob's
// status field and returned by /runs + /runs/{id}. Operator-intent values
// (Cancelled, TimedOut, CancellationFailed) dominate over success-mix
// values — see RunTracker.ComputeRunStatus for the precedence table.
public static class RunStatuses
{
    public const string Completed = "completed";
    public const string CompletedWithErrors = "completed_with_errors";
    public const string Failed = "failed";

    // Operator cancelled the run via the cancel endpoint, ACA confirmed
    // terminal. May coexist with some succeeded tasks if the cancel landed
    // mid-flight — the operator's intent overrides partial success.
    public const string Cancelled = "cancelled";

    // Run exceeded its resolved timeout; reconciler created a cancel intent
    // with trigger=timeout and the submitted stop drove ACA terminal. Same
    // dominance rule as Cancelled — applied uniformly even with some
    // succeeded tasks, since the deadline crossing is the load-bearing fact.
    public const string TimedOut = "timed_out";

    // Operator force-released a stalled cancellation without ACA terminal
    // confirmation (ForceReleaseRunFunction). Downstream must treat data
    // integrity as uncertain — the replica may have written after release.
    public const string CancellationFailed = "cancellation_failed";

    // Tenant's entity_selector intersected with the job's selector down to an
    // empty set, so nothing was dispatched. Written directly by RunExecutor
    // (no claim, no ACA executions, no finalization path). Distinct from
    // SkippedEmpty on DispatchResult — that's the run-wide "0 tenants OR 0
    // entities at job level" outcome, whereas SkippedFilter is per-tenant.
    public const string SkippedFilter = "skipped_filter";
}

public static class TenantSelectorModes
{
    public const string All = "all";
    public const string Specific = "specific";
    public const string AllExcept = "all_except";
}

public static class StorageAuthMethods
{
    public const string ManagedIdentity = "managed_identity";
    // SP modes are distinguished by the credential type: a cert pulled from
    // our KV (cert mode) or a secret value pulled from our KV (secret mode).
    // Both reference customer SP app registrations; the customer chooses
    // which credential type to bind based on their own constraints.
    public const string ServicePrincipalCert = "service_principal_cert";
    public const string ServicePrincipalSecret = "service_principal_secret";
}

public static class AcaExecutionStatuses
{
    // ACA REST API returns PascalCase; don't merge with TaskStatuses.
    // Full enum per JobExecutionRunningState: Running, Processing, Stopped,
    // Degraded, Failed, Unknown, Succeeded. Only terminal values are listed
    // here — Running/Processing/Unknown have no symbolic call sites and fall
    // through to the deferral branch in RunTracker.
    public const string Succeeded = "Succeeded";
    public const string Failed = "Failed";
    public const string Stopped = "Stopped";
    public const string Degraded = "Degraded";
}

public static class TriggerTypes
{
    public const string Scheduled = "scheduled";
    public const string Manual = "manual";
}

public static class RunTypes
{
    public const string Normal = "normal";
    public const string Backfill = "backfill";
}
