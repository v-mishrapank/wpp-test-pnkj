using IngestDispatcher.Functions.Models;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Services;

public interface IRunTracker
{
    // Finalization-only loop, fired by the timer every minute. Lists active
    // claims; for each, checks whether every expected_task is terminal
    // (run-state blob has terminal RunStatus, or ACA-fallback when the blob
    // never landed); if so, writes history + deletes claim. The run-state
    // blob is retained past finalization (#385) — `/runs/{id}` keeps reading
    // it. The pre-#314 polling-and-rewrite-tracking-blob loop is gone —
    // task state is derived at API time, not stored.
    Task CheckActiveRunsAsync(CancellationToken ct = default);
}

public class RunTracker : IRunTracker
{
    // Fallback only — used when a pre-#263 claim blob without a stamped
    // resolved_timeout_seconds is read. New claims carry their resolved value
    // (set by RunExecutor.ResolveTimeoutSeconds) and use it directly.
    private static readonly TimeSpan DefaultRunTimeout =
        TimeSpan.FromSeconds(RunExecutor.DefaultTimeoutSeconds);

    private readonly IClaimReader _claimReader;
    private readonly IClaimWriter _claimWriter;
    private readonly IAcaJobClient _aca;
    private readonly IRunHistoryWriter _historyWriter;
    private readonly IRunStateReader _runStateReader;
    private readonly IHeartbeatCache _runStateCache;
    private readonly IRunCanceller _canceller;
    private readonly ILogger<RunTracker> _logger;

    public RunTracker(
        IClaimReader claimReader,
        IClaimWriter claimWriter,
        IAcaJobClient aca,
        IRunHistoryWriter historyWriter,
        IRunStateReader runStateReader,
        IHeartbeatCache runStateCache,
        IRunCanceller canceller,
        ILogger<RunTracker> logger)
    {
        _claimReader = claimReader;
        _claimWriter = claimWriter;
        _aca = aca;
        _historyWriter = historyWriter;
        _runStateReader = runStateReader;
        _runStateCache = runStateCache;
        _canceller = canceller;
        _logger = logger;
    }

    // Precedence (#382): operator-intent values dominate over success-mix
    // rollup because the run didn't run to completion — the deadline (or
    // the operator's cancel) overrides any partial success.
    //   1. any task TimedOut → run TimedOut
    //   2. else any task Cancelled → run Cancelled
    //   3. else success-mix rollup (#509):
    //        "landed" = Succeeded | Partial | Skipped (data written or
    //          legitimately nothing to do). "clean" = Succeeded | Skipped.
    //        all tasks clean → Completed
    //        any task landed but not all clean → CompletedWithErrors
    //        no tasks landed → Failed — covers all-failed,
    //          all-dispatch_failed, and similar zero-data outcomes.
    //          CancellationFailed reaches this branch only via the explicit
    //          force-release surface, which writes
    //          RunStatuses.CancellationFailed directly without going
    //          through ComputeRunStatus.
    internal static string ComputeRunStatus(IEnumerable<ResolvedTaskOutcome> tasks)
    {
        var materialized = tasks as IReadOnlyCollection<ResolvedTaskOutcome> ?? tasks.ToList();
        if (materialized.Any(t => t.Status == TaskStatuses.TimedOut))
            return RunStatuses.TimedOut;
        if (materialized.Any(t => t.Status == TaskStatuses.Cancelled))
            return RunStatuses.Cancelled;
        var anyLanded = materialized.Any(t =>
            t.Status == TaskStatuses.Succeeded ||
            t.Status == TaskStatuses.Partial ||
            t.Status == TaskStatuses.Skipped);
        var allClean = materialized.All(t =>
            t.Status == TaskStatuses.Succeeded ||
            t.Status == TaskStatuses.Skipped);
        return !anyLanded ? RunStatuses.Failed
             : !allClean ? RunStatuses.CompletedWithErrors
             : RunStatuses.Completed;
    }

    // Map a terminal run-state blob to a TaskStatuses value, plus a
    // human-readable error_message naming offending entities. The blob's
    // top-level run_status is just terminal-or-not — for the four-way
    // task status (succeeded/partial/skipped/failed) we compute severity-
    // max across the per-entity rollups, the same vocabulary the deleted
    // manifest summary used to carry. Unknown entity statuses are treated
    // as failed so a future container status we haven't taught about
    // doesn't silently round-trip as success.
    //
    // Pre-#385 this was MapManifestSummary, fed by the now-removed
    // _dispatcher/manifests/ blob. Same shape, different source.
    internal static (string TaskStatus, string? ErrorMessage) MapRunStateBlob(HeartbeatPayload blob)
    {
        // Container-level early failure (#479): run_error captures the
        // exception message from before entity work started. Surface it
        // directly — entity rollups will all be 'pending' in this case.
        if (!string.IsNullOrEmpty(blob.RunError))
        {
            return (TaskStatuses.Failed, $"Container error: {blob.RunError}");
        }

        var entities = blob.Entities ?? [];
        var rolledUp = SeverityMax(entities.Select(e => e.Status));
        var mappedStatus = MapEntityStatus(rolledUp);

        if (mappedStatus == TaskStatuses.Succeeded)
            return (mappedStatus, null);

        // Include actual error text from entity errors[] (#482), not just
        // entity names and statuses.
        var nonSuccess = entities
            .Where(e => e.Status != EntityRollupStatuses.Success)
            .ToList();

        var detail = nonSuccess.Count > 0
            ? string.Join("; ", nonSuccess.Select(e =>
            {
                var errors = (e.Errors ?? []).Take(2).ToList();
                var errText = errors.Count > 0 ? ": " + string.Join(", ", errors) : "";
                return $"{e.Name}({e.Status}){errText}";
            }))
            : "no entity-level details";
        return (mappedStatus, $"Run state reports {rolledUp}: {detail}");
    }

    private static string MapEntityStatus(string entityStatus) => entityStatus switch
    {
        EntityRollupStatuses.Success => TaskStatuses.Succeeded,
        EntityRollupStatuses.Partial => TaskStatuses.Partial,
        EntityRollupStatuses.Skipped => TaskStatuses.Skipped,
        EntityRollupStatuses.Failed => TaskStatuses.Failed,
        _ => TaskStatuses.Failed,
    };

    // Severity ranking matches Invoke-Ingestion.ps1's $statusRank:
    // success(0) < skipped(1) < partial(2) < failed(3). Empty input
    // returns 'failed' so a blob with no entities — pathological — surfaces
    // as a failure rather than a silent success.
    private static string SeverityMax(IEnumerable<string> statuses)
    {
        int Rank(string s) => s switch
        {
            EntityRollupStatuses.Success => 0,
            EntityRollupStatuses.Skipped => 1,
            EntityRollupStatuses.Partial => 2,
            EntityRollupStatuses.Failed => 3,
            _ => 3,
        };
        var max = -1;
        var pick = EntityRollupStatuses.Failed;
        foreach (var s in statuses)
        {
            var r = Rank(s);
            if (r > max) { max = r; pick = s; }
        }
        return pick;
    }

    // Map a terminal ACA execution status to a TaskStatuses value + error
    // message. Returns null for non-terminal (Running/Processing/Unknown) so
    // the caller's deferral branch is preserved. Stopped and Degraded both
    // surface as Failed — without a run-state blob there's no entity-level
    // signal to justify Partial — but the error_message distinguishes
    // operator cancellation and degradation from organic failure for
    // debuggability.
    internal static (string TaskStatus, string? ErrorMessage)? MapAcaTerminalStatus(
        string acaStatus, string executionName) => acaStatus switch
        {
            AcaExecutionStatuses.Succeeded => (TaskStatuses.Succeeded, null),
            AcaExecutionStatuses.Failed => (TaskStatuses.Failed,
                $"ACA Job execution {executionName} failed (no run-state blob)"),
            AcaExecutionStatuses.Stopped => (TaskStatuses.Failed,
                $"ACA Job execution {executionName} was stopped (no run-state blob)"),
            AcaExecutionStatuses.Degraded => (TaskStatuses.Failed,
                $"ACA Job execution {executionName} is degraded (no run-state blob)"),
            _ => null,
        };

    // Per-task outcome resolved by the finalization step (manifest +
    // ACA-fallback). Used to construct TaskRecord history rows + compute
    // the run-level status.
    internal record ResolvedTaskOutcome(
        string ContainerType,
        string? AcaExecutionName,
        IReadOnlyList<string> Entities,
        string Status,
        DateTimeOffset? CompletedAt,
        string? ErrorMessage);

    public async Task CheckActiveRunsAsync(CancellationToken ct = default)
    {
        var claims = await _claimReader.ListActiveAsync();
        if (claims.Count == 0) return;

        _logger.LogInformation("Checking {Count} active run(s)", claims.Count);

        foreach (var claim in claims)
        {
            // Safe point: per-claim boundary. Observe cancellation before we
            // start finalizing the next claim, never mid-finalization — a claim
            // that has begun writing history + deleting its claim runs to
            // completion so cancellation can't leave a half-finalized run the
            // reconciler never observes.
            ct.ThrowIfCancellationRequested();
            try
            {
                await CheckClaimAsync(claim, ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error checking claim for run {RunId}", claim.RunId);
            }
        }
    }

    private async Task CheckClaimAsync(ClaimRecord claim, CancellationToken ct)
    {
        // Safe point only: cancellation is observed at entry (the per-claim
        // boundary). Everything below — resolution reads and, critically, the
        // history write + claim delete — runs without the token so finalization
        // is never torn in half.
        ct.ThrowIfCancellationRequested();

        var nowUtc = DateTimeOffset.UtcNow;
        var runTimeout = claim.ResolvedTimeoutSeconds is { } s && s > 0
            ? TimeSpan.FromSeconds(s)
            : DefaultRunTimeout;
        var timedOut = (nowUtc - claim.StartedAt) > runTimeout;

        // A cancel intent is present when either:
        //   - the operator called the cancel endpoint, or
        //   - the reconciler detected a deadline crossing and created one
        //     with trigger=timeout.
        // We use the trigger to map ACA-stopped terminal statuses (the
        // consequence of our own CancelExecutionAsync calls) onto the
        // right TaskStatuses value at finalization time.
        var cancelIntent = await _canceller.TryReadIntentAsync(claim.RunId);

        // Resolve every expected task's terminal outcome:
        //   1. dispatch_failed already captured in the claim — synthesize
        //      a TaskRecord directly without ACA polling.
        //   2. Run-state blob present with terminal RunStatus →
        //      MapRunStateBlob, completed_at from blob's last_heartbeat_at.
        //   3. Blob running or absent + ACA terminal → finalize via ACA
        //      fallback (container exited before terminal blob landed).
        //   4. Blob running or absent + ACA running → defer (genuinely
        //      in-flight).
        //   5. Run timed out, task otherwise unresolved → mark Timeout.
        //   6. Else: still in flight, defer finalization to a later tick.
        var resolved = new List<ResolvedTaskOutcome>();
        var anyUnresolved = false;

        foreach (var task in claim.ExpectedTasks)
        {
            if (task.AcaExecutionName == null && task.DispatchError != null)
            {
                // Captured at dispatch — instantaneous failure.
                resolved.Add(new ResolvedTaskOutcome(
                    ContainerType: task.ContainerType,
                    AcaExecutionName: null,
                    Entities: task.Entities,
                    Status: TaskStatuses.DispatchFailed,
                    CompletedAt: claim.StartedAt,
                    ErrorMessage: task.DispatchError));
                continue;
            }

            // Run-state blob is authoritative when present and terminal.
            var blob = await _runStateReader.TryReadAsync(
                claim.RunId, claim.TenantKey, task.ContainerType);
            if (blob != null && !string.Equals(blob.RunStatus, BlobRunStatuses.Running, StringComparison.Ordinal))
            {
                var (status, error) = MapRunStateBlob(blob);
                resolved.Add(new ResolvedTaskOutcome(
                    ContainerType: task.ContainerType,
                    AcaExecutionName: task.AcaExecutionName,
                    Entities: task.Entities,
                    Status: status,
                    CompletedAt: blob.LastHeartbeatAt,
                    ErrorMessage: error));
                continue;
            }

            // Blob is running or absent. Check ACA immediately — if ACA
            // reports terminal, the container exited before Stop-Heartbeat
            // could flush. This replaces the former 10-min staleness gate
            // (#457) which delayed finalization on fast cancels and crashes.
            if (task.AcaExecutionName != null)
            {
                try
                {
                    var status = await _aca.GetExecutionStatusAsync(
                        task.ContainerType, task.AcaExecutionName);
                    var mapped = MapAcaTerminalStatus(status, task.AcaExecutionName);
                    if (mapped.HasValue)
                    {
                        // Container exited but didn't write a manifest —
                        // crash before manifest, operator stop, degraded
                        // exit, or older image. Use ACA's terminal signal,
                        // unless a cancel intent is present and the
                        // execution is `Stopped`: that's our own cancel
                        // landing, so map onto Cancelled/Timeout per
                        // trigger instead of the generic Failed mapping.
                        var (taskStatus, errorMessage) = mapped.Value;
                        if (cancelIntent != null && status == AcaExecutionStatuses.Stopped)
                        {
                            taskStatus = cancelIntent.CancelTrigger == CancelTriggers.Timeout
                                ? TaskStatuses.TimedOut
                                : TaskStatuses.Cancelled;
                            errorMessage = cancelIntent.CancelTrigger == CancelTriggers.Timeout
                                ? "Run timed out and execution was stopped by dispatcher"
                                : $"Run cancelled by operator{(cancelIntent.CancelReason is null ? string.Empty : ": " + cancelIntent.CancelReason)}";
                        }
                        resolved.Add(new ResolvedTaskOutcome(
                            ContainerType: task.ContainerType,
                            AcaExecutionName: task.AcaExecutionName,
                            Entities: task.Entities,
                            Status: taskStatus,
                            CompletedAt: nowUtc,
                            ErrorMessage: errorMessage));
                        continue;
                    }

                    // Non-terminal: still running. We MUST defer here even
                    // when past the wall-clock deadline — finalizing while
                    // ACA reports non-terminal would re-introduce the
                    // split-brain #263 set out to fix (claim deleted, slot
                    // released, replica still writing). The cancel state
                    // machine (RunCanceller.ReconcileAsync Pass A) creates
                    // a cancel intent when the deadline passes; the
                    // intent's submitted ACA stop drives the execution to
                    // Stopped, and the cancel-intent override branch above
                    // then resolves the task cleanly with Timeout or
                    // Cancelled on a subsequent tick.
                    if (timedOut && cancelIntent == null)
                    {
                        // Reconciler Pass A runs before CheckActiveRunsAsync
                        // in the same tick, so this is rare — log so we
                        // notice if it persists across ticks.
                        _logger.LogWarning(
                            "Run {RunId} task {Container} past deadline but no cancel intent yet; reconciler should create on next tick",
                            claim.RunId, task.ContainerType);
                    }

                    anyUnresolved = true;
                }
                catch (AcaExecutionNotFoundException ex)
                {
                    // ACA execution gone (job redeployed mid-run, retention
                    // expiry, never-actually-started). Mark Failed so the
                    // claim finalizes now instead of churning until timeout.
                    resolved.Add(new ResolvedTaskOutcome(
                        ContainerType: task.ContainerType,
                        AcaExecutionName: task.AcaExecutionName,
                        Entities: task.Entities,
                        Status: TaskStatuses.Failed,
                        CompletedAt: nowUtc,
                        ErrorMessage: ex.Message));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex,
                        "Error checking {Container}/{Tenant} for run {RunId}",
                        task.ContainerType, claim.TenantKey, claim.RunId);
                    anyUnresolved = true;
                }
            }
            else if (timedOut)
            {
                // No execution name AND no dispatch error — pre-#314 claims
                // could end up here in pathological cases; treat as timeout
                // (or cancelled if an operator manual cancel is the trigger)
                // so we don't loop forever.
                var fallbackStatus = cancelIntent?.CancelTrigger == CancelTriggers.Manual
                    ? TaskStatuses.Cancelled
                    : TaskStatuses.TimedOut;
                resolved.Add(new ResolvedTaskOutcome(
                    ContainerType: task.ContainerType,
                    AcaExecutionName: null,
                    Entities: task.Entities,
                    Status: fallbackStatus,
                    CompletedAt: nowUtc,
                    ErrorMessage: fallbackStatus == TaskStatuses.Cancelled
                        ? "Run cancelled by operator"
                        : "Run timed out"));
            }
            else
            {
                anyUnresolved = true;
            }
        }

        if (anyUnresolved)
            return;

        // Every task resolved — finalize.
        var finalStatus = ComputeRunStatus(resolved);

        var runRecord = new RunRecord(
            claim.RunId, claim.JobName, claim.TenantKey, claim.TriggerType, claim.RunType,
            claim.TriggeredBy, finalStatus,
            claim.StartedAt, nowUtc,
            claim.ExpectedTasks.Count, claim.ResolvedEntities.Count, claim.ResolvedEntities);

        var runResult = await _historyWriter.WriteRunAsync(runRecord);
        if (runResult == WriteResult.Written)
        {
            _logger.LogInformation("Run {RunId} for job {Job} tenant {Tenant} finished: {Status}",
                claim.RunId, claim.JobName, claim.TenantKey, finalStatus);
        }

        var taskRecords = resolved.Select(t => new TaskRecord(
            claim.RunId, claim.JobName, claim.TenantKey, claim.RunType, t.ContainerType,
            t.AcaExecutionName, t.Entities, t.Status,
            claim.StartedAt, t.CompletedAt, t.ErrorMessage)).ToList();

        var taskResult = await _historyWriter.WriteTasksAsync(claim.RunId, taskRecords);

        if (runResult == WriteResult.Failed || taskResult == WriteResult.Failed)
        {
            // Retain the claim; next tick re-enters this branch and the
            // Written/AlreadyExists side is idempotent.
            _logger.LogWarning(
                "History write failed for run {RunId}; retaining claim for retry",
                claim.RunId);
            return;
        }

        // Cleanup: claim first, then cancel intent. The run-state blob is
        // retained past finalization (#385) and aged out by the
        // RunStateReader sweeper at the history horizon. The cancel intent
        // goes last because the reconciler self-heals on next tick if the
        // claim is already gone (it drops orphaned intents).
        await _claimWriter.DeleteAsync(claim.JobName, claim.TenantKey, claim.RunType);
        if (cancelIntent != null)
        {
            await _canceller.DeleteIntentAsync(claim.RunId);
        }

        // Evict the in-process run-state cache. The cache exists to paper
        // over transient prefix-list misses for in-flight runs; once a run
        // finalizes, the durable blob is the source of truth and the cached
        // entry is dead weight. Pre-#385 this was bundled into
        // ProgressReader.DeleteForRunAsync; under the durable-blob model
        // the equivalent call lives here.
        _runStateCache.EvictRun(claim.RunId);
    }
}
