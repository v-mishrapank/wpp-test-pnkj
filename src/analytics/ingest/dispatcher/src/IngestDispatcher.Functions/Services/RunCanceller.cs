using IngestDispatcher.Functions.Models;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Services;

public enum RequestCancelOutcome
{
    // Cancel intent persisted. The ARM stop submission was attempted in
    // parallel across the run's in-flight executions; per-task outcomes
    // are on StopAttempts. The intent state on the result reflects whether
    // every stop landed (cancel_submitted) or any are still pending
    // (cancel_requested — reconciler will retry). Caller should respond 202
    // either way; the operator's intent (record cancel + best-effort ARM)
    // was honored.
    Submitted,

    // Cancel intent was already present for this run at cancel_submitted
    // or cancel_stalled when we arrived; returned the existing intent's
    // state. No new ARM calls were issued. Idempotent — repeat calls to
    // the cancel endpoint after the first land here. Treat as 202.
    AlreadyCancelling,

    // Cancel intent could not be persisted (blob threw or returned an
    // inconsistent already-exists / null-read state). Distinct from
    // partial ARM stops, which still return Submitted because the intent
    // was recorded. Caller should respond 502 — we couldn't even record
    // the operator's intent, so the reconciler has nothing to pick up.
    IntentPersistFailed,

    // No active claim found for the supplied run_id. Caller should respond
    // 404.
    NotFound,

    // The run is already in a terminal state (claim is gone). Caller
    // should respond 409.
    AlreadyTerminal,
}

public enum StopAttemptOutcome
{
    // ARM returned 2xx (stop accepted) or 404 (execution already gone).
    // Both mean the same thing from the dispatcher's POV — the execution
    // will not produce more work.
    Accepted,

    // ARM returned 5xx/408/429 or the call threw. The reconciler will
    // retry on its next tick.
    Transient,
}

public record StopAttemptResult(
    string ContainerType,
    string ExecutionName,
    StopAttemptOutcome Outcome,
    string? Error);

public record RequestCancelResult(
    RequestCancelOutcome Outcome,
    CancelIntent? Intent,
    IReadOnlyList<StopAttemptResult>? StopAttempts = null);

public interface IRunCanceller
{
    // Trigger entry point. Used by the timeout reconciler and the manual
    // cancel HTTP endpoint. Idempotent on repeat invocations.
    Task<RequestCancelResult> RequestCancelAsync(
        string runId, string trigger, string? reason, CancellationToken ct = default);

    // Reconciliation pass. Called from the timer tick.
    // - For each cancel_requested intent, re-attempt ACA stop submission.
    // - For each cancel_submitted intent, count an attempt; once
    //   CancelStallThreshold attempts have passed without finalization
    //   (i.e., the claim is still here), flip to cancel_stalled.
    // - Detects timeout-deadline crossings on active claims that don't yet
    //   have a cancel intent and creates one with trigger=timeout.
    Task ReconcileAsync(CancellationToken ct = default);

    // Used by RunTracker.CheckClaimAsync to override task statuses at
    // finalization time when a cancel intent is present.
    Task<CancelIntent?> TryReadIntentAsync(string runId);

    // Called by RunTracker after a finalization writes JSONL + deletes
    // the claim. Removes the cancel-intent blob.
    Task DeleteIntentAsync(string runId);
}

public class RunCanceller : IRunCanceller
{
    // After this many reconciler passes in cancel_submitted state without
    // finalization, the intent flips to cancel_stalled. At a 2-minute
    // reconcile interval that's ~10 minutes from submission to stalled —
    // long enough to absorb ARM slowness without being invisible.
    private const int CancelStallThreshold = 5;

    private readonly IClaimReader _claimReader;
    private readonly ICancelIntentStore _intentStore;
    private readonly IAcaJobClient _aca;
    private readonly ILogger<RunCanceller> _logger;

    public RunCanceller(
        IClaimReader claimReader,
        ICancelIntentStore intentStore,
        IAcaJobClient aca,
        ILogger<RunCanceller> logger)
    {
        _claimReader = claimReader;
        _intentStore = intentStore;
        _aca = aca;
        _logger = logger;
    }

    public async Task<CancelIntent?> TryReadIntentAsync(string runId) =>
        await _intentStore.TryReadAsync(runId);

    public async Task DeleteIntentAsync(string runId) =>
        await _intentStore.DeleteAsync(runId);

    public async Task<RequestCancelResult> RequestCancelAsync(
        string runId, string trigger, string? reason, CancellationToken ct = default)
    {
        // Safe point: observe cancellation before any claim lookup or intent
        // write. Once we begin recording intent + fanning out ARM stops we run
        // to completion — a half-recorded cancel is worse than none.
        ct.ThrowIfCancellationRequested();

        var claim = await _claimReader.TryFindByRunIdAsync(runId);
        if (claim == null)
        {
            // No active claim. Either the run never existed or it's already
            // finalized — distinguishable only by the history reader, but the
            // operator's intent here is unambiguously "cancel an active run."
            // Return NotFound; the cancel endpoint decides 404 vs 409 based
            // on history.
            return new RequestCancelResult(RequestCancelOutcome.NotFound, null);
        }

        var nowUtc = DateTimeOffset.UtcNow;
        var newIntent = new CancelIntent
        {
            RunId = runId,
            JobName = claim.JobName,
            TenantKey = claim.TenantKey,
            RunType = claim.RunType,
            CancelState = CancelStates.CancelRequested,
            CancelTrigger = trigger,
            CancelReason = reason,
            CancelRequestedAt = nowUtc,
        };

        var createResult = await _intentStore.TryCreateAsync(newIntent);
        CancelIntent intent;
        if (createResult.Outcome == CancelIntentCreateOutcome.AlreadyExists)
        {
            // Idempotent. If already at cancel_submitted, return that state.
            // If at cancel_requested (a prior trigger's submission failed),
            // fall through and re-attempt the submission — same shape as a
            // first attempt.
            if (createResult.Intent == null)
            {
                _logger.LogWarning(
                    "Cancel intent blob said already-exists for run {RunId} but read returned null; treating as transient",
                    runId);
                return new RequestCancelResult(RequestCancelOutcome.IntentPersistFailed, null);
            }
            if (createResult.Intent.CancelState != CancelStates.CancelRequested)
            {
                return new RequestCancelResult(
                    RequestCancelOutcome.AlreadyCancelling, createResult.Intent);
            }
            intent = createResult.Intent;
        }
        else
        {
            intent = createResult.Intent!;
        }

        // Inline submit pass. Single parallel fanout across the run's
        // in-flight executions — ARM /stop can take >10s under load (#409)
        // and serializing them across multiple tasks blew through the
        // request budget. Each ExpectedTask carries its own AcaExecutionName
        // (null when dispatch_failed — skip those, there's nothing in ACA
        // to stop). Partial-success is fine here: the intent is recorded
        // and the reconciler will pick up any stops that didn't land.
        var stopAttempts = await SubmitStopsParallelAsync(claim);
        var allAccepted = stopAttempts.All(s => s.Outcome == StopAttemptOutcome.Accepted);
        if (!allAccepted)
        {
            var pending = stopAttempts.Count(s => s.Outcome == StopAttemptOutcome.Transient);
            _logger.LogInformation(
                "Inline cancel submission for run {RunId}: {Pending}/{Total} stops did not land; reconciler will retry",
                runId, pending, stopAttempts.Count);
            // Intent stays at cancel_requested. Bump CancelAttemptCount so
            // the documented "inline-submit + reconciler retry" counter
            // contract (CancelIntent.cs) is honored — without this the
            // operator-visible attempt_count stays at 0 across repeated
            // inline failures. ETag-safe; ignore if the reconciler raced
            // us, the next reconciler tick will re-bump.
            var bumped = intent with { CancelAttemptCount = intent.CancelAttemptCount + 1 };
            if (await _intentStore.TryUpdateAsync(bumped))
            {
                intent = bumped;
            }
            return new RequestCancelResult(RequestCancelOutcome.Submitted, intent, stopAttempts);
        }

        // Advance state to cancel_submitted. Use ETag-protected update so a
        // racing reconciler doesn't clobber our submitted_at stamp. Reset
        // CancelAttemptCount to 0 — the counter's meaning flips here from
        // "submit-retry count" (while requested) to "stall-detection count"
        // (while submitted), and carrying the submit retries forward would
        // trip stall detection almost immediately.
        var advanced = intent with
        {
            CancelState = CancelStates.CancelSubmitted,
            CancelSubmittedAt = DateTimeOffset.UtcNow,
            CancelAttemptCount = 0,
        };
        var updated = await _intentStore.TryUpdateAsync(advanced);
        if (!updated)
        {
            // Reload current state and return — it almost certainly says
            // cancel_submitted (the reconciler raced us). Either way ACA
            // got the stop, which is the load-bearing claim.
            var fresh = await _intentStore.TryReadAsync(runId);
            return new RequestCancelResult(
                RequestCancelOutcome.AlreadyCancelling, fresh, stopAttempts);
        }

        return new RequestCancelResult(RequestCancelOutcome.Submitted, advanced, stopAttempts);
    }

    public async Task ReconcileAsync(CancellationToken ct = default)
    {
        // Safe point: honor a pre-cancelled token before listing claims so a
        // drained host issues no ARM stops.
        ct.ThrowIfCancellationRequested();

        // Pass A: detect deadline crossings on active claims that don't yet
        // have a cancel intent. Create one with trigger=timeout.
        var claims = await _claimReader.ListActiveAsync();
        var nowUtc = DateTimeOffset.UtcNow;

        foreach (var claim in claims)
        {
            // Safe point: per-claim boundary.
            ct.ThrowIfCancellationRequested();

            var runTimeout = claim.ResolvedTimeoutSeconds is { } s && s > 0
                ? TimeSpan.FromSeconds(s)
                : TimeSpan.FromSeconds(RunExecutor.DefaultTimeoutSeconds);
            if ((nowUtc - claim.StartedAt) <= runTimeout) continue;

            var existing = await _intentStore.TryReadAsync(claim.RunId);
            if (existing != null) continue;

            _logger.LogWarning(
                "Run {RunId} exceeded resolved timeout ({Timeout}s); creating cancel intent trigger=timeout",
                claim.RunId, (int)runTimeout.TotalSeconds);
            try
            {
                await RequestCancelAsync(
                    claim.RunId, CancelTriggers.Timeout, reason: "Run exceeded resolved timeout", ct: ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Failed to create timeout cancel intent for run {RunId}", claim.RunId);
            }
        }

        // Pass B: advance every existing intent. Re-submit for
        // cancel_requested; count attempts and flip to stalled for
        // cancel_submitted.
        var intents = await _intentStore.ListAllAsync();
        foreach (var intent in intents)
        {
            // Safe point: per-intent boundary.
            ct.ThrowIfCancellationRequested();
            try
            {
                await ReconcileIntentAsync(intent, ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Failed to reconcile cancel intent for run {RunId}", intent.RunId);
            }
        }
    }

    private async Task ReconcileIntentAsync(CancelIntent intent, CancellationToken ct)
    {
        // Safe point only: cancellation is observed at entry (the per-intent
        // boundary). The ARM stop fanout below is never interrupted mid-flight.
        ct.ThrowIfCancellationRequested();

        // If the claim is gone, RunTracker finalized but the intent blob
        // wasn't deleted (transient storage failure during cleanup).
        // Idempotently drop the intent so future ticks don't churn.
        var claim = await _claimReader.TryFindByRunIdAsync(intent.RunId);
        if (claim == null)
        {
            _logger.LogInformation(
                "Cancel intent for run {RunId} has no matching claim; deleting", intent.RunId);
            await _intentStore.DeleteAsync(intent.RunId);
            return;
        }

        switch (intent.CancelState)
        {
            case CancelStates.CancelRequested:
                // Re-attempt ACA stop submission in parallel. Single pass per
                // tick — the tick itself is the retry loop. Reset
                // CancelAttemptCount on transition to cancel_submitted
                // because the counter's meaning changes from "submit retries"
                // to "stall ticks"; see RequestCancelAsync for the same reset.
                var reconcileAttempts = await SubmitStopsParallelAsync(claim);
                var nowAccepted = reconcileAttempts.All(s => s.Outcome == StopAttemptOutcome.Accepted);
                if (nowAccepted)
                {
                    var advanced = intent with
                    {
                        CancelState = CancelStates.CancelSubmitted,
                        CancelSubmittedAt = DateTimeOffset.UtcNow,
                        CancelAttemptCount = 0,
                    };
                    await _intentStore.TryUpdateAsync(advanced);
                    _logger.LogInformation(
                        "Reconciler advanced cancel intent for run {RunId} to cancel_submitted",
                        intent.RunId);
                }
                else
                {
                    var bumped = intent with
                    {
                        CancelAttemptCount = intent.CancelAttemptCount + 1,
                    };
                    await _intentStore.TryUpdateAsync(bumped);
                }
                break;

            case CancelStates.CancelSubmitted:
                // Wait for RunTracker.CheckActiveRunsAsync to observe ACA
                // terminal status and finalize — that deletes the claim, and
                // the next reconciler tick drops this intent. Until then,
                // count attempts; flip to stalled once we've waited too
                // long.
                if (intent.CancelAttemptCount + 1 >= CancelStallThreshold)
                {
                    var stalled = intent with
                    {
                        CancelState = CancelStates.CancelStalled,
                        CancelAttemptCount = intent.CancelAttemptCount + 1,
                    };
                    if (await _intentStore.TryUpdateAsync(stalled))
                    {
                        _logger.LogWarning(
                            "Cancel intent for run {RunId} stalled after {Attempts} reconciler passes; manual intervention may be required",
                            intent.RunId, stalled.CancelAttemptCount);
                    }
                }
                else
                {
                    var bumped = intent with { CancelAttemptCount = intent.CancelAttemptCount + 1 };
                    await _intentStore.TryUpdateAsync(bumped);
                }
                break;

            case CancelStates.CancelStalled:
                // Keep nudging ACA — a stuck stop may unstick. Don't flip
                // state. Operator's force-release endpoint is the only exit.
                await SubmitStopsParallelAsync(claim);
                break;
        }
    }

    // Fans the stop submission across every task with an aca_execution_name.
    // Parallel because ARM /stop can take >10s under load (#409); serializing
    // multiplied that latency by task count and exhausted the inline budget.
    // Returns per-task outcomes so callers can report what landed vs what
    // the reconciler still needs to pick up. Empty when nothing's executable
    // (every task failed dispatch) — caller treats that as all-accepted so
    // the state machine can advance and RunTracker finalizes on its existing
    // dispatch_failed path.
    private async Task<IReadOnlyList<StopAttemptResult>> SubmitStopsParallelAsync(ClaimRecord claim)
    {
        var executableTasks = claim.ExpectedTasks
            .Where(t => !string.IsNullOrEmpty(t.AcaExecutionName))
            .ToList();
        if (executableTasks.Count == 0)
        {
            return Array.Empty<StopAttemptResult>();
        }

        var tasks = executableTasks.Select(async task =>
        {
            try
            {
                var accepted = await _aca.CancelExecutionAsync(
                    task.ContainerType, task.AcaExecutionName!, claim.RunId);
                if (accepted)
                {
                    return new StopAttemptResult(
                        task.ContainerType, task.AcaExecutionName!,
                        StopAttemptOutcome.Accepted, null);
                }
                _logger.LogWarning(
                    "Stop submission failed for {Container}/{Execution}",
                    task.ContainerType, task.AcaExecutionName);
                return new StopAttemptResult(
                    task.ContainerType, task.AcaExecutionName!,
                    StopAttemptOutcome.Transient, "arm_transient");
            }
            catch (Exception ex)
            {
                // Log the full exception (subscription IDs, response bodies,
                // etc.) but return a sanitized short code on the response —
                // the cancel endpoint surfaces stop_attempts to operators
                // and ARM exception messages can include tenant identifiers.
                _logger.LogWarning(ex,
                    "Stop submission threw for {Container}/{Execution}",
                    task.ContainerType, task.AcaExecutionName);
                return new StopAttemptResult(
                    task.ContainerType, task.AcaExecutionName!,
                    StopAttemptOutcome.Transient, ClassifyStopException(ex));
            }
        });

        return await Task.WhenAll(tasks);
    }

    // Maps an exception thrown by IAcaJobClient.CancelExecutionAsync to a
    // short code safe to return through the cancel endpoint. The mapping is
    // coarse on purpose; detailed diagnosis lives in the logs. Resilience-
    // handler timeouts surface as Polly.Timeout.TimeoutRejectedException
    // (which doesn't inherit from BCL TimeoutException), so match by name to
    // avoid pulling Polly types into this file.
    private static string ClassifyStopException(Exception ex)
    {
        if (ex is OperationCanceledException) return "timeout";
        if (ex.GetType().Name.Contains("Timeout", StringComparison.Ordinal)) return "timeout";
        if (ex is HttpRequestException) return "network_error";
        if (ex is InvalidOperationException) return "arm_hard_failure";
        return "unknown_error";
    }
}
