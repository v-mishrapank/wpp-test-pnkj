using System.Text.Json;
using System.Text.Json.Serialization;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Functions;

// Operator escape hatch for cancel_stalled runs. Use only after externally
// verifying that the replica is no longer running (e.g., Azure Portal shows
// the execution Stopped/Failed, container logs are quiet). Writes a
// CancellationFailed terminal status to JSONL — downstream MUST treat the
// run's data integrity as uncertain.
public class ForceReleaseRunFunction
{
    private readonly IClaimReader _claimReader;
    private readonly IClaimWriter _claimWriter;
    private readonly IRunCanceller _canceller;
    private readonly IRunHistoryWriter _historyWriter;
    private readonly IHeartbeatCache _runStateCache;
    private readonly ILogger<ForceReleaseRunFunction> _logger;

    public ForceReleaseRunFunction(
        IClaimReader claimReader,
        IClaimWriter claimWriter,
        IRunCanceller canceller,
        IRunHistoryWriter historyWriter,
        IHeartbeatCache runStateCache,
        ILogger<ForceReleaseRunFunction> logger)
    {
        _claimReader = claimReader;
        _claimWriter = claimWriter;
        _canceller = canceller;
        _historyWriter = historyWriter;
        _runStateCache = runStateCache;
        _logger = logger;
    }

    [Function("ForceReleaseRun")]
    public async Task<IActionResult> ForceReleaseAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "runs/{runId}/force-release")] HttpRequest req,
        string runId)
    {
        ForceReleaseRequest? body;
        try
        {
            body = await JsonSerializer.DeserializeAsync<ForceReleaseRequest>(req.Body);
        }
        catch (JsonException ex)
        {
            return new BadRequestObjectResult($"Invalid JSON: {ex.Message}");
        }
        if (body == null || string.IsNullOrWhiteSpace(body.Verified))
        {
            return new BadRequestObjectResult(
                "Required field 'verified' is missing. Include a freeform description of how the replica's terminal state was confirmed.");
        }
        if (body.VerifiedAt == default)
        {
            return new BadRequestObjectResult(
                "Required field 'verified_at' is missing or invalid. Use ISO-8601.");
        }

        var triggeredBy = req.Headers.TryGetValue("X-MS-CLIENT-PRINCIPAL-NAME", out var name)
            ? name.ToString() : "unknown";

        _logger.LogWarning(
            "Force-release for run {RunId} by {User}: verified={Verified} verified_at={VerifiedAt}",
            runId, triggeredBy, body.Verified, body.VerifiedAt);

        var intent = await _canceller.TryReadIntentAsync(runId);
        if (intent == null)
        {
            return new NotFoundObjectResult(new ProblemResponse(
                "not_found",
                $"No cancel intent found for run '{runId}'."));
        }
        if (intent.CancelState != CancelStates.CancelStalled)
        {
            return new ConflictObjectResult(new ProblemResponse(
                "not_stalled",
                $"Cancel intent for run '{runId}' is in state '{intent.CancelState}'. Force-release only acts on cancel_stalled."));
        }

        var claim = await _claimReader.TryFindByRunIdAsync(runId);
        if (claim == null)
        {
            // Intent exists but claim is gone — RunTracker already finalized
            // and just left the intent behind. Drop the intent and tell the
            // caller it was already resolved.
            await _canceller.DeleteIntentAsync(runId);
            return new ConflictObjectResult(new ProblemResponse(
                "already_terminal",
                $"Run '{runId}' was already finalized; orphaned cancel intent removed."));
        }

        // Write CancellationFailed for every expected task. Downstream
        // (DLT) must treat this as data-integrity-uncertain: the replica
        // may have continued writing after force-release. The verified
        // string is captured in the per-task error_message so the audit
        // trail records why the operator gave up waiting for ACA.
        var nowUtc = DateTimeOffset.UtcNow;
        var errorMessage =
            $"Force-released by {triggeredBy} at {nowUtc:o}: verified={body.Verified} verified_at={body.VerifiedAt:o}";

        var taskRecords = claim.ExpectedTasks.Select(t => new TaskRecord(
            RunId: claim.RunId,
            JobName: claim.JobName,
            TenantKey: claim.TenantKey,
            RunType: claim.RunType,
            ContainerType: t.ContainerType,
            AcaExecutionName: t.AcaExecutionName,
            Entities: t.Entities,
            Status: TaskStatuses.CancellationFailed,
            StartedAt: claim.StartedAt,
            CompletedAt: nowUtc,
            ErrorMessage: errorMessage)).ToList();

        // Run-level CancellationFailed mirrors the per-task vocabulary
        // (#382): force-released runs aren't organic failures, they're an
        // operator-initiated cancellation that ACA never confirmed
        // terminal. Downstream consumers reading status='cancellation_failed'
        // must treat data integrity as uncertain.
        var runRecord = new RunRecord(
            claim.RunId, claim.JobName, claim.TenantKey, claim.TriggerType, claim.RunType,
            claim.TriggeredBy, RunStatuses.CancellationFailed,
            claim.StartedAt, nowUtc,
            claim.ExpectedTasks.Count, claim.ResolvedEntities.Count, claim.ResolvedEntities);

        var runResult = await _historyWriter.WriteRunAsync(runRecord);
        var taskResult = await _historyWriter.WriteTasksAsync(claim.RunId, taskRecords);

        if (runResult == WriteResult.Failed || taskResult == WriteResult.Failed)
        {
            _logger.LogError(
                "Force-release for run {RunId}: history write failed; claim and intent retained for retry",
                runId);
            return new ObjectResult(new ProblemResponse(
                "history_write_failed",
                "Failed to write run history; claim and intent retained. Retry the request."))
            {
                StatusCode = StatusCodes.Status500InternalServerError,
            };
        }

        // Cleanup: same ordering as RunTracker — claim, then cancel intent,
        // then evict in-process run-state cache. The run-state blob itself
        // is retained past finalization (#385) so the forensic /runs/{id}
        // view of the force-released run keeps showing last-known per-entity
        // detail. If anything fails the next reconciler tick picks it up.
        await _claimWriter.DeleteAsync(claim.JobName, claim.TenantKey, claim.RunType);
        await _canceller.DeleteIntentAsync(claim.RunId);
        _runStateCache.EvictRun(claim.RunId);

        _logger.LogWarning(
            "Force-release for run {RunId} completed; {Count} tasks marked CancellationFailed",
            runId, taskRecords.Count);

        return new OkObjectResult(new ForceReleaseResponse(
            RunId: runId,
            Status: "force_released",
            TasksMarked: taskRecords.Count,
            CompletedAt: nowUtc));
    }

    private record ForceReleaseRequest(
        [property: JsonPropertyName("verified")] string? Verified,
        [property: JsonPropertyName("verified_at")] DateTimeOffset VerifiedAt);

    private record ForceReleaseResponse(
        [property: JsonPropertyName("run_id")] string RunId,
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("tasks_marked")] int TasksMarked,
        [property: JsonPropertyName("completed_at")] DateTimeOffset CompletedAt);

    private record ProblemResponse(
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("message")] string Message);
}
