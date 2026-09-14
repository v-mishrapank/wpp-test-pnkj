using System.Text.Json;
using System.Text.Json.Serialization;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Functions;

public class CancelRunFunction
{
    private readonly IRunCanceller _canceller;
    private readonly IRunHistoryReader _historyReader;
    private readonly ILogger<CancelRunFunction> _logger;

    public CancelRunFunction(
        IRunCanceller canceller,
        IRunHistoryReader historyReader,
        ILogger<CancelRunFunction> logger)
    {
        _canceller = canceller;
        _historyReader = historyReader;
        _logger = logger;
    }

    [Function("CancelRun")]
    public async Task<IActionResult> CancelAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "runs/{runId}/cancel")] HttpRequest req,
        string runId)
    {
        CancelRunRequest? body = null;
        if (req.ContentLength > 0)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<CancelRunRequest>(req.Body);
            }
            catch (JsonException ex)
            {
                return new BadRequestObjectResult($"Invalid JSON: {ex.Message}");
            }
        }

        var triggeredBy = req.Headers.TryGetValue("X-MS-CLIENT-PRINCIPAL-NAME", out var name)
            ? name.ToString() : "unknown";

        _logger.LogInformation(
            "Cancel requested for run {RunId} by {User} (reason: {Reason})",
            runId, triggeredBy, body?.Reason ?? "(none)");

        var result = await _canceller.RequestCancelAsync(
            runId, CancelTriggers.Manual, body?.Reason, req.HttpContext.RequestAborted);

        return result.Outcome switch
        {
            // 202: intent persisted. The intent state on the response says
            // whether every ARM stop landed (cancel_submitted) or any are
            // still pending (cancel_requested — reconciler picks up). The
            // stop_attempts list gives per-task ARM outcomes so a caller can
            // see exactly what happened without polling.
            RequestCancelOutcome.Submitted =>
                new AcceptedResult(string.Empty, ToResponse(result.Intent!, result.StopAttempts)),

            RequestCancelOutcome.AlreadyCancelling =>
                new AcceptedResult(string.Empty, ToResponse(result.Intent!, result.StopAttempts)),

            // No active claim. Distinguish two cases via history:
            //   - run exists in history → 409 (already terminal)
            //   - run not in history either → 404
            RequestCancelOutcome.NotFound =>
                await ResolveNotFoundAsync(runId),

            // 502: the intent blob itself couldn't be persisted. Distinct
            // from partial ARM stops (those are still 202 with stop_attempts
            // showing which ones the reconciler will retry). This path means
            // we have nothing for the reconciler to pick up — operator
            // should retry.
            RequestCancelOutcome.IntentPersistFailed =>
                new ObjectResult(new ProblemResponse(
                    "intent_persist_failed",
                    "Cancel intent could not be recorded due to a transient storage failure. Re-call this endpoint."))
                {
                    StatusCode = StatusCodes.Status502BadGateway,
                },

            // Defensive — shouldn't be reached. Surface as 500 so it's visible.
            RequestCancelOutcome.AlreadyTerminal =>
                new ConflictObjectResult(new ProblemResponse(
                    "already_terminal",
                    "Run is already in a terminal state.")),

            _ => new ObjectResult($"Unexpected outcome: {result.Outcome}")
            {
                StatusCode = StatusCodes.Status500InternalServerError,
            },
        };
    }

    private async Task<IActionResult> ResolveNotFoundAsync(string runId)
    {
        // No active claim. Check history with a long window so a recently
        // completed run resolves as 409 rather than 404.
        var historyRun = await _historyReader.TryFindByRunIdAsync(runId, TimeSpan.FromDays(30));
        if (historyRun != null)
        {
            return new ConflictObjectResult(new ProblemResponse(
                "already_terminal",
                $"Run '{runId}' is already in a terminal state ({historyRun.Status})."));
        }
        return new NotFoundObjectResult(new ProblemResponse(
            "not_found",
            $"No run found with id '{runId}'."));
    }

    private static CancelIntentResponse ToResponse(
        CancelIntent intent, IReadOnlyList<StopAttemptResult>? stopAttempts) => new(
        RunId: intent.RunId,
        State: intent.CancelState,
        Trigger: intent.CancelTrigger,
        Reason: intent.CancelReason,
        RequestedAt: intent.CancelRequestedAt,
        SubmittedAt: intent.CancelSubmittedAt,
        AttemptCount: intent.CancelAttemptCount,
        StopAttempts: stopAttempts?.Select(s => new StopAttemptResponse(
            Container: s.ContainerType,
            Execution: s.ExecutionName,
            Outcome: s.Outcome switch
            {
                StopAttemptOutcome.Accepted => "accepted",
                StopAttemptOutcome.Transient => "transient",
                _ => "unknown",
            },
            Error: s.Error)).ToList());

    private record CancelRunRequest(
        [property: JsonPropertyName("reason")] string? Reason = null);

    private record CancelIntentResponse(
        [property: JsonPropertyName("run_id")] string RunId,
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("trigger")] string Trigger,
        [property: JsonPropertyName("reason")] string? Reason,
        [property: JsonPropertyName("requested_at")] DateTimeOffset RequestedAt,
        [property: JsonPropertyName("submitted_at")] DateTimeOffset? SubmittedAt,
        [property: JsonPropertyName("attempt_count")] int AttemptCount,
        [property: JsonPropertyName("stop_attempts")] IReadOnlyList<StopAttemptResponse>? StopAttempts);

    private record StopAttemptResponse(
        [property: JsonPropertyName("container")] string Container,
        [property: JsonPropertyName("execution")] string Execution,
        [property: JsonPropertyName("outcome")] string Outcome,
        [property: JsonPropertyName("error")] string? Error);

    private record ProblemResponse(
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("message")] string Message);
}
