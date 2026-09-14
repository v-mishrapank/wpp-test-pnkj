using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Functions;

public class RunsFunction
{
    // Default window (24h) when ?since= isn't supplied. Cap at 30d hard.
    private static readonly TimeSpan DefaultWindow = TimeSpan.FromHours(24);
    private static readonly TimeSpan MaxWindow = TimeSpan.FromDays(30);

    // Heartbeat staleness threshold for the `stale=true` display flag
    // returned by /runs/{id}. Sized to ignore the inline-fetch silences
    // that a live container hits regularly under load (large Dataverse
    // pages, Graph deep enumerations) — anything shorter produces false-
    // positive stale warnings on healthy runs. Display-only after #457
    // removed the finalization staleness gate (finalization now uses
    // ACA-terminal-first instead of blob age).
    private static readonly TimeSpan StaleThreshold = TimeSpan.FromMinutes(2);

    // Listing limits.
    private const int DefaultListLimit = 200;
    private const int MaxListLimit = 1000;

    private readonly IClaimReader _claimReader;
    private readonly IRunHistoryReader _historyReader;
    private readonly IRunStateReader _runStateReader;
    private readonly ITaskStateDeriver _stateDeriver;
    private readonly ILogger<RunsFunction> _logger;

    public RunsFunction(
        IClaimReader claimReader,
        IRunHistoryReader historyReader,
        IRunStateReader runStateReader,
        ITaskStateDeriver stateDeriver,
        ILogger<RunsFunction> logger)
    {
        _claimReader = claimReader;
        _historyReader = historyReader;
        _runStateReader = runStateReader;
        _stateDeriver = stateDeriver;
        _logger = logger;
    }

    [Function("RunsList")]
    public async Task<IActionResult> ListAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "runs")] HttpRequest req)
    {
        // Parse window
        TimeSpan window;
        try
        {
            window = ParseSince(req.Query["since"].ToString()) ?? DefaultWindow;
        }
        catch (ArgumentException ex)
        {
            return new BadRequestObjectResult(ex.Message);
        }
        if (window > MaxWindow)
            return new BadRequestObjectResult($"since exceeds {MaxWindow.TotalDays:0}d cap");

        // Parse limit
        var limit = DefaultListLimit;
        if (int.TryParse(req.Query["limit"].ToString(), out var parsedLimit))
        {
            if (parsedLimit < 1 || parsedLimit > MaxListLimit)
                return new BadRequestObjectResult($"limit must be 1..{MaxListLimit}");
            limit = parsedLimit;
        }

        // Active runs: from claim blobs. Apply the same window cutoff to
        // active claims as to history — a stuck claim (e.g. dispatcher crash
        // mid-finalize, storage outage during cleanup) past 24h shouldn't
        // bypass the user's `?since=` window.
        var cutoff = DateTimeOffset.UtcNow - window;
        var activeClaims = (await _claimReader.ListActiveAsync())
            .Where(c => c.StartedAt >= cutoff);
        var activeEntries = new List<RunListEntry>();
        foreach (var claim in activeClaims)
        {
            var blobs = await _runStateReader.ListForRunAsync(claim.RunId);
            // Count terminal tasks: ones whose run-state blob is terminal
            // (run_status != 'running'), plus the ones that failed dispatch
            // (captured in the claim itself, no blob will ever land for
            // them). Without this, a run with N tasks where one
            // dispatch_failed shows tasks_completed = (N - 1) and clients
            // think the run is stalled.
            var terminalBlobCount = blobs.Count(b =>
                !string.Equals(b.RunStatus, BlobRunStatuses.Running, StringComparison.Ordinal));
            var dispatchFailedCount = claim.ExpectedTasks.Count(t =>
                t.AcaExecutionName == null && t.DispatchError != null);
            var tasksCompleted = terminalBlobCount + dispatchFailedCount;
            activeEntries.Add(new RunListEntry(
                RunId: claim.RunId,
                JobName: claim.JobName,
                TenantKey: claim.TenantKey,
                RunType: claim.RunType,
                Status: RunListStatusValues.Running,
                StartedAt: claim.StartedAt,
                CompletedAt: null,
                ElapsedSeconds: (long)(DateTimeOffset.UtcNow - claim.StartedAt).TotalSeconds,
                TaskCount: claim.ExpectedTasks.Count,
                TasksCompleted: tasksCompleted));
        }

        // Completed runs: from history. RunRecord.TaskCount is populated at
        // finalization from the claim's ExpectedTasks count. tasks_completed
        // for a finalized run equals task_count (every expected task reached
        // a terminal state — that's what defines finalization).
        var historyRuns = await _historyReader.ListInWindowAsync(window);
        var historyEntries = historyRuns.Select(r => new RunListEntry(
            RunId: r.RunId,
            JobName: r.JobName,
            TenantKey: r.TenantKey,
            RunType: r.RunType,
            Status: r.Status,
            StartedAt: r.StartedAt,
            CompletedAt: r.CompletedAt,
            ElapsedSeconds: r.CompletedAt.HasValue
                ? (long)(r.CompletedAt.Value - r.StartedAt).TotalSeconds
                : 0,
            TaskCount: r.TaskCount,
            TasksCompleted: r.TaskCount));

        var sorted = MergeActiveAndHistory(activeEntries, historyEntries)
            .OrderByDescending(r => r.StartedAt)
            .Take(limit)
            .ToList();

        return new OkObjectResult(new RunListResponse(sorted));
    }

    // Dedupe by run_id with claim-wins precedence (#404). An active claim
    // means CheckClaimAsync hasn't finished — the claim is deleted as the
    // last step of finalization after history + tasks land. While the claim
    // is present, the run is in-flight from the dispatcher's point of view,
    // even if a history row exists from a finalize tick that's still
    // mid-cleanup or whose claim delete transiently failed. The previously-
    // "history wins" rule caused a brief flicker to a terminal status for
    // in-flight runs during that window. Self-heals: WriteRunAsync is
    // idempotent (IfNoneMatch=*), so a stuck-history+stuck-claim state
    // retries cleanly on the next tick and the claim eventually gets
    // deleted.
    internal static IReadOnlyList<RunListEntry> MergeActiveAndHistory(
        IEnumerable<RunListEntry> activeEntries,
        IEnumerable<RunListEntry> historyEntries)
    {
        var byRunId = new Dictionary<string, RunListEntry>();
        foreach (var entry in historyEntries) byRunId[entry.RunId] = entry;
        foreach (var entry in activeEntries) byRunId[entry.RunId] = entry;
        return byRunId.Values.ToList();
    }

    [Function("RunsDetail")]
    public async Task<IActionResult> DetailAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "runs/{runId}")] HttpRequest req,
        string runId)
    {
        // Resolution order:
        //   1. Claim present → in-flight projection (heartbeat + manifests).
        //   2. History present → completed projection (manifests + task records).
        //   3. Else 404.
        var claim = await _claimReader.TryFindByRunIdAsync(runId);
        if (claim != null)
        {
            return new OkObjectResult(await BuildInFlightAsync(claim));
        }

        var historyRun = await _historyReader.TryFindByRunIdAsync(runId, MaxWindow);
        if (historyRun == null)
        {
            return new NotFoundObjectResult($"Run '{runId}' not found within {MaxWindow.TotalDays:0}d window");
        }

        return new OkObjectResult(await BuildCompletedAsync(historyRun));
    }

    private async Task<RunStatusResponse> BuildInFlightAsync(ClaimRecord claim)
    {
        // Read every run-state blob for the run in parallel — one per
        // expected task. RunStateReader.ListForRunAsync handles the prefix
        // scan + cache gap-fill.
        var blobs = await _runStateReader.ListForRunAsync(claim.RunId);
        var blobsByContainer = blobs.ToDictionary(b => b.ContainerType, b => b);

        var tasks = _stateDeriver.Derive(claim, taskHistory: [], blobsByContainer,
            StaleThreshold, DateTimeOffset.UtcNow);

        return new RunStatusResponse(
            RunId: claim.RunId,
            JobName: claim.JobName,
            TenantKey: claim.TenantKey,
            TriggerType: claim.TriggerType,
            RunType: claim.RunType,
            TriggeredBy: claim.TriggeredBy,
            StartedAt: claim.StartedAt,
            CompletedAt: null,
            ElapsedSeconds: (long)(DateTimeOffset.UtcNow - claim.StartedAt).TotalSeconds,
            Status: RunListStatusValues.Running,
            ResolvedEntities: claim.ResolvedEntities,
            Tasks: tasks);
    }

    private async Task<RunStatusResponse> BuildCompletedAsync(RunRecord run)
    {
        var taskHistory = await _historyReader.ReadTasksAsync(run.RunId, run.StartedAt);

        // Read every run-state blob for the run — same as in-flight, since
        // blobs are durable past finalization (#385). Pre-#385 runs, or
        // post-rollout runs whose blob aged out beyond OrphanAgeThresholdHours,
        // produce an empty dict; the deriver falls back to taskHistory and
        // surfaces empty entities[] (no per-entity detail survives the
        // missing blob).
        var blobs = await _runStateReader.ListForRunAsync(run.RunId);
        var blobsByContainer = blobs.ToDictionary(b => b.ContainerType, b => b);

        var tasks = _stateDeriver.Derive(claim: null, taskHistory, blobsByContainer,
            StaleThreshold, DateTimeOffset.UtcNow);

        var elapsed = run.CompletedAt.HasValue
            ? (long)(run.CompletedAt.Value - run.StartedAt).TotalSeconds
            : 0;

        return new RunStatusResponse(
            RunId: run.RunId,
            JobName: run.JobName,
            TenantKey: run.TenantKey,
            TriggerType: run.TriggerType,
            RunType: run.RunType,
            TriggeredBy: run.TriggeredBy,
            StartedAt: run.StartedAt,
            CompletedAt: run.CompletedAt,
            ElapsedSeconds: elapsed,
            Status: run.Status,
            ResolvedEntities: run.ResolvedEntities,
            Tasks: tasks);
    }

    // Parse "?since=24h" / "?since=7d" / "?since=30m". Returns null when the
    // query string is empty (caller falls back to default). Throws
    // ArgumentException with a user-readable message on bad input.
    internal static TimeSpan? ParseSince(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;

        var trimmed = raw.Trim();
        const string expected = "expected e.g. 24h, 7d, 30m";
        if (trimmed.Length < 2)
            throw new ArgumentException($"Invalid since value '{raw}'; {expected}");

        var unit = trimmed[^1];
        var numberPart = trimmed[..^1];
        if (!int.TryParse(numberPart, out var n) || n < 0)
            throw new ArgumentException($"Invalid since value '{raw}'; {expected}");

        return unit switch
        {
            'h' or 'H' => TimeSpan.FromHours(n),
            'd' or 'D' => TimeSpan.FromDays(n),
            'm' or 'M' => TimeSpan.FromMinutes(n),
            _ => throw new ArgumentException($"Invalid since unit '{unit}'; {expected}"),
        };
    }
}
