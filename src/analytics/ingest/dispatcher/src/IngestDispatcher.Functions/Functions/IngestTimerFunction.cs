using Cronos;
using IngestDispatcher.Functions.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Functions;

public class IngestTimerFunction
{
    // Hardcoded throttle for the cancel reconciler. Picked tight enough that
    // operator kill-switch latency is bounded (~2 min worst-case before a
    // stuck cancel-requested intent gets its retry), loose enough that we
    // don't ARM-pound on every minute tick. See
    // docs/analytics/ingest-dispatcher-cancellation.md.
    private static readonly TimeSpan CancelReconcileInterval = TimeSpan.FromMinutes(2);
    private static DateTimeOffset _lastCancelReconcile = DateTimeOffset.MinValue;

    private readonly IConfigLoader _configLoader;
    private readonly IRunExecutor _runExecutor;
    private readonly IRunTracker _runTracker;
    private readonly IRunCanceller _runCanceller;
    private readonly ICronStateStore _cronStateStore;
    private readonly IRunStateReader _runStateReader;
    private readonly ILogger<IngestTimerFunction> _logger;

    public IngestTimerFunction(
        IConfigLoader configLoader,
        IRunExecutor runExecutor,
        IRunTracker runTracker,
        IRunCanceller runCanceller,
        ICronStateStore cronStateStore,
        IRunStateReader runStateReader,
        ILogger<IngestTimerFunction> logger)
    {
        _configLoader = configLoader;
        _runExecutor = runExecutor;
        _runTracker = runTracker;
        _runCanceller = runCanceller;
        _cronStateStore = cronStateStore;
        _runStateReader = runStateReader;
        _logger = logger;
    }

    // UseMonitor=true makes the extension take a blob lease so only one instance
    // of this timer runs across scale-out. Don't change this, and don't tighten
    // the schedule to sub-minute — UseMonitor silently defaults to false there
    // and overlap protection starts doing real work.
    [Function("IngestTimer")]
    public async Task RunAsync(
        [TimerTrigger("0 * * * * *", UseMonitor = true)] TimerInfo timer,
        CancellationToken cancellationToken = default)
    {
        // 1. Reconcile cancel intents (detect deadline crossings, re-submit
        // ARM stops for any cancel_requested intents from a previous trigger
        // whose inline submission failed, advance cancel_submitted →
        // cancel_stalled after threshold). Throttled — runs at most every
        // CancelReconcileInterval, no-ops on most ticks.
        var nowReconcile = DateTimeOffset.UtcNow;
        if (nowReconcile - _lastCancelReconcile >= CancelReconcileInterval)
        {
            _lastCancelReconcile = nowReconcile;
            try
            {
                await _runCanceller.ReconcileAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Cancel reconciler pass failed");
            }
        }

        // 2. Check status of previously dispatched runs (run-state blob +
        // ACA + cancel-intent → terminal task status; finalize when every
        // task resolved).
        await _runTracker.CheckActiveRunsAsync(cancellationToken);

        // 2b. Age out run-state blobs older than the history horizon
        // (default 30d). Post-#385 this is the only retention mechanism
        // for these blobs — they're no longer deleted at finalization.
        // Throttled internally (default 1/hour); a noop on most ticks.
        await _runStateReader.SweepOrphansAsync(cancellationToken);

        // 2. Evaluate cron schedules. Per-job lastEval timestamps let us catch up
        // occurrences missed when a tick runs late (host restart, long CheckActive,
        // etc.) instead of silently dropping them.
        var now = DateTimeOffset.UtcNow;
        var state = await _cronStateStore.LoadAsync();
        var activeJobNames = new HashSet<string>();

        foreach (var job in _configLoader.Jobs.Jobs)
        {
            // Safe point: per-job boundary. A draining host stops the cron scan
            // between jobs instead of dispatching the rest of the schedule.
            cancellationToken.ThrowIfCancellationRequested();

            if (!job.Enabled || string.IsNullOrEmpty(job.Cron))
                continue;

            activeJobNames.Add(job.Name);

            try
            {
                var cron = CronExpression.Parse(job.Cron);
                // First-ever eval: use now-60s to mirror the single-tick window,
                // avoiding a retroactive dispatch storm on fresh deploys.
                var lastEval = state.LastEvaluated.TryGetValue(job.Name, out var le)
                    ? le
                    : now.AddSeconds(-60);

                var next = cron.GetNextOccurrence(lastEval.UtcDateTime);

                if (next == null || next > now.UtcDateTime)
                {
                    // No occurrence in (lastEval, now]; safe to advance.
                    state.LastEvaluated[job.Name] = now;
                    continue;
                }

                _logger.LogInformation("Job {Job} is due (occurrence {Occurrence}), dispatching",
                    job.Name, next);
                await _runExecutor.ExecuteAsync(job, TriggerTypes.Scheduled, triggeredBy: null, ct: cancellationToken);

                // Advance only after the dispatch path completes. If ExecuteAsync
                // throws, the next tick re-evaluates this window and re-fires the
                // missed occurrence. Overlap protection catches the case where a
                // prior attempt already claimed the tracking blob.
                state.LastEvaluated[job.Name] = now;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error evaluating job {Job}", job.Name);
            }
        }

        // Drop state for jobs that were removed or disabled so the map doesn't grow forever.
        foreach (var key in state.LastEvaluated.Keys.ToList())
        {
            if (!activeJobNames.Contains(key))
                state.LastEvaluated.Remove(key);
        }

        await _cronStateStore.SaveAsync(state);
    }
}
