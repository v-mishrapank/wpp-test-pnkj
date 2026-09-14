using System.Text.Json.Serialization;
using IngestDispatcher.Functions.Models;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Services;

public enum DispatchResult
{
    Dispatched,
    SkippedOverlap,

    // force=true lost the first-write race against a concurrent forced
    // dispatch. No ACA executions were started. Same operator-visible effect
    // as overlap, but distinguished because it surfaced under force=true (and
    // is therefore unexpected — operators don't expect their force calls to
    // be rejected unless they're racing themselves).
    SkippedConcurrent,

    // force=true won the first-write race but lost the second write — ACA
    // executions DID start, but their names didn't get persisted to the claim
    // because another writer (the original race winner of the first write)
    // overwrote it with a different run's names. This dispatcher's
    // executions are tracked-but-orphaned. Worse than SkippedConcurrent.
    ClaimConcurrentlyModified,

    SkippedEmpty,

    // The job's resolved entity list intersected with this tenant's
    // entity_selector down to an empty set, so nothing was dispatched for
    // this tenant. A skipped_filter RunRecord is written directly (no claim,
    // no ACA executions). Per-tenant, distinct from SkippedEmpty which is
    // run-wide.
    SkippedFilter,
}

public record DispatchOutcome(
    [property: JsonPropertyName("tenants")] IReadOnlyList<TenantOutcome> Tenants,
    [property: JsonPropertyName("skipped_empty")] bool SkippedEmpty);

public record TenantOutcome(
    [property: JsonPropertyName("tenant_key")] string TenantKey,
    [property: JsonPropertyName("run_id")] string RunId,
    [property: JsonPropertyName("result")] DispatchResult Result,
    [property: JsonPropertyName("dispatched_count")] int DispatchedCount,
    [property: JsonPropertyName("failed_count")] int FailedCount,
    [property: JsonPropertyName("tasks")] IReadOnlyList<TaskOutcome> Tasks);

public record TaskOutcome(
    [property: JsonPropertyName("container_type")] string ContainerType,
    [property: JsonPropertyName("entities")] IReadOnlyList<string> Entities,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("aca_execution_name")] string? AcaExecutionName,
    [property: JsonPropertyName("completed_at")] DateTimeOffset? CompletedAt,
    [property: JsonPropertyName("error_message")] string? ErrorMessage);

public interface IRunExecutor
{
    Task<DispatchOutcome> ExecuteAsync(JobDefinition job, string triggerType, string? triggeredBy,
        IReadOnlyList<string>? tenantKeyOverrides = null,
        EntitySelector? entitySelectorOverride = null,
        bool force = false,
        DateTimeOffset? backfillStart = null,
        DateTimeOffset? backfillEnd = null,
        int? timeoutSecondsOverride = null,
        CancellationToken ct = default);
}

public class RunExecutor : IRunExecutor
{
    // Max concurrent StartJobAsync calls across the whole dispatch (all tenants).
    // Keeps ARM POST pressure bounded so the resilience handler's rate limiter
    // doesn't short-circuit when a multi-tenant dispatch fans out.
    private const int DispatchConcurrency = 5;

    // Dispatcher-side default run timeout, matching ACA's 7d ceiling so the
    // platform never preempts the dispatcher's wall-clock policy. Overridable
    // per job (jobs.json timeout_seconds) and per invocation (ad-hoc body
    // timeout_seconds). See docs/analytics/ingest-dispatcher-cancellation.md.
    public const int DefaultTimeoutSeconds = 604800;

    private readonly IEntityResolver _entityResolver;
    private readonly ITenantResolver _tenantResolver;
    private readonly IAcaJobClient _dispatcher;
    private readonly IClaimWriter _claimWriter;
    private readonly IConfigLoader _configLoader;
    private readonly IRunHistoryWriter _historyWriter;
    private readonly ILogger<RunExecutor> _logger;

    public RunExecutor(
        IEntityResolver entityResolver,
        ITenantResolver tenantResolver,
        IAcaJobClient dispatcher,
        IClaimWriter claimWriter,
        IConfigLoader configLoader,
        IRunHistoryWriter historyWriter,
        ILogger<RunExecutor> logger)
    {
        _entityResolver = entityResolver;
        _tenantResolver = tenantResolver;
        _dispatcher = dispatcher;
        _claimWriter = claimWriter;
        _configLoader = configLoader;
        _historyWriter = historyWriter;
        _logger = logger;
    }

    // Local mutable shadow of an ExpectedTask used during the dispatch loop.
    // Tracks pre-claim-update outcomes (status + execution-name + dispatch
    // error). Persisted to the claim once at the end of DispatchTenantAsync;
    // never mutated thereafter.
    private sealed class DispatchTaskState
    {
        public required string ContainerType { get; init; }
        public required IReadOnlyList<string> Entities { get; init; }
        public string Status { get; set; } = TaskStatuses.Dispatched;
        public string? AcaExecutionName { get; set; }
        public string? DispatchError { get; set; }
        public DateTimeOffset? CompletedAt { get; set; }
    }

    public async Task<DispatchOutcome> ExecuteAsync(JobDefinition job, string triggerType, string? triggeredBy,
        IReadOnlyList<string>? tenantKeyOverrides = null,
        EntitySelector? entitySelectorOverride = null,
        bool force = false,
        DateTimeOffset? backfillStart = null,
        DateTimeOffset? backfillEnd = null,
        int? timeoutSecondsOverride = null,
        CancellationToken ct = default)
    {
        // Safe point: honor a pre-cancelled token before any tenant/entity
        // resolution so a drained host does no dispatch work at all.
        ct.ThrowIfCancellationRequested();

        var startedAt = DateTimeOffset.UtcNow;
        var resolvedTimeoutSeconds = ResolveTimeoutSeconds(timeoutSecondsOverride, job.TimeoutSeconds);

        // RunType is derived server-side from request shape, never explicitly passed
        // by the caller. Both backfill bounds set → backfill; both unset → normal.
        // The ManualRun handler rejects "only one set" before this method is called.
        var runType = (backfillStart.HasValue && backfillEnd.HasValue)
            ? RunTypes.Backfill
            : RunTypes.Normal;

        // Resolve tenants
        IReadOnlyList<TenantConfig> tenants;
        if (tenantKeyOverrides is { Count: > 0 })
        {
            var allTenants = _tenantResolver.Resolve(new TenantSelector(TenantSelectorModes.All));
            var keySet = tenantKeyOverrides.ToHashSet();
            tenants = allTenants.Where(t => keySet.Contains(t.TenantKey)).ToList();
        }
        else
        {
            tenants = _tenantResolver.Resolve(job.TenantSelector);
        }

        // Resolve entities
        var entitySelector = entitySelectorOverride ?? job.EntitySelector;
        var entities = _entityResolver.Resolve(entitySelector);

        if (tenants.Count == 0 || entities.Count == 0)
        {
            _logger.LogWarning("Job {Job} resolved to 0 tenants or 0 entities, skipping", job.Name);
            return new DispatchOutcome([], SkippedEmpty: true);
        }

        // Per-tenant intersection: each tenant's optional entity_selector tightens
        // the job-resolved set. A tenant with null selector gets the full job set;
        // a tenant whose selector excludes everything in `entities` is filter-skipped.
        // Done before claim/template work so we don't burn ARM calls for entities
        // we won't dispatch.
        var perTenant = new List<(TenantConfig tenant, string runId, List<DispatchTaskState> dispatchTasks, ClaimRecord claim)>();
        var filterSkippedOutcomes = new List<TenantOutcome>();
        var jobEntityNames = entities.Select(e => e.Name).ToHashSet();

        foreach (var tenant in tenants)
        {
            var tenantEntities = _entityResolver.Intersect(tenant.EntitySelector, entities);
            var runId = Guid.NewGuid().ToString("N")[..12];

            // Misconfig tripwire: any entity the (effective) selector explicitly
            // named that the tenant's filter dropped. Fires whether the tenant
            // still partially runs or is fully filter-skipped. The adhoc path
            // normally catches body.EntityNames blocks via ManualRunFunction's
            // decline validator before reaching here; this warning surfaces the
            // jobs.json-level case (and any adhoc-by-job_name case where the
            // declined-path doesn't apply because body.EntityNames isn't set).
            if (entitySelector.IncludeEntities is { Count: > 0 } includeNames)
            {
                var tenantNames = tenantEntities.Select(e => e.Name).ToHashSet();
                foreach (var named in includeNames)
                {
                    if (jobEntityNames.Contains(named) && !tenantNames.Contains(named))
                    {
                        _logger.LogWarning(
                            "Named entity blocked for tenant: event={Event} job={Job} tenant={Tenant} entity={Entity} trigger={Trigger} run_id={RunId}",
                            "named_entity_blocked_for_tenant",
                            job.Name, tenant.TenantKey, named, triggerType, runId);
                    }
                }
            }

            if (tenantEntities.Count == 0)
            {
                var blockedNames = entities.Select(e => e.Name).ToList();
                _logger.LogInformation(
                    "Tenant {Tenant} skipped for job {Job}: per-tenant entity_selector intersected to empty (blocked: {Blocked})",
                    tenant.TenantKey, job.Name, string.Join(",", blockedNames));

                // No claim, no ACA — straight to history. The finalization path
                // (RunTracker → RunHistoryWriter) only runs for tenants with
                // active claims, so a filter-skipped tenant would otherwise leave
                // no trace in /runs. WriteRunAsync swallows transient exceptions
                // and returns Failed, so a degraded blob store won't abort the
                // whole dispatch — but we still surface the miss in logs so a
                // filter-skipped tenant doesn't silently disappear.
                var writeResult = await _historyWriter.WriteRunAsync(new RunRecord(
                    RunId: runId,
                    JobName: job.Name,
                    TenantKey: tenant.TenantKey,
                    TriggerType: triggerType,
                    RunType: runType,
                    TriggeredBy: triggeredBy,
                    Status: RunStatuses.SkippedFilter,
                    StartedAt: startedAt,
                    CompletedAt: DateTimeOffset.UtcNow,
                    TaskCount: 0,
                    EntityCount: 0,
                    ResolvedEntities: []));
                if (writeResult == WriteResult.Failed)
                {
                    _logger.LogWarning(
                        "Failed to write skipped_filter RunRecord for tenant {Tenant} job {Job} run {RunId}; skip will not appear in /runs",
                        tenant.TenantKey, job.Name, runId);
                }

                filterSkippedOutcomes.Add(new TenantOutcome(
                    tenant.TenantKey, runId, DispatchResult.SkippedFilter, 0, 0, []));
                continue;
            }

            // Per-tenant container grouping. Different tenants may have different
            // container sets if their selectors hit different containers.
            var entityGroups = tenantEntities.GroupBy(e => e.Container)
                .ToDictionary(g => g.Key, g => g.Select(e => e.Name).ToList());
            var dispatchTasks = entityGroups.Select(kvp => new DispatchTaskState
            {
                ContainerType = kvp.Key,
                Entities = kvp.Value,
            }).ToList();

            // Initial claim: aca_execution_name null on every task (none have
            // been started yet). Second write at the end of DispatchTenantAsync
            // populates the names.
            var claim = new ClaimRecord
            {
                SchemaVersion = 1,
                RunId = runId,
                JobName = job.Name,
                TenantKey = tenant.TenantKey,
                RunType = runType,
                TriggerType = triggerType,
                TriggeredBy = triggeredBy,
                StartedAt = startedAt,
                ResolvedEntities = tenantEntities.Select(e => e.Name).ToList(),
                ExpectedTasks = dispatchTasks.Select(t => new ExpectedTask
                {
                    ContainerType = t.ContainerType,
                    Entities = t.Entities,
                    AcaExecutionName = null,
                }).ToList(),
                ResolvedTimeoutSeconds = resolvedTimeoutSeconds,
            };
            perTenant.Add((tenant, runId, dispatchTasks, claim));
        }

        if (perTenant.Count == 0)
        {
            // Every tenant filtered out. Return the filter-skipped outcomes so the
            // caller can surface them; SkippedEmpty stays false because the run
            // *did* address tenants — it's not the run-wide empty case at line 145.
            return new DispatchOutcome(filterSkippedOutcomes, SkippedEmpty: false);
        }

        // Claim phase. Per-tenant claims are independent — a long-running run for
        // tenant A doesn't block tenant B. Claiming before any ARM template fetch
        // means an all-overlap timer tick (the typical case during a long backfill)
        // costs only N storage round-trips and zero ARM GETs, instead of paying for
        // templates that no one will use.
        //
        // Safe point: last observation before we begin creating claims. Once a
        // claim lands we run the dispatch to completion so cancellation can't
        // leave a claim with no ARM executions the reconciler never observes.
        ct.ThrowIfCancellationRequested();
        var claimResults = await Task.WhenAll(perTenant.Select(async pt =>
            (pt, result: await _claimWriter.TryCreateAsync(pt.claim, force))));

        var skipped = claimResults.Where(r => !r.result.Created)
            .Select(r => new TenantOutcome(r.pt.tenant.TenantKey, r.pt.runId,
                MapRejection(r.result.Outcome), 0, 0, []))
            .ToList();

        foreach (var r in claimResults.Where(r => r.result.Outcome == CreateOutcome.RejectedExists))
        {
            _logger.LogInformation(
                "Skipping job {Job} for tenant {Tenant}: active run exists (use force to override)",
                job.Name, r.pt.tenant.TenantKey);
        }

        // Seed claim.ETag from the create-time response so the second write
        // can use If-Match. Without this seeding, UpdateExecutionNamesAsync
        // falls back to unconditional overwrite and the race re-opens.
        var claimed = claimResults.Where(r => r.result.Created)
            .Select(r => r.pt with { claim = r.pt.claim with { ETag = r.result.ETag } })
            .ToList();
        if (claimed.Count == 0)
            return new DispatchOutcome(skipped, SkippedEmpty: false);

        // Fetch each distinct ACA job's template once per dispatch instead of per
        // (tenant, container). Only happens after at least one tenant claimed —
        // otherwise the templates would be fetched and discarded. The template
        // carries image + cpu/memory; all three must be echoed on every dispatch
        // because the ACA start endpoint replaces, not merges (see
        // AcaJobClient.StartJobAsync).
        // Union across claimed tenants — per-tenant entity filtering can leave
        // different tenants needing different container subsets.
        var containersInUse = claimed
            .SelectMany(pt => pt.dispatchTasks.Select(t => t.ContainerType))
            .Distinct()
            .ToList();
        var templatesByContainer = new Dictionary<string, JobContainerTemplate>();
        foreach (var containerJobName in containersInUse)
        {
            try
            {
                templatesByContainer[containerJobName] = await _dispatcher.GetJobTemplateAsync(containerJobName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to fetch template for ACA Job {Container}; all {Container} tasks will fail to dispatch",
                    containerJobName, containerJobName);
                // Leaving it out of the dict marks every task for this container as dispatch_failed.
            }
        }

        // Dispatch phase, only for tenants that successfully claimed.
        using var semaphore = new SemaphoreSlim(DispatchConcurrency);
        var dispatched = await Task.WhenAll(claimed.Select(pt =>
            DispatchTenantAsync(job, pt.tenant, pt.runId, pt.dispatchTasks, pt.claim,
                templatesByContainer, semaphore, runType, backfillStart, backfillEnd, ct)));

        return new DispatchOutcome(
            [.. filterSkippedOutcomes, .. skipped, .. dispatched],
            SkippedEmpty: false);
    }

    private async Task<TenantOutcome> DispatchTenantAsync(
        JobDefinition job, TenantConfig tenant, string runId,
        List<DispatchTaskState> dispatchTasks, ClaimRecord claim,
        IReadOnlyDictionary<string, JobContainerTemplate> templatesByContainer,
        SemaphoreSlim semaphore,
        string runType,
        DateTimeOffset? backfillStart,
        DateTimeOffset? backfillEnd,
        CancellationToken ct)
    {
        // No cancellation observation here: the claim is already committed.
        // Once claimed, dispatch and the second claim write must complete so
        // the reconciler can observe what launched or failed to launch.
        _ = ct;

        _logger.LogInformation(
            "Run {RunId} for job {Job} tenant {Tenant}: dispatching {TaskCount} container jobs ({EntityCount} entities)",
            runId, job.Name, tenant.TenantKey, dispatchTasks.Count, claim.ResolvedEntities.Count);

        // Dispatch in parallel under the shared semaphore. Each task mutates only
        // its own DispatchTaskState, so no shared-state races.
        await Task.WhenAll(dispatchTasks.Select(async task =>
        {
            await semaphore.WaitAsync();
            try
            {
                if (!templatesByContainer.TryGetValue(task.ContainerType, out var template))
                {
                    task.Status = TaskStatuses.DispatchFailed;
                    task.CompletedAt = DateTimeOffset.UtcNow;
                    task.DispatchError = $"Template for ACA Job {task.ContainerType} was not fetched; see earlier error";
                    return;
                }

                try
                {
                    var executionName = await _dispatcher.StartJobAsync(
                        task.ContainerType, template, tenant, task.Entities, _configLoader.Storage, runId,
                        runType, backfillStart, backfillEnd);

                    task.AcaExecutionName = executionName;

                    _logger.LogInformation(
                        "Dispatched {Container} for tenant {Tenant} (execution: {Execution})",
                        task.ContainerType, tenant.TenantKey, executionName);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to start {Container} for tenant {Tenant}",
                        task.ContainerType, tenant.TenantKey);
                    task.Status = TaskStatuses.DispatchFailed;
                    task.CompletedAt = DateTimeOffset.UtcNow;
                    task.DispatchError = ex.Message;
                }
            }
            finally
            {
                semaphore.Release();
            }
        }));

        var dispatchedCount = dispatchTasks.Count(t => t.Status == TaskStatuses.Dispatched);
        var failedCount = dispatchTasks.Count(t => t.Status == TaskStatuses.DispatchFailed);

        _logger.LogInformation(
            "Run {RunId} for job {Job} tenant {Tenant}: dispatched {Dispatched} jobs ({Failed} failed to dispatch)",
            runId, job.Name, tenant.TenantKey, dispatchedCount, failedCount);

        // Second claim write: persist execution names + dispatch errors. This is
        // the LAST mutation of the claim — it's immutable from here until the
        // finalization step deletes it. No per-task status, completed_at, or
        // error_message stored — those are derived at API/finalization time
        // from heartbeat + manifest + ACA status.
        var updatedClaim = claim with
        {
            ExpectedTasks = dispatchTasks.Select(t => new ExpectedTask
            {
                ContainerType = t.ContainerType,
                Entities = t.Entities,
                AcaExecutionName = t.AcaExecutionName,
                DispatchError = t.DispatchError,
            }).ToList()
        };
        var updateResult = await _claimWriter.UpdateExecutionNamesAsync(updatedClaim);

        var taskOutcomes = dispatchTasks.Select(t => new TaskOutcome(
            t.ContainerType, t.Entities, t.Status,
            t.AcaExecutionName, t.CompletedAt, t.DispatchError)).ToList();

        // ACA executions already kicked off above; the ConcurrentlyModified
        // path means their names couldn't be persisted to the claim, not
        // that they didn't run. dispatched_count/failed_count reflect actual
        // ACA outcomes; the result enum signals tracking-state correctness.
        var dispatchResult = updateResult == UpdateResult.ConcurrentlyModified
            ? DispatchResult.ClaimConcurrentlyModified
            : DispatchResult.Dispatched;

        return new TenantOutcome(tenant.TenantKey, runId, dispatchResult,
            dispatchedCount, failedCount, taskOutcomes);
    }

    private static DispatchResult MapRejection(CreateOutcome outcome) => outcome switch
    {
        CreateOutcome.RejectedExists => DispatchResult.SkippedOverlap,
        CreateOutcome.RejectedConcurrent => DispatchResult.SkippedConcurrent,
        _ => throw new InvalidOperationException($"Unexpected non-rejection outcome: {outcome}"),
    };

    // MIN of any non-null override, the job's configured value, and the
    // dispatcher default. Tightest wins. Nullable inputs are ignored.
    internal static int ResolveTimeoutSeconds(int? adhocOverride, int? jobConfigured)
    {
        var candidates = new List<int> { DefaultTimeoutSeconds };
        if (adhocOverride is { } a && a > 0) candidates.Add(a);
        if (jobConfigured is { } j && j > 0) candidates.Add(j);
        return candidates.Min();
    }
}
