using System.Text.Json;
using System.Text.Json.Serialization;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Functions;

public class ManualRunFunction
{
    // Caps on request arrays. Prevent a single /run call from flooding the ACA
    // environment (Easy-Auth gated, so attackers are already authenticated, but
    // any signed-in user could otherwise trigger thousands of dispatches).
    private const int MaxTenantKeys = 50;
    private const int MaxEntityNames = 100;
    private const int MaxIncludeTiers = 5;
    private const int MaxExcludeEntities = 100;

    private readonly IConfigLoader _configLoader;
    private readonly IRunExecutor _runExecutor;
    private readonly IEntityResolver _entityResolver;
    private readonly ITenantResolver _tenantResolver;
    private readonly ILogger<ManualRunFunction> _logger;

    public ManualRunFunction(
        IConfigLoader configLoader,
        IRunExecutor runExecutor,
        IEntityResolver entityResolver,
        ITenantResolver tenantResolver,
        ILogger<ManualRunFunction> logger)
    {
        _configLoader = configLoader;
        _runExecutor = runExecutor;
        _entityResolver = entityResolver;
        _tenantResolver = tenantResolver;
        _logger = logger;
    }

    [Function("ManualRun")]
    public async Task<IActionResult> RunAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "run")] HttpRequest req)
    {
        ManualRunRequest? body;
        try
        {
            body = await JsonSerializer.DeserializeAsync<ManualRunRequest>(req.Body);
        }
        catch (JsonException ex)
        {
            return new BadRequestObjectResult($"Invalid JSON: {ex.Message}");
        }
        if (body == null)
            return new BadRequestObjectResult("Invalid request body");

        if (body.TenantKeys is { Count: > MaxTenantKeys })
            return new BadRequestObjectResult($"tenant_keys exceeds limit of {MaxTenantKeys}");
        if (body.EntityNames is { Count: > MaxEntityNames })
            return new BadRequestObjectResult($"entity_names exceeds limit of {MaxEntityNames}");
        if (body.IncludeTiers is { Count: > MaxIncludeTiers })
            return new BadRequestObjectResult($"include_tiers exceeds limit of {MaxIncludeTiers}");
        if (body.ExcludeEntities is { Count: > MaxExcludeEntities })
            return new BadRequestObjectResult($"exclude_entities exceeds limit of {MaxExcludeEntities}");
        if (body.TimeoutSeconds is { } ts && ts <= 0)
            return new BadRequestObjectResult("timeout_seconds must be > 0");

        // Backfill window must be paired. Either both bounds or neither.
        // RunType is then derived: paired → backfill, neither → normal.
        if (body.BackfillStart.HasValue != body.BackfillEnd.HasValue)
        {
            return new BadRequestObjectResult(
                "backfill_start and backfill_end must be set together (or both omitted).");
        }
        if (body.BackfillStart.HasValue && body.BackfillEnd.HasValue
            && body.BackfillStart.Value >= body.BackfillEnd.Value)
        {
            return new BadRequestObjectResult(
                "backfill_start must be earlier than backfill_end.");
        }

        // Find job definition (optional — can run ad-hoc without a named job)
        JobDefinition job;
        if (!string.IsNullOrEmpty(body.JobName))
        {
            var found = _configLoader.Jobs.Jobs.FirstOrDefault(j => j.Name == body.JobName);
            if (found == null)
                return new NotFoundObjectResult($"Job '{body.JobName}' not found");
            job = found;
        }
        else
        {
            // Ad-hoc run: require either include_tiers or entity_names
            if (body.EntityNames is not { Count: > 0 } && body.IncludeTiers is not { Count: > 0 })
                return new BadRequestObjectResult(
                    "Either job_name, include_tiers, or entity_names is required");

            // Suffix prevents same-second name collisions between concurrent /run calls.
            var suffix = Guid.NewGuid().ToString("N")[..6];
            var adhocName = $"adhoc-{DateTimeOffset.UtcNow:yyyyMMddHHmmss}-{suffix}";
            job = new JobDefinition(
                adhocName,
                "Ad-hoc manual run",
                null,
                true,
                new EntitySelector(
                    IncludeTiers: body.IncludeTiers,
                    IncludeEntities: body.EntityNames,
                    ExcludeEntities: body.ExcludeEntities),
                new TenantSelector(
                    body.TenantKeys is { Count: > 0 } ? TenantSelectorModes.Specific : TenantSelectorModes.All,
                    TenantKeys: body.TenantKeys));
        }

        // Validate entity names against registry
        if (body.EntityNames is { Count: > 0 })
        {
            var knownEntities = _configLoader.EntityRegistry.Entities
                .Select(e => e.Name).ToHashSet();
            var unknown = body.EntityNames.Where(n => !knownEntities.Contains(n)).ToList();
            if (unknown.Count > 0)
                return new BadRequestObjectResult(new InvalidEntitiesResponse(
                    "invalid_entities", unknown, $"Unknown entities: {string.Join(", ", unknown)}"));
        }

        // Validate tenant keys against tenants.json. Without this, RunExecutor silently
        // filters unknown keys (TenantResolver intersects requested ⋂ known) and a
        // request like {tenant_keys:[good, ghost, also_good]} dispatches for 2 of 3
        // with no warning. Mirrors the entity validation above.
        if (body.TenantKeys is { Count: > 0 })
        {
            var knownTenants = _configLoader.Tenants.Tenants
                .Select(t => t.TenantKey).ToHashSet();
            var unknown = body.TenantKeys.Where(k => !knownTenants.Contains(k)).ToList();
            if (unknown.Count > 0)
                return new BadRequestObjectResult(new InvalidTenantsResponse(
                    "invalid_tenants", unknown, $"Unknown tenants: {string.Join(", ", unknown)}"));
        }

        // Per-tenant entity_selector decline. When the user explicitly names entities
        // (body.EntityNames) and any addressed tenant's selector would block one of
        // them, refuse the whole request rather than silently dropping. Decline-whole
        // (vs partial-success) matches the existing invalid_entities/invalid_tenants
        // shape and avoids the surprise of a dispatch that only ran for some named
        // (tenant, entity) pairs. See docs/analytics/tenants-config.md.
        if (body.EntityNames is { Count: > 0 })
        {
            IReadOnlyList<TenantConfig> addressedTenants;
            if (body.TenantKeys is { Count: > 0 })
            {
                var keySet = body.TenantKeys.ToHashSet();
                addressedTenants = _configLoader.Tenants.Tenants
                    .Where(t => t.AnalyticsEnabled && keySet.Contains(t.TenantKey))
                    .ToList();
            }
            else if (!string.IsNullOrEmpty(body.JobName))
            {
                addressedTenants = _tenantResolver.Resolve(job.TenantSelector);
            }
            else
            {
                // Adhoc-from-scratch with no tenant_keys → all analytics-enabled.
                addressedTenants = _configLoader.Tenants.Tenants
                    .Where(t => t.AnalyticsEnabled)
                    .ToList();
            }

            // Compute blocks via Intersect, not Resolve. Resolve has job-side
            // semantics where an exclude-only selector resolves to the empty
            // set; on the tenant side, exclude-only means "everything minus
            // these". Using Resolve here would mark every requested entity as
            // blocked for any tenant with `exclude_entities`-only — see the
            // tenant-side semantic note in EntityResolver.Intersect.
            var requestedAsEntities = _configLoader.EntityRegistry.Entities
                .Where(e => body.EntityNames.Contains(e.Name))
                .ToList();
            var blocks = new List<TenantBlock>();
            foreach (var tenant in addressedTenants)
            {
                if (tenant.EntitySelector is null) continue;
                var allowed = _entityResolver
                    .Intersect(tenant.EntitySelector, requestedAsEntities)
                    .Select(e => e.Name)
                    .ToHashSet();
                var blocked = body.EntityNames.Where(n => !allowed.Contains(n)).ToList();
                if (blocked.Count > 0)
                    blocks.Add(new TenantBlock(tenant.TenantKey, blocked));
            }

            if (blocks.Count > 0)
            {
                return new BadRequestObjectResult(new EntitiesBlockedResponse(
                    "entities_blocked_for_tenants",
                    blocks,
                    "Requested entities are blocked by per-tenant entity_selector for one or more addressed tenants. Adjust tenant_keys or entity_names and retry."));
            }
        }

        var triggeredBy = req.Headers.TryGetValue("X-MS-CLIENT-PRINCIPAL-NAME", out var name)
            ? name.ToString() : "unknown";

        _logger.LogInformation("Manual run triggered for job {Job} by {User}", job.Name, triggeredBy);

        // Build entity selector override. Two cases:
        //  - User provides include_tiers or entity_names: full replacement — the user is
        //    explicitly choosing what to run, so the job's default selector is discarded.
        //  - User provides only exclude_entities: overlay — keep the job's includes but
        //    add the user's exclusions to whatever the job already excludes. A full
        //    replacement here would produce an empty selector (no includes → no entities)
        //    and skipped_empty. The user clearly meant \"job defaults minus these\".
        EntitySelector? entityOverride = null;
        if (!string.IsNullOrEmpty(body.JobName))
        {
            var hasInclude = body.IncludeTiers is { Count: > 0 } || body.EntityNames is { Count: > 0 };
            var hasExclude = body.ExcludeEntities is { Count: > 0 };

            if (hasInclude)
            {
                entityOverride = new EntitySelector(
                    IncludeTiers: body.IncludeTiers,
                    IncludeEntities: body.EntityNames,
                    ExcludeEntities: body.ExcludeEntities);
            }
            else if (hasExclude)
            {
                var baseSelector = job.EntitySelector;
                var mergedExcludes = (baseSelector.ExcludeEntities ?? Array.Empty<string>())
                    .Concat(body.ExcludeEntities!)
                    .Distinct()
                    .ToList();
                entityOverride = new EntitySelector(
                    IncludeTiers: baseSelector.IncludeTiers,
                    IncludeEntities: baseSelector.IncludeEntities,
                    ExcludeEntities: mergedExcludes);
            }
        }

        // Dispatch ACA Jobs and return. Containers run independently —
        // the dispatcher doesn't poll or wait for completion.
        var outcome = await _runExecutor.ExecuteAsync(
            job, TriggerTypes.Manual, triggeredBy,
            body.TenantKeys, entityOverride,
            force: body.Force,
            backfillStart: body.BackfillStart,
            backfillEnd: body.BackfillEnd,
            timeoutSecondsOverride: body.TimeoutSeconds,
            ct: req.HttpContext.RequestAborted);

        if (outcome.SkippedEmpty)
        {
            return new BadRequestObjectResult(new ManualRunSkipResponse(
                job.Name, "skipped_empty",
                "Resolved to 0 tenants or 0 entities. Check tenant_keys and entity selectors."));
        }

        // Aggregate per-tenant outcomes. 409 only when every tenant claim was
        // rejected (true overlap); otherwise 200 with per-tenant breakdown so
        // partial-success dispatches don't masquerade as conflicts.
        var allOverlap = outcome.Tenants.All(t => t.Result == DispatchResult.SkippedOverlap);
        if (allOverlap)
        {
            return new ConflictObjectResult(new ManualRunSkipResponse(
                job.Name, "skipped_overlap",
                $"Job '{job.Name}' already has an active run for every requested tenant. Use force=true to override."));
        }

        // 409 also when every tenant lost a force=true race — same operator
        // response as overlap (no executions started, retry to recover) but
        // a distinct status because the cause is concurrent forces, not
        // standing overlap protection.
        var allConcurrent = outcome.Tenants.All(t => t.Result == DispatchResult.SkippedConcurrent);
        if (allConcurrent)
        {
            return new ConflictObjectResult(new ManualRunSkipResponse(
                job.Name, "skipped_concurrent",
                $"Job '{job.Name}' lost a race against another concurrent force=true dispatch for every requested tenant. Retry once that dispatch is in flight."));
        }

        var dispatchedCount = outcome.Tenants.Sum(t => t.DispatchedCount);
        var failedCount = outcome.Tenants.Sum(t => t.FailedCount);
        var skippedTenantCount = outcome.Tenants.Count(t => t.Result == DispatchResult.SkippedOverlap);
        // SkippedConcurrent: claim race lost on first write, no ACA started.
        // ClaimConcurrentlyModified: won first write but lost second; ACA
        // executions DID start but their names aren't tracked. Surfacing both
        // counts so operators can diagnose without reading per-tenant detail.
        var concurrentSkippedCount = outcome.Tenants.Count(t => t.Result == DispatchResult.SkippedConcurrent);
        var concurrentModifiedCount = outcome.Tenants.Count(t => t.Result == DispatchResult.ClaimConcurrentlyModified);
        // SkippedFilter: tenant's entity_selector intersected to empty. Not a
        // failure — the tenant correctly opted out — but surfaced as its own
        // count so operators can tell at a glance that some tenants in the
        // run weren't dispatched at all.
        var filterSkippedCount = outcome.Tenants.Count(t => t.Result == DispatchResult.SkippedFilter);

        string status;
        if (dispatchedCount == 0 && filterSkippedCount > 0
            && failedCount == 0 && skippedTenantCount == 0
            && concurrentSkippedCount == 0 && concurrentModifiedCount == 0)
        {
            // Every addressed tenant was filter-skipped — there were no failures,
            // just no work. Surface as skipped_filter rather than all_failed so
            // operators don't chase phantom errors.
            status = RunStatuses.SkippedFilter;
        }
        else if (dispatchedCount == 0)
        {
            status = "all_failed";
        }
        else if (failedCount == 0 && skippedTenantCount == 0
                 && concurrentSkippedCount == 0 && concurrentModifiedCount == 0)
        {
            status = TaskStatuses.Dispatched;
        }
        else
        {
            status = "dispatched_with_errors";
        }

        return new OkObjectResult(new ManualRunDispatchResponse(
            job.Name, status, dispatchedCount, failedCount, skippedTenantCount,
            concurrentSkippedCount, concurrentModifiedCount, filterSkippedCount,
            outcome.Tenants));
    }

    private record ManualRunRequest(
        [property: JsonPropertyName("job_name")] string? JobName = null,
        [property: JsonPropertyName("tenant_keys")] IReadOnlyList<string>? TenantKeys = null,
        [property: JsonPropertyName("include_tiers")] IReadOnlyList<int>? IncludeTiers = null,
        [property: JsonPropertyName("entity_names")] IReadOnlyList<string>? EntityNames = null,
        [property: JsonPropertyName("exclude_entities")] IReadOnlyList<string>? ExcludeEntities = null,
        [property: JsonPropertyName("force")] bool Force = false,
        [property: JsonPropertyName("backfill_start")] DateTimeOffset? BackfillStart = null,
        [property: JsonPropertyName("backfill_end")] DateTimeOffset? BackfillEnd = null,
        [property: JsonPropertyName("timeout_seconds")] int? TimeoutSeconds = null
    );

    public record ManualRunDispatchResponse(
        [property: JsonPropertyName("job_name")] string JobName,
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("dispatched_count")] int DispatchedCount,
        [property: JsonPropertyName("failed_count")] int FailedCount,
        [property: JsonPropertyName("skipped_tenant_count")] int SkippedTenantCount,
        [property: JsonPropertyName("concurrent_skipped_count")] int ConcurrentSkippedCount,
        [property: JsonPropertyName("concurrent_modified_count")] int ConcurrentModifiedCount,
        [property: JsonPropertyName("filter_skipped_count")] int FilterSkippedCount,
        [property: JsonPropertyName("tenants")] IReadOnlyList<TenantOutcome> Tenants);

    public record ManualRunSkipResponse(
        [property: JsonPropertyName("job_name")] string JobName,
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("message")] string Message);

    public record InvalidEntitiesResponse(
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("unknown_entities")] IReadOnlyList<string> UnknownEntities,
        [property: JsonPropertyName("message")] string Message);

    public record InvalidTenantsResponse(
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("unknown_tenants")] IReadOnlyList<string> UnknownTenants,
        [property: JsonPropertyName("message")] string Message);

    public record EntitiesBlockedResponse(
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("blocked")] IReadOnlyList<TenantBlock> Blocked,
        [property: JsonPropertyName("message")] string Message);

    public record TenantBlock(
        [property: JsonPropertyName("tenant_key")] string TenantKey,
        [property: JsonPropertyName("entities")] IReadOnlyList<string> Entities);
}
