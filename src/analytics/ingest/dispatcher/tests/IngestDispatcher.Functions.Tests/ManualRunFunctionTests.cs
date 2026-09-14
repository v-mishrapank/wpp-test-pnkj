using IngestDispatcher.Functions.Functions;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IngestDispatcher.Functions.Tests;

public class ManualRunFunctionTests
{
    private static readonly IReadOnlyList<EntityType> Entities =
    [
        new("entra_users", 1, "graph-ingest"),
        new("entra_groups", 2, "graph-ingest"),
    ];

    // Default tenants — neither has an entity_selector, so the new
    // entities_blocked_for_tenants decline path is a no-op for existing tests.
    private static readonly IReadOnlyList<TenantConfig> Tenants =
    [
        new("madev1", "tid-1", "MADEV1", null, true, 1),
        new("madev2", "tid-2", "MADEV2", null, true, 1),
    ];

    private readonly IConfigLoader _config = Substitute.For<IConfigLoader>();
    private readonly IRunExecutor _executor = Substitute.For<IRunExecutor>();
    private readonly ManualRunFunction _function;

    public ManualRunFunctionTests()
    {
        _config.EntityRegistry.Returns(new EntityRegistryConfig(Entities));
        _config.Tenants.Returns(new TenantsConfig(Tenants));
        _config.Jobs.Returns(new JobsConfig([
            new JobDefinition("daily", "d", "0 0 * * *", true,
                new EntitySelector(IncludeTiers: [1]),
                new TenantSelector(TenantSelectorModes.All))
        ]));
        _executor.ExecuteAsync(
                Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns(new DispatchOutcome(
                [new TenantOutcome("madev1", "run123", DispatchResult.Dispatched, 1, 0, [])],
                SkippedEmpty: false));

        // Real resolvers — substitutes would return null on Resolve() and break
        // the per-tenant decline path the moment any tenant has a selector.
        _function = new ManualRunFunction(
            _config, _executor,
            new EntityResolver(_config), new TenantResolver(_config),
            Substitute.For<ILogger<ManualRunFunction>>());
    }

    [Fact]
    public async Task InvalidBody_Returns400()
    {
        var req = HttpRequestHelper.BuildJsonRequest("null");
        var result = await _function.RunAsync(req);
        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task MalformedJson_Returns400()
    {
        // Regression: JsonException used to propagate unhandled, producing
        // HTTP 500 with a stack trace. Now caught and surfaced as 400.
        var req = HttpRequestHelper.BuildJsonRequest("{not valid json");
        var result = await _function.RunAsync(req);
        var bad = Assert.IsType<BadRequestObjectResult>(result);
        Assert.Contains("Invalid JSON", bad.Value?.ToString());
    }

    [Fact]
    public async Task UnknownJobName_Returns404()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "does-not-exist" }""");
        var result = await _function.RunAsync(req);
        Assert.IsType<NotFoundObjectResult>(result);
    }

    [Fact]
    public async Task AdhocWithoutTiersOrEntities_Returns400()
    {
        // No job_name, no include_tiers, no entity_names — not enough to run ad-hoc.
        var req = HttpRequestHelper.BuildJsonRequest("""{ }""");
        var result = await _function.RunAsync(req);
        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task AdhocWithTiers_BuildsAdhocNameAndDispatches()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""{ "include_tiers": [1] }""");
        var result = await _function.RunAsync(req);

        Assert.IsType<OkObjectResult>(result);
        await _executor.Received(1).ExecuteAsync(
            Arg.Is<JobDefinition>(j => j.Name.StartsWith("adhoc-") && j.Name.Length > 6),
            TriggerTypes.Manual, Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
    }

    [Fact]
    public async Task TenantKeysOverLimit_Returns400()
    {
        var keys = string.Join(",", Enumerable.Range(0, 51).Select(i => $"\"t{i}\""));
        var req = HttpRequestHelper.BuildJsonRequest($$"""
            { "job_name": "daily", "tenant_keys": [{{keys}}] }
            """);
        var result = await _function.RunAsync(req);
        var bad = Assert.IsType<BadRequestObjectResult>(result);
        Assert.Contains("tenant_keys", bad.Value?.ToString());
    }

    [Fact]
    public async Task EntityNamesOverLimit_Returns400()
    {
        var names = string.Join(",", Enumerable.Range(0, 101).Select(i => $"\"e{i}\""));
        var req = HttpRequestHelper.BuildJsonRequest($$"""
            { "include_tiers": [1], "entity_names": [{{names}}] }
            """);
        var result = await _function.RunAsync(req);
        var bad = Assert.IsType<BadRequestObjectResult>(result);
        Assert.Contains("entity_names", bad.Value?.ToString());
    }

    [Fact]
    public async Task UnknownEntityName_Returns400WithList()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "include_tiers": [1], "entity_names": ["entra_users", "bogus"] }
            """);
        var result = await _function.RunAsync(req);
        var bad = Assert.IsType<BadRequestObjectResult>(result);
        var response = Assert.IsType<ManualRunFunction.InvalidEntitiesResponse>(bad.Value);
        Assert.Equal("invalid_entities", response.Status);
        Assert.Contains("bogus", response.UnknownEntities);
    }

    [Fact]
    public async Task UnknownTenantKey_Returns400WithList()
    {
        // Regression: unknown tenant_keys used to be silently filtered by RunExecutor
        // (TenantResolver intersects requested ⋂ known), so {tenant_keys:[good, ghost,
        // also_good]} dispatched 2 of 3 with no warning. Now mirrors entity validation.
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "job_name": "daily", "tenant_keys": ["madev1", "ghost", "madev2"] }
            """);
        var result = await _function.RunAsync(req);
        var bad = Assert.IsType<BadRequestObjectResult>(result);
        var response = Assert.IsType<ManualRunFunction.InvalidTenantsResponse>(bad.Value);
        Assert.Equal("invalid_tenants", response.Status);
        Assert.Contains("ghost", response.UnknownTenants);
        Assert.Single(response.UnknownTenants);
    }

    [Fact]
    public async Task AllTenantsOverlap_Returns409()
    {
        _executor.ExecuteAsync(
                Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns(new DispatchOutcome(
                [
                    new TenantOutcome("madev1", "r1", DispatchResult.SkippedOverlap, 0, 0, []),
                    new TenantOutcome("madev2", "r2", DispatchResult.SkippedOverlap, 0, 0, []),
                ],
                SkippedEmpty: false));

        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        var result = await _function.RunAsync(req);

        Assert.IsType<ConflictObjectResult>(result);
    }

    [Fact]
    public async Task AllTenantsConcurrent_Returns409()
    {
        // Two force=true dispatches racing each other: every tenant lost the
        // first-write CAS. Mirrors the all-overlap policy with a distinct
        // status string so the operator can tell which kind of conflict
        // they hit.
        _executor.ExecuteAsync(
                Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns(new DispatchOutcome(
                [
                    new TenantOutcome("madev1", "r1", DispatchResult.SkippedConcurrent, 0, 0, []),
                    new TenantOutcome("madev2", "r2", DispatchResult.SkippedConcurrent, 0, 0, []),
                ],
                SkippedEmpty: false));

        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily", "force": true }""");
        var result = await _function.RunAsync(req);

        var conflict = Assert.IsType<ConflictObjectResult>(result);
        var body = Assert.IsType<ManualRunFunction.ManualRunSkipResponse>(conflict.Value);
        Assert.Equal("skipped_concurrent", body.Status);
    }

    [Fact]
    public async Task PartialClaimConcurrentlyModified_Returns200WithCount()
    {
        // One tenant dispatched cleanly, one had its claim overwritten by a
        // concurrent forced dispatcher (ACA executions did fire but tracking
        // is stale). 200 because dispatch happened; the count surfaces the
        // tracking-state failure for operator triage.
        _executor.ExecuteAsync(
                Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns(new DispatchOutcome(
                [
                    new TenantOutcome("madev1", "r1", DispatchResult.Dispatched, 1, 0,
                        [new TaskOutcome("c", ["e"], TaskStatuses.Dispatched, "exec-1", null, null)]),
                    new TenantOutcome("madev2", "r2", DispatchResult.ClaimConcurrentlyModified, 1, 0,
                        [new TaskOutcome("c", ["e"], TaskStatuses.Dispatched, "exec-2", null, null)]),
                ],
                SkippedEmpty: false));

        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily", "force": true }""");
        var result = await _function.RunAsync(req);

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = Assert.IsType<ManualRunFunction.ManualRunDispatchResponse>(ok.Value);
        Assert.Equal("dispatched_with_errors", body.Status);
        Assert.Equal(2, body.DispatchedCount);
        Assert.Equal(1, body.ConcurrentModifiedCount);
        Assert.Equal(0, body.ConcurrentSkippedCount);
    }

    [Fact]
    public async Task SkippedEmpty_Returns400()
    {
        _executor.ExecuteAsync(
                Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns(new DispatchOutcome([], SkippedEmpty: true));

        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        var result = await _function.RunAsync(req);

        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task MixedOutcome_StatusIsDispatchedWithErrors()
    {
        _executor.ExecuteAsync(
                Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns(new DispatchOutcome(
                [
                    new TenantOutcome("madev1", "r1", DispatchResult.Dispatched, 3, 2,
                        [new TaskOutcome("c", ["e"], TaskStatuses.Dispatched, null, null, null)])
                ],
                SkippedEmpty: false));

        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        var result = await _function.RunAsync(req);

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = Assert.IsType<ManualRunFunction.ManualRunDispatchResponse>(ok.Value);
        Assert.Equal("dispatched_with_errors", body.Status);
        Assert.Equal(3, body.DispatchedCount);
        Assert.Equal(2, body.FailedCount);
    }

    [Fact]
    public async Task PartialOverlap_Returns200WithSkippedTenantCount()
    {
        // One tenant overlapped, one dispatched cleanly. Should be 200 (not 409),
        // status reflects the partial state, and the response surfaces both per-tenant.
        _executor.ExecuteAsync(
                Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns(new DispatchOutcome(
                [
                    new TenantOutcome("madev1", "r1", DispatchResult.SkippedOverlap, 0, 0, []),
                    new TenantOutcome("madev2", "r2", DispatchResult.Dispatched, 1, 0,
                        [new TaskOutcome("c", ["e"], TaskStatuses.Dispatched, "exec-1", null, null)]),
                ],
                SkippedEmpty: false));

        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        var result = await _function.RunAsync(req);

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = Assert.IsType<ManualRunFunction.ManualRunDispatchResponse>(ok.Value);
        Assert.Equal("dispatched_with_errors", body.Status);
        Assert.Equal(1, body.DispatchedCount);
        Assert.Equal(0, body.FailedCount);
        Assert.Equal(1, body.SkippedTenantCount);
        Assert.Equal(2, body.Tenants.Count);
    }

    [Fact]
    public async Task AllFailed_StatusIsAllFailed()
    {
        _executor.ExecuteAsync(
                Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns(new DispatchOutcome(
                [new TenantOutcome("madev1", "r1", DispatchResult.Dispatched, 0, 5, [])],
                SkippedEmpty: false));

        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        var result = await _function.RunAsync(req);

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = Assert.IsType<ManualRunFunction.ManualRunDispatchResponse>(ok.Value);
        Assert.Equal("all_failed", body.Status);
    }

    [Fact]
    public async Task PrincipalHeader_PassedAsTriggeredBy()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""",
            ("X-MS-CLIENT-PRINCIPAL-NAME", "alice@example.com"));

        await _function.RunAsync(req);

        await _executor.Received(1).ExecuteAsync(
            Arg.Any<JobDefinition>(), TriggerTypes.Manual, "alice@example.com",
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
    }

    [Fact]
    public async Task JobNameOnly_NoOverride()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        await _function.RunAsync(req);

        await _executor.Received(1).ExecuteAsync(
            Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(),
            Arg.Is<EntitySelector?>(s => s == null),
            Arg.Any<bool>());
    }

    [Fact]
    public async Task JobNameWithExcludeEntitiesOnly_OverlaysOnJobDefaults()
    {
        // Regression: exclude_entities submitted without include_tiers/entity_names
        // used to produce a null override, silently dropping the exclusion. A naive
        // fix that made a full-replacement override produced an empty selector
        // (no includes → skipped_empty). Correct behavior: overlay the exclusion
        // on top of the job's default selector, preserving its includes.
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "job_name": "daily", "exclude_entities": ["entra_users"] }
            """);
        await _function.RunAsync(req);

        await _executor.Received(1).ExecuteAsync(
            Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(),
            Arg.Is<EntitySelector?>(s =>
                s != null
                && s.IncludeTiers!.SequenceEqual(new[] { 1 })  // job default preserved
                && s.ExcludeEntities!.Contains("entra_users")),  // user exclusion applied
            Arg.Any<bool>());
    }

    [Fact]
    public async Task BackfillStartOnly_Returns400()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "job_name": "daily", "backfill_start": "2026-04-01T00:00:00Z" }
            """);
        var result = await _function.RunAsync(req);
        var bad = Assert.IsType<BadRequestObjectResult>(result);
        Assert.Contains("backfill", bad.Value?.ToString());
    }

    [Fact]
    public async Task BackfillEndOnly_Returns400()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "job_name": "daily", "backfill_end": "2026-05-01T00:00:00Z" }
            """);
        var result = await _function.RunAsync(req);
        var bad = Assert.IsType<BadRequestObjectResult>(result);
        Assert.Contains("backfill", bad.Value?.ToString());
    }

    [Fact]
    public async Task BackfillStartAfterEnd_Returns400()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "job_name": "daily",
              "backfill_start": "2026-05-01T00:00:00Z",
              "backfill_end":   "2026-04-01T00:00:00Z" }
            """);
        var result = await _function.RunAsync(req);
        var bad = Assert.IsType<BadRequestObjectResult>(result);
        Assert.Contains("earlier", bad.Value?.ToString());
    }

    [Fact]
    public async Task BackfillBothSet_PassesWindowToExecutor()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "job_name": "daily",
              "backfill_start": "2026-04-01T00:00:00Z",
              "backfill_end":   "2026-05-01T00:00:00Z" }
            """);
        await _function.RunAsync(req);

        await _executor.Received(1).ExecuteAsync(
            Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(),
            Arg.Is<DateTimeOffset?>(d => d.HasValue && d.Value == DateTimeOffset.Parse("2026-04-01T00:00:00Z")),
            Arg.Is<DateTimeOffset?>(d => d.HasValue && d.Value == DateTimeOffset.Parse("2026-05-01T00:00:00Z")));
    }

    [Fact]
    public async Task NoBackfill_PassesNullsToExecutor()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        await _function.RunAsync(req);

        await _executor.Received(1).ExecuteAsync(
            Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(),
            Arg.Is<DateTimeOffset?>(d => !d.HasValue),
            Arg.Is<DateTimeOffset?>(d => !d.HasValue));
    }

    [Fact]
    public async Task JobNameWithIncludeTiers_FullReplacement()
    {
        // When user provides include_tiers, the override fully replaces the job's
        // selector — they're explicitly choosing what to run.
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "job_name": "daily", "include_tiers": [2] }
            """);
        await _function.RunAsync(req);

        await _executor.Received(1).ExecuteAsync(
            Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(),
            Arg.Is<EntitySelector?>(s =>
                s != null
                && s.IncludeTiers!.SequenceEqual(new[] { 2 })),
            Arg.Any<bool>());
    }

    [Fact]
    public async Task EntityNames_BlockedByTenantSelector_DeclinesWith400()
    {
        // madev2 excludes entra_users — a request that explicitly names
        // entra_users for madev2 must be declined rather than silently dropped.
        _config.Tenants.Returns(new TenantsConfig([
            new("madev1", "tid-1", "MADEV1", null, true, 1),
            new("madev2", "tid-2", "MADEV2", null, true, 1,
                new EntitySelector(ExcludeEntities: ["entra_users"]))
        ]));

        var req = HttpRequestHelper.BuildJsonRequest("""
            { "entity_names": ["entra_users"], "tenant_keys": ["madev2"] }
            """);
        var result = await _function.RunAsync(req);

        var bad = Assert.IsType<BadRequestObjectResult>(result);
        var resp = Assert.IsType<ManualRunFunction.EntitiesBlockedResponse>(bad.Value);
        Assert.Equal("entities_blocked_for_tenants", resp.Status);
        Assert.Single(resp.Blocked);
        Assert.Equal("madev2", resp.Blocked[0].TenantKey);
        Assert.Equal(["entra_users"], resp.Blocked[0].Entities);

        // Executor must not have been called — decline-whole, not partial-success.
        await _executor.DidNotReceiveWithAnyArgs().ExecuteAsync(default!, default!, default);
    }

    [Fact]
    public async Task EntityNames_BlockedForOnlySomeTenants_DeclinesAll()
    {
        // Mixed: madev1 has no selector (allows everything), madev2 blocks entra_users.
        // Request addresses both — decline-whole-request behavior must trigger,
        // not "run madev1 and report madev2 blocked".
        _config.Tenants.Returns(new TenantsConfig([
            new("madev1", "tid-1", "MADEV1", null, true, 1),
            new("madev2", "tid-2", "MADEV2", null, true, 1,
                new EntitySelector(ExcludeEntities: ["entra_users"]))
        ]));

        var req = HttpRequestHelper.BuildJsonRequest("""
            { "entity_names": ["entra_users"], "tenant_keys": ["madev1", "madev2"] }
            """);
        var result = await _function.RunAsync(req);

        var bad = Assert.IsType<BadRequestObjectResult>(result);
        var resp = Assert.IsType<ManualRunFunction.EntitiesBlockedResponse>(bad.Value);
        Assert.Single(resp.Blocked);
        Assert.Equal("madev2", resp.Blocked[0].TenantKey);
        await _executor.DidNotReceiveWithAnyArgs().ExecuteAsync(default!, default!, default);
    }

    [Fact]
    public async Task EntityNames_AllTenantsAllow_DoesNotDecline()
    {
        // Both tenants allow entra_users — request should reach the executor.
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "entity_names": ["entra_users"], "tenant_keys": ["madev1", "madev2"] }
            """);
        var result = await _function.RunAsync(req);

        Assert.IsType<OkObjectResult>(result);
        await _executor.Received(1).ExecuteAsync(
            Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(),
            Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
    }

    [Fact]
    public async Task EntityNames_TenantExcludesDifferentEntity_DoesNotDecline()
    {
        // Regression: an exclude-only tenant selector must use tenant-side
        // semantics ("everything minus these"), not job-side Resolve()
        // semantics ("nothing, minus these" = empty). Earlier impl wrongly
        // marked every requested entity as blocked when a tenant had only
        // `exclude_entities`, even if the request was for an unrelated entity.
        _config.Tenants.Returns(new TenantsConfig([
            new("madev1", "tid-1", "MADEV1", null, true, 1,
                new EntitySelector(ExcludeEntities: ["entra_groups"]))
        ]));

        // Request entra_users — NOT in the tenant's exclude list. Decline path
        // should be a no-op; executor must be invoked.
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "entity_names": ["entra_users"], "tenant_keys": ["madev1"] }
            """);
        var result = await _function.RunAsync(req);

        Assert.IsType<OkObjectResult>(result);
        await _executor.Received(1).ExecuteAsync(
            Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>(),
            Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
    }

    [Fact]
    public async Task IncludeTiersOnly_BlockedByTenantSelector_DoesNotDecline()
    {
        // Tier-based requests don't carry by-name operator intent. Even if a
        // tenant's selector blocks some entities in the tier, no decline — the
        // intersection silently drops them at dispatch (and a skipped_filter
        // RunRecord covers the empty case).
        _config.Tenants.Returns(new TenantsConfig([
            new("madev1", "tid-1", "MADEV1", null, true, 1,
                new EntitySelector(ExcludeEntities: ["entra_users"]))
        ]));

        var req = HttpRequestHelper.BuildJsonRequest("""
            { "include_tiers": [1] }
            """);
        var result = await _function.RunAsync(req);

        Assert.IsType<OkObjectResult>(result);
    }
}
