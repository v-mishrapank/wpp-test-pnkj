using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IngestDispatcher.Functions.Tests;

// Integration-style tests for the per-tenant entity_selector intersection in
// RunExecutor.ExecuteAsync. Covers: per-tenant ClaimRecord shape, skipped_filter
// RunRecord write, fully-blocked-tenant outcome, multi-tenant mixed outcomes.
public class PerTenantEntityFilteringTests
{
    private static readonly IReadOnlyList<EntityType> Entities =
    [
        new("entra_users", 1, "graph-ingest"),
        new("entra_groups", 1, "graph-ingest"),
        new("exo_mailboxes", 1, "exo-ingest"),
    ];

    private static readonly StorageConfig Storage = new(
        "https://test.dfs.core.windows.net", "landing",
        new StorageAuthConfig(StorageAuthMethods.ManagedIdentity));

    private static readonly JobDefinition TierJob = new(
        "daily", "tier 1 daily", "0 0 * * *", true,
        new EntitySelector(IncludeTiers: [1]),
        new TenantSelector(TenantSelectorModes.All));

    private readonly IClaimWriter _claimWriter = Substitute.For<IClaimWriter>();
    private readonly IAcaJobClient _dispatcher = Substitute.For<IAcaJobClient>();
    private readonly IRunHistoryWriter _historyWriter = Substitute.For<IRunHistoryWriter>();

    private RunExecutor BuildExecutor(IReadOnlyList<TenantConfig> tenants)
    {
        var configLoader = Substitute.For<IConfigLoader>();
        configLoader.EntityRegistry.Returns(new EntityRegistryConfig(Entities));
        configLoader.Tenants.Returns(new TenantsConfig(tenants));
        configLoader.Storage.Returns(Storage);

        _dispatcher.GetJobTemplateAsync(Arg.Any<string>())
            .Returns(new JobContainerTemplate("test.azurecr.io/image:v1", 2.0, "4Gi"));
        _dispatcher.StartJobAsync(Arg.Any<string>(), Arg.Any<JobContainerTemplate>(),
                Arg.Any<TenantConfig>(), Arg.Any<IReadOnlyList<string>>(),
                Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(),
                Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns(ci => $"exec-{Guid.NewGuid():N}"[..16]);
        _claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), Arg.Any<bool>())
            .Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"));

        return new RunExecutor(
            new EntityResolver(configLoader), new TenantResolver(configLoader),
            _dispatcher, _claimWriter, configLoader, _historyWriter,
            Substitute.For<ILogger<RunExecutor>>());
    }

    [Fact]
    public async Task NoTenantSelectors_BehavesLikePre310()
    {
        // Both tenants have null entity_selector — every tenant gets the full
        // job-resolved set, no skips, no history writes for filter cases.
        var executor = BuildExecutor([
            new("t1", "tid1", "o1", null, true, 5),
            new("t2", "tid2", "o2", null, true, 5),
        ]);

        var outcome = await executor.ExecuteAsync(TierJob, TriggerTypes.Scheduled, null);

        Assert.False(outcome.SkippedEmpty);
        Assert.Equal(2, outcome.Tenants.Count);
        Assert.All(outcome.Tenants, t => Assert.Equal(DispatchResult.Dispatched, t.Result));

        // Per-tenant ClaimRecord.ResolvedEntities should match the full job set.
        await _claimWriter.Received(2).TryCreateAsync(
            Arg.Is<ClaimRecord>(c => c.ResolvedEntities.Count == 3),
            Arg.Any<bool>());

        // No filter-skipped tenants → no direct history writes.
        await _historyWriter.DidNotReceiveWithAnyArgs().WriteRunAsync(default!);
    }

    [Fact]
    public async Task TenantWithExclude_DispatchesReducedSet()
    {
        // t2 excludes exo_mailboxes — its claim should list only entra_users and
        // entra_groups. t1 keeps the full set. Containers used differ across
        // tenants too (t2 no longer hits exo-ingest), so a template fetch for
        // exo-ingest should still happen (t1 uses it).
        var executor = BuildExecutor([
            new("t1", "tid1", "o1", null, true, 5),
            new("t2", "tid2", "o2", null, true, 5,
                new EntitySelector(ExcludeEntities: ["exo_mailboxes"])),
        ]);

        var outcome = await executor.ExecuteAsync(TierJob, TriggerTypes.Scheduled, null);

        Assert.Equal(2, outcome.Tenants.Count);

        await _claimWriter.Received(1).TryCreateAsync(
            Arg.Is<ClaimRecord>(c =>
                c.TenantKey == "t1"
                && c.ResolvedEntities.Count == 3),
            Arg.Any<bool>());

        await _claimWriter.Received(1).TryCreateAsync(
            Arg.Is<ClaimRecord>(c =>
                c.TenantKey == "t2"
                && c.ResolvedEntities.Count == 2
                && !c.ResolvedEntities.Contains("exo_mailboxes")),
            Arg.Any<bool>());

        // Both containers must still get templates fetched — t1 uses both.
        await _dispatcher.Received(1).GetJobTemplateAsync("graph-ingest");
        await _dispatcher.Received(1).GetJobTemplateAsync("exo-ingest");
    }

    [Fact]
    public async Task TenantWithEmptyIntersection_WritesSkippedFilterRunRecord()
    {
        // t2's selector picks tier 2 only; the job is tier 1, so intersection is
        // empty. t2 must not claim, must not start ACA, and must get a
        // skipped_filter RunRecord written directly to history.
        var executor = BuildExecutor([
            new("t1", "tid1", "o1", null, true, 5),
            new("t2", "tid2", "o2", null, true, 5,
                new EntitySelector(IncludeTiers: [2])),
        ]);

        var outcome = await executor.ExecuteAsync(TierJob, TriggerTypes.Scheduled, null);

        Assert.Equal(2, outcome.Tenants.Count);
        Assert.Single(outcome.Tenants, t => t.Result == DispatchResult.SkippedFilter);
        Assert.Single(outcome.Tenants, t => t.Result == DispatchResult.Dispatched);

        // Skipped tenant: no claim, no ACA start.
        await _claimWriter.DidNotReceive().TryCreateAsync(
            Arg.Is<ClaimRecord>(c => c.TenantKey == "t2"), Arg.Any<bool>());
        await _dispatcher.DidNotReceive().StartJobAsync(
            Arg.Any<string>(), Arg.Any<JobContainerTemplate>(),
            Arg.Is<TenantConfig>(t => t.TenantKey == "t2"),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(),
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());

        // Skipped tenant: RunRecord written with status=skipped_filter.
        await _historyWriter.Received(1).WriteRunAsync(
            Arg.Is<RunRecord>(r =>
                r.TenantKey == "t2"
                && r.Status == RunStatuses.SkippedFilter
                && r.TaskCount == 0
                && r.EntityCount == 0
                && r.ResolvedEntities.Count == 0));
    }

    [Fact]
    public async Task AllTenantsBlocked_ReturnsFilterOutcomesOnly()
    {
        // Every tenant blocks everything in the job. Result: filter-skipped
        // outcomes for each, no dispatch work happens. SkippedEmpty stays false
        // (we addressed tenants, they just all opted out).
        var executor = BuildExecutor([
            new("t1", "tid1", "o1", null, true, 5,
                new EntitySelector(IncludeTiers: [2])),
            new("t2", "tid2", "o2", null, true, 5,
                new EntitySelector(IncludeTiers: [2])),
        ]);

        var outcome = await executor.ExecuteAsync(TierJob, TriggerTypes.Scheduled, null);

        Assert.False(outcome.SkippedEmpty);
        Assert.Equal(2, outcome.Tenants.Count);
        Assert.All(outcome.Tenants, t => Assert.Equal(DispatchResult.SkippedFilter, t.Result));

        // No claims, no ACA, no template fetches — bailed before any of that work.
        await _claimWriter.DidNotReceiveWithAnyArgs().TryCreateAsync(default!, default);
        await _dispatcher.DidNotReceiveWithAnyArgs().GetJobTemplateAsync(default!);

        // Two filter RunRecords written.
        await _historyWriter.Received(2).WriteRunAsync(
            Arg.Is<RunRecord>(r => r.Status == RunStatuses.SkippedFilter));
    }

    [Fact]
    public async Task ContainerGroupedPerTenant_DropsUnusedContainers()
    {
        // t2 excludes the only entity in exo-ingest. t2's dispatch tasks should
        // not include the exo-ingest container at all (per-tenant grouping).
        var executor = BuildExecutor([
            new("t2", "tid2", "o2", null, true, 5,
                new EntitySelector(ExcludeEntities: ["exo_mailboxes"])),
        ]);

        await executor.ExecuteAsync(TierJob, TriggerTypes.Scheduled, null);

        await _claimWriter.Received(1).TryCreateAsync(
            Arg.Is<ClaimRecord>(c =>
                c.TenantKey == "t2"
                && c.ExpectedTasks.Count == 1
                && c.ExpectedTasks[0].ContainerType == "graph-ingest"),
            Arg.Any<bool>());

        // Sole surviving tenant doesn't use exo-ingest → template fetch is skipped.
        await _dispatcher.DidNotReceive().GetJobTemplateAsync("exo-ingest");
    }
}
