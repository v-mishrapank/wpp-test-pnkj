using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IngestDispatcher.Functions.Tests;

public class RunExecutorOverlapTests
{
    private static readonly IReadOnlyList<EntityType> SampleEntities =
    [
        new("entra_users", 1, "graph-ingest"),
    ];

    private static readonly IReadOnlyList<TenantConfig> SampleTenants =
    [
        new("madev1", "tid-1", "org1.onmicrosoft.com", null, true, 5),
    ];

    private static readonly StorageConfig SampleStorage = new(
        "https://test.dfs.core.windows.net", "landing",
        new StorageAuthConfig(StorageAuthMethods.ManagedIdentity));

    private static readonly JobDefinition SampleJob = new(
        "daily-full", "Full daily ingestion", "0 0 * * *", true,
        new EntitySelector(IncludeTiers: [1]),
        new TenantSelector(TenantSelectorModes.All));

    private readonly IClaimWriter _claimWriter = Substitute.For<IClaimWriter>();
    private readonly IAcaJobClient _dispatcher = Substitute.For<IAcaJobClient>();
    private readonly IRunExecutor _executor;

    public RunExecutorOverlapTests()
    {
        var configLoader = Substitute.For<IConfigLoader>();
        configLoader.EntityRegistry.Returns(new EntityRegistryConfig(SampleEntities));
        configLoader.Tenants.Returns(new TenantsConfig(SampleTenants));
        configLoader.Storage.Returns(SampleStorage);

        var entityResolver = new EntityResolver(configLoader);
        var tenantResolver = new TenantResolver(configLoader);

        _dispatcher.GetJobTemplateAsync(Arg.Any<string>())
            .Returns(new JobContainerTemplate("test.azurecr.io/image:v1", 2.0, "4Gi"));
        _dispatcher.StartJobAsync(Arg.Any<string>(), Arg.Any<JobContainerTemplate>(), Arg.Any<TenantConfig>(),
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns("exec-001");

        _executor = new RunExecutor(
            entityResolver, tenantResolver, _dispatcher, _claimWriter,
            configLoader, Substitute.For<IRunHistoryWriter>(), Substitute.For<ILogger<RunExecutor>>());
    }

    [Fact]
    public async Task ExecuteAsync_NoBackfillWindow_DerivesNormalRunType()
    {
        _claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), force: false).Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"));

        await _executor.ExecuteAsync(SampleJob, TriggerTypes.Scheduled, null);

        await _claimWriter.Received(1).TryCreateAsync(
            Arg.Is<ClaimRecord>(r => r.RunType == RunTypes.Normal), force: false);
        await _dispatcher.Received(1).StartJobAsync(
            Arg.Any<string>(), Arg.Any<JobContainerTemplate>(), Arg.Any<TenantConfig>(),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(),
            RunTypes.Normal,
            Arg.Is<DateTimeOffset?>(d => !d.HasValue),
            Arg.Is<DateTimeOffset?>(d => !d.HasValue));
    }

    [Fact]
    public async Task ExecuteAsync_WithBackfillWindow_DerivesBackfillRunType()
    {
        _claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), force: false).Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"));

        var start = DateTimeOffset.Parse("2026-04-01T00:00:00Z");
        var end = DateTimeOffset.Parse("2026-05-01T00:00:00Z");

        await _executor.ExecuteAsync(SampleJob, TriggerTypes.Manual, "user@example.com",
            backfillStart: start, backfillEnd: end);

        await _claimWriter.Received(1).TryCreateAsync(
            Arg.Is<ClaimRecord>(r => r.RunType == RunTypes.Backfill), force: false);
        await _dispatcher.Received(1).StartJobAsync(
            Arg.Any<string>(), Arg.Any<JobContainerTemplate>(), Arg.Any<TenantConfig>(),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(),
            RunTypes.Backfill,
            Arg.Is<DateTimeOffset?>(d => d == start),
            Arg.Is<DateTimeOffset?>(d => d == end));
    }

    [Fact]
    public async Task ExecuteAsync_WhenClaimRejected_SkipsDispatch()
    {
        _claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), force: false).Returns(new TryCreateResult(CreateOutcome.RejectedExists, null));

        var outcome = await _executor.ExecuteAsync(SampleJob, TriggerTypes.Scheduled, null);

        Assert.False(outcome.SkippedEmpty);
        Assert.Single(outcome.Tenants);
        Assert.Equal(DispatchResult.SkippedOverlap, outcome.Tenants[0].Result);
        Assert.Empty(outcome.Tenants[0].Tasks);
        await _dispatcher.DidNotReceive().StartJobAsync(
            Arg.Any<string>(), Arg.Any<JobContainerTemplate>(), Arg.Any<TenantConfig>(),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
        await _claimWriter.DidNotReceive().UpdateExecutionNamesAsync(Arg.Any<ClaimRecord>());
        // Template GETs are gated on a successful claim — an all-overlap dispatch
        // (the typical case during a long backfill, every cron tick) must not pay
        // for ARM template fetches that no one will use.
        await _dispatcher.DidNotReceive().GetJobTemplateAsync(Arg.Any<string>());
    }

    [Fact]
    public async Task ExecuteAsync_WhenClaimSucceeds_Dispatches()
    {
        _claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), force: false).Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"));

        var outcome = await _executor.ExecuteAsync(SampleJob, TriggerTypes.Scheduled, null);

        Assert.False(outcome.SkippedEmpty);
        Assert.Single(outcome.Tenants);
        Assert.Equal(DispatchResult.Dispatched, outcome.Tenants[0].Result);
        Assert.Equal(1, outcome.Tenants[0].DispatchedCount);
        Assert.Equal(0, outcome.Tenants[0].FailedCount);
        Assert.Single(outcome.Tenants[0].Tasks);
        Assert.Equal("exec-001", outcome.Tenants[0].Tasks[0].AcaExecutionName);
        await _claimWriter.Received(1).TryCreateAsync(Arg.Any<ClaimRecord>(), force: false);
        await _dispatcher.Received(1).GetJobTemplateAsync("graph-ingest");
        await _dispatcher.Received(1).StartJobAsync(
            "graph-ingest",
            Arg.Is<JobContainerTemplate>(t => t.Image == "test.azurecr.io/image:v1" && t.Cpu == 2.0 && t.Memory == "4Gi"),
            Arg.Any<TenantConfig>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
        // Second claim write — execution names persisted.
        await _claimWriter.Received(1).UpdateExecutionNamesAsync(
            Arg.Is<ClaimRecord>(r => r.ExpectedTasks.All(t => t.AcaExecutionName == "exec-001")));
    }

    [Fact]
    public async Task ExecuteAsync_WithForce_PassesForceToClaim()
    {
        _claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), force: true).Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"));

        var outcome = await _executor.ExecuteAsync(SampleJob, TriggerTypes.Manual, "user@test.com",
            force: true);

        Assert.Single(outcome.Tenants);
        Assert.Equal(DispatchResult.Dispatched, outcome.Tenants[0].Result);
        await _claimWriter.Received(1).TryCreateAsync(Arg.Any<ClaimRecord>(), force: true);
        await _dispatcher.Received(1).StartJobAsync(
            "graph-ingest", Arg.Any<JobContainerTemplate>(), Arg.Any<TenantConfig>(),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
    }

    [Fact]
    public async Task ExecuteAsync_WhenTemplateFetchFails_AllTasksForContainerFailDispatch()
    {
        _claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), Arg.Any<bool>()).Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"));
        _dispatcher.GetJobTemplateAsync("graph-ingest")
            .Returns<Task<JobContainerTemplate>>(_ => throw new InvalidOperationException("ARM threw"));

        var outcome = await _executor.ExecuteAsync(SampleJob, TriggerTypes.Scheduled, null);

        Assert.False(outcome.SkippedEmpty);
        Assert.Single(outcome.Tenants);
        Assert.Equal(DispatchResult.Dispatched, outcome.Tenants[0].Result);
        Assert.Equal(0, outcome.Tenants[0].DispatchedCount);
        Assert.Equal(1, outcome.Tenants[0].FailedCount);
        Assert.Single(outcome.Tenants[0].Tasks);
        Assert.Equal(TaskStatuses.DispatchFailed, outcome.Tenants[0].Tasks[0].Status);
        Assert.NotNull(outcome.Tenants[0].Tasks[0].ErrorMessage);
        await _dispatcher.DidNotReceive().StartJobAsync(
            Arg.Any<string>(), Arg.Any<JobContainerTemplate>(), Arg.Any<TenantConfig>(),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
        // Even on dispatch failure, the second claim write fires — to persist
        // the dispatch_error so TaskStateDeriver renders dispatch_failed.
        await _claimWriter.Received(1).UpdateExecutionNamesAsync(
            Arg.Is<ClaimRecord>(r => r.ExpectedTasks.All(t => t.DispatchError != null)));
    }

    [Fact]
    public async Task ExecuteAsync_WhenNoEntitiesResolve_ReturnsSkippedEmpty()
    {
        var jobWithNoEntities = new JobDefinition(
            "empty", "empty", "0 0 * * *", true,
            new EntitySelector(IncludeTiers: [5]),  // no tier-5 entities in SampleEntities
            new TenantSelector(TenantSelectorModes.All));

        var outcome = await _executor.ExecuteAsync(jobWithNoEntities, TriggerTypes.Scheduled, null);

        Assert.True(outcome.SkippedEmpty);
        Assert.Empty(outcome.Tenants);
        await _claimWriter.DidNotReceive().TryCreateAsync(Arg.Any<ClaimRecord>(), Arg.Any<bool>());
    }

    [Fact]
    public async Task ExecuteAsync_WhenNoTenantsResolve_ReturnsSkippedEmpty()
    {
        // "specific" mode with no tenant_keys — resolver returns empty list.
        var jobWithNoTenants = new JobDefinition(
            "empty", "empty", "0 0 * * *", true,
            new EntitySelector(IncludeTiers: [1]),
            new TenantSelector(TenantSelectorModes.Specific, TenantKeys: ["nonexistent"]));

        var outcome = await _executor.ExecuteAsync(jobWithNoTenants, TriggerTypes.Scheduled, null);

        Assert.True(outcome.SkippedEmpty);
        Assert.Empty(outcome.Tenants);
        await _claimWriter.DidNotReceive().TryCreateAsync(Arg.Any<ClaimRecord>(), Arg.Any<bool>());
    }

    [Fact]
    public async Task ExecuteAsync_FetchesTemplateOncePerDistinctContainer()
    {
        var entities = new List<EntityType>
        {
            new("entra_users", 1, "graph-ingest"),
            new("exo_mailboxes", 1, "exo-ingest"),
        };
        var configLoader = Substitute.For<IConfigLoader>();
        configLoader.EntityRegistry.Returns(new EntityRegistryConfig(entities));
        configLoader.Tenants.Returns(new TenantsConfig(SampleTenants));
        configLoader.Storage.Returns(SampleStorage);

        var dispatcher = Substitute.For<IAcaJobClient>();
        dispatcher.GetJobTemplateAsync(Arg.Any<string>())
            .Returns(new JobContainerTemplate("img:v1", 2.0, "4Gi"));
        dispatcher.StartJobAsync(Arg.Any<string>(), Arg.Any<JobContainerTemplate>(),
                Arg.Any<TenantConfig>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns("exec-xyz");

        var claimWriter = Substitute.For<IClaimWriter>();
        claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), Arg.Any<bool>()).Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"));

        var executor = new RunExecutor(
            new EntityResolver(configLoader), new TenantResolver(configLoader),
            dispatcher, claimWriter, configLoader, Substitute.For<IRunHistoryWriter>(), Substitute.For<ILogger<RunExecutor>>());

        var outcome = await executor.ExecuteAsync(SampleJob, TriggerTypes.Scheduled, null);

        Assert.Single(outcome.Tenants);
        Assert.Equal(DispatchResult.Dispatched, outcome.Tenants[0].Result);
        Assert.Equal(2, outcome.Tenants[0].Tasks.Count);
        await dispatcher.Received(1).GetJobTemplateAsync("graph-ingest");
        await dispatcher.Received(1).GetJobTemplateAsync("exo-ingest");
    }

    [Fact]
    public async Task ExecuteAsync_WhenSomeDispatchesFail_ReturnsMixedCounts()
    {
        var twoTenants = new List<TenantConfig>
        {
            new("t1", "tid-1", "org1.onmicrosoft.com", null, true, 5),
            new("t2", "tid-2", "org2.onmicrosoft.com", null, true, 5),
        };
        var configLoader = Substitute.For<IConfigLoader>();
        configLoader.EntityRegistry.Returns(new EntityRegistryConfig(SampleEntities));
        configLoader.Tenants.Returns(new TenantsConfig(twoTenants));
        configLoader.Storage.Returns(SampleStorage);

        var dispatcher = Substitute.For<IAcaJobClient>();
        dispatcher.GetJobTemplateAsync(Arg.Any<string>())
            .Returns(new JobContainerTemplate("test.azurecr.io/image:v1", 2.0, "4Gi"));
        dispatcher.StartJobAsync(Arg.Any<string>(), Arg.Any<JobContainerTemplate>(),
                Arg.Is<TenantConfig>(t => t.TenantKey == "t1"),
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns("exec-001");
        dispatcher.StartJobAsync(Arg.Any<string>(), Arg.Any<JobContainerTemplate>(),
                Arg.Is<TenantConfig>(t => t.TenantKey == "t2"),
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns<Task<string>>(_ => throw new InvalidOperationException("tenant t2 failed"));

        var claimWriter = Substitute.For<IClaimWriter>();
        claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), Arg.Any<bool>()).Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"));

        var executor = new RunExecutor(
            new EntityResolver(configLoader), new TenantResolver(configLoader),
            dispatcher, claimWriter, configLoader, Substitute.For<IRunHistoryWriter>(), Substitute.For<ILogger<RunExecutor>>());

        var outcome = await executor.ExecuteAsync(SampleJob, TriggerTypes.Scheduled, null);

        Assert.False(outcome.SkippedEmpty);
        Assert.Equal(2, outcome.Tenants.Count);
        var t1 = outcome.Tenants.Single(t => t.TenantKey == "t1");
        var t2 = outcome.Tenants.Single(t => t.TenantKey == "t2");
        Assert.Equal(DispatchResult.Dispatched, t1.Result);
        Assert.Equal(1, t1.DispatchedCount);
        Assert.Equal(0, t1.FailedCount);
        Assert.Equal(DispatchResult.Dispatched, t2.Result);
        Assert.Equal(0, t2.DispatchedCount);
        Assert.Equal(1, t2.FailedCount);
        Assert.Equal(TaskStatuses.DispatchFailed, t2.Tasks[0].Status);
    }

    [Fact]
    public async Task ExecuteAsync_OneTenantOverlap_OthersDispatch()
    {
        // The whole point of #301: a long-running run for tenant A must not
        // block a fresh trigger for tenant B. Per-tenant claim → tenant A
        // gets SkippedOverlap, tenant B dispatches normally.
        var twoTenants = new List<TenantConfig>
        {
            new("t1", "tid-1", "org1.onmicrosoft.com", null, true, 5),
            new("t2", "tid-2", "org2.onmicrosoft.com", null, true, 5),
        };
        var configLoader = Substitute.For<IConfigLoader>();
        configLoader.EntityRegistry.Returns(new EntityRegistryConfig(SampleEntities));
        configLoader.Tenants.Returns(new TenantsConfig(twoTenants));
        configLoader.Storage.Returns(SampleStorage);

        var dispatcher = Substitute.For<IAcaJobClient>();
        dispatcher.GetJobTemplateAsync(Arg.Any<string>())
            .Returns(new JobContainerTemplate("img:v1", 2.0, "4Gi"));
        dispatcher.StartJobAsync(Arg.Any<string>(), Arg.Any<JobContainerTemplate>(),
                Arg.Any<TenantConfig>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns("exec-002");

        var claimWriter = Substitute.For<IClaimWriter>();
        claimWriter.TryCreateAsync(Arg.Is<ClaimRecord>(r => r.TenantKey == "t1"), Arg.Any<bool>()).Returns(new TryCreateResult(CreateOutcome.RejectedExists, null));
        claimWriter.TryCreateAsync(Arg.Is<ClaimRecord>(r => r.TenantKey == "t2"), Arg.Any<bool>()).Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"));

        var executor = new RunExecutor(
            new EntityResolver(configLoader), new TenantResolver(configLoader),
            dispatcher, claimWriter, configLoader, Substitute.For<IRunHistoryWriter>(), Substitute.For<ILogger<RunExecutor>>());

        var outcome = await executor.ExecuteAsync(SampleJob, TriggerTypes.Scheduled, null);

        Assert.Equal(2, outcome.Tenants.Count);
        var t1 = outcome.Tenants.Single(t => t.TenantKey == "t1");
        var t2 = outcome.Tenants.Single(t => t.TenantKey == "t2");
        Assert.Equal(DispatchResult.SkippedOverlap, t1.Result);
        Assert.Empty(t1.Tasks);
        Assert.Equal(DispatchResult.Dispatched, t2.Result);
        Assert.Equal(1, t2.DispatchedCount);
        await dispatcher.Received(1).StartJobAsync(
            Arg.Any<string>(), Arg.Any<JobContainerTemplate>(),
            Arg.Is<TenantConfig>(t => t.TenantKey == "t2"),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
        await dispatcher.DidNotReceive().StartJobAsync(
            Arg.Any<string>(), Arg.Any<JobContainerTemplate>(),
            Arg.Is<TenantConfig>(t => t.TenantKey == "t1"),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
    }

    [Fact]
    public async Task ExecuteAsync_PerTenantRunIdsAreDistinct()
    {
        var twoTenants = new List<TenantConfig>
        {
            new("t1", "tid-1", "org1.onmicrosoft.com", null, true, 5),
            new("t2", "tid-2", "org2.onmicrosoft.com", null, true, 5),
        };
        var configLoader = Substitute.For<IConfigLoader>();
        configLoader.EntityRegistry.Returns(new EntityRegistryConfig(SampleEntities));
        configLoader.Tenants.Returns(new TenantsConfig(twoTenants));
        configLoader.Storage.Returns(SampleStorage);

        var dispatcher = Substitute.For<IAcaJobClient>();
        dispatcher.GetJobTemplateAsync(Arg.Any<string>())
            .Returns(new JobContainerTemplate("img:v1", 2.0, "4Gi"));
        dispatcher.StartJobAsync(Arg.Any<string>(), Arg.Any<JobContainerTemplate>(),
                Arg.Any<TenantConfig>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns("exec-z");

        var claimWriter = Substitute.For<IClaimWriter>();
        claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), Arg.Any<bool>()).Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"));

        var executor = new RunExecutor(
            new EntityResolver(configLoader), new TenantResolver(configLoader),
            dispatcher, claimWriter, configLoader, Substitute.For<IRunHistoryWriter>(), Substitute.For<ILogger<RunExecutor>>());

        var outcome = await executor.ExecuteAsync(SampleJob, TriggerTypes.Scheduled, null);

        Assert.Equal(2, outcome.Tenants.Count);
        var ids = outcome.Tenants.Select(t => t.RunId).ToList();
        Assert.Equal(2, ids.Distinct().Count());
        Assert.All(ids, id => Assert.False(string.IsNullOrEmpty(id)));
    }

    [Fact]
    public async Task ExecuteAsync_ThreadsCreateETagToSecondWrite()
    {
        // The ETag returned by TryCreateAsync must reach UpdateExecutionNamesAsync
        // via ClaimRecord.ETag — otherwise the second write falls back to
        // unconditional overwrite and the race the fix closed re-opens.
        _claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), Arg.Any<bool>())
            .Returns(new TryCreateResult(CreateOutcome.Created, "etag-from-create"));

        await _executor.ExecuteAsync(SampleJob, TriggerTypes.Scheduled, null);

        await _claimWriter.Received(1).UpdateExecutionNamesAsync(
            Arg.Is<ClaimRecord>(r => r.ETag == "etag-from-create"));
    }

    [Fact]
    public async Task ExecuteAsync_WhenSecondWriteConcurrent_OutcomeIsClaimConcurrentlyModified()
    {
        // ACA executions DID start (so dispatchedCount stays > 0); only the
        // claim's tracking record is stale because a concurrent forced
        // dispatcher overwrote it. The TaskOutcomes still reflect the real
        // exec names this dispatcher kicked off — those are derived from
        // dispatchTasks, not from the claim.
        _claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), Arg.Any<bool>())
            .Returns(new TryCreateResult(CreateOutcome.Created, "etag-x"));
        _claimWriter.UpdateExecutionNamesAsync(Arg.Any<ClaimRecord>())
            .Returns(UpdateResult.ConcurrentlyModified);

        var outcome = await _executor.ExecuteAsync(SampleJob, TriggerTypes.Manual, "user@test.com",
            force: true);

        Assert.Single(outcome.Tenants);
        Assert.Equal(DispatchResult.ClaimConcurrentlyModified, outcome.Tenants[0].Result);
        Assert.Equal(1, outcome.Tenants[0].DispatchedCount);
        await _dispatcher.Received(1).StartJobAsync(
            Arg.Any<string>(), Arg.Any<JobContainerTemplate>(), Arg.Any<TenantConfig>(),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
    }

    [Fact]
    public async Task ExecuteAsync_WhenForceLosesFirstWriteRace_OutcomeIsSkippedConcurrentNoStartJob()
    {
        // RejectedConcurrent on the first write means another forced dispatcher
        // got the slot. No ACA executions should be started. The skip must
        // surface as SkippedConcurrent (not SkippedOverlap) so operators can
        // distinguish "you raced yourself" from "you forgot --force".
        _claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), force: true)
            .Returns(new TryCreateResult(CreateOutcome.RejectedConcurrent, null));

        var outcome = await _executor.ExecuteAsync(SampleJob, TriggerTypes.Manual, "user@test.com",
            force: true);

        Assert.Single(outcome.Tenants);
        Assert.Equal(DispatchResult.SkippedConcurrent, outcome.Tenants[0].Result);
        Assert.Empty(outcome.Tenants[0].Tasks);
        await _dispatcher.DidNotReceive().StartJobAsync(
            Arg.Any<string>(), Arg.Any<JobContainerTemplate>(), Arg.Any<TenantConfig>(),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
        await _claimWriter.DidNotReceive().UpdateExecutionNamesAsync(Arg.Any<ClaimRecord>());
    }
}
