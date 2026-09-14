using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IngestDispatcher.Functions.Tests;

// Issue #570: binding-provided CancellationTokens flow through the long-running
// service loops and are observed only at safe points — between per-claim /
// per-tenant / per-intent units, never mid-finalization or mid-ARM-fanout.
public class CancellationPropagationTests
{
    private static ClaimRecord ResolvableClaim(string runId, string tenantKey) =>
        new()
        {
            RunId = runId,
            JobName = "daily",
            TenantKey = tenantKey,
            RunType = RunTypes.Normal,
            TriggerType = TriggerTypes.Scheduled,
            StartedAt = DateTimeOffset.UtcNow.AddMinutes(-5),
            ResolvedTimeoutSeconds = 24 * 3600,
            ResolvedEntities = ["entra_users"],
            ExpectedTasks = new List<ExpectedTask>
            {
                new()
                {
                    ContainerType = "caj-graph",
                    AcaExecutionName = "exec-1",
                    Entities = ["entra_users"],
                },
            },
            ETag = "etag-1",
        };

    private static RunTracker BuildTracker(
        IClaimReader claimReader,
        IClaimWriter claimWriter,
        IAcaJobClient aca,
        IRunHistoryWriter history,
        IRunStateReader runState,
        IRunCanceller canceller) =>
        new(claimReader, claimWriter, aca, history, runState,
            new HeartbeatCache(), canceller,
            Substitute.For<ILogger<RunTracker>>());

    [Fact]
    public async Task CheckActiveRuns_CancelBetweenClaims_FinalizesFirstNeverAbortsMidFinalization()
    {
        // Two resolvable claims. Cancellation fires during the FIRST claim's
        // finalization (right after its run-history write). The finalization
        // section takes no token, so claim A must still finalize fully (tasks
        // write + claim delete). Claim B must never be touched — the loop
        // observes the cancel at the per-claim boundary and throws.
        var claimA = ResolvableClaim("rid-A", "tenantA");
        var claimB = ResolvableClaim("rid-B", "tenantB");

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claimA, claimB });

        var aca = Substitute.For<IAcaJobClient>();
        aca.GetExecutionStatusAsync("caj-graph", "exec-1")
            .Returns(AcaExecutionStatuses.Succeeded);

        var runState = Substitute.For<IRunStateReader>();
        runState.TryReadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>())
            .Returns((HeartbeatPayload?)null);

        var canceller = Substitute.For<IRunCanceller>();
        canceller.TryReadIntentAsync(Arg.Any<string>()).Returns((CancelIntent?)null);

        using var cts = new CancellationTokenSource();
        var history = Substitute.For<IRunHistoryWriter>();
        // Cancel the moment claim A's run-history write lands — i.e. partway
        // through claim A's finalization.
        history.WriteRunAsync(Arg.Any<RunRecord>())
            .Returns(WriteResult.Written)
            .AndDoes(_ => cts.Cancel());
        history.WriteTasksAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<TaskRecord>>())
            .Returns(WriteResult.Written);
        var claimWriter = Substitute.For<IClaimWriter>();

        var tracker = BuildTracker(claimReader, claimWriter, aca, history, runState, canceller);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => tracker.CheckActiveRunsAsync(cts.Token));

        // Claim A finalized end-to-end despite the cancel arriving mid-finalization.
        await history.Received(1).WriteRunAsync(Arg.Is<RunRecord>(r => r.RunId == "rid-A"));
        await history.Received(1).WriteTasksAsync("rid-A", Arg.Any<IReadOnlyList<TaskRecord>>());
        await claimWriter.Received(1).DeleteAsync("daily", "tenantA", RunTypes.Normal);

        // Claim B was never processed — cancel was observed at the claim boundary.
        await history.DidNotReceive().WriteRunAsync(Arg.Is<RunRecord>(r => r.RunId == "rid-B"));
        await claimWriter.DidNotReceive().DeleteAsync("daily", "tenantB", RunTypes.Normal);
    }

    [Fact]
    public async Task CheckActiveRuns_AlreadyCancelled_FinalizesNothing()
    {
        var claim = ResolvableClaim("rid-A", "tenantA");

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });

        var aca = Substitute.For<IAcaJobClient>();
        aca.GetExecutionStatusAsync("caj-graph", "exec-1")
            .Returns(AcaExecutionStatuses.Succeeded);

        var runState = Substitute.For<IRunStateReader>();
        runState.TryReadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>())
            .Returns((HeartbeatPayload?)null);

        var canceller = Substitute.For<IRunCanceller>();
        var history = Substitute.For<IRunHistoryWriter>();
        var claimWriter = Substitute.For<IClaimWriter>();

        var tracker = BuildTracker(claimReader, claimWriter, aca, history, runState, canceller);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => tracker.CheckActiveRunsAsync(cts.Token));

        await history.DidNotReceiveWithAnyArgs().WriteRunAsync(default!);
        await claimWriter.DidNotReceiveWithAnyArgs().DeleteAsync(default!, default!, default!);
    }

    [Fact]
    public async Task Reconcile_AlreadyCancelled_SubmitsNoStops()
    {
        var claim = ResolvableClaim("rid-A", "tenantA");
        // Started long enough ago to be past the timeout so Pass A would
        // otherwise create a cancel intent — proving the token short-circuits
        // real work, not an empty loop.
        claim = claim with
        {
            StartedAt = DateTimeOffset.UtcNow.AddHours(-25),
        };

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });

        var intentStore = Substitute.For<ICancelIntentStore>();
        var aca = Substitute.For<IAcaJobClient>();

        var canceller = new RunCanceller(
            claimReader, intentStore, aca, Substitute.For<ILogger<RunCanceller>>());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => canceller.ReconcileAsync(cts.Token));

        await intentStore.DidNotReceiveWithAnyArgs().TryCreateAsync(default!);
        await aca.DidNotReceiveWithAnyArgs()
            .CancelExecutionAsync(default!, default!, default!);
    }

    [Fact]
    public async Task RequestCancel_AlreadyCancelled_TouchesNothing()
    {
        var claimReader = Substitute.For<IClaimReader>();
        var intentStore = Substitute.For<ICancelIntentStore>();
        var aca = Substitute.For<IAcaJobClient>();

        var canceller = new RunCanceller(
            claimReader, intentStore, aca, Substitute.For<ILogger<RunCanceller>>());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => canceller.RequestCancelAsync("rid-A", CancelTriggers.Manual, null, cts.Token));

        // Cancel observed at entry, before any claim lookup or intent create.
        await claimReader.DidNotReceiveWithAnyArgs().TryFindByRunIdAsync(default!);
        await intentStore.DidNotReceiveWithAnyArgs().TryCreateAsync(default!);
    }

    [Fact]
    public async Task Execute_AlreadyCancelled_CreatesNoClaimsOrExecutions()
    {
        var configLoader = Substitute.For<IConfigLoader>();
        configLoader.EntityRegistry.Returns(new EntityRegistryConfig(
            [new("entra_users", 1, "graph-ingest")]));
        configLoader.Tenants.Returns(new TenantsConfig(
            [new("madev1", "tid-1", "org1.onmicrosoft.com", null, true, 5)]));
        configLoader.Storage.Returns(new StorageConfig(
            "https://test.dfs.core.windows.net", "landing",
            new StorageAuthConfig(StorageAuthMethods.ManagedIdentity)));

        var claimWriter = Substitute.For<IClaimWriter>();
        var dispatcher = Substitute.For<IAcaJobClient>();

        var executor = new RunExecutor(
            new EntityResolver(configLoader), new TenantResolver(configLoader),
            dispatcher, claimWriter, configLoader,
            Substitute.For<IRunHistoryWriter>(), Substitute.For<ILogger<RunExecutor>>());

        var job = new JobDefinition(
            "daily-full", "Full daily ingestion", "0 0 * * *", true,
            new EntitySelector(IncludeTiers: [1]),
            new TenantSelector(TenantSelectorModes.All));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => executor.ExecuteAsync(job, TriggerTypes.Scheduled, null, ct: cts.Token));

        // Cancel observed at the pre-claim boundary — nothing claimed,
        // no ACA execution started, no template fetched.
        await claimWriter.DidNotReceiveWithAnyArgs().TryCreateAsync(default!, default);
        await dispatcher.DidNotReceiveWithAnyArgs().StartJobAsync(
            default!, default!, default!, default!, default!, default!, default!, default, default);
        await dispatcher.DidNotReceiveWithAnyArgs().GetJobTemplateAsync(default!);
    }

    [Fact]
    public async Task Execute_CancelAfterClaim_DispatchesAndUpdatesClaim()
    {
        var configLoader = Substitute.For<IConfigLoader>();
        configLoader.EntityRegistry.Returns(new EntityRegistryConfig(
            [new("entra_users", 1, "graph-ingest")]));
        configLoader.Tenants.Returns(new TenantsConfig(
            [new("madev1", "tid-1", "org1.onmicrosoft.com", null, true, 5)]));
        configLoader.Storage.Returns(new StorageConfig(
            "https://test.dfs.core.windows.net", "landing",
            new StorageAuthConfig(StorageAuthMethods.ManagedIdentity)));

        using var cts = new CancellationTokenSource();
        var claimWriter = Substitute.For<IClaimWriter>();
        claimWriter.TryCreateAsync(Arg.Any<ClaimRecord>(), force: false)
            .Returns(new TryCreateResult(CreateOutcome.Created, "etag-fake"))
            .AndDoes(_ => cts.Cancel());

        var dispatcher = Substitute.For<IAcaJobClient>();
        dispatcher.GetJobTemplateAsync("graph-ingest")
            .Returns(new JobContainerTemplate("test.azurecr.io/image:v1", 2.0, "4Gi"));
        dispatcher.StartJobAsync("graph-ingest", Arg.Any<JobContainerTemplate>(),
                Arg.Any<TenantConfig>(), Arg.Any<IReadOnlyList<string>>(),
                Arg.Any<StorageConfig>(), Arg.Any<string>(), RunTypes.Normal,
                Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>())
            .Returns("exec-001");

        var executor = new RunExecutor(
            new EntityResolver(configLoader), new TenantResolver(configLoader),
            dispatcher, claimWriter, configLoader,
            Substitute.For<IRunHistoryWriter>(), Substitute.For<ILogger<RunExecutor>>());
        var job = new JobDefinition(
            "daily-full", "Full daily ingestion", "0 0 * * *", true,
            new EntitySelector(IncludeTiers: [1]),
            new TenantSelector(TenantSelectorModes.All));

        var outcome = await executor.ExecuteAsync(
            job, TriggerTypes.Scheduled, null, ct: cts.Token);

        Assert.Equal(DispatchResult.Dispatched, Assert.Single(outcome.Tenants).Result);
        await dispatcher.Received(1).StartJobAsync(
            "graph-ingest", Arg.Any<JobContainerTemplate>(), Arg.Any<TenantConfig>(),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<StorageConfig>(), Arg.Any<string>(),
            RunTypes.Normal, Arg.Any<DateTimeOffset?>(), Arg.Any<DateTimeOffset?>());
        await claimWriter.Received(1).UpdateExecutionNamesAsync(
            Arg.Is<ClaimRecord>(claim =>
                claim.ETag == "etag-fake" &&
                Assert.Single(claim.ExpectedTasks).AcaExecutionName == "exec-001"));
    }
}
