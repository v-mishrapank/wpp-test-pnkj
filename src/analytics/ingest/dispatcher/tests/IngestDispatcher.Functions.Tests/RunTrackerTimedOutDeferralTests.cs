using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IngestDispatcher.Functions.Tests;

// The split-brain risk that #263 set out to fix: previously the dispatcher's
// wall-clock deadline could finalize a run (delete the claim, release the
// overlap slot) while the ACA replica was still writing. After the cancel
// state machine landed, the timeout-fire path creates a cancel intent and
// drives ACA to Stopped — but only if RunTracker.CheckClaimAsync DEFERS
// finalization until ACA actually reports terminal. These tests pin that
// deferral.
public class RunTrackerTimedOutDeferralTests
{
    private static ClaimRecord ExpiredClaim(string acaExecution = "exec-1") =>
        new()
        {
            RunId = "rid-1",
            JobName = "daily",
            TenantKey = "madev1",
            RunType = RunTypes.Normal,
            TriggerType = TriggerTypes.Scheduled,
            // Started 25h ago, 24h resolved timeout → past deadline.
            StartedAt = DateTimeOffset.UtcNow.AddHours(-25),
            ResolvedTimeoutSeconds = 24 * 3600,
            ResolvedEntities = ["entra_users"],
            ExpectedTasks = new List<ExpectedTask>
            {
                new()
                {
                    ContainerType = "caj-graph",
                    AcaExecutionName = acaExecution,
                    Entities = ["entra_users"],
                },
            },
            ETag = "etag-1",
        };

    private static RunTracker Build(
        IClaimReader claimReader,
        IClaimWriter claimWriter,
        IAcaJobClient aca,
        IRunHistoryWriter history,
        IRunStateReader runState,
        IRunCanceller canceller,
        IHeartbeatCache? cache = null) =>
        new(claimReader, claimWriter, aca, history, runState,
            cache ?? new HeartbeatCache(), canceller,
            Substitute.For<ILogger<RunTracker>>());

    [Fact]
    public async Task TimedOut_AcaStillRunning_DoesNotFinalize()
    {
        // The whole point of #263: even past the wall-clock deadline, if
        // ACA still reports non-terminal we must defer. Otherwise we'd
        // delete the claim and free the overlap slot while the replica
        // keeps writing.
        var claim = ExpiredClaim();

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });

        var aca = Substitute.For<IAcaJobClient>();
        aca.GetExecutionStatusAsync("caj-graph", "exec-1").Returns("Running");

        var runState = Substitute.For<IRunStateReader>();
        runState.TryReadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>())
            .Returns((HeartbeatPayload?)null);

        var canceller = Substitute.For<IRunCanceller>();
        // Intent already exists (reconciler created it earlier this tick).
        canceller.TryReadIntentAsync(claim.RunId).Returns(new CancelIntent
        {
            RunId = claim.RunId,
            JobName = claim.JobName,
            TenantKey = claim.TenantKey,
            RunType = claim.RunType,
            CancelState = CancelStates.CancelSubmitted,
            CancelTrigger = CancelTriggers.Timeout,
            CancelRequestedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
        });

        var history = Substitute.For<IRunHistoryWriter>();
        var claimWriter = Substitute.For<IClaimWriter>();

        var tracker = Build(claimReader, claimWriter, aca, history, runState, canceller);
        await tracker.CheckActiveRunsAsync();

        // No JSONL write, no claim delete. Run is still in flight from the
        // dispatcher's point of view; the cancel state machine will drive
        // ACA to Stopped and a later tick will finalize cleanly.
        await history.DidNotReceiveWithAnyArgs().WriteRunAsync(default!);
        await history.DidNotReceiveWithAnyArgs().WriteTasksAsync(default!, default!);
        await claimWriter.DidNotReceiveWithAnyArgs().DeleteAsync(default!, default!, default!);
    }

    [Fact]
    public async Task TimedOut_AcaStopped_WithTimeoutIntent_FinalizesAsTimeout()
    {
        // Once ACA reports Stopped (because the cancel intent's submitted
        // stop took effect), finalization should fire and map the task to
        // Timeout (not Failed, which is what the legacy ACA-Stopped mapper
        // returns absent an intent).
        var claim = ExpiredClaim();

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });

        var aca = Substitute.For<IAcaJobClient>();
        aca.GetExecutionStatusAsync("caj-graph", "exec-1")
            .Returns(AcaExecutionStatuses.Stopped);

        var runState = Substitute.For<IRunStateReader>();
        runState.TryReadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>())
            .Returns((HeartbeatPayload?)null);

        var canceller = Substitute.For<IRunCanceller>();
        canceller.TryReadIntentAsync(claim.RunId).Returns(new CancelIntent
        {
            RunId = claim.RunId,
            JobName = claim.JobName,
            TenantKey = claim.TenantKey,
            RunType = claim.RunType,
            CancelState = CancelStates.CancelSubmitted,
            CancelTrigger = CancelTriggers.Timeout,
            CancelRequestedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
        });

        var history = Substitute.For<IRunHistoryWriter>();
        history.WriteRunAsync(Arg.Any<RunRecord>()).Returns(WriteResult.Written);
        history.WriteTasksAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<TaskRecord>>())
            .Returns(WriteResult.Written);
        var claimWriter = Substitute.For<IClaimWriter>();

        var tracker = Build(claimReader, claimWriter, aca, history, runState, canceller);
        await tracker.CheckActiveRunsAsync();

        await history.Received(1).WriteTasksAsync(
            claim.RunId,
            Arg.Is<IReadOnlyList<TaskRecord>>(tasks =>
                tasks.Count == 1 && tasks[0].Status == TaskStatuses.TimedOut));
        // #382: run-level rollup must echo the timeout, not collapse to Failed.
        await history.Received(1).WriteRunAsync(
            Arg.Is<RunRecord>(r => r.Status == RunStatuses.TimedOut));
        await claimWriter.Received(1).DeleteAsync(claim.JobName, claim.TenantKey, claim.RunType);
        await canceller.Received(1).DeleteIntentAsync(claim.RunId);
    }

    [Fact]
    public async Task TimedOut_AcaStopped_WithManualIntent_FinalizesAsCancelled()
    {
        // Manual cancel trigger → JSONL records Cancelled, not Timeout.
        var claim = ExpiredClaim();

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });

        var aca = Substitute.For<IAcaJobClient>();
        aca.GetExecutionStatusAsync("caj-graph", "exec-1")
            .Returns(AcaExecutionStatuses.Stopped);

        var runState = Substitute.For<IRunStateReader>();
        runState.TryReadAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>())
            .Returns((HeartbeatPayload?)null);

        var canceller = Substitute.For<IRunCanceller>();
        canceller.TryReadIntentAsync(claim.RunId).Returns(new CancelIntent
        {
            RunId = claim.RunId,
            JobName = claim.JobName,
            TenantKey = claim.TenantKey,
            RunType = claim.RunType,
            CancelState = CancelStates.CancelSubmitted,
            CancelTrigger = CancelTriggers.Manual,
            CancelReason = "ops",
            CancelRequestedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
        });

        var history = Substitute.For<IRunHistoryWriter>();
        history.WriteRunAsync(Arg.Any<RunRecord>()).Returns(WriteResult.Written);
        history.WriteTasksAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<TaskRecord>>())
            .Returns(WriteResult.Written);
        var claimWriter = Substitute.For<IClaimWriter>();

        var tracker = Build(claimReader, claimWriter, aca, history, runState, canceller);
        await tracker.CheckActiveRunsAsync();

        await history.Received(1).WriteTasksAsync(
            claim.RunId,
            Arg.Is<IReadOnlyList<TaskRecord>>(tasks =>
                tasks.Count == 1 && tasks[0].Status == TaskStatuses.Cancelled));
        // #382: run-level rollup echoes the cancel, doesn't collapse to Failed.
        await history.Received(1).WriteRunAsync(
            Arg.Is<RunRecord>(r => r.Status == RunStatuses.Cancelled));
    }

    private static HeartbeatPayload BlobRunning(
        string containerType, string runId, string tenantKey, DateTimeOffset lastBeat) =>
        new(SchemaVersion: 4, RunId: runId, TenantKey: tenantKey,
            ContainerType: containerType,
            ContainerStartedAt: lastBeat.AddMinutes(-1),
            LastHeartbeatAt: lastBeat,
            RunStatus: BlobRunStatuses.Running,
            Entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Running,
                    RecordCount: 100)
            ],
            PrerequisiteEntities: null);

    [Fact]
    public async Task BlobRunning_Fresh_AcaTerminal_FinalizesViaAcaFallback()
    {
        // Post-#457: the former 10-min staleness gate is removed. When the
        // blob is 'running' but ACA reports terminal, RunTracker immediately
        // uses the ACA signal to finalize — freshness no longer causes
        // deferral. This eliminates delayed finalization on fast cancels and
        // crashes.
        var claim = new ClaimRecord
        {
            RunId = "rid-fresh-defer",
            JobName = "daily",
            TenantKey = "madev1",
            RunType = RunTypes.Normal,
            TriggerType = TriggerTypes.Scheduled,
            // Not yet timed out.
            StartedAt = DateTimeOffset.UtcNow.AddMinutes(-5),
            ResolvedTimeoutSeconds = 24 * 3600,
            ResolvedEntities = ["entra_users"],
            ExpectedTasks = new List<ExpectedTask>
            {
                new() { ContainerType = "caj-graph", AcaExecutionName = "exec-1", Entities = ["entra_users"] }
            },
            ETag = "etag-1",
        };

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });

        var aca = Substitute.For<IAcaJobClient>();
        // ACA reports terminal while blob still says running.
        aca.GetExecutionStatusAsync("caj-graph", "exec-1").Returns(AcaExecutionStatuses.Succeeded);

        var runState = Substitute.For<IRunStateReader>();
        // Fresh: 2s old — but staleness no longer matters for deferral.
        runState.TryReadAsync(claim.RunId, claim.TenantKey, "caj-graph")
            .Returns(BlobRunning("caj-graph", claim.RunId, claim.TenantKey, DateTimeOffset.UtcNow.AddSeconds(-2)));

        var canceller = Substitute.For<IRunCanceller>();
        canceller.TryReadIntentAsync(claim.RunId).Returns((CancelIntent?)null);

        var history = Substitute.For<IRunHistoryWriter>();
        history.WriteRunAsync(Arg.Any<RunRecord>()).Returns(WriteResult.Written);
        history.WriteTasksAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<TaskRecord>>())
            .Returns(WriteResult.Written);
        var claimWriter = Substitute.For<IClaimWriter>();

        var tracker = Build(claimReader, claimWriter, aca, history, runState, canceller);
        await tracker.CheckActiveRunsAsync();

        // ACA-fallback fires immediately: task is finalized as Succeeded,
        // claim is deleted.
        await history.Received(1).WriteTasksAsync(
            claim.RunId,
            Arg.Is<IReadOnlyList<TaskRecord>>(tasks =>
                tasks.Count == 1 && tasks[0].Status == TaskStatuses.Succeeded));
        await claimWriter.Received(1).DeleteAsync(claim.JobName, claim.TenantKey, claim.RunType);
    }

    [Fact]
    public async Task BlobRunning_Stale_AcaTerminal_FallsBackToAca()
    {
        // The flip side: blob says 'running' but its last_heartbeat_at is
        // older than BlobStaleForFallbackThreshold. Stop-Heartbeat's full
        // retry window has elapsed without landing terminal state — the
        // blob carries no useful signal and ACA exit code is the only
        // signal we have.
        var claim = new ClaimRecord
        {
            RunId = "rid-stale-fallback",
            JobName = "daily",
            TenantKey = "madev1",
            RunType = RunTypes.Normal,
            TriggerType = TriggerTypes.Scheduled,
            StartedAt = DateTimeOffset.UtcNow.AddMinutes(-20),
            ResolvedTimeoutSeconds = 24 * 3600,
            ResolvedEntities = ["entra_users"],
            ExpectedTasks = new List<ExpectedTask>
            {
                new() { ContainerType = "caj-graph", AcaExecutionName = "exec-1", Entities = ["entra_users"] }
            },
            ETag = "etag-1",
        };

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });

        var aca = Substitute.For<IAcaJobClient>();
        aca.GetExecutionStatusAsync("caj-graph", "exec-1").Returns(AcaExecutionStatuses.Failed);

        var runState = Substitute.For<IRunStateReader>();
        // Stale: 15 minutes old, well past the 10 min threshold.
        runState.TryReadAsync(claim.RunId, claim.TenantKey, "caj-graph")
            .Returns(BlobRunning("caj-graph", claim.RunId, claim.TenantKey, DateTimeOffset.UtcNow.AddMinutes(-15)));

        var canceller = Substitute.For<IRunCanceller>();
        canceller.TryReadIntentAsync(claim.RunId).Returns((CancelIntent?)null);

        var history = Substitute.For<IRunHistoryWriter>();
        history.WriteRunAsync(Arg.Any<RunRecord>()).Returns(WriteResult.Written);
        history.WriteTasksAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<TaskRecord>>())
            .Returns(WriteResult.Written);
        var claimWriter = Substitute.For<IClaimWriter>();

        var tracker = Build(claimReader, claimWriter, aca, history, runState, canceller);
        await tracker.CheckActiveRunsAsync();

        // ACA-fallback fired: task.Status is Failed with the no-blob-signal
        // error message, claim was deleted.
        await history.Received(1).WriteTasksAsync(
            claim.RunId,
            Arg.Is<IReadOnlyList<TaskRecord>>(tasks =>
                tasks.Count == 1 &&
                tasks[0].Status == TaskStatuses.Failed &&
                tasks[0].ErrorMessage!.Contains("no run-state blob")));
        await claimWriter.Received(1).DeleteAsync(claim.JobName, claim.TenantKey, claim.RunType);
    }

    [Fact]
    public async Task BlobRunning_StaleByMinutes_AcaTerminal_FinalizesViaAcaFallback()
    {
        // Post-#457: the former 10-min staleness gate is removed. When blob
        // says 'running' and ACA reports terminal, RunTracker immediately
        // finalizes regardless of how old the heartbeat is. The staleness
        // threshold no longer provides a deferral window — ACA terminal is
        // definitive that the container has exited.
        var claim = new ClaimRecord
        {
            RunId = "rid-silent-but-alive",
            JobName = "daily",
            TenantKey = "madev1",
            RunType = RunTypes.Normal,
            TriggerType = TriggerTypes.Scheduled,
            StartedAt = DateTimeOffset.UtcNow.AddMinutes(-15),
            ResolvedTimeoutSeconds = 24 * 3600,
            ResolvedEntities = ["entra_users"],
            ExpectedTasks = new List<ExpectedTask>
            {
                new() { ContainerType = "caj-graph", AcaExecutionName = "exec-1", Entities = ["entra_users"] }
            },
            ETag = "etag-1",
        };

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });

        var aca = Substitute.For<IAcaJobClient>();
        aca.GetExecutionStatusAsync("caj-graph", "exec-1").Returns(AcaExecutionStatuses.Succeeded);

        var runState = Substitute.For<IRunStateReader>();
        // 5 minutes since last heartbeat — staleness no longer blocks fallback.
        runState.TryReadAsync(claim.RunId, claim.TenantKey, "caj-graph")
            .Returns(BlobRunning("caj-graph", claim.RunId, claim.TenantKey, DateTimeOffset.UtcNow.AddMinutes(-5)));

        var canceller = Substitute.For<IRunCanceller>();
        canceller.TryReadIntentAsync(claim.RunId).Returns((CancelIntent?)null);

        var history = Substitute.For<IRunHistoryWriter>();
        history.WriteRunAsync(Arg.Any<RunRecord>()).Returns(WriteResult.Written);
        history.WriteTasksAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<TaskRecord>>())
            .Returns(WriteResult.Written);
        var claimWriter = Substitute.For<IClaimWriter>();

        var tracker = Build(claimReader, claimWriter, aca, history, runState, canceller);
        await tracker.CheckActiveRunsAsync();

        // ACA-fallback fires: task finalized as Succeeded, claim deleted.
        await history.Received(1).WriteTasksAsync(
            claim.RunId,
            Arg.Is<IReadOnlyList<TaskRecord>>(tasks =>
                tasks.Count == 1 && tasks[0].Status == TaskStatuses.Succeeded));
        await claimWriter.Received(1).DeleteAsync(claim.JobName, claim.TenantKey, claim.RunType);
    }

    [Fact]
    public async Task Finalization_EvictsCachedRunState()
    {
        // #385 Copilot finding 1: cache must be evicted at finalization or
        // it grows for the Function App instance lifetime. Pre-#385 this
        // bookkeeping lived inside ProgressReader.DeleteForRunAsync.
        var claim = new ClaimRecord
        {
            RunId = "rid-evict",
            JobName = "daily",
            TenantKey = "madev1",
            RunType = RunTypes.Normal,
            TriggerType = TriggerTypes.Scheduled,
            StartedAt = DateTimeOffset.UtcNow.AddMinutes(-2),
            ResolvedTimeoutSeconds = 24 * 3600,
            ResolvedEntities = ["entra_users"],
            ExpectedTasks = new List<ExpectedTask>
            {
                new() { ContainerType = "caj-graph", AcaExecutionName = "exec-1", Entities = ["entra_users"] }
            },
            ETag = "etag-1",
        };

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });

        var aca = Substitute.For<IAcaJobClient>();
        // Won't be called — terminal blob is authoritative.
        var runState = Substitute.For<IRunStateReader>();
        var terminal = new HeartbeatPayload(
            SchemaVersion: 4, RunId: claim.RunId, TenantKey: claim.TenantKey,
            ContainerType: "caj-graph",
            ContainerStartedAt: DateTimeOffset.UtcNow.AddMinutes(-3),
            LastHeartbeatAt: DateTimeOffset.UtcNow.AddSeconds(-5),
            RunStatus: BlobRunStatuses.Success,
            Entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Success,
                    RecordCount: 100,
                    StartedAt: DateTimeOffset.UtcNow.AddMinutes(-2),
                    CompletedAt: DateTimeOffset.UtcNow.AddSeconds(-5),
                    Errors: [])
            ]);
        runState.TryReadAsync(claim.RunId, claim.TenantKey, "caj-graph").Returns(terminal);

        var canceller = Substitute.For<IRunCanceller>();
        canceller.TryReadIntentAsync(claim.RunId).Returns((CancelIntent?)null);

        var history = Substitute.For<IRunHistoryWriter>();
        history.WriteRunAsync(Arg.Any<RunRecord>()).Returns(WriteResult.Written);
        history.WriteTasksAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<TaskRecord>>())
            .Returns(WriteResult.Written);
        var claimWriter = Substitute.For<IClaimWriter>();

        var cache = new HeartbeatCache();
        // Pre-seed the cache with this run's payload — simulates the API
        // having read the blob during the run.
        cache.Update(terminal);
        Assert.Single(cache.GetForRun(claim.RunId));

        var tracker = Build(claimReader, claimWriter, aca, history, runState, canceller, cache);
        await tracker.CheckActiveRunsAsync();

        // Cache entry for this run is gone now.
        Assert.Empty(cache.GetForRun(claim.RunId));
    }
}
