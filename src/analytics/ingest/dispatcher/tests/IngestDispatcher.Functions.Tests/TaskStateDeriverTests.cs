using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;

namespace IngestDispatcher.Functions.Tests;

// Unit tests for the pure derivation logic that maps
// (claim OR task history) + run-state blobs → per-task projections in
// API responses. Post-#385 the deriver has a single Derive() entry point;
// the pre-#385 DeriveInFlight/DeriveCompleted split is gone now that the
// run-state blob lives past finalization and carries terminal status.
public class TaskStateDeriverTests
{
    private static readonly TimeSpan StaleThreshold = TimeSpan.FromSeconds(25);
    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-05-09T13:50:00Z");

    private static ClaimRecord BuildClaim(params ExpectedTask[] tasks) => new()
    {
        SchemaVersion = 1,
        RunId = "run-1",
        JobName = "core-ingest",
        TenantKey = "madev1",
        RunType = RunTypes.Normal,
        TriggerType = TriggerTypes.Manual,
        TriggeredBy = "owen@example.com",
        StartedAt = DateTimeOffset.Parse("2026-05-09T13:42:00Z"),
        ResolvedEntities = ["entra_users"],
        ExpectedTasks = tasks,
    };

    private static HeartbeatPayload BlobRunning(
        string containerType, string runId, string tenantKey,
        DateTimeOffset lastBeat,
        IReadOnlyList<HeartbeatEntity> entities,
        IReadOnlyList<HeartbeatEntity>? prereqs = null) =>
        new(SchemaVersion: 4, RunId: runId, TenantKey: tenantKey,
            ContainerType: containerType,
            ContainerStartedAt: lastBeat.AddMinutes(-5),
            LastHeartbeatAt: lastBeat,
            RunStatus: BlobRunStatuses.Running,
            Entities: entities,
            PrerequisiteEntities: prereqs);

    private static HeartbeatPayload BlobTerminal(
        string containerType, string runId, string tenantKey,
        string runStatus,
        DateTimeOffset lastBeat,
        IReadOnlyList<HeartbeatEntity> entities,
        IReadOnlyList<HeartbeatEntity>? prereqs = null) =>
        new(SchemaVersion: 4, RunId: runId, TenantKey: tenantKey,
            ContainerType: containerType,
            ContainerStartedAt: lastBeat.AddMinutes(-5),
            LastHeartbeatAt: lastBeat,
            RunStatus: runStatus,
            Entities: entities,
            PrerequisiteEntities: prereqs);

    [Fact]
    public void Derive_DispatchFailedTask_RendersDispatchFailed()
    {
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["entra_users"],
            AcaExecutionName = null,
            DispatchError = "ARM returned 500",
        });

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload>(),
            StaleThreshold, Now);

        Assert.Single(result);
        Assert.Equal(TaskStatuses.DispatchFailed, result[0].Status);
        Assert.Equal("ARM returned 500", result[0].ErrorMessage);
        Assert.Null(result[0].Heartbeat);
        Assert.Empty(result[0].Entities);
    }

    [Fact]
    public void Derive_NoBlob_RendersPending()
    {
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["entra_users"],
            AcaExecutionName = "exec-001",
        });

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload>(),
            StaleThreshold, Now);

        Assert.Single(result);
        Assert.Equal(TaskStatusValues.Pending, result[0].Status);
        Assert.Null(result[0].Heartbeat);
        Assert.Empty(result[0].Entities);
    }

    [Fact]
    public void Derive_FreshRunningBlob_RendersRunningWithNotStale()
    {
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["entra_users"],
            AcaExecutionName = "exec-001",
        });
        var blob = BlobRunning("graph-ingest", claim.RunId, claim.TenantKey,
            lastBeat: Now.AddSeconds(-3),
            entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Running,
                    RecordCount: 100, RecordsSoFar: 100,
                    StartedAt: Now.AddMinutes(-1), CompletedAt: null)
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        Assert.Equal(TaskStatusValues.Running, result[0].Status);
        Assert.NotNull(result[0].Heartbeat);
        Assert.False(result[0].Heartbeat!.Stale);
        Assert.Equal(3, result[0].Heartbeat!.LastHeartbeatAgeSeconds);
        Assert.Single(result[0].Entities);
        Assert.Equal(100, result[0].Entities[0].RecordCount);
    }

    [Fact]
    public void Derive_StaleRunningBlob_FlipsStaleFlag()
    {
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["entra_users"],
            AcaExecutionName = "exec-001",
        });
        var blob = BlobRunning("graph-ingest", claim.RunId, claim.TenantKey,
            // 60s old > 25s threshold
            lastBeat: Now.AddSeconds(-60),
            entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Running,
                    RecordCount: 100)
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        Assert.True(result[0].Heartbeat!.Stale);
        // Status is still 'running' — staleness alone doesn't infer death.
        Assert.Equal(TaskStatusValues.Running, result[0].Status);
    }

    [Fact]
    public void Derive_AllEntitiesPendingAndNoPrereq_RendersPending()
    {
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["entra_users"],
            AcaExecutionName = "exec-001",
        });
        var blob = BlobRunning("graph-ingest", claim.RunId, claim.TenantKey,
            lastBeat: Now.AddSeconds(-2),
            entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Pending)
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        Assert.Equal(TaskStatusValues.Pending, result[0].Status);
    }

    [Fact]
    public void Derive_TerminalBlobSuccess_MapsToSucceeded()
    {
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["entra_users"],
            AcaExecutionName = "exec-001",
        });
        var blob = BlobTerminal("graph-ingest", claim.RunId, claim.TenantKey,
            runStatus: BlobRunStatuses.Success,
            lastBeat: Now.AddMinutes(-1),
            entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Success,
                    RecordCount: 264, StartedAt: Now.AddMinutes(-2),
                    CompletedAt: Now.AddMinutes(-1), Errors: [])
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        Assert.Equal(TaskStatuses.Succeeded, result[0].Status);
        Assert.Null(result[0].Heartbeat);          // terminal → no heartbeat block
        Assert.Equal(264, result[0].Entities[0].RecordCount);
        Assert.Null(result[0].ErrorMessage);
    }

    [Fact]
    public void Derive_TerminalBlobPartial_MapsToPartialWithErrorDetail()
    {
        // Severity-max picks 'partial' (rank 2) over 'success' (rank 0).
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["entra_users", "entra_groups"],
            AcaExecutionName = "exec-001",
        });
        var blob = BlobTerminal("graph-ingest", claim.RunId, claim.TenantKey,
            runStatus: BlobRunStatuses.Partial,
            lastBeat: Now.AddMinutes(-1),
            entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Success,
                    RecordCount: 264, StartedAt: Now.AddMinutes(-2),
                    CompletedAt: Now.AddMinutes(-1), Errors: []),
                new HeartbeatEntity(Name: "entra_groups", Status: EntityStatuses.Partial,
                    RecordCount: 50, StartedAt: Now.AddMinutes(-2),
                    CompletedAt: Now.AddMinutes(-1), Errors: ["page 3/8 503"]),
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        Assert.Equal(TaskStatuses.Partial, result[0].Status);
        Assert.NotNull(result[0].ErrorMessage);
        Assert.Contains("entra_groups(partial)", result[0].ErrorMessage!);
    }

    [Fact]
    public void Derive_TerminalBlobPreservesPrereqEntitiesAndEntityProgress()
    {
        // The whole point of #385: prereq + per-entity progress persists past
        // termination. Pre-#385 a finalized run dropped both.
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["teams_channel_members"],
            AcaExecutionName = "exec-001",
        });
        var blob = BlobTerminal("graph-ingest", claim.RunId, claim.TenantKey,
            runStatus: BlobRunStatuses.Success,
            lastBeat: Now.AddMinutes(-1),
            entities: [
                new HeartbeatEntity(Name: "teams_channel_members", Status: EntityStatuses.Success,
                    RecordCount: 18000, InputCount: 100, RecordsSoFar: 18000,
                    StartedAt: Now.AddMinutes(-15), CompletedAt: Now.AddMinutes(-1),
                    DurationMs: 60000, Errors: [])
            ],
            prereqs: [
                new HeartbeatEntity(Name: "team_channels", Status: EntityStatuses.Success,
                    InputCount: 42, RecordsSoFar: 18000,
                    StartedAt: Now.AddMinutes(-14), CompletedAt: Now.AddMinutes(-3),
                    DurationMs: 660000)
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        Assert.Equal(TaskStatuses.Succeeded, result[0].Status);
        Assert.NotNull(result[0].PrerequisiteEntities);
        Assert.Equal("team_channels", result[0].PrerequisiteEntities![0].Name);
        Assert.Equal(18000, result[0].Entities[0].RecordsSoFar);
        Assert.Equal(100, result[0].Entities[0].InputCount);
    }

    [Fact]
    public void Derive_FromTaskHistory_BlobAbsent_FallsBackToHistory()
    {
        // Pre-#385 run (or post-#385 with blob aged out beyond 30d). Task
        // history is authoritative; per-entity detail is absent.
        var taskHistory = new List<TaskRecord>
        {
            new(
                RunId: "run-1", JobName: "core-ingest", TenantKey: "madev1",
                RunType: RunTypes.Normal, ContainerType: "graph-ingest",
                AcaExecutionName: "exec-001", Entities: ["entra_users"],
                Status: TaskStatuses.Failed,
                StartedAt: DateTimeOffset.Parse("2026-05-09T05:00:00Z"),
                CompletedAt: DateTimeOffset.Parse("2026-05-09T05:38:00Z"),
                ErrorMessage: "ACA Job execution exec-001 failed (no run-state blob)"),
        };

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim: null, taskHistory,
            new Dictionary<string, HeartbeatPayload>(),
            StaleThreshold, Now);

        Assert.Single(result);
        Assert.Equal(TaskStatuses.Failed, result[0].Status);
        Assert.Empty(result[0].Entities);
        Assert.NotNull(result[0].ErrorMessage);
    }

    [Fact]
    public void Derive_FromTaskHistory_BlobPresent_PrefersBlobForEntityDetail()
    {
        // #385's core: completed runs get full entity + stage detail when
        // the durable blob is still around.
        var taskHistory = new List<TaskRecord>
        {
            new(
                RunId: "run-1", JobName: "core-ingest", TenantKey: "madev1",
                RunType: RunTypes.Normal, ContainerType: "graph-ingest",
                AcaExecutionName: "exec-001", Entities: ["entra_users"],
                Status: TaskStatuses.Succeeded,
                StartedAt: DateTimeOffset.Parse("2026-05-09T05:00:00Z"),
                CompletedAt: DateTimeOffset.Parse("2026-05-09T05:38:00Z"),
                ErrorMessage: null),
        };
        var blob = BlobTerminal("graph-ingest", "run-1", "madev1",
            runStatus: BlobRunStatuses.Success,
            lastBeat: DateTimeOffset.Parse("2026-05-09T05:37:58Z"),
            entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Success,
                    RecordCount: 264, RecordsSoFar: 264, DurationMs: 2264000,
                    StartedAt: DateTimeOffset.Parse("2026-05-09T05:00:14Z"),
                    CompletedAt: DateTimeOffset.Parse("2026-05-09T05:37:58Z"),
                    Errors: [])
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim: null, taskHistory,
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        Assert.Single(result);
        Assert.Equal(TaskStatuses.Succeeded, result[0].Status);
        Assert.Single(result[0].Entities);
        Assert.Equal(264, result[0].Entities[0].RecordCount);
        // Per-entity progress preserved — the #373 symptom this fixes.
        Assert.Equal(264, result[0].Entities[0].RecordsSoFar);
    }

    // --- Abandoned blob (#385 follow-up) ------------------------------
    //
    // When the cancel state machine kills a container before Stop-Heartbeat
    // can flush terminal state, the run-state blob is frozen at its last
    // mid-flight snapshot (run_status='running', some stages still
    // 'running'/'pending'). RunTracker correctly finalizes via ACA-fallback
    // and writes a terminal TaskRecord. The deriver must then prefer that
    // history record for task-level fields and reinterpret non-terminal
    // sub-statuses to match.

    [Fact]
    public void Derive_AbandonedBlob_CancelledTask_ReinterpretsNonTerminalAsCancelled()
    {
        // Mirrors the live observation on run 1e4242370e3c after manual
        // cancel: teams_root completed cleanly before the cancel; team_channels
        // was mid-flight (running); teams_channel_members never started
        // (pending). Operator should see all three end-states match the run
        // outcome.
        var killedAt = DateTimeOffset.Parse("2026-05-12T21:30:00Z");
        var runStartedAt = DateTimeOffset.Parse("2026-05-12T21:08:09Z");
        var taskHistory = new List<TaskRecord>
        {
            new(
                RunId: "run-cancelled", JobName: "adhoc", TenantKey: "madev2",
                RunType: RunTypes.Normal, ContainerType: "graph-ingest",
                AcaExecutionName: "exec-cancelled",
                Entities: ["teams_channel_members"],
                Status: TaskStatuses.Cancelled,
                StartedAt: runStartedAt,
                CompletedAt: killedAt,
                ErrorMessage: "Run cancelled by operator: smoke-test cleanup"),
        };
        var blob = BlobRunning("graph-ingest", "run-cancelled", "madev2",
            lastBeat: killedAt.AddSeconds(-30),
            entities: [
                new HeartbeatEntity(Name: "teams_channel_members", Status: EntityStatuses.Pending)
            ],
            prereqs: [
                new HeartbeatEntity(Name: "teams_root", Status: EntityStatuses.Success,
                    RecordsSoFar: 25078,
                    StartedAt: runStartedAt.AddSeconds(24), CompletedAt: runStartedAt.AddSeconds(38),
                    DurationMs: 14175),
                new HeartbeatEntity(Name: "team_channels", Status: EntityStatuses.Running,
                    InputCount: 25078, RecordsSoFar: 46522,
                    StartedAt: runStartedAt.AddSeconds(38), CompletedAt: null,
                    DurationMs: null,
                    ItemsProcessed: 18461, ItemsFailed: 0, ItemsSkipped: 3)
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim: null, taskHistory,
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        var task = Assert.Single(result);

        // Task-level fields come from history (the bug pre-fix: blob's
        // 'running' projection leaked through here).
        Assert.Equal(TaskStatuses.Cancelled, task.Status);
        Assert.Equal(killedAt, task.CompletedAt);
        Assert.Equal(runStartedAt, task.StartedAt);
        Assert.Equal("Run cancelled by operator: smoke-test cleanup", task.ErrorMessage);
        Assert.Null(task.Heartbeat);

        // Entity rollup: pending → cancelled (matches task).
        var entity = Assert.Single(task.Entities);
        Assert.Equal("teams_channel_members", entity.Name);
        Assert.Equal(EntityStatuses.Cancelled, entity.Status);
        // Forensic timestamps stay null — entity never started.
        Assert.Null(entity.StartedAt);
        Assert.Null(entity.CompletedAt);

        // Prereq entities: terminal stays terminal; running becomes cancelled.
        Assert.NotNull(task.PrerequisiteEntities);
        Assert.Equal(2, task.PrerequisiteEntities!.Count);
        Assert.Equal(EntityStatuses.Success, task.PrerequisiteEntities[0].Status);   // teams_root unchanged
        Assert.Equal(EntityStatuses.Cancelled, task.PrerequisiteEntities[1].Status);  // team_channels reinterpreted
        // Forensic counters preserved on the cancelled entity.
        Assert.Equal(46522, task.PrerequisiteEntities[1].RecordsSoFar);
        Assert.Equal(18461, task.PrerequisiteEntities[1].ItemsProcessed);
        Assert.Equal(3, task.PrerequisiteEntities[1].ItemsSkipped);
        // We don't fabricate a completed_at/duration_ms — the container was
        // killed; we don't know exactly when this entity stopped.
        Assert.Null(task.PrerequisiteEntities[1].CompletedAt);
        Assert.Null(task.PrerequisiteEntities[1].DurationMs);
    }

    [Fact]
    public void Derive_AbandonedBlob_FailedTask_ReinterpretsAsFailed()
    {
        // Same shape as the cancellation case but the task ran into an
        // organic failure (not operator cancel). Reinterpret non-terminal
        // sub-statuses to 'failed'. Verifies the transform follows the
        // task's terminal status verbatim.
        var taskHistory = new List<TaskRecord>
        {
            new(
                RunId: "run-failed", JobName: "adhoc", TenantKey: "madev2",
                RunType: RunTypes.Normal, ContainerType: "graph-ingest",
                AcaExecutionName: "exec-failed",
                Entities: ["entra_users"],
                Status: TaskStatuses.Failed,
                StartedAt: Now.AddMinutes(-10),
                CompletedAt: Now.AddMinutes(-1),
                ErrorMessage: "ACA Job execution exec-failed failed (no run-state blob)"),
        };
        var blob = BlobRunning("graph-ingest", "run-failed", "madev2",
            lastBeat: Now.AddMinutes(-2),
            entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Running,
                    RecordCount: 100, RecordsSoFar: 100,
                    StartedAt: Now.AddMinutes(-9), CompletedAt: null)
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim: null, taskHistory,
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        var task = Assert.Single(result);
        Assert.Equal(TaskStatuses.Failed, task.Status);
        Assert.Null(task.Heartbeat);

        var entity = Assert.Single(task.Entities);
        Assert.Equal(EntityStatuses.Failed, entity.Status);
        // records_so_far snapshot preserved at entity level.
        Assert.Equal(100, entity.RecordsSoFar);
    }

    [Fact]
    public void Derive_AbandonedBlob_SucceededTask_TranslatesToVocabCorrectTerminals()
    {
        // Edge case: container exited cleanly (ACA Succeeded) but Stop-
        // Heartbeat's bounded retry exhausted, leaving the blob stuck on
        // 'running'. RunTracker maps via ACA-fallback to TaskStatuses.Succeeded.
        // The deriver must then translate the task-vocab "succeeded" into
        // entity-vocab "success" and stage-vocab "completed" — different
        // strings across the three vocabularies. Pre-fix this test would
        // have rendered "succeeded" verbatim into entity/stage Status fields,
        // violating the wire format.
        var taskHistory = new List<TaskRecord>
        {
            new(
                RunId: "run-succeeded-via-fallback", JobName: "adhoc", TenantKey: "madev2",
                RunType: RunTypes.Normal, ContainerType: "graph-ingest",
                AcaExecutionName: "exec-aca-succeeded",
                Entities: ["entra_users"],
                Status: TaskStatuses.Succeeded,
                StartedAt: Now.AddMinutes(-10),
                CompletedAt: Now.AddMinutes(-1),
                ErrorMessage: null),
        };
        var blob = BlobRunning("graph-ingest", "run-succeeded-via-fallback", "madev2",
            lastBeat: Now.AddMinutes(-2),
            entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Running,
                    RecordCount: 100, RecordsSoFar: 100,
                    StartedAt: Now.AddMinutes(-9), CompletedAt: null)
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim: null, taskHistory,
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        var task = Assert.Single(result);
        Assert.Equal(TaskStatuses.Succeeded, task.Status);

        var entity = Assert.Single(task.Entities);
        // Entity vocab: 'success' not 'succeeded'.
        Assert.Equal(EntityStatuses.Success, entity.Status);
    }

    [Fact]
    public void Derive_AbandonedBlob_TerminalSubstatesPassThrough()
    {
        // A stage that completed cleanly before the container died keeps
        // its 'completed' status — only pending/running get reinterpreted.
        var taskHistory = new List<TaskRecord>
        {
            new(
                RunId: "run-mixed", JobName: "adhoc", TenantKey: "madev2",
                RunType: RunTypes.Normal, ContainerType: "graph-ingest",
                AcaExecutionName: "exec-mixed",
                Entities: ["teams_channel_members"],
                Status: TaskStatuses.Cancelled,
                StartedAt: Now.AddMinutes(-20),
                CompletedAt: Now.AddMinutes(-1),
                ErrorMessage: "Run cancelled by operator"),
        };
        var blob = BlobRunning("graph-ingest", "run-mixed", "madev2",
            lastBeat: Now.AddMinutes(-2),
            entities: [
                // Entity completed successfully before cancel. Should pass through.
                new HeartbeatEntity(Name: "teams_channel_members", Status: EntityStatuses.Success,
                    RecordCount: 18000, InputCount: 100, RecordsSoFar: 18000,
                    StartedAt: Now.AddMinutes(-15), CompletedAt: Now.AddMinutes(-3),
                    DurationMs: 60000, Errors: [])
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim: null, taskHistory,
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        var task = Assert.Single(result);
        Assert.Equal(TaskStatuses.Cancelled, task.Status);
        var entity = Assert.Single(task.Entities);
        // Entity was 'success' before the cancel — passes through verbatim.
        Assert.Equal(EntityStatuses.Success, entity.Status);
    }

    // --- Prerequisite entities (#362) ---------------------------------

    [Fact]
    public void Derive_PrereqEntitiesInRunningBlob_PropagateToTaskResponse()
    {
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["teams_channel_members"],
            AcaExecutionName = "exec-001",
        });
        var blob = BlobRunning("graph-ingest", claim.RunId, claim.TenantKey,
            lastBeat: Now.AddSeconds(-3),
            entities: [
                new HeartbeatEntity(Name: "teams_channel_members", Status: EntityStatuses.Pending)
            ],
            prereqs: [
                new HeartbeatEntity(Name: "teams_filtered_root", Status: EntityStatuses.Success,
                    RecordsSoFar: 42,
                    StartedAt: Now.AddMinutes(-15), CompletedAt: Now.AddMinutes(-14),
                    DurationMs: 11000),
                new HeartbeatEntity(Name: "team_channels", Status: EntityStatuses.Running,
                    InputCount: 42, RecordsSoFar: 18000,
                    StartedAt: Now.AddMinutes(-14), CompletedAt: null,
                    DurationMs: null)
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        Assert.NotNull(result[0].PrerequisiteEntities);
        Assert.Equal(2, result[0].PrerequisiteEntities!.Count);
        Assert.Equal("team_channels", result[0].PrerequisiteEntities![1].Name);
        Assert.Equal(18000, result[0].PrerequisiteEntities![1].RecordsSoFar);
    }

    [Fact]
    public void Derive_PrereqStarted_FlipsTaskFromPendingToRunning()
    {
        // Reproduces #362's observable symptom: a partial-ingest task whose
        // only requested entity is still pending while a prereq stage runs.
        // Pre-fix the task surfaced as 'pending' for the full prereq duration;
        // post-fix the running prereq counts as activity.
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["teams_channel_members"],
            AcaExecutionName = "exec-001",
        });
        var blob = BlobRunning("graph-ingest", claim.RunId, claim.TenantKey,
            lastBeat: Now.AddSeconds(-3),
            entities: [
                new HeartbeatEntity(Name: "teams_channel_members", Status: EntityStatuses.Pending)
            ],
            prereqs: [
                new HeartbeatEntity(Name: "team_channels", Status: EntityStatuses.Running,
                    InputCount: 42, RecordsSoFar: 18000,
                    StartedAt: Now.AddMinutes(-14), CompletedAt: null,
                    DurationMs: null)
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        Assert.Equal(TaskStatusValues.Running, result[0].Status);
    }

    [Fact]
    public void Derive_BlobWithoutPrereqField_PropagatesNull()
    {
        // Backward compatibility: older container images don't emit
        // prerequisite_entities; the field deserializes as null and the
        // API response surfaces null (not []) so consumers can distinguish
        // "not reported" from "reported, empty".
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["entra_users"],
            AcaExecutionName = "exec-001",
        });
        var blob = BlobRunning("graph-ingest", claim.RunId, claim.TenantKey,
            lastBeat: Now.AddSeconds(-3),
            entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Running,
                    RecordCount: 100, StartedAt: Now.AddMinutes(-1),
                    CompletedAt: null)
            ],
            prereqs: null);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        Assert.Null(result[0].PrerequisiteEntities);
    }

    [Fact]
    public void Derive_ItemsCounters_ProjectThroughToBothEntityAndPrereqEntities()
    {
        // #383: items_processed / items_failed / items_skipped on
        // HeartbeatEntity should flow verbatim into TaskEntityProjection
        // and TaskStatusResponse.PrerequisiteEntities.
        var claim = BuildClaim(new ExpectedTask
        {
            ContainerType = "graph-ingest",
            Entities = ["teams_channel_members"],
            AcaExecutionName = "exec-001",
        });
        var blob = BlobRunning("graph-ingest", claim.RunId, claim.TenantKey,
            lastBeat: Now.AddSeconds(-3),
            entities: [
                new HeartbeatEntity(Name: "teams_channel_members", Status: EntityStatuses.Running,
                    RecordCount: 18000, InputCount: 100, RecordsSoFar: 18000,
                    StartedAt: Now.AddMinutes(-2), CompletedAt: null,
                    ItemsProcessed: 42, ItemsFailed: 1, ItemsSkipped: 2)
            ],
            prereqs: [
                new HeartbeatEntity(Name: "team_channels", Status: EntityStatuses.Running,
                    InputCount: 25078, RecordsSoFar: 63091,
                    StartedAt: Now.AddMinutes(-14), CompletedAt: null,
                    DurationMs: null,
                    ItemsProcessed: 12500, ItemsFailed: 0, ItemsSkipped: 3)
            ]);

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(claim, taskHistory: [],
            new Dictionary<string, HeartbeatPayload> { ["graph-ingest"] = blob },
            StaleThreshold, Now);

        var entity = result[0].Entities.Single();
        Assert.Equal(42, entity.ItemsProcessed);
        Assert.Equal(1, entity.ItemsFailed);
        Assert.Equal(2, entity.ItemsSkipped);

        var prereq = result[0].PrerequisiteEntities!.Single();
        Assert.Equal(12500, prereq.ItemsProcessed);
        Assert.Equal(0, prereq.ItemsFailed);
        Assert.Equal(3, prereq.ItemsSkipped);
    }

    [Fact]
    public void HeartbeatEntity_V4JsonFixture_DeserializesAllFields()
    {
        // v4 flat entity shape: all progress counters live at entity level.
        const string json = """
        {
          "name": "teams_channel_members",
          "status": "running",
          "record_count": 18000,
          "input_count": 100,
          "items_processed": 42,
          "items_failed": 1,
          "items_skipped": 2,
          "records_so_far": 18000,
          "started_at": "2026-05-12T10:00:00Z",
          "completed_at": null,
          "duration_ms": null,
          "errors": ["page 3/8 503"]
        }
        """;

        var entity = System.Text.Json.JsonSerializer.Deserialize<HeartbeatEntity>(json);

        Assert.NotNull(entity);
        Assert.Equal("teams_channel_members", entity!.Name);
        Assert.Equal("running", entity.Status);
        Assert.Equal(18000, entity.RecordCount);
        Assert.Equal(100, entity.InputCount);
        Assert.Equal(42, entity.ItemsProcessed);
        Assert.Equal(1, entity.ItemsFailed);
        Assert.Equal(2, entity.ItemsSkipped);
        Assert.Equal(18000, entity.RecordsSoFar);
        Assert.NotNull(entity.StartedAt);
        Assert.Null(entity.CompletedAt);
        Assert.Null(entity.DurationMs);
        Assert.Single(entity.Errors!);
        Assert.Equal("page 3/8 503", entity.Errors![0]);
    }

    [Fact]
    public void HeartbeatEntity_MinimalJsonFixture_DeserializesWithNulls()
    {
        // Minimal entity (just name + status) — all optional fields null.
        const string json = """
        {
          "name": "entra_users",
          "status": "pending"
        }
        """;

        var entity = System.Text.Json.JsonSerializer.Deserialize<HeartbeatEntity>(json);

        Assert.NotNull(entity);
        Assert.Equal("entra_users", entity!.Name);
        Assert.Equal("pending", entity.Status);
        Assert.Null(entity.RecordCount);
        Assert.Null(entity.InputCount);
        Assert.Null(entity.ItemsProcessed);
        Assert.Null(entity.ItemsFailed);
        Assert.Null(entity.ItemsSkipped);
        Assert.Null(entity.RecordsSoFar);
        Assert.Null(entity.StartedAt);
        Assert.Null(entity.CompletedAt);
        Assert.Null(entity.DurationMs);
        Assert.Null(entity.Errors);
    }

    [Fact]
    public void HeartbeatEntity_V4JsonRoundTrip_PreservesAllFields()
    {
        // Serialize then deserialize to confirm the record's JSON
        // property names produce a valid round-trip.
        var original = new HeartbeatEntity(
            Name: "team_channels", Status: "success",
            RecordCount: 500, InputCount: 25078,
            ItemsProcessed: 12500, ItemsFailed: 0, ItemsSkipped: 3,
            RecordsSoFar: 63091,
            StartedAt: DateTimeOffset.Parse("2026-05-12T10:00:00Z"),
            CompletedAt: DateTimeOffset.Parse("2026-05-12T10:11:00Z"),
            DurationMs: 660000,
            Errors: []);

        var json = System.Text.Json.JsonSerializer.Serialize(original);
        var deserialized = System.Text.Json.JsonSerializer.Deserialize<HeartbeatEntity>(json);

        Assert.NotNull(deserialized);
        Assert.Equal(original.Name, deserialized!.Name);
        Assert.Equal(original.Status, deserialized.Status);
        Assert.Equal(original.RecordCount, deserialized.RecordCount);
        Assert.Equal(original.InputCount, deserialized.InputCount);
        Assert.Equal(original.ItemsProcessed, deserialized.ItemsProcessed);
        Assert.Equal(original.ItemsFailed, deserialized.ItemsFailed);
        Assert.Equal(original.ItemsSkipped, deserialized.ItemsSkipped);
        Assert.Equal(original.RecordsSoFar, deserialized.RecordsSoFar);
        Assert.Equal(original.StartedAt, deserialized.StartedAt);
        Assert.Equal(original.CompletedAt, deserialized.CompletedAt);
        Assert.Equal(original.DurationMs, deserialized.DurationMs);
        Assert.Empty(deserialized.Errors!);
    }
}
