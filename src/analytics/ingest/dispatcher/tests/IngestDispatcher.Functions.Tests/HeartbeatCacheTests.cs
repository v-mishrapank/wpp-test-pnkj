using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;

namespace IngestDispatcher.Functions.Tests;

// Tests for the in-process heartbeat cache that papers over transient
// blob read misses on /api/runs/{runId}. The cache itself is dumb storage;
// the staleness signal flows through TaskStateDeriver.BuildFromHeartbeat,
// which compares the cached LastHeartbeatAt to `now`.
public class HeartbeatCacheTests
{
    private static HeartbeatPayload BuildHb(string runId, string containerType, DateTimeOffset lastHeartbeatAt) =>
        new(
            SchemaVersion: 1,
            RunId: runId,
            TenantKey: "madev1",
            ContainerType: containerType,
            ContainerStartedAt: lastHeartbeatAt.AddMinutes(-1),
            LastHeartbeatAt: lastHeartbeatAt,
            RunStatus: "running",
            Entities: [
                new HeartbeatEntity(Name: "entra_users", Status: EntityStatuses.Running,
                    RecordCount: 100)
            ]);

    [Fact]
    public void Update_ThenGetForRun_ReturnsPayload()
    {
        var cache = new HeartbeatCache();
        var hb = BuildHb("run-1", "graph-ingest", DateTimeOffset.UtcNow);

        cache.Update(hb);

        var result = cache.GetForRun("run-1");
        Assert.Single(result);
        Assert.Same(hb, result[0]);
    }

    [Fact]
    public void Update_SameKey_OverwritesWithLatest()
    {
        var cache = new HeartbeatCache();
        var older = BuildHb("run-1", "graph-ingest", DateTimeOffset.Parse("2026-05-09T13:42:00Z"));
        var newer = BuildHb("run-1", "graph-ingest", DateTimeOffset.Parse("2026-05-09T13:50:00Z"));

        cache.Update(older);
        cache.Update(newer);

        var result = cache.GetForRun("run-1");
        Assert.Single(result);
        Assert.Equal(newer.LastHeartbeatAt, result[0].LastHeartbeatAt);
    }

    [Fact]
    public void GetForRun_ReturnsAllContainersForThatRun()
    {
        var cache = new HeartbeatCache();
        cache.Update(BuildHb("run-1", "graph-ingest", DateTimeOffset.UtcNow));
        cache.Update(BuildHb("run-1", "exo-ingest", DateTimeOffset.UtcNow));
        cache.Update(BuildHb("run-2", "graph-ingest", DateTimeOffset.UtcNow));

        var result = cache.GetForRun("run-1");

        Assert.Equal(2, result.Count);
        Assert.Contains(result, h => h.ContainerType == "graph-ingest");
        Assert.Contains(result, h => h.ContainerType == "exo-ingest");
    }

    [Fact]
    public void GetForRun_UnknownRun_ReturnsEmpty()
    {
        var cache = new HeartbeatCache();
        cache.Update(BuildHb("run-1", "graph-ingest", DateTimeOffset.UtcNow));

        Assert.Empty(cache.GetForRun("does-not-exist"));
    }

    [Fact]
    public void EvictRun_RemovesOnlyThatRunsEntries()
    {
        var cache = new HeartbeatCache();
        cache.Update(BuildHb("run-1", "graph-ingest", DateTimeOffset.UtcNow));
        cache.Update(BuildHb("run-1", "exo-ingest", DateTimeOffset.UtcNow));
        cache.Update(BuildHb("run-2", "graph-ingest", DateTimeOffset.UtcNow));

        cache.EvictRun("run-1");

        Assert.Empty(cache.GetForRun("run-1"));
        Assert.Single(cache.GetForRun("run-2"));
    }

    [Fact]
    public void CachedPayload_FedToDeriver_RendersStaleWhenAged()
    {
        // Acceptance: a missing-fresh-blob fallback that returns a cached
        // payload aged >25s comes out of the deriver as running with
        // stale=true. This is the end-to-end behavior #326 requires.
        var now = DateTimeOffset.Parse("2026-05-09T13:50:00Z");
        var cache = new HeartbeatCache();
        var aged = BuildHb("run-1", "graph-ingest", now.AddSeconds(-60));
        cache.Update(aged);

        var cached = cache.GetForRun("run-1");
        Assert.Single(cached);

        var claim = new ClaimRecord
        {
            SchemaVersion = 1,
            RunId = "run-1",
            JobName = "core-ingest",
            TenantKey = "madev1",
            RunType = RunTypes.Normal,
            TriggerType = TriggerTypes.Manual,
            TriggeredBy = "owen@example.com",
            StartedAt = now.AddMinutes(-10),
            ResolvedEntities = ["entra_users"],
            ExpectedTasks = [
                new ExpectedTask {
                    ContainerType = "graph-ingest",
                    Entities = ["entra_users"],
                    AcaExecutionName = "exec-001",
                }
            ],
        };

        var deriver = new TaskStateDeriver();
        var result = deriver.Derive(
            claim,
            taskHistory: [],
            cached.ToDictionary(h => h.ContainerType, h => h),
            TimeSpan.FromSeconds(25),
            now);

        Assert.Single(result);
        Assert.Equal(TaskStatusValues.Running, result[0].Status);
        Assert.NotNull(result[0].Heartbeat);
        Assert.True(result[0].Heartbeat!.Stale);
        Assert.Equal(60, result[0].Heartbeat!.LastHeartbeatAgeSeconds);
        Assert.Single(result[0].Entities);
    }
}
