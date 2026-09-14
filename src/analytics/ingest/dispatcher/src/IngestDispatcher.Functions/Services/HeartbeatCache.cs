using System.Collections.Concurrent;
using IngestDispatcher.Functions.Models;

namespace IngestDispatcher.Functions.Services;

// In-process last-seen heartbeat cache. Papers over transient blob read
// misses on /api/runs/{runId}: when ListForRunAsync's prefix scan or
// per-blob download fails to surface one task's heartbeat for a single
// poll (eventual consistency on the bulk list, mid-rewrite, etc.), the
// deriver previously fell through to `pending`+empty entities — a
// dispatch-failure-shaped status for a run that's actually healthy
// (#326). Returning the last-seen payload preserves the staleness
// signal already in the schema (LastHeartbeatAt vs now) and lets the
// deriver render `stale=true` for "we know it was running, we just
// can't see it right now."
public interface IHeartbeatCache
{
    void Update(HeartbeatPayload payload);
    IReadOnlyList<HeartbeatPayload> GetForRun(string runId);
    void EvictRun(string runId);
}

// Two-level dict (runId → containerType → payload) so GetForRun is O(k)
// and EvictRun is O(1), independent of how many other runs the instance
// has seen. No TTL/size cap: bounded by explicit EvictRun calls at
// finalization (RunTracker.CheckClaimAsync after the claim delete, and
// ForceReleaseRunFunction after force-release cleanup). The cache is an
// in-flight-only papering over for prefix-list eventual consistency;
// once a run finalizes, the durable blob is the source of truth and the
// cached entry is dead weight, so eviction happens at the same moment the
// run leaves in-flight state. Pre-#385 the equivalent eviction lived
// inside ProgressReader.DeleteForRunAsync.
public class HeartbeatCache : IHeartbeatCache
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, HeartbeatPayload>> _byRun = new();

    public void Update(HeartbeatPayload payload)
    {
        var inner = _byRun.GetOrAdd(payload.RunId,
            _ => new ConcurrentDictionary<string, HeartbeatPayload>());
        inner[payload.ContainerType] = payload;
    }

    public IReadOnlyList<HeartbeatPayload> GetForRun(string runId) =>
        _byRun.TryGetValue(runId, out var inner) ? inner.Values.ToList() : [];

    public void EvictRun(string runId) => _byRun.TryRemove(runId, out _);
}
