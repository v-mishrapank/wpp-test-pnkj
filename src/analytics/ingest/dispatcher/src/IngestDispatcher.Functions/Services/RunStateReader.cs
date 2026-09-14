using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using IngestDispatcher.Functions.Models;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Services;

// Reader for the per-container run-state blob written by the container's
// ProgressHeartbeat module. Renamed from ProgressReader in #385 — the blob
// is now durable past finalization and carries terminal status, so the
// "progress" framing no longer fits.
public interface IRunStateReader
{
    // Returns null only when the blob is absent (404). Other failures —
    // transient HTTP, deserialize errors, throttle — propagate to the
    // caller. RunTracker uses this for finalization decisions and relies
    // on exception propagation to defer the per-task outcome to the next
    // tick instead of misclassifying a transient blob read failure as
    // "no run-state blob" and falling through to ACA exit code (which
    // would lose the per-entity outcomes the blob exists to carry). Pre-
    // #385 ManifestSummaryReader.TryReadAsync had the same strict contract.
    //
    // For best-effort API reads (e.g. /runs/{id}), use ListForRunAsync,
    // which has its own per-blob exception-swallowing for resilience.
    Task<HeartbeatPayload?> TryReadAsync(string runId, string tenantKey, string containerType);

    // Returns every run-state blob for a runId. Reads each task's blob fresh,
    // then fills gaps from the in-process last-seen cache (#326): when a
    // single task's blob is invisible on this poll (prefix-list eventual
    // consistency, mid-rewrite, transient read failure), the cached payload
    // is returned in its place. The cached LastHeartbeatAt naturally drives
    // the deriver's `stale=true` flag while in-flight; for terminal blobs
    // the durable copy makes this gap-fill less critical but still useful.
    Task<IReadOnlyList<HeartbeatPayload>> ListForRunAsync(string runId);

    // Time-based retention sweep (#385). Run-state blobs are no longer
    // deleted at finalization — they're durable until aged out here, at
    // the same horizon as run history (default 30d, configurable via
    // Storage.OrphanAgeThresholdHours). Throttled by
    // Storage.OrphanSweepIntervalMinutes. Storage errors are logged and
    // swallowed so the dispatcher tick is never blocked on cleanup;
    // OperationCanceledException is propagated so host shutdown can unwind
    // promptly.
    Task SweepOrphansAsync(CancellationToken ct = default);
}

public class RunStateReader : IRunStateReader
{
    public const string RunStatePrefix = "_dispatcher/run_state/";

    private readonly BlobContainerClient _container;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly IHeartbeatCache _cache;
    private readonly ILogger<RunStateReader> _logger;
    private readonly TimeSpan _sweepInterval;
    private readonly TimeSpan _orphanAge;
    private DateTimeOffset _lastSweepAt = DateTimeOffset.MinValue;

    public RunStateReader(
        IConfigLoader configLoader,
        BlobServiceClient blobServiceClient,
        JsonSerializerOptions jsonOptions,
        IHeartbeatCache cache,
        ILogger<RunStateReader> logger)
    {
        var storage = configLoader.Storage;
        _container = blobServiceClient.GetBlobContainerClient(storage.Container);
        _jsonOptions = jsonOptions;
        _cache = cache;
        _logger = logger;
        _sweepInterval = TimeSpan.FromMinutes(Math.Max(0, storage.OrphanSweepIntervalMinutes));
        _orphanAge = TimeSpan.FromHours(Math.Max(1, storage.OrphanAgeThresholdHours));
    }

    public static string RunStateBlobPath(string runId, string tenantKey, string containerType) =>
        $"{RunStatePrefix}{runId}/{tenantKey}/{containerType}.json";

    public async Task<HeartbeatPayload?> TryReadAsync(string runId, string tenantKey, string containerType)
    {
        var path = RunStateBlobPath(runId, tenantKey, containerType);
        var blob = _container.GetBlobClient(path);
        try
        {
            var response = await blob.DownloadContentAsync();
            var hb = JsonSerializer.Deserialize<HeartbeatPayload>(
                response.Value.Content.ToString(), _jsonOptions);
            if (hb != null) _cache.Update(hb);
            return hb;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
        // Other exceptions (transient 5xx, throttle, deserialize errors)
        // propagate. See IRunStateReader.TryReadAsync docstring for why —
        // swallowing here would silently misclassify finalization outcomes
        // as ACA-exit-only.
    }

    public async Task<IReadOnlyList<HeartbeatPayload>> ListForRunAsync(string runId)
    {
        var prefix = $"{RunStatePrefix}{runId}/";
        var freshByContainer = new Dictionary<string, HeartbeatPayload>();
        await foreach (var blobItem in _container.GetBlobsAsync(prefix: prefix))
        {
            if (!blobItem.Name.EndsWith(".json", StringComparison.Ordinal)) continue;
            try
            {
                var blob = _container.GetBlobClient(blobItem.Name);
                var response = await blob.DownloadContentAsync();
                var hb = JsonSerializer.Deserialize<HeartbeatPayload>(
                    response.Value.Content.ToString(), _jsonOptions);
                if (hb != null)
                {
                    freshByContainer[hb.ContainerType] = hb;
                    _cache.Update(hb);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to read run-state blob {Blob}", blobItem.Name);
            }
        }

        // Fill gaps from cache. A container whose fresh read missed this
        // poll surfaces its last-seen payload; the deriver renders it as
        // running with `stale=true` once LastHeartbeatAt crosses the
        // configured threshold (see RunsFunction.StaleThreshold and
        // TaskStateDeriver.BuildFromHeartbeat).
        foreach (var cached in _cache.GetForRun(runId))
        {
            if (!freshByContainer.ContainsKey(cached.ContainerType))
            {
                _logger.LogInformation(
                    "Run-state blob read missed for run {RunId} container {Container}; serving cached payload (last_heartbeat_at={LastHeartbeatAt:o})",
                    runId, cached.ContainerType, cached.LastHeartbeatAt);
                freshByContainer[cached.ContainerType] = cached;
            }
        }

        return freshByContainer.Values.ToList();
    }

    public async Task SweepOrphansAsync(CancellationToken ct = default)
    {
        // Throttle. Per-instance state is sufficient — RunStateReader is a
        // singleton and IngestTimer's UseMonitor=true serializes ticks
        // across scale-out, so we won't get parallel sweeps.
        var now = DateTimeOffset.UtcNow;
        if (now - _lastSweepAt < _sweepInterval) return;
        _lastSweepAt = now;

        var threshold = now - _orphanAge;
        int scanned = 0, deleted = 0, failed = 0;

        try
        {
            await foreach (var item in _container.GetBlobsAsync(prefix: RunStatePrefix, cancellationToken: ct))
            {
                scanned++;
                if (item.Properties.LastModified is null || item.Properties.LastModified > threshold) continue;

                try
                {
                    await _container.GetBlobClient(item.Name).DeleteIfExistsAsync(cancellationToken: ct);
                    deleted++;
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    failed++;
                    _logger.LogWarning(ex, "Failed to delete aged run-state blob {Blob}", item.Name);
                }
            }

            _logger.LogInformation(
                "Run-state retention sweep complete scanned={Scanned} deleted={Deleted} failed={Failed} threshold={Threshold:o}",
                scanned, deleted, failed, threshold);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            // Listing failure must not stall the dispatcher tick. Log and move on;
            // the next sweep window will retry.
            _logger.LogWarning(ex, "Run-state retention sweep aborted at scanned={Scanned}", scanned);
        }
    }
}
