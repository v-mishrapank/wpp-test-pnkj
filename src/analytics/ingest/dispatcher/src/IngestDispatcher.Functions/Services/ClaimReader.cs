using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using IngestDispatcher.Functions.Models;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Services;

public interface IClaimReader
{
    // Returns every active claim. Filters ADLS Gen2 directory placeholders
    // and pre-#314 legacy shapes via the same exactly-two-separator rule
    // RunTracker.CheckActiveRunsAsync used.
    Task<IReadOnlyList<ClaimRecord>> ListActiveAsync();

    // Returns null when the claim blob is absent (404).
    Task<ClaimRecord?> TryReadAsync(string jobName, string tenantKey, string runType);

    // True when an active claim exists for this run_id under any (job, tenant,
    // runtype) tracking blob. Used by the API's GET /runs/{runId} resolver to
    // pick the in-flight projection.
    Task<ClaimRecord?> TryFindByRunIdAsync(string runId);
}

public class ClaimReader : IClaimReader
{
    private readonly BlobContainerClient _container;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ILogger<ClaimReader> _logger;

    public ClaimReader(
        IConfigLoader configLoader,
        BlobServiceClient blobServiceClient,
        JsonSerializerOptions jsonOptions,
        ILogger<ClaimReader> logger)
    {
        _container = blobServiceClient.GetBlobContainerClient(configLoader.Storage.Container);
        _jsonOptions = jsonOptions;
        _logger = logger;
    }

    private static int CountSeparators(ReadOnlySpan<char> s)
    {
        var n = 0;
        foreach (var c in s) if (c == '/') n++;
        return n;
    }

    // Match {prefix}{jobName}/{tenantKey}/{runType}.json exactly: ends in .json
    // AND contains exactly two path separators after the prefix. Skips three
    // non-claim shapes (ADLS Gen2 directory placeholders, pre-#301 leaf, post-#301/pre-#296 leaf).
    private static bool IsClaimPath(string blobName)
    {
        if (!blobName.EndsWith(".json", StringComparison.Ordinal)) return false;
        if (blobName.Length <= ClaimWriter.TrackingPrefix.Length) return false;
        var relative = blobName.AsSpan(ClaimWriter.TrackingPrefix.Length);
        return CountSeparators(relative) == 2;
    }

    public async Task<IReadOnlyList<ClaimRecord>> ListActiveAsync()
    {
        var results = new List<ClaimRecord>();
        await foreach (var blobItem in _container.GetBlobsAsync(prefix: ClaimWriter.TrackingPrefix))
        {
            if (!IsClaimPath(blobItem.Name)) continue;
            try
            {
                var blob = _container.GetBlobClient(blobItem.Name);
                var response = await blob.DownloadContentAsync();
                var claim = JsonSerializer.Deserialize<ClaimRecord>(
                    response.Value.Content.ToString(), _jsonOptions);
                if (claim != null)
                    results.Add(claim with { ETag = response.Value.Details.ETag.ToString() });
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to read claim blob {Blob}", blobItem.Name);
            }
        }
        return results;
    }

    public async Task<ClaimRecord?> TryReadAsync(string jobName, string tenantKey, string runType)
    {
        var path = ClaimWriter.ClaimBlobPath(jobName, tenantKey, runType);
        var blob = _container.GetBlobClient(path);
        try
        {
            var response = await blob.DownloadContentAsync();
            var claim = JsonSerializer.Deserialize<ClaimRecord>(
                response.Value.Content.ToString(), _jsonOptions);
            return claim is null ? null
                : claim with { ETag = response.Value.Details.ETag.ToString() };
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<ClaimRecord?> TryFindByRunIdAsync(string runId)
    {
        var all = await ListActiveAsync();
        return all.FirstOrDefault(c => c.RunId == runId);
    }
}
