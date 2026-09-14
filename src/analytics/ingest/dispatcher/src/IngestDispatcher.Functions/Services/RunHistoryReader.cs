using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using IngestDispatcher.Functions.Models;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Services;

public interface IRunHistoryReader
{
    // Returns finalized run records that started within the given window.
    // History blobs are partitioned by date (yyyy-MM-dd), so a window scan
    // walks N date prefixes from today backwards.
    Task<IReadOnlyList<RunRecord>> ListInWindowAsync(TimeSpan window);

    // Look up a single run by id; scans date prefixes back from today until
    // found or the window is exhausted. Worst case ~30 prefix lists.
    Task<RunRecord?> TryFindByRunIdAsync(string runId, TimeSpan window);

    // Read the JSONL task records for a finalized run. Empty when not found.
    Task<IReadOnlyList<TaskRecord>> ReadTasksAsync(string runId, DateTimeOffset startedAt);
}

public class RunHistoryReader : IRunHistoryReader
{
    private const string RunsPrefix = "_dispatcher/runs/";
    private const string TasksPrefix = "_dispatcher/tasks/";

    private readonly BlobContainerClient _container;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ILogger<RunHistoryReader> _logger;

    public RunHistoryReader(
        IConfigLoader configLoader,
        BlobServiceClient blobServiceClient,
        JsonSerializerOptions jsonOptions,
        ILogger<RunHistoryReader> logger)
    {
        _container = blobServiceClient.GetBlobContainerClient(configLoader.Storage.Container);
        _jsonOptions = jsonOptions;
        _logger = logger;
    }

    public async Task<IReadOnlyList<RunRecord>> ListInWindowAsync(TimeSpan window)
    {
        var cutoff = DateTimeOffset.UtcNow - window;
        var earliestDate = cutoff.UtcDateTime.Date;
        var todayUtc = DateTimeOffset.UtcNow.UtcDateTime.Date;
        var results = new List<RunRecord>();

        // Walk only the date partitions inside the window — bounded list-blob
        // calls regardless of how big the long-tail history grows. For each
        // day prefix from today back to the cutoff, list its blobs and
        // post-filter by StartedAt for the partial-day boundary at `cutoff`.
        for (var date = todayUtc; date >= earliestDate; date = date.AddDays(-1))
        {
            var dayPrefix = $"{RunsPrefix}{date:yyyy-MM-dd}/";
            await foreach (var blobItem in _container.GetBlobsAsync(prefix: dayPrefix))
            {
                if (!blobItem.Name.EndsWith(".json", StringComparison.Ordinal)) continue;

                try
                {
                    var blob = _container.GetBlobClient(blobItem.Name);
                    var response = await blob.DownloadContentAsync();
                    var run = JsonSerializer.Deserialize<RunRecord>(
                        response.Value.Content.ToString(), _jsonOptions);
                    if (run != null && run.StartedAt >= cutoff)
                        results.Add(run);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to read history blob {Blob}", blobItem.Name);
                }
            }
        }

        return results;
    }

    public async Task<RunRecord?> TryFindByRunIdAsync(string runId, TimeSpan window)
    {
        // Date isn't part of the run_id, so we can't go straight to the
        // partition. Walk back from today, stopping once we exhaust the
        // window or find the run. Worst case is 30 prefix lists for a
        // missing run within the cap; cheap. A per-runId index blob is a
        // future optimization.
        var maxDays = (int)Math.Ceiling(window.TotalDays) + 1;
        for (var dayOffset = 0; dayOffset < maxDays; dayOffset++)
        {
            var date = DateTimeOffset.UtcNow.AddDays(-dayOffset);
            var prefix = $"{RunsPrefix}{date:yyyy-MM-dd}/run_{runId}.json";
            var blob = _container.GetBlobClient(prefix);
            try
            {
                var response = await blob.DownloadContentAsync();
                return JsonSerializer.Deserialize<RunRecord>(
                    response.Value.Content.ToString(), _jsonOptions);
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                continue;
            }
        }
        return null;
    }

    public async Task<IReadOnlyList<TaskRecord>> ReadTasksAsync(string runId, DateTimeOffset startedAt)
    {
        var path = $"{TasksPrefix}{startedAt.UtcDateTime:yyyy-MM-dd}/tasks_{runId}.jsonl";
        var blob = _container.GetBlobClient(path);
        try
        {
            var response = await blob.DownloadContentAsync();
            var content = response.Value.Content.ToString();
            var results = new List<TaskRecord>();
            foreach (var line in content.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                var trimmed = line.Trim();
                if (string.IsNullOrEmpty(trimmed)) continue;
                try
                {
                    var task = JsonSerializer.Deserialize<TaskRecord>(trimmed, _jsonOptions);
                    if (task != null) results.Add(task);
                }
                catch (JsonException ex)
                {
                    _logger.LogWarning(ex, "Skipping malformed task line in {Path}", path);
                }
            }
            return results;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return [];
        }
    }
}
