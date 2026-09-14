using System.Text;
using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using IngestDispatcher.Functions.Models;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Services;

public enum WriteResult
{
    Written,         // blob newly created this call
    AlreadyExists,   // 412 on If-None-Match: history already persisted on an earlier tick
    Failed           // transient exception; caller should retain retry state
}

public interface IRunHistoryWriter
{
    Task<WriteResult> WriteRunAsync(RunRecord run);
    Task<WriteResult> WriteTasksAsync(string runId, IReadOnlyList<TaskRecord> tasks);
}

public class RunHistoryWriter : IRunHistoryWriter
{
    private readonly BlobContainerClient _container;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ILogger<RunHistoryWriter> _logger;

    public RunHistoryWriter(
        IConfigLoader configLoader,
        BlobServiceClient blobServiceClient,
        JsonSerializerOptions jsonOptions,
        ILogger<RunHistoryWriter> logger)
    {
        _logger = logger;
        _jsonOptions = jsonOptions;
        _container = blobServiceClient.GetBlobContainerClient(configLoader.Storage.Container);
    }

    public async Task<WriteResult> WriteRunAsync(RunRecord run)
    {
        var date = run.StartedAt.UtcDateTime.ToString("yyyy-MM-dd");
        var blobPath = $"_dispatcher/runs/{date}/run_{run.RunId}.json";
        var json = JsonSerializer.Serialize(run, _jsonOptions);
        return await UploadIfNotExistsAsync(blobPath, json, "run history", run.RunId);
    }

    public async Task<WriteResult> WriteTasksAsync(string runId, IReadOnlyList<TaskRecord> tasks)
    {
        // No tasks is a legitimate empty success — nothing to retain or retry.
        if (tasks.Count == 0) return WriteResult.AlreadyExists;

        var date = tasks[0].StartedAt?.UtcDateTime.ToString("yyyy-MM-dd")
            ?? DateTimeOffset.UtcNow.ToString("yyyy-MM-dd");
        var blobPath = $"_dispatcher/tasks/{date}/tasks_{runId}.jsonl";

        // Explicit '\n' — AppendLine uses Environment.NewLine which is "\r\n" on
        // Windows, breaking strict JSONL consumers. App runs on Linux but tests
        // don't, so be explicit.
        var sb = new StringBuilder();
        foreach (var task in tasks)
            sb.Append(JsonSerializer.Serialize(task, _jsonOptions)).Append('\n');

        return await UploadIfNotExistsAsync(blobPath, sb.ToString(), "task history", runId);
    }

    private async Task<WriteResult> UploadIfNotExistsAsync(string blobPath, string content, string what, string runId)
    {
        var blob = _container.GetBlobClient(blobPath);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
        try
        {
            await blob.UploadAsync(stream, new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All }
            });
            return WriteResult.Written;
        }
        catch (RequestFailedException ex) when (ex.Status == 409 && ex.ErrorCode == "BlobAlreadyExists")
        {
            // PutBlob with If-None-Match: * returns 409 BlobAlreadyExists when the
            // blob exists (not 412 — that's the IfMatch ETag-mismatch shape).
            return WriteResult.AlreadyExists;
        }
        catch (Exception ex)
        {
            // Structured properties (blobPath, what, runId) surface as OpenTelemetry
            // log attributes and land in AppTraces. Exception type/message come from
            // the logged Exception argument. Previously duplicated as a TrackEvent.
            _logger.LogWarning(ex,
                "Failed to write {What} for {RunId} at {BlobPath}; tracking blob will be retained for retry",
                what, runId, blobPath);
            return WriteResult.Failed;
        }
    }
}
