using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using IngestDispatcher.Functions.Models;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Services;

public enum CancelIntentCreateOutcome
{
    Created,

    // A cancel-intent blob already exists for this run_id. Caller should
    // treat this as idempotent — the run is already cancelling. The
    // existing intent is returned in the result so the caller can reflect
    // its current state in the HTTP response.
    AlreadyExists,
}

public record CancelIntentCreateResult(
    CancelIntentCreateOutcome Outcome,
    CancelIntent? Intent);

public interface ICancelIntentStore
{
    // Atomic create with If-None-Match: *. Returns AlreadyExists when an
    // intent already exists for this run_id (the existing intent is loaded
    // and returned). Used by both the timeout-fire path and the manual
    // cancel endpoint.
    Task<CancelIntentCreateResult> TryCreateAsync(CancelIntent intent);

    // ETag-protected update. Used by the cancel-state transitions (e.g.
    // cancel_requested → cancel_submitted, → cancel_stalled). Returns false
    // if the ETag no longer matches — caller should reload and retry.
    Task<bool> TryUpdateAsync(CancelIntent intent);

    // Returns null if no cancel-intent blob exists for this run_id.
    Task<CancelIntent?> TryReadAsync(string runId);

    // Lists every existing cancel-intent. Used by the reconciler.
    Task<IReadOnlyList<CancelIntent>> ListAllAsync();

    // Idempotent delete. Called by RunTracker after finalization, and by
    // the force-release endpoint.
    Task DeleteAsync(string runId);
}

public class CancelIntentStore : ICancelIntentStore
{
    public const string CancelPrefix = "_dispatcher/cancellations/";

    private readonly BlobContainerClient _container;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ILogger<CancelIntentStore> _logger;

    public CancelIntentStore(
        IConfigLoader configLoader,
        BlobServiceClient blobServiceClient,
        JsonSerializerOptions jsonOptions,
        ILogger<CancelIntentStore> logger)
    {
        _container = blobServiceClient.GetBlobContainerClient(configLoader.Storage.Container);
        _jsonOptions = jsonOptions;
        _logger = logger;
    }

    public static string IntentBlobPath(string runId) => $"{CancelPrefix}{runId}.json";

    public async Task<CancelIntentCreateResult> TryCreateAsync(CancelIntent intent)
    {
        var blob = _container.GetBlobClient(IntentBlobPath(intent.RunId));
        var json = JsonSerializer.Serialize(intent, _jsonOptions);
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        var options = new BlobUploadOptions
        {
            Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All }
        };

        try
        {
            var response = await blob.UploadAsync(stream, options);
            _logger.LogInformation(
                "Created cancel intent for run {RunId} (trigger={Trigger}, reason={Reason})",
                intent.RunId, intent.CancelTrigger, intent.CancelReason ?? "(none)");
            return new CancelIntentCreateResult(
                CancelIntentCreateOutcome.Created,
                intent with { ETag = response.Value.ETag.ToString() });
        }
        catch (RequestFailedException ex) when (ex.Status == 409 && ex.ErrorCode == "BlobAlreadyExists")
        {
            var existing = await TryReadAsync(intent.RunId);
            return new CancelIntentCreateResult(
                CancelIntentCreateOutcome.AlreadyExists,
                existing);
        }
    }

    public async Task<bool> TryUpdateAsync(CancelIntent intent)
    {
        var blob = _container.GetBlobClient(IntentBlobPath(intent.RunId));
        var json = JsonSerializer.Serialize(intent, _jsonOptions);
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        var options = new BlobUploadOptions();
        if (intent.ETag is not null)
        {
            options.Conditions = new BlobRequestConditions { IfMatch = new ETag(intent.ETag) };
        }

        try
        {
            await blob.UploadAsync(stream, options);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 412)
        {
            _logger.LogWarning(
                "Cancel intent for run {RunId} was concurrently modified; caller should reload",
                intent.RunId);
            return false;
        }
    }

    public async Task<CancelIntent?> TryReadAsync(string runId)
    {
        var blob = _container.GetBlobClient(IntentBlobPath(runId));
        try
        {
            var response = await blob.DownloadContentAsync();
            var intent = JsonSerializer.Deserialize<CancelIntent>(
                response.Value.Content.ToString(), _jsonOptions);
            return intent is null ? null
                : intent with { ETag = response.Value.Details.ETag.ToString() };
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<CancelIntent>> ListAllAsync()
    {
        var results = new List<CancelIntent>();
        await foreach (var blobItem in _container.GetBlobsAsync(prefix: CancelPrefix))
        {
            if (!blobItem.Name.EndsWith(".json", StringComparison.Ordinal)) continue;
            try
            {
                var blob = _container.GetBlobClient(blobItem.Name);
                var response = await blob.DownloadContentAsync();
                var intent = JsonSerializer.Deserialize<CancelIntent>(
                    response.Value.Content.ToString(), _jsonOptions);
                if (intent != null)
                    results.Add(intent with { ETag = response.Value.Details.ETag.ToString() });
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to read cancel intent blob {Blob}", blobItem.Name);
            }
        }
        return results;
    }

    public async Task DeleteAsync(string runId)
    {
        try
        {
            await _container.GetBlobClient(IntentBlobPath(runId)).DeleteIfExistsAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete cancel intent blob for run {RunId}", runId);
        }
    }
}
