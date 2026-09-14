using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Services;

public interface ICronStateStore
{
    Task<CronState> LoadAsync();
    Task SaveAsync(CronState state);
}

public record CronState
{
    public Dictionary<string, DateTimeOffset> LastEvaluated { get; init; } = new();
}

public class CronStateStore : ICronStateStore
{
    private const string BlobPath = "_dispatcher/state/cron.json";

    private readonly BlobContainerClient _container;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ILogger<CronStateStore> _logger;

    public CronStateStore(
        IConfigLoader configLoader,
        BlobServiceClient blobServiceClient,
        JsonSerializerOptions jsonOptions,
        ILogger<CronStateStore> logger)
    {
        _jsonOptions = jsonOptions;
        _logger = logger;
        _container = blobServiceClient.GetBlobContainerClient(configLoader.Storage.Container);
    }

    public async Task<CronState> LoadAsync()
    {
        var blob = _container.GetBlobClient(BlobPath);
        try
        {
            var response = await blob.DownloadContentAsync();
            return JsonSerializer.Deserialize<CronState>(
                response.Value.Content.ToString(), _jsonOptions) ?? new CronState();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // First-ever load; no state blob exists yet.
            return new CronState();
        }
        catch (Exception ex)
        {
            // Storage transient / corrupt blob / permissions. Fall back to empty
            // state so the tick can continue. Overlap protection handles any
            // re-dispatch caused by lost lastEval. Exception type/message come
            // from the logged Exception argument; BlobPath is a structured
            // property that lands in AppTraces. Previously duplicated as a TrackEvent.
            _logger.LogWarning(ex,
                "Failed to load cron state from {BlobPath}; continuing with empty state", BlobPath);
            return new CronState();
        }
    }

    public async Task SaveAsync(CronState state)
    {
        var blob = _container.GetBlobClient(BlobPath);
        var json = JsonSerializer.Serialize(state, _jsonOptions);
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        await blob.UploadAsync(stream, overwrite: true);
    }
}
