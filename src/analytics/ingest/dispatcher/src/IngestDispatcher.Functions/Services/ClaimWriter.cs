using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using IngestDispatcher.Functions.Models;
using Microsoft.Extensions.Logging;

namespace IngestDispatcher.Functions.Services;

public enum CreateOutcome
{
    // Claim was written successfully; ETag carries the new blob's ETag for
    // the second-write CAS.
    Created,

    // Non-forced create lost to an existing claim (409 BlobAlreadyExists on
    // If-None-Match: *). Standard overlap rejection.
    RejectedExists,

    // Forced create lost to a concurrent forced create (412 Precondition
    // Failed on If-Match, or 409 on the no-prior-claim fallback path).
    // The other writer's claim is the new source of truth.
    RejectedConcurrent,
}

public record TryCreateResult(CreateOutcome Outcome, string? ETag)
{
    public bool Created => Outcome == CreateOutcome.Created;
}

public enum UpdateResult
{
    Updated,

    // Second write found the claim's ETag had changed (412). Another writer
    // overwrote between TryCreateAsync and now — only possible when two
    // force=true dispatches race the same (job, tenant, runtype) slot.
    // Caller must NOT retry: the other writer's claim is now authoritative.
    ConcurrentlyModified,
}

public interface IClaimWriter
{
    // Atomic claim creation. force=false uses If-None-Match: * for overlap
    // protection. force=true reads the existing ETag and writes with If-Match
    // so two concurrent forces produce exactly one winner. Returns the new
    // blob's ETag in the TryCreateResult — caller must thread it onto
    // ClaimRecord.ETag for the second write.
    Task<TryCreateResult> TryCreateAsync(ClaimRecord claim, bool force);

    // Overwrite the claim with execution names (and any dispatch_error fields)
    // populated. Called exactly once per dispatch, after every StartJobAsync
    // call has returned. Uses If-Match: claim.ETag when present; returns
    // ConcurrentlyModified on 412 instead of throwing or silently stomping.
    Task<UpdateResult> UpdateExecutionNamesAsync(ClaimRecord claim);

    // Delete the claim during finalization. Idempotent.
    Task DeleteAsync(string jobName, string tenantKey, string runType);
}

public class ClaimWriter : IClaimWriter
{
    public const string TrackingPrefix = "_dispatcher/tracking/";

    private readonly BlobContainerClient _container;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ILogger<ClaimWriter> _logger;

    public ClaimWriter(
        IConfigLoader configLoader,
        BlobServiceClient blobServiceClient,
        JsonSerializerOptions jsonOptions,
        ILogger<ClaimWriter> logger)
    {
        _container = blobServiceClient.GetBlobContainerClient(configLoader.Storage.Container);
        _jsonOptions = jsonOptions;
        _logger = logger;
    }

    public static string ClaimBlobPath(string jobName, string tenantKey, string runType) =>
        $"{TrackingPrefix}{jobName}/{tenantKey}/{runType}.json";

    public async Task<TryCreateResult> TryCreateAsync(ClaimRecord claim, bool force)
    {
        var blob = _container.GetBlobClient(ClaimBlobPath(claim.JobName, claim.TenantKey, claim.RunType));

        if (force)
        {
            // Read existing ETag first; if no claim exists, fall through to the
            // If-None-Match: * path so we don't create-on-stale-state. If a
            // claim does exist, write with If-Match so two concurrent forces
            // produce exactly one winner. The non-forced path's atomicity
            // (single PutBlob with If-None-Match: *) is preserved unchanged.
            ETag? existingETag;
            try
            {
                var props = await blob.GetPropertiesAsync();
                existingETag = props.Value.ETag;
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                existingETag = null;
            }

            if (existingETag.HasValue)
            {
                return await WriteWithConditionAsync(blob, claim,
                    new BlobRequestConditions { IfMatch = existingETag.Value },
                    forceLog: true,
                    onPreconditionFailed: () =>
                    {
                        _logger.LogWarning(
                            "Forced claim for {Job}/{Tenant}/{RunType} lost a race against another forced dispatch",
                            claim.JobName, claim.TenantKey, claim.RunType);
                        return new TryCreateResult(CreateOutcome.RejectedConcurrent, null);
                    });
            }
            // No prior claim → use create-only condition. If a third writer
            // creates between our HEAD and our PUT, this 409s and we treat
            // it as the same "lost the race" outcome.
            return await WriteWithConditionAsync(blob, claim,
                new BlobRequestConditions { IfNoneMatch = ETag.All },
                forceLog: true,
                onAlreadyExists: () =>
                {
                    _logger.LogWarning(
                        "Forced claim for {Job}/{Tenant}/{RunType} lost a race against another dispatch (created between HEAD and PUT)",
                        claim.JobName, claim.TenantKey, claim.RunType);
                    return new TryCreateResult(CreateOutcome.RejectedConcurrent, null);
                });
        }

        return await WriteWithConditionAsync(blob, claim,
            new BlobRequestConditions { IfNoneMatch = ETag.All },
            onAlreadyExists: () =>
            {
                _logger.LogInformation(
                    "Job {JobName} tenant {TenantKey} already has an active run; claim rejected",
                    claim.JobName, claim.TenantKey);
                return new TryCreateResult(CreateOutcome.RejectedExists, null);
            });
    }

    private async Task<TryCreateResult> WriteWithConditionAsync(
        BlobClient blob,
        ClaimRecord claim,
        BlobRequestConditions conditions,
        Func<TryCreateResult>? onAlreadyExists = null,
        Func<TryCreateResult>? onPreconditionFailed = null,
        bool forceLog = false)
    {
        var json = JsonSerializer.Serialize(claim, _jsonOptions);
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        var options = new BlobUploadOptions { Conditions = conditions };

        try
        {
            var response = await blob.UploadAsync(stream, options);
            _logger.LogInformation(
                "Claimed run {RunId} for job {JobName} tenant {TenantKey} ({TaskCount} tasks){ForcedSuffix}",
                claim.RunId, claim.JobName, claim.TenantKey, claim.ExpectedTasks.Count,
                forceLog ? " (forced)" : string.Empty);
            return new TryCreateResult(CreateOutcome.Created, response.Value.ETag.ToString());
        }
        catch (RequestFailedException ex) when (ex.Status == 409 && ex.ErrorCode == "BlobAlreadyExists" && onAlreadyExists is not null)
        {
            return onAlreadyExists();
        }
        catch (RequestFailedException ex) when (ex.Status == 412 && onPreconditionFailed is not null)
        {
            return onPreconditionFailed();
        }
    }

    public async Task<UpdateResult> UpdateExecutionNamesAsync(ClaimRecord claim)
    {
        var blob = _container.GetBlobClient(ClaimBlobPath(claim.JobName, claim.TenantKey, claim.RunType));
        var json = JsonSerializer.Serialize(claim, _jsonOptions);
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

        var options = new BlobUploadOptions();
        if (claim.ETag is not null)
        {
            options.Conditions = new BlobRequestConditions { IfMatch = new ETag(claim.ETag) };
        }
        else
        {
            // No ETag means TryCreateAsync didn't run (callers wired without
            // the new path) or the claim was synthesized in a test. Fall back
            // to today's unconditional overwrite — but warn loudly because in
            // production this is a regression that re-opens the race.
            _logger.LogWarning(
                "UpdateExecutionNamesAsync called without ETag for {Job}/{Tenant}/{RunType} run {RunId}; falling back to unconditional overwrite",
                claim.JobName, claim.TenantKey, claim.RunType, claim.RunId);
        }

        try
        {
            await blob.UploadAsync(stream, options);
            return UpdateResult.Updated;
        }
        catch (RequestFailedException ex) when (ex.Status == 412)
        {
            _logger.LogWarning(
                "Claim for {Job}/{Tenant}/{RunType} run {RunId} was concurrently overwritten; this dispatcher's execution names will not be persisted",
                claim.JobName, claim.TenantKey, claim.RunType, claim.RunId);
            return UpdateResult.ConcurrentlyModified;
        }
    }

    public async Task DeleteAsync(string jobName, string tenantKey, string runType)
    {
        try
        {
            await _container.GetBlobClient(ClaimBlobPath(jobName, tenantKey, runType))
                .DeleteIfExistsAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete claim blob for {Job}/{Tenant}/{RunType}",
                jobName, tenantKey, runType);
        }
    }
}
