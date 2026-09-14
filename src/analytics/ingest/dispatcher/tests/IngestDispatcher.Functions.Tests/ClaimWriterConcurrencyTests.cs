using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IngestDispatcher.Functions.Tests;

// Direct ClaimWriter coverage for the concurrency-control behavior added in
// issue #323. The other dispatcher tests mock IClaimWriter wholesale; these
// tests substitute the underlying BlobClient so we can verify the actual
// IfMatch/IfNoneMatch wiring and 412 handling that the fix depends on.
public class ClaimWriterConcurrencyTests
{
    private static readonly ClaimRecord SampleClaim = new()
    {
        RunId = "run-1",
        JobName = "daily",
        TenantKey = "t1",
        RunType = "normal",
        TriggerType = "manual",
        StartedAt = DateTimeOffset.UtcNow,
        ResolvedEntities = ["entra_users"],
        ExpectedTasks = [],
    };

    private static (ClaimWriter writer, BlobClient blob) BuildWriter()
    {
        var blob = Substitute.For<BlobClient>();
        var container = Substitute.For<BlobContainerClient>();
        container.GetBlobClient(Arg.Any<string>()).Returns(blob);
        var service = Substitute.For<BlobServiceClient>();
        service.GetBlobContainerClient(Arg.Any<string>()).Returns(container);

        var config = Substitute.For<IConfigLoader>();
        config.Storage.Returns(new StorageConfig(
            "https://test.dfs.core.windows.net", "landing",
            new StorageAuthConfig(StorageAuthMethods.ManagedIdentity)));

        var writer = new ClaimWriter(config, service,
            new JsonSerializerOptions(),
            Substitute.For<ILogger<ClaimWriter>>());
        return (writer, blob);
    }

    private static Response<BlobContentInfo> SuccessResponse(string etag)
    {
        var info = BlobsModelFactory.BlobContentInfo(
            eTag: new ETag(etag),
            lastModified: DateTimeOffset.UtcNow,
            contentHash: null,
            versionId: null,
            encryptionKeySha256: null,
            encryptionScope: null,
            blobSequenceNumber: 0);
        return Response.FromValue(info, Substitute.For<Response>());
    }

    private static Response<BlobProperties> PropertiesResponse(string etag)
    {
        var props = BlobsModelFactory.BlobProperties(eTag: new ETag(etag));
        return Response.FromValue(props, Substitute.For<Response>());
    }

    private static RequestFailedException PreconditionFailed() =>
        new(412, "The condition specified using HTTP conditional header(s) is not met.",
            "ConditionNotMet", innerException: null);

    [Fact]
    public async Task UpdateExecutionNamesAsync_WithMatchingETag_PassesIfMatchAndReturnsUpdated()
    {
        var (writer, blob) = BuildWriter();
        blob.UploadAsync(Arg.Any<Stream>(), Arg.Any<BlobUploadOptions>())
            .Returns(SuccessResponse("etag-after"));

        var claim = SampleClaim with { ETag = "etag-before" };
        var result = await writer.UpdateExecutionNamesAsync(claim);

        Assert.Equal(UpdateResult.Updated, result);
        await blob.Received(1).UploadAsync(
            Arg.Any<Stream>(),
            Arg.Is<BlobUploadOptions>(o =>
                o.Conditions != null
                && o.Conditions.IfMatch == new ETag("etag-before")));
    }

    [Fact]
    public async Task UpdateExecutionNamesAsync_On412_ReturnsConcurrentlyModified()
    {
        var (writer, blob) = BuildWriter();
        blob.UploadAsync(Arg.Any<Stream>(), Arg.Any<BlobUploadOptions>())
            .Returns<Task<Response<BlobContentInfo>>>(_ => throw PreconditionFailed());

        var claim = SampleClaim with { ETag = "stale-etag" };
        var result = await writer.UpdateExecutionNamesAsync(claim);

        Assert.Equal(UpdateResult.ConcurrentlyModified, result);
    }

    [Fact]
    public async Task TwoConcurrentSecondWrites_ProduceOneUpdatedAndOneConcurrentlyModified()
    {
        // The issue's acceptance test. Simulates the real ETag-CAS server
        // semantic: the blob's "current ETag" advances on every successful
        // upload, and any IfMatch against a stale ETag returns 412. Two
        // writers that both started with the same claim therefore see
        // exactly one Updated and one ConcurrentlyModified — never two
        // Updateds (the silent-stomp bug) and never two failures.
        var (writer, blob) = BuildWriter();

        var currentETag = "v1";
        blob.UploadAsync(Arg.Any<Stream>(), Arg.Any<BlobUploadOptions>())
            .Returns(call =>
            {
                var opts = call.Arg<BlobUploadOptions>();
                var requestedIfMatch = opts.Conditions?.IfMatch?.ToString();
                if (requestedIfMatch != currentETag)
                    throw PreconditionFailed();
                currentETag = $"v{Guid.NewGuid().ToString("N")[..4]}";
                return SuccessResponse(currentETag);
            });

        var claimA = SampleClaim with { ETag = "v1" };
        var claimB = SampleClaim with { ETag = "v1" };

        // Sequential resolution is sufficient — ETag CAS serializes regardless
        // of whether the calls overlap on the wire. The point is that both
        // writers carry the same read-time ETag and only one of their writes
        // can satisfy IfMatch.
        var resultA = await writer.UpdateExecutionNamesAsync(claimA);
        var resultB = await writer.UpdateExecutionNamesAsync(claimB);

        var outcomes = new[] { resultA, resultB };
        Assert.Single(outcomes, UpdateResult.Updated);
        Assert.Single(outcomes, UpdateResult.ConcurrentlyModified);
    }

    [Fact]
    public async Task TryCreateAsync_ForceTrueWithExistingClaim_UsesIfMatchOnExistingETag()
    {
        // Forced create reads the existing claim's ETag via GetPropertiesAsync
        // and writes with IfMatch — so two concurrent forces produce one
        // winner (the original Race #1 fix). The non-forced path's
        // IfNoneMatch=* atomicity is preserved separately.
        var (writer, blob) = BuildWriter();
        blob.GetPropertiesAsync(Arg.Any<BlobRequestConditions>())
            .Returns(PropertiesResponse("existing-etag"));
        blob.UploadAsync(Arg.Any<Stream>(), Arg.Any<BlobUploadOptions>())
            .Returns(SuccessResponse("new-etag"));

        var result = await writer.TryCreateAsync(SampleClaim, force: true);

        Assert.Equal(CreateOutcome.Created, result.Outcome);
        Assert.Equal("new-etag", result.ETag);
        await blob.Received(1).UploadAsync(
            Arg.Any<Stream>(),
            Arg.Is<BlobUploadOptions>(o =>
                o.Conditions != null
                && o.Conditions.IfMatch == new ETag("existing-etag")));
    }

    [Fact]
    public async Task TryCreateAsync_ForceTrueLosesIfMatchRace_ReturnsRejectedConcurrent()
    {
        // Two forced dispatchers both read ETag X. The first writes
        // (advancing the blob's ETag); the second's IfMatch=X fails with
        // 412. The loser must surface as RejectedConcurrent, not as the
        // standard RejectedExists overlap path — operators should be told
        // they raced another forced call.
        var (writer, blob) = BuildWriter();
        blob.GetPropertiesAsync(Arg.Any<BlobRequestConditions>())
            .Returns(PropertiesResponse("existing-etag"));
        blob.UploadAsync(Arg.Any<Stream>(), Arg.Any<BlobUploadOptions>())
            .Returns<Task<Response<BlobContentInfo>>>(_ => throw PreconditionFailed());

        var result = await writer.TryCreateAsync(SampleClaim, force: true);

        Assert.Equal(CreateOutcome.RejectedConcurrent, result.Outcome);
        Assert.Null(result.ETag);
    }

    [Fact]
    public async Task TryCreateAsync_ForceTrueWithNoExistingClaim_FallsBackToIfNoneMatch()
    {
        // No existing claim → GetPropertiesAsync 404. Forced path falls
        // through to IfNoneMatch=* so we don't create a phantom blob;
        // succeeds normally, returning the new ETag for the second write.
        var (writer, blob) = BuildWriter();
        blob.GetPropertiesAsync(Arg.Any<BlobRequestConditions>())
            .Returns<Task<Response<BlobProperties>>>(_ =>
                throw new RequestFailedException(404, "Not Found", "BlobNotFound", null));
        blob.UploadAsync(Arg.Any<Stream>(), Arg.Any<BlobUploadOptions>())
            .Returns(SuccessResponse("first-etag"));

        var result = await writer.TryCreateAsync(SampleClaim, force: true);

        Assert.Equal(CreateOutcome.Created, result.Outcome);
        Assert.Equal("first-etag", result.ETag);
        await blob.Received(1).UploadAsync(
            Arg.Any<Stream>(),
            Arg.Is<BlobUploadOptions>(o =>
                o.Conditions != null
                && o.Conditions.IfNoneMatch == ETag.All));
    }

    [Fact]
    public async Task TryCreateAsync_ForceFalseExistingClaim_ReturnsRejectedExists()
    {
        // Non-forced create against an existing claim is the standard
        // overlap path: 409 BlobAlreadyExists from IfNoneMatch=* maps to
        // RejectedExists (the original behavior), not RejectedConcurrent.
        var (writer, blob) = BuildWriter();
        blob.UploadAsync(Arg.Any<Stream>(), Arg.Any<BlobUploadOptions>())
            .Returns<Task<Response<BlobContentInfo>>>(_ =>
                throw new RequestFailedException(409, "Already exists", "BlobAlreadyExists", null));

        var result = await writer.TryCreateAsync(SampleClaim, force: false);

        Assert.Equal(CreateOutcome.RejectedExists, result.Outcome);
        Assert.Null(result.ETag);
    }
}
