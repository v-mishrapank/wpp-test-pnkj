using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IngestDispatcher.Functions.Tests;

// Coverage for the retention sweep (#385). Pre-#385 this class also tested
// DeleteForRunAsync — that method is gone now because the blob is durable
// past finalization. The sweep is the only retention mechanism.
public class RunStateReaderTests
{
    private static BlobItem ItemAt(string name, DateTimeOffset lastModified)
    {
        var props = BlobsModelFactory.BlobItemProperties(
            accessTierInferred: false,
            lastModified: lastModified);
        return BlobsModelFactory.BlobItem(name: name, properties: props);
    }

    private static (RunStateReader reader, BlobContainerClient container) BuildReader(
        int sweepIntervalMinutes = 0,
        int orphanAgeThresholdHours = 720)
    {
        var container = Substitute.For<BlobContainerClient>();
        var service = Substitute.For<BlobServiceClient>();
        service.GetBlobContainerClient(Arg.Any<string>()).Returns(container);

        var config = Substitute.For<IConfigLoader>();
        config.Storage.Returns(new StorageConfig(
            "https://test.dfs.core.windows.net", "landing",
            new StorageAuthConfig(StorageAuthMethods.ManagedIdentity),
            OrphanSweepIntervalMinutes: sweepIntervalMinutes,
            OrphanAgeThresholdHours: orphanAgeThresholdHours));

        var reader = new RunStateReader(
            config, service,
            new JsonSerializerOptions(),
            new HeartbeatCache(),
            Substitute.For<ILogger<RunStateReader>>());
        return (reader, container);
    }

    private static AsyncPageable<BlobItem> Pageable(IEnumerable<BlobItem> items)
    {
        var page = Page<BlobItem>.FromValues(items.ToList(), continuationToken: null, Substitute.For<Response>());
        return AsyncPageable<BlobItem>.FromPages(new[] { page });
    }

    [Fact]
    public async Task SweepOrphansAsync_DeletesBlobsOlderThanThreshold()
    {
        var (reader, container) = BuildReader(orphanAgeThresholdHours: 24);
        var old = DateTimeOffset.UtcNow.AddDays(-2);
        container.GetBlobsAsync(prefix: RunStateReader.RunStatePrefix, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Pageable([
                ItemAt("_dispatcher/run_state/orphan-run/t/c.json", old)
            ]));
        var blob = Substitute.For<BlobClient>();
        container.GetBlobClient("_dispatcher/run_state/orphan-run/t/c.json").Returns(blob);

        await reader.SweepOrphansAsync();

        await blob.Received(1).DeleteIfExistsAsync(cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SweepOrphansAsync_SkipsBlobsWithinThreshold()
    {
        var (reader, container) = BuildReader(orphanAgeThresholdHours: 720);
        var recent = DateTimeOffset.UtcNow.AddDays(-1);
        container.GetBlobsAsync(prefix: RunStateReader.RunStatePrefix, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Pageable([
                ItemAt("_dispatcher/run_state/live-run/t/c.json", recent)
            ]));
        var blob = Substitute.For<BlobClient>();
        container.GetBlobClient(Arg.Any<string>()).Returns(blob);

        await reader.SweepOrphansAsync();

        await blob.DidNotReceive().DeleteIfExistsAsync(cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SweepOrphansAsync_RespectsSweepInterval()
    {
        var (reader, container) = BuildReader(sweepIntervalMinutes: 60);
        var old = DateTimeOffset.UtcNow.AddDays(-40);
        container.GetBlobsAsync(prefix: RunStateReader.RunStatePrefix, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(_ => Pageable([
                ItemAt("_dispatcher/run_state/orphan-run/t/c.json", old)
            ]));
        container.GetBlobClient(Arg.Any<string>()).Returns(Substitute.For<BlobClient>());

        await reader.SweepOrphansAsync();
        await reader.SweepOrphansAsync();

        // Second call hits the throttle before any I/O.
        container.Received(1).GetBlobsAsync(prefix: RunStateReader.RunStatePrefix, cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SweepOrphansAsync_SwallowsListingException()
    {
        var (reader, container) = BuildReader();
        container.GetBlobsAsync(prefix: RunStateReader.RunStatePrefix, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(_ => throw new RequestFailedException(503, "ServiceUnavailable"));

        // Must not throw — sweep is fire-and-forget from the tick's view.
        await reader.SweepOrphansAsync();
    }

    [Fact]
    public async Task TryReadAsync_404_ReturnsNull()
    {
        var (reader, container) = BuildReader();
        var blob = Substitute.For<BlobClient>();
        blob.DownloadContentAsync()
            .Returns<Task<Response<BlobDownloadResult>>>(_ =>
                throw new RequestFailedException(404, "NotFound"));
        container.GetBlobClient(Arg.Any<string>()).Returns(blob);

        var result = await reader.TryReadAsync("run-1", "t", "c");
        Assert.Null(result);
    }

    [Fact]
    public async Task TryReadAsync_TransientStorageError_Propagates()
    {
        // #385 Copilot finding 2: only 404 returns null. Other failures
        // propagate so RunTracker's per-task handler defers to the next
        // tick instead of silently misclassifying via ACA-fallback.
        var (reader, container) = BuildReader();
        var blob = Substitute.For<BlobClient>();
        blob.DownloadContentAsync()
            .Returns<Task<Response<BlobDownloadResult>>>(_ =>
                throw new RequestFailedException(503, "ServiceUnavailable"));
        container.GetBlobClient(Arg.Any<string>()).Returns(blob);

        await Assert.ThrowsAsync<RequestFailedException>(
            () => reader.TryReadAsync("run-1", "t", "c"));
    }

    [Fact]
    public async Task SweepOrphansAsync_ContinuesAfterPerBlobDeleteFailure()
    {
        var (reader, container) = BuildReader(orphanAgeThresholdHours: 24);
        var old = DateTimeOffset.UtcNow.AddDays(-2);
        container.GetBlobsAsync(prefix: RunStateReader.RunStatePrefix, cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Pageable([
                ItemAt("_dispatcher/run_state/a/t/c.json", old),
                ItemAt("_dispatcher/run_state/b/t/c.json", old)
            ]));
        var blobA = Substitute.For<BlobClient>();
        blobA.DeleteIfExistsAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns<Task<Response<bool>>>(_ => throw new RequestFailedException(500, "boom"));
        var blobB = Substitute.For<BlobClient>();
        container.GetBlobClient("_dispatcher/run_state/a/t/c.json").Returns(blobA);
        container.GetBlobClient("_dispatcher/run_state/b/t/c.json").Returns(blobB);

        await reader.SweepOrphansAsync();

        // Second blob still got attempted despite first failing.
        await blobB.Received(1).DeleteIfExistsAsync(cancellationToken: Arg.Any<CancellationToken>());
    }
}
