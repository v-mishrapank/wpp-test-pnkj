using IngestDispatcher.Functions.Functions;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;

namespace IngestDispatcher.Functions.Tests;

// Pure-function tests for ParseSince. End-to-end function tests would need
// a fake BlobServiceClient; integration testing is left to a deployed run.
public class ParseSinceTests
{
    [Theory]
    [InlineData("24h", 24 * 60 * 60)]
    [InlineData("7d", 7 * 24 * 60 * 60)]
    [InlineData("72h", 72 * 60 * 60)]
    [InlineData("30D", 30 * 24 * 60 * 60)]
    [InlineData("60m", 60 * 60)]
    public void ParseSince_ValidValues_Parsed(string raw, int expectedSeconds)
    {
        var actual = RunsFunction.ParseSince(raw);
        Assert.NotNull(actual);
        Assert.Equal(expectedSeconds, (int)actual!.Value.TotalSeconds);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    public void ParseSince_Empty_ReturnsNull(string? raw)
    {
        Assert.Null(RunsFunction.ParseSince(raw!));
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("24")]
    [InlineData("xh")]
    [InlineData("24z")]
    [InlineData("-1d")]
    public void ParseSince_Invalid_Throws(string raw)
    {
        Assert.Throws<ArgumentException>(() => RunsFunction.ParseSince(raw));
    }
}

// Pure-function tests for the active+history merge used by /runs. Verifies
// the claim-wins precedence (#404) — the previous history-wins rule briefly
// surfaced terminal status for runs whose claim hadn't yet been deleted.
public class MergeActiveAndHistoryTests
{
    private static RunListEntry Entry(string runId, string status, int tasksCompleted = 0) =>
        new(
            RunId: runId,
            JobName: "daily",
            TenantKey: "madev1",
            RunType: RunTypes.Normal,
            Status: status,
            StartedAt: DateTimeOffset.UtcNow,
            CompletedAt: status == RunListStatusValues.Running ? null : DateTimeOffset.UtcNow,
            ElapsedSeconds: 0,
            TaskCount: 6,
            TasksCompleted: tasksCompleted);

    [Fact]
    public void BothEmpty_ReturnsEmpty()
    {
        var merged = RunsFunction.MergeActiveAndHistory([], []);
        Assert.Empty(merged);
    }

    [Fact]
    public void ActiveOnly_ReturnedAsIs()
    {
        var active = new[] { Entry("rid-1", RunListStatusValues.Running, 3) };
        var merged = RunsFunction.MergeActiveAndHistory(active, []);
        Assert.Single(merged);
        Assert.Equal("rid-1", merged[0].RunId);
        Assert.Equal(RunListStatusValues.Running, merged[0].Status);
    }

    [Fact]
    public void HistoryOnly_ReturnedAsIs()
    {
        var history = new[] { Entry("rid-1", RunStatuses.Completed) };
        var merged = RunsFunction.MergeActiveAndHistory([], history);
        Assert.Single(merged);
        Assert.Equal(RunStatuses.Completed, merged[0].Status);
    }

    [Fact]
    public void RunInBothCollections_ClaimWins()
    {
        // The race the issue fixes: a finalization tick wrote history but
        // either hasn't deleted the claim yet, or its delete is mid-flight.
        // The list endpoint must show the in-flight (running) projection
        // until the claim is gone, not flicker to the terminal status.
        var active = new[] { Entry("rid-1", RunListStatusValues.Running, 5) };
        var history = new[] { Entry("rid-1", RunStatuses.CompletedWithErrors) };

        var merged = RunsFunction.MergeActiveAndHistory(active, history);

        Assert.Single(merged);
        Assert.Equal(RunListStatusValues.Running, merged[0].Status);
        Assert.Equal(5, merged[0].TasksCompleted);
        Assert.Null(merged[0].CompletedAt);
    }

    [Fact]
    public void MixedCollections_EachRunResolvedIndependently()
    {
        var active = new[]
        {
            Entry("rid-active-only", RunListStatusValues.Running, 1),
            Entry("rid-both", RunListStatusValues.Running, 2),
        };
        var history = new[]
        {
            Entry("rid-history-only", RunStatuses.Completed),
            Entry("rid-both", RunStatuses.Completed),
        };

        var merged = RunsFunction.MergeActiveAndHistory(active, history)
            .ToDictionary(e => e.RunId);

        Assert.Equal(3, merged.Count);
        Assert.Equal(RunListStatusValues.Running, merged["rid-active-only"].Status);
        Assert.Equal(RunStatuses.Completed, merged["rid-history-only"].Status);
        Assert.Equal(RunListStatusValues.Running, merged["rid-both"].Status);
    }
}
