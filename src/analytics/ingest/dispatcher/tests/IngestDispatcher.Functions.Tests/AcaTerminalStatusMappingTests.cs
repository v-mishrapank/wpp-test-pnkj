using IngestDispatcher.Functions.Services;

namespace IngestDispatcher.Functions.Tests;

// Pure-function tests for the ACA-fallback terminal-status mapping added in
// #322. Exercises the no-manifest path in RunTracker.CheckClaimAsync — each
// terminal value the dispatcher recognizes from JobExecutionRunningState
// must produce a TaskStatuses value, and non-terminal values must return
// null so the caller defers.
public class AcaTerminalStatusMappingTests
{
    private const string Execution = "caj-ma-toolkit-branch-graph-001-t1e77pl";

    [Fact]
    public void Succeeded_MapsToSucceeded_NoErrorMessage()
    {
        var result = RunTracker.MapAcaTerminalStatus(AcaExecutionStatuses.Succeeded, Execution);
        Assert.NotNull(result);
        Assert.Equal(TaskStatuses.Succeeded, result.Value.TaskStatus);
        Assert.Null(result.Value.ErrorMessage);
    }

    [Theory]
    [InlineData(AcaExecutionStatuses.Failed, "failed (no run-state blob)")]
    [InlineData(AcaExecutionStatuses.Stopped, "was stopped (no run-state blob)")]
    [InlineData(AcaExecutionStatuses.Degraded, "is degraded (no run-state blob)")]
    public void TerminalNonSuccess_MapsToFailed_WithDistinguishingErrorMessage(
        string acaStatus, string expectedFragment)
    {
        var result = RunTracker.MapAcaTerminalStatus(acaStatus, Execution);
        Assert.NotNull(result);
        Assert.Equal(TaskStatuses.Failed, result.Value.TaskStatus);
        Assert.NotNull(result.Value.ErrorMessage);
        Assert.Contains(expectedFragment, result.Value.ErrorMessage);
        // Execution name must appear in the message — operators rely on it
        // to correlate with `az containerapp job execution show`.
        Assert.Contains(Execution, result.Value.ErrorMessage);
    }

    [Theory]
    [InlineData("Running")]
    [InlineData("Processing")]
    [InlineData("Unknown")]
    [InlineData("")]
    [InlineData("future_status_we_havent_seen")]
    public void NonTerminal_ReturnsNull(string acaStatus)
    {
        // Defer branch — RunTracker keeps polling on the next tick (and the
        // 8h RunTimeout still backstops anything that never resolves).
        var result = RunTracker.MapAcaTerminalStatus(acaStatus, Execution);
        Assert.Null(result);
    }
}
