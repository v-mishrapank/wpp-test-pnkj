using IngestDispatcher.Functions.Services;

namespace IngestDispatcher.Functions.Tests;

// Pins the precedence table for #382. Pre-fix, all six cancel/timeout
// scenarios below collapsed to RunStatuses.Failed.
public class ComputeRunStatusTests
{
    private static RunTracker.ResolvedTaskOutcome T(string status) =>
        new(ContainerType: "caj-graph",
            AcaExecutionName: "exec-1",
            Entities: ["entra_users"],
            Status: status,
            CompletedAt: DateTimeOffset.UtcNow,
            ErrorMessage: null);

    [Fact]
    public void AllSucceeded_RollsUpCompleted()
    {
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Succeeded), T(TaskStatuses.Succeeded)]);
        Assert.Equal(RunStatuses.Completed, status);
    }

    [Fact]
    public void MixedSucceededAndFailed_RollsUpCompletedWithErrors()
    {
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Succeeded), T(TaskStatuses.Failed)]);
        Assert.Equal(RunStatuses.CompletedWithErrors, status);
    }

    [Fact]
    public void AllFailed_RollsUpFailed()
    {
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Failed), T(TaskStatuses.Failed)]);
        Assert.Equal(RunStatuses.Failed, status);
    }

    [Fact]
    public void AllCancelled_RollsUpCancelled()
    {
        // Matches the live example from #382 (run a7ea2c34569e) — single
        // task cancelled by operator, observed run.status="failed" pre-fix.
        var status = RunTracker.ComputeRunStatus([T(TaskStatuses.Cancelled)]);
        Assert.Equal(RunStatuses.Cancelled, status);
    }

    [Fact]
    public void AllTimedOut_RollsUpTimedOut()
    {
        var status = RunTracker.ComputeRunStatus([T(TaskStatuses.TimedOut)]);
        Assert.Equal(RunStatuses.TimedOut, status);
    }

    [Fact]
    public void SucceededPlusCancelled_CancelDominates()
    {
        // Operator-intent dominates partial success: the run didn't run to
        // completion.
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Succeeded), T(TaskStatuses.Cancelled)]);
        Assert.Equal(RunStatuses.Cancelled, status);
    }

    [Fact]
    public void SucceededPlusTimedOut_TimeoutDominates()
    {
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Succeeded), T(TaskStatuses.TimedOut)]);
        Assert.Equal(RunStatuses.TimedOut, status);
    }

    [Fact]
    public void CancelledPlusTimedOut_TimeoutDominatesCancel()
    {
        // Only one cancel intent exists per run, so this combo is mostly
        // theoretical — but the precedence is pinned: deadline trumps
        // operator intent.
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Cancelled), T(TaskStatuses.TimedOut)]);
        Assert.Equal(RunStatuses.TimedOut, status);
    }

    [Fact]
    public void CancelledPlusFailed_CancelStillDominates()
    {
        // A task that organically failed before the cancel landed doesn't
        // demote the run-level rollup from cancelled to failed.
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Cancelled), T(TaskStatuses.Failed)]);
        Assert.Equal(RunStatuses.Cancelled, status);
    }

    // #509: Partial and Skipped are "data landed" — not failures.

    [Fact]
    public void AllPartial_RollsUpCompletedWithErrors()
    {
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Partial)]);
        Assert.Equal(RunStatuses.CompletedWithErrors, status);
    }

    [Fact]
    public void SucceededPlusPartial_RollsUpCompletedWithErrors()
    {
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Succeeded), T(TaskStatuses.Partial)]);
        Assert.Equal(RunStatuses.CompletedWithErrors, status);
    }

    [Fact]
    public void AllSkipped_RollsUpCompleted()
    {
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Skipped), T(TaskStatuses.Skipped)]);
        Assert.Equal(RunStatuses.Completed, status);
    }

    [Fact]
    public void SucceededPlusSkipped_RollsUpCompleted()
    {
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Succeeded), T(TaskStatuses.Skipped)]);
        Assert.Equal(RunStatuses.Completed, status);
    }

    [Fact]
    public void PartialPlusFailed_RollsUpCompletedWithErrors()
    {
        // At least one task landed data (partial), so the run isn't a
        // total failure — CompletedWithErrors distinguishes from Failed.
        var status = RunTracker.ComputeRunStatus(
            [T(TaskStatuses.Partial), T(TaskStatuses.Failed)]);
        Assert.Equal(RunStatuses.CompletedWithErrors, status);
    }
}
