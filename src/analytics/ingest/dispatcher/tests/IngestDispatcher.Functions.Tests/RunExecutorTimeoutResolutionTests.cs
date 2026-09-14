using IngestDispatcher.Functions.Services;

namespace IngestDispatcher.Functions.Tests;

// Pure-function tests for the MIN(adhoc, jobs.json, default) resolution
// used to stamp resolved_timeout_seconds onto the ClaimRecord at dispatch.
public class RunExecutorTimeoutResolutionTests
{
    [Fact]
    public void NoOverrides_PicksDefault()
    {
        Assert.Equal(
            RunExecutor.DefaultTimeoutSeconds,
            RunExecutor.ResolveTimeoutSeconds(adhocOverride: null, jobConfigured: null));
    }

    [Fact]
    public void OnlyJobConfigured_TightensFromDefault()
    {
        Assert.Equal(3600,
            RunExecutor.ResolveTimeoutSeconds(adhocOverride: null, jobConfigured: 3600));
    }

    [Fact]
    public void OnlyAdhoc_TightensFromDefault()
    {
        Assert.Equal(1800,
            RunExecutor.ResolveTimeoutSeconds(adhocOverride: 1800, jobConfigured: null));
    }

    [Fact]
    public void BothSet_TightestWins()
    {
        // Ad-hoc tighter than job.
        Assert.Equal(600,
            RunExecutor.ResolveTimeoutSeconds(adhocOverride: 600, jobConfigured: 3600));
        // Job tighter than ad-hoc.
        Assert.Equal(900,
            RunExecutor.ResolveTimeoutSeconds(adhocOverride: 7200, jobConfigured: 900));
    }

    [Fact]
    public void ZeroOrNegativeOverrides_AreIgnored()
    {
        // Bad input shouldn't loosen the default; ManualRunFunction rejects
        // these at the boundary, but the resolution is also defensive.
        Assert.Equal(RunExecutor.DefaultTimeoutSeconds,
            RunExecutor.ResolveTimeoutSeconds(adhocOverride: 0, jobConfigured: null));
        Assert.Equal(RunExecutor.DefaultTimeoutSeconds,
            RunExecutor.ResolveTimeoutSeconds(adhocOverride: -5, jobConfigured: -1));
    }

    [Fact]
    public void OverridesNeverLoosenBeyondDefault()
    {
        // A caller asking for 10d (864000) on a 7d (604800) default is
        // clipped — the dispatcher default is the ceiling.
        Assert.Equal(RunExecutor.DefaultTimeoutSeconds,
            RunExecutor.ResolveTimeoutSeconds(adhocOverride: 864000, jobConfigured: 864000));
    }
}
