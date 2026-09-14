using IngestDispatcher.Functions.Functions;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IngestDispatcher.Functions.Tests;

public class IngestTimerFunctionTests
{
    private readonly IConfigLoader _config = Substitute.For<IConfigLoader>();
    private readonly IRunExecutor _executor = Substitute.For<IRunExecutor>();
    private readonly IRunTracker _tracker = Substitute.For<IRunTracker>();
    private readonly IRunCanceller _canceller = Substitute.For<IRunCanceller>();
    private readonly ICronStateStore _cronState = Substitute.For<ICronStateStore>();
    private readonly IRunStateReader _runState = Substitute.For<IRunStateReader>();
    private readonly IngestTimerFunction _function;

    public IngestTimerFunctionTests()
    {
        _function = new IngestTimerFunction(
            _config, _executor, _tracker, _canceller, _cronState, _runState,
            Substitute.For<ILogger<IngestTimerFunction>>());
    }

    private static JobDefinition JobEveryMinute(string name = "every-min", bool enabled = true) =>
        new(name, "d", "* * * * *", enabled,
            new EntitySelector(IncludeTiers: [1]),
            new TenantSelector(TenantSelectorModes.All));

    private static JobDefinition JobDaily(string name, string cron = "0 12 * * *") =>
        new(name, "d", cron, true,
            new EntitySelector(IncludeTiers: [1]),
            new TenantSelector(TenantSelectorModes.All));

    [Fact]
    public async Task FirstEverTick_NoExistingState_DoesNotRetroactivelyDispatchDaily()
    {
        // Cron is daily at noon; we're running at 3pm with no prior state.
        // First-ever default lastEval = now-60s, so we shouldn't re-fire
        // today's noon occurrence.
        _config.Jobs.Returns(new JobsConfig([JobDaily("noon")]));
        _cronState.LoadAsync().Returns(new CronState());

        await _function.RunAsync(new TimerInfo());

        await _executor.DidNotReceive().ExecuteAsync(
            Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>());
        await _cronState.Received(1).SaveAsync(Arg.Any<CronState>());
    }

    [Fact]
    public async Task CatchUpTick_StatePredatesOccurrence_DispatchesOnce()
    {
        // lastEval was 25 hours ago; daily-at-noon means we missed yesterday's noon.
        // Catch-up should dispatch once.
        var job = JobDaily("noon");
        _config.Jobs.Returns(new JobsConfig([job]));
        _cronState.LoadAsync().Returns(new CronState
        {
            LastEvaluated = { [job.Name] = DateTimeOffset.UtcNow.AddHours(-25) }
        });

        await _function.RunAsync(new TimerInfo());

        await _executor.Received(1).ExecuteAsync(
            Arg.Is<JobDefinition>(j => j.Name == job.Name),
            TriggerTypes.Scheduled, Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>());
    }

    [Fact]
    public async Task DisabledJob_Skipped()
    {
        _config.Jobs.Returns(new JobsConfig([JobEveryMinute("off", enabled: false)]));
        _cronState.LoadAsync().Returns(new CronState
        {
            LastEvaluated = { ["off"] = DateTimeOffset.UtcNow.AddHours(-1) }
        });

        await _function.RunAsync(new TimerInfo());

        await _executor.DidNotReceive().ExecuteAsync(
            Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>());
    }

    [Fact]
    public async Task JobWithoutCron_Skipped()
    {
        var adhocShape = new JobDefinition(
            "no-cron", "d", null, true,
            new EntitySelector(IncludeTiers: [1]),
            new TenantSelector(TenantSelectorModes.All));
        _config.Jobs.Returns(new JobsConfig([adhocShape]));
        _cronState.LoadAsync().Returns(new CronState());

        await _function.RunAsync(new TimerInfo());

        await _executor.DidNotReceive().ExecuteAsync(
            Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>());
    }

    [Fact]
    public async Task StatePruned_ForJobsNoLongerInConfig()
    {
        // Config has one job; state has entries for that job plus a stale one.
        _config.Jobs.Returns(new JobsConfig([JobEveryMinute("still-here")]));

        var loadedState = new CronState
        {
            LastEvaluated =
            {
                ["still-here"] = DateTimeOffset.UtcNow.AddMinutes(-10),
                ["removed-job"] = DateTimeOffset.UtcNow.AddMinutes(-10),
                ["also-removed"] = DateTimeOffset.UtcNow.AddHours(-2)
            }
        };
        _cronState.LoadAsync().Returns(loadedState);

        CronState? savedState = null;
        await _cronState.SaveAsync(Arg.Do<CronState>(s => savedState = s));

        await _function.RunAsync(new TimerInfo());

        Assert.NotNull(savedState);
        Assert.Contains("still-here", savedState!.LastEvaluated.Keys);
        Assert.DoesNotContain("removed-job", savedState.LastEvaluated.Keys);
        Assert.DoesNotContain("also-removed", savedState.LastEvaluated.Keys);
    }

    [Fact]
    public async Task CheckActiveRunsAsync_CalledBeforeEvaluation()
    {
        _config.Jobs.Returns(new JobsConfig([]));
        _cronState.LoadAsync().Returns(new CronState());

        await _function.RunAsync(new TimerInfo());

        await _tracker.Received(1).CheckActiveRunsAsync();
    }

    [Fact]
    public async Task ExecuteAsyncThrows_LastEvalNotAdvancedForThatJob()
    {
        var job = JobEveryMinute("flaky");
        _config.Jobs.Returns(new JobsConfig([job]));
        var originalLastEval = DateTimeOffset.UtcNow.AddMinutes(-5);
        _cronState.LoadAsync().Returns(new CronState
        {
            LastEvaluated = { [job.Name] = originalLastEval }
        });
        _executor.ExecuteAsync(
                Arg.Any<JobDefinition>(), Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>())
            .Returns<Task<DispatchOutcome>>(_ => throw new InvalidOperationException("boom"));

        CronState? savedState = null;
        await _cronState.SaveAsync(Arg.Do<CronState>(s => savedState = s));

        await _function.RunAsync(new TimerInfo());

        // LastEval for the throwing job was NOT advanced, so the next tick
        // re-evaluates the same window and re-fires the missed occurrence.
        Assert.NotNull(savedState);
        Assert.Equal(originalLastEval, savedState!.LastEvaluated[job.Name]);
    }

    [Fact]
    public async Task ExecutorException_CaughtPerJob_DoesNotHaltOthers()
    {
        var job1 = JobEveryMinute("one");
        var job2 = JobEveryMinute("two");
        _config.Jobs.Returns(new JobsConfig([job1, job2]));

        _cronState.LoadAsync().Returns(new CronState
        {
            LastEvaluated =
            {
                [job1.Name] = DateTimeOffset.UtcNow.AddMinutes(-5),
                [job2.Name] = DateTimeOffset.UtcNow.AddMinutes(-5)
            }
        });

        _executor.ExecuteAsync(
                Arg.Is<JobDefinition>(j => j.Name == "one"),
                Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>())
            .Returns<Task<DispatchOutcome>>(_ => throw new InvalidOperationException("boom"));

        _executor.ExecuteAsync(
                Arg.Is<JobDefinition>(j => j.Name == "two"),
                Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>())
            .Returns(new DispatchOutcome(
                [new TenantOutcome("t", "id", DispatchResult.Dispatched, 1, 0, [])],
                SkippedEmpty: false));

        await _function.RunAsync(new TimerInfo());

        // Job two still dispatched despite job one throwing.
        await _executor.Received(1).ExecuteAsync(
            Arg.Is<JobDefinition>(j => j.Name == "two"),
            Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>());
    }

    [Fact]
    public async Task ExecutorCancelled_AbortsCronScan_DoesNotDispatchRemainingJobs()
    {
        // #570 safe-point: an OperationCanceledException from dispatch must
        // propagate (drained host), not be swallowed per-job like an organic
        // failure — otherwise every remaining job re-fires against a dead token.
        var job1 = JobEveryMinute("one");
        var job2 = JobEveryMinute("two");
        _config.Jobs.Returns(new JobsConfig([job1, job2]));
        _cronState.LoadAsync().Returns(new CronState
        {
            LastEvaluated =
            {
                [job1.Name] = DateTimeOffset.UtcNow.AddMinutes(-5),
                [job2.Name] = DateTimeOffset.UtcNow.AddMinutes(-5)
            }
        });

        _executor.ExecuteAsync(
                Arg.Is<JobDefinition>(j => j.Name == "one"),
                Arg.Any<string>(), Arg.Any<string?>(),
                Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>())
            .Returns<Task<DispatchOutcome>>(_ => throw new OperationCanceledException());

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _function.RunAsync(new TimerInfo()));

        await _executor.DidNotReceive().ExecuteAsync(
            Arg.Is<JobDefinition>(j => j.Name == "two"),
            Arg.Any<string>(), Arg.Any<string?>(),
            Arg.Any<IReadOnlyList<string>?>(), Arg.Any<EntitySelector?>(), Arg.Any<bool>());
    }

    [Fact]
    public async Task CancelledToken_SkipsDispatchAtPerJobSafePoint()
    {
        var job = JobEveryMinute("due");
        _config.Jobs.Returns(new JobsConfig([job]));
        _cronState.LoadAsync().Returns(new CronState
        {
            LastEvaluated = { [job.Name] = DateTimeOffset.UtcNow.AddMinutes(-5) }
        });

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _function.RunAsync(new TimerInfo(), cts.Token));

        await _executor.DidNotReceiveWithAnyArgs().ExecuteAsync(
            default!, default!, default!, default!, default!, default!);
    }
}
