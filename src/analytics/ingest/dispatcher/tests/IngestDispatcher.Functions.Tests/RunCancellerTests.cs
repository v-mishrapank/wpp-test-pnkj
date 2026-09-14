using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace IngestDispatcher.Functions.Tests;

// Covers the cancel state machine: trigger paths (manual + timeout) flowing
// through RequestCancelAsync, and the tick-side ReconcileAsync that drives
// state forward without operator involvement.
public class RunCancellerTests
{
    private static ClaimRecord MakeClaim(
        string runId = "rid-001",
        DateTimeOffset? startedAt = null,
        int? resolvedTimeoutSeconds = null,
        params string[] executionNames)
    {
        startedAt ??= DateTimeOffset.UtcNow;
        var tasks = executionNames.Length == 0
            ? new List<ExpectedTask>
            {
                new() { ContainerType = "caj-graph", AcaExecutionName = "exec-1", Entities = ["entra_users"] },
            }
            : executionNames.Select((n, i) => new ExpectedTask
            {
                ContainerType = $"caj-{i}",
                AcaExecutionName = n,
                Entities = [$"entity-{i}"],
            }).ToList();
        return new ClaimRecord
        {
            RunId = runId,
            JobName = "daily",
            TenantKey = "madev1",
            RunType = RunTypes.Normal,
            TriggerType = TriggerTypes.Scheduled,
            StartedAt = startedAt.Value,
            ResolvedEntities = tasks.SelectMany(t => t.Entities).ToList(),
            ExpectedTasks = tasks,
            ResolvedTimeoutSeconds = resolvedTimeoutSeconds,
        };
    }

    private static RunCanceller Build(
        IClaimReader claimReader, ICancelIntentStore intentStore, IAcaJobClient aca) =>
        new(claimReader, intentStore, aca, Substitute.For<ILogger<RunCanceller>>());

    [Fact]
    public async Task RequestCancel_NoClaim_ReturnsNotFound()
    {
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.TryFindByRunIdAsync("ghost").Returns((ClaimRecord?)null);

        var canceller = Build(claimReader, Substitute.For<ICancelIntentStore>(), Substitute.For<IAcaJobClient>());

        var result = await canceller.RequestCancelAsync("ghost", CancelTriggers.Manual, null);

        Assert.Equal(RequestCancelOutcome.NotFound, result.Outcome);
        Assert.Null(result.Intent);
    }

    [Fact]
    public async Task RequestCancel_HappyPath_CreatesIntentAndSubmitsToAca()
    {
        var claim = MakeClaim();
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.TryCreateAsync(Arg.Any<CancelIntent>())
            .Returns(ci => new CancelIntentCreateResult(
                CancelIntentCreateOutcome.Created,
                ci.Arg<CancelIntent>() with { ETag = "etag-1" }));
        intentStore.TryUpdateAsync(Arg.Any<CancelIntent>()).Returns(true);

        var aca = Substitute.For<IAcaJobClient>();
        aca.CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>()).Returns(true);

        var canceller = Build(claimReader, intentStore, aca);
        var result = await canceller.RequestCancelAsync(
            claim.RunId, CancelTriggers.Manual, reason: "ops");

        Assert.Equal(RequestCancelOutcome.Submitted, result.Outcome);
        Assert.NotNull(result.Intent);
        Assert.Equal(CancelStates.CancelSubmitted, result.Intent!.CancelState);
        Assert.Equal(CancelTriggers.Manual, result.Intent.CancelTrigger);
        Assert.Equal("ops", result.Intent.CancelReason);
        Assert.NotNull(result.Intent.CancelSubmittedAt);
        await aca.Received(1).CancelExecutionAsync("caj-graph", "exec-1", Arg.Any<string?>());
    }

    [Fact]
    public async Task RequestCancel_ArmSubmitTransient_StillSubmittedWithPerTaskOutcomes()
    {
        var claim = MakeClaim();
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.TryCreateAsync(Arg.Any<CancelIntent>())
            .Returns(ci => new CancelIntentCreateResult(
                CancelIntentCreateOutcome.Created,
                ci.Arg<CancelIntent>() with { ETag = "etag-1" }));
        intentStore.TryUpdateAsync(Arg.Any<CancelIntent>()).Returns(true);

        var aca = Substitute.For<IAcaJobClient>();
        aca.CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>()).Returns(false);

        var canceller = Build(claimReader, intentStore, aca);
        var result = await canceller.RequestCancelAsync(
            claim.RunId, CancelTriggers.Manual, null);

        // Intent persisted — caller's contract is "intent recorded";
        // reconciler handles the un-landed ARM stops.
        Assert.Equal(RequestCancelOutcome.Submitted, result.Outcome);
        Assert.Equal(CancelStates.CancelRequested, result.Intent!.CancelState);
        // Counter bumped to reflect the inline attempt (no state transition).
        Assert.Equal(1, result.Intent.CancelAttemptCount);

        // Single parallel pass — no inline retry loop.
        await aca.Received(1).CancelExecutionAsync("caj-graph", "exec-1", Arg.Any<string?>());

        // Per-task outcomes carry the transient signal up to the endpoint.
        Assert.NotNull(result.StopAttempts);
        Assert.Single(result.StopAttempts!);
        Assert.Equal(StopAttemptOutcome.Transient, result.StopAttempts![0].Outcome);
    }

    [Fact]
    public async Task RequestCancel_FansOutStopsInParallelAcrossTasks()
    {
        var claim = MakeClaim(executionNames: new[] { "exec-a", "exec-b", "exec-c" });
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.TryCreateAsync(Arg.Any<CancelIntent>())
            .Returns(ci => new CancelIntentCreateResult(
                CancelIntentCreateOutcome.Created,
                ci.Arg<CancelIntent>() with { ETag = "etag-1" }));
        intentStore.TryUpdateAsync(Arg.Any<CancelIntent>()).Returns(true);

        var aca = Substitute.For<IAcaJobClient>();
        aca.CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>()).Returns(true);

        var canceller = Build(claimReader, intentStore, aca);
        var result = await canceller.RequestCancelAsync(
            claim.RunId, CancelTriggers.Manual, null);

        Assert.Equal(RequestCancelOutcome.Submitted, result.Outcome);
        Assert.Equal(CancelStates.CancelSubmitted, result.Intent!.CancelState);
        // Exactly one ARM call per execution — fanout, no serial retry loop.
        await aca.Received(1).CancelExecutionAsync(Arg.Any<string>(), "exec-a", Arg.Any<string?>());
        await aca.Received(1).CancelExecutionAsync(Arg.Any<string>(), "exec-b", Arg.Any<string?>());
        await aca.Received(1).CancelExecutionAsync(Arg.Any<string>(), "exec-c", Arg.Any<string?>());
        await aca.Received(3).CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>());

        Assert.NotNull(result.StopAttempts);
        Assert.Equal(3, result.StopAttempts!.Count);
        Assert.All(result.StopAttempts, s =>
            Assert.Equal(StopAttemptOutcome.Accepted, s.Outcome));
    }

    [Fact]
    public async Task RequestCancel_PartialTransient_BumpsCancelAttemptCount()
    {
        // CancelIntent.cs documents cancel_attempt_count in cancel_requested as
        // "inline-submit + each reconciler retry". Inline transient must bump
        // the counter so the operator-visible attempt_count reflects the
        // inline attempt rather than stalling at 0.
        var claim = MakeClaim(executionNames: new[] { "exec-a", "exec-b" });
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.TryCreateAsync(Arg.Any<CancelIntent>())
            .Returns(ci => new CancelIntentCreateResult(
                CancelIntentCreateOutcome.Created,
                ci.Arg<CancelIntent>() with { ETag = "etag-1", CancelAttemptCount = 0 }));
        intentStore.TryUpdateAsync(Arg.Any<CancelIntent>()).Returns(true);

        var aca = Substitute.For<IAcaJobClient>();
        // exec-a accepted, exec-b transient.
        aca.CancelExecutionAsync(Arg.Any<string>(), "exec-a", Arg.Any<string?>()).Returns(true);
        aca.CancelExecutionAsync(Arg.Any<string>(), "exec-b", Arg.Any<string?>()).Returns(false);

        var canceller = Build(claimReader, intentStore, aca);
        var result = await canceller.RequestCancelAsync(
            claim.RunId, CancelTriggers.Manual, null);

        Assert.Equal(RequestCancelOutcome.Submitted, result.Outcome);
        Assert.Equal(CancelStates.CancelRequested, result.Intent!.CancelState);
        Assert.Equal(1, result.Intent.CancelAttemptCount);
        await intentStore.Received(1).TryUpdateAsync(Arg.Is<CancelIntent>(i =>
            i.CancelState == CancelStates.CancelRequested
            && i.CancelAttemptCount == 1));
    }

    [Fact]
    public async Task RequestCancel_StopThrows_SurfaceSanitizedCode_NotExMessage()
    {
        // ARM exception messages can include subscription IDs, response bodies,
        // and other tenant details. The endpoint surfaces stop_attempts to
        // operators, so Error must be a sanitized short code.
        var claim = MakeClaim();
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.TryCreateAsync(Arg.Any<CancelIntent>())
            .Returns(ci => new CancelIntentCreateResult(
                CancelIntentCreateOutcome.Created,
                ci.Arg<CancelIntent>() with { ETag = "etag-1" }));
        intentStore.TryUpdateAsync(Arg.Any<CancelIntent>()).Returns(true);

        var leakyMessage = "Failed to stop caj-graph/exec-1: BadRequest - {\"subscriptionId\":\"00000000-0000-0000-0000-000000000000\",\"detail\":\"...\"}";
        var aca = Substitute.For<IAcaJobClient>();
        aca.CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>())
            .ThrowsAsync(new InvalidOperationException(leakyMessage));

        var canceller = Build(claimReader, intentStore, aca);
        var result = await canceller.RequestCancelAsync(
            claim.RunId, CancelTriggers.Manual, null);

        Assert.Equal(RequestCancelOutcome.Submitted, result.Outcome);
        Assert.NotNull(result.StopAttempts);
        var attempt = Assert.Single(result.StopAttempts!);
        Assert.Equal(StopAttemptOutcome.Transient, attempt.Outcome);
        Assert.Equal("arm_hard_failure", attempt.Error);
        Assert.DoesNotContain("subscriptionId", attempt.Error);
        Assert.DoesNotContain("BadRequest", attempt.Error);
    }

    [Fact]
    public async Task RequestCancel_AlreadyCancelling_IdempotentReturn()
    {
        var claim = MakeClaim();
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var existingIntent = new CancelIntent
        {
            RunId = claim.RunId,
            JobName = claim.JobName,
            TenantKey = claim.TenantKey,
            RunType = claim.RunType,
            CancelState = CancelStates.CancelSubmitted,
            CancelTrigger = CancelTriggers.Manual,
            CancelRequestedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
            CancelSubmittedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
            ETag = "etag-existing",
        };
        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.TryCreateAsync(Arg.Any<CancelIntent>())
            .Returns(new CancelIntentCreateResult(
                CancelIntentCreateOutcome.AlreadyExists, existingIntent));

        var aca = Substitute.For<IAcaJobClient>();

        var canceller = Build(claimReader, intentStore, aca);
        var result = await canceller.RequestCancelAsync(
            claim.RunId, CancelTriggers.Manual, null);

        Assert.Equal(RequestCancelOutcome.AlreadyCancelling, result.Outcome);
        Assert.Equal(CancelStates.CancelSubmitted, result.Intent!.CancelState);
        // No new ACA stops issued — the prior trigger handled it.
        await aca.DidNotReceiveWithAnyArgs().CancelExecutionAsync(default!, default!, default);
    }

    [Fact]
    public async Task RequestCancel_AlreadyExistsAtRequested_ReattemptsSubmission()
    {
        var claim = MakeClaim();
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        // Prior call landed at cancel_requested (its inline submit failed).
        var existingIntent = new CancelIntent
        {
            RunId = claim.RunId,
            JobName = claim.JobName,
            TenantKey = claim.TenantKey,
            RunType = claim.RunType,
            CancelState = CancelStates.CancelRequested,
            CancelTrigger = CancelTriggers.Manual,
            CancelRequestedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
            CancelAttemptCount = 1,
            ETag = "etag-existing",
        };
        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.TryCreateAsync(Arg.Any<CancelIntent>())
            .Returns(new CancelIntentCreateResult(
                CancelIntentCreateOutcome.AlreadyExists, existingIntent));
        intentStore.TryUpdateAsync(Arg.Any<CancelIntent>()).Returns(true);

        var aca = Substitute.For<IAcaJobClient>();
        aca.CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>()).Returns(true);

        var canceller = Build(claimReader, intentStore, aca);
        var result = await canceller.RequestCancelAsync(
            claim.RunId, CancelTriggers.Manual, null);

        // Submission succeeded this time — advanced to cancel_submitted.
        Assert.Equal(RequestCancelOutcome.Submitted, result.Outcome);
        Assert.Equal(CancelStates.CancelSubmitted, result.Intent!.CancelState);
        await aca.Received(1).CancelExecutionAsync("caj-graph", "exec-1", Arg.Any<string?>());
    }

    [Fact]
    public async Task Reconcile_TimeoutDeadlineCrossed_CreatesIntentTriggerTimeout()
    {
        var startedAt = DateTimeOffset.UtcNow.AddHours(-9);
        var claim = MakeClaim(startedAt: startedAt, resolvedTimeoutSeconds: 8 * 3600);
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.TryReadAsync(claim.RunId).Returns((CancelIntent?)null);
        intentStore.ListAllAsync().Returns(Array.Empty<CancelIntent>());
        intentStore.TryCreateAsync(Arg.Any<CancelIntent>())
            .Returns(ci => new CancelIntentCreateResult(
                CancelIntentCreateOutcome.Created,
                ci.Arg<CancelIntent>() with { ETag = "etag-1" }));
        intentStore.TryUpdateAsync(Arg.Any<CancelIntent>()).Returns(true);

        var aca = Substitute.For<IAcaJobClient>();
        aca.CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>()).Returns(true);

        var canceller = Build(claimReader, intentStore, aca);
        await canceller.ReconcileAsync();

        await intentStore.Received().TryCreateAsync(
            Arg.Is<CancelIntent>(i =>
                i.RunId == claim.RunId
                && i.CancelTrigger == CancelTriggers.Timeout));
    }

    [Fact]
    public async Task Reconcile_NotPastDeadline_NoIntentCreated()
    {
        var claim = MakeClaim(
            startedAt: DateTimeOffset.UtcNow.AddMinutes(-30),
            resolvedTimeoutSeconds: 3600);
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.ListAllAsync().Returns(Array.Empty<CancelIntent>());

        var canceller = Build(claimReader, intentStore, Substitute.For<IAcaJobClient>());
        await canceller.ReconcileAsync();

        await intentStore.DidNotReceiveWithAnyArgs().TryCreateAsync(default!);
    }

    [Fact]
    public async Task Reconcile_CancelRequested_RetriesAndAdvancesOnSuccess_ResetsAttemptCount()
    {
        var claim = MakeClaim();
        // Submit retries have racked up some attempts while in
        // cancel_requested. The transition to cancel_submitted must reset
        // the counter or stall detection (which uses the same field once
        // we're in cancel_submitted) trips almost immediately.
        var existingIntent = new CancelIntent
        {
            RunId = claim.RunId,
            JobName = claim.JobName,
            TenantKey = claim.TenantKey,
            RunType = claim.RunType,
            CancelState = CancelStates.CancelRequested,
            CancelTrigger = CancelTriggers.Manual,
            CancelRequestedAt = DateTimeOffset.UtcNow.AddMinutes(-3),
            CancelAttemptCount = 4,
            ETag = "etag-1",
        };

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.ListAllAsync().Returns(new[] { existingIntent });
        intentStore.TryUpdateAsync(Arg.Any<CancelIntent>()).Returns(true);

        var aca = Substitute.For<IAcaJobClient>();
        aca.CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>()).Returns(true);

        var canceller = Build(claimReader, intentStore, aca);
        await canceller.ReconcileAsync();

        await intentStore.Received().TryUpdateAsync(Arg.Is<CancelIntent>(i =>
            i.CancelState == CancelStates.CancelSubmitted
            && i.CancelSubmittedAt != null
            && i.CancelAttemptCount == 0));
    }

    [Fact]
    public async Task RequestCancel_FromRequestedToSubmitted_ResetsAttemptCount()
    {
        // Inline-submit path: prior call landed at cancel_requested with
        // submit retries on the counter; this call succeeds and must
        // advance to cancel_submitted with the counter zeroed.
        var claim = MakeClaim();
        var claimReader = Substitute.For<IClaimReader>();
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var existingIntent = new CancelIntent
        {
            RunId = claim.RunId,
            JobName = claim.JobName,
            TenantKey = claim.TenantKey,
            RunType = claim.RunType,
            CancelState = CancelStates.CancelRequested,
            CancelTrigger = CancelTriggers.Manual,
            CancelRequestedAt = DateTimeOffset.UtcNow.AddMinutes(-2),
            CancelAttemptCount = 3,
            ETag = "etag-prior",
        };
        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.TryCreateAsync(Arg.Any<CancelIntent>())
            .Returns(new CancelIntentCreateResult(
                CancelIntentCreateOutcome.AlreadyExists, existingIntent));
        intentStore.TryUpdateAsync(Arg.Any<CancelIntent>()).Returns(true);

        var aca = Substitute.For<IAcaJobClient>();
        aca.CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>()).Returns(true);

        var canceller = Build(claimReader, intentStore, aca);
        var result = await canceller.RequestCancelAsync(
            claim.RunId, CancelTriggers.Manual, null);

        Assert.Equal(RequestCancelOutcome.Submitted, result.Outcome);
        Assert.Equal(0, result.Intent!.CancelAttemptCount);
    }

    [Fact]
    public async Task Reconcile_CancelSubmitted_FlipsToStalledAfterThreshold()
    {
        var claim = MakeClaim();
        // CancelStallThreshold is 5; one more pass takes us there.
        var existingIntent = new CancelIntent
        {
            RunId = claim.RunId,
            JobName = claim.JobName,
            TenantKey = claim.TenantKey,
            RunType = claim.RunType,
            CancelState = CancelStates.CancelSubmitted,
            CancelTrigger = CancelTriggers.Manual,
            CancelRequestedAt = DateTimeOffset.UtcNow.AddMinutes(-15),
            CancelSubmittedAt = DateTimeOffset.UtcNow.AddMinutes(-14),
            CancelAttemptCount = 4,
            ETag = "etag-1",
        };

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.ListAllAsync().Returns(new[] { existingIntent });
        intentStore.TryUpdateAsync(Arg.Any<CancelIntent>()).Returns(true);

        var canceller = Build(claimReader, intentStore, Substitute.For<IAcaJobClient>());
        await canceller.ReconcileAsync();

        await intentStore.Received().TryUpdateAsync(Arg.Is<CancelIntent>(i =>
            i.CancelState == CancelStates.CancelStalled));
    }

    [Fact]
    public async Task Reconcile_IntentForGoneClaim_DeletesIntent()
    {
        var orphanIntent = new CancelIntent
        {
            RunId = "ghost",
            JobName = "daily",
            TenantKey = "madev1",
            RunType = RunTypes.Normal,
            CancelState = CancelStates.CancelSubmitted,
            CancelTrigger = CancelTriggers.Manual,
            CancelRequestedAt = DateTimeOffset.UtcNow.AddMinutes(-30),
            ETag = "etag-orphan",
        };

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(Array.Empty<ClaimRecord>());
        claimReader.TryFindByRunIdAsync("ghost").Returns((ClaimRecord?)null);

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.ListAllAsync().Returns(new[] { orphanIntent });

        var canceller = Build(claimReader, intentStore, Substitute.For<IAcaJobClient>());
        await canceller.ReconcileAsync();

        await intentStore.Received().DeleteAsync("ghost");
    }

    [Fact]
    public async Task Reconcile_Stalled_KeepsRetryingStop_NoStateChange()
    {
        var claim = MakeClaim();
        var stalledIntent = new CancelIntent
        {
            RunId = claim.RunId,
            JobName = claim.JobName,
            TenantKey = claim.TenantKey,
            RunType = claim.RunType,
            CancelState = CancelStates.CancelStalled,
            CancelTrigger = CancelTriggers.Manual,
            CancelRequestedAt = DateTimeOffset.UtcNow.AddMinutes(-30),
            CancelSubmittedAt = DateTimeOffset.UtcNow.AddMinutes(-29),
            CancelAttemptCount = 10,
            ETag = "etag-stalled",
        };

        var claimReader = Substitute.For<IClaimReader>();
        claimReader.ListActiveAsync().Returns(new[] { claim });
        claimReader.TryFindByRunIdAsync(claim.RunId).Returns(claim);

        var intentStore = Substitute.For<ICancelIntentStore>();
        intentStore.ListAllAsync().Returns(new[] { stalledIntent });

        var aca = Substitute.For<IAcaJobClient>();
        aca.CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>()).Returns(false);

        var canceller = Build(claimReader, intentStore, aca);
        await canceller.ReconcileAsync();

        // Should have nudged ACA again.
        await aca.Received().CancelExecutionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>());
        // But not flipped state — stalled is the operator-visible terminal
        // for the cancel state machine until force-release.
        await intentStore.DidNotReceiveWithAnyArgs().TryUpdateAsync(default!);
    }
}
