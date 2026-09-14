using System.Text.Json;
using IngestDispatcher.Functions.Models;

namespace IngestDispatcher.Functions.Tests;

// Pin the wire format of HeartbeatEntity (the v4 flat entity shape).
// The PowerShell-side New-EntityRollup factory + ProgressHeartbeat blob
// serialization must produce identical JSON for the same logical inputs.
//
// Drift detection: any field rename / type change / missing field on either
// side breaks one of the asserts here.
public class HeartbeatEntityContractTests
{
    [Fact]
    public void HeartbeatEntity_AllFields_RoundTripsViaJson()
    {
        var original = new HeartbeatEntity(
            Name: "entra_users",
            Status: "success",
            RecordCount: 264,
            InputCount: 1000,
            ItemsProcessed: 1000,
            ItemsFailed: 3,
            ItemsSkipped: 2,
            RecordsSoFar: 264,
            StartedAt: DateTimeOffset.Parse("2026-05-09T13:42:14Z"),
            CompletedAt: DateTimeOffset.Parse("2026-05-09T13:43:01Z"),
            DurationMs: 47000,
            Errors: ["page 3/8 503"]);

        var json = JsonSerializer.Serialize(original);
        var roundTripped = JsonSerializer.Deserialize<HeartbeatEntity>(json);

        Assert.NotNull(roundTripped);
        Assert.Equal(original.Name, roundTripped!.Name);
        Assert.Equal(original.Status, roundTripped.Status);
        Assert.Equal(original.RecordCount, roundTripped.RecordCount);
        Assert.Equal(original.InputCount, roundTripped.InputCount);
        Assert.Equal(original.ItemsProcessed, roundTripped.ItemsProcessed);
        Assert.Equal(original.ItemsFailed, roundTripped.ItemsFailed);
        Assert.Equal(original.ItemsSkipped, roundTripped.ItemsSkipped);
        Assert.Equal(original.RecordsSoFar, roundTripped.RecordsSoFar);
        Assert.Equal(original.StartedAt, roundTripped.StartedAt);
        Assert.Equal(original.CompletedAt, roundTripped.CompletedAt);
        Assert.Equal(original.DurationMs, roundTripped.DurationMs);
        Assert.Equal(original.Errors, roundTripped.Errors);
    }

    [Fact]
    public void HeartbeatEntity_PendingEntity_MatchesPowerShellBlobShape()
    {
        // PowerShell blob writer emits records_so_far=0 and errors=[] for
        // pending entities — pin those values explicitly.
        var original = new HeartbeatEntity(
            Name: "spo_sites",
            Status: "pending",
            RecordsSoFar: 0,
            Errors: []);

        var json = JsonSerializer.Serialize(original);
        var roundTripped = JsonSerializer.Deserialize<HeartbeatEntity>(json);

        Assert.NotNull(roundTripped);
        Assert.Equal("spo_sites", roundTripped!.Name);
        Assert.Equal("pending", roundTripped.Status);
        Assert.Null(roundTripped.RecordCount);
        Assert.Null(roundTripped.InputCount);
        Assert.Null(roundTripped.ItemsProcessed);
        Assert.Null(roundTripped.ItemsFailed);
        Assert.Null(roundTripped.ItemsSkipped);
        Assert.Equal(0, roundTripped.RecordsSoFar);
        Assert.Null(roundTripped.StartedAt);
        Assert.Null(roundTripped.CompletedAt);
        Assert.Null(roundTripped.DurationMs);
        Assert.Empty(roundTripped.Errors!);
    }

    [Fact]
    public void HeartbeatEntity_FromPowerShellBlobShape_Deserializes()
    {
        // This payload mirrors what ProgressHeartbeat.psm1 produces in the
        // v4 blob: flat entity with all 12 fields at top level (snake_case).
        var pwshJson = """
        {
          "name": "entra_groups",
          "status": "partial",
          "record_count": 18432,
          "input_count": 500,
          "items_processed": 500,
          "items_failed": 12,
          "items_skipped": 3,
          "records_so_far": 18432,
          "started_at": "2026-05-09T13:42:00Z",
          "completed_at": "2026-05-09T13:48:30Z",
          "duration_ms": 390000,
          "errors": ["throttle on /groups", "throttle on /groups/{id}/members"]
        }
        """;

        var entity = JsonSerializer.Deserialize<HeartbeatEntity>(pwshJson);

        Assert.NotNull(entity);
        Assert.Equal("entra_groups", entity!.Name);
        Assert.Equal("partial", entity.Status);
        Assert.Equal(18432, entity.RecordCount);
        Assert.Equal(500, entity.InputCount);
        Assert.Equal(500, entity.ItemsProcessed);
        Assert.Equal(12, entity.ItemsFailed);
        Assert.Equal(3, entity.ItemsSkipped);
        Assert.Equal(18432, entity.RecordsSoFar);
        Assert.Equal(390000, entity.DurationMs);
        Assert.Equal(2, entity.Errors!.Count);
    }

    [Fact]
    public void HeartbeatPayload_WithSchemaVersion4_RoundTrips()
    {
        var original = new HeartbeatPayload(
            SchemaVersion: 4, RunId: "run-1", TenantKey: "madev1",
            ContainerType: "graph-ingest",
            ContainerStartedAt: DateTimeOffset.Parse("2026-05-09T13:42:00Z"),
            LastHeartbeatAt: DateTimeOffset.Parse("2026-05-09T13:43:01Z"),
            RunStatus: "success",
            Entities: [
                new HeartbeatEntity(Name: "entra_users", Status: "success",
                    RecordCount: 264, RecordsSoFar: 264, DurationMs: 47000,
                    StartedAt: DateTimeOffset.Parse("2026-05-09T13:42:14Z"),
                    CompletedAt: DateTimeOffset.Parse("2026-05-09T13:43:01Z"),
                    Errors: [])
            ],
            RunError: null,
            PrerequisiteEntities: [
                new HeartbeatEntity(Name: "groups_root", Status: "success",
                    RecordsSoFar: 57, DurationMs: 1200,
                    StartedAt: DateTimeOffset.Parse("2026-05-09T13:42:05Z"),
                    CompletedAt: DateTimeOffset.Parse("2026-05-09T13:42:06Z"),
                    Errors: [])
            ]);

        var json = JsonSerializer.Serialize(original);
        Assert.Contains("\"schema_version\":4", json);
        Assert.Contains("\"prerequisite_entities\"", json);
        Assert.Contains("\"run_error\":null", json);

        var roundTripped = JsonSerializer.Deserialize<HeartbeatPayload>(json);
        Assert.NotNull(roundTripped);
        Assert.Equal(4, roundTripped!.SchemaVersion);
        Assert.Null(roundTripped.RunError);
        Assert.NotNull(roundTripped.PrerequisiteEntities);
        Assert.Equal("groups_root", roundTripped.PrerequisiteEntities![0].Name);
        Assert.Empty(roundTripped.PrerequisiteEntities[0].Errors!);
    }

    [Fact]
    public void HeartbeatPayload_WithRunError_RoundTrips()
    {
        var original = new HeartbeatPayload(
            SchemaVersion: 4, RunId: "run-err", TenantKey: "madev1",
            ContainerType: "graph-ingest",
            ContainerStartedAt: DateTimeOffset.Parse("2026-05-09T13:42:00Z"),
            LastHeartbeatAt: DateTimeOffset.Parse("2026-05-09T13:42:05Z"),
            RunStatus: "failed",
            Entities: [
                new HeartbeatEntity(Name: "entra_users", Status: "pending",
                    RecordsSoFar: 0, Errors: [])
            ],
            RunError: "Connect-Service failed: certificate not found in Key Vault");

        var json = JsonSerializer.Serialize(original);
        Assert.Contains("\"run_error\":\"Connect-Service failed", json);

        var roundTripped = JsonSerializer.Deserialize<HeartbeatPayload>(json);
        Assert.NotNull(roundTripped);
        Assert.Equal("Connect-Service failed: certificate not found in Key Vault", roundTripped!.RunError);
        Assert.Equal(0, roundTripped.Entities[0].RecordsSoFar);
        Assert.Empty(roundTripped.Entities[0].Errors!);
    }
}
