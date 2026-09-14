using IngestDispatcher.Functions.Models;

namespace IngestDispatcher.Functions.Services;

public interface ITaskStateDeriver
{
    IReadOnlyList<TaskStatusResponse> Derive(
        ClaimRecord? claim,
        IReadOnlyList<TaskRecord> taskHistory,
        IReadOnlyDictionary<string, HeartbeatPayload> runStateByContainer,
        TimeSpan staleThreshold,
        DateTimeOffset now);
}

public class TaskStateDeriver : ITaskStateDeriver
{
    public IReadOnlyList<TaskStatusResponse> Derive(
        ClaimRecord? claim,
        IReadOnlyList<TaskRecord> taskHistory,
        IReadOnlyDictionary<string, HeartbeatPayload> runStateByContainer,
        TimeSpan staleThreshold,
        DateTimeOffset now)
    {
        var specs = claim != null
            ? claim.ExpectedTasks.Select(t => new TaskSpec(
                ContainerType: t.ContainerType,
                AcaExecutionName: t.AcaExecutionName,
                DispatchError: t.DispatchError,
                StartedAt: claim.StartedAt,
                HistoryRecord: null)).ToList()
            : taskHistory.Select(t => new TaskSpec(
                ContainerType: t.ContainerType,
                AcaExecutionName: t.AcaExecutionName,
                DispatchError: null,
                StartedAt: t.StartedAt ?? default,
                HistoryRecord: t)).ToList();

        var results = new List<TaskStatusResponse>(specs.Count);
        foreach (var spec in specs)
        {
            if (spec.AcaExecutionName == null && spec.DispatchError != null)
            {
                results.Add(BuildDispatchFailed(spec));
                continue;
            }

            if (runStateByContainer.TryGetValue(spec.ContainerType, out var blob))
            {
                results.Add(BuildFromBlob(spec, blob, staleThreshold, now));
                continue;
            }

            if (spec.HistoryRecord != null)
            {
                results.Add(BuildFromHistory(spec, spec.HistoryRecord));
                continue;
            }

            results.Add(BuildPending(spec));
        }
        return results;
    }

    private record TaskSpec(
        string ContainerType,
        string? AcaExecutionName,
        string? DispatchError,
        DateTimeOffset StartedAt,
        TaskRecord? HistoryRecord);

    private static TaskStatusResponse BuildDispatchFailed(TaskSpec spec) =>
        new(
            ContainerType: spec.ContainerType,
            AcaExecutionName: null,
            Status: TaskStatuses.DispatchFailed,
            StartedAt: spec.StartedAt,
            CompletedAt: spec.StartedAt,
            ErrorMessage: spec.DispatchError,
            Entities: [],
            Heartbeat: null
        );

    private static TaskStatusResponse BuildPending(TaskSpec spec) =>
        new(
            ContainerType: spec.ContainerType,
            AcaExecutionName: spec.AcaExecutionName,
            Status: TaskStatusValues.Pending,
            StartedAt: spec.StartedAt,
            CompletedAt: null,
            ErrorMessage: null,
            Entities: [],
            Heartbeat: null
        );

    private static TaskStatusResponse BuildFromHistory(TaskSpec spec, TaskRecord task) =>
        new(
            ContainerType: spec.ContainerType,
            AcaExecutionName: task.AcaExecutionName,
            Status: task.Status,
            StartedAt: task.StartedAt,
            CompletedAt: task.CompletedAt,
            ErrorMessage: task.ErrorMessage,
            Entities: [],
            Heartbeat: null
        );

    private static TaskStatusResponse BuildFromBlob(
        TaskSpec spec, HeartbeatPayload blob, TimeSpan staleThreshold, DateTimeOffset now)
    {
        var isTerminal = !string.Equals(blob.RunStatus, BlobRunStatuses.Running, StringComparison.Ordinal);

        // Abandoned-blob: blob still says 'running' but the run is finalized.
        // Reinterpret non-terminal sub-statuses to match the task's terminal status.
        if (!isTerminal && spec.HistoryRecord != null)
        {
            var taskTerminal = spec.HistoryRecord.Status;
            return new TaskStatusResponse(
                ContainerType: spec.ContainerType,
                AcaExecutionName: spec.HistoryRecord.AcaExecutionName,
                Status: taskTerminal,
                StartedAt: spec.HistoryRecord.StartedAt,
                CompletedAt: spec.HistoryRecord.CompletedAt,
                ErrorMessage: spec.HistoryRecord.ErrorMessage,
                Entities: MapEntities(blob.Entities, taskTerminal),
                Heartbeat: null,
                PrerequisiteEntities: MapPrerequisiteEntities(blob.PrerequisiteEntities, taskTerminal)
            );
        }

        var entities = MapEntities(blob.Entities, transformTaskTerminal: null);

        if (isTerminal)
        {
            var (status, error) = RunTracker.MapRunStateBlob(blob);
            DateTimeOffset? completedAt = entities
                .Where(e => e.CompletedAt.HasValue)
                .Select(e => e.CompletedAt)
                .DefaultIfEmpty(blob.LastHeartbeatAt)
                .Max();

            return new TaskStatusResponse(
                ContainerType: spec.ContainerType,
                AcaExecutionName: spec.AcaExecutionName,
                Status: status,
                StartedAt: spec.StartedAt,
                CompletedAt: completedAt,
                ErrorMessage: error,
                Entities: entities,
                Heartbeat: null,
                PrerequisiteEntities: MapPrerequisiteEntities(blob.PrerequisiteEntities, null)
            );
        }

        // In-flight projection.
        var ageSeconds = (long)Math.Max(0, (now - blob.LastHeartbeatAt).TotalSeconds);
        var stale = ageSeconds > (long)staleThreshold.TotalSeconds;

        var anyPrereqStarted = blob.PrerequisiteEntities?.Any(e => e.Status != EntityStatuses.Pending) ?? false;
        var taskStatus = entities.All(e => e.Status == EntityStatuses.Pending) && !anyPrereqStarted
            ? TaskStatusValues.Pending
            : TaskStatusValues.Running;

        return new TaskStatusResponse(
            ContainerType: spec.ContainerType,
            AcaExecutionName: spec.AcaExecutionName,
            Status: taskStatus,
            StartedAt: spec.StartedAt,
            CompletedAt: null,
            ErrorMessage: null,
            Entities: entities,
            Heartbeat: new HeartbeatBlock(
                LastHeartbeatAt: blob.LastHeartbeatAt,
                LastHeartbeatAgeSeconds: ageSeconds,
                Stale: stale
            ),
            PrerequisiteEntities: MapPrerequisiteEntities(blob.PrerequisiteEntities, null)
        );
    }

    private static List<TaskEntityProjection> MapEntities(
        IReadOnlyList<HeartbeatEntity>? blobEntities, string? transformTaskTerminal) =>
        (blobEntities ?? [])
            .Select(he => new TaskEntityProjection(
                Name: he.Name,
                Status: transformTaskTerminal is null
                    ? he.Status
                    : ReinterpretEntity(he.Status, transformTaskTerminal),
                RecordCount: he.RecordCount,
                InputCount: he.InputCount,
                ItemsProcessed: he.ItemsProcessed,
                ItemsFailed: he.ItemsFailed,
                ItemsSkipped: he.ItemsSkipped,
                RecordsSoFar: he.RecordsSoFar,
                StartedAt: he.StartedAt,
                CompletedAt: he.CompletedAt,
                DurationMs: he.DurationMs,
                Errors: he.Errors ?? []))
            .ToList();

    private static IReadOnlyList<TaskEntityProjection>? MapPrerequisiteEntities(
        IReadOnlyList<HeartbeatEntity>? prereqs, string? transformTaskTerminal)
    {
        if (prereqs is null or { Count: 0 })
            return null;

        return prereqs.Select(pe => new TaskEntityProjection(
            Name: pe.Name,
            Status: transformTaskTerminal is null
                ? pe.Status
                : ReinterpretEntity(pe.Status, transformTaskTerminal),
            RecordCount: pe.RecordCount,
            InputCount: pe.InputCount,
            ItemsProcessed: pe.ItemsProcessed,
            ItemsFailed: pe.ItemsFailed,
            ItemsSkipped: pe.ItemsSkipped,
            RecordsSoFar: pe.RecordsSoFar,
            StartedAt: pe.StartedAt,
            CompletedAt: pe.CompletedAt,
            DurationMs: pe.DurationMs,
            Errors: pe.Errors ?? []
        )).ToList();
    }

    private static string ReinterpretEntity(string current, string taskTerminal) => current switch
    {
        EntityStatuses.Pending or EntityStatuses.Running => MapTaskStatusToEntityStatus(taskTerminal),
        _ => current,
    };

    private static string MapTaskStatusToEntityStatus(string taskStatus) => taskStatus switch
    {
        TaskStatuses.Succeeded => EntityStatuses.Success,
        TaskStatuses.Partial => EntityStatuses.Partial,
        TaskStatuses.Skipped => EntityStatuses.Skipped,
        TaskStatuses.Failed => EntityStatuses.Failed,
        TaskStatuses.Cancelled => EntityStatuses.Cancelled,
        TaskStatuses.TimedOut => EntityStatuses.TimedOut,
        TaskStatuses.CancellationFailed => EntityStatuses.CancellationFailed,
        _ => EntityStatuses.Failed,
    };
}

public static class BlobRunStatuses
{
    public const string Running = "running";
    public const string Success = "success";
    public const string Partial = "partial";
    public const string Failed = "failed";
    public const string Completed = "completed";
}
