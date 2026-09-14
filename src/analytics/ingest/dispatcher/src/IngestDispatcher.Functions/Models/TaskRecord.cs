using System.Text.Json.Serialization;

namespace IngestDispatcher.Functions.Models;

public record TaskRecord(
    [property: JsonPropertyName("run_id")] string RunId,
    [property: JsonPropertyName("job_name")] string JobName,
    [property: JsonPropertyName("tenant_key")] string TenantKey,
    [property: JsonPropertyName("run_type")] string RunType,
    [property: JsonPropertyName("container_type")] string ContainerType,
    [property: JsonPropertyName("aca_execution_name")] string? AcaExecutionName,
    [property: JsonPropertyName("entities")] IReadOnlyList<string> Entities,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("started_at")] DateTimeOffset? StartedAt,
    [property: JsonPropertyName("completed_at")] DateTimeOffset? CompletedAt,
    [property: JsonPropertyName("error_message")] string? ErrorMessage
);
