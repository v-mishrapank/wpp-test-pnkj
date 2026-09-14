using System.Text.Json;
using System.Text.Json.Serialization;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;

namespace IngestDispatcher.Functions.Functions;

public class PreviewFunction
{
    private const int MaxTenantKeys = 50;
    private const int MaxEntityNames = 100;
    private const int MaxIncludeTiers = 5;
    private const int MaxExcludeEntities = 100;

    private readonly IConfigLoader _configLoader;
    private readonly IEntityResolver _entityResolver;
    private readonly ITenantResolver _tenantResolver;

    public PreviewFunction(
        IConfigLoader configLoader,
        IEntityResolver entityResolver,
        ITenantResolver tenantResolver)
    {
        _configLoader = configLoader;
        _entityResolver = entityResolver;
        _tenantResolver = tenantResolver;
    }

    [Function("Preview")]
    public async Task<IActionResult> RunAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "preview")] HttpRequest req)
    {
        PreviewRequest? body;
        try
        {
            body = await JsonSerializer.DeserializeAsync<PreviewRequest>(req.Body);
        }
        catch (JsonException ex)
        {
            return new BadRequestObjectResult($"Invalid JSON: {ex.Message}");
        }
        if (body == null)
            return new BadRequestObjectResult("Invalid request body");

        if (body.TenantSelector?.TenantKeys is { Count: > MaxTenantKeys })
            return new BadRequestObjectResult($"tenant_selector.tenant_keys exceeds limit of {MaxTenantKeys}");
        if (body.TenantSelector?.ExcludeKeys is { Count: > MaxTenantKeys })
            return new BadRequestObjectResult($"tenant_selector.exclude_keys exceeds limit of {MaxTenantKeys}");
        if (body.EntitySelector?.IncludeEntities is { Count: > MaxEntityNames })
            return new BadRequestObjectResult($"entity_selector.include_entities exceeds limit of {MaxEntityNames}");
        if (body.EntitySelector?.ExcludeEntities is { Count: > MaxExcludeEntities })
            return new BadRequestObjectResult($"entity_selector.exclude_entities exceeds limit of {MaxExcludeEntities}");
        if (body.EntitySelector?.IncludeTiers is { Count: > MaxIncludeTiers })
            return new BadRequestObjectResult($"entity_selector.include_tiers exceeds limit of {MaxIncludeTiers}");

        EntitySelector entitySelector;
        TenantSelector tenantSelector;

        if (!string.IsNullOrEmpty(body.JobName))
        {
            var job = _configLoader.Jobs.Jobs.FirstOrDefault(j => j.Name == body.JobName);
            if (job == null)
                return new NotFoundObjectResult($"Job '{body.JobName}' not found");
            entitySelector = job.EntitySelector;
            tenantSelector = job.TenantSelector;
        }
        else
        {
            entitySelector = body.EntitySelector ?? new EntitySelector();
            tenantSelector = body.TenantSelector ?? new TenantSelector(TenantSelectorModes.All);
        }

        var entities = _entityResolver.Resolve(entitySelector);
        var resolvedTenants = _tenantResolver.Resolve(tenantSelector);

        // Per-tenant intersection — same logic RunExecutor applies at dispatch.
        // Surviving tenants list their effective set; fully-blocked tenants move
        // to skipped_tenants so the operator sees both kinds of outcome.
        var previewTenants = new List<PreviewTenant>();
        var skippedTenants = new List<SkippedTenant>();
        var jobEntityNames = entities.Select(e => e.Name).ToList();

        foreach (var tenant in resolvedTenants)
        {
            var tenantEntities = _entityResolver.Intersect(tenant.EntitySelector, entities);
            var tenantNames = tenantEntities.Select(e => e.Name).ToHashSet();
            var blocked = jobEntityNames.Where(n => !tenantNames.Contains(n)).ToList();

            if (tenantEntities.Count == 0)
            {
                skippedTenants.Add(new SkippedTenant(
                    tenant.TenantKey,
                    "no_entities_after_intersection",
                    blocked));
                continue;
            }

            previewTenants.Add(new PreviewTenant(
                tenant.TenantKey,
                tenant.Organization,
                tenantEntities.Select(e => e.Name).ToList(),
                blocked));
        }

        // Top-level `entities` / `container_groups` describe the UNION across
        // surviving tenants. Back-compat with pre-#310 preview consumers: when
        // no tenant has an entity_selector, the union equals the job-resolved
        // set (the previous shape).
        var unionNames = previewTenants.SelectMany(t => t.EffectiveEntities).ToHashSet();
        var unionEntities = entities.Where(e => unionNames.Contains(e.Name)).ToList();
        var containerGroups = unionEntities
            .GroupBy(e => e.Container)
            .ToDictionary(g => g.Key, g => g.Select(e => e.Name).ToList());

        // task_count = Σ tenants (containers they actually use). Tenants with
        // reduced entity sets may also have reduced container sets — counting
        // tenants × union-containers would over-report. Build the name→container
        // lookup once; a naive `entities.First(e => e.Name == name)` inside the
        // Sum would be O(T·E²).
        var nameToContainer = entities.ToDictionary(e => e.Name, e => e.Container);
        var taskCount = previewTenants.Sum(t => t.EffectiveEntities
            .Select(name => nameToContainer[name])
            .Distinct()
            .Count());

        return new OkObjectResult(new PreviewResponse(
            Tenants: previewTenants,
            TenantCount: previewTenants.Count,
            Entities: unionEntities.Select(e => new PreviewEntity(e.Name, e.Tier, e.Container)).ToList(),
            EntityCount: unionEntities.Count,
            ContainerGroups: containerGroups,
            TaskCount: taskCount,
            SkippedTenants: skippedTenants));
    }

    private record PreviewRequest(
        [property: JsonPropertyName("job_name")] string? JobName = null,
        [property: JsonPropertyName("entity_selector")] EntitySelector? EntitySelector = null,
        [property: JsonPropertyName("tenant_selector")] TenantSelector? TenantSelector = null
    );

    public record PreviewResponse(
        [property: JsonPropertyName("tenants")] IReadOnlyList<PreviewTenant> Tenants,
        [property: JsonPropertyName("tenant_count")] int TenantCount,
        [property: JsonPropertyName("entities")] IReadOnlyList<PreviewEntity> Entities,
        [property: JsonPropertyName("entity_count")] int EntityCount,
        [property: JsonPropertyName("container_groups")] IReadOnlyDictionary<string, List<string>> ContainerGroups,
        [property: JsonPropertyName("task_count")] int TaskCount,
        [property: JsonPropertyName("skipped_tenants")] IReadOnlyList<SkippedTenant> SkippedTenants);

    public record PreviewTenant(
        [property: JsonPropertyName("tenant_key")] string TenantKey,
        [property: JsonPropertyName("organization")] string Organization,
        [property: JsonPropertyName("effective_entities")] IReadOnlyList<string> EffectiveEntities,
        [property: JsonPropertyName("blocked_entities")] IReadOnlyList<string> BlockedEntities);

    public record PreviewEntity(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("tier")] int Tier,
        [property: JsonPropertyName("container")] string Container);

    public record SkippedTenant(
        [property: JsonPropertyName("tenant_key")] string TenantKey,
        [property: JsonPropertyName("reason")] string Reason,
        [property: JsonPropertyName("blocked_entities")] IReadOnlyList<string> BlockedEntities);
}
