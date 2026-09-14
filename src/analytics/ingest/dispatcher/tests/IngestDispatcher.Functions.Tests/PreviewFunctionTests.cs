using IngestDispatcher.Functions.Functions;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using Microsoft.AspNetCore.Mvc;
using NSubstitute;

namespace IngestDispatcher.Functions.Tests;

public class PreviewFunctionTests
{
    private static readonly IReadOnlyList<EntityType> Entities =
    [
        new("entra_users", 1, "graph-ingest"),
        new("entra_groups", 1, "graph-ingest"),
        new("exo_mailboxes", 1, "exo-ingest"),
    ];

    private static readonly IReadOnlyList<TenantConfig> Tenants =
    [
        new("t1", "tid1", "o1", null, true, 5),
        new("t2", "tid2", "o2", null, true, 5),
    ];

    private readonly IConfigLoader _config = Substitute.For<IConfigLoader>();
    private readonly PreviewFunction _function;

    public PreviewFunctionTests()
    {
        _config.EntityRegistry.Returns(new EntityRegistryConfig(Entities));
        _config.Tenants.Returns(new TenantsConfig(Tenants));
        _config.Jobs.Returns(new JobsConfig([
            new JobDefinition("daily", "d", "0 0 * * *", true,
                new EntitySelector(IncludeTiers: [1]),
                new TenantSelector(TenantSelectorModes.All))
        ]));

        _function = new PreviewFunction(
            _config, new EntityResolver(_config), new TenantResolver(_config));
    }

    // TenantResolver eagerly captures `configLoader.Tenants.Tenants` at
    // construction; tests that want to change the tenant list after the
    // fixture is built must rebuild the function via this helper.
    private PreviewFunction RebuildFunctionWith(IReadOnlyList<TenantConfig> tenants)
    {
        _config.Tenants.Returns(new TenantsConfig(tenants));
        return new PreviewFunction(
            _config, new EntityResolver(_config), new TenantResolver(_config));
    }

    [Fact]
    public async Task InvalidBody_Returns400()
    {
        var req = HttpRequestHelper.BuildJsonRequest("null");
        var result = await _function.RunAsync(req);
        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task MalformedJson_Returns400()
    {
        // Regression: JsonException used to propagate unhandled, producing
        // HTTP 500 with a stack trace. Now caught and surfaced as 400.
        var req = HttpRequestHelper.BuildJsonRequest("{not valid json");
        var result = await _function.RunAsync(req);
        var bad = Assert.IsType<BadRequestObjectResult>(result);
        Assert.Contains("Invalid JSON", bad.Value?.ToString());
    }

    [Fact]
    public async Task UnknownJobName_Returns404()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "nope" }""");
        var result = await _function.RunAsync(req);
        Assert.IsType<NotFoundObjectResult>(result);
    }

    [Fact]
    public async Task JobName_UsesJobSelectors_ReturnsResolvedCounts()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        var result = await _function.RunAsync(req);

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = Assert.IsType<PreviewFunction.PreviewResponse>(ok.Value);
        Assert.Equal(2, body.TenantCount);
        Assert.Equal(3, body.EntityCount);
        // tenants(2) x container groups(2: graph + exo) = 4 task slots
        Assert.Equal(4, body.TaskCount);
    }

    [Fact]
    public async Task ExplicitSelectors_ApplyOverJob()
    {
        var req = HttpRequestHelper.BuildJsonRequest("""
            { "entity_selector": { "include_entities": ["entra_users"] },
              "tenant_selector": { "mode": "specific", "tenant_keys": ["t1"] } }
            """);
        var result = await _function.RunAsync(req);

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = Assert.IsType<PreviewFunction.PreviewResponse>(ok.Value);
        Assert.Equal(1, body.TenantCount);
        Assert.Equal(1, body.EntityCount);
        Assert.Equal(1, body.TaskCount);
    }

    [Fact]
    public async Task TenantKeysOverLimit_Returns400()
    {
        var keys = string.Join(",", Enumerable.Range(0, 51).Select(i => $"\"t{i}\""));
        var req = HttpRequestHelper.BuildJsonRequest($$"""
            { "tenant_selector": { "mode": "specific", "tenant_keys": [{{keys}}] } }
            """);
        var result = await _function.RunAsync(req);
        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task EntityNamesOverLimit_Returns400()
    {
        var names = string.Join(",", Enumerable.Range(0, 101).Select(i => $"\"e{i}\""));
        var req = HttpRequestHelper.BuildJsonRequest($$"""
            { "entity_selector": { "include_entities": [{{names}}] } }
            """);
        var result = await _function.RunAsync(req);
        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task PerTenantSelector_PartialBlock_ReportsBlockedEntities()
    {
        // t2 excludes exo_mailboxes. Preview should show t2's effective set
        // (without exo_mailboxes) and list exo_mailboxes in blocked_entities.
        var fn = RebuildFunctionWith([
            new("t1", "tid1", "o1", null, true, 5),
            new("t2", "tid2", "o2", null, true, 5,
                new EntitySelector(ExcludeEntities: ["exo_mailboxes"]))
        ]);

        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        var result = await fn.RunAsync(req);

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = Assert.IsType<PreviewFunction.PreviewResponse>(ok.Value);
        Assert.Equal(2, body.TenantCount);
        Assert.Empty(body.SkippedTenants);

        var t1 = body.Tenants.First(t => t.TenantKey == "t1");
        Assert.Equal(3, t1.EffectiveEntities.Count);
        Assert.Empty(t1.BlockedEntities);

        var t2 = body.Tenants.First(t => t.TenantKey == "t2");
        Assert.Equal(2, t2.EffectiveEntities.Count);
        Assert.DoesNotContain("exo_mailboxes", t2.EffectiveEntities);
        Assert.Equal(["exo_mailboxes"], t2.BlockedEntities);

        // Union: all three entities still appear at top-level (some tenant uses each).
        Assert.Equal(3, body.EntityCount);
    }

    [Fact]
    public async Task PerTenantSelector_FullBlock_ReportsInSkippedTenants()
    {
        // t2 has an include_tiers: [2] selector — the job is tier 1, so t2's
        // intersection is empty. t2 moves to skipped_tenants; t1 alone surviving.
        var fn = RebuildFunctionWith([
            new("t1", "tid1", "o1", null, true, 5),
            new("t2", "tid2", "o2", null, true, 5,
                new EntitySelector(IncludeTiers: [2]))
        ]);

        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        var result = await fn.RunAsync(req);

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = Assert.IsType<PreviewFunction.PreviewResponse>(ok.Value);
        Assert.Equal(1, body.TenantCount);
        Assert.Single(body.Tenants);
        Assert.Equal("t1", body.Tenants[0].TenantKey);

        Assert.Single(body.SkippedTenants);
        Assert.Equal("t2", body.SkippedTenants[0].TenantKey);
        Assert.Equal("no_entities_after_intersection", body.SkippedTenants[0].Reason);
        // All job entities are blocked for t2.
        Assert.Equal(3, body.SkippedTenants[0].BlockedEntities.Count);
    }

    [Fact]
    public async Task PerTenantSelector_NoTenantHasSelector_BackCompatShape()
    {
        // Existing pre-#310 behavior: when no tenant has a selector, the union
        // at top-level equals the job-resolved set and every tenant's
        // effective_entities equals that union. Back-compat read.
        var req = HttpRequestHelper.BuildJsonRequest("""{ "job_name": "daily" }""");
        var result = await _function.RunAsync(req);

        var ok = Assert.IsType<OkObjectResult>(result);
        var body = Assert.IsType<PreviewFunction.PreviewResponse>(ok.Value);
        Assert.Equal(2, body.TenantCount);
        Assert.Equal(3, body.EntityCount);
        Assert.Empty(body.SkippedTenants);
        Assert.All(body.Tenants, t => Assert.Equal(3, t.EffectiveEntities.Count));
        Assert.All(body.Tenants, t => Assert.Empty(t.BlockedEntities));
    }
}
