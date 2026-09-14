using System.Text.Json;
using IngestDispatcher.Functions.Services;

namespace IngestDispatcher.Functions.Tests;

public class ConfigLoaderTests
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        WriteIndented = false
    };

    private const string ValidRegistry = """
        { "entities": [
            { "name": "entra_users", "tier": 1, "container": "graph-ingest" },
            { "name": "entra_groups", "tier": 2, "container": "graph-ingest" }
        ] }
        """;

    private const string ValidTenants = """
        { "tenants": [
            { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
              "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5 }
        ] }
        """;

    private const string ValidJobs = """
        { "jobs": [
            { "name": "daily", "description": "d", "cron": "0 0 * * *", "enabled": true,
              "entity_selector": { "include_tiers": [1] },
              "tenant_selector": { "mode": "all" } }
        ] }
        """;

    private const string ValidStorage = """
        { "account_url": "https://a.dfs.core.windows.net", "container": "c",
          "auth": { "method": "managed_identity" } }
        """;

    [Fact]
    public void Load_HappyPath_Succeeds()
    {
        using var dir = new TempConfigDir();
        dir.WriteAll(ValidRegistry, ValidTenants, ValidJobs, ValidStorage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);

        Assert.Equal(2, loader.EntityRegistry.Entities.Count);
        Assert.Single(loader.Tenants.Tenants);
        Assert.Single(loader.Jobs.Jobs);
        Assert.Equal("managed_identity", loader.Storage.Auth.Method);
    }

    [Fact]
    public void Load_MissingFile_Throws()
    {
        using var dir = new TempConfigDir();
        // Only write 3 of 4 files
        dir.Write("entity-registry.json", ValidRegistry);
        dir.Write("tenants.json", ValidTenants);
        dir.Write("jobs.json", ValidJobs);
        // missing storage.json

        var ex = Assert.Throws<FileNotFoundException>(() => new ConfigLoader(dir.Path, JsonOpts));
        Assert.Contains("storage.json", ex.Message);
    }

    [Fact]
    public void Validate_DuplicateEntities_CaseInsensitive_Throws()
    {
        var registry = """
            { "entities": [
                { "name": "entra_users", "tier": 1, "container": "graph-ingest" },
                { "name": "ENTRA_USERS", "tier": 2, "container": "graph-ingest" }
            ] }
            """;
        var ex = LoadAndExpect(registry, ValidTenants, ValidJobs, ValidStorage);
        Assert.Contains("Duplicate (case-insensitive) entities", ex.Message);
    }

    [Fact]
    public void Validate_EntityTierOutOfRange_Throws()
    {
        var registry = """
            { "entities": [
                { "name": "foo", "tier": 6, "container": "graph-ingest" }
            ] }
            """;
        var ex = LoadAndExpect(registry, ValidTenants, ValidJobs, ValidStorage);
        Assert.Contains("invalid tier", ex.Message);
    }

    [Fact]
    public void Validate_DuplicateTenantKeys_CaseInsensitive_Throws()
    {
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5 },
                { "tenant_key": "T1", "tenant_id": "tid2", "organization": "o2",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5 }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, tenants, ValidJobs, ValidStorage);
        Assert.Contains("Duplicate (case-insensitive) tenant keys", ex.Message);
    }

    [Fact]
    public void Validate_InvalidStorageAuthMethod_Throws()
    {
        var storage = """
            { "account_url": "https://a.dfs.core.windows.net", "container": "c",
              "auth": { "method": "anonymous" } }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, ValidJobs, storage);
        Assert.Contains("Invalid storage auth method", ex.Message);
    }

    [Fact]
    public void Validate_ServicePrincipalCertMissingFields_Throws()
    {
        var storage = """
            { "account_url": "https://a.dfs.core.windows.net", "container": "c",
              "auth": { "method": "service_principal_cert" } }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, ValidJobs, storage);
        Assert.Contains("service_principal_cert requires tenant_id", ex.Message);
    }

    [Fact]
    public void Validate_ServicePrincipalCertWithAllFields_Succeeds()
    {
        var storage = """
            { "account_url": "https://a.dfs.core.windows.net", "container": "c",
              "auth": { "method": "service_principal_cert",
                        "tenant_id": "tid", "client_id": "cid", "cert_name": "cert" } }
            """;
        using var dir = new TempConfigDir();
        dir.WriteAll(ValidRegistry, ValidTenants, ValidJobs, storage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);

        Assert.Equal("service_principal_cert", loader.Storage.Auth.Method);
        Assert.Equal("cert", loader.Storage.Auth.CertName);
    }

    [Fact]
    public void Validate_ServicePrincipalSecretMissingFields_Throws()
    {
        var storage = """
            { "account_url": "https://a.dfs.core.windows.net", "container": "c",
              "auth": { "method": "service_principal_secret" } }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, ValidJobs, storage);
        Assert.Contains("service_principal_secret requires tenant_id", ex.Message);
    }

    [Fact]
    public void Validate_ServicePrincipalSecretMissingSecretName_Throws()
    {
        var storage = """
            { "account_url": "https://a.dfs.core.windows.net", "container": "c",
              "auth": { "method": "service_principal_secret",
                        "tenant_id": "tid", "client_id": "cid" } }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, ValidJobs, storage);
        Assert.Contains("service_principal_secret requires secret_name", ex.Message);
    }

    [Fact]
    public void Validate_ServicePrincipalSecretWithAllFields_Succeeds()
    {
        var storage = """
            { "account_url": "https://a.dfs.core.windows.net", "container": "c",
              "auth": { "method": "service_principal_secret",
                        "tenant_id": "tid", "client_id": "cid", "secret_name": "kv-secret" } }
            """;
        using var dir = new TempConfigDir();
        dir.WriteAll(ValidRegistry, ValidTenants, ValidJobs, storage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);

        Assert.Equal("service_principal_secret", loader.Storage.Auth.Method);
        Assert.Equal("kv-secret", loader.Storage.Auth.SecretName);
    }

    [Fact]
    public void Validate_DuplicateJobNames_CaseInsensitive_Throws()
    {
        var jobs = """
            { "jobs": [
                { "name": "daily", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": { "include_tiers": [1] },
                  "tenant_selector": { "mode": "all" } },
                { "name": "DAILY", "cron": "0 6 * * *", "enabled": true,
                  "entity_selector": { "include_tiers": [1] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("Duplicate (case-insensitive) job names", ex.Message);
    }

    [Theory]
    [InlineData("foo/bar")]
    [InlineData("foo\\bar")]
    [InlineData("../foo")]
    [InlineData("foo bar")]
    [InlineData("foo:bar")]
    [InlineData(".")]
    [InlineData("..")]
    public void Validate_JobNameWithPathChars_Throws(string badName)
    {
        var jsonEscapedName = badName.Replace("\\", "\\\\");
        var jobs = $$"""
            { "jobs": [
                { "name": "{{jsonEscapedName}}", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": { "include_tiers": [1] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("must match", ex.Message);
    }

    [Theory]
    [InlineData("foo/bar")]
    [InlineData("foo\\bar")]
    [InlineData("../foo")]
    [InlineData("foo bar")]
    [InlineData(".")]
    [InlineData("..")]
    public void Validate_TenantKeyWithUnsafeChars_Throws(string badKey)
    {
        var jsonEscapedKey = badKey.Replace("\\", "\\\\");
        var tenants = $$"""
            { "tenants": [
                { "tenant_key": "{{jsonEscapedKey}}", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true,
                  "analytics_max_concurrency": 1 }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, tenants, ValidJobs, ValidStorage);
        Assert.Contains("Tenant key", ex.Message);
        Assert.Contains("must match", ex.Message);
    }

    [Fact]
    public void Validate_InvalidCron_Throws()
    {
        var jobs = """
            { "jobs": [
                { "name": "bad", "cron": "not-a-cron", "enabled": true,
                  "entity_selector": { "include_tiers": [1] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("invalid cron", ex.Message);
    }

    [Fact]
    public void Validate_UnknownEntityInIncludeEntities_Throws()
    {
        var jobs = """
            { "jobs": [
                { "name": "bad", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": { "include_entities": ["not_a_real_entity"] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("unknown entity 'not_a_real_entity'", ex.Message);
    }

    [Fact]
    public void Validate_UnknownTenantInTenantKeys_Throws()
    {
        var jobs = """
            { "jobs": [
                { "name": "bad", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": { "include_tiers": [1] },
                  "tenant_selector": { "mode": "specific", "tenant_keys": ["t999"] } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("unknown tenant 't999'", ex.Message);
    }

    [Fact]
    public void Validate_SpecificModeWithoutTenantKeys_Throws()
    {
        var jobs = """
            { "jobs": [
                { "name": "bad", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": { "include_tiers": [1] },
                  "tenant_selector": { "mode": "specific" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("mode 'specific' but has no tenant_keys", ex.Message);
    }

    [Fact]
    public void Validate_AllExceptModeWithoutExcludeKeys_Throws()
    {
        var jobs = """
            { "jobs": [
                { "name": "bad", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": { "include_tiers": [1] },
                  "tenant_selector": { "mode": "all_except" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("mode 'all_except' but has no exclude_keys", ex.Message);
    }

    [Fact]
    public void Validate_IncludeTierOutOfRange_Throws()
    {
        var jobs = """
            { "jobs": [
                { "name": "bad", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": { "include_tiers": [-1] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("invalid include_tier -1", ex.Message);
    }

    [Fact]
    public void Validate_TierZero_Succeeds()
    {
        var registry = """
            { "entities": [
                { "name": "entra_sign_in_logs", "tier": 0, "container": "log-ingest" }
            ] }
            """;
        var jobs = """
            { "jobs": [
                { "name": "hourly-logs", "cron": "0 * * * *", "enabled": true,
                  "entity_selector": { "include_tiers": [0] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        using var dir = new TempConfigDir();
        dir.WriteAll(registry, ValidTenants, jobs, ValidStorage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);

        Assert.Equal(0, loader.EntityRegistry.Entities[0].Tier);
        Assert.Equal(0, loader.Jobs.Jobs[0].EntitySelector.IncludeTiers![0]);
    }

    [Fact]
    public void Validate_EmptyJobName_Throws()
    {
        var jobs = """
            { "jobs": [
                { "name": "  ", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": { "include_tiers": [1] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("must match", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-30)]
    public void Validate_NonPositiveTimeoutSeconds_Throws(int value)
    {
        var jobs = $$"""
            { "jobs": [
                { "name": "bad", "cron": "0 0 * * *", "enabled": true,
                  "timeout_seconds": {{value}},
                  "entity_selector": { "include_tiers": [1] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("invalid timeout_seconds", ex.Message);
    }

    [Fact]
    public void Validate_PositiveTimeoutSeconds_Succeeds()
    {
        var jobs = """
            { "jobs": [
                { "name": "tight", "cron": "0 0 * * *", "enabled": true,
                  "timeout_seconds": 3600,
                  "entity_selector": { "include_tiers": [1] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        using var dir = new TempConfigDir();
        dir.WriteAll(ValidRegistry, ValidTenants, jobs, ValidStorage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);
        Assert.Equal(3600, loader.Jobs.Jobs[0].TimeoutSeconds);
    }

    [Fact]
    public void Validate_TimeoutSecondsUnset_Succeeds()
    {
        // The default config (no timeout_seconds field) must continue to load.
        using var dir = new TempConfigDir();
        dir.WriteAll(ValidRegistry, ValidTenants, ValidJobs, ValidStorage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);
        Assert.Null(loader.Jobs.Jobs[0].TimeoutSeconds);
    }

    [Fact]
    public void Validate_TenantEntitySelector_UnknownInclude_Throws()
    {
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5,
                  "entity_selector": { "include_entities": ["ghost_entity"] } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, tenants, ValidJobs, ValidStorage);
        Assert.Contains("unknown entity 'ghost_entity'", ex.Message);
        Assert.Contains("t1", ex.Message);
    }

    [Fact]
    public void Validate_TenantEntitySelector_UnknownExclude_Throws()
    {
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5,
                  "entity_selector": { "exclude_entities": ["ghost_entity"] } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, tenants, ValidJobs, ValidStorage);
        Assert.Contains("unknown entity 'ghost_entity'", ex.Message);
    }

    [Fact]
    public void Validate_TenantEntitySelector_TierOutOfRange_Throws()
    {
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5,
                  "entity_selector": { "include_tiers": [6] } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, tenants, ValidJobs, ValidStorage);
        Assert.Contains("invalid include_tier 6", ex.Message);
    }

    [Fact]
    public void Validate_TenantEntitySelector_Tier0_Succeeds()
    {
        // Tier 0 was added for hourly-delta log entities; the tenant validator
        // must accept it (range is 0..5, not 1..5).
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5,
                  "entity_selector": { "include_tiers": [0, 1] } }
            ] }
            """;
        using var dir = new TempConfigDir();
        dir.WriteAll(ValidRegistry, tenants, ValidJobs, ValidStorage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);
        Assert.NotNull(loader.Tenants.Tenants[0].EntitySelector);
    }

    [Fact]
    public void Validate_TenantEntitySelector_IncludeExcludeOverlap_Throws()
    {
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5,
                  "entity_selector": {
                    "include_entities": ["entra_users"],
                    "exclude_entities": ["entra_users"]
                  } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, tenants, ValidJobs, ValidStorage);
        Assert.Contains("overlapping include_entities and exclude_entities", ex.Message);
        Assert.Contains("entra_users", ex.Message);
    }

    [Fact]
    public void Validate_TenantEntitySelector_AllFieldsEmpty_Throws()
    {
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5,
                  "entity_selector": {} }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, tenants, ValidJobs, ValidStorage);
        Assert.Contains("empty entity_selector", ex.Message);
        Assert.Contains("analytics_enabled: false", ex.Message);
    }

    [Fact]
    public void Validate_TenantEntitySelector_OmittedField_Succeeds()
    {
        // Omitting entity_selector entirely is the "no constraint" case and must
        // continue to load — distinct from `entity_selector: {}` which is rejected.
        using var dir = new TempConfigDir();
        dir.WriteAll(ValidRegistry, ValidTenants, ValidJobs, ValidStorage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);
        Assert.Null(loader.Tenants.Tenants[0].EntitySelector);
    }

    [Fact]
    public void Validate_TenantDisabledWithSelector_RecordsWarning()
    {
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": false, "analytics_max_concurrency": 5,
                  "entity_selector": { "include_tiers": [1] } }
            ] }
            """;
        using var dir = new TempConfigDir();
        dir.WriteAll(ValidRegistry, tenants, ValidJobs, ValidStorage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);
        Assert.Single(loader.Warnings);
        Assert.Contains("analytics_enabled: false", loader.Warnings[0]);
        Assert.Contains("dead config", loader.Warnings[0]);
    }

    [Fact]
    public void Validate_JobIncludeExcludeOverlap_Throws()
    {
        // Symmetric with the tenant-level check.
        var jobs = """
            { "jobs": [
                { "name": "daily", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": {
                    "include_entities": ["entra_users"],
                    "exclude_entities": ["entra_users"]
                  },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, ValidTenants, jobs, ValidStorage);
        Assert.Contains("overlapping include_entities and exclude_entities", ex.Message);
        Assert.Contains("daily", ex.Message);
    }

    [Fact]
    public void Validate_ScheduledJobEntityBlockedForEveryTenant_Throws()
    {
        // Job names entra_users; the only analytics-enabled tenant blocks it.
        // Cross-config check should fail config load.
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5,
                  "entity_selector": { "exclude_entities": ["entra_users"] } }
            ] }
            """;
        var jobs = """
            { "jobs": [
                { "name": "named-job", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": { "include_entities": ["entra_users"] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        var ex = LoadAndExpect(ValidRegistry, tenants, jobs, ValidStorage);
        Assert.Contains("'entra_users'", ex.Message);
        Assert.Contains("guaranteed no-op", ex.Message);
    }

    [Fact]
    public void Validate_ScheduledJobEntityAllowedForAtLeastOneTenant_Succeeds()
    {
        // Two tenants — only one blocks the named entity. Cross-config check
        // should accept this (at least one tenant will run it).
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5,
                  "entity_selector": { "exclude_entities": ["entra_users"] } },
                { "tenant_key": "t2", "tenant_id": "tid2", "organization": "o2",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5 }
            ] }
            """;
        var jobs = """
            { "jobs": [
                { "name": "named-job", "cron": "0 0 * * *", "enabled": true,
                  "entity_selector": { "include_entities": ["entra_users"] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        using var dir = new TempConfigDir();
        dir.WriteAll(ValidRegistry, tenants, jobs, ValidStorage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);
        Assert.Single(loader.Jobs.Jobs);
    }

    [Fact]
    public void Validate_DisabledJobBlockedForAllTenants_Succeeds()
    {
        // Only `enabled: true` scheduled jobs are subject to the cross-config
        // dead-job check. A disabled job (or one with no cron) is reachable
        // via manual /api/run and shouldn't be rejected at load time even if
        // it would be a no-op for every tenant.
        var tenants = """
            { "tenants": [
                { "tenant_key": "t1", "tenant_id": "tid1", "organization": "o1",
                  "admin_url": null, "analytics_enabled": true, "analytics_max_concurrency": 5,
                  "entity_selector": { "exclude_entities": ["entra_users"] } }
            ] }
            """;
        var jobs = """
            { "jobs": [
                { "name": "named-job", "cron": "0 0 * * *", "enabled": false,
                  "entity_selector": { "include_entities": ["entra_users"] },
                  "tenant_selector": { "mode": "all" } }
            ] }
            """;
        using var dir = new TempConfigDir();
        dir.WriteAll(ValidRegistry, tenants, jobs, ValidStorage);

        var loader = new ConfigLoader(dir.Path, JsonOpts);
        Assert.Single(loader.Jobs.Jobs);
    }

    private static InvalidOperationException LoadAndExpect(
        string registry, string tenants, string jobs, string storage)
    {
        using var dir = new TempConfigDir();
        dir.WriteAll(registry, tenants, jobs, storage);
        return Assert.Throws<InvalidOperationException>(
            () => new ConfigLoader(dir.Path, JsonOpts));
    }

    private sealed class TempConfigDir : IDisposable
    {
        public string Path { get; }

        public TempConfigDir()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                $"orch-test-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Path);
        }

        public void Write(string fileName, string contents) =>
            File.WriteAllText(System.IO.Path.Combine(Path, fileName), contents);

        public void WriteAll(string registry, string tenants, string jobs, string storage)
        {
            Write("entity-registry.json", registry);
            Write("tenants.json", tenants);
            Write("jobs.json", jobs);
            Write("storage.json", storage);
        }

        public void Dispose()
        {
            try { Directory.Delete(Path, recursive: true); }
            catch { /* best-effort cleanup */ }
        }
    }
}
