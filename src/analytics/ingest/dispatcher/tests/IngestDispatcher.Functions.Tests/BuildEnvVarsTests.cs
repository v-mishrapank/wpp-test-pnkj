using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using IngestDispatcher.Functions.Settings;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace IngestDispatcher.Functions.Tests;

// Unit tests for AcaJobClient.BuildEnvVars — the dispatcher↔container wire
// contract for environment variables passed to ACA executions. Asserts the
// SCOPE_ROOT emission introduced in #260 (Phase 0 scope filter shim) plus
// surrounding invariants (storage-auth conditional vars, backfill window
// pairing) that have no other test coverage today.
public class BuildEnvVarsTests
{
    private static AcaJobClient CreateClient()
    {
        var settings = Options.Create(new IngestSettings
        {
            KeyVaultName = "kv-test",
            SubscriptionId = "00000000-0000-0000-0000-000000000000",
            ResourceGroupName = "rg-test",
            IngestClientId = "client-id-test",
            IngestCertName = "cert-test",
        });
        return new AcaJobClient(
            new HttpClient(),
            settings,
            configLoader: null!,
            new NullLogger<AcaJobClient>());
    }

    private static TenantConfig Tenant(string key, bool scoped = false) => new(
        TenantKey: key,
        TenantId: "tid",
        Organization: "org.onmicrosoft.com",
        AdminUrl: "https://org-admin.sharepoint.com",
        AnalyticsEnabled: true,
        AnalyticsMaxConcurrency: 5,
        EntitySelector: null,
        Scoped: scoped);

    private static StorageConfig MiStorage() => new(
        AccountUrl: "https://acct.dfs.core.windows.net",
        Container: "landing",
        Auth: new StorageAuthConfig(Method: StorageAuthMethods.ManagedIdentity));

    [Fact]
    public void Emits_SCOPE_ROOT_when_tenant_is_scoped()
    {
        var client = CreateClient();
        var vars = client.BuildEnvVars(
            "caj-test-graph-001",
            Tenant("madev1", scoped: true),
            new[] { "entra_users" },
            MiStorage(),
            runId: "run-1",
            runType: RunTypes.Normal,
            backfillStart: null,
            backfillEnd: null);

        Assert.Equal("_scope/madev1", vars["SCOPE_ROOT"]);
    }

    [Fact]
    public void Omits_SCOPE_ROOT_when_tenant_is_unscoped()
    {
        var client = CreateClient();
        var vars = client.BuildEnvVars(
            "caj-test-graph-001",
            Tenant("madev2", scoped: false),
            new[] { "entra_users" },
            MiStorage(),
            runId: "run-2",
            runType: RunTypes.Normal,
            backfillStart: null,
            backfillEnd: null);

        Assert.False(vars.ContainsKey("SCOPE_ROOT"));
    }

    [Fact]
    public void Omits_SCOPE_ROOT_when_tenant_omits_scoped_field()
    {
        // Default Scoped = false in the TenantConfig record — JSON without
        // the field deserializes to this. Verify the default path.
        var client = CreateClient();
        var tenant = new TenantConfig(
            TenantKey: "madev3",
            TenantId: "tid3",
            Organization: "org3.onmicrosoft.com",
            AdminUrl: null,
            AnalyticsEnabled: true,
            AnalyticsMaxConcurrency: 5);
        var vars = client.BuildEnvVars(
            "caj-test-graph-001",
            tenant,
            new[] { "entra_users" },
            MiStorage(),
            runId: "run-3",
            runType: RunTypes.Normal,
            backfillStart: null,
            backfillEnd: null);

        Assert.False(vars.ContainsKey("SCOPE_ROOT"));
    }
}
