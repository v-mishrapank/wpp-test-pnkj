using System.Text.Json;
using Azure.Core;
using Azure.Identity;
using Azure.Monitor.OpenTelemetry.Exporter;
using Azure.Security.KeyVault.Certificates;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage.Blobs;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Services;
using IngestDispatcher.Functions.Settings;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Azure.Functions.Worker.OpenTelemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

builder.Services.AddOptions<IngestSettings>()
    .Bind(builder.Configuration.GetSection(IngestSettings.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

// Shared JSON options. ReadCommentHandling covers ConfigLoader; the rest ignore it.
builder.Services.AddSingleton(new JsonSerializerOptions
{
    PropertyNameCaseInsensitive = true,
    WriteIndented = false,
    ReadCommentHandling = JsonCommentHandling.Skip
});

builder.Services.AddSingleton<IConfigLoader>(sp =>
{
    var settings = sp.GetRequiredService<IOptions<IngestSettings>>().Value;
    var jsonOptions = sp.GetRequiredService<JsonSerializerOptions>();
    var configPath = Path.IsPathRooted(settings.ConfigPath)
        ? settings.ConfigPath
        : Path.Combine(AppContext.BaseDirectory, settings.ConfigPath);
    return new ConfigLoader(configPath, jsonOptions);
});

// Shared BlobServiceClient. Credential honors storage.Auth.Method — managed
// identity for Azure-tenant storage, or service-principal (cert or secret)
// for customer-managed ADLS or Fabric/OneLake scenarios. ARM calls against
// ACA Jobs stay on the Function App's own MI (see AcaJobClient) regardless.
builder.Services.AddSingleton(sp =>
{
    var configLoader = sp.GetRequiredService<IConfigLoader>();
    var settings = sp.GetRequiredService<IOptions<IngestSettings>>().Value;
    var storage = configLoader.Storage;
    var credential = BuildStorageCredential(storage.Auth, settings.KeyVaultName);
    return new BlobServiceClient(BlobEndpoint(storage.AccountUrl), credential);
});

// Parse account name from the configured ADLS URL and construct the blob endpoint,
// rather than naively string-replacing ".dfs." → ".blob.". Guards against account
// names or endpoint suffixes that contain ".dfs." legitimately.
static Uri BlobEndpoint(string accountUrl)
{
    var uri = new Uri(accountUrl);
    var host = uri.Host;
    var firstDot = host.IndexOf('.');
    if (firstDot < 0 || !(host.Contains(".dfs.") || host.Contains(".blob.")))
        throw new InvalidOperationException(
            $"Storage account_url '{accountUrl}' must be a .dfs. or .blob. endpoint.");
    var accountName = host[..firstDot];
    var suffix = host[(firstDot + 1)..];         // e.g. "dfs.core.windows.net"
    suffix = suffix.StartsWith("dfs.") ? "blob." + suffix[4..]
           : suffix.StartsWith("blob.") ? suffix
           : throw new InvalidOperationException(
                 $"Storage account_url '{accountUrl}' has unsupported endpoint subdomain.");
    return new Uri($"https://{accountName}.{suffix}");
}

static TokenCredential BuildStorageCredential(StorageAuthConfig auth, string keyVaultName)
{
    if (auth.Method == StorageAuthMethods.ManagedIdentity)
        return new DefaultAzureCredential(AzureCredentials.NarrowedDacOptions());

    var miForKv = new DefaultAzureCredential(AzureCredentials.NarrowedDacOptions());
    var kvUri = new Uri($"https://{keyVaultName}.vault.azure.net");

    if (auth.Method == StorageAuthMethods.ServicePrincipalCert)
    {
        // service_principal_cert: pull the cert from Key Vault using the
        // Function App's MI, then build a cert-based credential targeting
        // the customer's tenant. DownloadCertificate makes two calls:
        // GET /certificates/{name} for the metadata (needs Key Vault
        // Certificate User) then GET /secrets/{name} for the full PFX with
        // private key (needs Key Vault Secrets User). The MI must have BOTH
        // roles — terraform grants both at env_module/main.tf.
        var certClient = new CertificateClient(kvUri, miForKv);
        var cert = certClient.DownloadCertificate(auth.CertName!).Value;
        return new ClientCertificateCredential(
            auth.TenantId!, auth.ClientId!, cert,
            new ClientCertificateCredentialOptions { SendCertificateChain = true });
    }

    if (auth.Method == StorageAuthMethods.ServicePrincipalSecret)
    {
        // service_principal_secret: pull the client secret value from Key
        // Vault and build a secret-based credential. Only needs Key Vault
        // Secrets User (no Certificate User), so the secret-mode path is
        // slightly cheaper on RBAC than the cert-mode path. Secret rotation
        // is fully customer-driven here — KV doesn't auto-renew opaque
        // secrets the way it does its own issued certs.
        var secretClient = new SecretClient(kvUri, miForKv);
        var secretValue = secretClient.GetSecret(auth.SecretName!).Value.Value;
        return new ClientSecretCredential(
            auth.TenantId!, auth.ClientId!, secretValue);
    }

    // ConfigLoader validates auth.Method at startup, so this is defense-in-
    // depth against a future regression in the validator or a new method
    // added without a corresponding branch here.
    throw new InvalidOperationException(
        $"Unhandled storage auth method: '{auth.Method}'");
}

builder.Services.AddSingleton<IEntityResolver, EntityResolver>();
builder.Services.AddSingleton<ITenantResolver, TenantResolver>();
builder.Services.AddSingleton<IRunHistoryWriter, RunHistoryWriter>();
builder.Services.AddHttpClient<IAcaJobClient, AcaJobClient>()
    .AddStandardResilienceHandler(options =>
    {
        // ACA /start has no idempotency key. Retrying on 5xx could dupe
        // executions if the server committed before the response was lost.
        // Retry POSTs only on 429 (server rejected pre-processing, safe).
        // GETs retain default transient-retry behavior for responses.
        //
        // Transport-level exceptions (no response at all) can't tell us whether
        // the request hit the server — skip retry entirely to avoid dupe POSTs.
        // We can't distinguish method in the exception case because Outcome.Result
        // is null there, so err on the safe side for both methods.
        options.Retry.ShouldHandle = args =>
        {
            var result = args.Outcome.Result;
            if (result is null) return ValueTask.FromResult(false);

            var method = result.RequestMessage?.Method;
            var status = (int)result.StatusCode;

            if (method == HttpMethod.Post)
                return ValueTask.FromResult(status == 429);

            return ValueTask.FromResult(
                status is 408 or 429 || (status >= 500 && status < 600));
        };

        // ARM POST /executions/{exec}/stop can take >10s under load (#409).
        // The default 10s AttemptTimeout fires before slow stops land, which
        // forces every parallel-fanout cancel onto the reconciler. Give ARM
        // calls more headroom; CircuitBreaker.SamplingDuration must be at
        // least 2× AttemptTimeout, so bump it in lockstep.
        options.AttemptTimeout.Timeout = TimeSpan.FromSeconds(30);
        options.TotalRequestTimeout.Timeout = TimeSpan.FromSeconds(90);
        options.CircuitBreaker.SamplingDuration = TimeSpan.FromSeconds(60);
    });
builder.Services.AddSingleton<IClaimWriter, ClaimWriter>();
builder.Services.AddSingleton<IClaimReader, ClaimReader>();
builder.Services.AddSingleton<IRunHistoryReader, RunHistoryReader>();
builder.Services.AddSingleton<IHeartbeatCache, HeartbeatCache>();
builder.Services.AddSingleton<IRunStateReader, RunStateReader>();
builder.Services.AddSingleton<ITaskStateDeriver, TaskStateDeriver>();
builder.Services.AddSingleton<ICancelIntentStore, CancelIntentStore>();
builder.Services.AddSingleton<IRunCanceller, RunCanceller>();
builder.Services.AddSingleton<IRunTracker, RunTracker>();
builder.Services.AddSingleton<IRunExecutor, RunExecutor>();
builder.Services.AddSingleton<ICronStateStore, CronStateStore>();

// OpenTelemetry + Azure Monitor exporter replaces the legacy AI SDK layering
// (AddApplicationInsightsTelemetryWorkerService + ConfigureFunctionsApplicationInsights).
// Per MS Learn the classic SDK path is labeled "legacy" on .NET isolated worker;
// OpenTelemetry is the recommended direction on .NET 8+ Functions runtimes.
// UseFunctionsWorkerDefaults wires up traces/metrics/logs for the Functions invocation
// pipeline. Azure Monitor export is enabled only when
// APPLICATIONINSIGHTS_CONNECTION_STRING is configured.
// host.json must set telemetryMode=OpenTelemetry to match.
var otelBuilder = builder.Services.AddOpenTelemetry()
    .UseFunctionsWorkerDefaults();

var aiConnectionString = builder.Configuration["APPLICATIONINSIGHTS_CONNECTION_STRING"];
if (!string.IsNullOrWhiteSpace(aiConnectionString))
{
    otelBuilder.UseAzureMonitorExporter();
}
else if (builder.Environment.IsDevelopment())
{
    // Local fallback when no App Insights instance is configured.
    builder.Logging.AddJsonConsole(options =>
    {
        options.IncludeScopes = true;
        options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
    });
}

var host = builder.Build();

// Surface non-fatal ConfigLoader warnings at startup. Resolving IConfigLoader
// here is the eager-load trigger — ConfigLoader's Validate() runs in its ctor,
// so by the time we read Warnings the validation pass has completed.
var loader = host.Services.GetRequiredService<IConfigLoader>();
if (loader.Warnings.Count > 0)
{
    var startupLogger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("ConfigLoader");
    foreach (var w in loader.Warnings)
        startupLogger.LogWarning("Config load warning: {Warning}", w);
}

host.Run();
