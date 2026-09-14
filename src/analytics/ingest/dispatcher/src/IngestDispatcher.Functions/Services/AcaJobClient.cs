using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Identity;
using IngestDispatcher.Functions.Models;
using IngestDispatcher.Functions.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IngestDispatcher.Functions.Services;

public class AcaExecutionNotFoundException : Exception
{
    public string ContainerJobName { get; }
    public string ExecutionName { get; }

    public AcaExecutionNotFoundException(string containerJobName, string executionName)
        : base($"ACA Job execution {containerJobName}/{executionName} no longer exists (404)")
    {
        ContainerJobName = containerJobName;
        ExecutionName = executionName;
    }
}

// Fields needed from the job's stored template to construct a container-override
// start request. The ACA start endpoint replaces the container spec rather than
// merging with the template (see StartJobAsync), so resources must be echoed
// back on every dispatch or executions silently fall back to ACA defaults
// (0.5 CPU / 1Gi) — the root cause of issue #158.
//
// SpecEnv captures the template's plain-value env entries so StartJobAsync can
// re-emit them under the same replace-not-merge constraint. Without this,
// Terraform-managed env vars on the job template silently disappear at run time
// (#355). Entries using secretRef are intentionally skipped — resolving them
// would require KV access at dispatch time; the few we use today are dispatcher-
// owned plain values anyway.
public record JobContainerTemplate(
    string Image,
    double Cpu,
    string Memory,
    IReadOnlyList<KeyValuePair<string, string>>? SpecEnv = null);

public interface IAcaJobClient
{
    Task<JobContainerTemplate> GetJobTemplateAsync(string containerJobName);
    Task<string> StartJobAsync(string containerJobName, JobContainerTemplate template, TenantConfig tenant,
        IReadOnlyList<string> entityNames, StorageConfig storage, string runId,
        string runType,
        DateTimeOffset? backfillStart = null,
        DateTimeOffset? backfillEnd = null);
    Task<string> GetExecutionStatusAsync(string containerJobName, string executionName);

    // POST .../jobs/{name}/executions/{execution}/stop on ARM. Returns true when
    // the stop was accepted (2xx) or the execution is already gone (404 — same
    // semantics as "stopped" from the caller's perspective). Returns false on
    // transient ARM failures (5xx, network). Caller is expected to retry.
    // Non-success non-transient (4xx other than 404) throws.
    Task<bool> CancelExecutionAsync(string containerJobName, string executionName, string? runId = null);
}

public class AcaJobClient : IAcaJobClient
{
    private const string ArmApiVersion = "2024-03-01";
    private const string ArmBaseUrl = "https://management.azure.com";

    private readonly HttpClient _httpClient;
    private readonly IngestSettings _settings;
    private readonly IConfigLoader _configLoader;
    private readonly ILogger<AcaJobClient> _logger;
    // ARM calls always use the Function App's own MI regardless of storage.Auth.Method.
    private readonly DefaultAzureCredential _credential = new(AzureCredentials.NarrowedDacOptions());

    public AcaJobClient(
        HttpClient httpClient,
        IOptions<IngestSettings> settings,
        IConfigLoader configLoader,
        ILogger<AcaJobClient> logger)
    {
        _httpClient = httpClient;
        _settings = settings.Value;
        _configLoader = configLoader;
        _logger = logger;
    }

    public async Task<JobContainerTemplate> GetJobTemplateAsync(string containerJobName)
    {
        var token = await GetAccessTokenAsync();

        var jobUrl = $"{ArmBaseUrl}/subscriptions/{_settings.SubscriptionId}/resourceGroups/{_settings.ResourceGroupName}" +
                     $"/providers/Microsoft.App/jobs/{containerJobName}?api-version={ArmApiVersion}";

        using var getRequest = new HttpRequestMessage(HttpMethod.Get, jobUrl);
        getRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var jobResponse = await _httpClient.SendAsync(getRequest);
        if (!jobResponse.IsSuccessStatusCode)
        {
            var errorBody = await jobResponse.Content.ReadAsStringAsync();
            throw new InvalidOperationException(
                $"Failed to get ACA Job config for {containerJobName}: {jobResponse.StatusCode} - {errorBody}");
        }

        var jobJson = await jobResponse.Content.ReadAsStringAsync();
        var jobDoc = JsonDocument.Parse(jobJson);

        if (!jobDoc.RootElement.TryGetProperty("properties", out var props) ||
            !props.TryGetProperty("template", out var template) ||
            !template.TryGetProperty("containers", out var containers) ||
            containers.GetArrayLength() == 0 ||
            containers[0].GetProperty("image").GetString() is not { } image)
        {
            throw new InvalidOperationException(
                $"ACA Job {containerJobName} response missing expected properties.template.containers[0].image");
        }

        if (!containers[0].TryGetProperty("resources", out var resources) ||
            !resources.TryGetProperty("cpu", out var cpuEl) ||
            !resources.TryGetProperty("memory", out var memEl) ||
            !cpuEl.TryGetDouble(out var cpu) ||
            memEl.GetString() is not { } memory)
        {
            throw new InvalidOperationException(
                $"ACA Job {containerJobName} response missing expected properties.template.containers[0].resources.{{cpu,memory}}");
        }

        // Capture plain-value env from the template so StartJobAsync can re-emit
        // it. secretRef entries are skipped; they'd need KV access at dispatch
        // time and we don't use any today on these jobs. (#355)
        //
        // Log levels (#358):
        //   - secretRef: WARN. This is the silent-drop pattern that motivated
        //     filing #355 in the first place. Any future Terraform-managed
        //     secretRef env var would otherwise disappear at dispatch with no
        //     trace at the default Functions log verbosity.
        //   - duplicate name in the spec env list: WARN. The ACA env block is
        //     a list-of-objects, not a map — duplicates are syntactically valid
        //     but almost certainly misconfiguration. Last-write-wins silently
        //     would hide that.
        var specEnv = new List<KeyValuePair<string, string>>();
        var seenSpecNames = new HashSet<string>(StringComparer.Ordinal);
        if (containers[0].TryGetProperty("env", out var envArr) && envArr.ValueKind == JsonValueKind.Array)
        {
            foreach (var entry in envArr.EnumerateArray())
            {
                if (!entry.TryGetProperty("name", out var nameEl)) continue;
                var name = nameEl.GetString();
                if (string.IsNullOrEmpty(name)) continue;
                if (entry.TryGetProperty("value", out var valEl) && valEl.ValueKind == JsonValueKind.String)
                {
                    if (!seenSpecNames.Add(name))
                    {
                        _logger.LogWarning(
                            "Duplicate env var name {Name} on job {Job} template; later occurrence wins",
                            name, containerJobName);
                    }
                    specEnv.Add(new KeyValuePair<string, string>(name, valEl.GetString() ?? ""));
                }
                else if (entry.TryGetProperty("secretRef", out _))
                {
                    _logger.LogWarning(
                        "Skipping spec env var {Name} on job {Job}: secretRef pass-through is not implemented by the dispatcher. " +
                        "The container will not see this variable at runtime.",
                        name, containerJobName);
                }
            }
        }

        return new JobContainerTemplate(image, cpu, memory, specEnv);
    }

    public async Task<string> StartJobAsync(string containerJobName, JobContainerTemplate template, TenantConfig tenant,
        IReadOnlyList<string> entityNames, StorageConfig storage, string runId,
        string runType,
        DateTimeOffset? backfillStart = null,
        DateTimeOffset? backfillEnd = null)
    {
        var token = await GetAccessTokenAsync();
        var envVars = BuildEnvVars(containerJobName, tenant, entityNames, storage, runId, runType, backfillStart, backfillEnd);

        // Merge spec env first, then overlay dispatcher-injected vars — dispatcher
        // wins on collision because its values are per-execution (TENANT_KEY, RUN_ID,
        // etc.) and the spec can't predict them. Spec-only keys pass through
        // unchanged. Same replace-not-merge constraint as resources (#158/#355).
        var mergedEnv = new Dictionary<string, string>();
        if (template.SpecEnv != null)
        {
            foreach (var kv in template.SpecEnv)
            {
                mergedEnv[kv.Key] = kv.Value;
            }
        }
        foreach (var kv in envVars)
        {
            mergedEnv[kv.Key] = kv.Value;
        }

        var startUrl = $"{ArmBaseUrl}/subscriptions/{_settings.SubscriptionId}/resourceGroups/{_settings.ResourceGroupName}" +
                       $"/providers/Microsoft.App/jobs/{containerJobName}/start?api-version={ArmApiVersion}";

        // ACA's start endpoint replaces the container spec rather than merging —
        // omitted fields fall back to service defaults (0.5 CPU / 1Gi), not the
        // job template's values. Echo the template's resources on every dispatch
        // so executions run at the sized cpu/memory. (Fix for #158 root cause.)
        var body = new
        {
            containers = new[]
            {
                new
                {
                    name = "ingest",
                    image = template.Image,
                    resources = new { cpu = template.Cpu, memory = template.Memory },
                    env = mergedEnv.Select(kv => new { name = kv.Key, value = kv.Value }).ToArray()
                }
            }
        };

        using var startRequest = new HttpRequestMessage(HttpMethod.Post, startUrl);
        startRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        startRequest.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        using var startResponse = await _httpClient.SendAsync(startRequest);
        if (!startResponse.IsSuccessStatusCode)
        {
            var errorBody = await startResponse.Content.ReadAsStringAsync();
            throw new InvalidOperationException(
                $"Failed to start ACA Job {containerJobName}: {startResponse.StatusCode} - {errorBody}");
        }

        var startJson = await startResponse.Content.ReadAsStringAsync();
        var startDoc = JsonDocument.Parse(startJson);
        var executionId = startDoc.RootElement.GetProperty("id").GetString()
            ?? throw new InvalidOperationException($"ACA Job start response missing 'id' field");
        var executionName = executionId.Split('/').Last();

        _logger.LogInformation("Started ACA Job {Job} execution {Execution} for tenant {Tenant}",
            containerJobName, executionName, tenant.TenantKey);

        return executionName;
    }

    public async Task<string> GetExecutionStatusAsync(string containerJobName, string executionName)
    {
        var token = await GetAccessTokenAsync();

        var url = $"{ArmBaseUrl}/subscriptions/{_settings.SubscriptionId}/resourceGroups/{_settings.ResourceGroupName}" +
                  $"/providers/Microsoft.App/jobs/{containerJobName}/executions/{executionName}?api-version={ArmApiVersion}";

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var response = await _httpClient.SendAsync(request);

        // 404 is terminal: the ACA execution no longer exists (job redeployed, retention
        // expiry, or never started). Surface it as a typed signal so RunTracker can mark
        // the task Failed immediately instead of backing off until the 8h run timeout.
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            throw new AcaExecutionNotFoundException(containerJobName, executionName);
        }

        if (!response.IsSuccessStatusCode)
        {
            var errorBody = await response.Content.ReadAsStringAsync();
            throw new InvalidOperationException(
                $"Failed to get execution status for {containerJobName}/{executionName}: {response.StatusCode} - {errorBody}");
        }

        var json = await response.Content.ReadAsStringAsync();
        var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("properties").GetProperty("status").GetString()
            ?? throw new InvalidOperationException($"Execution status response missing properties.status");
    }

    public async Task<bool> CancelExecutionAsync(string containerJobName, string executionName, string? runId = null)
    {
        var token = await GetAccessTokenAsync();

        var url = $"{ArmBaseUrl}/subscriptions/{_settings.SubscriptionId}/resourceGroups/{_settings.ResourceGroupName}" +
                  $"/providers/Microsoft.App/jobs/{containerJobName}/executions/{executionName}/stop?api-version={ArmApiVersion}";

        using var request = new HttpRequestMessage(HttpMethod.Post, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var response = await _httpClient.SendAsync(request);

        var status = (int)response.StatusCode;
        switch (ClassifyStopResponse(status))
        {
            case StopResponseClass.Accepted:
                _logger.LogInformation(
                    "Submitted stop for {Container}/{Execution} (run {RunId}): {Status}",
                    containerJobName, executionName, runId, status);
                return true;
            case StopResponseClass.AlreadyGone:
                _logger.LogInformation(
                    "Stop request for {Container}/{Execution} (run {RunId}) returned 404 — execution is already gone",
                    containerJobName, executionName, runId);
                return true;
            case StopResponseClass.Transient:
                var transientBody = await response.Content.ReadAsStringAsync();
                _logger.LogWarning(
                    "Stop request for {Container}/{Execution} (run {RunId}) got transient {Status}: {Body}",
                    containerJobName, executionName, runId, status, transientBody);
                return false;
            default:
                var hardErrorBody = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException(
                    $"Failed to stop {containerJobName}/{executionName} (run {runId}): {response.StatusCode} - {hardErrorBody}");
        }
    }

    internal enum StopResponseClass
    {
        Accepted,      // 2xx — ACA accepted the stop
        AlreadyGone,   // 404 — execution no longer exists (same outcome from dispatcher's POV)
        Transient,     // 5xx, 429 (throttle), 408 (timeout) — caller retries
        HardFailure,   // other 4xx — not retryable, throw
    }

    // Classifies an ARM POST /stop response status code. Split out so the
    // policy (especially "treat 408/429 as transient") is testable without
    // mocking HttpMessageHandler.
    internal static StopResponseClass ClassifyStopResponse(int statusCode) => statusCode switch
    {
        >= 200 and < 300 => StopResponseClass.Accepted,
        404 => StopResponseClass.AlreadyGone,
        408 or 429 => StopResponseClass.Transient,
        >= 500 and < 600 => StopResponseClass.Transient,
        _ => StopResponseClass.HardFailure,
    };

    // internal — exposed for unit tests via InternalsVisibleTo. Pure mapping
    // function over (tenant, settings, runId, …), no I/O, so it's the right
    // surface to assert wire-contract invariants (SCOPE_ROOT emission,
    // backfill env, storage-auth conditional vars). See #260.
    internal Dictionary<string, string> BuildEnvVars(string containerJobName, TenantConfig tenant,
        IReadOnlyList<string> entityNames, StorageConfig storage, string runId,
        string runType,
        DateTimeOffset? backfillStart,
        DateTimeOffset? backfillEnd)
    {
        var vars = new Dictionary<string, string>
        {
            ["TENANT_KEY"] = tenant.TenantKey,
            ["TENANT_ID"] = tenant.TenantId,
            ["ORGANIZATION"] = tenant.Organization,
            ["CLIENT_ID"] = _settings.IngestClientId,
            ["CERT_NAME"] = _settings.IngestCertName,
            ["ENTITY_NAMES"] = string.Join(",", entityNames),
            ["KEYVAULT_NAME"] = _settings.KeyVaultName,
            ["MAX_CONCURRENCY"] = tenant.AnalyticsMaxConcurrency.ToString(),
            ["STORAGE_ACCOUNT_URL"] = storage.AccountUrl,
            ["LANDING_CONTAINER"] = storage.Container,
            ["STORAGE_AUTH_METHOD"] = storage.Auth.Method,
            // RUN_ID correlates dispatcher RunRecord, every customEvent, every
            // manifest. PS-side falls back to a locally generated guid if this
            // env var is unset (preserves manual `docker run` testability).
            // See EVENT_SCHEMA.md.
            ["RUN_ID"] = runId,
            // CONTAINER_TYPE tells the script which name to use when writing
            // the per-task summary blob the dispatcher reads to map manifest
            // outcomes onto task.Status (#164).
            ["CONTAINER_TYPE"] = containerJobName
        };

        if (tenant.AdminUrl is not null)
            vars["ADMIN_URL"] = tenant.AdminUrl;

        // Scoped tenants: signal consumers (entra_users, exo_mailboxes, spo_sites
        // root stages) to filter their work to the set(s) under
        // _scope/<tenant_key>/<dimension>/ on the landing container. Two
        // dimensions are wired in #260: `users` (UPN set — entra_users,
        // exo_mailboxes, and spo_sites personal-OneDrive emissions) and
        // `sites` (webUrl set — spo_sites non-personal emissions). Containers
        // append the dimension subfolder themselves, so adding a future
        // dimension (e.g., `groups`) doesn't require dispatcher changes.
        if (tenant.Scoped)
            vars["SCOPE_ROOT"] = $"_scope/{tenant.TenantKey}";

        if (storage.Auth.Method == StorageAuthMethods.ServicePrincipalCert)
        {
            vars["STORAGE_SP_TENANT_ID"] = storage.Auth.TenantId!;
            vars["STORAGE_SP_CLIENT_ID"] = storage.Auth.ClientId!;
            vars["STORAGE_SP_CERT_NAME"] = storage.Auth.CertName!;
        }
        else if (storage.Auth.Method == StorageAuthMethods.ServicePrincipalSecret)
        {
            vars["STORAGE_SP_TENANT_ID"] = storage.Auth.TenantId!;
            vars["STORAGE_SP_CLIENT_ID"] = storage.Auth.ClientId!;
            vars["STORAGE_SP_SECRET_NAME"] = storage.Auth.SecretName!;
        }

        // Backfill window is injected only when RunType=backfill. log-ingest's
        // entity modules read these to override their HWM-driven window calc
        // and skip HWM read/write for the duration of the run. Other jobs
        // ignore them.
        if (runType == RunTypes.Backfill && backfillStart.HasValue && backfillEnd.HasValue)
        {
            vars["BACKFILL_MODE"] = "true";
            vars["BACKFILL_START"] = backfillStart.Value.UtcDateTime.ToString("o");
            vars["BACKFILL_END"] = backfillEnd.Value.UtcDateTime.ToString("o");
        }

        return vars;
    }

    private async Task<string> GetAccessTokenAsync()
    {
        var token = await _credential.GetTokenAsync(
            new Azure.Core.TokenRequestContext(["https://management.azure.com/.default"]));
        return token.Token;
    }
}
