using System.Text.Json;
using System.Text.RegularExpressions;
using Cronos;
using IngestDispatcher.Functions.Models;

namespace IngestDispatcher.Functions.Services;

public interface IConfigLoader
{
    EntityRegistryConfig EntityRegistry { get; }
    TenantsConfig Tenants { get; }
    JobsConfig Jobs { get; }
    StorageConfig Storage { get; }

    // Non-fatal load-time issues — currently used for "tenant has both
    // analytics_enabled: false and an entity_selector" (the selector is dead
    // config but not actively harmful). Program.cs surfaces these at startup.
    IReadOnlyList<string> Warnings { get; }
}

public class ConfigLoader : IConfigLoader
{
    // Tenant keys and job names appear as ADLS blob path segments (landing layout,
    // tracking blobs, run history). Restrict to characters that are unambiguous in
    // URLs and rule out path separators that could split a single identifier across
    // multiple segments. The literal "." and ".." segments are also rejected
    // (the regex alone permits them) — some client libraries normalize these
    // into traversal, and they have no legitimate use as identifiers.
    private static readonly Regex SafeIdentifier = new("^[a-zA-Z0-9._-]+$", RegexOptions.Compiled);

    private static bool IsValidIdentifier(string? value) =>
        !string.IsNullOrWhiteSpace(value)
        && SafeIdentifier.IsMatch(value)
        && value != "."
        && value != "..";

    private readonly JsonSerializerOptions _jsonOptions;
    private readonly List<string> _warnings = new();

    public EntityRegistryConfig EntityRegistry { get; }
    public TenantsConfig Tenants { get; }
    public JobsConfig Jobs { get; }
    public StorageConfig Storage { get; }
    public IReadOnlyList<string> Warnings => _warnings;

    public ConfigLoader(string configPath, JsonSerializerOptions jsonOptions)
    {
        _jsonOptions = jsonOptions;

        EntityRegistry = Load<EntityRegistryConfig>(configPath, "entity-registry.json");
        Tenants = Load<TenantsConfig>(configPath, "tenants.json");
        Jobs = Load<JobsConfig>(configPath, "jobs.json");
        Storage = Load<StorageConfig>(configPath, "storage.json");

        Validate();
    }

    private T Load<T>(string configPath, string fileName)
    {
        var filePath = Path.Combine(configPath, fileName);
        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Config file not found: {filePath}");

        var json = File.ReadAllText(filePath);
        return JsonSerializer.Deserialize<T>(json, _jsonOptions)
            ?? throw new InvalidOperationException($"Failed to deserialize {fileName}");
    }

    private void Validate()
    {
        var entityNames = EntityRegistry.Entities.Select(e => e.Name).ToHashSet();

        // Validate entity registry (reject near-duplicates that only differ by case)
        var duplicateEntities = EntityRegistry.Entities
            .GroupBy(e => e.Name, StringComparer.OrdinalIgnoreCase)
            .Where(g => g.Count() > 1)
            .Select(g => string.Join(" / ", g.Select(e => e.Name)))
            .ToList();
        if (duplicateEntities.Count > 0)
            throw new InvalidOperationException(
                $"Duplicate (case-insensitive) entities in entity-registry.json: {string.Join("; ", duplicateEntities)}");

        foreach (var entity in EntityRegistry.Entities)
        {
            if (entity.Tier is < 0 or > 5)
                throw new InvalidOperationException(
                    $"Entity '{entity.Name}' has invalid tier {entity.Tier} (must be 0-5)");
        }

        // Validate tenant keys are unique (reject case-insensitive near-duplicates)
        var duplicateTenants = Tenants.Tenants
            .GroupBy(t => t.TenantKey, StringComparer.OrdinalIgnoreCase)
            .Where(g => g.Count() > 1)
            .Select(g => string.Join(" / ", g.Select(t => t.TenantKey)))
            .ToList();
        if (duplicateTenants.Count > 0)
            throw new InvalidOperationException(
                $"Duplicate (case-insensitive) tenant keys in tenants.json: {string.Join("; ", duplicateTenants)}");

        foreach (var tenant in Tenants.Tenants)
        {
            if (!IsValidIdentifier(tenant.TenantKey))
                throw new InvalidOperationException(
                    $"Tenant key '{tenant.TenantKey}' must match {SafeIdentifier} (alphanumerics, '.', '_', '-') and cannot be '.' or '..'");

            if (tenant.EntitySelector is { } sel)
            {
                // Reject "empty selector" — all three fields null/empty resolves to
                // zero entities (EntityResolver.Resolve returns an empty list), which
                // is almost certainly an operator mistake. To opt a tenant out of all
                // analytics, set `analytics_enabled: false` instead.
                var hasFields = (sel.IncludeTiers is { Count: > 0 })
                              || (sel.IncludeEntities is { Count: > 0 })
                              || (sel.ExcludeEntities is { Count: > 0 });
                if (!hasFields)
                    throw new InvalidOperationException(
                        $"Tenant '{tenant.TenantKey}' has an empty entity_selector. Use `analytics_enabled: false` to opt out, or set at least one of include_tiers / include_entities / exclude_entities.");

                foreach (var include in sel.IncludeEntities ?? [])
                {
                    if (!entityNames.Contains(include))
                        throw new InvalidOperationException(
                            $"Tenant '{tenant.TenantKey}' references unknown entity '{include}' in entity_selector.include_entities");
                }
                foreach (var exclude in sel.ExcludeEntities ?? [])
                {
                    if (!entityNames.Contains(exclude))
                        throw new InvalidOperationException(
                            $"Tenant '{tenant.TenantKey}' references unknown entity '{exclude}' in entity_selector.exclude_entities");
                }

                foreach (var tier in sel.IncludeTiers ?? [])
                {
                    if (tier is < 0 or > 5)
                        throw new InvalidOperationException(
                            $"Tenant '{tenant.TenantKey}' has invalid include_tier {tier} in entity_selector (must be 0-5)");
                }

                if (sel.IncludeEntities is { Count: > 0 } incl
                    && sel.ExcludeEntities is { Count: > 0 } excl)
                {
                    var overlap = incl.Intersect(excl).ToList();
                    if (overlap.Count > 0)
                        throw new InvalidOperationException(
                            $"Tenant '{tenant.TenantKey}' entity_selector has overlapping include_entities and exclude_entities: {string.Join(", ", overlap)}");
                }

                // `analytics_enabled: false` short-circuits in TenantResolver before
                // any entity selector is consulted, so combining it with a non-null
                // selector is harmless but dead config. Warn so an operator notices.
                if (!tenant.AnalyticsEnabled)
                    _warnings.Add(
                        $"Tenant '{tenant.TenantKey}' has analytics_enabled: false AND entity_selector set; the selector is dead config (analytics_enabled wins).");
            }
        }

        // Validate storage auth
        if (Storage.Auth.Method is not (StorageAuthMethods.ManagedIdentity
            or StorageAuthMethods.ServicePrincipalCert
            or StorageAuthMethods.ServicePrincipalSecret))
            throw new InvalidOperationException(
                $"Invalid storage auth method: '{Storage.Auth.Method}' (must be " +
                $"'{StorageAuthMethods.ManagedIdentity}', " +
                $"'{StorageAuthMethods.ServicePrincipalCert}', or " +
                $"'{StorageAuthMethods.ServicePrincipalSecret}')");

        if (Storage.Auth.Method == StorageAuthMethods.ServicePrincipalCert)
        {
            if (string.IsNullOrWhiteSpace(Storage.Auth.TenantId))
                throw new InvalidOperationException("Storage auth: service_principal_cert requires tenant_id");
            if (string.IsNullOrWhiteSpace(Storage.Auth.ClientId))
                throw new InvalidOperationException("Storage auth: service_principal_cert requires client_id");
            if (string.IsNullOrWhiteSpace(Storage.Auth.CertName))
                throw new InvalidOperationException("Storage auth: service_principal_cert requires cert_name");
        }

        if (Storage.Auth.Method == StorageAuthMethods.ServicePrincipalSecret)
        {
            if (string.IsNullOrWhiteSpace(Storage.Auth.TenantId))
                throw new InvalidOperationException("Storage auth: service_principal_secret requires tenant_id");
            if (string.IsNullOrWhiteSpace(Storage.Auth.ClientId))
                throw new InvalidOperationException("Storage auth: service_principal_secret requires client_id");
            if (string.IsNullOrWhiteSpace(Storage.Auth.SecretName))
                throw new InvalidOperationException("Storage auth: service_principal_secret requires secret_name");
        }

        // Validate job uniqueness (case-insensitive). Active-run tracking is keyed by job
        // name, so near-duplicates would collapse into a single overlap slot.
        var duplicateJobs = Jobs.Jobs
            .GroupBy(j => j.Name, StringComparer.OrdinalIgnoreCase)
            .Where(g => g.Count() > 1)
            .Select(g => string.Join(" / ", g.Select(j => j.Name)))
            .ToList();
        if (duplicateJobs.Count > 0)
            throw new InvalidOperationException(
                $"Duplicate (case-insensitive) job names in jobs.json: {string.Join("; ", duplicateJobs)}");

        // Validate job references
        var tenantKeys = Tenants.Tenants.Select(t => t.TenantKey).ToHashSet();

        foreach (var job in Jobs.Jobs)
        {
            // Tracking blobs are keyed by job name (_dispatcher/tracking/{jobName}.json).
            // Same SafeIdentifier rules as tenant_key — single path segment, URL-safe.
            if (!IsValidIdentifier(job.Name))
                throw new InvalidOperationException(
                    $"Job name '{job.Name}' must match {SafeIdentifier} (alphanumerics, '.', '_', '-') and cannot be '.' or '..'");

            // Validate cron expression (if present)
            if (!string.IsNullOrEmpty(job.Cron))
            {
                try
                {
                    CronExpression.Parse(job.Cron);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Job '{job.Name}' has invalid cron expression '{job.Cron}': {ex.Message}");
                }
            }

            // Validate entity references
            foreach (var include in job.EntitySelector.IncludeEntities ?? [])
            {
                if (!entityNames.Contains(include))
                    throw new InvalidOperationException(
                        $"Job '{job.Name}' references unknown entity '{include}' in include_entities");
            }
            foreach (var exclude in job.EntitySelector.ExcludeEntities ?? [])
            {
                if (!entityNames.Contains(exclude))
                    throw new InvalidOperationException(
                        $"Job '{job.Name}' references unknown entity '{exclude}' in exclude_entities");
            }

            // Reject include/exclude overlap. EntityResolver applies excludes after
            // includes so the entity would be silently dropped — almost always a
            // copy/paste mistake. Symmetric with the tenant-selector check above.
            if (job.EntitySelector.IncludeEntities is { Count: > 0 } jincl
                && job.EntitySelector.ExcludeEntities is { Count: > 0 } jexcl)
            {
                var overlap = jincl.Intersect(jexcl).ToList();
                if (overlap.Count > 0)
                    throw new InvalidOperationException(
                        $"Job '{job.Name}' entity_selector has overlapping include_entities and exclude_entities: {string.Join(", ", overlap)}");
            }

            // Validate tenant selector references
            foreach (var key in job.TenantSelector.TenantKeys ?? [])
            {
                if (!tenantKeys.Contains(key))
                    throw new InvalidOperationException(
                        $"Job '{job.Name}' references unknown tenant '{key}' in tenant_keys");
            }
            foreach (var key in job.TenantSelector.ExcludeKeys ?? [])
            {
                if (!tenantKeys.Contains(key))
                    throw new InvalidOperationException(
                        $"Job '{job.Name}' references unknown tenant '{key}' in exclude_keys");
            }

            // Validate tenant selector mode has required fields
            if (job.TenantSelector.Mode == TenantSelectorModes.Specific && (job.TenantSelector.TenantKeys == null || job.TenantSelector.TenantKeys.Count == 0))
                throw new InvalidOperationException(
                    $"Job '{job.Name}' uses mode '{TenantSelectorModes.Specific}' but has no tenant_keys");
            if (job.TenantSelector.Mode == TenantSelectorModes.AllExcept && (job.TenantSelector.ExcludeKeys == null || job.TenantSelector.ExcludeKeys.Count == 0))
                throw new InvalidOperationException(
                    $"Job '{job.Name}' uses mode '{TenantSelectorModes.AllExcept}' but has no exclude_keys");

            // Validate include_tiers range
            foreach (var tier in job.EntitySelector.IncludeTiers ?? [])
            {
                if (tier is < 0 or > 5)
                    throw new InvalidOperationException(
                        $"Job '{job.Name}' has invalid include_tier {tier} (must be 0-5)");
            }

            // Validate optional per-job timeout. Resolver silently ignores
            // <= 0 to fall back to the dispatcher default; a misconfigured
            // jobs.json would then behave like "unset" without any signal.
            // Fail fast at startup instead.
            if (job.TimeoutSeconds is { } ts && ts <= 0)
                throw new InvalidOperationException(
                    $"Job '{job.Name}' has invalid timeout_seconds {ts} (must be > 0 if set)");
        }

        // Cross-config dead-job check. For each scheduled job that explicitly
        // names entities, confirm at least one applicable tenant allows each
        // named entity. A job where every applicable tenant blocks the named
        // entity is a guaranteed no-op — fail load so the misconfig surfaces
        // at deploy time, not at first scheduled fire (where it would only
        // show up as warning logs + skipped_filter RunRecords).
        foreach (var job in Jobs.Jobs)
        {
            if (string.IsNullOrEmpty(job.Cron) || !job.Enabled) continue;
            if (job.EntitySelector.IncludeEntities is not { Count: > 0 } includeNames) continue;

            var applicable = ApplicableTenants(job.TenantSelector);
            if (applicable.Count == 0) continue;

            foreach (var entity in includeNames)
            {
                var anyAllows = applicable.Any(t =>
                    t.EntitySelector is null
                    || AllowedTenantEntityNames(t.EntitySelector).Contains(entity));
                if (!anyAllows)
                    throw new InvalidOperationException(
                        $"Scheduled job '{job.Name}' includes entity '{entity}' but every applicable tenant blocks it via entity_selector. The job would be a guaranteed no-op — adjust the job's tenant_selector, the tenants' entity_selectors, or remove the entity from include_entities.");
            }
        }
    }

    // Inlined mirrors of TenantResolver.Resolve / EntityResolver.Resolve. Used
    // only by the cross-config dead-job check above; not exposed. Kept inline
    // because ConfigLoader is built before the resolver services exist (the
    // resolvers depend on IConfigLoader via DI).
    private List<TenantConfig> ApplicableTenants(TenantSelector selector)
    {
        var enabled = Tenants.Tenants.Where(t => t.AnalyticsEnabled).ToList();
        return selector.Mode switch
        {
            TenantSelectorModes.All => enabled,
            TenantSelectorModes.Specific => enabled
                .Where(t => selector.TenantKeys?.Contains(t.TenantKey) == true)
                .ToList(),
            TenantSelectorModes.AllExcept => enabled
                .Where(t => selector.ExcludeKeys?.Contains(t.TenantKey) != true)
                .ToList(),
            _ => []  // Unknown modes throw at TenantResolver runtime; no behavior to anticipate here.
        };
    }

    // Tenant-side allowlist semantics: when no include_* is set, the implicit
    // allowlist is "everything in the registry" (exclude-only is filter-out,
    // not start-from-nothing). Mirrors EntityResolver.Intersect for the
    // cross-config dead-job check.
    private HashSet<string> AllowedTenantEntityNames(EntitySelector selector)
    {
        var hasInclude = (selector.IncludeTiers is { Count: > 0 })
                      || (selector.IncludeEntities is { Count: > 0 });

        HashSet<string> result;
        if (hasInclude)
        {
            result = new HashSet<string>();
            if (selector.IncludeTiers is { Count: > 0 } tiers)
            {
                var tierSet = tiers.ToHashSet();
                foreach (var e in EntityRegistry.Entities)
                    if (tierSet.Contains(e.Tier)) result.Add(e.Name);
            }
            if (selector.IncludeEntities is { Count: > 0 } incl)
                foreach (var n in incl) result.Add(n);
        }
        else
        {
            result = EntityRegistry.Entities.Select(e => e.Name).ToHashSet();
        }

        if (selector.ExcludeEntities is { Count: > 0 } excl)
            foreach (var n in excl) result.Remove(n);

        return result;
    }
}
