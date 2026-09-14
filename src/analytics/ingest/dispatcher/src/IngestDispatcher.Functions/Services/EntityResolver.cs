using IngestDispatcher.Functions.Models;

namespace IngestDispatcher.Functions.Services;

public interface IEntityResolver
{
    IReadOnlyList<EntityType> Resolve(EntitySelector selector);

    // Intersect a job-resolved entity list with a tenant's optional entity_selector.
    // Null selector = no constraint (pass-through). Non-null = resolve the tenant
    // selector against the registry and return the intersection (job ∩ tenant), in
    // job-list order. Used by RunExecutor to apply per-tenant filtering at dispatch.
    IReadOnlyList<EntityType> Intersect(EntitySelector? tenantSelector, IReadOnlyList<EntityType> jobEntities);
}

public class EntityResolver : IEntityResolver
{
    private readonly IReadOnlyList<EntityType> _entities;

    public EntityResolver(IConfigLoader configLoader)
    {
        _entities = configLoader.EntityRegistry.Entities;
    }

    public IReadOnlyList<EntityType> Resolve(EntitySelector selector)
    {
        var result = new HashSet<string>();

        // Step 1: Add all entities from included tiers
        if (selector.IncludeTiers is { Count: > 0 })
        {
            var tierSet = selector.IncludeTiers.ToHashSet();
            foreach (var entity in _entities)
            {
                if (tierSet.Contains(entity.Tier))
                    result.Add(entity.Name);
            }
        }

        // Step 2: Add explicitly included entities
        if (selector.IncludeEntities is { Count: > 0 })
        {
            foreach (var name in selector.IncludeEntities)
                result.Add(name);
        }

        // Step 3: Remove explicitly excluded entities
        if (selector.ExcludeEntities is { Count: > 0 })
        {
            foreach (var name in selector.ExcludeEntities)
                result.Remove(name);
        }

        // Return in registry order for deterministic output
        return _entities
            .Where(e => result.Contains(e.Name))
            .ToList();
    }

    public IReadOnlyList<EntityType> Intersect(EntitySelector? tenantSelector, IReadOnlyList<EntityType> jobEntities)
    {
        if (tenantSelector is null) return jobEntities;

        // Tenant-side semantics differ from job-side. On the job, an
        // EntitySelector with no `include_*` resolves to an empty set (Resolve()
        // returns []). On the tenant, the operator-intuitive meaning of an
        // exclude-only selector is "everything except these" — not "nothing,
        // then subtract these." So: if the tenant sets any `include_*` field
        // those are an allowlist (intersected with the job's set); otherwise
        // the allowlist is implicit (everything). `exclude_entities` always
        // removes, after the allowlist applies.
        var hasInclude = (tenantSelector.IncludeTiers is { Count: > 0 })
                      || (tenantSelector.IncludeEntities is { Count: > 0 });

        HashSet<string>? allowedNames = null;
        if (hasInclude)
        {
            allowedNames = new HashSet<string>();
            if (tenantSelector.IncludeTiers is { Count: > 0 } tiers)
            {
                var tierSet = tiers.ToHashSet();
                foreach (var e in _entities)
                    if (tierSet.Contains(e.Tier)) allowedNames.Add(e.Name);
            }
            if (tenantSelector.IncludeEntities is { Count: > 0 } incl)
                foreach (var n in incl) allowedNames.Add(n);
        }

        var excludes = tenantSelector.ExcludeEntities is { Count: > 0 } excl
            ? excl.ToHashSet()
            : null;

        return jobEntities
            .Where(e => (allowedNames is null || allowedNames.Contains(e.Name))
                     && (excludes is null || !excludes.Contains(e.Name)))
            .ToList();
    }
}
