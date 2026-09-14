using IngestDispatcher.Functions.Models;

namespace IngestDispatcher.Functions.Services;

public interface ITenantResolver
{
    IReadOnlyList<TenantConfig> Resolve(TenantSelector selector);
}

public class TenantResolver : ITenantResolver
{
    private readonly IReadOnlyList<TenantConfig> _tenants;

    public TenantResolver(IConfigLoader configLoader)
    {
        _tenants = configLoader.Tenants.Tenants;
    }

    public IReadOnlyList<TenantConfig> Resolve(TenantSelector selector)
    {
        var enabled = _tenants.Where(t => t.AnalyticsEnabled).ToList();

        return selector.Mode switch
        {
            TenantSelectorModes.All => enabled,
            TenantSelectorModes.Specific => enabled
                .Where(t => selector.TenantKeys?.Contains(t.TenantKey) == true)
                .ToList(),
            TenantSelectorModes.AllExcept => enabled
                .Where(t => selector.ExcludeKeys?.Contains(t.TenantKey) != true)
                .ToList(),
            _ => throw new ArgumentException($"Unknown tenant selector mode: '{selector.Mode}'")
        };
    }
}
