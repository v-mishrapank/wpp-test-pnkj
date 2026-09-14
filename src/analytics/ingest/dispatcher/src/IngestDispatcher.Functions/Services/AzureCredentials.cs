using Azure.Identity;

namespace IngestDispatcher.Functions.Services;

public static class AzureCredentials
{
    // DAC narrowed to the sources that actually apply to a headless Function App:
    //   EnvironmentCredential         — CI / tests
    //   WorkloadIdentityCredential    — AKS / federated (future)
    //   ManagedIdentityCredential     — prod (Function App MI)
    //   AzureCliCredential            — local dev via `az login`
    // Skips interactive browser, Visual Studio, VS Code, Azure PowerShell, and
    // azd — none of which apply here and all of which cost cold-start time
    // while probing. SharedTokenCacheCredential was removed from DAC in
    // Azure.Identity 1.17+, so no longer needs an explicit exclude.
    public static DefaultAzureCredentialOptions NarrowedDacOptions() => new()
    {
        ExcludeInteractiveBrowserCredential = true,
        ExcludeVisualStudioCredential = true,
        ExcludeVisualStudioCodeCredential = true,
        ExcludeAzurePowerShellCredential = true,
        ExcludeAzureDeveloperCliCredential = true
    };
}
