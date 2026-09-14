# Power Platform container's auth module.
#
# Exports:
#   Connect-Service           — initial auth. Called once in the main process
#                               (by Invoke-Ingestion.ps1) and once per worker
#                               runspace (by the dispatch template's
#                               first-dispatch self-auth, see WorkerPool.psm1).
#   Restore-ServiceConnection — mid-run auth recovery. Called inside a worker
#                               runspace when a fetch is classified Auth and
#                               we reconnect before retrying.
#   Get-PowerPlatToken        — public token-cache accessor used by entity
#                               modules. Returns a cached token for an
#                               audience or acquires a fresh one.
#
# === Why this differs from graph/exo Connect.psm1 ===
#
# Power Platform has no PowerShell module that's compatible with pwsh 7 /
# Linux (Microsoft.PowerApps.Administration.PowerShell hard-fails at
# Import-Module — see #243 verification). Auth is therefore raw MSAL via the
# shared MsalTokenHelper, and data access is raw Invoke-RestMethod with a
# Bearer header. There's no Connect-MgGraph / Connect-ExchangeOnline session
# object — instead we maintain a per-audience token cache keyed by audience
# string ($script:TokenCache).
#
# Two audience families: legacy-admin-plane (https://service.powerapps.com/.default,
# covers BAP / PowerApps admin / Flow admin) and per-env Dataverse Web API
# (https://{org}.crm.dynamics.com/.default — used by dataverse_onboardings
# and the 5 Dataverse data entities). The Dataverse audience is derived
# from each env's instanceUrl (non-`api.` host); REST calls go against
# instanceApiUrl (`api.` host). One Dataverse audience per env is cached;
# 1 + N entries total in steady state.
#
# MSAL's AcquireTokenForClient handles caching internally for its own
# application object, but we cache the resulting bearer string so entity
# modules don't need to reach into MSAL.
#
# === Auth state lives in $script: scope ===
#
# Connect-Service stashes $script:AuthConfig (which carries the base64 cert
# for token re-acquisition); Get-PowerPlatToken reads it. Module-scope is
# runspace-local — see graph-ingest/Connect.psm1 for the explainer.
#
# === Env list ===
#
# environments_root in PowerPlatEnvironments.psm1 IS the BAP env-list fetch.
# Per-env URL data (instanceUrl, instanceApiUrl, environmentType) flows to
# pool workers via $Context.InputTags (#257). Connect.psm1 doesn't fetch or
# cache the env list anymore — auth + sanity probe + token cache only.

# Get-IngestAccessToken comes from shared/modules/MsalTokenHelper.psm1, which
# Invoke-Ingestion.ps1 imports globally before any container module loads.

$script:AuthConfig = $null

# Keyed by audience string. Populated lazily by Get-PowerPlatToken.
# $script: scope: visible to all functions in this module within the same
# session; not visible across runspaces (workers re-import this module and
# get their own $script:TokenCache).
$script:TokenCache = @{}

$LegacyAdminAudience = 'https://service.powerapps.com/.default'

function Get-PowerPlatToken {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Audience
    )

    if ($script:TokenCache.ContainsKey($Audience)) {
        return $script:TokenCache[$Audience]
    }

    if (-not $script:AuthConfig) {
        throw "Get-PowerPlatToken called before Connect-Service initialized auth state."
    }
    $cfg = $script:AuthConfig

    $bytes = [Convert]::FromBase64String($cfg.CertificateBase64)
    try {
        $token = Get-IngestAccessToken `
            -CertBytes $bytes `
            -ClientId $cfg.ClientId `
            -TenantId $cfg.TenantId `
            -Audience $Audience
    }
    finally {
        [Array]::Clear($bytes, 0, $bytes.Length)
    }

    $script:TokenCache[$Audience] = $token
    return $token
}

function Connect-Service {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Config
    )

    if ([string]::IsNullOrEmpty($Config.CertificateBase64)) {
        throw "Connect-Service requires Config.CertificateBase64."
    }

    # Stash for runspace reconnect / Get-PowerPlatToken before any token call.
    $authConfig = @{
        ClientId          = $Config.ClientId
        TenantId          = $Config.TenantId
        CertificateBase64 = $Config.CertificateBase64
    }
    $script:AuthConfig = $authConfig

    # Acquire the legacy-admin token immediately and prime the cache. The
    # sanity-check call below uses it; entity modules read from the cache
    # via Get-PowerPlatToken.
    $adminToken = Get-PowerPlatToken -Audience $LegacyAdminAudience

    # Post-connect sanity check. Mirrors the graph /organization probe — a
    # successful token acquisition doesn't mean the SP is registered as a
    # PP tenant management app in this tenant. Failure here surfaces a
    # missing New-PowerAppManagementApp registration as a single clean
    # error rather than 401s leaking out of every entity fetch.
    try {
        $probeUri = 'https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments?api-version=2020-10-01&$top=1'
        Invoke-RestMethod -Method GET -Uri $probeUri -Headers @{ Authorization = "Bearer $adminToken" } -ErrorAction Stop | Out-Null
    } catch {
        throw "Power Platform sanity check failed for tenant '$($env:TENANT_KEY)': BAP /scopes/admin/environments threw: $($_.Exception.Message). Verify the SP is registered as a tenant management app via New-PowerAppManagementApp — see jobs/powerplat-ingest/CLAUDE.md."
    }

    return @{
        AuthConfig = $authConfig
    }
}

function Restore-ServiceConnection {
    [CmdletBinding()]
    param()

    # Token-cache invalidation — Restore is called when a fetch hit a 401,
    # so the cached bearer for that fetch's audience is by definition stale.
    # Clear all cached tokens so the next Get-PowerPlatToken acquires fresh.
    # Cheaper than tracking per-audience expiry; misses are bounded by the
    # number of audiences the run touches (1 legacy-admin + 1 Dataverse per
    # env with linkedEnvironmentMetadata).
    $script:TokenCache = @{}
}

Export-ModuleMember -Function Connect-Service, Restore-ServiceConnection, Get-PowerPlatToken
