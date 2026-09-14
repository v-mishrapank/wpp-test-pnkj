# Power BI / Fabric container's auth + shared-helper module.
#
# Exports:
#   Connect-Service           — initial auth. Called once in the main process
#                               (by Invoke-Ingestion.ps1) and once per worker
#                               runspace (by the dispatch template's
#                               first-dispatch self-auth, see WorkerPool.psm1).
#   Restore-ServiceConnection — mid-run auth recovery. Called inside a worker
#                               runspace when a fetch is classified Auth and
#                               we reconnect before retrying.
#   Get-PowerBiToken          — public token-cache accessor used by entity
#                               modules. Returns a cached token or acquires
#                               a fresh one.
#   Invoke-PowerBiRequest     — generic REST helper with retry + reconnect.
#                               Audience-parameterized (Power BI default,
#                               Fabric optional). Lives here (not in an entity
#                               module) because cross-entity-module exports
#                               aren't visible from a fetcher's runtime
#                               session — Connect.psm1 IS, since the framework
#                               imports it into every fetcher's scope chain.
#                               C3 hit this when PowerBiGateways tried to call
#                               Invoke-PowerBiRequest from PowerBiTenant.
#   Invoke-PowerBiPagedFetch  — OData @odata.nextLink loop for V1 admin endpoints.
#   Invoke-FabricPagedFetch   — continuationUri loop for Fabric admin/items.
#   ConvertTo-PowerBiArray    — defensive normalization for V2 endpoints whose
#                               response shape is inconsistent (some wrap in
#                               .value, some return a top-level array).
#
# === Why this differs from graph/exo Connect.psm1 ===
#
# Power BI has no PowerShell module compatible with pwsh 7 / Linux —
# MicrosoftPowerBIMgmt is .NET-Framework-only. Auth is therefore raw MSAL via
# the shared MsalTokenHelper, and data access is raw Invoke-RestMethod with a
# Bearer header. There's no Connect-PowerBIServiceAccount session object —
# instead we maintain a per-audience token cache keyed by audience string
# ($script:TokenCache).
#
# Single audience: https://analysis.windows.net/powerbi/api/.default. Covers
# V1 (/v1.0/myorg/...), V2 (/v2.0/myorg/...), scan endpoints, and gateway
# endpoints — all on api.powerbi.com. Note: the audience grants the *token*;
# tenant-side authorization differs per endpoint family. /v1.0/myorg/admin/*
# checks the Fabric admin SP toggle; /v2.0/myorg/gatewayClusters/* checks
# per-cluster gateway-admin role (granted via the
# repo-root scripts/Add-IngestGatewayAdmin.ps1 bootstrap, see CLAUDE.md step 3).
#
# The cache is a hashtable for symmetry with powerplat (per-env Dataverse
# audiences), even though Power BI uses one. Adding a second audience later
# (e.g., api.fabric.microsoft.com if a Fabric-specific endpoint surfaces)
# is a no-op at this layer.
#
# === Auth state lives in $script: scope ===
#
# Connect-Service stashes $script:AuthConfig (which carries the base64 cert
# for token re-acquisition); Get-PowerBiToken reads it. Module-scope is
# runspace-local — see graph-ingest/Connect.psm1 for the explainer.

# Get-IngestAccessToken comes from shared/modules/MsalTokenHelper.psm1, which
# Invoke-Ingestion.ps1 imports globally before any container module loads.

$script:AuthConfig = $null

# Keyed by audience string. Populated lazily by Get-PowerBiToken.
# $script: scope: visible to all functions in this module within the same
# session; not visible across runspaces (workers re-import this module and
# get their own $script:TokenCache).
$script:TokenCache = @{}

$PowerBiAudience = 'https://analysis.windows.net/powerbi/api/.default'
$FabricAudience  = 'https://api.fabric.microsoft.com/.default'

function Get-PowerBiToken {
    [CmdletBinding()]
    param(
        [string]$Audience = $PowerBiAudience
    )

    if ($script:TokenCache.ContainsKey($Audience)) {
        return $script:TokenCache[$Audience]
    }

    if (-not $script:AuthConfig) {
        throw "Get-PowerBiToken called before Connect-Service initialized auth state."
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

    # Stash for runspace reconnect / Get-PowerBiToken before any token call.
    $authConfig = @{
        ClientId          = $Config.ClientId
        TenantId          = $Config.TenantId
        CertificateBase64 = $Config.CertificateBase64
    }
    $script:AuthConfig = $authConfig

    # Acquire the token immediately and prime the cache. The sanity-check call
    # below uses it; entity modules read from the cache via Get-PowerBiToken.
    $token = Get-PowerBiToken

    # Post-connect sanity check. Mirrors graph's /organization probe and
    # powerplat's BAP /environments probe — a successful token acquisition
    # doesn't mean the SP has Fabric admin role enabled in this tenant.
    # Failure here surfaces a missing Fabric admin SP toggle as a single
    # clean error rather than 401s leaking out of every entity fetch.
    #
    # Power BI returns rich error context in two non-default places: the
    # response body (text/json, captured by Invoke-RestMethod into
    # $_.ErrorDetails.Message) and the x-powerbi-error-info header (a
    # symbolic code like "UserNotLicensed", "PowerBIServiceNotActivated").
    # Surfacing both gives a downstream operator enough to act without
    # round-tripping through the container logs.
    try {
        # No $top — /admin/capacities rejects it with `InvalidRequest "Query option 'Top' is not allowed"`.
        # Tenants typically have <100 capacities (often 0); the unfiltered list is cheap.
        $probeUri = 'https://api.powerbi.com/v1.0/myorg/admin/capacities'
        Invoke-RestMethod -Method GET -Uri $probeUri -Headers @{ Authorization = "Bearer $token" } -ErrorAction Stop | Out-Null
    } catch {
        $statusCode = $null
        $errorInfo  = $null
        $bodyMsg    = $null
        try { $statusCode = [int]$_.Exception.Response.StatusCode } catch { Write-Verbose $_.Exception.Message }
        try { $errorInfo  = $_.Exception.Response.Headers['x-powerbi-error-info'] } catch { Write-Verbose $_.Exception.Message }
        if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
            $bodyMsg = $_.ErrorDetails.Message
            if ($bodyMsg.Length -gt 500) { $bodyMsg = $bodyMsg.Substring(0, 500) + '…' }
        }
        $detail = @(
            "exception=$($_.Exception.Message)"
            if ($statusCode) { "status=$statusCode" }
            if ($errorInfo)  { "x-powerbi-error-info=$errorInfo" }
            if ($bodyMsg)    { "body=$bodyMsg" }
        ) -join ' | '
        throw "Power BI sanity check failed for tenant '$($env:TENANT_KEY)': /v1.0/myorg/admin/capacities — $detail. Verify the SP has Fabric admin access — see jobs/powerbi-ingest/CLAUDE.md (per-target-tenant onboarding step 2)."
    }

    return @{
        AuthConfig = $authConfig
    }
}

function Restore-ServiceConnection {
    [CmdletBinding()]
    param()

    # Token-cache invalidation — Restore is called when a fetch hit a 401, so
    # the cached bearer is by definition stale. Clear all cached tokens so the
    # next Get-PowerBiToken acquires fresh.
    $script:TokenCache = @{}
}

# === Shared REST helpers ===

function Invoke-PowerBiRequest {
    # Generic REST call against any Power BI / Fabric admin endpoint.
    # Audience defaults to the Power BI admin one; pass $FabricAudience for
    # api.fabric.microsoft.com endpoints. Each call gets its own $headers
    # hashtable so the reconnect closure's Authorization mutation is naturally
    # scoped — no cross-call stale-header risk (mirrors powerplat's helper
    # pattern at PowerPlatEnvironments.psm1:496).
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Uri,
        [string]$Method = 'GET',
        $Body = $null,
        [string]$Audience = $null
    )

    $token = if ($Audience) { Get-PowerBiToken -Audience $Audience } else { Get-PowerBiToken }
    $headers = @{ Authorization = "Bearer $token" }

    $reconnect = {
        Restore-ServiceConnection
        $newToken = if ($Audience) { Get-PowerBiToken -Audience $Audience } else { Get-PowerBiToken }
        $headers['Authorization'] = "Bearer $newToken"
    }.GetNewClosure()

    Invoke-WithRetry -ApiFamily 'powerbi' -OnAuthReconnect $reconnect -ScriptBlock {
        if ($Body) {
            try {
                $headers['Content-Type'] = 'application/json'
                Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers -Body $Body -ErrorAction Stop
            } finally {
                $headers.Remove('Content-Type')
            }
        } else {
            Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers -ErrorAction Stop
        }
    }
}

function Invoke-PowerBiPagedFetch {
    # OData-pagination loop for Power BI V1 admin endpoints.
    # Walks @odata.nextLink chain, projects .value records via $Writer.WriteRecord.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)]$Writer
    )
    do {
        $response = Invoke-PowerBiRequest -Uri $Uri
        foreach ($item in $response.value) {
            $Writer.WriteRecord($item)
        }
        $Uri = $response.'@odata.nextLink'
    } while ($Uri)
}

function Invoke-FabricPagedFetch {
    # Continuation-token-pagination loop for Fabric admin/items.
    # Walks continuationUri chain (full URL provided in response), projects
    # .itemEntities records via $Writer.WriteRecord. continuationUri is null
    # on the final page.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)]$Writer
    )
    do {
        $response = Invoke-PowerBiRequest -Uri $Uri -Audience $FabricAudience
        foreach ($item in $response.itemEntities) {
            $Writer.WriteRecord($item)
        }
        $Uri = $response.continuationUri
    } while ($Uri)
}

function ConvertTo-PowerBiArray {
    # Power BI V2 endpoints are inconsistent: some wrap results in `{ value: [...] }`
    # (OData convention), others return a top-level JSON array. Normalize either
    # shape to a plain array. Single object responses get coerced to a 1-element
    # array.
    [CmdletBinding()]
    param([Parameter(Mandatory)]$Response)

    if ($null -eq $Response)         { return @() }
    if ($Response.value -is [array]) { return $Response.value }
    if ($Response -is [array])       { return $Response }
    return @($Response)
}

Export-ModuleMember -Function `
    Connect-Service, Restore-ServiceConnection, Get-PowerBiToken, `
    Invoke-PowerBiRequest, Invoke-PowerBiPagedFetch, Invoke-FabricPagedFetch, `
    ConvertTo-PowerBiArray
