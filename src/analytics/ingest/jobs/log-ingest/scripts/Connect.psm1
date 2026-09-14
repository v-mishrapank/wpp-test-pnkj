# log-ingest container's auth module.
#
# Acquires two tokens up front:
#   - Graph audience (https://graph.microsoft.com/.default) — for entra_sign_in_logs
#   - Mgmt API audience (https://manage.office.com/.default) — for the 4 audit_*
#     content types pulled from the Office 365 Management Activity API
#     (audit_entra, audit_exchange, audit_sharepoint, audit_general). PowerBI
#     events route through audit_general; Audit.PowerBI is not a separate
#     subscribable content type per the Mgmt API reference.
#
# Connect-Service is invoked once in the main process and once per worker
# runspace; each runspace gets its own $script:AuthConfig that persists for
# its lifetime. Restore-ServiceConnection re-acquires both tokens on auth-error
# retry. See graph-ingest/Connect.psm1 for the pattern; this one diverges by
# holding two tokens instead of an MgGraph session.

# Promote log-ingest's helper modules to session scope so entity modules can
# call Get-HighWaterMark / Set-HighWaterMark / Invoke-MgmtContentFetch /
# Get-CurrentMgmtToken without needing per-module Import-Module statements.
# `-Global` is required because we're a module ourselves; without it these
# functions would only be visible within Connect.psm1's scope. In container,
# /app is WORKDIR and these paths resolve; in local dev (rare), the same
# relative paths work from src/analytics/ingest/jobs/log-ingest/scripts.
#
# `-ErrorAction Stop` so a missing/broken module file fails the container
# fast at startup, surfacing as a clear "module not found" instead of the
# downstream "command not recognized" we'd otherwise see when an entity
# module calls a function that's silently absent. MsalTokenHelper duplicates
# WorkerPool ISS's import, but failing here is fine — if the path is wrong
# in this file, it's wrong in the container.
Import-Module (Join-Path $PSScriptRoot 'modules' 'HighWaterMark.psm1') -Force -Global -ErrorAction Stop
Import-Module (Join-Path $PSScriptRoot 'MgmtApiCommon.psm1') -Force -Global -ErrorAction Stop
Import-Module (Join-Path $PSScriptRoot 'modules' 'MsalTokenHelper.psm1') -Force -Global -ErrorAction Stop

$script:AuthConfig = $null

function Connect-Service {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Config
    )

    if ([string]::IsNullOrEmpty($Config.CertificateBase64)) {
        throw "Connect-Service requires Config.CertificateBase64."
    }

    $certBytes = [Convert]::FromBase64String($Config.CertificateBase64)
    try {
        $graphToken = Get-IngestAccessToken `
            -CertBytes $certBytes `
            -ClientId $Config.ClientId `
            -TenantId $Config.TenantId `
            -Audience 'https://graph.microsoft.com/.default'

        $mgmtToken = Get-IngestAccessToken `
            -CertBytes $certBytes `
            -ClientId $Config.ClientId `
            -TenantId $Config.TenantId `
            -Audience 'https://manage.office.com/.default'

        # Sanity probe: GET /organization on Graph. A "successful" auth that
        # produces empty results (consent revoked, cert/tenant mismatch MSAL
        # doesn't surface at token-acquisition time) gets caught here, before
        # we waste a run pulling against a broken session.
        $orgResponse = $null
        try {
            $orgResponse = Invoke-RestMethod `
                -Method GET `
                -Uri 'https://graph.microsoft.com/v1.0/organization?$top=1' `
                -Headers @{ Authorization = "Bearer $graphToken" } `
                -ErrorAction Stop
        }
        catch {
            throw "Microsoft Graph sanity check failed for tenant '$($env:TENANT_KEY)': GET /organization threw: $($_.Exception.Message)"
        }
        if (-not $orgResponse -or -not $orgResponse.value -or $orgResponse.value.Count -eq 0) {
            throw "Microsoft Graph sanity check failed for tenant '$($env:TENANT_KEY)': GET /organization returned no records."
        }
    }
    finally {
        [Array]::Clear($certBytes, 0, $certBytes.Length)
    }

    $authConfig = @{
        ClientId          = $Config.ClientId
        TenantId          = $Config.TenantId
        CertificateBase64 = $Config.CertificateBase64
        GraphToken        = $graphToken
        MgmtToken         = $mgmtToken
    }
    $script:AuthConfig = $authConfig

    return @{
        AuthConfig       = $authConfig
        OrganizationName = $orgResponse.value[0].displayName
    }
}

function Restore-ServiceConnection {
    [CmdletBinding()]
    param()

    if (-not $script:AuthConfig) {
        throw "Restore-ServiceConnection called before Connect-Service initialized auth state."
    }
    $cfg = $script:AuthConfig

    $bytes = [Convert]::FromBase64String($cfg.CertificateBase64)
    try {
        $graphToken = Get-IngestAccessToken `
            -CertBytes $bytes `
            -ClientId $cfg.ClientId `
            -TenantId $cfg.TenantId `
            -Audience 'https://graph.microsoft.com/.default'
        $mgmtToken = Get-IngestAccessToken `
            -CertBytes $bytes `
            -ClientId $cfg.ClientId `
            -TenantId $cfg.TenantId `
            -Audience 'https://manage.office.com/.default'
    }
    finally {
        [Array]::Clear($bytes, 0, $bytes.Length)
    }

    $cfg.GraphToken = $graphToken
    $cfg.MgmtToken  = $mgmtToken
}

# Helper: returns the current Graph token. Entity modules call this rather than
# poking at $script:AuthConfig so the auth source can be swapped without
# touching every fetcher.
function Get-CurrentGraphToken { return $script:AuthConfig.GraphToken }
function Get-CurrentMgmtToken  { return $script:AuthConfig.MgmtToken }

Export-ModuleMember -Function Connect-Service, Restore-ServiceConnection,
    Get-CurrentGraphToken, Get-CurrentMgmtToken
