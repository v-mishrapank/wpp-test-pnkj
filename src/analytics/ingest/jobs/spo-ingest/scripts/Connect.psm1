# SharePoint Online container's auth module — REST-only.
#
# Drops PnP.PowerShell entirely (PnP CSOM was silently swallowing 429s and
# sitting on the HttpClient until the 100s timeout fired, see #460/#461).
# Four audiences are minted via the shared MsalTokenHelper and cached per
# runspace in $script:AuthConfig.TokenCache keyed by host URL:
#
#   https://{tenant}.sharepoint.com/.default        — per-site main
#   https://{tenant}-my.sharepoint.com/.default     — per-site OneDrive
#   https://{tenant}-admin.sharepoint.com/.default  — tenant admin REST
#   https://graph.microsoft.com/.default            — Graph (#471 — site
#                                                     enumeration via
#                                                     /v1.0/sites/getAllSites)
#
# Exports:
#   Connect-Service           — initial auth. Called once in the main process
#                               (by Invoke-Ingestion.ps1) and once per worker
#                               runspace (by the dispatch template's first-
#                               dispatch self-auth, see WorkerPool.psm1).
#   Restore-ServiceConnection — mid-run auth recovery. Wipes the token cache
#                               so the next Get-SpoToken re-mints. Called
#                               from inside the WorkerPool retry loop when an
#                               item is classified Auth.
#   Get-SpoToken              — resolve a bearer for a given SiteUrl by host,
#                               minting/refreshing as needed. Called from
#                               entity Get-* functions before every REST call.
#
# This module also imports SpoRestClient.psm1 with -Global so the REST helpers
# (Invoke-SpoRest, Invoke-SpoRestPaged, Invoke-SpoBatch) land in the runspace's
# top-level command namespace alongside the auth helpers. The shared framework
# only wires one AuthModulePath per container; piggybacking the REST client on
# top of Connect.psm1 is the cleanest way to make both halves available in
# every worker runspace without extending the framework's pool-ISS surface.

Import-Module (Join-Path $PSScriptRoot 'SpoRestClient.psm1') -Force -Global -DisableNameChecking

# Per-runspace auth state. Set by Connect-Service. Read by Restore-Service-
# Connection and Get-SpoToken. Module scope is runspace-local — Connect.psm1
# is imported into each worker runspace's ISS, and each runspace gets its own
# $script:AuthConfig that persists for the runspace's lifetime.
$script:AuthConfig = $null

# Refresh a cached token if it expires within this window. MSAL tokens are
# ~1h-lived; 5 min of slack covers a long Invoke-SpoBatch round-trip plus the
# next retry's wall time.
$script:TokenRefreshSkewSeconds = 300

# User-Agent for the startup sanity probes below. SharePoint Online throttles
# "undecorated" traffic (no AppID + User-Agent) more aggressively, so every
# call — including these one-shot probes — carries the enterprise-format UA
# `NONISV|CompanyName|AppName/Version`. Sourced from SpoRestClient.psm1's
# Get-SpoUserAgent (imported -Global above) so the value lives in one place;
# cached here once at import since script-scope vars are module-private.
# See https://learn.microsoft.com/sharepoint/dev/general-development/how-to-avoid-getting-throttled-or-blocked-in-sharepoint-online
$script:SpoUserAgent = Get-SpoUserAgent

function Connect-Service {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Config
    )

    foreach ($k in 'ClientId','TenantId','CertificateBase64','AdminUrl') {
        if ([string]::IsNullOrEmpty($Config.$k)) {
            throw "Connect-Service requires Config.$k."
        }
    }

    # Parse the tenant prefix (e.g. 'xtlab2') from AdminUrl. Per-site and
    # OneDrive audiences are derived from this prefix; the admin audience is
    # the AdminUrl itself.
    $adminHost = ([System.Uri]$Config.AdminUrl).Host  # xtlab2-admin.sharepoint.com
    if ($adminHost -notmatch '^([a-z0-9\-]+)-admin\.sharepoint\.com$') {
        throw "Connect-Service: AdminUrl host '$adminHost' does not match the expected '<tenant>-admin.sharepoint.com' pattern."
    }
    $tenantPrefix = $Matches[1]

    $spoHosts = @{
        Main  = "$tenantPrefix.sharepoint.com"
        My    = "$tenantPrefix-my.sharepoint.com"
        Admin = "$tenantPrefix-admin.sharepoint.com"
        # Graph host is fixed (tenant-independent); Get-SpoToken's host-dispatch
        # routes by URL host, so Get-SpoToken -SiteUrl 'https://graph.microsoft.com'
        # mints the Graph bearer.
        Graph = 'graph.microsoft.com'
    }
    $audiences = @{
        $spoHosts.Main  = "https://$($spoHosts.Main)/.default"
        $spoHosts.My    = "https://$($spoHosts.My)/.default"
        $spoHosts.Admin = "https://$($spoHosts.Admin)/.default"
        $spoHosts.Graph = 'https://graph.microsoft.com/.default'
    }

    $authConfig = @{
        ClientId          = $Config.ClientId
        TenantId          = $Config.TenantId
        CertificateBase64 = $Config.CertificateBase64
        # Organization / TenantDomain stays for parity with other containers'
        # AuthConfig shape — graph-ingest exposes TenantId, exo-ingest exposes
        # Organization. Some shared modules read TenantDomain when present.
        Organization      = $Config.Organization
        TenantDomain      = $Config.Organization
        AdminUrl          = $Config.AdminUrl
        TenantPrefix      = $tenantPrefix
        Hosts             = $spoHosts
        Audiences         = $audiences
        # Token cache shape: host -> @{ Token = '...'; ExpiresAt = [DateTime] }.
        # Get-SpoToken refreshes when ExpiresAt is within TokenRefreshSkewSeconds.
        # Empty at module load; populated lazily on first Get-SpoToken call.
        TokenCache        = @{}
    }
    $script:AuthConfig = $authConfig

    # Sanity check — mint the admin token and hit a trivial admin endpoint
    # before we declare auth healthy. /_api/contextinfo is a cheap POST that
    # exercises the cert-auth path without depending on Sites.* permissions.
    # If this fails, every per-site fetch would fail too — surface here.
    # Pre-flight the admin mint so a cert/consent failure surfaces with a
    # clean message. The bearer itself is re-minted inside the probe
    # scriptblock (Get-SpoToken caches, so this doesn't double-mint).
    try {
        [void](Get-SpoToken -SiteUrl $Config.AdminUrl)
    } catch {
        throw "SharePoint Online sanity check failed for tenant '$($env:TENANT_KEY)': admin token mint threw: $($_.Exception.Message)"
    }
    # OnAuthReconnect closure shared by both probes: on an Auth-classified
    # failure (401 / invalid or stale cached token) it wipes the token cache
    # so the next Get-SpoToken re-mints, mirroring the mid-fetch recovery in
    # SpoSites. Without this a 401 would just be retried with the same cached
    # bearer until MaxRetries. The token must also be re-minted *inside* the
    # scriptblock (below) so each attempt reads the refreshed cache.
    $reconnect = { Restore-ServiceConnection }.GetNewClosure()
    # Route the probe through Invoke-WithRetry so a transient 429/503 backs
    # off and retries instead of failing the whole job/worker on first hit
    try {
        $sanityResp = Invoke-WithRetry -ApiFamily 'spo' -OnAuthReconnect $reconnect -ScriptBlock {
            $bearer = Get-SpoToken -SiteUrl $Config.AdminUrl
            Invoke-RestMethod `
                -Uri "$($Config.AdminUrl)/_api/contextinfo" `
                -Method POST `
                -Headers @{
                    Authorization = "Bearer $bearer"
                    Accept        = 'application/json;odata=verbose'
                    'User-Agent'  = $script:SpoUserAgent
                } `
                -TimeoutSec 30 -ErrorAction Stop
        }
    } catch {
        throw "SharePoint Online sanity check failed for tenant '$($env:TENANT_KEY)': admin /_api/contextinfo threw: $($_.Exception.Message)"
    }
    if (-not $sanityResp -or -not $sanityResp.d -or -not $sanityResp.d.GetContextWebInformation) {
        throw "SharePoint Online sanity check failed for tenant '$($env:TENANT_KEY)': /_api/contextinfo returned an unexpected shape — session is not usable."
    }

    # Graph sanity check — site enumeration moved from admin REST to Graph
    # /v1.0/sites/getAllSites in #471. Probe /v1.0/sites?$top=1 rather than
    # /organization: same Sites.Read.All scope as the actual enumeration, so
    # a permissions / consent regression on the Sites scope surfaces here
    # instead of leaking into spo_sites_root as a 403 mid-fetch. Empty
    # value[] is a legitimate response for /sites?$top=1 (tenants with no
    # root sites — rare but possible), so we only treat a hard error as
    # session-unusable, not an empty page.
    try {
        [void](Get-SpoToken -SiteUrl 'https://graph.microsoft.com')
    } catch {
        throw "Microsoft Graph sanity check failed for tenant '$($env:TENANT_KEY)': Graph token mint threw: $($_.Exception.Message)"
    }
    # Same transient-throttle guard as the admin probe above — Graph
    # /sites can 429 under concurrent worker start-up. Reuses the same
    # -OnAuthReconnect closure + fresh per-attempt mint so a 401 refreshes
    # $graphToken instead of retrying the stale cached bearer.
    try {
        $graphResp = Invoke-WithRetry -ApiFamily 'graph' -OnAuthReconnect $reconnect -ScriptBlock {
            $bearer = Get-SpoToken -SiteUrl 'https://graph.microsoft.com'
            Invoke-RestMethod `
                -Uri 'https://graph.microsoft.com/v1.0/sites?$top=1&$select=id' `
                -Method GET `
                -Headers @{ Authorization = "Bearer $bearer"; Accept = 'application/json'; 'User-Agent' = $script:SpoUserAgent } `
                -TimeoutSec 30 -ErrorAction Stop
        }
    } catch {
        throw "Microsoft Graph sanity check failed for tenant '$($env:TENANT_KEY)': GET /sites threw: $($_.Exception.Message)"
    }
    if (-not $graphResp -or -not $graphResp.PSObject.Properties['value']) {
        throw "Microsoft Graph sanity check failed for tenant '$($env:TENANT_KEY)': GET /sites returned an unexpected shape — Graph session is not usable."
    }

    return @{
        AuthConfig       = $authConfig
        # OrganizationName left empty; SPO doesn't carry a tenant display name
        # in the contextinfo response. Other containers return one for the
        # "Connected to upstream service for tenant 'X' (org Y)" log line.
        OrganizationName = ''
    }
}

function Restore-ServiceConnection {
    [CmdletBinding()]
    param()

    if (-not $script:AuthConfig) {
        throw "Restore-ServiceConnection called before Connect-Service initialized auth state."
    }
    # Wipe the token cache. The next Get-SpoToken call re-mints from the
    # cached CertificateBase64. This is the universal recovery for a 401 —
    # whether the token expired between retries, the runspace's cached MSAL
    # state got into a bad state, or the cert was rotated in Key Vault and
    # MSAL closed its handles, a fresh mint always restores the right shape.
    $script:AuthConfig.TokenCache.Clear()
}

function Get-SpoToken {
    <#
    .SYNOPSIS
        Resolve a bearer token for a given SiteUrl. Parses the host, looks up
        the cached token for that host, refreshes if expiring within
        TokenRefreshSkewSeconds, and returns the bearer string.

        Three hosts are recognized per tenant: main, -my (OneDrive), -admin.
        Any other host throws — defensive against typos in entity fetchers.
    .PARAMETER SiteUrl
        Absolute https URL. The host is extracted; path is ignored.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$SiteUrl
    )
    if (-not $script:AuthConfig) {
        throw "Get-SpoToken called before Connect-Service initialized auth state."
    }
    $tokenHost = ([System.Uri]$SiteUrl).Host
    if (-not $script:AuthConfig.Audiences.ContainsKey($tokenHost)) {
        $known = ($script:AuthConfig.Audiences.Keys | Sort-Object) -join ', '
        throw "Get-SpoToken: host '$tokenHost' is not one of the configured SPO audiences. Known: $known"
    }

    # Cache hit + not-near-expiry — return immediately. This is the dominant
    # path for steady-state per-site fetches.
    $cache = $script:AuthConfig.TokenCache
    if ($cache.ContainsKey($tokenHost)) {
        $entry = $cache[$tokenHost]
        $skew = [TimeSpan]::FromSeconds($script:TokenRefreshSkewSeconds)
        if ($entry.ExpiresAt -and ([DateTime]::UtcNow + $skew) -lt $entry.ExpiresAt) {
            return $entry.Token
        }
    }

    # Mint a fresh token. Decode the cert bytes locally so the key material
    # only lives in this scope. The base64 string in $script:AuthConfig is
    # the canonical store across the runspace's lifetime.
    $certBytes = [Convert]::FromBase64String($script:AuthConfig.CertificateBase64)
    try {
        $audience = $script:AuthConfig.Audiences[$tokenHost]
        $token = Get-IngestAccessToken `
            -CertBytes $certBytes `
            -ClientId  $script:AuthConfig.ClientId `
            -TenantId  $script:AuthConfig.TenantId `
            -Audience  $audience
        $expiresAt = Get-JwtExpiry -Token $token
        $cache[$tokenHost] = @{
            Token     = $token
            ExpiresAt = $expiresAt
        }
        return $token
    }
    finally {
        [Array]::Clear($certBytes, 0, $certBytes.Length)
    }
}

function Get-JwtExpiry {
    <#
    Extract the `exp` claim from a JWT and return it as UTC DateTime.
    Returns [DateTime]::MaxValue if the token isn't a well-formed JWT — we'd
    rather cache an opaque token to expiry than throw, since the token still
    works until SP rejects it; on rejection the 401 path re-mints.
    #>
    param([Parameter(Mandatory)][string]$Token)
    $parts = $Token -split '\.'
    if ($parts.Count -lt 2) { return [DateTime]::MaxValue }
    try {
        $payload = $parts[1]
        # JWT uses URL-safe base64 without padding. Pad to a multiple of 4
        # before Convert::FromBase64String, and translate URL-safe chars.
        $payload = $payload.Replace('-', '+').Replace('_', '/')
        switch ($payload.Length % 4) {
            2 { $payload += '==' }
            3 { $payload += '=' }
        }
        $json = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($payload))
        $claims = $json | ConvertFrom-Json -ErrorAction Stop
        if ($null -eq $claims.exp) { return [DateTime]::MaxValue }
        # `exp` is Unix seconds.
        return [DateTimeOffset]::FromUnixTimeSeconds([long]$claims.exp).UtcDateTime
    } catch {
        Write-Verbose "Get-JwtExpiry: failed to parse exp claim — $($_.Exception.Message)"
        return [DateTime]::MaxValue
    }
}

Export-ModuleMember -Function Connect-Service, Restore-ServiceConnection, Get-SpoToken, Get-JwtExpiry
