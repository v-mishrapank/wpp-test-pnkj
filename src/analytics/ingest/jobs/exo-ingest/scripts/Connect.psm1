# Exchange Online container's auth module.
#
# Exports:
#   Connect-Service           — initial auth. Called once in the main process
#                               (by Invoke-Ingestion.ps1) and once per worker
#                               runspace (by the dispatch template's
#                               first-dispatch self-auth, see WorkerPool.psm1).
#   Restore-ServiceConnection — mid-run auth recovery. Called inside a worker
#                               runspace when a fetch is classified Auth and
#                               we reconnect before retrying.
#
# Both functions route through the private Get-ExoAccessToken helper so the
# cert rebind + MSAL direct-token flow exists in exactly one place.
#
# === Why the rebind + direct-token flow is necessary ===
#
# Loading a PFX with X509Certificate2::new on Linux/.NET 8 produces a cert
# whose private key is a wrapped reference MSAL cannot use to sign JWT
# assertions — every cert-based Connect-ExchangeOnline returns HTTP 401. The
# standard workaround is to rebuild the cert via RSA.Create() + RSAOpenSsl
# + CopyWithPrivateKey, producing a cert whose RSA provider is MSAL-
# compatible.
#
# That alone is not sufficient against ExchangeOnlineManagement 3.6.x in the
# current base image: EXO internally copies the X509Certificate2 when handing
# it to MSAL for token refresh, and the copy loses the RSAOpenSsl binding —
# Get-EXO* calls then silently 401 (issue #156 bug #6). The fix is to skip
# EXO's internal MSAL path entirely: acquire the token ourselves with the
# rebuilt cert and pass it via -AccessToken. With no cert handed to EXO, it
# cannot re-acquire and simply uses our token as-is.
#
# Originally ported from the legacy src/analytics/containers/analytics-ingest
# tree (Mohsin, commit 5cf28bf).
#
# === Auth state lives in $script: scope ===
#
# Connect-Service stashes $script:AuthConfig (which carries the base64 cert);
# Restore-ServiceConnection reads it. Module-scope is runspace-local — see
# the same explainer in graph-ingest/Connect.psm1.

$script:AuthConfig = $null

# Restricting cmdlet load reduces per-runspace baseline footprint from the
# default ~800 EXO cmdlets to the ~10 we actually call. Combined with
# -SkipLoadingCmdletHelp on Connect-ExchangeOnline below, drops baseline by
# ~100-200 MB per runspace (×10 runspaces = ~1-2 GiB headroom). One prong of
# the #484 mitigation; does NOT fix the per-call leak, only the baseline.
#
# If a new entity module starts calling an EXO cmdlet that isn't in this
# list, runs fail with CommandNotFoundException on first invocation — add
# the new cmdlet here. The list is intentionally explicit (not derived from
# a Get-Command scan) so the consequence of adding a new cmdlet is visible
# to whoever's adding the entity.
$script:RequiredCmdlets = @(
    'Get-EXOMailbox',
    'Get-EXOMailboxStatistics',
    'Get-EXORecipient',
    'Get-MailboxPermission',
    'Get-RecipientPermission',
    'Get-DistributionGroup',
    'Get-DistributionGroupMember',
    'Get-UnifiedGroup',
    'Get-UnifiedGroupLinks',
    'Get-OrganizationConfig'
)

function Get-ExoAccessToken {
    # Private — not exported. Rebuilds the cert, loads MSAL from the EXO
    # module's own bundled DLL (keeps versions in sync), acquires a token for
    # the outlook.office365.com audience, and returns the bearer string. The
    # caller feeds that token into Connect-ExchangeOnline -AccessToken.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][byte[]]$CertBytes,
        [Parameter(Mandatory)][string]$ClientId,
        [Parameter(Mandatory)][string]$TenantId
    )

    $certFlags = [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::EphemeralKeySet -bor
                 [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::Exportable
    $certOrig = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new($CertBytes, [string]$null, $certFlags)
    if (-not $certOrig.HasPrivateKey) { throw "Certificate has no private key." }

    $rsaOrig = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::GetRSAPrivateKey($certOrig)
    if ($null -eq $rsaOrig) { throw "GetRSAPrivateKey returned null." }
    $rsaParams = $rsaOrig.ExportParameters($true)
    $rsaNew = [System.Security.Cryptography.RSA]::Create()
    $rsaNew.ImportParameters($rsaParams)

    $certPubOnly = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new($certOrig.RawData)
    $certOrig.Dispose()
    $exoCert = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::CopyWithPrivateKey($certPubOnly, $rsaNew)
    $certPubOnly.Dispose()

    # Load the MSAL.NET assembly that ships with ExchangeOnlineManagement
    # (avoids a separate Microsoft.Identity.Client dependency — versions must
    # agree).
    $exoModule = Get-Module -ListAvailable ExchangeOnlineManagement | Select-Object -First 1
    if (-not $exoModule) { throw "ExchangeOnlineManagement module not installed — check the Dockerfile's Install-Module step." }
    $msalDll = Get-ChildItem -Path $exoModule.ModuleBase -Recurse -Filter 'Microsoft.Identity.Client.dll' -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $msalDll) { throw "MSAL.NET DLL not found in EXO module directory: $($exoModule.ModuleBase)" }
    Add-Type -Path $msalDll.FullName -ErrorAction Stop

    $msalApp = [Microsoft.Identity.Client.ConfidentialClientApplicationBuilder]::Create($ClientId).
        WithAuthority("https://login.microsoftonline.com/$TenantId").
        WithCertificate($exoCert).
        Build()
    $msalScopes = [System.Collections.Generic.List[string]]::new()
    $msalScopes.Add('https://outlook.office365.com/.default')
    $tokenResult = $msalApp.AcquireTokenForClient($msalScopes).ExecuteAsync().GetAwaiter().GetResult()
    $accessToken = $tokenResult.AccessToken

    # Cert + RSA aren't needed after token acquisition; drop references so the
    # private key material doesn't linger for the run's duration.
    $exoCert.Dispose()
    $rsaNew.Dispose()
    Remove-Variable -Name exoCert, rsaNew, rsaOrig, rsaParams, msalApp -ErrorAction SilentlyContinue

    return $accessToken
}

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
        $accessToken = Get-ExoAccessToken -CertBytes $certBytes -ClientId $Config.ClientId -TenantId $Config.TenantId
    }
    finally {
        [Array]::Clear($certBytes, 0, $certBytes.Length)
    }

    $exoParams = @{
        AccessToken           = $accessToken
        Organization          = $Config.Organization
        ShowBanner            = $false
        ErrorAction           = 'Stop'
        SkipLoadingCmdletHelp = $true
        CommandName           = $script:RequiredCmdlets
    }
    Connect-ExchangeOnline @exoParams

    # Post-connect sanity check. A successful Connect-ExchangeOnline doesn't
    # guarantee the session is usable — a rebind regression or module-version
    # mismatch can leave us "connected" but silently returning empty result
    # sets from every Get-EXO* call (issue #156 bug #6). Make a trivial live
    # call — Get-OrganizationConfig returns exactly one record for any
    # connected tenant — so a silent-auth failure surfaces here as zero
    # records or a thrown error, rather than leaking downstream into every
    # entity fetch.
    # (Don't gate on Get-ConnectionInformation's State field — it reports
    # "Broken" for healthy REST-only sessions in some module versions because
    # there's no underlying RPS pipeline to be "Opened".)
    try {
        $orgConfig = @(Get-OrganizationConfig -ErrorAction Stop)
    } catch {
        throw "Exchange Online sanity check failed for tenant '$($env:TENANT_KEY)': Get-OrganizationConfig threw: $($_.Exception.Message)"
    }
    if ($orgConfig.Count -eq 0) {
        throw "Exchange Online sanity check failed for tenant '$($env:TENANT_KEY)': Get-OrganizationConfig returned no records — session is not usable despite Connect-ExchangeOnline succeeding."
    }

    # AuthConfig keeps the base64 so Restore-ServiceConnection can rebuild
    # the MSAL token from it without re-fetching from Key Vault.
    $authConfig = @{
        TenantId          = $Config.TenantId
        ClientId          = $Config.ClientId
        Organization      = $Config.Organization
        CertificateBase64 = $Config.CertificateBase64
    }
    $script:AuthConfig = $authConfig

    return @{
        AuthConfig       = $authConfig
        OrganizationName = $orgConfig[0].Name
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
        $accessToken = Get-ExoAccessToken -CertBytes $bytes -ClientId $cfg.ClientId -TenantId $cfg.TenantId
    }
    finally {
        [Array]::Clear($bytes, 0, $bytes.Length)
    }

    $exoParams = @{
        AccessToken           = $accessToken
        Organization          = $cfg.Organization
        ShowBanner            = $false
        ErrorAction           = 'Stop'
        SkipLoadingCmdletHelp = $true
        CommandName           = $script:RequiredCmdlets
    }
    Connect-ExchangeOnline @exoParams
}

Export-ModuleMember -Function Connect-Service, Restore-ServiceConnection
