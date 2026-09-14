# Cross-platform confidential-client bearer-token acquisition for ingest
# containers. Supports two credential types via parameter sets:
#
#   Get-IngestAccessToken -CertBytes …    (Cert set — default)
#   Get-IngestAccessToken -ClientSecret … (Secret set)
#
# The cert path includes a Linux-pwsh-specific RSA rebind; the secret path is
# a direct MSAL .WithClientSecret() call with none of that machinery.
#
# === Why the cert rebind + direct-token flow is necessary ===
#
# Loading a PFX with X509Certificate2::new on Linux/.NET 8 produces a cert
# whose private key is a wrapped reference MSAL cannot use to sign JWT
# assertions. The standard workaround is to rebuild the cert via RSA.Create()
# + RSAOpenSsl + CopyWithPrivateKey, producing a cert whose RSA provider is
# MSAL-compatible. We then call MSAL directly (ConfidentialClientApplication-
# Builder) with the rebuilt cert and pull the access token out — no module
# auth wrappers in between, so there's nothing left to silently lose the cert
# binding mid-flight (the EXO bug we hit in #156).
#
# Originally extracted from src/analytics/ingest/jobs/exo-ingest/scripts/
# Connect.psm1 (Get-ExoAccessToken) and generalized for arbitrary audiences.

function Get-IngestAccessToken {
    [CmdletBinding(DefaultParameterSetName = 'Cert')]
    param(
        [Parameter(Mandatory, ParameterSetName = 'Cert')][byte[]]$CertBytes,
        [Parameter(Mandatory, ParameterSetName = 'Secret')][string]$ClientSecret,
        [Parameter(Mandatory)][string]$ClientId,
        [Parameter(Mandatory)][string]$TenantId,
        # Examples:
        #   https://service.powerapps.com/.default     — BAP / PowerApps admin / Flow admin
        #   https://outlook.office365.com/.default     — Exchange Online
        #   https://{org}.crm.dynamics.com/.default    — per-env Dataverse Web API
        #   https://storage.azure.com/.default         — ADLS / Blob data plane
        [Parameter(Mandatory)][string]$Audience
    )

    # All disposable refs initialized to $null up front so the finally block
    # can null-guard each Dispose. A throw between cert/RSA setup and the
    # try/finally would otherwise leak private key material — see MdeDevices'
    # Get-MdeAccessToken for the established pattern this mirrors.
    $certOrig       = $null
    $certPubOnly    = $null
    $certForSigning = $null
    $rsaOrig        = $null
    $rsaNew         = $null
    $rsaParams      = $null
    $msalApp        = $null
    try {
        if ($PSCmdlet.ParameterSetName -eq 'Cert') {
            # EphemeralKeySet keeps the private key out of any on-disk store —
            # works on Linux and Windows. macOS routes private keys through
            # Keychain and rejects EphemeralKeySet at runtime, so we drop it
            # when running on a Mac (e.g., dev workstation spikes). The
            # container deploy target is Linux, where EphemeralKeySet is
            # preferred for the security posture.
            $certFlags = [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::Exportable
            if (-not $IsMacOS) {
                $certFlags = $certFlags -bor [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::EphemeralKeySet
            }
            $certOrig = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new($CertBytes, [string]$null, $certFlags)
            if (-not $certOrig.HasPrivateKey) { throw "Certificate has no private key." }

            $rsaOrig = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::GetRSAPrivateKey($certOrig)
            if ($null -eq $rsaOrig) { throw "GetRSAPrivateKey returned null." }
            $rsaParams = $rsaOrig.ExportParameters($true)
            $rsaNew = [System.Security.Cryptography.RSA]::Create()
            $rsaNew.ImportParameters($rsaParams)

            $certPubOnly = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new($certOrig.RawData)
            $certForSigning = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::CopyWithPrivateKey($certPubOnly, $rsaNew)
        }

        # Find Microsoft.Identity.Client.dll. Az.Accounts ships it and is a
        # standard dependency in our ingest containers (see graph-ingest/Dockerfile).
        # Fall back to ExchangeOnlineManagement for the EXO container's existing
        # callers, then to any other module that bundles it. Failing all three is
        # a hard error — surface it instead of silent missing-type at MSAL call.
        $msalDll = $null
        foreach ($mod in @('Az.Accounts', 'ExchangeOnlineManagement', 'Microsoft.Graph.Authentication')) {
            $loaded = Get-Module -ListAvailable -Name $mod | Select-Object -First 1
            if (-not $loaded) { continue }
            $candidate = Get-ChildItem -Path $loaded.ModuleBase -Recurse -Filter 'Microsoft.Identity.Client.dll' -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($candidate) { $msalDll = $candidate; break }
        }
        if (-not $msalDll) {
            throw "Microsoft.Identity.Client.dll not found in Az.Accounts / ExchangeOnlineManagement / Microsoft.Graph.Authentication. Install one of these (Dockerfile or local pwsh) before calling Get-IngestAccessToken."
        }
        Add-Type -Path $msalDll.FullName -ErrorAction Stop

        $builder = [Microsoft.Identity.Client.ConfidentialClientApplicationBuilder]::Create($ClientId).
            WithAuthority("https://login.microsoftonline.com/$TenantId")
        if ($PSCmdlet.ParameterSetName -eq 'Cert') {
            $builder = $builder.WithCertificate($certForSigning)
        } else {
            $builder = $builder.WithClientSecret($ClientSecret)
        }
        $msalApp = $builder.Build()

        $msalScopes = [System.Collections.Generic.List[string]]::new()
        $msalScopes.Add($Audience)

        $tokenResult = $msalApp.AcquireTokenForClient($msalScopes).ExecuteAsync().GetAwaiter().GetResult()
        return $tokenResult.AccessToken
    }
    finally {
        # Dispose / zero in reverse order. Null-guarded so a throw partway
        # through setup still runs through the cleanup path safely.
        if ($certForSigning) { $certForSigning.Dispose() }
        if ($certPubOnly)    { $certPubOnly.Dispose() }
        if ($certOrig)       { $certOrig.Dispose() }
        if ($rsaNew)         { $rsaNew.Dispose() }
        if ($rsaOrig)        { $rsaOrig.Dispose() }
        # Zero the RSAParameters byte arrays before dropping references —
        # ExportParameters produced raw private-key material that GC won't
        # wipe for us.
        if ($null -ne $rsaParams) {
            foreach ($field in 'D','DP','DQ','Exponent','InverseQ','Modulus','P','Q') {
                $value = $rsaParams.$field
                if ($null -ne $value) {
                    [System.Array]::Clear($value, 0, $value.Length)
                }
            }
        }
        Remove-Variable -Name certForSigning, certPubOnly, certOrig, rsaNew, rsaOrig, rsaParams, msalApp -ErrorAction SilentlyContinue
    }
}

Export-ModuleMember -Function Get-IngestAccessToken
