# MDE (Microsoft Defender for Endpoint) uses a different auth path than Graph —
# MSAL-direct client_credentials against the securitycenter audience. Auth
# goes through the shared Get-IngestAccessToken (shared/modules/MsalTokenHelper.psm1)
# which handles the cert-rebind dance MSAL needs on Linux/.NET 8 (PFX-loaded
# private keys are wrapped references MSAL can't sign with; rebuilding via
# RSA.Create() + CopyWithPrivateKey produces an MSAL-compatible cert).
#
# Runs inline because MDE isn't part of the runspace pool's pre-auth flow.
# AuthConfig and CertBytes are passed through $Context.

function Get-ModuleStages {
    @{
        'mde_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-MdeDevicesRoot'
            ApiFamily  = 'graph'
        }
    }
}

function Get-ModuleEntities {
    @{
        'mde_devices' = @{
            Stage    = 'mde_root'
            WritesTo = 'root'
        }
    }
}

function Get-MdeDevicesRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $clientId  = $Context.AuthConfig.ClientId
    $tenantId  = $Context.AuthConfig.TenantId
    # Post-#145: $Context.CertBytes is gone; cert flows as
    # $Context.AuthConfig.CertificateBase64. Decode once for this fetch,
    # zero the byte[] on exit so private key material doesn't outlive the
    # call (the base64 string in AuthConfig is what survives).
    $certBytes = [Convert]::FromBase64String($Context.AuthConfig.CertificateBase64)
    $audience  = 'https://api.securitycenter.microsoft.com/.default'

    try {
        $token = Get-IngestAccessToken -CertBytes $certBytes -ClientId $clientId -TenantId $tenantId -Audience $audience
        $headers = @{ Authorization = "Bearer $token" }

        # Reconnect callback for Invoke-WithRetry — re-acquires token on 401.
        # GetNewClosure binds $certBytes/$clientId/$tenantId/$audience/$headers by
        # value so the scriptblock is self-contained when Invoke-WithRetry invokes it.
        $reconnect = {
            $newToken = Get-IngestAccessToken -CertBytes $certBytes -ClientId $clientId -TenantId $tenantId -Audience $audience
            $headers['Authorization'] = "Bearer $newToken"
        }.GetNewClosure()

        $uri = 'https://api.security.microsoft.com/api/machines?$top=10000'
        do {
            $response = Invoke-WithRetry -OnAuthReconnect $reconnect -ScriptBlock {
                Invoke-RestMethod -Method GET -Uri $uri -Headers $headers -ErrorAction Stop
            }
            foreach ($device in $response.value) {
                $Writer.WriteRecord($device)
            }
            $uri = $response.'@odata.nextLink'
        } while ($uri)
    }
    finally {
        [Array]::Clear($certBytes, 0, $certBytes.Length)
    }
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-MdeDevicesRoot
