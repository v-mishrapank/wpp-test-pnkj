function Get-CertificateFromKeyVault {
    param(
        [Parameter(Mandatory)][string]$VaultName,
        [Parameter(Mandatory)][string]$CertName
    )

    $cert = Get-AzKeyVaultCertificate -VaultName $VaultName -Name $CertName
    $secret = Get-AzKeyVaultSecret -VaultName $VaultName -Name $cert.Name -AsPlainText
    $certBytes = [System.Convert]::FromBase64String($secret)

    $pfxPath = Join-Path ([System.IO.Path]::GetTempPath()) "$CertName.pfx"
    [System.IO.File]::WriteAllBytes($pfxPath, $certBytes)

    return $pfxPath
}

function Get-CertificateBytes {
    param(
        [Parameter(Mandatory)][string]$VaultName,
        [Parameter(Mandatory)][string]$CertName
    )

    $cert = Get-AzKeyVaultCertificate -VaultName $VaultName -Name $CertName
    $secret = Get-AzKeyVaultSecret -VaultName $VaultName -Name $cert.Name -AsPlainText
    return [System.Convert]::FromBase64String($secret)
}

function Remove-CertificateFile {
    param(
        [Parameter(Mandatory)][string]$Path
    )

    if (Test-Path $Path) {
        $bytes = [byte[]]::new((Get-Item $Path).Length)
        [System.IO.File]::WriteAllBytes($Path, $bytes)
        Remove-Item $Path -Force
    }
}

function Get-SecretValue {
    # Returns the plaintext value of a KV secret. Used by the SP-secret
    # storage auth path (StorageHelperRest.Get-AdlsAccessToken). The container
    # MI's `Key Vault Secrets User` role on the env KV is sufficient — the
    # cert-mode `Key Vault Certificate User` role is not required for this path.
    param(
        [Parameter(Mandatory)][string]$VaultName,
        [Parameter(Mandatory)][string]$SecretName
    )
    return Get-AzKeyVaultSecret -VaultName $VaultName -Name $SecretName -AsPlainText
}

Export-ModuleMember -Function Get-CertificateFromKeyVault, Get-CertificateBytes, Remove-CertificateFile, Get-SecretValue
