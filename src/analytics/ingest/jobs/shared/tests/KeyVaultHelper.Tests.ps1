#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Unit tests for KeyVaultHelper.psm1. The module wraps Az.KeyVault cmdlets
# (Get-AzKeyVaultCertificate / Get-AzKeyVaultSecret) and writes the decoded
# PFX bytes to a temp file. Tests stub the Az cmdlets so no network or auth
# is required.

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    Import-Module (Join-Path $modulesPath 'KeyVaultHelper.psm1') -Force

    # 1KB of bytes to stand in for a PFX blob. The actual content doesn't
    # matter — these tests verify wiring, not certificate parsing.
    $script:fakePfxBytes  = [byte[]]::new(1024)
    [System.Random]::new(42).NextBytes($script:fakePfxBytes)
    $script:fakePfxBase64 = [Convert]::ToBase64String($script:fakePfxBytes)
}

Describe 'Get-CertificateFromKeyVault' {
    It 'writes the decoded bytes to a temp PFX file and returns the path' {
        $b64 = $script:fakePfxBase64
        $bytes = $script:fakePfxBytes
        $path = InModuleScope KeyVaultHelper -Parameters @{ b64 = $b64 } {
            param($b64)
            Mock Get-AzKeyVaultCertificate { [PSCustomObject]@{ Name = $Name; VaultName = $VaultName } }
            Mock Get-AzKeyVaultSecret { $b64 }
            Get-CertificateFromKeyVault -VaultName 'kv-test' -CertName 'mycert'
        }
        try {
            Test-Path $path | Should -Be $true
            $path | Should -Match 'mycert\.pfx$'
            $written = [System.IO.File]::ReadAllBytes($path)
            $written.Length | Should -Be 1024
            $written[0..3] | Should -Be $bytes[0..3]
        }
        finally {
            if (Test-Path $path) { Remove-Item $path -Force }
        }
    }

    It 'lands the file under the system temp directory' {
        $b64 = $script:fakePfxBase64
        $path = InModuleScope KeyVaultHelper -Parameters @{ b64 = $b64 } {
            param($b64)
            Mock Get-AzKeyVaultCertificate { [PSCustomObject]@{ Name = $Name; VaultName = $VaultName } }
            Mock Get-AzKeyVaultSecret { $b64 }
            Get-CertificateFromKeyVault -VaultName 'kv-test' -CertName 'cert2'
        }
        try {
            $tempRoot = [System.IO.Path]::GetTempPath().TrimEnd([System.IO.Path]::DirectorySeparatorChar)
            $path | Should -Match ([regex]::Escape($tempRoot))
        }
        finally {
            if (Test-Path $path) { Remove-Item $path -Force }
        }
    }

    It 'requires VaultName and CertName' {
        { Get-CertificateFromKeyVault -CertName 'x' } | Should -Throw
        { Get-CertificateFromKeyVault -VaultName 'y' } | Should -Throw
    }
}

Describe 'Get-CertificateBytes' {
    It 'returns the raw decoded PFX bytes (no file written)' {
        $b64 = $script:fakePfxBase64
        $bytes = $script:fakePfxBytes
        $result = InModuleScope KeyVaultHelper -Parameters @{ b64 = $b64 } {
            param($b64)
            Mock Get-AzKeyVaultCertificate { [PSCustomObject]@{ Name = $Name; VaultName = $VaultName } }
            Mock Get-AzKeyVaultSecret { $b64 }
            Get-CertificateBytes -VaultName 'kv-test' -CertName 'mycert'
        }
        # Pester unwraps a single-element byte[] return into raw bytes via
        # output stream — re-cast to defend against that.
        $resultBytes = [byte[]]$result
        $resultBytes.Length | Should -Be 1024
        $resultBytes[0..3] | Should -Be $bytes[0..3]
    }

    It 'requires VaultName and CertName' {
        { Get-CertificateBytes -CertName 'x' } | Should -Throw
        { Get-CertificateBytes -VaultName 'y' } | Should -Throw
    }
}

Describe 'Remove-CertificateFile' {
    It 'removes the file when it exists' {
        $tempPath = Join-Path ([System.IO.Path]::GetTempPath()) "kvhelper-test-$(Get-Random).pfx"
        [System.IO.File]::WriteAllBytes($tempPath, [byte[]](1..32))

        Remove-CertificateFile -Path $tempPath
        Test-Path $tempPath | Should -Be $false
    }

    It 'is a no-op when the path does not exist' {
        $missing = Join-Path ([System.IO.Path]::GetTempPath()) "kvhelper-missing-$(Get-Random).pfx"
        { Remove-CertificateFile -Path $missing } | Should -Not -Throw
    }

    It 'zeros the bytes on disk before unlinking (best-effort secret hygiene)' {
        # Mock Remove-Item so we can read the file content at the moment
        # Remove-CertificateFile would unlink — verifies the zero-pass ran first.
        $tempPath = Join-Path ([System.IO.Path]::GetTempPath()) "kvhelper-zero-$(Get-Random).pfx"
        [System.IO.File]::WriteAllBytes($tempPath, [byte[]](7..38))   # 32 nonzero bytes

        InModuleScope KeyVaultHelper -Parameters @{ p = $tempPath } {
            param($p)
            $script:capturedBytes = $null
            Mock Remove-Item {
                $script:capturedBytes = [System.IO.File]::ReadAllBytes($p)
            }
            Remove-CertificateFile -Path $p
            ($script:capturedBytes | Where-Object { $_ -ne 0 }).Count | Should -Be 0
            Should -Invoke Remove-Item -Times 1
        }
        if (Test-Path $tempPath) { Remove-Item $tempPath -Force }
    }
}
