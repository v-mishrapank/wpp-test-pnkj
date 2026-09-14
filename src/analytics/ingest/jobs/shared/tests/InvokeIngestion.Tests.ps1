#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Subprocess-style integration tests for Invoke-Ingestion.ps1, the container
# entrypoint. The script calls `exit N` on validation failures and runs
# heavy external dependencies (Connect-AzAccount, Get-AzKeyVault*, ADLS REST
# uploads) — neither plays well with in-process Pester. These tests run the
# script as a child pwsh process, so:
#   - they're invisible to Pester's CodeCoverage instrumentation, BUT
#   - they exercise the real env-var validation and entrypoint orchestration
#     path that production containers hit on every run.

BeforeAll {
    $sourceScript = (Resolve-Path (
        Join-Path $PSScriptRoot '..' 'scripts' 'Invoke-Ingestion.ps1'
    )).Path
    $sourceModules = (Resolve-Path (Join-Path $PSScriptRoot '..' 'modules')).Path
    $stubConnect   = (Resolve-Path (Join-Path $PSScriptRoot 'StubConnect.psm1')).Path
    $script:pwshPath = (Get-Process -Id $PID).Path

    # The script uses `using module ./modules/...` and Join-Path $PSScriptRoot
    # 'modules' / 'Connect.psm1' / 'entities' — all resolved relative to the
    # script's location. In production the Dockerfile lays this out as
    # /app/Invoke-Ingestion.ps1 + /app/modules/ + /app/Connect.psm1 +
    # /app/entities/. We replicate that layout in a fresh temp dir so the
    # script can run as-shipped.
    $script:scriptDir = New-Item -ItemType Directory -Path (
        Join-Path ([System.IO.Path]::GetTempPath()) "InvokeIngestionTest_$(Get-Random)"
    ) -Force
    Copy-Item -Path $sourceScript -Destination (Join-Path $script:scriptDir.FullName 'Invoke-Ingestion.ps1')
    Copy-Item -Path $sourceModules -Destination (Join-Path $script:scriptDir.FullName 'modules') -Recurse
    Copy-Item -Path $stubConnect -Destination (Join-Path $script:scriptDir.FullName 'Connect.psm1')
    New-Item -ItemType Directory -Path (Join-Path $script:scriptDir.FullName 'entities') -Force | Out-Null

    $script:scriptPath = Join-Path $script:scriptDir.FullName 'Invoke-Ingestion.ps1'

    # Overlay KeyVaultHelper with a stub so tests that push past env-var
    # validation don't hit real Azure. Only Get-CertificateFromKeyVault is on
    # the early path; the others are kept signature-compatible so import
    # doesn't fail. Cert content is junk because StubConnect.Connect-Service
    # ignores the cert bytes — it just stores AuthConfig.
    $stubKvContent = @'
function Get-CertificateFromKeyVault {
    param([Parameter(Mandatory)][string]$VaultName, [Parameter(Mandatory)][string]$CertName)
    $pfxPath = Join-Path ([System.IO.Path]::GetTempPath()) "stub-$CertName-$(Get-Random).pfx"
    [System.IO.File]::WriteAllBytes($pfxPath, [byte[]](0x00, 0x01, 0x02, 0x03))
    return $pfxPath
}
function Get-CertificateBytes { param([string]$VaultName, [string]$CertName) return [byte[]](0x00, 0x01, 0x02, 0x03) }
function Remove-CertificateFile { param([string]$Path) if (Test-Path $Path) { Remove-Item $Path -Force } }
function Get-SecretValue { param([string]$VaultName, [string]$SecretName) return 'stub-secret' }
Export-ModuleMember -Function Get-CertificateFromKeyVault, Get-CertificateBytes, Remove-CertificateFile, Get-SecretValue
'@
    Set-Content -Path (Join-Path $script:scriptDir.FullName 'modules' 'KeyVaultHelper.psm1') -Value $stubKvContent

    # Wrapper script that pre-defines a no-op Connect-AzAccount in the
    # subprocess scope before invoking the real script. Connect-AzAccount
    # comes from Az.Accounts and requires a managed-identity environment;
    # the function defined here shadows the cmdlet during script execution.
    $wrapperContent = @'
function Connect-AzAccount {
    param([switch]$Identity, $WarningAction)
}
& "$PSScriptRoot/Invoke-Ingestion.ps1"
'@
    $script:wrapperPath = Join-Path $script:scriptDir.FullName 'wrapper-stub-az.ps1'
    Set-Content -Path $script:wrapperPath -Value $wrapperContent

    # Stub entity modules for the deterministic-order test (#408). Each
    # declares exactly one entity, all in the same single-stage root shape
    # Resolve-RootEntities expects. Filenames are chosen so the alphabetical
    # order (Alpha, Beta, Mu, Zeta) differs from any plausible insertion or
    # creation order — that's the property the test asserts on.
    function script:New-StubEntityModule {
        param([string]$EntityName, [string]$ModulePath)
        $content = @"
function Get-ModuleStages {
    @{
        '${EntityName}_root' = @{ InputFrom = `$null; RunsOnPool = `$false; Function = 'Get-${EntityName}Root'; ApiFamily = 'graph' }
    }
}
function Get-ModuleEntities {
    @{
        '${EntityName}' = @{ Stage = '${EntityName}_root'; WritesTo = 'root'; SelectFields = @('id') }
    }
}
function Get-${EntityName}Root { param([hashtable]`$Context, `$Writer) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-${EntityName}Root
"@
        Set-Content -Path $ModulePath -Value $content
    }

    # Helper: run Invoke-Ingestion.ps1 in a clean child shell with the given
    # env vars and capture exit code + combined output.
    function script:Invoke-Script {
        param(
            [hashtable]$EnvVars = @{},
            [int]$TimeoutSec   = 30
        )

        $clearVars = @(
            'TENANT_KEY','TENANT_ID','ORGANIZATION','CLIENT_ID','CERT_NAME',
            'KEYVAULT_NAME','STORAGE_ACCOUNT_URL','ENTITY_NAMES',
            'STORAGE_AUTH_METHOD','STORAGE_SP_TENANT_ID','STORAGE_SP_CLIENT_ID',
            'STORAGE_SP_CERT_NAME','ADMIN_URL','MAX_CONCURRENCY','LANDING_CONTAINER'
        )
        $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
        $startInfo.FileName  = $script:pwshPath
        $startInfo.Arguments = "-NoProfile -NonInteractive -File `"$script:scriptPath`""
        $startInfo.UseShellExecute        = $false
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError  = $true
        foreach ($v in $clearVars) {
            $startInfo.Environment[$v] = ''
        }
        foreach ($k in $EnvVars.Keys) {
            $startInfo.Environment[$k] = [string]$EnvVars[$k]
        }

        $proc = [System.Diagnostics.Process]::Start($startInfo)
        if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
            $proc.Kill($true)
            throw "Invoke-Ingestion.ps1 timed out after ${TimeoutSec}s"
        }
        return @{
            ExitCode = $proc.ExitCode
            Stdout   = $proc.StandardOutput.ReadToEnd()
            Stderr   = $proc.StandardError.ReadToEnd()
        }
    }

    # Helper: spawn the wrapper subprocess, stream stdout line-by-line, and
    # return as soon as a line matches $UntilMatch (or timeout fires).
    # Used by tests that need to assert on logs emitted before the script
    # would otherwise drag through real work the test environment can't
    # support (Invoke-ModuleRun on fake modules, ADLS uploads). Reads stderr
    # to-end after kill — fine for short-lived runs that don't fill the
    # 64 KB pipe buffer before the match.
    function script:Invoke-ScriptStreamingUntil {
        param(
            [hashtable]$EnvVars = @{},
            [Parameter(Mandatory)][string]$UntilMatch,
            [int]$TimeoutSec = 30
        )

        $clearVars = @(
            'TENANT_KEY','TENANT_ID','ORGANIZATION','CLIENT_ID','CERT_NAME',
            'KEYVAULT_NAME','STORAGE_ACCOUNT_URL','ENTITY_NAMES',
            'STORAGE_AUTH_METHOD','STORAGE_SP_TENANT_ID','STORAGE_SP_CLIENT_ID',
            'STORAGE_SP_CERT_NAME','ADMIN_URL','MAX_CONCURRENCY','LANDING_CONTAINER',
            'CONTAINER_TYPE','HEARTBEAT_FLUSH_SECONDS','RUN_ID'
        )
        $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
        $startInfo.FileName  = $script:pwshPath
        $startInfo.Arguments = "-NoProfile -NonInteractive -File `"$script:wrapperPath`""
        $startInfo.UseShellExecute        = $false
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError  = $true
        foreach ($v in $clearVars) { $startInfo.Environment[$v] = '' }
        foreach ($k in $EnvVars.Keys) { $startInfo.Environment[$k] = [string]$EnvVars[$k] }

        $proc = [System.Diagnostics.Process]::Start($startInfo)
        $stdoutLines = [System.Collections.Generic.List[string]]::new()
        $matchedLine = $null
        $deadline = (Get-Date).AddSeconds($TimeoutSec)
        $pendingTask = $null
        try {
            while (-not $proc.HasExited -and (Get-Date) -lt $deadline) {
                if ($null -eq $pendingTask) {
                    $pendingTask = $proc.StandardOutput.ReadLineAsync()
                }
                # Keep one outstanding ReadLineAsync at a time — issuing a
                # second one before the first completes throws "stream in use".
                if (-not $pendingTask.Wait(250)) { continue }
                $line = $pendingTask.Result
                $pendingTask = $null
                if ($null -eq $line) { break }
                $stdoutLines.Add($line)
                if ($line -match $UntilMatch) {
                    $matchedLine = $line
                    break
                }
            }
        }
        finally {
            if (-not $proc.HasExited) { try { $proc.Kill($true) } catch { Write-Verbose $_.Exception.Message } }
            try { $proc.WaitForExit(2000) | Out-Null } catch { Write-Verbose $_.Exception.Message }
        }
        $stderrText = ''
        try { $stderrText = $proc.StandardError.ReadToEnd() } catch { Write-Verbose $_.Exception.Message }
        $exitCode = -1
        try { $exitCode = $proc.ExitCode } catch { Write-Verbose $_.Exception.Message }
        return @{
            MatchedLine = $matchedLine
            Stdout      = ($stdoutLines -join "`n")
            Stderr      = $stderrText
            ExitCode    = $exitCode
        }
    }
}

AfterAll {
    if ($script:scriptDir -and (Test-Path $script:scriptDir.FullName)) {
        Remove-Item $script:scriptDir.FullName -Recurse -Force
    }
}

Describe 'Invoke-Ingestion env-var validation' {
    It 'exits 1 when no required env vars are set' {
        $r = Invoke-Script -EnvVars @{}
        $r.ExitCode | Should -Be 1
        $r.Stderr   | Should -Match 'Missing required environment variable'
    }

    It 'lists every missing var in a single error' {
        $r = Invoke-Script -EnvVars @{}
        # The script should batch all missing vars into one message — this
        # avoids the failure mode where you fix one, re-run, fix another, etc.
        $r.Stderr | Should -Match 'TENANT_KEY'
        $r.Stderr | Should -Match 'TENANT_ID'
        $r.Stderr | Should -Match 'KEYVAULT_NAME'
        $r.Stderr | Should -Match 'STORAGE_ACCOUNT_URL'
        $r.Stderr | Should -Match 'ENTITY_NAMES'
    }

    It 'exits 1 when service_principal_cert mode is set but SP vars are missing' {
        $r = Invoke-Script -EnvVars @{
            TENANT_KEY          = 'fab'
            TENANT_ID           = '11111111-1111-1111-1111-111111111111'
            ORGANIZATION        = 'fab'
            CLIENT_ID           = '22222222-2222-2222-2222-222222222222'
            CERT_NAME           = 'cert'
            KEYVAULT_NAME       = 'kv'
            STORAGE_ACCOUNT_URL = 'https://acct.dfs.core.windows.net'
            ENTITY_NAMES        = 'foo'
            STORAGE_AUTH_METHOD = 'service_principal_cert'
        }
        $r.ExitCode | Should -Be 1
        $r.Stderr   | Should -Match 'STORAGE_SP_CERT_NAME'
    }

    It 'exits 1 when service_principal_secret mode is set but SP vars are missing' {
        $r = Invoke-Script -EnvVars @{
            TENANT_KEY          = 'fab'
            TENANT_ID           = '11111111-1111-1111-1111-111111111111'
            ORGANIZATION        = 'fab'
            CLIENT_ID           = '22222222-2222-2222-2222-222222222222'
            CERT_NAME           = 'cert'
            KEYVAULT_NAME       = 'kv'
            STORAGE_ACCOUNT_URL = 'https://acct.dfs.core.windows.net'
            ENTITY_NAMES        = 'foo'
            STORAGE_AUTH_METHOD = 'service_principal_secret'
        }
        $r.ExitCode | Should -Be 1
        $r.Stderr   | Should -Match 'STORAGE_SP_SECRET_NAME'
        # secret mode must NOT demand the cert-mode var
        $r.Stderr   | Should -Not -Match 'STORAGE_SP_CERT_NAME'
    }

    It 'does not list STORAGE_SP_* in the required set in default (managed_identity) mode' {
        # When STORAGE_AUTH_METHOD is unset, the script defaults to managed
        # identity and should NOT demand STORAGE_SP_TENANT_ID / _CLIENT_ID /
        # _CERT_NAME / _SECRET_NAME. Easiest way to verify: omit every var.
        # The "missing required" message should mention all the always-required
        # vars, but not the SP-only ones.
        $r = Invoke-Script -EnvVars @{}
        $r.ExitCode | Should -Be 1
        $r.Stderr | Should -Not -Match 'STORAGE_SP_TENANT_ID'
        $r.Stderr | Should -Not -Match 'STORAGE_SP_CLIENT_ID'
        $r.Stderr | Should -Not -Match 'STORAGE_SP_CERT_NAME'
        $r.Stderr | Should -Not -Match 'STORAGE_SP_SECRET_NAME'
    }
}

Describe 'Invoke-Ingestion deterministic module order (#408)' {
    BeforeAll {
        # Filenames chosen so alphabetical order (Alpha, Beta, Mu, Zeta)
        # differs from on-disk creation order — guards against a "happens
        # to enumerate in creation order" false pass.
        $entitiesDir = Join-Path $script:scriptDir.FullName 'entities'
        New-StubEntityModule -EntityName 'zeta'  -ModulePath (Join-Path $entitiesDir 'Zeta.psm1')
        New-StubEntityModule -EntityName 'alpha' -ModulePath (Join-Path $entitiesDir 'Alpha.psm1')
        New-StubEntityModule -EntityName 'mu'    -ModulePath (Join-Path $entitiesDir 'Mu.psm1')
        New-StubEntityModule -EntityName 'beta'  -ModulePath (Join-Path $entitiesDir 'Beta.psm1')
    }

    AfterAll {
        # Don't leak stub entity modules to other Describe blocks.
        $entitiesDir = Join-Path $script:scriptDir.FullName 'entities'
        Get-ChildItem $entitiesDir -Filter '*.psm1' -ErrorAction SilentlyContinue |
            Remove-Item -Force -ErrorAction SilentlyContinue
    }

    It 'iterates modules in alphabetical order regardless of registry input order' {
        # ENTITY_NAMES intentionally in non-alphabetical order. If the script
        # iterated $entitiesByModule.Keys directly (the bug), the log would
        # reflect either insertion order or hashtable enumeration order —
        # both non-deterministic. With the sort in place, output is always
        # Alpha, Beta, Mu, Zeta.
        $r = Invoke-ScriptStreamingUntil -UntilMatch 'Module execution order:' -EnvVars @{
            TENANT_KEY          = 'fab'
            TENANT_ID           = '11111111-1111-1111-1111-111111111111'
            ORGANIZATION        = 'fab'
            CLIENT_ID           = '22222222-2222-2222-2222-222222222222'
            CERT_NAME           = 'cert'
            KEYVAULT_NAME       = 'kv'
            STORAGE_ACCOUNT_URL = 'https://acct.dfs.core.windows.net'
            ENTITY_NAMES        = 'zeta,alpha,mu,beta'
        } -TimeoutSec 30
        $r.MatchedLine | Should -Not -BeNullOrEmpty -Because "expected to see the order log; stdout was:`n$($r.Stdout)`nstderr:`n$($r.Stderr)"
        $r.MatchedLine | Should -Match 'Module execution order: Alpha\.psm1, Beta\.psm1, Mu\.psm1, Zeta\.psm1'
    }
}
