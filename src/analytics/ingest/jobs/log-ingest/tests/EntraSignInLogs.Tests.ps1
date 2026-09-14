#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for Resolve-SignInWindow in EntraSignInLogs.psm1 — verifies the
# no-HWM, HWM-present, and backfill branches produce the correct window.

BeforeAll {
    $script:SharedModules = Join-Path $PSScriptRoot '..' '..' 'shared' 'modules'
    $script:Scripts       = Join-Path $PSScriptRoot '..' 'scripts'
    Import-Module (Join-Path $script:SharedModules 'LogHelper.psm1')          -Force
    Import-Module (Join-Path $script:SharedModules 'RetryHelper.psm1')        -Force
    Import-Module (Join-Path $script:SharedModules 'StorageHelperRest.psm1')  -Force
    Import-Module (Join-Path $script:SharedModules 'HighWaterMark.psm1')      -Force
    Import-Module (Join-Path $script:Scripts 'MgmtApiCommon.psm1')            -Force -DisableNameChecking
    Import-Module (Join-Path $PSScriptRoot '_TestHelpers.psm1')               -Force
    Import-Module (Join-Path $script:Scripts 'entities' 'EntraSignInLogs.psm1') -Force -DisableNameChecking
}

AfterAll {
    Remove-Module EntraSignInLogs, _TestHelpers, MgmtApiCommon, HighWaterMark, StorageHelperRest, RetryHelper, LogHelper -Force -ErrorAction SilentlyContinue
}

Describe 'Resolve-SignInWindow' {
    BeforeEach {
        $env:BACKFILL_MODE       = $null
        $env:BACKFILL_START      = $null
        $env:BACKFILL_END        = $null
        $env:STORAGE_ACCOUNT_URL = 'https://test.dfs.core.windows.net'
        $env:LANDING_CONTAINER   = 'log'
    }
    AfterEach {
        $env:BACKFILL_MODE       = $null
        $env:BACKFILL_START      = $null
        $env:BACKFILL_END        = $null
        $env:STORAGE_ACCOUNT_URL = $null
        $env:LANDING_CONTAINER   = $null
    }

    Context 'no HWM (first run / post-wipe)' {
        It 'returns a 24h look-back window' {
            InModuleScope 'EntraSignInLogs' {
                Mock Get-HighWaterMark { return $null }

                $before = (Get-Date).ToUniversalTime()
                $window = Resolve-SignInWindow -TenantKey 'xtlab2'
                $after  = (Get-Date).ToUniversalTime()

                $window.IsBackfill | Should -BeFalse
                $window.End   | Should -BeGreaterOrEqual $before
                $window.End   | Should -BeLessOrEqual    $after
                $window.Start | Should -BeGreaterOrEqual $before.AddHours(-24).AddSeconds(-1)
                $window.Start | Should -BeLessOrEqual    $after.AddHours(-24).AddSeconds(1)
            }
        }
    }

    Context 'HWM present (steady state)' {
        It 'returns [hwm - 15min, now] when HWM is recent' {
            InModuleScope 'EntraSignInLogs' {
                Mock Get-HighWaterMark {
                    return @{ high_water_mark = (Get-Date).ToUniversalTime().AddHours(-2) }
                }

                $window = Resolve-SignInWindow -TenantKey 'xtlab2'
                $now = (Get-Date).ToUniversalTime()

                $window.IsBackfill | Should -BeFalse
                $window.Start | Should -BeGreaterOrEqual $now.AddHours(-2).AddMinutes(-16)
                $window.Start | Should -BeLessOrEqual    $now.AddHours(-2).AddMinutes(-14)
            }
        }

        It 'clamps start to now-24h when HWM is old' {
            InModuleScope 'EntraSignInLogs' {
                Mock Get-HighWaterMark {
                    return @{ high_water_mark = (Get-Date).ToUniversalTime().AddHours(-48) }
                }

                $before = (Get-Date).ToUniversalTime()
                $window = Resolve-SignInWindow -TenantKey 'xtlab2'
                $after  = (Get-Date).ToUniversalTime()

                $window.IsBackfill | Should -BeFalse
                $window.Start | Should -BeGreaterOrEqual $before.AddHours(-24).AddSeconds(-1)
                $window.Start | Should -BeLessOrEqual    $after.AddHours(-24).AddSeconds(1)
            }
        }
    }

    Context 'backfill mode' {
        It 'returns exact [BACKFILL_START, BACKFILL_END] window' {
            InModuleScope 'EntraSignInLogs' {
                $env:BACKFILL_MODE  = 'true'
                $env:BACKFILL_START = '2025-06-01T00:00:00Z'
                $env:BACKFILL_END   = '2025-06-02T00:00:00Z'

                $window = Resolve-SignInWindow -TenantKey 'xtlab2'

                $window.IsBackfill | Should -BeTrue
                $window.Start | Should -Be ([datetime]::Parse('2025-06-01T00:00:00Z').ToUniversalTime())
                $window.End   | Should -Be ([datetime]::Parse('2025-06-02T00:00:00Z').ToUniversalTime())
            }
        }

        It 'throws when BACKFILL_START or BACKFILL_END is missing' {
            InModuleScope 'EntraSignInLogs' {
                $env:BACKFILL_MODE  = 'true'
                $env:BACKFILL_START = $null
                $env:BACKFILL_END   = $null

                { Resolve-SignInWindow -TenantKey 'xtlab2' } | Should -Throw '*BACKFILL_START*'
            }
        }
    }
}

Describe 'Get-EntraSignInLogs HWM normalization' {
    BeforeEach {
        $env:TENANT_KEY          = 'xtlab2'
        $env:STORAGE_ACCOUNT_URL = 'https://test.dfs.core.windows.net'
        $env:LANDING_CONTAINER   = 'log'
        $env:RUN_ID              = 'test-run'
        $env:BACKFILL_MODE       = $null
    }

    AfterEach {
        $env:TENANT_KEY          = $null
        $env:STORAGE_ACCOUNT_URL = $null
        $env:LANDING_CONTAINER   = $null
        $env:RUN_ID              = $null
        $env:BACKFILL_MODE       = $null
    }

    It 'uses ConvertTo-UtcDateTime for [datetime] createdDateTime values before Set-HighWaterMark' {
        InModuleScope 'EntraSignInLogs' {
            $originalCulture   = [System.Threading.Thread]::CurrentThread.CurrentCulture
            $originalUiCulture = [System.Threading.Thread]::CurrentThread.CurrentUICulture
            try {
                $de = [System.Globalization.CultureInfo]::GetCultureInfo('de-DE')
                [System.Threading.Thread]::CurrentThread.CurrentCulture = $de
                [System.Threading.Thread]::CurrentThread.CurrentUICulture = $de

                $windowStart = [datetime]::new(2026, 6, 11, 0, 0, 0, [System.DateTimeKind]::Utc)
                $windowEnd   = [datetime]::new(2026, 6, 11, 1, 0, 0, [System.DateTimeKind]::Utc)

                $signInEarly = [datetime]::new(2026, 6, 11, 0, 20, 0, [System.DateTimeKind]::Utc)
                $signInLate  = [datetime]::new(2026, 6, 11, 0, 50, 0, [System.DateTimeKind]::Utc)
                $expectedLate = MgmtApiCommon\ConvertTo-UtcDateTime -Value $signInLate

                Mock Resolve-SignInWindow {
                    return @{ Start = $windowStart; End = $windowEnd; IsBackfill = $false }
                }
                Mock Invoke-WithRetry {
                    return [pscustomobject]@{
                        value = @(
                            [pscustomobject]@{ id = 'a'; createdDateTime = $signInEarly },
                            [pscustomobject]@{ id = 'b'; createdDateTime = $signInLate }
                        )
                        '@odata.nextLink' = $null
                    }
                }
                Mock Set-HighWaterMark {}

                $writer = New-CaptureWriter
                $context = @{ SelectFields = @('id', 'createdDateTime') }

                Get-EntraSignInLogs -Context $context -Writer $writer

                $writer.TotalWritten | Should -Be 2
                Should -Invoke Set-HighWaterMark -Times 1 -Exactly -ParameterFilter {
                    $Entity -eq 'entra_sign_in_logs' -and
                    $TenantKey -eq 'xtlab2' -and
                    $Timestamp -eq $expectedLate -and
                    $Timestamp.Kind -eq [System.DateTimeKind]::Utc
                }
            }
            finally {
                [System.Threading.Thread]::CurrentThread.CurrentCulture = $originalCulture
                [System.Threading.Thread]::CurrentThread.CurrentUICulture = $originalUiCulture
            }
        }
    }
}
