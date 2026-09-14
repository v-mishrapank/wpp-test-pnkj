#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for Resolve-IngestionWindow in MgmtApiCommon.psm1 — verifies the
# no-HWM, HWM-present, and backfill branches produce the correct window.
# Covers the 4 audit_* entities (audit_entra, audit_exchange,
# audit_sharepoint, audit_general).

BeforeAll {
    $script:SharedModules = Join-Path $PSScriptRoot '..' '..' 'shared' 'modules'
    $script:Scripts       = Join-Path $PSScriptRoot '..' 'scripts'
    Import-Module (Join-Path $script:SharedModules 'LogHelper.psm1')          -Force
    Import-Module (Join-Path $script:SharedModules 'StorageHelperRest.psm1')  -Force
    Import-Module (Join-Path $script:SharedModules 'HighWaterMark.psm1')      -Force
    Import-Module (Join-Path $script:Scripts 'MgmtApiCommon.psm1')            -Force -DisableNameChecking
}

AfterAll {
    Remove-Module MgmtApiCommon, HighWaterMark, StorageHelperRest, LogHelper -Force -ErrorAction SilentlyContinue
}

Describe 'Resolve-IngestionWindow' {
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
            InModuleScope 'MgmtApiCommon' {
                Mock Get-HighWaterMark { return $null }

                $before = (Get-Date).ToUniversalTime()
                $window = Resolve-IngestionWindow -Entity 'audit_entra' -TenantKey 'xtlab2'
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
            InModuleScope 'MgmtApiCommon' {
                Mock Get-HighWaterMark {
                    return @{ high_water_mark = (Get-Date).ToUniversalTime().AddHours(-2) }
                }

                $window = Resolve-IngestionWindow -Entity 'audit_exchange' -TenantKey 'xtlab2'
                $now = (Get-Date).ToUniversalTime()

                $window.IsBackfill | Should -BeFalse
                $window.Start | Should -BeGreaterOrEqual $now.AddHours(-2).AddMinutes(-16)
                $window.Start | Should -BeLessOrEqual    $now.AddHours(-2).AddMinutes(-14)
            }
        }

        It 'clamps start to now-24h when HWM is old' {
            InModuleScope 'MgmtApiCommon' {
                Mock Get-HighWaterMark {
                    return @{ high_water_mark = (Get-Date).ToUniversalTime().AddHours(-48) }
                }

                $before = (Get-Date).ToUniversalTime()
                $window = Resolve-IngestionWindow -Entity 'audit_sharepoint' -TenantKey 'xtlab2'
                $after  = (Get-Date).ToUniversalTime()

                $window.IsBackfill | Should -BeFalse
                $window.Start | Should -BeGreaterOrEqual $before.AddHours(-24).AddSeconds(-1)
                $window.Start | Should -BeLessOrEqual    $after.AddHours(-24).AddSeconds(1)
            }
        }
    }

    Context 'backfill mode' {
        It 'returns exact [BACKFILL_START, BACKFILL_END] window' {
            InModuleScope 'MgmtApiCommon' {
                $env:BACKFILL_MODE  = 'true'
                $env:BACKFILL_START = '2025-06-01T00:00:00Z'
                $env:BACKFILL_END   = '2025-06-02T00:00:00Z'

                $window = Resolve-IngestionWindow -Entity 'audit_general' -TenantKey 'xtlab2'

                $window.IsBackfill | Should -BeTrue
                $window.Start | Should -Be ([datetime]::Parse('2025-06-01T00:00:00Z').ToUniversalTime())
                $window.End   | Should -Be ([datetime]::Parse('2025-06-02T00:00:00Z').ToUniversalTime())
            }
        }

        It 'throws when BACKFILL_START or BACKFILL_END is missing' {
            InModuleScope 'MgmtApiCommon' {
                $env:BACKFILL_MODE  = 'true'
                $env:BACKFILL_START = $null
                $env:BACKFILL_END   = $null

                { Resolve-IngestionWindow -Entity 'audit_entra' -TenantKey 'xtlab2' } | Should -Throw '*BACKFILL_START*'
            }
        }
    }
}

Describe 'ConvertTo-UtcDateTime' {
    It 'preserves UTC DateTime input under de-DE culture (no stringify/re-parse)' {
        InModuleScope 'MgmtApiCommon' {
            $originalCulture   = [System.Threading.Thread]::CurrentThread.CurrentCulture
            $originalUiCulture = [System.Threading.Thread]::CurrentThread.CurrentUICulture
            try {
                $de = [System.Globalization.CultureInfo]::GetCultureInfo('de-DE')
                [System.Threading.Thread]::CurrentThread.CurrentCulture = $de
                [System.Threading.Thread]::CurrentThread.CurrentUICulture = $de

                $utcValue = [datetime]::new(2026, 6, 11, 20, 53, 17, [System.DateTimeKind]::Utc)
                $legacyRoundTrip = [datetime]::Parse($utcValue)

                $legacyRoundTrip.Kind | Should -Not -Be ([System.DateTimeKind]::Utc)

                $normalized = ConvertTo-UtcDateTime -Value $utcValue
                $normalized | Should -Be $utcValue
                $normalized.Kind | Should -Be ([System.DateTimeKind]::Utc)
            }
            finally {
                [System.Threading.Thread]::CurrentThread.CurrentCulture = $originalCulture
                [System.Threading.Thread]::CurrentThread.CurrentUICulture = $originalUiCulture
            }
        }
    }
}

Describe 'Invoke-MgmtContentFetch HWM normalization' {
    BeforeEach {
        $env:TENANT_ID           = '11111111-1111-1111-1111-111111111111'
        $env:TENANT_KEY          = 'xtlab2'
        $env:STORAGE_ACCOUNT_URL = 'https://test.dfs.core.windows.net'
        $env:LANDING_CONTAINER   = 'log'
        $env:RUN_ID              = 'test-run'
    }

    AfterEach {
        $env:TENANT_ID           = $null
        $env:TENANT_KEY          = $null
        $env:STORAGE_ACCOUNT_URL = $null
        $env:LANDING_CONTAINER   = $null
        $env:RUN_ID              = $null
    }

    It 'uses ConvertTo-UtcDateTime for [datetime] CreationTime values before Set-HighWaterMark' {
        InModuleScope 'MgmtApiCommon' {
            $originalCulture   = [System.Threading.Thread]::CurrentThread.CurrentCulture
            $originalUiCulture = [System.Threading.Thread]::CurrentThread.CurrentUICulture
            try {
                $de = [System.Globalization.CultureInfo]::GetCultureInfo('de-DE')
                [System.Threading.Thread]::CurrentThread.CurrentCulture = $de
                [System.Threading.Thread]::CurrentThread.CurrentUICulture = $de

                $chunkStart = [datetime]::new(2026, 6, 11, 0, 0, 0, [System.DateTimeKind]::Utc)
                $chunkEnd   = [datetime]::new(2026, 6, 11, 1, 0, 0, [System.DateTimeKind]::Utc)

                $recordEarly = [datetime]::new(2026, 6, 11, 0, 10, 0, [System.DateTimeKind]::Utc)
                $recordLate  = [datetime]::new(2026, 6, 11, 0, 45, 0, [System.DateTimeKind]::Utc)
                $expectedLate = MgmtApiCommon\ConvertTo-UtcDateTime -Value $recordLate

                Mock Initialize-MgmtSubscription {}
                Mock Resolve-IngestionWindow {
                    return @{ Start = $chunkStart; End = $chunkEnd; IsBackfill = $false }
                }
                Mock Get-WindowsToWalk {
                    return @(@{ Start = $chunkStart; End = $chunkEnd })
                }
                Mock Get-PublisherQueryArg { return 'PublisherIdentifier=test' }
                Mock Invoke-MgmtApiRequest {
                    param([string]$Method, [string]$Uri, [switch]$WebRequest)
                    if ($WebRequest) {
                        return [pscustomobject]@{
                            Content = '[{"contentUri":"https://example.invalid/blob/1"}]'
                            Headers = @{}
                        }
                    }

                    return @(
                        [pscustomobject]@{ CreationTime = $recordEarly },
                        [pscustomobject]@{ CreationTime = $recordLate }
                    )
                }
                Mock Set-HighWaterMark {}

                $writer = [pscustomobject]@{ Count = 0 }
                $writer | Add-Member -MemberType ScriptMethod -Name WriteRecord -Value {
                    param($record)
                    $this.Count++
                }

                Invoke-MgmtContentFetch `
                    -Entity 'audit_entra' `
                    -ContentType 'Audit.AzureActiveDirectory' `
                    -Writer $writer

                $writer.Count | Should -Be 2
                Should -Invoke Set-HighWaterMark -Times 1 -Exactly -ParameterFilter {
                    $Entity -eq 'audit_entra' -and
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
