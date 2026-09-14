#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Unit tests for HighWaterMark.psm1 — sentinel-blob read/write for log-ingest.
#
# Verifies:
#   - Get-HighWaterMark returns parsed JSON on 200
#   - Get-HighWaterMark returns $null on 404 (first run)
#   - Get-HighWaterMark re-throws on other errors
#   - Set-HighWaterMark writes via single Put Blob (overwrite-by-default)
#   - Both functions use the blob.* endpoint (not dfs.*) so the
#     create-only DFS path can't 409 on a constant sentinel path
#   - Set-HighWaterMark emits the documented JSON schema (schema_version=1)

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    # LogHelper supplies Write-Log, used by the transient/401 retry wrapper.
    Import-Module (Join-Path $modulesPath 'LogHelper.psm1')         -Force
    Import-Module (Join-Path $modulesPath 'StorageHelperRest.psm1') -Force
    Import-Module (Join-Path $modulesPath 'HighWaterMark.psm1')     -Force
}

Describe 'Get-HighWaterMark' {

    Context 'success path' {
        It 'returns parsed JSON when the sentinel blob exists' {
            InModuleScope HighWaterMark {
                Mock Get-AdlsAccessToken { 'fake-token' }
                Mock Invoke-WebRequest {
                    return [PSCustomObject]@{
                        Content = '{"high_water_mark":"2026-05-08T00:41:09Z","last_run_id":"abc123","last_run_completed_at":"2026-05-08T00:48:07Z","schema_version":1}'
                    }
                }

                $hwm = Get-HighWaterMark `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -Container 'landing' `
                    -Entity 'entra_sign_in_logs' `
                    -TenantKey 'madev1'

                # ConvertFrom-Json auto-parses ISO-8601 timestamps into
                # DateTime, so high_water_mark comes back as [datetime] —
                # compare against its parsed value, not the string source.
                $hwm.high_water_mark | Should -Be ([datetime]::Parse('2026-05-08T00:41:09Z').ToUniversalTime())
                $hwm.last_run_id     | Should -Be 'abc123'
                $hwm.schema_version  | Should -Be 1
            }
        }

        It 'targets the blob.* endpoint (not dfs.*) and the _state path' {
            InModuleScope HighWaterMark {
                Mock Get-AdlsAccessToken { 'tok' }
                $script:CapturedUri = $null
                Mock Invoke-WebRequest {
                    $script:CapturedUri = $Uri
                    return [PSCustomObject]@{ Content = '{"schema_version":1}' }
                }

                Get-HighWaterMark `
                    -StorageAccountUrl 'https://stmatoolkit.dfs.core.windows.net' `
                    -Container 'landing' `
                    -Entity 'audit_general' `
                    -TenantKey 'contoso'

                # .dfs. → .blob. swap
                $script:CapturedUri | Should -BeLike 'https://stmatoolkit.blob.core.windows.net/*'
                $script:CapturedUri | Should -Match '_state/audit_general/contoso/high_water_mark\.json$'
            }
        }
    }

    Context '404 (first run)' {
        It 'returns $null when the sentinel doesn''t exist' {
            InModuleScope HighWaterMark {
                Mock Get-AdlsAccessToken { 'tok' }
                # Build an exception that mirrors the .NET HttpResponseException
                # shape Get-HighWaterMark inspects (.Exception.Response.StatusCode).
                $resp = [PSCustomObject]@{ StatusCode = 404 }
                $ex = [System.Exception]::new('Not Found')
                Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                Mock Invoke-WebRequest { throw $ex }

                $hwm = Get-HighWaterMark `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -Container 'landing' `
                    -Entity 'audit_entra' `
                    -TenantKey 'madev1'

                $hwm | Should -Be $null
            }
        }
    }

    Context 'other errors' {
        It 'rethrows non-transient, non-404 status codes without retrying' {
            InModuleScope HighWaterMark {
                Mock Get-AdlsAccessToken { 'tok' }
                Mock Start-Sleep { }
                $script:Calls = 0
                $resp = [PSCustomObject]@{ StatusCode = 403 }
                $ex = [System.Exception]::new('Forbidden')
                Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                Mock Invoke-WebRequest { $script:Calls++; throw $ex }

                {
                    Get-HighWaterMark `
                        -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                        -Container 'landing' `
                        -Entity 'audit_entra' `
                        -TenantKey 'madev1'
                } | Should -Throw

                # 403 is non-transient: one attempt, no backoff.
                $script:Calls | Should -Be 1
                Should -Invoke Start-Sleep -Times 0
            }
        }
    }
}

Describe 'High-water-mark transient-fault retry' {

    It 'Get-HighWaterMark retries a transient 5xx and eventually succeeds' {
        InModuleScope HighWaterMark {
            Mock Get-AdlsAccessToken { 'tok' }
            Mock Start-Sleep { }
            Mock Write-Log { }
            $script:Calls = 0
            Mock Invoke-WebRequest {
                $script:Calls++
                if ($script:Calls -lt 3) {
                    $resp = [PSCustomObject]@{ StatusCode = 503 }
                    $ex = [System.Exception]::new('Service Unavailable')
                    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                    throw $ex
                }
                return [PSCustomObject]@{ Content = '{"schema_version":1}' }
            }

            $hwm = Get-HighWaterMark `
                -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                -Container 'landing' `
                -Entity 'audit_general' `
                -TenantKey 'madev1'

            $hwm.schema_version | Should -Be 1
            $script:Calls | Should -Be 3
        }
    }

    It 'Get-HighWaterMark gives up after MaxAttempts on a persistent 5xx' {
        InModuleScope HighWaterMark {
            Mock Get-AdlsAccessToken { 'tok' }
            Mock Start-Sleep { }
            Mock Write-Log { }
            $script:Calls = 0
            Mock Invoke-WebRequest {
                $script:Calls++
                $resp = [PSCustomObject]@{ StatusCode = 500 }
                $ex = [System.Exception]::new('Internal Server Error')
                Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                throw $ex
            }

            {
                Get-HighWaterMark `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -Container 'landing' `
                    -Entity 'audit_general' `
                    -TenantKey 'madev1'
            } | Should -Throw

            # Default MaxAttempts = 5.
            $script:Calls | Should -Be 5
        }
    }

    It 'Set-HighWaterMark retries a transient 5xx and eventually succeeds' {
        InModuleScope HighWaterMark {
            Mock Get-AdlsAccessToken { 'tok' }
            Mock Start-Sleep { }
            Mock Write-Log { }
            $script:Calls = 0
            Mock Invoke-WebRequest {
                $script:Calls++
                if ($script:Calls -lt 2) {
                    $resp = [PSCustomObject]@{ StatusCode = 503 }
                    $ex = [System.Exception]::new('Service Unavailable')
                    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                    throw $ex
                }
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            Set-HighWaterMark `
                -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                -Container 'landing' `
                -Entity 'entra_sign_in_logs' `
                -TenantKey 'madev1' `
                -Timestamp ([datetime]::UtcNow) `
                -RunId 'run-503'

            $script:Calls | Should -Be 2
        }
    }
}

Describe 'Set-HighWaterMark' {

    It 'uses a single Put Blob (one PUT, not the DFS three-step)' {
        InModuleScope HighWaterMark {
            Mock Get-AdlsAccessToken { 'tok' }
            $script:Calls = [System.Collections.Generic.List[hashtable]]::new()
            Mock Invoke-WebRequest {
                $script:Calls.Add(@{
                    Uri     = $Uri
                    Method  = $Method
                    Headers = $Headers
                    Body    = $Body
                })
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            Set-HighWaterMark `
                -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                -Container 'landing' `
                -Entity 'entra_sign_in_logs' `
                -TenantKey 'madev1' `
                -Timestamp ([datetime]::Parse('2026-05-08T00:41:09Z')) `
                -RunId 'run-abc'

            # Single call — proves we're not on the DFS create/append/flush path
            # (which would 409 on the constant sentinel path)
            $script:Calls.Count | Should -Be 1
            $script:Calls[0].Method | Should -Be 'PUT'
            $script:Calls[0].Headers['x-ms-blob-type'] | Should -Be 'BlockBlob'
        }
    }

    It 'targets the blob.* endpoint and _state path' {
        InModuleScope HighWaterMark {
            Mock Get-AdlsAccessToken { 'tok' }
            $script:Uri = $null
            Mock Invoke-WebRequest {
                $script:Uri = $Uri
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            Set-HighWaterMark `
                -StorageAccountUrl 'https://stmatoolkit.dfs.core.windows.net' `
                -Container 'landing' `
                -Entity 'audit_sharepoint' `
                -TenantKey 'contoso' `
                -Timestamp ([datetime]::UtcNow) `
                -RunId 'r1'

            $script:Uri | Should -BeLike 'https://stmatoolkit.blob.core.windows.net/*'
            $script:Uri | Should -Match '_state/audit_sharepoint/contoso/high_water_mark\.json$'
        }
    }

    It 'emits a JSON body matching the documented schema (v1)' {
        InModuleScope HighWaterMark {
            Mock Get-AdlsAccessToken { 'tok' }
            $script:Body = $null
            Mock Invoke-WebRequest {
                $script:Body = $Body
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            $ts = [datetime]::Parse('2026-05-08T00:41:09Z').ToUniversalTime()
            Set-HighWaterMark `
                -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                -Container 'landing' `
                -Entity 'audit_general' `
                -TenantKey 'madev1' `
                -Timestamp $ts `
                -RunId 'run-xyz'

            $json = [System.Text.Encoding]::UTF8.GetString($script:Body) | ConvertFrom-Json
            $json.schema_version | Should -Be 1
            $json.last_run_id    | Should -Be 'run-xyz'
            # high_water_mark also auto-parses to DateTime via ConvertFrom-Json;
            # match value semantics not raw string format.
            $json.high_water_mark | Should -Be $ts
            $json.last_run_completed_at | Should -Not -BeNullOrEmpty
        }
    }
}

Describe 'High-water-mark 401 safety net' {

    It 'Get-HighWaterMark re-mints and retries once on a 401' {
        InModuleScope HighWaterMark {
            Mock Get-AdlsAccessToken { 'tok' }
            $script:Calls = 0
            Mock Invoke-WebRequest {
                $script:Calls++
                if ($script:Calls -eq 1) {
                    # First pass: the cached token is rejected.
                    $resp = [PSCustomObject]@{ StatusCode = 401 }
                    $ex = [System.Exception]::new('Unauthorized')
                    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                    throw $ex
                }
                return [PSCustomObject]@{ Content = '{"schema_version":1}' }
            }

            $hwm = Get-HighWaterMark `
                -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                -Container 'landing' `
                -Entity 'audit_general' `
                -TenantKey 'madev1'

            $hwm.schema_version | Should -Be 1
            $script:Calls | Should -Be 2
            # The 401 forces a token refresh before the retry.
            Should -Invoke Get-AdlsAccessToken -Times 2
        }
    }

    It 'Get-HighWaterMark does not retry a second time when the 401 persists' {
        InModuleScope HighWaterMark {
            Mock Get-AdlsAccessToken { 'tok' }
            $resp = [PSCustomObject]@{ StatusCode = 401 }
            $ex = [System.Exception]::new('Unauthorized')
            Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
            Mock Invoke-WebRequest { throw $ex }

            {
                Get-HighWaterMark `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -Container 'landing' `
                    -Entity 'audit_general' `
                    -TenantKey 'madev1'
            } | Should -Throw

            Should -Invoke Get-AdlsAccessToken -Times 2
        }
    }

    It 'Set-HighWaterMark re-mints and retries once on a 401' {
        InModuleScope HighWaterMark {
            Mock Get-AdlsAccessToken { 'tok' }
            $script:Calls = 0
            Mock Invoke-WebRequest {
                $script:Calls++
                if ($script:Calls -eq 1) {
                    $resp = [PSCustomObject]@{ StatusCode = 401 }
                    $ex = [System.Exception]::new('Unauthorized')
                    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                    throw $ex
                }
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            Set-HighWaterMark `
                -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                -Container 'landing' `
                -Entity 'entra_sign_in_logs' `
                -TenantKey 'madev1' `
                -Timestamp ([datetime]::UtcNow) `
                -RunId 'run-401'

            $script:Calls | Should -Be 2
            Should -Invoke Get-AdlsAccessToken -Times 2
        }
    }

    It 'Get-HighWaterMark force-refreshes only on the retry right after a 401, not on later transient backoffs' {
        InModuleScope HighWaterMark {
            Mock Get-AdlsAccessToken { 'tok' }
            Mock Start-Sleep { }
            Mock Write-Log { }
            $script:Calls = 0
            Mock Invoke-WebRequest {
                $script:Calls++
                if ($script:Calls -eq 1) {
                    $resp = [PSCustomObject]@{ StatusCode = 401 }
                    $ex = [System.Exception]::new('Unauthorized')
                    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                    throw $ex
                }
                if ($script:Calls -eq 2) {
                    # After the 401 re-mint, a transient 5xx must NOT trigger
                    # another force-refresh on the following backoff retry.
                    $resp = [PSCustomObject]@{ StatusCode = 503 }
                    $ex = [System.Exception]::new('Service Unavailable')
                    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                    throw $ex
                }
                return [PSCustomObject]@{ Content = '{"schema_version":1}' }
            }

            $hwm = Get-HighWaterMark `
                -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                -Container 'landing' `
                -Entity 'audit_general' `
                -TenantKey 'madev1'

            $hwm.schema_version | Should -Be 1
            $script:Calls | Should -Be 3
            # Exactly one force-mint: the 401 re-mint. The up-front fetch and the
            # post-401 transient backoff retry both reuse the cached token.
            Should -Invoke Get-AdlsAccessToken -Exactly -Times 1 -ParameterFilter { $ForceRefresh -eq $true }
            Should -Invoke Get-AdlsAccessToken -Exactly -Times 1 -ParameterFilter { -not $ForceRefresh }
        }
    }
}
