#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Unit tests for ScopeFilter.psm1 — reads a UPN set from _scope/<tenant>/<dim>/
# on the landing container and returns a case-insensitive HashSet for root
# fetchers to filter against. See issue #260.

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    Import-Module (Join-Path $modulesPath 'LogHelper.psm1')         -Force
    Import-Module (Join-Path $modulesPath 'StorageHelperRest.psm1') -Force
    Import-Module (Join-Path $modulesPath 'ScopeFilter.psm1')       -Force
}

AfterAll {
    Remove-Module ScopeFilter, StorageHelperRest, LogHelper -Force -ErrorAction SilentlyContinue
}

Describe 'Get-ScopeKeySet' {

    Context 'happy path' {
        It 'lists the dimension folder, picks the lex-max .jsonl, returns case-insensitive UPN set' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'fake-token' }
                $script:Calls = 0
                Mock Invoke-WebRequest -MockWith {
                    $script:Calls++
                    if ($script:Calls -eq 1) {
                        # List Paths response — three dated files
                        return [pscustomobject]@{
                            Content = (@{
                                paths = @(
                                    @{ name = '_scope/madev1/users/2026-05-26.jsonl'; isDirectory = $false; contentLength = '50' }
                                    @{ name = '_scope/madev1/users/2026-05-27.jsonl'; isDirectory = $false; contentLength = '60' }
                                    @{ name = '_scope/madev1/users/2026-05-28.jsonl'; isDirectory = $false; contentLength = '90' }
                                )
                            } | ConvertTo-Json -Depth 5)
                            Headers = @{}
                        }
                    }
                    # Second call: read the lex-max blob
                    return [pscustomobject]@{
                        Content = (@(
                            '{"_record":{"userPrincipalName":"alice@contoso.com"}}'
                            '{"_record":{"userPrincipalName":"bob@contoso.com","id":"aad-guid-2"}}'
                            '{"_record":{"userPrincipalName":"CAROL@contoso.com"}}'
                        ) -join "`n")
                        Headers = @{}
                    }
                }

                $upns = Get-ScopeKeySet `
                    -ScopeRoot '_scope/madev1' `
                    -Dimension 'users' `
                    -KeyField 'userPrincipalName' `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing'

                $upns                                        | Should -Not -BeNullOrEmpty
                $upns.Count                                  | Should -Be 3
                $upns.Contains('alice@contoso.com')          | Should -BeTrue
                # Case-insensitive lookup — input was CAROL, query lower-case
                $upns.Contains('carol@contoso.com')          | Should -BeTrue
                # Open schema: extra `id` field tolerated
                $upns.Contains('bob@contoso.com')            | Should -BeTrue
            }
        }

        It 'reads the lex-max blob, not an earlier one' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'fake-token' }
                $script:ReadUri = $null
                $script:CallCount = 0
                Mock Invoke-WebRequest -MockWith {
                    $script:CallCount++
                    if ($script:CallCount -eq 1) {
                        return [pscustomobject]@{
                            Content = (@{
                                paths = @(
                                    @{ name = '_scope/madev1/users/2026-05-26.jsonl'; isDirectory = $false; contentLength = '50' }
                                    @{ name = '_scope/madev1/users/2026-05-28.jsonl'; isDirectory = $false; contentLength = '90' }
                                    @{ name = '_scope/madev1/users/2026-05-27.jsonl'; isDirectory = $false; contentLength = '60' }
                                )
                            } | ConvertTo-Json -Depth 5)
                            Headers = @{}
                        }
                    }
                    $script:ReadUri = $Uri
                    return [pscustomobject]@{
                        Content = '{"_record":{"userPrincipalName":"alice@contoso.com"}}'
                        Headers = @{}
                    }
                }

                Get-ScopeKeySet `
                    -ScopeRoot '_scope/madev1' `
                    -Dimension 'users' `
                    -KeyField 'userPrincipalName' `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing' | Out-Null

                # Picked lex-max date, not earliest or middle
                $script:ReadUri | Should -BeLike '*2026-05-28.jsonl'
            }
        }
    }

    Context 'missing scope data' {
        It 'returns $null when the dimension directory 404s' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'tok' }
                $resp = [PSCustomObject]@{ StatusCode = 404 }
                $ex = [System.Exception]::new('Not Found')
                Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                Mock Invoke-WebRequest { throw $ex }

                $upns = Get-ScopeKeySet `
                    -ScopeRoot '_scope/madev1' `
                    -Dimension 'users' `
                    -KeyField 'userPrincipalName' `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing'

                $upns | Should -BeNullOrEmpty
            }
        }

        It 'returns $null when the directory exists but has no .jsonl files' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'tok' }
                Mock Invoke-WebRequest -MockWith {
                    return [pscustomobject]@{
                        Content = (@{
                            paths = @(
                                @{ name = '_scope/madev1/users/README.md'; isDirectory = $false; contentLength = '40' }
                            )
                        } | ConvertTo-Json -Depth 5)
                        Headers = @{}
                    }
                }

                $upns = Get-ScopeKeySet `
                    -ScopeRoot '_scope/madev1' `
                    -Dimension 'users' `
                    -KeyField 'userPrincipalName' `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing'

                $upns | Should -BeNullOrEmpty
            }
        }

        It 'returns $null when the directory listing is empty' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'tok' }
                Mock Invoke-WebRequest -MockWith {
                    return [pscustomobject]@{
                        Content = '{"paths":[]}'
                        Headers = @{}
                    }
                }

                $upns = Get-ScopeKeySet `
                    -ScopeRoot '_scope/madev1' `
                    -Dimension 'users' `
                    -KeyField 'userPrincipalName' `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing'

                $upns | Should -BeNullOrEmpty
            }
        }
    }

    Context 'non-UPN dimension (sites)' {
        It 'reads webUrl from the sites dimension and throws on missing webUrl' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'tok' }
                $script:Calls = 0
                Mock Invoke-WebRequest -MockWith {
                    $script:Calls++
                    if ($script:Calls -eq 1) {
                        return [pscustomobject]@{
                            Content = (@{
                                paths = @(
                                    @{ name = '_scope/madev1/sites/2026-05-28.jsonl'; isDirectory = $false; contentLength = '100' }
                                )
                            } | ConvertTo-Json -Depth 5)
                            Headers = @{}
                        }
                    }
                    return [pscustomobject]@{
                        Content = (@(
                            '{"_record":{"webUrl":"https://x.sharepoint.com/sites/A"}}'
                            '{"_record":{"webUrl":"https://x.sharepoint.com/sites/B","title":"B"}}'
                        ) -join "`n")
                        Headers = @{}
                    }
                }

                $urls = Get-ScopeKeySet `
                    -ScopeRoot '_scope/madev1' `
                    -Dimension 'sites' `
                    -KeyField 'webUrl' `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing'

                $urls.Count | Should -Be 2
                $urls.Contains('https://x.sharepoint.com/sites/A') | Should -BeTrue
                $urls.Contains('https://x.sharepoint.com/sites/B') | Should -BeTrue
            }
        }

        It 'strips trailing slashes so populator-emitted webUrls match TrimEnd-normalized consumer lookups' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'tok' }
                $script:Calls = 0
                Mock Invoke-WebRequest -MockWith {
                    $script:Calls++
                    if ($script:Calls -eq 1) {
                        return [pscustomobject]@{
                            Content = (@{
                                paths = @(
                                    @{ name = '_scope/madev1/sites/2026-05-28.jsonl'; isDirectory = $false; contentLength = '100' }
                                )
                            } | ConvertTo-Json -Depth 5)
                            Headers = @{}
                        }
                    }
                    return [pscustomobject]@{
                        Content = (@(
                            '{"_record":{"webUrl":"https://x.sharepoint.com/sites/A/"}}'
                            '{"_record":{"webUrl":"https://x.sharepoint.com/sites/B"}}'
                        ) -join "`n")
                        Headers = @{}
                    }
                }

                $urls = Get-ScopeKeySet `
                    -ScopeRoot '_scope/madev1' `
                    -Dimension 'sites' `
                    -KeyField 'webUrl' `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing'

                # Both URLs stored without trailing slash — matches the form
                # consumers (SpoSites) use after Graph webUrl.TrimEnd('/').
                $urls.Count | Should -Be 2
                $urls.Contains('https://x.sharepoint.com/sites/A') | Should -BeTrue
                $urls.Contains('https://x.sharepoint.com/sites/B') | Should -BeTrue
                $urls.Contains('https://x.sharepoint.com/sites/A/') | Should -BeFalse
            }
        }
    }

    Context 'malformed records' {
        It 'throws on a line that has _record but no userPrincipalName' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'tok' }
                $script:Calls = 0
                Mock Invoke-WebRequest -MockWith {
                    $script:Calls++
                    if ($script:Calls -eq 1) {
                        return [pscustomobject]@{
                            Content = (@{
                                paths = @(
                                    @{ name = '_scope/madev1/users/2026-05-28.jsonl'; isDirectory = $false; contentLength = '90' }
                                )
                            } | ConvertTo-Json -Depth 5)
                            Headers = @{}
                        }
                    }
                    return [pscustomobject]@{
                        Content = (@(
                            '{"_record":{"userPrincipalName":"alice@contoso.com"}}'
                            '{"_record":{"id":"aad-guid-2"}}'  # no UPN — must throw
                        ) -join "`n")
                        Headers = @{}
                    }
                }

                {
                    Get-ScopeKeySet `
                        -ScopeRoot '_scope/madev1' `
                        -Dimension 'users' `
                    -KeyField 'userPrincipalName' `
                        -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                        -ContainerName 'landing'
                } | Should -Throw '*no userPrincipalName*'
            }
        }

        It 'throws on a line that is not valid JSON' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'tok' }
                $script:Calls = 0
                Mock Invoke-WebRequest -MockWith {
                    $script:Calls++
                    if ($script:Calls -eq 1) {
                        return [pscustomobject]@{
                            Content = (@{
                                paths = @(
                                    @{ name = '_scope/madev1/users/2026-05-28.jsonl'; isDirectory = $false; contentLength = '90' }
                                )
                            } | ConvertTo-Json -Depth 5)
                            Headers = @{}
                        }
                    }
                    return [pscustomobject]@{
                        Content = '{"_record":{"userPrincipalName":"alice@contoso.com"}}' + "`n" + '{not-json'
                        Headers = @{}
                    }
                }

                {
                    Get-ScopeKeySet `
                        -ScopeRoot '_scope/madev1' `
                        -Dimension 'users' `
                    -KeyField 'userPrincipalName' `
                        -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                        -ContainerName 'landing'
                } | Should -Throw '*not valid JSON*'
            }
        }

        It 'silently skips lines that do not have an _record envelope' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'tok' }
                $script:Calls = 0
                Mock Invoke-WebRequest -MockWith {
                    $script:Calls++
                    if ($script:Calls -eq 1) {
                        return [pscustomobject]@{
                            Content = (@{
                                paths = @(
                                    @{ name = '_scope/madev1/users/2026-05-28.jsonl'; isDirectory = $false; contentLength = '90' }
                                )
                            } | ConvertTo-Json -Depth 5)
                            Headers = @{}
                        }
                    }
                    return [pscustomobject]@{
                        Content = (@(
                            '{"metadata":"header row from populator"}'
                            '{"_record":{"userPrincipalName":"alice@contoso.com"}}'
                        ) -join "`n")
                        Headers = @{}
                    }
                }

                $upns = Get-ScopeKeySet `
                    -ScopeRoot '_scope/madev1' `
                    -Dimension 'users' `
                    -KeyField 'userPrincipalName' `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing'

                $upns.Count | Should -Be 1
                $upns.Contains('alice@contoso.com') | Should -BeTrue
            }
        }
    }

    Context '401 safety net' {
        It 're-mints the ADLS token and retries the list once on a 401' {
            InModuleScope ScopeFilter {
                Mock Get-AdlsAccessToken { 'tok' }
                $script:Calls = 0
                Mock Invoke-WebRequest -MockWith {
                    $script:Calls++
                    if ($script:Calls -eq 1) {
                        # First list attempt: cached token rejected.
                        $resp = [PSCustomObject]@{ StatusCode = 401 }
                        $ex = [System.Exception]::new('Unauthorized')
                        Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                        throw $ex
                    }
                    if ($script:Calls -eq 2) {
                        # Retry after refresh: list succeeds.
                        return [pscustomobject]@{
                            Content = (@{
                                paths = @(
                                    @{ name = '_scope/madev1/users/2026-05-28.jsonl'; isDirectory = $false; contentLength = '50' }
                                )
                            } | ConvertTo-Json -Depth 5)
                            Headers = @{}
                        }
                    }
                    # Read of the blob.
                    return [pscustomobject]@{
                        Content = '{"_record":{"userPrincipalName":"alice@contoso.com"}}'
                        Headers = @{}
                    }
                }

                $upns = Get-ScopeKeySet `
                    -ScopeRoot '_scope/madev1' `
                    -Dimension 'users' `
                    -KeyField 'userPrincipalName' `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing'

                $upns.Count | Should -Be 1
                $upns.Contains('alice@contoso.com') | Should -BeTrue
                # The 401 forces a token refresh before the list retry.
                Should -Invoke Get-AdlsAccessToken -Times 2
            }
        }
    }
}
