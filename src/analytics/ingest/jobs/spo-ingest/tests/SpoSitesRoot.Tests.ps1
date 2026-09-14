#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for the spo_sites_root inline stage — Graph /v1.0/sites/getAllSites
# enumeration (rewritten in #471).
#
# Prior shape used SP admin REST GetSitePropertiesFromSharePointByFilters and
# tested NextStartIndexFromSharePoint pagination + RedirectSite#1 EmitId skip.
# Both moved: Graph paginates via @odata.nextLink, and the RedirectSite filter
# moved to spo_site_details (which now sees the Template field that Graph
# doesn't surface). See SpoSiteDetails.Tests.ps1 for that test.

BeforeAll {
    $script:SharedModules = Join-Path $PSScriptRoot '..' '..' 'shared' 'modules'
    $script:Scripts       = Join-Path $PSScriptRoot '..' 'scripts'
    Import-Module (Join-Path $script:SharedModules 'LogHelper.psm1')         -Force
    Import-Module (Join-Path $script:SharedModules 'EventEmitter.psm1')      -Force
    Import-Module (Join-Path $script:SharedModules 'RetryHelper.psm1')       -Force
    Import-Module (Join-Path $script:SharedModules 'MsalTokenHelper.psm1')   -Force
    Import-Module (Join-Path $script:SharedModules 'StorageHelperRest.psm1') -Force
    Import-Module (Join-Path $script:SharedModules 'ScopeFilter.psm1')       -Force
    Import-Module (Join-Path $script:Scripts 'SpoRestClient.psm1')           -Force -DisableNameChecking
    Import-Module (Join-Path $script:Scripts 'Connect.psm1')                 -Force -DisableNameChecking
    Import-Module (Join-Path $PSScriptRoot '_TestHelpers.psm1')              -Force
    Import-Module (Join-Path $script:Scripts 'entities' 'SpoSites.psm1')     -Force -DisableNameChecking
}

AfterAll {
    Remove-Module SpoSites, _TestHelpers, Connect, SpoRestClient, ScopeFilter, StorageHelperRest, MsalTokenHelper, RetryHelper, EventEmitter, LogHelper -Force -ErrorAction SilentlyContinue
    Remove-Item Env:SCOPE_ROOT -ErrorAction SilentlyContinue
    Remove-Item Env:STORAGE_ACCOUNT_URL -ErrorAction SilentlyContinue
    Remove-Item Env:LANDING_CONTAINER -ErrorAction SilentlyContinue
}

Describe 'Get-SpoSitesRoot — Graph getAllSites enumeration' {
    BeforeEach {
        $global:SpoTestState = @{ Pages = 0 }
        Initialize-MockConnect
    }
    AfterEach  { Remove-Variable -Scope Global -Name SpoTestState -ErrorAction SilentlyContinue }

    It 'paginates via @odata.nextLink, lands every site verbatim, emits webUrl as InputId for every site' {
        InModuleScope 'SpoSites' {
            Mock Invoke-RestMethod -MockWith {
                $global:SpoTestState.Pages++
                if ($global:SpoTestState.Pages -eq 1) {
                    return [pscustomobject]@{
                        value = @(
                            [pscustomobject]@{
                                id                   = 'xtlab2.sharepoint.com,site-A-guid,web-A-guid'
                                name                 = 'A'
                                displayName          = 'Site A'
                                webUrl               = 'https://xtlab2.sharepoint.com/sites/A'
                                description          = 'A team'
                                createdDateTime      = '2024-01-01T00:00:00Z'
                                lastModifiedDateTime = '2024-06-01T00:00:00Z'
                                isPersonalSite       = $false
                            }
                            [pscustomobject]@{
                                id                   = 'xtlab2-my.sharepoint.com,site-B-guid,web-B-guid'
                                name                 = 'personal/user_xtlab2'
                                displayName          = "User's OneDrive"
                                webUrl               = 'https://xtlab2-my.sharepoint.com/personal/user_xtlab2'
                                isPersonalSite       = $true
                            }
                        )
                        '@odata.nextLink' = 'https://graph.microsoft.com/v1.0/sites/getAllSites?$skiptoken=fake-token-1'
                    }
                }
                # Page 2 — final page, no nextLink, tenant-root with trailing slash
                return [pscustomobject]@{
                    value = @(
                        [pscustomobject]@{
                            id     = 'xtlab2.sharepoint.com,root-guid,root-web-guid'
                            name   = ''
                            displayName = 'Root'
                            webUrl = 'https://xtlab2.sharepoint.com/'  # tenant root — trailing slash
                            isPersonalSite = $false
                        }
                    )
                }
            }

            $writer = New-CaptureWriter
            $ctx = New-MockContext
            Get-SpoSitesRoot -Context $ctx -Writer $writer

            # 3 rows landed verbatim — Graph camelCase, no filter at this stage
            $writer.Records.Count | Should -Be 3
            $writer.Records[0].webUrl         | Should -Be 'https://xtlab2.sharepoint.com/sites/A'
            $writer.Records[0].displayName    | Should -Be 'Site A'
            $writer.Records[0].isPersonalSite | Should -Be $false
            $writer.Records[1].webUrl         | Should -Be 'https://xtlab2-my.sharepoint.com/personal/user_xtlab2'
            $writer.Records[1].isPersonalSite | Should -Be $true

            # Tenant-root URL: trailing slash normalized off both in the record AND the InputId
            $writer.Records[2].webUrl | Should -Be 'https://xtlab2.sharepoint.com'

            # EVERY site emits an InputId — RedirectSite filter moved to site_details
            $writer.EmittedIds.Count | Should -Be 3
            $emittedUrls = @($writer.EmittedIds | ForEach-Object { $_.Id })
            $emittedUrls | Should -Contain 'https://xtlab2.sharepoint.com/sites/A'
            $emittedUrls | Should -Contain 'https://xtlab2-my.sharepoint.com/personal/user_xtlab2'
            $emittedUrls | Should -Contain 'https://xtlab2.sharepoint.com'

            # Two GET requests to Graph
            $global:SpoTestState.Pages | Should -Be 2
        }
    }

    It 'stops when @odata.nextLink is absent on the first page' {
        InModuleScope 'SpoSites' {
            Mock Invoke-RestMethod -MockWith {
                $global:SpoTestState.Pages++
                return [pscustomobject]@{
                    value = @(
                        [pscustomobject]@{ webUrl = 'https://xtlab2.sharepoint.com/sites/A'; name = 'A'; isPersonalSite = $false }
                    )
                }
            }

            $writer = New-CaptureWriter
            $ctx = New-MockContext
            Get-SpoSitesRoot -Context $ctx -Writer $writer

            $global:SpoTestState.Pages   | Should -Be 1
            $writer.Records.Count        | Should -Be 1
            $writer.EmittedIds.Count     | Should -Be 1
        }
    }

    It 'skips rows with null/empty webUrl' {
        InModuleScope 'SpoSites' {
            Mock Invoke-RestMethod -MockWith {
                $global:SpoTestState.Pages++
                return [pscustomobject]@{
                    value = @(
                        [pscustomobject]@{ webUrl = $null;   name = 'broken-row-1' }
                        [pscustomobject]@{ webUrl = '';      name = 'broken-row-2' }
                        [pscustomobject]@{ webUrl = 'https://xtlab2.sharepoint.com/sites/A'; name = 'A' }
                    )
                }
            }

            $writer = New-CaptureWriter
            $ctx = New-MockContext
            Get-SpoSitesRoot -Context $ctx -Writer $writer

            # Only the valid row should land
            $writer.Records.Count    | Should -Be 1
            $writer.EmittedIds.Count | Should -Be 1
            $writer.EmittedIds[0].Id | Should -Be 'https://xtlab2.sharepoint.com/sites/A'
        }
    }
}

Describe 'Get-SpoSitesRoot — scope filter (issue #260)' {
    BeforeEach {
        $global:SpoTestState = @{ Pages = 0 }
        Initialize-MockConnect
        $env:SCOPE_ROOT         = '_scope/madev1'
        $env:STORAGE_ACCOUNT_URL = 'https://acct.dfs.core.windows.net'
        $env:LANDING_CONTAINER   = 'landing'
    }
    AfterEach {
        Remove-Variable -Scope Global -Name SpoTestState -ErrorAction SilentlyContinue
        Remove-Item Env:SCOPE_ROOT -ErrorAction SilentlyContinue
        Remove-Item Env:STORAGE_ACCOUNT_URL -ErrorAction SilentlyContinue
        Remove-Item Env:LANDING_CONTAINER -ErrorAction SilentlyContinue
    }

    # Test helpers — produce the four Get-Scope return shapes for SpoSites's
    # users/sites dimension reads. Mocks dispatch by -Dimension parameter.
    function script:New-SiteSample {
        return [pscustomobject]@{
            value = @(
                [pscustomobject]@{ webUrl = 'https://xtlab2.sharepoint.com/sites/Marketing'; name = 'Marketing'; isPersonalSite = $false }
                [pscustomobject]@{ webUrl = 'https://xtlab2.sharepoint.com/sites/HR';        name = 'HR';        isPersonalSite = $false }
                [pscustomobject]@{ webUrl = 'https://xtlab2-my.sharepoint.com/personal/alice_xtlab2_onmicrosoft_com'; name = 'alice'; isPersonalSite = $true }
                [pscustomobject]@{ webUrl = 'https://xtlab2-my.sharepoint.com/personal/bob_xtlab2_onmicrosoft_com';   name = 'bob';   isPersonalSite = $true }
                [pscustomobject]@{ webUrl = 'https://xtlab2-my.sharepoint.com/personal/carol_xtlab2_onmicrosoft_com'; name = 'carol'; isPersonalSite = $true }
            )
        }
    }

    It 'both scopes present: personal sites filtered by users, team sites filtered by sites' {
        InModuleScope 'SpoSites' {
            Mock Get-ScopeKeySet -MockWith {
                $set = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
                if ($Dimension -eq 'users') {
                    [void]$set.Add('alice@xtlab2.onmicrosoft.com')
                    [void]$set.Add('bob@xtlab2.onmicrosoft.com')
                } else {
                    [void]$set.Add('https://xtlab2.sharepoint.com/sites/Marketing')
                }
                return $set
            }
            Mock Invoke-RestMethod -MockWith { New-SiteSample }

            $writer = New-CaptureWriter
            Get-SpoSitesRoot -Context (New-MockContext) -Writer $writer

            $emittedUrls = @($writer.EmittedIds | ForEach-Object { $_.Id })
            # 3 emitted: Marketing (team-in-scope), alice + bob (personal-in-scope)
            $writer.Records.Count | Should -Be 3
            $emittedUrls | Should -Contain 'https://xtlab2.sharepoint.com/sites/Marketing'
            $emittedUrls | Should -Contain 'https://xtlab2-my.sharepoint.com/personal/alice_xtlab2_onmicrosoft_com'
            $emittedUrls | Should -Contain 'https://xtlab2-my.sharepoint.com/personal/bob_xtlab2_onmicrosoft_com'
            # Filtered out: HR (team not in sites scope), carol (personal not in users scope)
            $emittedUrls | Should -Not -Contain 'https://xtlab2.sharepoint.com/sites/HR'
            $emittedUrls | Should -Not -Contain 'https://xtlab2-my.sharepoint.com/personal/carol_xtlab2_onmicrosoft_com'
        }
    }

    It 'users scope only: all team sites skipped, personal sites filtered by users' {
        InModuleScope 'SpoSites' {
            Mock Get-ScopeKeySet -MockWith {
                if ($Dimension -eq 'users') {
                    $set = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
                    [void]$set.Add('alice@xtlab2.onmicrosoft.com')
                    return $set
                }
                return $null
            }
            Mock Invoke-RestMethod -MockWith { New-SiteSample }

            $writer = New-CaptureWriter
            Get-SpoSitesRoot -Context (New-MockContext) -Writer $writer

            $emittedUrls = @($writer.EmittedIds | ForEach-Object { $_.Id })
            # Only alice's personal site — no team sites, no other personal sites
            $writer.Records.Count | Should -Be 1
            $emittedUrls | Should -Contain 'https://xtlab2-my.sharepoint.com/personal/alice_xtlab2_onmicrosoft_com'
        }
    }

    It 'sites scope only: all personal sites skipped, team sites filtered by sites' {
        InModuleScope 'SpoSites' {
            Mock Get-ScopeKeySet -MockWith {
                if ($Dimension -eq 'sites') {
                    $set = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
                    [void]$set.Add('https://xtlab2.sharepoint.com/sites/Marketing')
                    return $set
                }
                return $null
            }
            Mock Invoke-RestMethod -MockWith { New-SiteSample }

            $writer = New-CaptureWriter
            Get-SpoSitesRoot -Context (New-MockContext) -Writer $writer

            $emittedUrls = @($writer.EmittedIds | ForEach-Object { $_.Id })
            # Only Marketing — no personal sites at all
            $writer.Records.Count | Should -Be 1
            $emittedUrls | Should -Contain 'https://xtlab2.sharepoint.com/sites/Marketing'
        }
    }

    It 'neither scope present: root is a no-op, never hits Graph' {
        InModuleScope 'SpoSites' {
            Mock Get-ScopeKeySet { return $null }
            $script:RestCalled = $false
            Mock Invoke-RestMethod -MockWith {
                $script:RestCalled = $true
                throw 'should not be called'
            }

            $writer = New-CaptureWriter
            Get-SpoSitesRoot -Context (New-MockContext) -Writer $writer

            $writer.Records.Count    | Should -Be 0
            $writer.EmittedIds.Count | Should -Be 0
            $script:RestCalled       | Should -BeFalse
        }
    }

    It 'throws when Hosts.My is missing but users scope is present' {
        InModuleScope 'SpoSites' {
            Mock Get-ScopeKeySet -MockWith {
                if ($Dimension -eq 'users') {
                    $set = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
                    [void]$set.Add('alice@contoso.com')
                    return $set
                }
                return $null
            }

            $writer = New-CaptureWriter
            # AuthConfig without Hosts — simulates a broken Connect-Service
            # return that didn't populate the host triple. Get-SpoSitesRoot
            # should fail loudly, not silently mis-derive personal-site URLs.
            $ctx = New-MockContext -AuthConfig @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            }

            { Get-SpoSitesRoot -Context $ctx -Writer $writer } | Should -Throw '*Hosts.My*'
        }
    }
}
