#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for the site_groups pool stage — /_api/web/SiteGroups?$expand=Users
# with Users nested as a flat array (verbose-array wrapper unwrapped).

BeforeAll {
    $script:SharedModules = Join-Path $PSScriptRoot '..' '..' 'shared' 'modules'
    $script:Scripts       = Join-Path $PSScriptRoot '..' 'scripts'
    Import-Module (Join-Path $script:SharedModules 'LogHelper.psm1')       -Force
    Import-Module (Join-Path $script:SharedModules 'EventEmitter.psm1')    -Force
    Import-Module (Join-Path $script:SharedModules 'RetryHelper.psm1')     -Force
    Import-Module (Join-Path $script:SharedModules 'MsalTokenHelper.psm1') -Force
    Import-Module (Join-Path $script:Scripts 'SpoRestClient.psm1')         -Force -DisableNameChecking
    Import-Module (Join-Path $script:Scripts 'Connect.psm1')               -Force -DisableNameChecking
    Import-Module (Join-Path $PSScriptRoot '_TestHelpers.psm1')            -Force
    Import-Module (Join-Path $script:Scripts 'entities' 'SpoSites.psm1')   -Force -DisableNameChecking
}

AfterAll {
    Remove-Module SpoSites, _TestHelpers, Connect, SpoRestClient, MsalTokenHelper, RetryHelper, EventEmitter, LogHelper -Force -ErrorAction SilentlyContinue
}

Describe 'Get-SpoSiteGroups — nested Users array preserved' {
    BeforeEach { Initialize-MockConnect }

    It 'flattens the verbose-array Users wrapper to a plain array of user objects' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRestPaged -MockWith {
                # One SP group with two members in the verbose-array shape.
                $row = [pscustomobject]@{
                    __metadata = [pscustomobject]@{ type = 'SP.Group' }
                    Id         = 5
                    Title      = 'Site Members'
                    OwnerTitle = 'owner-a'
                    AllowMembersEditMembership = $true
                    Users = [pscustomobject]@{
                        __metadata = [pscustomobject]@{ type = 'Collection(SP.User)' }
                        results    = @(
                            [pscustomobject]@{
                                __metadata = [pscustomobject]@{ type = 'SP.User' }
                                Id = 7; LoginName = 'a@x'; Title = 'Alice'
                            }
                            [pscustomobject]@{
                                __metadata = [pscustomobject]@{ type = 'SP.User' }
                                Id = 8; LoginName = 'b@x'; Title = 'Bob'
                            }
                        )
                    }
                }
                & $OnRow $row
                return 1
            }

            $writer = New-CaptureWriter
            Get-SpoSiteGroups -InputId 'https://xtlab2.sharepoint.com/sites/X' -Context (New-MockContext) -Writer $writer

            $writer.Records.Count | Should -Be 1
            $g = $writer.Records[0]
            $g.Id        | Should -Be 5
            $g.Title     | Should -Be 'Site Members'
            $g.SiteUrl   | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $g.Contains('__metadata') | Should -BeFalse

            # Users is a flat array of 2 cleaned objects (no __metadata, no `results` wrapper).
            $users = @($g.Users)
            $users.Count       | Should -Be 2
            $users[0].LoginName | Should -Be 'a@x'
            $users[0].Title     | Should -Be 'Alice'
            $users[0].Contains('__metadata') | Should -BeFalse
            $users[1].LoginName | Should -Be 'b@x'
        }
    }
}

