#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for the web_role_assignments pool stage — two REST calls per web
# ($expand=Member, $expand=RoleDefinitionBindings) zipped on PrincipalId.

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

Describe 'Get-SpoWebRoleAssignments — zip Member + RoleDefinitionBindings' {
    BeforeEach { Initialize-MockConnect }

    It 'merges Member (call 1) + RoleDefinitionBindings (call 2) on PrincipalId; keeps both nested' {
        InModuleScope 'SpoSites' {
            # The fetcher hits two URLs in order, then zips on PrincipalId.
            Mock Invoke-SpoRestPaged -MockWith {
                if ($Url -match '\$expand=Member$') {
                    & $OnRow ([pscustomobject]@{
                        __metadata  = [pscustomobject]@{ type = 'SP.RoleAssignment' }
                        PrincipalId = 7
                        Member      = [pscustomobject]@{
                            __metadata    = [pscustomobject]@{ type = 'SP.Group' }
                            Id            = 7
                            LoginName     = 'g7'
                            Title         = 'Owners'
                            PrincipalType = 8
                        }
                    })
                    & $OnRow ([pscustomobject]@{
                        __metadata  = [pscustomobject]@{ type = 'SP.RoleAssignment' }
                        PrincipalId = 8
                        Member      = [pscustomobject]@{
                            Id = 8; LoginName = 'g8'; Title = 'Members'; PrincipalType = 8
                        }
                    })
                    return 2
                }
                if ($Url -match '\$expand=RoleDefinitionBindings$') {
                    & $OnRow ([pscustomobject]@{
                        PrincipalId = 7
                        RoleDefinitionBindings = [pscustomobject]@{
                            __metadata = [pscustomobject]@{ type = 'Collection(SP.RoleDefinition)' }
                            results    = @(
                                [pscustomobject]@{ Id = 1073741826; Name = 'Full Control' }
                            )
                        }
                    })
                    & $OnRow ([pscustomobject]@{
                        PrincipalId = 8
                        RoleDefinitionBindings = [pscustomobject]@{
                            __metadata = [pscustomobject]@{ type = 'Collection(SP.RoleDefinition)' }
                            results    = @(
                                [pscustomobject]@{ Id = 1073741827; Name = 'Edit' }
                                [pscustomobject]@{ Id = 1073741828; Name = 'Read' }
                            )
                        }
                    })
                    return 2
                }
                throw "unexpected URL: $Url"
            }

            $writer = New-CaptureWriter
            $ctx = New-MockContext -InputTags @{ SiteUrl = 'https://xtlab2.sharepoint.com/sites/X' }
            Get-SpoWebRoleAssignments -InputId 'https://xtlab2.sharepoint.com/sites/X' -Context $ctx -Writer $writer

            $writer.Records.Count | Should -Be 2

            $owners = $writer.Records | Where-Object { $_.PrincipalId -eq 7 }
            $owners.Member.LoginName | Should -Be 'g7'
            $owners.Member.Title     | Should -Be 'Owners'
            @($owners.RoleDefinitionBindings).Count | Should -Be 1
            $owners.RoleDefinitionBindings[0].Name | Should -Be 'Full Control'
            $owners.SiteUrl | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $owners.WebUrl  | Should -Be 'https://xtlab2.sharepoint.com/sites/X'

            $members = $writer.Records | Where-Object { $_.PrincipalId -eq 8 }
            $members.Member.LoginName | Should -Be 'g8'
            @($members.RoleDefinitionBindings).Count | Should -Be 2
            $members.RoleDefinitionBindings[1].Name | Should -Be 'Read'
        }
    }
}
