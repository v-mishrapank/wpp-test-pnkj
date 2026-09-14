#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for the web_role_defs pool stage — /_api/web/RoleDefinitions per web.

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

Describe 'Get-SpoWebRoleDefinitions' {
    BeforeEach { Initialize-MockConnect }

    It 'preserves BasePermissions/RoleTypeKind verbatim and adds SiteUrl + WebUrl FKs' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRestPaged -MockWith {
                $rows = @(
                    [pscustomobject]@{
                        __metadata     = [pscustomobject]@{ type = 'SP.RoleDefinition' }
                        Id             = 1073741826
                        Name           = 'Full Control'
                        Description    = ''
                        Hidden         = $false
                        Order          = 32
                        RoleTypeKind   = 5
                        BasePermissions = [pscustomobject]@{
                            __metadata = [pscustomobject]@{ type = 'SP.BasePermissions' }
                            High = '2147483647'
                            Low  = '4294967295'
                        }
                    }
                )
                foreach ($r in $rows) { & $OnRow $r }
                return $rows.Count
            }

            $writer = New-CaptureWriter
            $ctx = New-MockContext -InputTags @{ SiteUrl = 'https://xtlab2.sharepoint.com/sites/X' }
            Get-SpoWebRoleDefinitions -InputId 'https://xtlab2.sharepoint.com/sites/X/subweb1' -Context $ctx -Writer $writer

            $writer.Records.Count | Should -Be 1
            $rd = $writer.Records[0]
            $rd.Id                      | Should -Be 1073741826
            $rd.Name                    | Should -Be 'Full Control'
            $rd.RoleTypeKind            | Should -Be 5
            $rd.BasePermissions.High    | Should -Be '2147483647'
            $rd.BasePermissions.Low     | Should -Be '4294967295'
            $rd.SiteUrl                 | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $rd.WebUrl                  | Should -Be 'https://xtlab2.sharepoint.com/sites/X/subweb1'
            # __metadata stripped from BasePermissions and from the root.
            $rd.Contains('__metadata') | Should -BeFalse
            $rd.BasePermissions.Contains('__metadata') | Should -BeFalse
        }
    }

    It 'falls back to InputId as SiteUrl when no InputTags supplied (defensive)' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRestPaged -MockWith {
                & $OnRow ([pscustomobject]@{ Id = 1; Name = 'r' })
                return 1
            }
            $writer = New-CaptureWriter
            $ctx = New-MockContext  # no InputTags
            Get-SpoWebRoleDefinitions -InputId 'https://xtlab2.sharepoint.com/sites/X' -Context $ctx -Writer $writer
            $writer.Records[0].SiteUrl | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
        }
    }
}
