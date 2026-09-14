#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for the site_users pool stage — /_api/web/SiteUsers per site collection.

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

Describe 'Get-SpoSiteUsers — landed rows + FK columns' {
    BeforeEach { Initialize-MockConnect }

    It 'lands every user verbatim with SiteUrl FK appended, __metadata stripped' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRestPaged -MockWith {
                # Replay two user rows through the supplied -OnRow callback.
                $rows = @(
                    [pscustomobject]@{
                        __metadata     = [pscustomobject]@{ type = 'SP.User' }
                        Id             = 7
                        LoginName      = 'i:0#.f|membership|alice@xtlab2.com'
                        Title          = 'Alice'
                        Email          = 'alice@xtlab2.com'
                        IsSiteAdmin    = $true
                        PrincipalType  = 1
                    }
                    [pscustomobject]@{
                        __metadata     = [pscustomobject]@{ type = 'SP.User' }
                        Id             = 8
                        LoginName      = 'i:0#.f|membership|bob@xtlab2.com'
                        Title          = 'Bob'
                        Email          = 'bob@xtlab2.com'
                        IsSiteAdmin    = $false
                        PrincipalType  = 1
                    }
                )
                foreach ($row in $rows) { & $OnRow $row }
                return $rows.Count
            }

            $writer = New-CaptureWriter
            $ctx = New-MockContext
            Get-SpoSiteUsers -InputId 'https://xtlab2.sharepoint.com/sites/X' -Context $ctx -Writer $writer

            $writer.Records.Count | Should -Be 2

            $alice = $writer.Records[0]
            $alice.LoginName   | Should -Be 'i:0#.f|membership|alice@xtlab2.com'
            $alice.Title       | Should -Be 'Alice'
            $alice.IsSiteAdmin | Should -BeTrue
            $alice.SiteUrl     | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $alice.Contains('__metadata') | Should -BeFalse

            $writer.Records[1].SiteUrl | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
        }
    }

    It 'trims trailing slash from InputId before composing the URL' {
        InModuleScope 'SpoSites' {
            $script:lastUrl = $null
            Mock Invoke-SpoRestPaged -MockWith {
                $script:lastUrl = $Url
                return 0
            }
            $writer = New-CaptureWriter
            Get-SpoSiteUsers -InputId 'https://xtlab2.sharepoint.com/sites/X/' -Context (New-MockContext) -Writer $writer
            $script:lastUrl | Should -Be 'https://xtlab2.sharepoint.com/sites/X/_api/web/SiteUsers'
        }
    }
}

