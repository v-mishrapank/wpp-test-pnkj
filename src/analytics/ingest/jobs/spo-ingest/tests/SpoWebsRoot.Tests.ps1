#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for the webs_root pool stage — recursive /_api/web/Webs walk per
# site collection. Each row carries SiteUrl + ParentWebUrl + IsRootWeb FKs.

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

Describe 'Get-SpoWebsRoot — recursive subweb walk' {
    BeforeEach { Initialize-MockConnect }

    It 'emits the root web plus every subweb with correct FKs and IDs' {
        InModuleScope 'SpoSites' {
            # The fetcher uses two different SpoRestClient entrypoints:
            #   /sites/X/_api/web           via Invoke-SpoRest      (single object)
            #   /sites/X/.../Webs           via Invoke-SpoRestPaged (OnRow callback)
            # The paged variant was introduced so a web with more subwebs than
            # fit on one SP REST page doesn't silently lose descendants.
            Mock Invoke-SpoRest -MockWith {
                if ($Url -match '/sites/X/_api/web$') {
                    return [pscustomobject]@{
                        __metadata    = [pscustomobject]@{ type = 'SP.Web' }
                        Url           = 'https://xtlab2.sharepoint.com/sites/X'
                        Title         = 'Root'
                        Created       = '2024-01-01T00:00:00Z'
                        WebTemplate   = 'GROUP'
                    }
                }
                throw "unexpected Invoke-SpoRest URL: $Url"
            }
            Mock Invoke-SpoRestPaged -MockWith {
                if ($Url -match '/sites/X/_api/web/Webs$') {
                    $child = [pscustomobject]@{
                        __metadata = [pscustomobject]@{ type = 'SP.Web' }
                        Url        = 'https://xtlab2.sharepoint.com/sites/X/subweb1'
                        Title      = 'Sub 1'
                    }
                    & $OnRow $child
                    return 1
                }
                if ($Url -match '/sites/X/subweb1/_api/web/Webs$') {
                    return 0
                }
                throw "unexpected Invoke-SpoRestPaged URL: $Url"
            }

            $writer = New-CaptureWriter
            Get-SpoWebsRoot -InputId 'https://xtlab2.sharepoint.com/sites/X' -Context (New-MockContext) -Writer $writer

            # 2 webs: root + 1 subweb.
            $writer.Records.Count | Should -Be 2

            $root = $writer.Records[0]
            $root.Url           | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $root.Title         | Should -Be 'Root'
            $root.SiteUrl       | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $root.ParentWebUrl  | Should -BeNullOrEmpty
            $root.IsRootWeb     | Should -BeTrue
            $root.Contains('__metadata') | Should -BeFalse

            $sub = $writer.Records[1]
            $sub.Url           | Should -Be 'https://xtlab2.sharepoint.com/sites/X/subweb1'
            $sub.SiteUrl       | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $sub.ParentWebUrl  | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $sub.IsRootWeb     | Should -BeFalse

            # 2 EmitIds, with SiteUrl tag attached to each.
            $writer.EmittedIds.Count | Should -Be 2
            $writer.EmittedIds[0].Id | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $writer.EmittedIds[0].Tags.SiteUrl | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $writer.EmittedIds[1].Id | Should -Be 'https://xtlab2.sharepoint.com/sites/X/subweb1'
            $writer.EmittedIds[1].Tags.SiteUrl | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
        }
    }
}
