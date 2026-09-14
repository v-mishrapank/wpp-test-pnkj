#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for the web_lists pool stage — /_api/web/Lists per web, emitting
# (SiteUrl, WebUrl, BaseTemplate, BaseType) IdTags so the web_item_perms
# descendant's InputFilter on BaseType=1 can drop non-document-library lists
# at the dispatch boundary while keeping every doc-library variant (regular
# BaseTemplate=101, OneDrive personal Documents BaseTemplate=700, etc.).

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

Describe 'Get-SpoWebLists' {
    BeforeEach { Initialize-MockConnect }

    It 'lands every list verbatim with FKs and emits IdTags carrying BaseTemplate + BaseType' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRestPaged -MockWith {
                $rows = @(
                    [pscustomobject]@{
                        __metadata     = [pscustomobject]@{ type = 'SP.List' }
                        Id             = '11111111-1111-1111-1111-111111111111'
                        Title          = 'Documents'
                        BaseTemplate   = 101
                        BaseType       = 1
                        Hidden         = $false
                        ItemCount      = 42
                    }
                    [pscustomobject]@{
                        __metadata     = [pscustomobject]@{ type = 'SP.List' }
                        Id             = '22222222-2222-2222-2222-222222222222'
                        Title          = 'System List'
                        BaseTemplate   = 100
                        BaseType       = 0
                        Hidden         = $true
                        ItemCount      = 0
                    }
                    # OneDrive personal Documents library — BaseTemplate=700 is
                    # the SpoPdmpLibrary template (different enum from the
                    # web-template 700/SPSPERS that names the parent web).
                    [pscustomobject]@{
                        __metadata     = [pscustomobject]@{ type = 'SP.List' }
                        Id             = '33333333-3333-3333-3333-333333333333'
                        Title          = 'Documents'
                        BaseTemplate   = 700
                        BaseType       = 1
                        Hidden         = $false
                        ItemCount      = 33
                    }
                )
                foreach ($r in $rows) { & $OnRow $r }
                return $rows.Count
            }

            $writer = New-CaptureWriter
            $ctx = New-MockContext -InputTags @{ SiteUrl = 'https://xtlab2.sharepoint.com/sites/X' }
            Get-SpoWebLists -InputId 'https://xtlab2.sharepoint.com/sites/X' -Context $ctx -Writer $writer

            $writer.Records.Count | Should -Be 3
            $writer.Records[0].Title        | Should -Be 'Documents'
            $writer.Records[0].BaseTemplate | Should -Be 101
            $writer.Records[0].BaseType     | Should -Be 1
            $writer.Records[0].SiteUrl      | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $writer.Records[0].WebUrl       | Should -Be 'https://xtlab2.sharepoint.com/sites/X'

            # All three lists emit IDs. The InputFilter on web_item_perms
            # (BaseType=1) admits doc libs at BaseTemplate=101 AND 700, drops
            # the system list at BaseType=0. Filter runs in StageExecutor, not here.
            #
            # Emitted IDs are composite `<webUrl>::<listId>` so duplicates across
            # webs (OneDrive personal sites all use the same Documents-lib GUID)
            # don't collide in StageExecutor's tag lookup. The real listId is in
            # the ListId tag for the consumer.
            $writer.EmittedIds.Count | Should -Be 3
            $writer.EmittedIds[0].Id              | Should -Be 'https://xtlab2.sharepoint.com/sites/X:::11111111-1111-1111-1111-111111111111'
            $writer.EmittedIds[0].Tags.SiteUrl    | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $writer.EmittedIds[0].Tags.WebUrl     | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $writer.EmittedIds[0].Tags.ListId       | Should -Be '11111111-1111-1111-1111-111111111111'
            $writer.EmittedIds[0].Tags.BaseTemplate | Should -Be 101
            $writer.EmittedIds[0].Tags.BaseType     | Should -Be 1
            $writer.EmittedIds[1].Tags.ListId       | Should -Be '22222222-2222-2222-2222-222222222222'
            $writer.EmittedIds[1].Tags.BaseTemplate | Should -Be 100
            $writer.EmittedIds[1].Tags.BaseType     | Should -Be 0
            $writer.EmittedIds[2].Tags.ListId       | Should -Be '33333333-3333-3333-3333-333333333333'
            $writer.EmittedIds[2].Tags.BaseTemplate | Should -Be 700
            $writer.EmittedIds[2].Tags.BaseType     | Should -Be 1
        }
    }
}

Describe 'Get-ModuleStages.web_item_perms.InputFilter' {
    It 'admits a regular doc lib (BaseTemplate=101, BaseType=1)' {
        InModuleScope 'SpoSites' {
            $stages = Get-ModuleStages
            $filter = $stages['web_item_perms'].InputFilter
            (& $filter @{ BaseTemplate = 101; BaseType = 1 }) | Should -BeTrue
        }
    }
    It 'admits a OneDrive personal Documents library (BaseTemplate=700, BaseType=1)' {
        InModuleScope 'SpoSites' {
            $stages = Get-ModuleStages
            $filter = $stages['web_item_perms'].InputFilter
            (& $filter @{ BaseTemplate = 700; BaseType = 1 }) | Should -BeTrue
        }
    }
    It 'rejects a generic list (BaseType=0)' {
        InModuleScope 'SpoSites' {
            $stages = Get-ModuleStages
            $filter = $stages['web_item_perms'].InputFilter
            (& $filter @{ BaseTemplate = 100; BaseType = 0 }) | Should -BeFalse
        }
    }
    It 'rejects when tags are $null (defensive — would mean parent emitted no tags)' {
        InModuleScope 'SpoSites' {
            $stages = Get-ModuleStages
            $filter = $stages['web_item_perms'].InputFilter
            (& $filter $null) | Should -BeFalse
        }
    }
}
