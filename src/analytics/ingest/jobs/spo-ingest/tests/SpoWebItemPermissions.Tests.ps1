#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for web_item_perms — per (web, doc-library): enumerate items with
# unique role assignments, then batch GetSharingInformation for ~50 at a time.

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

Describe 'Get-SpoWebItemPermissions' {
    BeforeEach { Initialize-MockConnect }

    It 'client-side-filters items by HasUniqueRoleAssignments then batches GetSharingInformation; preserves nested permissionsInformation' {
        InModuleScope 'SpoSites' {
            # Step 1 — Items query. Rows include the HasUniqueRoleAssignments
            # flag now (the production code dropped the server-side $filter
            # because HasUniqueRoleAssignments is a computed property and SP's
            # OData layer 500s when you put it in $filter — so the client-
            # side filter is part of the contract this test exercises).
            Mock Invoke-SpoRestPaged -MockWith {
                $rows = @(
                    [pscustomobject]@{ Id = 5; FileRef = '/sites/X/Documents/a.docx'; FileSystemObjectType = 0; HasUniqueRoleAssignments = $true }
                    [pscustomobject]@{ Id = 9; FileRef = '/sites/X/Documents/b'; FileSystemObjectType = 1; HasUniqueRoleAssignments = $true }
                    # An item that inherits — must be dropped before the GetSharingInformation batch.
                    [pscustomobject]@{ Id = 11; FileRef = '/sites/X/Documents/c.docx'; FileSystemObjectType = 0; HasUniqueRoleAssignments = $false }
                )
                foreach ($r in $rows) { & $OnRow $r }
                return $rows.Count
            }

            # Step 2 — $batch with GetSharingInformation per item.
            Mock Invoke-SpoBatch -MockWith {
                return @(
                    @{
                        Status = 200
                        Body   = [ordered]@{
                            permissionsInformation = [ordered]@{
                                links      = @(
                                    [ordered]@{ linkDetails = [ordered]@{ Url = 'https://share.x/link-a' } }
                                )
                                principals = @(
                                    [ordered]@{ principal = [ordered]@{ name = 'alice' } }
                                )
                            }
                        }
                    }
                    # Second item: empty permissionsInformation — should be dropped
                    @{
                        Status = 200
                        Body   = [ordered]@{
                            permissionsInformation = [ordered]@{
                                links      = @()
                                principals = @()
                            }
                        }
                    }
                )
            }

            $writer = New-CaptureWriter
            $ctx = New-MockContext -InputTags @{
                SiteUrl      = 'https://xtlab2.sharepoint.com/sites/X'
                WebUrl       = 'https://xtlab2.sharepoint.com/sites/X'
                ListId       = '11111111-1111-1111-1111-111111111111'
                BaseTemplate = 101
                BaseType     = 1
            }
            # InputId is the composite the framework will dispatch (webUrl::listId);
            # production code reads listId from ListId tag, not from InputId.
            Get-SpoWebItemPermissions -InputId 'https://xtlab2.sharepoint.com/sites/X:::11111111-1111-1111-1111-111111111111' -Context $ctx -Writer $writer

            # Only the item with non-empty permissionsInformation lands. The
            # empty one is dropped (no sharing data to record).
            $writer.Records.Count | Should -Be 1
            $r = $writer.Records[0]
            $r.SiteUrl              | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $r.WebUrl               | Should -Be 'https://xtlab2.sharepoint.com/sites/X'
            $r.ListId               | Should -Be '11111111-1111-1111-1111-111111111111'
            $r.ItemId               | Should -Be 5
            $r.FileRef              | Should -Be '/sites/X/Documents/a.docx'
            $r.FileSystemObjectType | Should -Be 0
            $r.permissionsInformation.links[0].linkDetails.Url   | Should -Be 'https://share.x/link-a'
            $r.permissionsInformation.principals[0].principal.name | Should -Be 'alice'
        }
    }

    It 'silently skips items when a batch sub-response is 403/404 and emits item_failed event' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRestPaged -MockWith {
                & $OnRow ([pscustomobject]@{ Id = 5; FileRef = '/sites/X/Documents/a.docx'; FileSystemObjectType = 0; HasUniqueRoleAssignments = $true })
                return 1
            }
            Mock Invoke-SpoBatch -MockWith {
                return ,@(
                    @{ Status = 403; Body = '{"error":{"message":"AccessDenied"}}' }
                )
            }
            Mock Write-ItemFailedEvent -MockWith { }

            $writer = New-CaptureWriter
            $ctx = New-MockContext -InputTags @{
                SiteUrl      = 'https://xtlab2.sharepoint.com/sites/X'
                WebUrl       = 'https://xtlab2.sharepoint.com/sites/X'
                ListId       = '11111111-1111-1111-1111-111111111111'
                BaseTemplate = 101
                BaseType     = 1
            }
            Get-SpoWebItemPermissions -InputId 'https://xtlab2.sharepoint.com/sites/X:::11111111-1111-1111-1111-111111111111' -Context $ctx -Writer $writer

            $writer.Records.Count | Should -Be 0
            Should -Invoke Write-ItemFailedEvent -Times 1 -ParameterFilter {
                $Category -eq 'Skippable' -and
                $StatusCode -eq 403 -and
                $ItemId -like '*/lists/11111111-1111-1111-1111-111111111111/items/5'
            }
        }
    }

    It 'short-circuits when no items have unique role assignments (no $batch call)' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRestPaged -MockWith { return 0 }
            Mock Invoke-SpoBatch -MockWith { throw 'should never be called when items=0' }

            $writer = New-CaptureWriter
            $ctx = New-MockContext -InputTags @{
                SiteUrl      = 'https://xtlab2.sharepoint.com/sites/X'
                WebUrl       = 'https://xtlab2.sharepoint.com/sites/X'
                ListId       = 'list-id'
                BaseTemplate = 101
                BaseType     = 1
            }
            { Get-SpoWebItemPermissions -InputId 'https://xtlab2.sharepoint.com/sites/X:::list-id' -Context $ctx -Writer $writer } |
                Should -Not -Throw
            $writer.Records.Count | Should -Be 0
        }
    }
}
