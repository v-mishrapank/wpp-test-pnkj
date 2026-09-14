#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for the spo_site_details pool stage — per-URL SP admin REST
# GetSitePropertiesByUrl. Two responsibilities being verified:
#   1. Lands the 134-field SiteProperties shape with FK SiteUrl.
#   2. Suppresses descendant fan-out (EmitId) for REDIRECTSITE templates
#      so renamed-site shells don't 403-Skippable through every per-site
#      descendant. See #471.

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

Describe 'Get-SpoSiteDetails — GetSitePropertiesByUrl per-URL fan-out' {
    BeforeEach { Initialize-MockConnect }

    It 'lands the SP REST shape verbatim with FK SiteUrl, emits InputId for a normal site' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRest -MockWith {
                if ($Url -notmatch 'GetSitePropertiesByUrl$') {
                    throw "unexpected URL: $Url"
                }
                # Single-entity verbose shape — Invoke-SpoRest unwraps .d so
                # the mock returns the inner SiteProperties object directly.
                return [pscustomobject]@{
                    __metadata           = [pscustomobject]@{ type = 'Microsoft.Online.SharePoint.TenantAdministration.SiteProperties' }
                    Url                  = 'https://xtlab2.sharepoint.com/sites/A'
                    Title                = 'Site A'
                    Template             = 'GROUP#0'
                    Owner                = 'owner@xtlab2.com'
                    OwnerEmail           = 'owner@xtlab2.com'
                    LockState            = 'Unlock'
                    GroupId              = 'group-guid-1'
                    SharingCapability    = 'ExternalUserAndGuestSharing'
                    StorageMaximumLevel  = 26214400
                    StorageUsage         = 12345
                }
            }

            $writer = New-CaptureWriter
            Get-SpoSiteDetails -InputId 'https://xtlab2.sharepoint.com/sites/A' -Context (New-MockContext) -Writer $writer

            # Row landed verbatim, PascalCase, __metadata stripped, FK SiteUrl added
            $writer.Records.Count | Should -Be 1
            $rec = $writer.Records[0]
            $rec.Url                | Should -Be 'https://xtlab2.sharepoint.com/sites/A'
            $rec.Template           | Should -Be 'GROUP#0'
            $rec.Owner              | Should -Be 'owner@xtlab2.com'
            $rec.LockState          | Should -Be 'Unlock'
            $rec.SharingCapability  | Should -Be 'ExternalUserAndGuestSharing'
            $rec.SiteUrl            | Should -Be 'https://xtlab2.sharepoint.com/sites/A'
            $rec.Contains('__metadata') | Should -BeFalse

            # Non-redirect → InputId emitted for descendant fan-out
            $writer.EmittedIds.Count | Should -Be 1
            $writer.EmittedIds[0].Id | Should -Be 'https://xtlab2.sharepoint.com/sites/A'
        }
    }

    It 'lands the row but SUPPRESSES EmitId when Template is REDIRECTSITE#0 (tenant rename / site move shell)' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRest -MockWith {
                return [pscustomobject]@{
                    __metadata = [pscustomobject]@{ type = 'Microsoft.Online.SharePoint.TenantAdministration.SiteProperties' }
                    Url        = 'https://xtlab2.sharepoint.com/sites/OldName'
                    Title      = 'OldName redirect'
                    Template   = 'REDIRECTSITE#0'
                    LockState  = 'ReadOnly'
                }
            }

            $writer = New-CaptureWriter
            Get-SpoSiteDetails -InputId 'https://xtlab2.sharepoint.com/sites/OldName' -Context (New-MockContext) -Writer $writer

            # Redirect row lands in spo_site_details (for accounting)
            $writer.Records.Count    | Should -Be 1
            $writer.Records[0].Template | Should -Be 'REDIRECTSITE#0'
            $writer.Records[0].SiteUrl  | Should -Be 'https://xtlab2.sharepoint.com/sites/OldName'

            # But NO InputId emitted — descendants don't fire on redirect shells
            $writer.EmittedIds.Count | Should -Be 0
        }
    }

    It 'matches Template prefix case-insensitively (RedirectSite#1 with mixed case)' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRest -MockWith {
                return [pscustomobject]@{
                    __metadata = [pscustomobject]@{ type = 'SP' }
                    Url        = 'https://xtlab2.sharepoint.com/sites/Renamed'
                    Template   = 'RedirectSite#1'   # mixed case variant seen in probe
                }
            }

            $writer = New-CaptureWriter
            Get-SpoSiteDetails -InputId 'https://xtlab2.sharepoint.com/sites/Renamed' -Context (New-MockContext) -Writer $writer

            $writer.Records.Count    | Should -Be 1
            $writer.EmittedIds.Count | Should -Be 0
        }
    }

    It 'trims trailing slash from InputId before calling and emitting' {
        InModuleScope 'SpoSites' {
            Mock Invoke-SpoRest -MockWith {
                return [pscustomobject]@{
                    Url      = 'https://xtlab2.sharepoint.com'
                    Template = 'SITEPAGEPUBLISHING#0'
                }
            }

            $writer = New-CaptureWriter
            # Pass a URL WITH trailing slash (tenant root case)
            Get-SpoSiteDetails -InputId 'https://xtlab2.sharepoint.com/' -Context (New-MockContext) -Writer $writer

            # FK SiteUrl is the trimmed input
            $writer.Records[0].SiteUrl  | Should -Be 'https://xtlab2.sharepoint.com'
            $writer.EmittedIds.Count    | Should -Be 1
            $writer.EmittedIds[0].Id    | Should -Be 'https://xtlab2.sharepoint.com'
        }
    }
}

