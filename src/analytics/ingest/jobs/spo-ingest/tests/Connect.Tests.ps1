#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Unit tests for Connect.psm1 — the four-audience token cache that replaces
# PnP's per-site Connect-PnPOnline. Four audiences: main site, OneDrive (-my),
# tenant admin, and Graph (added in #471 for /sites/getAllSites enumeration).

BeforeAll {
    $script:SharedModules = Join-Path $PSScriptRoot '..' '..' 'shared' 'modules'
    $script:Scripts       = Join-Path $PSScriptRoot '..' 'scripts'
    Import-Module (Join-Path $script:SharedModules 'LogHelper.psm1')        -Force
    Import-Module (Join-Path $script:SharedModules 'EventEmitter.psm1')     -Force
    Import-Module (Join-Path $script:SharedModules 'RetryHelper.psm1')      -Force
    Import-Module (Join-Path $script:SharedModules 'MsalTokenHelper.psm1')  -Force
    Import-Module (Join-Path $script:Scripts 'SpoRestClient.psm1')          -Force -DisableNameChecking
    Import-Module (Join-Path $script:Scripts 'Connect.psm1')                -Force -DisableNameChecking
}

AfterAll {
    Remove-Module Connect, SpoRestClient, MsalTokenHelper, RetryHelper, EventEmitter, LogHelper -Force -ErrorAction SilentlyContinue
}

# Helper — build a fake JWT with the given exp seconds-from-now. Connect.psm1
# parses .exp out of the payload to compute the cache TTL.
#
# Defined in global scope so Pester's Mock scriptblocks (which run in the
# InModuleScope context for the Connect module) can resolve it.
function global:New-FakeJwt {
    param([int]$ExpSecondsFromNow = 3600)
    $exp = [DateTimeOffset]::UtcNow.AddSeconds($ExpSecondsFromNow).ToUnixTimeSeconds()
    $header  = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"alg":"RS256","typ":"JWT"}')).TrimEnd('=').Replace('+','-').Replace('/','_')
    $payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("{`"exp`":$exp}")).TrimEnd('=').Replace('+','-').Replace('/','_')
    return "$header.$payload.sig-placeholder"
}

# Helper — Invoke-RestMethod mock that routes by URL. Connect-Service's sanity
# check makes TWO probes: SP admin /_api/contextinfo and Graph /v1.0/sites?$top=1.
# Each test that exercises Connect-Service needs both probes to succeed (or
# fail deterministically, for the throw cases).
function global:Get-SanityCheckResponseForUri {
    param([string]$Uri)
    if ($Uri -match 'graph\.microsoft\.com') {
        return [pscustomobject]@{
            value = @([pscustomobject]@{ id = 'xtlab2.sharepoint.com,root-guid,web-guid' })
        }
    }
    if ($Uri -match '/_api/contextinfo') {
        return [pscustomobject]@{
            d = [pscustomobject]@{ GetContextWebInformation = [pscustomobject]@{ WebFullUrl = 'https://xtlab2-admin.sharepoint.com' } }
        }
    }
    throw "unexpected URI in sanity-check mock: $Uri"
}

Describe 'Get-JwtExpiry — JWT exp claim parser' {
    It 'returns the UTC DateTime corresponding to the exp claim' {
        InModuleScope 'Connect' {
            $now = [DateTimeOffset]::UtcNow.AddSeconds(3600).ToUnixTimeSeconds()
            $payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("{`"exp`":$now}")).TrimEnd('=').Replace('+','-').Replace('/','_')
            $jwt = "header.$payload.sig"
            $expiry = Get-JwtExpiry -Token $jwt
            ($expiry - [DateTime]::UtcNow).TotalSeconds | Should -BeGreaterThan 3500
            ($expiry - [DateTime]::UtcNow).TotalSeconds | Should -BeLessThan 3700
        }
    }

    It 'returns DateTime.MaxValue for a malformed token (opaque tokens still cache to expiry)' {
        InModuleScope 'Connect' {
            Get-JwtExpiry -Token 'not-a-jwt' | Should -Be ([DateTime]::MaxValue)
        }
    }
}

Describe 'Connect-Service — 4-audience setup + sanity check' {
    BeforeEach {
        $global:SpoConnectState = @{ MintedAudiences = @() }
    }
    AfterEach {
        Remove-Variable -Scope Global -Name SpoConnectState -ErrorAction SilentlyContinue
    }

    It 'parses tenant prefix from AdminUrl and configures 4 audiences keyed by host (incl. Graph)' {
        InModuleScope 'Connect' {
            Mock Get-IngestAccessToken -MockWith {
                $global:SpoConnectState.MintedAudiences += $Audience
                New-FakeJwt -ExpSecondsFromNow 3600
            }
            Mock Invoke-RestMethod -MockWith { Get-SanityCheckResponseForUri -Uri $Uri }

            $cfg = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                Organization      = 'xtlab2.onmicrosoft.com'
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            }
            $out = Connect-Service -Config $cfg
            $out.AuthConfig.TenantPrefix | Should -Be 'xtlab2'
            $out.AuthConfig.Hosts.Main   | Should -Be 'xtlab2.sharepoint.com'
            $out.AuthConfig.Hosts.My     | Should -Be 'xtlab2-my.sharepoint.com'
            $out.AuthConfig.Hosts.Admin  | Should -Be 'xtlab2-admin.sharepoint.com'
            $out.AuthConfig.Hosts.Graph  | Should -Be 'graph.microsoft.com'
            $out.AuthConfig.Audiences['xtlab2.sharepoint.com']        | Should -Be 'https://xtlab2.sharepoint.com/.default'
            $out.AuthConfig.Audiences['xtlab2-my.sharepoint.com']     | Should -Be 'https://xtlab2-my.sharepoint.com/.default'
            $out.AuthConfig.Audiences['xtlab2-admin.sharepoint.com']  | Should -Be 'https://xtlab2-admin.sharepoint.com/.default'
            $out.AuthConfig.Audiences['graph.microsoft.com']          | Should -Be 'https://graph.microsoft.com/.default'

            # Both admin and graph tokens were minted during sanity check
            $global:SpoConnectState.MintedAudiences.Count | Should -Be 2
            ($global:SpoConnectState.MintedAudiences -join ',') | Should -Match 'xtlab2-admin'
            ($global:SpoConnectState.MintedAudiences -join ',') | Should -Match 'graph\.microsoft\.com'
        }
    }

    It 'throws if AdminUrl is not a *-admin.sharepoint.com host' {
        InModuleScope 'Connect' {
            $cfg = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                Organization      = 'x.com'
                AdminUrl          = 'https://not-admin.example.com'
            }
            { Connect-Service -Config $cfg } | Should -Throw "*does not match the expected*"
        }
    }

    It 'throws if the SP admin sanity check fails' {
        InModuleScope 'Connect' {
            # Probes now run through Invoke-WithRetry; a persistent failure
            # classifies Unknown and retries with backoff. Mock the sleep in
            # RetryHelper's scope so the retry loop is instant.
            Mock Start-Sleep -ModuleName 'RetryHelper' -MockWith { }
            Mock Get-IngestAccessToken -MockWith { New-FakeJwt }
            Mock Invoke-RestMethod -MockWith {
                if ($Uri -match '/_api/contextinfo') { throw 'simulated admin sanity failure' }
                Get-SanityCheckResponseForUri -Uri $Uri
            }
            $cfg = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                Organization      = 'xtlab2.onmicrosoft.com'
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            }
            { Connect-Service -Config $cfg } | Should -Throw '*SharePoint Online sanity check failed*'
        }
    }

    It 'throws if the Graph sanity check fails' {
        InModuleScope 'Connect' {
            # See admin-probe test above — instant backoff for the retry loop.
            Mock Start-Sleep -ModuleName 'RetryHelper' -MockWith { }
            Mock Get-IngestAccessToken -MockWith { New-FakeJwt }
            Mock Invoke-RestMethod -MockWith {
                if ($Uri -match 'graph\.microsoft\.com') { throw 'simulated graph sanity failure' }
                Get-SanityCheckResponseForUri -Uri $Uri
            }
            $cfg = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                Organization      = 'xtlab2.onmicrosoft.com'
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            }
            { Connect-Service -Config $cfg } | Should -Throw '*Microsoft Graph sanity check failed*'
        }
    }

    It 'throws if Graph /sites returns a shape with no value property (broken session)' {
        InModuleScope 'Connect' {
            Mock Get-IngestAccessToken -MockWith { New-FakeJwt }
            Mock Invoke-RestMethod -MockWith {
                if ($Uri -match 'graph\.microsoft\.com') {
                    # 200 OK but missing the expected OData collection envelope —
                    # session response is corrupt / not the API we asked for.
                    return [pscustomobject]@{ unexpected = 'shape' }
                }
                Get-SanityCheckResponseForUri -Uri $Uri
            }
            $cfg = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                Organization      = 'xtlab2.onmicrosoft.com'
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            }
            { Connect-Service -Config $cfg } | Should -Throw '*Microsoft Graph sanity check failed*unexpected shape*'
        }
    }
}

Describe 'Get-SpoToken — host routing + cache' {
    BeforeEach {
        $global:SpoConnectState = @{ MintedAudiences = @() }
    }
    AfterEach {
        Remove-Variable -Scope Global -Name SpoConnectState -ErrorAction SilentlyContinue
    }

    It 'caches a token per host and returns the same value on second call (no second mint)' {
        InModuleScope 'Connect' {
            Mock Get-IngestAccessToken -MockWith {
                $global:SpoConnectState.MintedAudiences += $Audience
                New-FakeJwt -ExpSecondsFromNow 3600
            }
            Mock Invoke-RestMethod -MockWith { Get-SanityCheckResponseForUri -Uri $Uri }

            $cfg = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                Organization      = 'xtlab2.onmicrosoft.com'
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            }
            $null = Connect-Service -Config $cfg  # 2 mints during sanity (admin + graph)

            # First call for main host: mint
            $t1 = Get-SpoToken -SiteUrl 'https://xtlab2.sharepoint.com/sites/X'
            # Second call for same host: cache hit, no new mint
            $t2 = Get-SpoToken -SiteUrl 'https://xtlab2.sharepoint.com/sites/Y'
            $t1 | Should -Be $t2
            # 3 mints total: admin + graph (sanity) + main (first call)
            $global:SpoConnectState.MintedAudiences.Count | Should -Be 3
        }
    }

    It 'routes per-host: main / -my / -admin / graph audiences are 4 distinct cache entries' {
        InModuleScope 'Connect' {
            Mock Get-IngestAccessToken -MockWith {
                $global:SpoConnectState.MintedAudiences += $Audience
                New-FakeJwt -ExpSecondsFromNow 3600
            }
            Mock Invoke-RestMethod -MockWith { Get-SanityCheckResponseForUri -Uri $Uri }

            $cfg = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                Organization      = 'xtlab2.onmicrosoft.com'
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            }
            $null = Connect-Service -Config $cfg  # admin + graph mints

            $null = Get-SpoToken -SiteUrl 'https://xtlab2.sharepoint.com/sites/X'                      # new mint: main
            $null = Get-SpoToken -SiteUrl 'https://xtlab2-my.sharepoint.com/personal/me_xtlab2_com'    # new mint: -my
            $null = Get-SpoToken -SiteUrl 'https://xtlab2-admin.sharepoint.com'                       # cache hit (admin from sanity)
            $null = Get-SpoToken -SiteUrl 'https://graph.microsoft.com'                               # cache hit (graph from sanity)

            # admin + graph + main + -my = 4 distinct audiences total. The admin
            # and graph calls above hit cache from Connect-Service sanity, no new mint.
            ($global:SpoConnectState.MintedAudiences | Sort-Object -Unique).Count | Should -Be 4
        }
    }

    It 'throws on an unknown host (defensive against typos in entity fetchers)' {
        InModuleScope 'Connect' {
            Mock Get-IngestAccessToken -MockWith { New-FakeJwt }
            Mock Invoke-RestMethod -MockWith { Get-SanityCheckResponseForUri -Uri $Uri }

            $cfg = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                Organization      = 'xtlab2.onmicrosoft.com'
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            }
            $null = Connect-Service -Config $cfg
            { Get-SpoToken -SiteUrl 'https://contoso.sharepoint.com/sites/X' } |
                Should -Throw "*not one of the configured SPO audiences*"
        }
    }

    It 'refreshes when the cached token expires within the skew window' {
        InModuleScope 'Connect' {
            # URL-keyed mock: sanity mints (admin + graph) return long-lived
            # tokens so they don't interfere with the refresh assertion. Only
            # the MAIN audience mints return a near-expiry token on its first
            # call, then a fresh one on subsequent calls.
            $script:MainMintCount = 0
            Mock Get-IngestAccessToken -MockWith {
                $global:SpoConnectState.MintedAudiences += $Audience
                if ($Audience -match 'xtlab2\.sharepoint\.com') {
                    $script:MainMintCount++
                    if ($script:MainMintCount -eq 1) {
                        return (New-FakeJwt -ExpSecondsFromNow 60)   # near-expiry (under the 300s skew)
                    }
                    return (New-FakeJwt -ExpSecondsFromNow 3600)
                }
                return (New-FakeJwt -ExpSecondsFromNow 3600)
            }
            Mock Invoke-RestMethod -MockWith { Get-SanityCheckResponseForUri -Uri $Uri }

            $cfg = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                Organization      = 'xtlab2.onmicrosoft.com'
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            }
            $null = Connect-Service -Config $cfg  # admin + graph mints (long-lived)

            $null = Get-SpoToken -SiteUrl 'https://xtlab2.sharepoint.com/sites/X'   # main mint #1 — 60s, near expiry
            $null = Get-SpoToken -SiteUrl 'https://xtlab2.sharepoint.com/sites/Y'   # main mint #2 — fresh, because #1 is expiring
            $null = Get-SpoToken -SiteUrl 'https://xtlab2.sharepoint.com/sites/Z'   # cache hit on #2, no new mint
            # 4 total: admin + graph + main #1 + main #2
            $global:SpoConnectState.MintedAudiences.Count | Should -Be 4
        }
    }
}

Describe 'Restore-ServiceConnection — token cache invalidation' {
    BeforeEach {
        $global:SpoConnectState = @{ MintedAudiences = @() }
    }
    AfterEach {
        Remove-Variable -Scope Global -Name SpoConnectState -ErrorAction SilentlyContinue
    }

    It 'clears the cache so the next Get-SpoToken re-mints' {
        InModuleScope 'Connect' {
            Mock Get-IngestAccessToken -MockWith {
                $global:SpoConnectState.MintedAudiences += $Audience
                New-FakeJwt -ExpSecondsFromNow 3600
            }
            Mock Invoke-RestMethod -MockWith { Get-SanityCheckResponseForUri -Uri $Uri }

            $cfg = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                Organization      = 'xtlab2.onmicrosoft.com'
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            }
            $null = Connect-Service -Config $cfg                                      # mints #1 admin + #2 graph
            $null = Get-SpoToken -SiteUrl 'https://xtlab2.sharepoint.com/sites/X'    # mint #3 main
            Restore-ServiceConnection
            $null = Get-SpoToken -SiteUrl 'https://xtlab2.sharepoint.com/sites/Y'    # mint #4 main (fresh, cache cleared)
            $global:SpoConnectState.MintedAudiences.Count | Should -Be 4
        }
    }

    It 'throws when called before Connect-Service' {
        # New runspace by way of a fresh Connect module import.
        Remove-Module Connect -Force -ErrorAction SilentlyContinue
        $modulePath = Join-Path $PSScriptRoot '..' 'scripts' 'Connect.psm1'
        Import-Module $modulePath -Force -DisableNameChecking
        InModuleScope 'Connect' {
            { Restore-ServiceConnection } | Should -Throw '*before Connect-Service*'
        }
    }
}
