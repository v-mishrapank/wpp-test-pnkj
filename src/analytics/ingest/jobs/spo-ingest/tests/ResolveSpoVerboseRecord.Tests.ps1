#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for Resolve-SpoVerboseRecord — the SPO verbose-envelope cleanup helper
# that strips __metadata, flattens {results:[...]} wrappers, drops __deferred
# navigation-property stubs, and normalizes naked ISO datetime strings to Z.

BeforeAll {
    $script:SharedModules = Join-Path $PSScriptRoot '..' '..' 'shared' 'modules'
    $script:Scripts       = Join-Path $PSScriptRoot '..' 'scripts'
    Import-Module (Join-Path $script:SharedModules 'LogHelper.psm1')       -Force
    Import-Module (Join-Path $script:SharedModules 'EventEmitter.psm1')    -Force
    Import-Module (Join-Path $script:SharedModules 'RetryHelper.psm1')     -Force
    Import-Module (Join-Path $script:SharedModules 'MsalTokenHelper.psm1') -Force
    Import-Module (Join-Path $script:Scripts 'SpoRestClient.psm1')         -Force -DisableNameChecking
    Import-Module (Join-Path $script:Scripts 'Connect.psm1')               -Force -DisableNameChecking
    Import-Module (Join-Path $script:Scripts 'entities' 'SpoSites.psm1')   -Force -DisableNameChecking
}

AfterAll {
    Remove-Module SpoSites, Connect, SpoRestClient, MsalTokenHelper, RetryHelper, EventEmitter, LogHelper -Force -ErrorAction SilentlyContinue
}

Describe 'Resolve-SpoVerboseRecord — __deferred stub elimination' {
    It 'returns $null for a bare deferred stub' {
        InModuleScope 'SpoSites' {
            $stub = @{ '__deferred' = @{ uri = 'https://t/_api/web/RoleAssignments' } }
            $r = Resolve-SpoVerboseRecord $stub
            $r | Should -BeNullOrEmpty
        }
    }

    It 'returns $null for a deferred stub on a PSCustomObject (not just a hashtable)' {
        InModuleScope 'SpoSites' {
            $stub = [pscustomobject]@{ '__deferred' = [pscustomobject]@{ uri = 'https://t/_api/web/Author' } }
            $r = Resolve-SpoVerboseRecord $stub
            $r | Should -BeNullOrEmpty
        }
    }

    It 'drops parent-record fields whose only value was a deferred stub (no empty {} pollution)' {
        InModuleScope 'SpoSites' {
            # Realistic /_api/web record where most navigation properties are deferred
            $rec = @{
                Id              = 'web-id'
                Title           = 'Home'
                RoleAssignments = @{ '__deferred' = @{ uri = 'https://t/RoleAssignments' } }
                Author          = @{ '__deferred' = @{ uri = 'https://t/Author' } }
                ContentTypes    = @{ '__deferred' = @{ uri = 'https://t/ContentTypes' } }
            }
            $r = Resolve-SpoVerboseRecord $rec
            $r['Id']    | Should -Be 'web-id'
            $r['Title'] | Should -Be 'Home'
            # Each deferred stub becomes a $null value (not an empty {})
            $r['RoleAssignments'] | Should -BeNullOrEmpty
            $r['Author']          | Should -BeNullOrEmpty
            $r['ContentTypes']    | Should -BeNullOrEmpty
            # Critically: the values are *literal null*, not empty hashtables
            ($r['RoleAssignments'] -is [System.Collections.IDictionary]) | Should -BeFalse
            ($r['Author']          -is [System.Collections.IDictionary]) | Should -BeFalse
        }
    }
}

Describe 'Resolve-SpoVerboseRecord — datetime normalization' {
    It 'normalizes a [DateTime] with Kind=Unspecified to UTC Z (the production case — Invoke-RestMethod parses JSON dates as Unspecified)' {
        InModuleScope 'SpoSites' {
            $dt = [DateTime]::new(2026, 5, 10, 10, 42, 18, 917, [System.DateTimeKind]::Unspecified)
            Resolve-SpoVerboseRecord $dt | Should -Be '2026-05-10T10:42:18.917Z'
        }
    }

    It 'normalizes a [DateTime] with Kind=Utc to Z (round-trips the wall-clock without TZ conversion)' {
        InModuleScope 'SpoSites' {
            $dt = [DateTime]::new(2026, 5, 10, 10, 42, 18, 917, [System.DateTimeKind]::Utc)
            Resolve-SpoVerboseRecord $dt | Should -Be '2026-05-10T10:42:18.917Z'
        }
    }

    It 'normalizes a [DateTime] with Kind=Local to its UTC equivalent' {
        InModuleScope 'SpoSites' {
            $local = [DateTime]::new(2026, 5, 10, 10, 42, 18, 917, [System.DateTimeKind]::Local)
            $expected = $local.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ", [System.Globalization.CultureInfo]::InvariantCulture)
            Resolve-SpoVerboseRecord $local | Should -Be $expected
        }
    }

    It 'normalizes a [DateTimeOffset] to UTC Z' {
        InModuleScope 'SpoSites' {
            $dto = [DateTimeOffset]::new(2026, 5, 10, 10, 42, 18, 917, [System.TimeSpan]::FromHours(-5))
            Resolve-SpoVerboseRecord $dto | Should -Be '2026-05-10T15:42:18.917Z'
        }
    }

    It 'appends Z to a naked ISO datetime string (defensive — any code path that produces raw strings)' {
        InModuleScope 'SpoSites' {
            Resolve-SpoVerboseRecord '2026-05-10T10:42:18.917' | Should -Be '2026-05-10T10:42:18.917Z'
        }
    }

    It 'leaves a Z-suffixed ISO datetime string unchanged (no double-Z)' {
        InModuleScope 'SpoSites' {
            Resolve-SpoVerboseRecord '2026-05-10T10:42:18Z'       | Should -Be '2026-05-10T10:42:18Z'
            Resolve-SpoVerboseRecord '2026-05-10T10:42:18.917Z'   | Should -Be '2026-05-10T10:42:18.917Z'
        }
    }

    It 'leaves an explicit-offset ISO datetime string unchanged' {
        InModuleScope 'SpoSites' {
            Resolve-SpoVerboseRecord '2026-05-10T10:42:18+00:00'  | Should -Be '2026-05-10T10:42:18+00:00'
            Resolve-SpoVerboseRecord '2026-05-10T10:42:18-05:00'  | Should -Be '2026-05-10T10:42:18-05:00'
        }
    }

    It 'does not touch non-date strings (no spurious Z on URL/UPN/etc)' {
        InModuleScope 'SpoSites' {
            Resolve-SpoVerboseRecord 'https://tenant.sharepoint.com/sites/X' | Should -Be 'https://tenant.sharepoint.com/sites/X'
            Resolve-SpoVerboseRecord 'i:0#.f|membership|user@tenant.com'      | Should -Be 'i:0#.f|membership|user@tenant.com'
            Resolve-SpoVerboseRecord '2026-05-10'                              | Should -Be '2026-05-10'  # date-only, no T
        }
    }

    It 'normalizes [DateTime] values inside nested records (the real production path)' {
        InModuleScope 'SpoSites' {
            $rec = @{
                Created              = [DateTime]::new(2026, 5, 10, 10, 42, 18, 917, [System.DateTimeKind]::Unspecified)
                LastItemModifiedDate = [DateTime]::new(2026, 5, 10, 10, 42, 18, 0,   [System.DateTimeKind]::Utc)
                Member               = @{
                    Created = [DateTime]::new(2025, 11, 1, 5, 19, 42, 0, [System.DateTimeKind]::Unspecified)
                }
            }
            $r = Resolve-SpoVerboseRecord $rec
            $r['Created']              | Should -Be '2026-05-10T10:42:18.917Z'
            $r['LastItemModifiedDate'] | Should -Be '2026-05-10T10:42:18.000Z'
            $r['Member']['Created']    | Should -Be '2025-11-01T05:19:42.000Z'
        }
    }
}

Describe 'Resolve-SpoVerboseRecord — array/list flattening (List+Add path)' {
    It 'flattens a {results:[...]} verbose wrapper into a cleaned array (multi-element)' {
        InModuleScope 'SpoSites' {
            $wrapper = @{
                results = @(
                    @{ __metadata = @{ type = 'SP.X' }; Id = 1; Title = 'a' }
                    @{ __metadata = @{ type = 'SP.X' }; Id = 2; Title = 'b' }
                    @{ __metadata = @{ type = 'SP.X' }; Id = 3; Title = 'c' }
                )
            }
            $r = Resolve-SpoVerboseRecord $wrapper
            ($r -is [array])            | Should -BeTrue
            $r.Count                    | Should -Be 3
            $r[0].Contains('__metadata') | Should -BeFalse   # each row cleaned during accumulation
            $r[0]['Id']                 | Should -Be 1
            $r[2]['Title']              | Should -Be 'c'
        }
    }

    It 'returns a single-element wrapper as an array, not a bare object (comma-guard preserved)' {
        InModuleScope 'SpoSites' {
            $r = Resolve-SpoVerboseRecord @{ results = @( @{ Id = 99 } ) }
            ($r -is [array]) | Should -BeTrue
            $r.Count         | Should -Be 1
            $r[0]['Id']      | Should -Be 99
        }
    }

    It 'flattens a bare array (generic IList branch) and strips __metadata per element' {
        InModuleScope 'SpoSites' {
            $arr = @(
                @{ __metadata = @{ type = 'x' }; A = 1 }
                @{ A = 2 }
            )
            $r = Resolve-SpoVerboseRecord $arr
            ($r -is [array])             | Should -BeTrue
            $r.Count                     | Should -Be 2
            $r[0].Contains('__metadata') | Should -BeFalse
            $r[0]['A']                   | Should -Be 1
            $r[1]['A']                   | Should -Be 2
        }
    }

    It 'returns a single-element bare array as an array (comma-guard)' {
        InModuleScope 'SpoSites' {
            $one = @( @{ A = 5 } )
            $r = Resolve-SpoVerboseRecord $one
            ($r -is [array]) | Should -BeTrue
            $r.Count         | Should -Be 1
            $r[0]['A']       | Should -Be 5
        }
    }
}
