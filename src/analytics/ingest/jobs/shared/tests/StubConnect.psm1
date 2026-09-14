# Test stub for Connect-Service / Restore-ServiceConnection.
#
# Pester tests that exercise Invoke-ModuleRun's pool path need an auth module
# to point at via Context.AuthModulePath. The real container modules would
# attempt actual Connect-MgGraph / Connect-ExchangeOnline calls that fail
# against fake config; this stub no-ops so the dispatch loop can run the
# fake fetcher functions unimpeded. The "auth warnings" in test output come
# from this stub being called with empty test fixtures — expected, not a
# regression.

function Connect-Service {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Config
    )
    # Mirrors the post-#145 Connect-Service shape: no CertBytes parameter,
    # AuthConfig is the source of truth (tests pass an empty hashtable).
    return @{ AuthConfig = $Config }
}

function Restore-ServiceConnection {
    [CmdletBinding()]
    param()
}

Export-ModuleMember -Function Connect-Service, Restore-ServiceConnection
