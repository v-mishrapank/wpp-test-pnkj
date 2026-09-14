# Graph container's auth module.
#
# Exports:
#   Connect-Service           — initial auth. Called once in the main process
#                               (by Invoke-Ingestion.ps1) and once per worker
#                               runspace (by the dispatch template's
#                               first-dispatch self-auth, see WorkerPool.psm1).
#   Restore-ServiceConnection — mid-run auth recovery. Called inside a worker
#                               runspace when a fetch is classified Auth and
#                               we reconnect before retrying.
#
# === Auth state lives in $script: scope ===
#
# The two functions communicate via $script:AuthConfig, set by Connect-Service
# and read by Restore-ServiceConnection. Module-scope is runspace-local —
# Connect.psm1 is imported into each worker runspace's ISS, and each runspace
# gets its own $script:AuthConfig that persists for the runspace's lifetime.
# Pre-#145 used $global: scope because the reconnect path ran inside an
# anonymous scriptblock where $script: would resolve to the block, not this
# module; the post-#145 dispatch template invokes Connect-Service /
# Restore-ServiceConnection through scriptblocks created from strings, so the
# function's module-scope is reached normally and $script: works.

# Per-runspace auth state. Set by Connect-Service. Read by Restore-Service-
# Connection. CertificateBase64 inside is the serialized cert (immutable
# string); decode to bytes only at the moment of use.
$script:AuthConfig = $null

# Pin the X509Certificate2 we passed to Connect-MgGraph at script scope so it
# stays alive for the lifetime of the runspace. MSAL retains a reference to the
# cert for silent token refresh; if we Dispose the .NET wrapper or let it GC,
# its SafeCertContextHandle is closed and the next refresh throws
# "m_safeCertContext is an invalid handle" — surfaced as
# AuthenticationFailedException ~5–60 min into a long run, which then deadlocks
# the host's error-marshal path. See issue #321.
$script:KeepCertAlive = $null

function Connect-Service {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Config
    )

    if ([string]::IsNullOrEmpty($Config.CertificateBase64)) {
        throw "Connect-Service requires Config.CertificateBase64."
    }

    # Stash AuthConfig in this runspace's $script: scope. Restore-
    # ServiceConnection reads it for mid-run auth recovery. Even when we
    # piggyback below, this still has to be set so the runspace can recover
    # from a 401 if its token expires. The base64 cert keeps the cert
    # decode/re-decode flow at the moment of use.
    $authConfig = @{
        ClientId          = $Config.ClientId
        TenantId          = $Config.TenantId
        CertificateBase64 = $Config.CertificateBase64
    }
    $script:AuthConfig = $authConfig

    # === Fix for issue #269 — fresh-pool first-dispatch silent hang ===
    #
    # Worker runspaces piggyback on the parent process's existing GraphSession
    # singleton instead of calling Connect-MgGraph themselves. The parent's
    # initial Connect-Service already populated GraphSession.Instance.AuthContext,
    # which is process-shared (static state in the .NET AppDomain), so any
    # worker runspace can use the existing session — there's no need (or
    # benefit) to re-run Connect-MgGraph per runspace.
    #
    # Why this matters: when N worker runspaces called Connect-MgGraph
    # concurrently on a fresh-pool first dispatch (post-#145 sibling-tier
    # batched dispatch made this even more bursty), they raced on the
    # singleton's HttpClient handler chain. Symptom: every thread parked on
    # an internal lock; CPU drops to ~0.001 cores; no log activity for many
    # minutes; ACA replicaTimeout (8h default) was the only thing eventually
    # killing the container. We reproduced this twice within a few minutes
    # of dispatch in our smoke loop, on graph stages that match the
    # sibling-batch pattern (entra_group_members + entra_group_owners,
    # entra_app_proxy_config + entra_app_owners,
    # entra_sp_role_assignees + entra_sp_owners + ...).
    #
    # Tests we ran on the way to this fix:
    #   - T1: raise ThreadPool min-worker count to 50. Disproved — hang
    #     reproduces with min threads at 50 (CPU pegged at 0.001 cores
    #     means threads aren't merely starved, they're parked on a lock).
    #   - T2: tighten GraphSession.RequestContext.ClientTimeout to 120s.
    #     Disproved — the lock holding threads is not bounded by HttpClient
    #     timeouts.
    #   - T3a: serialize Connect-MgGraph across runspaces via SemaphoreSlim.
    #     Disproved — even one-at-a-time Connect-MgGraph calls leave the
    #     subsequent concurrent Invoke-MgGraphRequest pattern in the same
    #     racy state on the handler chain.
    #   - T3 (this fix): skip Connect-MgGraph in worker runspaces entirely.
    #     Confirmed — 4 consecutive clean smokes against madev1/madev2 with
    #     8 graph runs total, 0 hangs.
    #
    # We force-load the Microsoft.Graph.Authentication assembly before
    # referencing the GraphSession type. Neither the parent's first call
    # to Connect-Service nor a worker runspace's ISS guarantees the
    # assembly has been touched — Import-Module is idempotent and cheap.
    # If the type is still unresolvable after the load (very unexpected),
    # we fall through to the original Connect-MgGraph path below as a
    # safety net.
    Import-Module Microsoft.Graph.Authentication -ErrorAction SilentlyContinue
    $session = $null
    try {
        $session = [Microsoft.Graph.PowerShell.Authentication.GraphSession]::Instance
    } catch {
        # Type still not loaded — fall through to the Connect-MgGraph path below.
        Write-Verbose $_.Exception.Message
    }
    if ($session -and $session.AuthContext) {
        Write-Log "Worker runspace piggybacking on existing GraphSession.Instance auth"
        # Reapply ClientTimeout on the shared singleton. Idempotent — every
        # runspace writes the same value. Defense-in-depth from the #269
        # investigation: doesn't fix the hang on its own, but tightens the
        # per-request budget so any unforeseen future stall surfaces as a
        # noisy TaskCanceledException instead of an indefinite block.
        if ($session.RequestContext) {
            $session.RequestContext.ClientTimeout = [TimeSpan]::FromSeconds(120)
        }
        return @{
            AuthConfig       = $authConfig
            OrganizationName = ''
        }
    }

    # Parent path — initial Connect-MgGraph. Only the main process should hit
    # this branch.
    $certBytes = [Convert]::FromBase64String($Config.CertificateBase64)
    $cert = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new(
        $certBytes, [string]::Empty,
        [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::EphemeralKeySet
    )
    # Track whether we successfully handed the cert to MSAL. If Connect-MgGraph
    # throws, MSAL never captured the reference, so the finally block must
    # Dispose() to avoid leaking native cert handles across an auth-retry
    # storm. Only when $pinned is true do we hold the cert alive at script
    # scope on purpose. (Per Copilot review on PR #329.)
    $pinned = $false
    try {
        Connect-MgGraph -Certificate $cert `
            -ClientId $Config.ClientId `
            -TenantId $Config.TenantId `
            -NoWelcome `
            -ErrorAction Stop

        # Pin the cert at script scope (see comment on $script:KeepCertAlive
        # above). Must happen before we leave this function — the local $cert
        # going out of scope is enough to make it GC-eligible despite MSAL's
        # internal reference, since MSAL stores the cert reference inside its
        # own object graph but the SafeHandle gets finalized when this wrapper
        # is collected.
        $script:KeepCertAlive = $cert
        $pinned = $true

        # Tighten the SDK's per-request budget on the process-shared singleton.
        # Default is 5 minutes; 120s is generous enough not to break legitimate
        # slow tier-1 pages but short enough to convert silent receive-stalls
        # into noisy TaskCanceledExceptions. Defense-in-depth from the #269
        # investigation — does not fix the silent hang on its own (T2 disproven),
        # but bounds the surface area of any future request-side stall. The
        # piggyback path above reapplies the same value on each worker; this
        # parent-side write is the first time it's set per process.
        $rc = [Microsoft.Graph.PowerShell.Authentication.GraphSession]::Instance.RequestContext
        if ($rc) {
            $rc.ClientTimeout = [TimeSpan]::FromSeconds(120)
            Write-Log "Graph SDK RequestContext.ClientTimeout set to $($rc.ClientTimeout.TotalSeconds)s (default was 5m)"
        }

        # Post-connect sanity check — a "successful" Connect-MgGraph can leave
        # the session in a state where every Get-Mg* call returns empty (consent
        # revoked, cert mismatch MSAL doesn't surface at auth time). /organization
        # is a trivial universal endpoint that returns exactly one record for any
        # Entra tenant; failure here surfaces the broken session now instead of
        # leaking into every entity fetch as silent zero records.
        try {
            $orgResponse = Invoke-MgGraphRequest -Method GET -Uri '/v1.0/organization?$top=1' -ErrorAction Stop
        } catch {
            throw "Microsoft Graph sanity check failed for tenant '$($env:TENANT_KEY)': GET /organization threw: $($_.Exception.Message)"
        }
        if (-not $orgResponse -or -not $orgResponse.value -or $orgResponse.value.Count -eq 0) {
            throw "Microsoft Graph sanity check failed for tenant '$($env:TENANT_KEY)': GET /organization returned no records — session is not usable despite Connect-MgGraph succeeding."
        }
    }
    finally {
        # Only dispose if we never handed the cert to MSAL. Once pinned, MSAL
        # retains a reference for token refresh and disposing closes its
        # SafeCertContextHandle (issue #321). When $pinned is false (Connect-
        # MgGraph threw before we set the flag), MSAL has no reference and
        # we must dispose to avoid leaking native handles across retries.
        if (-not $pinned) { $cert.Dispose() }
        # Wiping the raw bytes is always safe; X509Certificate2 has copied
        # them into the cert context.
        [Array]::Clear($certBytes, 0, $certBytes.Length)
    }

    return @{
        AuthConfig       = $authConfig
        OrganizationName = $orgResponse.value[0].displayName
    }
}

function Restore-ServiceConnection {
    [CmdletBinding()]
    param()

    if (-not $script:AuthConfig) {
        throw "Restore-ServiceConnection called before Connect-Service initialized auth state."
    }
    $cfg = $script:AuthConfig

    $bytes = [Convert]::FromBase64String($cfg.CertificateBase64)
    $cert = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new(
        $bytes, [string]::Empty,
        [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::EphemeralKeySet
    )
    # See Connect-Service for why we track $pinned. In an Auth-classified
    # retry loop, repeated Connect-MgGraph failures here would otherwise leak
    # one cert handle per attempt.
    $pinned = $false
    try {
        Connect-MgGraph -Certificate $cert `
            -ClientId $cfg.ClientId `
            -TenantId $cfg.TenantId `
            -NoWelcome `
            -ErrorAction Stop

        # Same cert-pinning rule as Connect-Service. Replacing the previous
        # reference is fine: after the new Connect-MgGraph call, MSAL has
        # captured this cert and the previous one is no longer referenced
        # internally, so letting the prior $script:KeepCertAlive get
        # overwritten (and GC'd) won't break the active session.
        $script:KeepCertAlive = $cert
        $pinned = $true
    }
    finally {
        if (-not $pinned) { $cert.Dispose() }
        [Array]::Clear($bytes, 0, $bytes.Length)
    }
}

Export-ModuleMember -Function Connect-Service, Restore-ServiceConnection
