# Scope filter for entity root stages. Reads a single-field set (UPNs,
# site webUrls, etc.) from a convention-based ADLS path
# (_scope/<tenant_key>/<dimension>/) and returns it as a HashSet for root
# fetchers to filter against.

# Explicit dependency on StorageHelperRest for Get-AdlsAccessToken.
# Without this import the function resolves only when callers happen to
# import StorageHelperRest first into the parent scope (the production
# wire), and breaks in standalone test harnesses or non-Invoke-Ingestion
# containers.
#
# CRITICAL: -Global is required. Without it, the parse-time -Force import
# from inside this module steals StorageHelperRest's exported functions
# into ScopeFilter's module scope, removing them from the global function
# table. Invoke-Ingestion.ps1 captures `$uploadFunction = ${function:Write-ToAdlsRest}`
# at line 36 — if that capture happens AFTER ScopeFilter is imported (it
# does), Write-ToAdlsRest is no longer globally visible and $uploadFunction
# is null. Same shape as the StageExecutor RetryHelper bug (#458) and the
# LogHelper issue (#189). Mirrors the `-Force -Global` pattern Connect.psm1
# uses to import SpoRestClient. See #260.
Import-Module (Join-Path $PSScriptRoot 'StorageHelperRest.psm1') -Force -Global -DisableNameChecking
#
# Wire contract:
#   - Dispatcher sets SCOPE_ROOT env var to "_scope/<tenant_key>" for scoped
#     tenants; absent for unscoped tenants. Root fetchers branch on env presence
#     before calling into this module.
#   - The scope folder layout is _scope/<tenant_key>/<dimension>/{ISO-date}.jsonl
#     stacked over time. Lex-max filename wins (ISO dates sort correctly).
#   - JSONL records use the standard envelope shape: {"_record": {...}}.
#     Only `_record.<KeyField>` (caller-specified — userPrincipalName for users,
#     webUrl for sites, etc.) is required; all other fields are ignored. Future
#     populators can add `id`, `mail`, etc. without breaking consumers.
#   - Missing folder or no .jsonl files in folder → returns $null (caller
#     treats as "no scope for this dimension available").
#   - Malformed records (envelope present but no key field) → hard throw. Never
#     silently emit a partial set.
#
# Dimension naming (e.g., "users", "groups", "sites") is convention; each
# root fetcher picks its own dimension(s). Multiple dimensions can share the
# same dispatcher SCOPE_ROOT — SpoSites consumes both users (for personal
# sites) and sites (for team sites) from _scope/<tenant>/users/ and
# _scope/<tenant>/sites/ respectively.
#
# See issue #260 for design and the long-term scope-ingest container that
# will eventually populate these folders.

function Invoke-WithRetryAndStatus {
    # Module-local retry wrapper for the two ADLS reads in this module.
    # Mirrors Invoke-AdlsDataPlaneWithRetry in StorageHelperRest.psm1 but
    # lives here so Pester's Mock Invoke-WebRequest from inside
    # InModuleScope ScopeFilter intercepts the actual call. Cross-module
    # scriptblock invocation (`& $sb` from StorageHelperRest) resolves
    # Invoke-WebRequest through StorageHelperRest's command frame, which
    # the mock can't reach — so we keep the retry colocated with the call
    # site. ~15 lines duplicated; worth it for testability. See #260.
    param(
        [Parameter(Mandatory)][string]$RequestUri,
        [Parameter(Mandatory)][hashtable]$Headers,
        [int]$MaxAttempts = 5,
        [int]$MaxDelaySeconds = 60
    )
    # Guards the 401 re-mint so a persistent 401 falls through to the caller.
    $authRetried = $false
    $attempt = 0
    while ($true) {
        $attempt++
        try {
            return Invoke-WebRequest -Uri $RequestUri -Method GET -Headers $Headers -UseBasicParsing -ErrorAction Stop
        }
        catch {
            $status = $null
            if ($_.Exception.Response -and $_.Exception.Response.PSObject.Properties['StatusCode']) {
                $status = [int]$_.Exception.Response.StatusCode
            }
            # 401 safety net: the cached ADLS token was rejected (lapsed inside
            # the refresh skew or the storage credential rotated mid-run). Force
            # a fresh mint and rewrite the Authorization header in place — the
            # caller shares this hashtable across the list + read calls, so the
            # new token carries forward. The re-mint doesn't count against
            # MaxAttempts.
            if ($status -eq 401 -and -not $authRetried) {
                $authRetried = $true
                $freshToken = Get-AdlsAccessToken -ForceRefresh
                $Headers['Authorization'] = "Bearer $freshToken"
                Write-Log "Scope read got 401; re-minting ADLS token and retrying once" -Level WARN
                $attempt--
                continue
            }
            # Transient: network errors (no status) + 5xx + 429.
            # Non-transient (4xx other than 429): bubble up immediately — the
            # 404 case is the expected "no scope dir" signal the caller catches.
            $isTransient = ($null -eq $status) -or ($status -ge 500) -or ($status -eq 429)
            if (-not $isTransient -or $attempt -ge $MaxAttempts) { throw }

            # Honor Retry-After when the server sent one (typical on 429 and
            # some 503s). Fall back to exponential backoff capped at
            # MaxDelaySeconds. Get-RetryAfterSeconds is in StorageHelperRest.
            $retryAfter = Get-RetryAfterSeconds -Response $_.Exception.Response
            $delay = if ($retryAfter -and $retryAfter -gt 0) {
                [int][math]::Min($MaxDelaySeconds, $retryAfter)
            } else {
                [int][math]::Min($MaxDelaySeconds, [math]::Pow(2, $attempt))
            }
            Write-Log "Scope read retry attempt $attempt after status=$status, backing off ${delay}s: $($_.Exception.Message)" -Level WARN
            Start-Sleep -Seconds $delay
        }
    }
}

function Get-ScopeKeySet {
    [CmdletBinding()]
    [OutputType([System.Collections.Generic.HashSet[string]])]
    param(
        [Parameter(Mandatory)][string]$ScopeRoot,
        [Parameter(Mandatory)][string]$Dimension,
        [Parameter(Mandatory)][string]$KeyField,
        [Parameter(Mandatory)][string]$StorageAccountUrl,
        [Parameter(Mandatory)][string]$ContainerName
    )

    $directory = "$ScopeRoot/$Dimension"
    $token = Get-AdlsAccessToken
    $headers = @{
        'Authorization' = "Bearer $token"
        'x-ms-version'  = '2021-08-06'
    }

    # List Paths (ADLS Gen2). 404 on the filesystem-level list is rare — the
    # container always exists once provisioned. 404 here typically means the
    # directory doesn't exist (no scope ever uploaded for this tenant).
    #
    # Both ADLS calls (list + read) go through Invoke-WithRetryAndStatus so
    # transient 5xx / 429 / network errors honor Retry-After and exponential
    # backoff instead of failing the scoped root stage on the first blip.
    # 4xx (including the expected 404 for missing scope dir) is non-transient
    # and surfaces immediately as a throw — we catch 404 here and translate
    # to $null.
    # List+AddRange, not array += : a scope dir can hold thousands of blobs
    # across many pages, and += rebuilds the whole array per page (O(n^2)).
    $blobs = [System.Collections.Generic.List[object]]::new()
    $continuation = $null
    do {
        $listUrl = "$StorageAccountUrl/$ContainerName" +
            "?resource=filesystem&recursive=false&directory=$([uri]::EscapeDataString($directory))"
        if ($continuation) {
            $listUrl += "&continuation=$([uri]::EscapeDataString($continuation))"
        }
        try {
            $resp = Invoke-WithRetryAndStatus -RequestUri $listUrl -Headers $headers
        }
        catch {
            $status = $null
            if ($_.Exception.Response -and $_.Exception.Response.PSObject.Properties['StatusCode']) {
                $status = [int]$_.Exception.Response.StatusCode
            }
            if ($status -eq 404) {
                Write-Log "Scope directory not found at $directory (404); skipping scope filter" -Level WARN
                return $null
            }
            throw
        }

        $body = $resp.Content | ConvertFrom-Json
        if ($body.paths) {
            # AddRange (not Add): $body.paths is itself an array of path entries.
            $blobs.AddRange([object[]]$body.paths)
        }
        $continuation = $resp.Headers['x-ms-continuation']
        # The header may surface as a string[] depending on transport; normalize.
        if ($continuation -is [array]) { $continuation = $continuation[0] }
    } while ($continuation)

    # Filter to .jsonl files only and pick lex-max. ADLS List Paths returns
    # `name` as the full path including the directory prefix; strip to the
    # leaf for display, sort on full path (equivalent for same-directory siblings).
    $jsonlBlobs = @($blobs | Where-Object { $_.name -like '*.jsonl' -and -not $_.isDirectory })
    if ($jsonlBlobs.Count -eq 0) {
        Write-Log "Scope directory $directory contains no .jsonl files; skipping scope filter" -Level WARN
        return $null
    }
    $latest = ($jsonlBlobs | Sort-Object -Property name)[-1]
    Write-Log "Scope: reading $($latest.name) ($($latest.contentLength) bytes)"

    # Read the blob. Path returned by List Paths is relative to the filesystem
    # root; GET on $StorageAccountUrl/$ContainerName/$path returns the raw bytes.
    $readUrl = "$StorageAccountUrl/$ContainerName/$($latest.name)"
    $readResp = Invoke-WithRetryAndStatus -RequestUri $readUrl -Headers $headers
    $content = $readResp.Content
    if ($content -is [byte[]]) {
        $content = [System.Text.Encoding]::UTF8.GetString($content)
    }

    $keys = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $lineNum = 0
    foreach ($line in $content -split "`n") {
        $lineNum++
        $trimmed = $line.Trim()
        if ([string]::IsNullOrEmpty($trimmed)) { continue }

        $obj = $null
        try {
            $obj = $trimmed | ConvertFrom-Json -ErrorAction Stop
        }
        catch {
            throw "Scope file $($latest.name) line $lineNum is not valid JSON: $($_.Exception.Message)"
        }

        # Standard _record envelope. Lines without _record are skipped silently
        # — covers any future metadata header rows the populator might add.
        if (-not $obj.PSObject.Properties['_record']) { continue }

        $record = $obj._record
        if (-not $record.PSObject.Properties[$KeyField] -or
            [string]::IsNullOrWhiteSpace([string]$record.$KeyField)) {
            throw "Scope file $($latest.name) line $lineNum has _record but no $KeyField"
        }

        # Trim whitespace AND trailing slashes so site webUrls in the scope
        # file match the .TrimEnd('/')-normalized form consumers see (e.g.
        # SpoSites root strips trailing slashes off Graph getAllSites
        # webUrls before lookup). Safe for UPNs (no trailing slash to
        # remove). See #260.
        [void]$keys.Add((([string]$record.$KeyField).Trim()).TrimEnd('/'))
    }

    Write-Log "Scope: loaded $($keys.Count) $KeyField value(s) from $($latest.name)"
    # Comma wraps to single-element array so PowerShell's pipeline-unroll
    # doesn't enumerate the HashSet into Object[] on return. Caller receives
    # the HashSet as one object.
    return ,$keys
}

Export-ModuleMember -Function Get-ScopeKeySet
