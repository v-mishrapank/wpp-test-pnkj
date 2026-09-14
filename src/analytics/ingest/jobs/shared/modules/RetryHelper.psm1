function Get-HttpStatusCode {
    <#
    .SYNOPSIS
        Extract a typed HTTP status code from an exception chain.
        Returns [int] status code or $null if the exception is not HTTP-typed.
    #>
    param([Parameter(Mandatory)][System.Exception]$Exception)

    $current = $Exception
    while ($current) {
        # HttpResponseException (Invoke-RestMethod in PS 7.4+)
        # HttpRequestException (.NET 5+ has .StatusCode)
        # WebException (.Response is HttpWebResponse)
        if ($current.PSObject.Properties['Response'] -and $null -ne $current.Response) {
            $resp = $current.Response
            if ($resp.PSObject.Properties['StatusCode'] -and $null -ne $resp.StatusCode) {
                return [int]$resp.StatusCode
            }
        }
        # HttpRequestException in .NET 5+ has StatusCode directly
        if ($current.PSObject.Properties['StatusCode'] -and $null -ne $current.StatusCode) {
            return [int]$current.StatusCode
        }
        # Graph SDK ODataError has ResponseStatusCode
        if ($current.PSObject.Properties['ResponseStatusCode'] -and $null -ne $current.ResponseStatusCode) {
            return [int]$current.ResponseStatusCode
        }
        $current = $current.InnerException
    }
    return $null
}

function Get-ErrorClassification {
    <#
    .SYNOPSIS
        Classify an error as Auth, Throttle, Skippable, or Unknown.
        Checks typed HTTP status codes first, then falls back to message matching.
    .PARAMETER ErrorRecord
        The $_ from a catch block.
    .PARAMETER ApiFamily
        One of 'graph', 'exo', 'spo', 'mde', 'powerplat', 'powerbi', 'log'.
        Drives which skippable/throttle patterns to use. 'log' covers both the
        Office 365 Management Activity API (audit_* entities) and Graph
        sign-ins, since they share the same retry/throttle semantics in
        log-ingest.
    #>
    param(
        [Parameter(Mandatory)]$ErrorRecord,
        [ValidateSet('graph', 'exo', 'spo', 'mde', 'powerplat', 'powerbi', 'log')]
        [string]$ApiFamily = 'graph'
    )

    $ex = $ErrorRecord.Exception
    # Walk exception chain to find deepest meaningful message
    $innermost = $ex
    while ($innermost.InnerException) { $innermost = $innermost.InnerException }
    $message = if (-not [string]::IsNullOrWhiteSpace($innermost.Message)) {
        $innermost.Message
    } elseif (-not [string]::IsNullOrWhiteSpace($ex.Message)) {
        $ex.Message
    } else {
        $ex.GetType().FullName
    }
    # If the response body is available (Invoke-RestMethod with
    # -ErrorAction Stop captures it on $ErrorRecord.ErrorDetails for non-2xx
    # responses), append it to the message. Without this, a 400 from a bad
    # $select surfaces as just "400 Bad Request" with no detail about which
    # field is wrong — the actual Dataverse JSON error body lives in
    # ErrorDetails. Cap at 500 chars to keep manifests readable.
    #
    # Cmdlet divergence (#405): Invoke-RestMethod stores just the response
    # body here; Invoke-MgGraphRequest stores the full HTTP wire format
    # (request line + request headers + blank + response status + response
    # headers + blank + body) via HttpMessageFormatter.ReadAsStringAsync.
    # We pass either shape through verbatim — the wire dump carries useful
    # diagnostic context (request URL, request-id, status) that's worth
    # keeping. The newline-row-split problem this used to cause is handled
    # at the Write-Log boundary by replacing CR/LF with literal `\n`.
    $bodyMsg = $null
    if ($ErrorRecord.ErrorDetails -and $ErrorRecord.ErrorDetails.Message) {
        $bodyMsg = $ErrorRecord.ErrorDetails.Message
        if ($bodyMsg.Length -gt 500) { $bodyMsg = $bodyMsg.Substring(0, 500) + '…' }
        $message = "$message | body=$bodyMsg"
    }
    # $matchText drives the message-pattern classifier below. Body content is
    # included because some error codes (notably the Mgmt API's AF429 throttle
    # code) live in the JSON response body, not the exception message — without
    # the body, AF429 falls through to Auth and we reconnect-loop instead of
    # backing off.
    $matchText = "$($ex.Message) $($innermost.Message)"
    if ($bodyMsg) { $matchText = "$matchText $bodyMsg" }

    $statusCode = Get-HttpStatusCode -Exception $ex

    # Extract Retry-After from response headers or message text
    $retryAfter = $null
    if ($null -ne $statusCode) {
        try {
            $resp = $null
            $cur = $ex
            while ($cur -and -not $resp) {
                if ($cur.PSObject.Properties['Response'] -and $null -ne $cur.Response) { $resp = $cur.Response }
                $cur = $cur.InnerException
            }
            if ($resp -and $resp.PSObject.Properties['Headers'] -and $resp.Headers['Retry-After']) {
                $retryAfter = [int]$resp.Headers['Retry-After']
            }
        } catch { Write-Verbose $_.Exception.Message }
    }
    if ($null -eq $retryAfter -and $matchText -match 'Retry-After[:\s]+(\d+)') {
        $retryAfter = [int]$Matches[1]
    }

    $result = @{
        Category   = 'Unknown'
        StatusCode = $statusCode
        RetryAfter = $retryAfter
        Message    = $message
    }

    # --- Inner-retry exhaustion marker (#526) ---
    # An inner Invoke-WithRetry already spent its full retry budget on this
    # error and re-threw it wrapped with the RETRY_EXHAUSTED prefix. Checked
    # BEFORE the status-code switch: the wrapped original (e.g. a persistent
    # 500) still sits on the InnerException chain, and classifying by its
    # status would send outer retry layers (WorkerPool dispatch loop,
    # StageExecutor inline loop) into their own MaxRetries on top of the
    # inner ones — 5 × 5 attempts, ~40 min observed on one dead item.
    # The whole chain is walked (not just $ex) so a fetcher that catches the
    # wrapper and re-throws it inside its own context exception can't bury
    # the marker and silently revert that path to 5 × 5. Each level is
    # prefix-anchored (-like 'RETRY_EXHAUSTED:*'), so original error text
    # that merely mentions the token mid-message never trips it.
    $markerCur = $ex
    while ($markerCur) {
        if ($markerCur.Message -like 'RETRY_EXHAUSTED:*') {
            $result.Category = 'RetryExhausted'
            return $result
        }
        $markerCur = $markerCur.InnerException
    }

    # --- Classify by typed HTTP status code first ---
    if ($null -ne $statusCode) {
        switch ($statusCode) {
            400 { $result.Category = 'NonRetryable'; return $result }
            401 { $result.Category = 'Auth'; return $result }
            429 { $result.Category = 'Throttle'; return $result }
            503 { $result.Category = 'Throttle'; return $result }
            404 { $result.Category = 'Skippable'; return $result }
            403 {
                # Office 365 Mgmt API returns 403 (not 429) on quota throttling, with
                # body code "AF429". Per Microsoft's Mgmt API troubleshooting docs.
                # See: docs/analytics/audit-log-ingestion.md.
                if ($ApiFamily -eq 'log' -and $matchText -match 'AF429') {
                    $result.Category = 'Throttle'
                } elseif ($ApiFamily -in @('spo', 'mde')) {
                    $result.Category = 'Skippable'
                } else {
                    $result.Category = 'Auth'
                }
                return $result
            }
            409 {
                # BAP returns 409 NoPermissionsForEmbeddedApplications on Microsoft-
                # internal embedded apps (SharepointFormApp etc.) — the
                # /apps/{id}/permissions endpoint has nothing to return because role
                # assignments don't exist at the BAP layer for embedded apps (their
                # access is governed by the host SP list ACL). This is a permanent,
                # by-design boundary, not transient. See issue #344.
                if ($ApiFamily -eq 'powerplat' -and $matchText -match 'NoPermissionsForEmbeddedApplications') {
                    $result.Category = 'Skippable'
                } else {
                    $result.Category = 'NonRetryable'
                }
                return $result
            }
        }
    }

    # --- Fall back to message matching (non-HTTP exceptions) ---
    # Auth
    if ($matchText -match 'Unauthorized|token.*expired|Access token has expired|ACS50012') {
        $result.Category = 'Auth'
        return $result
    }

    # Throttle (base patterns for all APIs)
    $throttlePattern = 'TooManyRequests|throttled|Too many requests|Rate limit|Server Busy|ServerBusyException'
    if ($ApiFamily -eq 'exo') {
        $throttlePattern += '|MicroDelay|BackoffException|Too many concurrent'
    }
    if ($ApiFamily -eq 'log') {
        # Office 365 Mgmt API throttle code; see status-403 branch above.
        $throttlePattern += '|AF429'
    }
    if ($matchText -match $throttlePattern) {
        $result.Category = 'Throttle'
        return $result
    }

    # EXO concurrent-enumeration race (#343). When a sibling runspace mutates
    # shared EXO module state mid-iteration, .NET throws InvalidOperationException
    # with this message. Left as Unknown — it's a transient race, not rate
    # limiting (so Throttle would be misleading) and not a permanent skip. The
    # Unknown path already retries with backoff; this branch just tags the
    # message so the unknown_retry_event in LAW is identifiable.
    if ($ApiFamily -eq 'exo' -and $matchText -match 'Collection was modified|enumeration operation may not execute') {
        $result.Message = "EXO_CONCURRENT_MUTATION (#343): $message"
        return $result
    }

    # Skippable (per-API patterns)
    $skippablePattern = switch ($ApiFamily) {
        'graph' { 'Request_ResourceNotFound|ResourceNotFound|Synchronization_ObjectNotFound' }
        'exo'   { 'MapiExceptionNotFound|ManagementObjectNotFoundException|couldn''t be found|mailbox.*doesn''t exist|couldn''t find' }
        # "Attempted to perform an unauthorized operation" covers Get-PnPSite /
        # Get-PnPWeb 403s on system sites (contentTypeHub, CompliancePolicyCenter,
        # search) and unauthorized per-site OneDrives. Classifying as Skippable
        # turns 5×-retry + hard-error into single-attempt skip. See issue #165.
        # "not supported for site" covers SP REST 500s from GetSitePropertiesByUrl
        # on the same system sites (contentTypeHub etc.) — these are permanent
        # per-site API constraints, not transient failures. See #471.
        'spo'   { 'locked|no access|does not exist|Cannot find site|AccessDenied|SiteNotFound|Attempted to perform an unauthorized operation|not supported for site' }
        'mde'   { 'ResourceNotFound|Not Found' }
        # Empty starter set — broaden as real 404/403 patterns surface in PR-2/3
        # (per-env iteration over apps/connections/flows will produce the first
        # batch). Tracked in #241.
        'powerplat' { '' }
        # Empty starter set for powerbi — broaden as patterns surface in C2+
        # (Fabric items, gateways, workspace scan results may produce 404/403
        # patterns we don't yet know). Tracked in #242.
        'powerbi' { '' }
        # Mgmt API + Graph signIns. Empty starter set; broaden as real
        # patterns surface in branch-env runs.
        'log'     { '' }
    }
    if ([string]::IsNullOrEmpty($skippablePattern)) {
        # Skip the message regex entirely when the family has no patterns
        # registered — '' would match every message.
        return $result
    }
    if ($matchText -match $skippablePattern) {
        $result.Category = 'Skippable'
        return $result
    }

    return $result
}

function Get-RetryDelay {
    <#
    .SYNOPSIS
        Compute backoff delay in seconds for a retry attempt.
    #>
    param(
        [Parameter(Mandatory)][hashtable]$Classification,
        [Parameter(Mandatory)][int]$Attempt,
        [int]$BaseDelay = 2,
        [int]$MaxDelay = 120
    )

    if ($Classification.RetryAfter -and $Classification.RetryAfter -gt 0) {
        return $Classification.RetryAfter
    }

    $exp = [math]::Min($BaseDelay * [math]::Pow(2, $Attempt - 1), $MaxDelay)
    $jitter = Get-Random -Minimum 0.0 -Maximum ($exp * 0.3)
    return [math]::Round($exp + $jitter, 1)
}

function Invoke-WithRetry {
    param(
        [Parameter(Mandatory)][scriptblock]$ScriptBlock,
        [int]$MaxRetries = 5,
        [int]$BaseDelay = 2,
        [int]$MaxDelay = 120,
        [string]$ApiFamily = 'mde',
        [scriptblock]$OnAuthReconnect = $null
    )

    $attempt = 0
    while ($true) {
        try {
            return & $ScriptBlock
        }
        catch {
            $attempt++
            $class = Get-ErrorClassification -ErrorRecord $_ -ApiFamily $ApiFamily

            # Already-exhausted marker from a nested Invoke-WithRetry (#526):
            # rethrow immediately so stacked helpers don't multiply budgets.
            if ($class.Category -eq 'RetryExhausted') {
                throw
            }

            if ($class.Category -eq 'Skippable') {
                throw [System.InvalidOperationException]::new("SKIPPABLE: $($class.Message)", $_.Exception)
            }

            # 400 / NonRetryable: client-side error (bad query, malformed
            # body) — retries can't fix this. Throw immediately so the
            # caller surfaces the real error instead of silently looping
            # for ~5 minutes per chunk. Wrap in a new exception carrying
            # $class.Message so the body enrichment from
            # Get-ErrorClassification reaches callers that only log
            # $_.Exception.Message — a bare `throw` would surface just
            # "400 Bad Request" without the ODataError body.
            if ($class.Category -eq 'NonRetryable') {
                throw [System.InvalidOperationException]::new("NONRETRYABLE: $($class.Message)", $_.Exception)
            }

            if ($attempt -gt $MaxRetries) {
                # Exhaustion marker (#526). Unknown/Auth exhaustion means this
                # error survived a full inner retry budget — wrap it so outer
                # retry layers fail the item instead of re-running the whole
                # inner cycle (nested 5 × 5 amplification: ~40 min on one
                # persistently-500ing item, issue #526). Throttle exhaustion
                # stays a bare rethrow: 429/503 weather is about the API, not
                # the item, so the outer layer may legitimately retry later.
                if ($class.Category -in @('Unknown', 'Auth')) {
                    throw [System.InvalidOperationException]::new("RETRY_EXHAUSTED: $($class.Message)", $_.Exception)
                }
                throw
            }

            if ($class.Category -eq 'Auth') {
                # WARN (not Verbose) so auth-retry storms are visible in stdout
                # without operators having to enable $VerbosePreference. See #264.
                Write-Log "Auth error on $ApiFamily, reconnecting (attempt $attempt): $($class.Message)" -Level WARN
                if ($OnAuthReconnect) { & $OnAuthReconnect }
                continue
            }

            if ($class.Category -eq 'Throttle') {
                $delay = Get-RetryDelay -Classification $class -Attempt $attempt -BaseDelay $BaseDelay -MaxDelay $MaxDelay
                Write-Log "Throttled on $ApiFamily, backing off ${delay}s (attempt $attempt): $($class.Message)" -Level WARN
                Write-ThrottleEvent -RetryAfterSeconds ([int]$delay) -Attempt $attempt `
                    -StatusCode ([int]($class.StatusCode ?? 0)) -Message $class.Message
                Start-Sleep -Seconds $delay
                continue
            }

            # Unknown — retry with backoff (conservative). #327: emit
            # WARN + unknown_retry_event so a silent-retry storm is
            # observable. Without this, the sleep is indistinguishable
            # from a wedged container.
            $delay = Get-RetryDelay -Classification $class -Attempt $attempt -BaseDelay $BaseDelay -MaxDelay $MaxDelay
            $exType = $_.Exception.GetType().FullName
            $innerType = $exType
            $cur = $_.Exception.InnerException
            while ($cur) { $innerType = $cur.GetType().FullName; $cur = $cur.InnerException }
            Write-Log "Retrying on $ApiFamily after unknown error, backing off ${delay}s (attempt $attempt): ${exType}: $($class.Message)" -Level WARN
            Write-UnknownRetryEvent -ApiFamily $ApiFamily -Attempt $attempt -DelaySeconds ([int]$delay) `
                -StatusCode ([int]($class.StatusCode ?? 0)) -ExceptionType $exType `
                -InnerExceptionType $innerType -Message $class.Message
            Start-Sleep -Seconds $delay
        }
    }
}

Export-ModuleMember -Function Get-ErrorClassification, Get-RetryDelay, Invoke-WithRetry
