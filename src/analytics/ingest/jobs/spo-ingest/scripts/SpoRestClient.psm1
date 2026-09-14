# SPO REST client. Single source of truth for talking to /_api/* and
# /_api/$batch from the spo-ingest container. Wraps every call in
# Invoke-WithRetry -ApiFamily 'spo' so 429 Retry-After, 401 reconnect, and
# 403/404 Skippable classification come for free.
#
# Exports:
#   Invoke-SpoRest      — single REST call; unwraps the verbose envelope
#   Invoke-SpoRestPaged — pagination loop over verbose `.d.__next` cursors;
#                         streams rows through an -OnRow callback so the caller
#                         can write straight into a StageWriter without holding
#                         the full page set in memory. Verbose-only by design —
#                         see Get-SpoNextLink notes.
#   Invoke-SpoBatch     — multipart /_api/$batch composer + response parser,
#                         used for fan-out of per-item GetSharingInformation
#                         (~50 requests per HTTP). Returns one entry per
#                         sub-request with its own status + parsed body, so
#                         the caller decides whether a per-item 403 is a row-
#                         level skip or a fatal error.
#
# === Why this exists ===
# The previous SPO ingest used PnP.PowerShell's CSOM wrappers, which silently
# swallowed HTTP 429 responses and sat on the connection until the .NET
# HttpClient.Timeout of 100s fired (#461). When we probed the same calls
# directly against the SharePoint REST API the 429s surfaced in <0.5s with a
# real Retry-After header — i.e. throttling was real but unobservable through
# PnP. The rewrite drops PnP entirely; this module owns the REST surface.
#
# === Accept header choice ===
# Default Accept is `application/json;odata=verbose`. The verbose envelope is
# the only shape we observed reliably across all SP REST endpoints — the
# nometadata variant 406'd on combined `$expand` and on a few admin-tenant
# endpoints during the live probe. Verbose is bytes-heavier but stable.
# Unwrapping `.d` / `.d.results` happens in Invoke-SpoRest so callers see a
# uniform record shape.

# Per-call HTTP timeout. SPO REST is fast (<2s steady-state on warm-token
# CSOM calls were the slow path, not REST), but the per-site Lists call can
# legitimately take 30-60s on Scale_Team_* sites with thousands of lists.
# 90s gives headroom without masking a real hang.
$script:SpoHttpTimeoutSec = 90

# Default Accept header. See module header.
$script:SpoAcceptVerbose = 'application/json;odata=verbose'

# User-Agent string sent on every SP REST / $batch call. SharePoint Online
# prioritizes "well-decorated" traffic and is more likely to throttle calls
# that arrive without an AppID + User-Agent string. The format is the
# enterprise (non-ISV) convention `NONISV|CompanyName|AppName/Version` from
# https://learn.microsoft.com/sharepoint/dev/general-development/how-to-avoid-getting-throttled-or-blocked-in-sharepoint-online
# — pipe-separated company/app, slash-separated version, per RFC 2616.
#
# This is the single source of truth for the SPO ingest User-Agent. Connect.psm1
# and the entity modules consume it via the exported Get-SpoUserAgent below
# rather than repeating the literal (script-scope vars are module-private, so
# the value can't be read across modules directly).
$script:SpoUserAgent = 'NONISV|Microsoft|MaToolkit-SpoIngest/1.0'

function Get-SpoUserAgent {
    <#
    .SYNOPSIS
        The SPO ingest User-Agent string. Single source of truth for all
        SP REST / $batch / Graph traffic across the container's modules.
    #>
    [CmdletBinding()]
    param()
    $script:SpoUserAgent
}

function Invoke-SpoRest {
    <#
    .SYNOPSIS
        Single SP REST call. Wraps Invoke-WithRetry, unwraps the verbose envelope.
    .PARAMETER Url
        Full absolute URL, e.g. https://tenant.sharepoint.com/sites/X/_api/web.
    .PARAMETER Method
        HTTP method. Default GET.
    .PARAMETER Body
        Optional request body. Pass a string for already-serialized JSON.
    .PARAMETER Token
        Bearer access token for the matching SP audience. Caller resolves via
        Get-SpoToken in Connect.psm1.
    .PARAMETER OnAuthReconnect
        Optional scriptblock that re-mints the token on 401. Same contract as
        Invoke-WithRetry's -OnAuthReconnect. Closure should update the caller's
        token-by-reference (typically a hashtable slot) so the next attempt uses
        the new bearer.
    .PARAMETER GetToken
        Optional scriptblock returning the current bearer. Called on every
        attempt. Lets a caller mint a fresh token per attempt without
        materializing the closure pattern above.
    .OUTPUTS
        For OData collections (`{"d":{"results":[...]}}` shape): the [object[]]
        results array.
        For single-entity responses (`{"d":{...}}` shape): the inner object.
        For raw responses (e.g. nometadata or non-OData endpoints): the parsed
        object as-is.
        For empty responses (204): $null.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Url,
        [string]$Method = 'GET',
        [object]$Body,
        [string]$Token,
        [scriptblock]$OnAuthReconnect,
        [scriptblock]$GetToken,
        [string]$Accept = $script:SpoAcceptVerbose,
        [string]$ContentType = 'application/json;odata=verbose',
        [int]$TimeoutSec = $script:SpoHttpTimeoutSec
    )

    if (-not $Token -and -not $GetToken) {
        throw "Invoke-SpoRest requires either -Token or -GetToken"
    }
    # When -OnAuthReconnect is supplied, the retry path needs a refreshable
    # token source — -Token captures the bearer at call-entry and the next
    # attempt would re-send the same stale value, defeating reconnect. Force
    # callers using reconnect to pass -GetToken (a closure that re-resolves
    # against the updated cache).
    if ($OnAuthReconnect -and -not $GetToken) {
        throw "Invoke-SpoRest with -OnAuthReconnect requires -GetToken (a refreshable token source). -Token alone caches a stale value across retries."
    }

    $response = Invoke-WithRetry -ApiFamily 'spo' -OnAuthReconnect $OnAuthReconnect -ScriptBlock {
        # GetToken (if supplied) is read fresh per attempt — OnAuthReconnect
        # updates the underlying source, GetToken pulls the new value. Lets
        # the closure-by-reference pattern stay invisible to callers that just
        # hand us a captured $Token.
        $bearer = if ($GetToken) { & $GetToken } else { $Token }
        $headers = @{
            Authorization = "Bearer $bearer"
            Accept        = $Accept
            'User-Agent'  = $script:SpoUserAgent
        }
        # Use Invoke-WebRequest + explicit ConvertFrom-Json. Invoke-RestMethod's
        # auto-JSON-parsing silently breaks on SP's `/Items` endpoint because SP
        # serves it with `Transfer-Encoding: chunked` — PowerShell 7's parser
        # returns the body as `System.String` instead of a PSCustomObject for
        # chunked responses, regardless of Content-Type. ConvertFrom-Json works
        # deterministically regardless of transfer encoding.
        $params = @{
            Uri         = $Url
            Method      = $Method
            Headers     = $headers
            TimeoutSec  = $TimeoutSec
            ErrorAction = 'Stop'
        }
        if ($null -ne $Body) {
            $params['Body'] = $Body
            $params['ContentType'] = $ContentType
        }
        $webResp = Invoke-WebRequest @params
        if ($webResp.StatusCode -eq 204 -or [string]::IsNullOrEmpty($webResp.Content)) {
            return $null
        }
        # -AsHashtable handles two SP REST quirks at once:
        #   1. Some endpoints (/Items in particular) return JSON with BOTH `Id`
        #      and `ID` properties — default ConvertFrom-Json fails with
        #      "contains keys with different casing". PowerShell hashtables are
        #      case-insensitive by default; the second copy silently overwrites.
        #   2. Downstream Resolve-SpoVerboseRecord checks `-is IDictionary`
        #      first, so the hashtable shape is the cheaper path anyway.
        # Depth 20 covers SP's deepest verbose shapes (GetSharingInformation's
        # nested permissionsInformation graph). Default depth 1024 is fine but
        # explicit is clearer.
        return $webResp.Content | ConvertFrom-Json -AsHashtable -Depth 20
    }

    return ConvertFrom-SpoEnvelope -Response $response
}

function ConvertFrom-SpoEnvelope {
    <#
    .SYNOPSIS
        Unwrap the verbose OData envelope. Returns:
          - results array for {"d":{"results":[...]}}
          - inner .d object for {"d":{...}} single-entity
          - raw object for non-verbose shapes
          - $null if response is null/empty

    The `,` (comma) operator on every array return is load-bearing: PowerShell
    unwraps single-element arrays on the function output stream, so a plain
    `return @($d.results)` of a 1-element results page would emit the single
    inner object instead of a 1-element array — the caller's foreach would
    iterate the object's PROPERTIES, not its rows. Commas force-wrap.
    #>
    param($Response)

    if ($null -eq $Response) { return $null }

    # Walk the .d / .results unwrap. Handle both IDictionary (from
    # ConvertFrom-Json -AsHashtable, the production path post-#466) and
    # PSCustomObject (legacy / test fixtures) shapes.
    $d = $null
    if ($Response -is [System.Collections.IDictionary]) {
        if ($Response.Contains('d')) { $d = $Response['d'] }
    } elseif ($Response.PSObject.Properties['d']) {
        $d = $Response.d
    }
    if ($null -ne $d) {
        if ($d -is [System.Collections.IDictionary] -and $d.Contains('results')) {
            return ,@($d['results'])
        }
        if ($d.PSObject -and $d.PSObject.Properties['results']) {
            return ,@($d.results)
        }
        return $d
    }
    # nometadata / non-OData shapes pass through. A response with a top-level
    # `value` array (e.g. some admin REST endpoints) is left for the caller to
    # walk — we don't presume `value` is always a results array, because some
    # SP responses use `value` as a scalar field name.
    return $Response
}

function Get-SpoNextLink {
    <#
    .SYNOPSIS
        Extract the next-page URL from a verbose SP response (.d.__next).
        Returns $null when there's no next page.
    .NOTES
        Verbose-only by design — every call in this client uses
        `Accept: application/json;odata=verbose`, so SP returns the next-page
        cursor as `d.__next`. The nometadata `odata.nextLink` / Graph-style
        `@odata.nextLink` shapes are not supported because
        `ConvertFrom-SpoEnvelope` doesn't extract their `value`-array bodies.
        If a future caller needs nometadata paging, fix both functions
        together — partial support would silently zero-row-loop.
    #>
    param($Response)

    if ($null -eq $Response) { return $null }

    # Handle both IDictionary and PSCustomObject input shapes — see
    # ConvertFrom-SpoEnvelope for the same dual handling.
    $d = $null
    if ($Response -is [System.Collections.IDictionary]) {
        if ($Response.Contains('d')) { $d = $Response['d'] }
    } elseif ($Response.PSObject.Properties['d']) {
        $d = $Response.d
    }
    if ($null -eq $d) { return $null }

    if ($d -is [System.Collections.IDictionary]) {
        if ($d.Contains('__next') -and -not [string]::IsNullOrWhiteSpace($d['__next'])) {
            return [string]$d['__next']
        }
    } elseif ($d.PSObject.Properties['__next'] -and -not [string]::IsNullOrWhiteSpace($d.__next)) {
        return [string]$d.__next
    }
    return $null
}

function Invoke-SpoRestPaged {
    <#
    .SYNOPSIS
        Pagination loop over SP REST. Calls -OnRow for each record so the
        caller can stream into a StageWriter without buffering whole pages.
    .PARAMETER Url
        Starting page URL. Subsequent pages come from the response's __next.
    .PARAMETER OnRow
        Scriptblock invoked once per record. Receives the record object.
        Closure responsibility: capture any state (writer, counters) needed.
    .OUTPUTS
        Total row count.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Url,
        [Parameter(Mandatory)][scriptblock]$OnRow,
        [string]$Token,
        [scriptblock]$OnAuthReconnect,
        [scriptblock]$GetToken,
        [string]$Method = 'GET',
        [object]$Body,
        [string]$Accept = $script:SpoAcceptVerbose,
        [int]$TimeoutSec = $script:SpoHttpTimeoutSec
    )

    if (-not $Token -and -not $GetToken) {
        throw "Invoke-SpoRestPaged requires either -Token or -GetToken"
    }
    # See Invoke-SpoRest's identical guard — reconnect needs a refreshable
    # token source so the retry attempt doesn't re-send the stale bearer.
    if ($OnAuthReconnect -and -not $GetToken) {
        throw "Invoke-SpoRestPaged with -OnAuthReconnect requires -GetToken (a refreshable token source). -Token alone caches a stale value across retries."
    }

    $next = $Url
    $count = 0
    while ($next) {
        $rawResponse = Invoke-WithRetry -ApiFamily 'spo' -OnAuthReconnect $OnAuthReconnect -ScriptBlock {
            $bearer = if ($GetToken) { & $GetToken } else { $Token }
            $headers = @{
                Authorization = "Bearer $bearer"
                Accept        = $Accept
                'User-Agent'  = $script:SpoUserAgent
            }
            # Invoke-WebRequest + explicit ConvertFrom-Json. Invoke-RestMethod
            # returns the body as `System.String` instead of a PSCustomObject
            # on responses with `Transfer-Encoding: chunked` (regardless of
            # Content-Type). SP's `/Items` endpoint sends chunked — without
            # this every per-list item enumeration returned 0 rows.
            $params = @{
                Uri         = $next
                Method      = $Method
                Headers     = $headers
                TimeoutSec  = $TimeoutSec
                ErrorAction = 'Stop'
            }
            if ($null -ne $Body) {
                $params['Body'] = $Body
                $params['ContentType'] = 'application/json;odata=verbose'
            }
            try {
                $webResp = Invoke-WebRequest @params
            } catch {
                # Diagnostic for #466: log everything we can find about the
                # error before re-throwing. Different exception types populate
                # different members (PowerShell 7 / .NET 8 HttpResponseException
                # vs older shapes), so probe each in turn and emit them all.
                $exType = if ($_.Exception) { $_.Exception.GetType().FullName } else { '<null>' }
                $statusCode = $null
                if ($_.Exception -and $_.Exception.Response) { $statusCode = $_.Exception.Response.StatusCode }
                $errorDetails = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { '<no ErrorDetails>' }
                $exceptionMsg = if ($_.Exception) { $_.Exception.Message } else { '<no Exception.Message>' }
                $responseBody = '<no Response stream>'
                if ($_.Exception -and $_.Exception.Response) {
                    try {
                        $stream = $_.Exception.Response.GetResponseStream()
                        if ($stream) {
                            $reader = [System.IO.StreamReader]::new($stream)
                            $responseBody = $reader.ReadToEnd()
                        }
                    } catch { $responseBody = "<read failed: $_>" }
                }
                $log = "SpoRestPaged HTTP error url='$next' exType=$exType status=$statusCode exMsg='$exceptionMsg' errorDetails='$($errorDetails.Substring(0, [Math]::Min(400, $errorDetails.Length)))' body='$($responseBody.Substring(0, [Math]::Min(400, $responseBody.Length)))'"
                Write-Log $log -Level WARN
                throw
            }
            if ($webResp.StatusCode -eq 204 -or [string]::IsNullOrEmpty($webResp.Content)) {
                return $null
            }
            # -AsHashtable handles two SP REST quirks at once:
        #   1. Some endpoints (/Items in particular) return JSON with BOTH `Id`
        #      and `ID` properties — default ConvertFrom-Json fails with
        #      "contains keys with different casing". PowerShell hashtables are
        #      case-insensitive by default; the second copy silently overwrites.
        #   2. Downstream Resolve-SpoVerboseRecord checks `-is IDictionary`
        #      first, so the hashtable shape is the cheaper path anyway.
        # Depth 20 covers SP's deepest verbose shapes (GetSharingInformation's
        # nested permissionsInformation graph). Default depth 1024 is fine but
        # explicit is clearer.
        return $webResp.Content | ConvertFrom-Json -AsHashtable -Depth 20
        }

        $rows = ConvertFrom-SpoEnvelope -Response $rawResponse
        # Single-entity shapes (verbose `{"d":{...}}` without `results`) shouldn't
        # be passed through Invoke-SpoRestPaged in practice, but guard with a
        # type check so we don't iterate the property bag of an entity object
        # and emit per-property OnRow calls. Treat a non-array/non-collection
        # response as zero rows for the page.
        if ($null -ne $rows -and ($rows -is [System.Array] -or $rows -is [System.Collections.IList])) {
            foreach ($row in $rows) {
                & $OnRow $row
                $count++
            }
        }

        $next = Get-SpoNextLink -Response $rawResponse
        # POST-paginated endpoints (e.g. GetSitePropertiesFromSharePointByFilters)
        # echo the same body on every page until they exhaust. We can't replay
        # the original body verbatim because the admin endpoint encodes the
        # next start-index inside the JSON; that's why Get-SpoSitesRoot uses a
        # manual loop instead of Invoke-SpoRestPaged. Keep this function GET-
        # only by resetting -Body to $null after the first page so a caller
        # that does pass -Body with -Method GET (legal — some endpoints
        # support GET-with-body) doesn't double-send it.
        $Body = $null
        $Method = 'GET'
    }
    return $count
}

function Invoke-SpoBatch {
    <#
    .SYNOPSIS
        Multipart /_api/$batch composer + response parser. Each sub-request
        is one batched HTTP call; the response is one parsed entry per
        sub-request with status + parsed body. Use for fan-out of per-item
        operations (GetSharingInformation, GetById, etc.) where ~50 per HTTP
        cuts latency vs N serial calls.
    .PARAMETER BatchUrl
        Absolute /_api/$batch endpoint for the target site, e.g.
        https://tenant.sharepoint.com/sites/X/_api/$batch
    .PARAMETER Requests
        Array of hashtables. Each entry:
          @{
            Method      = 'GET' | 'POST' | ...
            Url         = '/_api/...' (relative or absolute)
            Body        = optional [string] — JSON for POST
            ContentType = optional, defaults to application/json;odata=verbose
            Accept      = optional, defaults to application/json;odata=verbose
          }
    .OUTPUTS
        Array of @{ Status; Headers; Body } — one per sub-request, in order.
        Body is the parsed JSON object (envelope already unwrapped via
        ConvertFrom-SpoEnvelope). When a sub-request fails, Body is the raw
        text — the caller switches on Status to decide whether to parse.
    .NOTES
        Write sub-requests (POST/PUT/MERGE/DELETE, including action
        invocations like GetSharingInformation) are wrapped in a ChangeSet
        by Build-SpoBatchBody — SP's $batch enforces the OData spec and
        400s on bare-part writes with a misleading "An invalid HTTP method
        'POST' was detected for a query operation." Each write goes in its
        own ChangeSet; per the SP $batch docs, multi-write ChangeSets are
        not guaranteed transactional anyway, so one-write-per-ChangeSet
        loses nothing and keeps response parsing straightforward. GETs
        remain as flat batch parts (no wrapper). See Build-SpoBatchBody for
        the wire-format detail.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$BatchUrl,
        [Parameter(Mandatory)][array]$Requests,
        [string]$Token,
        [scriptblock]$OnAuthReconnect,
        [scriptblock]$GetToken,
        [int]$TimeoutSec = $script:SpoHttpTimeoutSec
    )

    if (-not $Token -and -not $GetToken) {
        throw "Invoke-SpoBatch requires either -Token or -GetToken"
    }
    # See Invoke-SpoRest's identical guard — reconnect needs a refreshable
    # token source so the retry attempt doesn't re-send the stale bearer.
    if ($OnAuthReconnect -and -not $GetToken) {
        throw "Invoke-SpoBatch with -OnAuthReconnect requires -GetToken (a refreshable token source). -Token alone caches a stale value across retries."
    }
    if ($Requests.Count -eq 0) { return @() }

    # Boundary tokens. Distinct per call so a captured wire trace can't be
    # confused between two batches.
    $batchBoundary = "batch_$([guid]::NewGuid().ToString('N'))"

    $body = Build-SpoBatchBody -Requests $Requests -Boundary $batchBoundary
    $batchContentType = "multipart/mixed; boundary=$batchBoundary"
    $expectedCount = $Requests.Count

    # Parse the multipart response INSIDE the Invoke-WithRetry scriptblock so
    # we can synthesize a typed exception when any sub-response is retryable.
    # Without this, retryable statuses inside the batch envelope were reaching
    # the caller as Status=429/401/5xx and being written as permanent skips —
    # the exact regression #460 was reporting on the PnP path. Sub-requests
    # are idempotent reads (GetSharingInformation, GET /Items), so re-sending
    # the whole batch when any sub-response is retryable is safe.
    #
    # Only 403/404 sub-responses stay in the returned array — those are
    # permanent per-item denials the caller silently skips. Everything
    # else gets re-thrown with an HttpRequestException that RetryHelper
    # classifies for an appropriate backoff / reconnect policy.
    $results = Invoke-WithRetry -ApiFamily 'spo' -OnAuthReconnect $OnAuthReconnect -ScriptBlock {
        $bearer = if ($GetToken) { & $GetToken } else { $Token }
        $headers = @{
            Authorization = "Bearer $bearer"
            Accept        = 'application/json;odata=verbose'
            'User-Agent'  = $script:SpoUserAgent
        }
        try {
            $response = Invoke-WebRequest -Uri $BatchUrl `
                -Method POST `
                -Headers $headers `
                -ContentType $batchContentType `
                -Body $body `
                -TimeoutSec $TimeoutSec `
                -ErrorAction Stop
        } catch {
            # Diagnostic for #466 — Invoke-SpoBatch HTTP-level failure (the
            # batch envelope POST itself, not a sub-response within the body).
            # Log everything we can find about the error, plus the first
            # ~300 chars of the batch body so we know what request we sent.
            $exType = if ($_.Exception) { $_.Exception.GetType().FullName } else { '<null>' }
            $statusCode = $null
            if ($_.Exception -and $_.Exception.Response) { $statusCode = $_.Exception.Response.StatusCode }
            $errorDetails = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { '<no ErrorDetails>' }
            $exceptionMsg = if ($_.Exception) { $_.Exception.Message } else { '<no Exception.Message>' }
            $responseBody = '<no Response stream>'
            if ($_.Exception -and $_.Exception.Response) {
                try {
                    $stream = $_.Exception.Response.GetResponseStream()
                    if ($stream) {
                        $reader = [System.IO.StreamReader]::new($stream)
                        $responseBody = $reader.ReadToEnd()
                    }
                } catch { $responseBody = "<read failed: $_>" }
            }
            $bodySnippet = if ($body.Length -gt 300) { $body.Substring(0, 300) } else { $body }
            $log = "SpoBatch HTTP error url='$BatchUrl' exType=$exType status=$statusCode exMsg='$exceptionMsg' errorDetails='$($errorDetails.Substring(0, [Math]::Min(400, $errorDetails.Length)))' body='$($responseBody.Substring(0, [Math]::Min(400, $responseBody.Length)))' requestBodyHead='$bodySnippet'"
            Write-Log $log -Level WARN
            throw
        }

        # Outer Content-Type carries the response boundary, e.g.
        #   multipart/mixed; boundary=batchresponse_<guid>
        $responseContentType = $null
        foreach ($key in 'Content-Type', 'content-type') {
            if ($response.Headers.Keys -contains $key) {
                $responseContentType = ($response.Headers[$key] | Select-Object -First 1)
                break
            }
        }
        if (-not $responseContentType) {
            throw "Invoke-SpoBatch response missing Content-Type header — cannot parse multipart body"
        }
        $responseBoundary = $null
        if ($responseContentType -match 'boundary=([^;\s]+)') {
            $responseBoundary = $Matches[1].Trim('"')
        }
        if (-not $responseBoundary) {
            throw "Invoke-SpoBatch response Content-Type has no boundary token: '$responseContentType'"
        }
        # Invoke-WebRequest returns Content as byte[] for non-text Content-Types,
        # which multipart/mixed is. Decode UTF-8 explicitly before parsing.
        # Falling through with a [byte[]] would hit a String-cast failure in the
        # ConvertFrom-SpoBatchResponse param binding (surfaces as a generic
        # "Cannot convert value to type System.String" with no URL context).
        $rawText = if ($response.Content -is [byte[]]) {
            [System.Text.Encoding]::UTF8.GetString($response.Content)
        } else {
            [string]$response.Content
        }
        $parsed = ConvertFrom-SpoBatchResponse -RawText $rawText -Boundary $responseBoundary -ExpectedCount $expectedCount

        # Sub-response retryable-status detection. Three buckets, checked in
        # priority order:
        #   401         — token expired mid-batch. Reconnect THEN retry; no
        #                 point re-sending the batch with the same expired
        #                 token. RetryHelper classifies 401 as 'Auth' and
        #                 calls OnAuthReconnect before the next attempt.
        #   429 / 503   — throttle. Back off using the strongest Retry-After
        #                 hint from the throttled subset.
        #   500/502/504 — transient server-side failure. Standard backoff.
        $authFailed = @($parsed | Where-Object { $_.Status -eq 401 })
        if ($authFailed.Count -gt 0) {
            $fakeResp = [pscustomobject]@{ StatusCode = 401; Headers = @{} }
            $msg = "Invoke-SpoBatch: $($authFailed.Count) of $($parsed.Count) sub-response(s) returned 401 — token likely expired mid-batch. Reconnecting before re-send."
            $ex = [System.Net.Http.HttpRequestException]::new(
                $msg, $null, [System.Net.HttpStatusCode]::Unauthorized)
            $ex | Add-Member -NotePropertyName Response -NotePropertyValue $fakeResp
            throw $ex
        }

        $throttled = @($parsed | Where-Object { $_.Status -in 429, 503 })
        if ($throttled.Count -gt 0) {
            $maxRetryAfter = 0
            foreach ($t in $throttled) {
                if ($t.Headers) {
                    $ra = $t.Headers['Retry-After']
                    if ($ra) {
                        $raInt = 0
                        if ([int]::TryParse([string]$ra, [ref]$raInt) -and $raInt -gt $maxRetryAfter) {
                            $maxRetryAfter = $raInt
                        }
                    }
                }
            }
            $fakeResp = [pscustomobject]@{
                StatusCode = 429
                Headers    = @{ 'Retry-After' = [string]$maxRetryAfter }
            }
            $msg = "Invoke-SpoBatch: $($throttled.Count) of $($parsed.Count) sub-response(s) throttled (status in 429/503). Re-sending the whole batch — sub-requests are idempotent reads."
            $ex = [System.Net.Http.HttpRequestException]::new(
                $msg, $null, [System.Net.HttpStatusCode]::TooManyRequests)
            $ex | Add-Member -NotePropertyName Response -NotePropertyValue $fakeResp
            throw $ex
        }

        $transient = @($parsed | Where-Object { $_.Status -in 500, 502, 504 })
        if ($transient.Count -gt 0) {
            $statusList = ($transient | ForEach-Object { $_.Status } | Sort-Object -Unique) -join ','
            $worstStatus = ($transient | Sort-Object @{Expression={$_.Status}; Descending=$true} | Select-Object -First 1).Status
            $fakeResp = [pscustomobject]@{ StatusCode = $worstStatus; Headers = @{} }
            $msg = "Invoke-SpoBatch: $($transient.Count) of $($parsed.Count) sub-response(s) returned 5xx ($statusList) — transient, re-sending the whole batch."
            $ex = [System.Net.Http.HttpRequestException]::new(
                $msg, $null, [int]$worstStatus)
            $ex | Add-Member -NotePropertyName Response -NotePropertyValue $fakeResp
            throw $ex
        }

        # Final tightening: only 2xx, 403, and 404 are allowed through to the
        # caller. 403/404 are legitimate per-item denials that the caller
        # silently skips. Anything else — 400 (bad request), 405 (method not
        # allowed), 409 (conflict), 412 (precondition failed), 410 (gone),
        # etc. — indicates we generated a bad request and should fail loudly.
        # Throw as NonRetryable (HTTP 400 maps to that in RetryHelper) so
        # the framework's stage_failed event records it.
        $unexpected = @($parsed | Where-Object {
            $s = [int]$_.Status
            -not ($s -ge 200 -and $s -lt 300) -and $s -ne 403 -and $s -ne 404
        })
        if ($unexpected.Count -gt 0) {
            $statusList = ($unexpected | ForEach-Object { $_.Status } | Sort-Object -Unique) -join ','
            $worstStatus = [int]$unexpected[0].Status
            $fakeResp = [pscustomobject]@{ StatusCode = $worstStatus; Headers = @{} }
            $bodySnippet = ''
            if ($unexpected[0].Body) {
                $bodyStr = if ($unexpected[0].Body -is [string]) { $unexpected[0].Body } else { ($unexpected[0].Body | ConvertTo-Json -Depth 4 -Compress) }
                $bodySnippet = " body=$($bodyStr.Substring(0, [Math]::Min(300, $bodyStr.Length)))"
            }
            $msg = "Invoke-SpoBatch: $($unexpected.Count) of $($parsed.Count) sub-response(s) returned unexpected non-2xx status ($statusList) — failing fast.$bodySnippet"
            $ex = [System.Net.Http.HttpRequestException]::new(
                $msg, $null, $worstStatus)
            $ex | Add-Member -NotePropertyName Response -NotePropertyValue $fakeResp
            throw $ex
        }

        # Comma operator preserves the array shape across the function
        # output stream — single-sub-response batches would otherwise unwrap
        # to a bare hashtable, breaking the caller's `$responses.Count` /
        # `$responses[$j]` per-item iteration.
        return ,$parsed
    }
    return ,$results
}

function Build-SpoBatchBody {
    <#
    Compose the multipart/mixed body for /_api/$batch. RFC-7230 line endings
    (CRLF) are required by SP — LF-only bodies parse fine in places but the
    admin endpoint occasionally 400s on them. Build with a StringBuilder and
    emit `\r\n` explicitly.

    Write operations (POST/PUT/MERGE/DELETE — including action invocations
    like GetSharingInformation) MUST be wrapped in a ChangeSet per the OData
    $batch spec. SP's REST $batch endpoint enforces this strictly: an
    unwrapped POST sub-request gets 400 with `Microsoft.Data.OData.ODataException`
    "An invalid HTTP method 'POST' was detected for a query operation."
    (the error misleadingly says "query operation" — the underlying issue
    is that bare parts in a $batch are interpreted as query/read operations
    and only accept GET). Each write goes in its own ChangeSet — SP doesn't
    treat multi-write changesets as atomic anyway (per
    learn.microsoft.com/sharepoint/dev/sp-add-ins/make-batch-requests-with-the-rest-apis),
    so the per-sub-request granularity loses nothing and keeps response
    parsing straightforward.
    #>
    param(
        [Parameter(Mandatory)][array]$Requests,
        [Parameter(Mandatory)][string]$Boundary
    )

    $sb = [System.Text.StringBuilder]::new()
    foreach ($req in $Requests) {
        $method = if ($req.Method) { $req.Method.ToUpper() } else { 'GET' }
        $url    = $req.Url
        if ([string]::IsNullOrEmpty($url)) {
            throw "Invoke-SpoBatch sub-request is missing 'Url'"
        }
        $accept = if ($req.Accept)      { $req.Accept }      else { 'application/json;odata=verbose' }
        $ctype  = if ($req.ContentType) { $req.ContentType } else { 'application/json;odata=verbose' }
        $isWrite = $method -in 'POST','PUT','MERGE','DELETE','PATCH'

        if ($isWrite) {
            $csBoundary = "changeset_$([guid]::NewGuid().ToString('N'))"
            [void]$sb.Append("--$Boundary`r`n")
            [void]$sb.Append("Content-Type: multipart/mixed; boundary=$csBoundary`r`n")
            [void]$sb.Append("`r`n")
            [void]$sb.Append("--$csBoundary`r`n")
            [void]$sb.Append("Content-Type: application/http`r`n")
            [void]$sb.Append("Content-Transfer-Encoding: binary`r`n")
            [void]$sb.Append("`r`n")
            [void]$sb.Append("$method $url HTTP/1.1`r`n")
            [void]$sb.Append("Accept: $accept`r`n")
            [void]$sb.Append("User-Agent: $($script:SpoUserAgent)`r`n")
            [void]$sb.Append("Content-Type: $ctype`r`n")
            [void]$sb.Append("`r`n")
            if ($null -ne $req.Body) {
                [void]$sb.Append([string]$req.Body)
            }
            [void]$sb.Append("`r`n")
            [void]$sb.Append("--$csBoundary--`r`n")
        } else {
            [void]$sb.Append("--$Boundary`r`n")
            [void]$sb.Append("Content-Type: application/http`r`n")
            [void]$sb.Append("Content-Transfer-Encoding: binary`r`n")
            [void]$sb.Append("`r`n")
            [void]$sb.Append("$method $url HTTP/1.1`r`n")
            [void]$sb.Append("Accept: $accept`r`n")
            [void]$sb.Append("User-Agent: $($script:SpoUserAgent)`r`n")
            [void]$sb.Append("`r`n")
        }
    }
    [void]$sb.Append("--$Boundary--`r`n")
    return $sb.ToString()
}

function ConvertFrom-SpoBatchResponse {
    <#
    Parse a multipart/mixed response from /_api/$batch. Returns
    @{ Status; Headers; Body } per sub-request in input order.

    Each part body is the inner HTTP response: status line, headers, blank,
    body. We split on the part boundary, locate the embedded HTTP envelope,
    extract status and body, and JSON-parse the body when Content-Type
    looks like JSON.

    Robustness: SP sometimes emits an inner `Content-Type: application/http`
    on the part body (the wrapping) and sometimes nests `multipart/mixed`
    when a request was wrapped in a changeset by the server. The parser
    walks every embedded HTTP envelope it finds in input order. If
    ExpectedCount is supplied and we under-count, we throw — partial parses
    almost always indicate a malformed input, and silently dropping a
    sub-result would land zero rows where rows were expected.
    #>
    param(
        [Parameter(Mandatory)][string]$RawText,
        [Parameter(Mandatory)][string]$Boundary,
        [int]$ExpectedCount = 0
    )

    $results = [System.Collections.Generic.List[object]]::new()

    # Split on the boundary. Each part starts with `--<boundary>` and the
    # closing marker is `--<boundary>--`. The first segment before the first
    # boundary is preamble (usually empty). The last segment after the
    # closing marker is epilogue.
    $marker = "--$Boundary"
    $parts = $RawText -split [regex]::Escape($marker)
    foreach ($part in $parts) {
        $trim = $part.Trim()
        if ([string]::IsNullOrEmpty($trim)) { continue }
        if ($trim -eq '--') { continue }  # closing marker
        if (-not ($trim -match '(?ms)HTTP/\d\.\d\s+(\d{3})')) {
            # Some preambles carry only the wrapping headers (Content-Type:
            # application/http) — no inner HTTP envelope to parse. Skip.
            continue
        }

        # Find the inner HTTP envelope. The part itself looks like:
        #   Content-Type: application/http
        #   Content-Transfer-Encoding: binary
        #   <blank>
        #   HTTP/1.1 200 OK
        #   <inner headers>
        #   <blank>
        #   <body>
        # Locate the HTTP/N.N line and parse from there.
        $httpStart = $part.IndexOf('HTTP/')
        if ($httpStart -lt 0) { continue }
        $inner = $part.Substring($httpStart)

        # Status from the first line.
        $firstLineEnd = $inner.IndexOf("`n")
        if ($firstLineEnd -lt 0) { continue }
        $firstLine = $inner.Substring(0, $firstLineEnd).Trim()
        $statusCode = $null
        if ($firstLine -match '^HTTP/\d\.\d\s+(\d{3})') {
            $statusCode = [int]$Matches[1]
        }

        # Headers and body. Body starts after the first blank line (CRLFCRLF
        # or LFLF, depending on what the server sent). Normalize both.
        $afterStatus = $inner.Substring($firstLineEnd + 1)
        $bodyStart = $afterStatus.IndexOf("`r`n`r`n")
        if ($bodyStart -lt 0) { $bodyStart = $afterStatus.IndexOf("`n`n") }
        $headerText = if ($bodyStart -gt 0) { $afterStatus.Substring(0, $bodyStart) } else { $afterStatus }
        $bodyText   = if ($bodyStart -gt 0) {
            $afterStatus.Substring($bodyStart).TrimStart("`r","`n")
        } else { '' }

        # Parse inner headers into a hashtable. Stop at the first blank line.
        $headers = @{}
        foreach ($line in ($headerText -split "`r?`n")) {
            if ([string]::IsNullOrWhiteSpace($line)) { continue }
            $colon = $line.IndexOf(':')
            if ($colon -lt 1) { continue }
            $hname = $line.Substring(0, $colon).Trim()
            $hval  = $line.Substring($colon + 1).Trim()
            $headers[$hname] = $hval
        }

        # Body parsing: only attempt JSON when the inner Content-Type says so.
        # SP sometimes returns an HTML error page for a 500 — leaving Body as
        # the raw text lets the caller surface it without a confusing JSON
        # parse error masking the real failure.
        $body = $null
        $innerContentType = $headers['Content-Type']
        if (-not $innerContentType) { $innerContentType = $headers['content-type'] }
        if ($bodyText -and $innerContentType -match 'json') {
            try {
                # -AsHashtable for the same reasons as Invoke-SpoRest: handles
                # duplicate-cased keys (Id vs ID on /Items sub-requests in
                # batches) and keeps the IDictionary shape Resolve-SpoVerboseRecord
                # expects.
                $parsed = $bodyText | ConvertFrom-Json -AsHashtable -Depth 20 -ErrorAction Stop
                $body = ConvertFrom-SpoEnvelope -Response $parsed
            } catch {
                $body = $bodyText
            }
        } elseif ($bodyText) {
            $body = $bodyText
        }

        $results.Add(@{
            Status  = $statusCode
            Headers = $headers
            Body    = $body
        })
    }

    if ($ExpectedCount -gt 0 -and $results.Count -ne $ExpectedCount) {
        throw "Invoke-SpoBatch: parsed $($results.Count) sub-responses but sent $ExpectedCount sub-requests. Multipart response was malformed or partially truncated."
    }
    # Comma operator: PowerShell's function output stream unwraps a single-
    # element array to its inner value, which would turn a 1-sub-response
    # batch into a bare hashtable at the caller. `,` force-wraps so the
    # shape stays Array regardless of length.
    return ,$results.ToArray()
}

Export-ModuleMember -Function `
    Invoke-SpoRest, Invoke-SpoRestPaged, Invoke-SpoBatch, `
    ConvertFrom-SpoEnvelope, Get-SpoNextLink, `
    Build-SpoBatchBody, ConvertFrom-SpoBatchResponse, `
    Get-SpoUserAgent
