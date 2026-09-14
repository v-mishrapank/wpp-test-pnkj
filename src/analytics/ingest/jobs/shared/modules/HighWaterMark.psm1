# High-water-mark sentinel state for log-ingest entities.
#
# Stores per-(entity, tenant) timestamps marking the last successfully-landed
# record's source-time, so the next run can fetch from that point forward
# instead of re-pulling overlapping windows. Sentinels live at
#   _state/<entity>/<tenant_key>/high_water_mark.json
# inside the same landing container as the entity's records. The `_state/`
# prefix keeps these out of DLT bronze cloudFiles globs (Spark filters `_*`
# paths by default).
#
# Schema:
#   { high_water_mark, last_run_id, last_run_completed_at, schema_version: 1 }
#
# Get-HighWaterMark returns $null on 404 (first run) — caller treats this as
# "no prior run" and falls back to a default startTime (typically now-1h).

Import-Module (Join-Path $PSScriptRoot 'StorageHelperRest.psm1') -Force

function Invoke-HwmAdlsRequest {
    # Transient-fault + 401 retry wrapper for the sentinel read/write below.
    # HWM state blobs are on the ingest critical path — a lost read re-pulls an
    # overlapping window; a lost write drops the watermark and silently widens
    # the next run — so a 429/5xx/network blip must not fail the run, matching
    # the data-plane upload path. Retries 5xx/429/network with Retry-After +
    # capped exponential backoff, hard-fails other 4xx (the 404 first-run signal
    # bubbles to Get-HighWaterMark), and re-mints once on a 401 (cached token
    # lapsed inside the refresh skew, or the storage credential rotated mid-run).
    #
    # Kept module-local rather than reusing StorageHelperRest's
    # Invoke-AdlsDataPlaneWithRetry: that path emits chunk_upload_retry events
    # (wrong telemetry for state blobs) and carries no 401 re-mint. Local also
    # keeps the scriptblock's Invoke-WebRequest and this function's
    # Get-AdlsAccessToken in HighWaterMark's scope, where the suite's
    # InModuleScope mocks live — the #260 cross-module mock trap. Mirrors
    # ScopeFilter's Invoke-WithRetryAndStatus. $RequestScript takes the bearer
    # token as its one argument.
    param(
        [Parameter(Mandatory)][scriptblock]$RequestScript,
        [int]$MaxAttempts = 5,
        [int]$MaxDelaySeconds = 60
    )

    # Fetch the token once up front (cached); re-mint only inside the 401 branch.
    # Transient 5xx/429 retries reuse it, so a fault storm can't hammer the token
    # endpoint. One-time guard ($authRetried); mirrors ScopeFilter's retry loop.
    $authRetried = $false
    $attempt = 0
    $token = Get-AdlsAccessToken
    while ($true) {
        $attempt++
        try {
            return & $RequestScript $token
        }
        catch {
            $status = $null
            $resp = $_.Exception.Response
            if ($resp -and $resp.PSObject.Properties['StatusCode']) {
                $status = [int]$resp.StatusCode
            }

            # 401 re-mint doesn't count against MaxAttempts.
            if ($status -eq 401 -and -not $authRetried) {
                $authRetried = $true
                $token = Get-AdlsAccessToken -ForceRefresh
                $attempt--
                Write-Log "High-water-mark request got 401; re-minting ADLS token and retrying once" -Level WARN
                continue
            }

            # Transient: network errors (no status) + 5xx + 429. Everything else
            # (incl. the expected 404 first-run signal) is non-transient.
            $isTransient = ($null -eq $status) -or ($status -ge 500) -or ($status -eq 429)
            if (-not $isTransient -or $attempt -ge $MaxAttempts) { throw }

            $retryAfter = Get-RetryAfterSeconds -Response $resp
            $delay = if ($retryAfter -and $retryAfter -gt 0) {
                [int][math]::Min($MaxDelaySeconds, $retryAfter)
            } else {
                [int][math]::Min($MaxDelaySeconds, [math]::Pow(2, $attempt))
            }
            Write-Log "High-water-mark request retry attempt $attempt after status=$status, backing off ${delay}s: $($_.Exception.Message)" -Level WARN
            Start-Sleep -Seconds $delay
        }
    }
}

function Get-HighWaterMark {
    param(
        [Parameter(Mandatory)][string]$StorageAccountUrl,
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][string]$Entity,
        [Parameter(Mandatory)][string]$TenantKey
    )

    $blobPath = "_state/$Entity/$TenantKey/high_water_mark.json"
    # Symmetric with Set-HighWaterMark: use the blob endpoint so reads and
    # writes go through the same alias.
    $blobUrl = $StorageAccountUrl -replace '\.dfs\.', '.blob.'
    $url = "$blobUrl/$Container/$blobPath"

    try {
        $response = Invoke-HwmAdlsRequest -RequestScript {
            param($token)
            Invoke-WebRequest -Uri $url `
                -Method GET `
                -Headers @{
                    'Authorization' = "Bearer $token"
                    'x-ms-version'  = '2021-08-06'
                } `
                -UseBasicParsing
        }
        return $response.Content | ConvertFrom-Json
    }
    catch {
        # 404 is the first-run signal — no sentinel yet; caller falls back to a
        # default startTime. Transient faults and 401 were handled in the wrapper.
        $resp = $_.Exception.Response
        if ($resp -and $resp.PSObject.Properties['StatusCode'] -and [int]$resp.StatusCode -eq 404) {
            return $null
        }
        throw
    }
}

function Set-HighWaterMark {
    param(
        [Parameter(Mandatory)][string]$StorageAccountUrl,
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][string]$Entity,
        [Parameter(Mandatory)][string]$TenantKey,
        [Parameter(Mandatory)][datetime]$Timestamp,
        [Parameter(Mandatory)][string]$RunId
    )

    $sentinel = @{
        high_water_mark        = $Timestamp.ToUniversalTime().ToString('o')
        last_run_id            = $RunId
        last_run_completed_at  = ([datetime]::UtcNow).ToString('o')
        schema_version         = 1
    } | ConvertTo-Json -Compress

    $blobPath = "_state/$Entity/$TenantKey/high_water_mark.json"
    # Use the Blob endpoint (not DFS): Put Blob overwrites by default, so the
    # constant sentinel path can be rewritten on every successful run. The
    # DFS three-step create/append/flush we use for landed records 409s on
    # the create step when the path already exists (each landed-record path
    # is unique, so it doesn't matter there — but the HWM path is stable).
    # On HNS-enabled accounts the blob.* endpoint is aliased to the same
    # account, so the auth token works against either.
    $blobUrl = $StorageAccountUrl -replace '\.dfs\.', '.blob.'
    $url = "$blobUrl/$Container/$blobPath"
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($sentinel)

    # Route the write through the shared transient-fault + 401 retry wrapper so a
    # 429/5xx blip can't drop the watermark. Put Blob overwrites by default, so
    # re-issuing the PUT on retry is idempotent.
    Invoke-HwmAdlsRequest -RequestScript {
        param($token)
        Invoke-WebRequest -Uri $url `
            -Method PUT `
            -Headers @{
                'Authorization'    = "Bearer $token"
                'x-ms-version'     = '2021-08-06'
                'x-ms-blob-type'   = 'BlockBlob'
            } `
            -Body $bytes `
            -ContentType 'application/json' `
            -UseBasicParsing | Out-Null
    } | Out-Null
}

Export-ModuleMember -Function Get-HighWaterMark, Set-HighWaterMark
