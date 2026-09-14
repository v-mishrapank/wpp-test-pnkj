# Shared helpers for the 4 Office 365 Management Activity API audit entities
# (audit_entra, audit_exchange, audit_sharepoint, audit_general). PowerBI
# events flow through audit_general; Audit.PowerBI is not a separate
# subscribable content type per the Mgmt API reference.
#
# Each Audit.<ContentType> entity is a thin wrapper that sets `ContentType`
# and the entity name, then delegates the fetch loop here.
#
# The Mgmt API is a multi-step pull:
#   1. GET /subscriptions/list — what's currently subscribed
#   2. POST /subscriptions/start — start the content type if not active
#   3. GET /subscriptions/content?contentType=...&startTime=...&endTime=...
#      — list blob URIs (paginated via NextPageUri header)
#   4. GET <contentUri> per blob — actual records
#
# All calls require ?PublisherIdentifier=<our_tenant_guid> on every request
# to land in the per-source-tenant 2k/min quota instead of the contended
# general pool. The publisher identifier is sourced from $env:AZURE_TENANT_ID
# (the deployment's home tenant, auto-populated on ACA jobs by managed
# identity). See docs/analytics/audit-log-ingestion.md.
#
# Window calculation:
#   normal mode → max(hwm - 15min, now - 24h) to now (first run defaults to now-24h)
#   backfill mode → [BACKFILL_START, BACKFILL_END], walked in 24h chunks
#
# 24h chunking is a hard Mgmt API constraint: a single /content call can span
# at most 24h (Microsoft enforces this). Hourly cadence + small window stays
# well under the limit; backfill mode walks a longer window in 24h slices.

function ConvertTo-UtcDateTime {
    # Coerces a value to a UTC [datetime], whether it's already a [datetime]
    # (ConvertFrom-Json deserializes ISO-8601 timestamps that way) or a string
    # (env-var input from BACKFILL_START / BACKFILL_END). Avoids the silent
    # culture-sensitive round-trip you'd get from `[datetime]::Parse(<dt>)`,
    # which calls ToString() on the DateTime first and then re-parses — that
    # path drops Kind=Utc and re-interprets as local time on non-en-US
    # cultures, shifting the HWM by hours.
    param([Parameter(Mandatory)]$Value)
    if ($Value -is [datetime]) { return $Value.ToUniversalTime() }
    return [datetime]::Parse(
        [string]$Value,
        [System.Globalization.CultureInfo]::InvariantCulture,
        [System.Globalization.DateTimeStyles]::AssumeUniversal -bor [System.Globalization.DateTimeStyles]::AdjustToUniversal
    )
}

function Get-MgmtApiBaseUrl {
    param([Parameter(Mandatory)][string]$TenantId)
    return "https://manage.office.com/api/v1.0/$TenantId/activity/feed"
}

function Get-PublisherQueryArg {
    # PublisherIdentifier must be a GUID; the deployment's home tenant is the
    # documented value for ISV/multi-customer apps. AZURE_TENANT_ID is auto-
    # populated on ACA jobs by managed identity.
    $publisher = $env:AZURE_TENANT_ID
    if ([string]::IsNullOrWhiteSpace($publisher)) {
        # Fall back to the tenant we're querying. Misuses the param vs. spec
        # (we should be the publisher, not the customer), but it keeps us out
        # of the unidentified-callers shared pool when AZURE_TENANT_ID is
        # absent (e.g., docker run testing).
        $publisher = $env:TENANT_ID
    }
    return "PublisherIdentifier=$publisher"
}

function Invoke-MgmtApiRequest {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Method,
        [Parameter(Mandatory)][string]$Uri,
        # WebRequest=true returns the full response object so callers can read
        # headers like NextPageUri. Default returns the parsed body only.
        [switch]$WebRequest
    )

    $boundMethod = $Method
    $boundUri    = $Uri
    $useWeb      = $WebRequest.IsPresent

    # Read the token fresh inside each attempt (not captured at scriptblock-
    # creation time). On 401 mid-run — typical at the AAD ~1h token-lifetime
    # boundary or on backfills — Invoke-WithRetry classifies as Auth and fires
    # OnAuthReconnect, which runs Restore-ServiceConnection to mint a new
    # token. The next iteration reads the new token here, so the retry uses
    # fresh credentials. Capturing $token once outside this block would make
    # auth retry useless.
    $req = {
        $hdrs = @{ Authorization = "Bearer $(Get-CurrentMgmtToken)" }
        if ($useWeb) {
            Invoke-WebRequest `
                -Method $boundMethod `
                -Uri $boundUri `
                -Headers $hdrs `
                -UseBasicParsing `
                -ErrorAction Stop
        } else {
            Invoke-RestMethod `
                -Method $boundMethod `
                -Uri $boundUri `
                -Headers $hdrs `
                -ErrorAction Stop
        }
    }
    return Invoke-WithRetry -ScriptBlock $req -ApiFamily 'log' `
        -OnAuthReconnect { Restore-ServiceConnection }
}

function Initialize-MgmtSubscription {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$BaseUrl,
        [Parameter(Mandatory)][string]$ContentType
    )

    # /subscriptions/start has a 15-minute cooldown after /subscriptions/stop.
    # /subscriptions/list lets us check whether the content type is already
    # active (status=enabled) so we only POST start on first activation. Most
    # repeat runs find the subscription already active and skip the POST.
    $listUri = "$BaseUrl/subscriptions/list?$(Get-PublisherQueryArg)"
    $subs = Invoke-MgmtApiRequest -Method GET -Uri $listUri

    $existing = $subs | Where-Object { $_.contentType -ieq $ContentType }
    if ($existing -and $existing.status -ieq 'enabled') {
        Write-Log "Mgmt API subscription for $ContentType already active"
        return
    }

    $startUri = "$BaseUrl/subscriptions/start?contentType=$ContentType&$(Get-PublisherQueryArg)"
    Invoke-MgmtApiRequest -Method POST -Uri $startUri | Out-Null
    Write-Log "Started Mgmt API subscription for $ContentType"
}

function Get-WindowsToWalk {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][datetime]$StartTime,
        [Parameter(Mandatory)][datetime]$EndTime
    )

    # Mgmt API caps a single /content query at 24h. Slice the window into
    # contiguous 24h chunks; the last slice is whatever fraction is left.
    $windows = New-Object System.Collections.ArrayList
    $cursor = $StartTime.ToUniversalTime()
    $endUtc = $EndTime.ToUniversalTime()
    while ($cursor -lt $endUtc) {
        $next = $cursor.AddHours(24)
        if ($next -gt $endUtc) { $next = $endUtc }
        [void]$windows.Add(@{ Start = $cursor; End = $next })
        $cursor = $next
    }
    return $windows
}

function Resolve-IngestionWindow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Entity,
        [Parameter(Mandatory)][string]$TenantKey
    )

    # Backfill mode short-circuits HWM read/write entirely. Window is exactly
    # the operator-supplied [BACKFILL_START, BACKFILL_END]. Caller is
    # responsible for not writing HWM after the run.
    if ($env:BACKFILL_MODE -eq 'true') {
        if ([string]::IsNullOrWhiteSpace($env:BACKFILL_START) -or
            [string]::IsNullOrWhiteSpace($env:BACKFILL_END)) {
            throw "BACKFILL_MODE=true requires BACKFILL_START and BACKFILL_END env vars."
        }
        return @{
            Start = ConvertTo-UtcDateTime -Value $env:BACKFILL_START
            End   = ConvertTo-UtcDateTime -Value $env:BACKFILL_END
            IsBackfill = $true
        }
    }

    # Normal mode: read sentinel, fall back to (now - 24h) on first run.
    $hwm = Get-HighWaterMark `
        -StorageAccountUrl $env:STORAGE_ACCOUNT_URL `
        -Container $env:LANDING_CONTAINER `
        -Entity $Entity `
        -TenantKey $TenantKey

    $now = (Get-Date).ToUniversalTime()
    if ($null -eq $hwm) {
        # First run: fetch the most recent 24h — matches the HWM-present ceiling
        # and avoids empty windows on quiet tenants or off-hours runs.
        return @{ Start = $now.AddHours(-24); End = $now; IsBackfill = $false }
    }

    # Steady state: 15-min lookback overlap to absorb late-arriving events
    # (Mgmt API blob aggregation can lag actual event timestamps), capped at
    # 24h to prevent runaway windows after a long outage.
    $hwmTs = ConvertTo-UtcDateTime -Value $hwm.high_water_mark
    $start = $hwmTs.AddMinutes(-15)
    $earliest = $now.AddHours(-24)
    if ($start -lt $earliest) { $start = $earliest }
    return @{ Start = $start; End = $now; IsBackfill = $false }
}

function Invoke-MgmtContentFetch {
    # Drives the 4 Mgmt API audit entities (audit_entra, audit_exchange,
    # audit_sharepoint, audit_general).
    #
    # Walks the resolved window in 24h chunks. For each chunk:
    #   1. List /content with pagination (NextPageUri header)
    #   2. GET each blob URI; stream records into Writer
    #
    # Tracks the maximum record-level CreationTime (`record.CreationTime`,
    # the timestamp on each individual audit event) across landed records —
    # used as the new HWM after a successful end-to-end run. The /content
    # listing also exposes a blob-metadata `contentCreated` field; the
    # record-level CreationTime is the right HWM anchor since that's what
    # the next run's window starts from.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Entity,
        [Parameter(Mandatory)][string]$ContentType,
        [Parameter(Mandatory)]$Writer
    )

    $tenantId = $env:TENANT_ID
    $tenantKey = $env:TENANT_KEY
    $baseUrl = Get-MgmtApiBaseUrl -TenantId $tenantId

    Initialize-MgmtSubscription -BaseUrl $baseUrl -ContentType $ContentType

    $window = Resolve-IngestionWindow -Entity $Entity -TenantKey $tenantKey
    # Tracks the latest record-level timestamp landed across this run; written
    # back to the HWM sentinel on success. Note: Mgmt API records carry
    # `CreationTime` (the record's own timestamp); the surrounding `/content`
    # listing also exposes a `contentCreated` blob-metadata field, but the
    # record-level timestamp is the right HWM anchor — that's what we resume
    # from on the next run.
    $maxRecordTimestamp = $null

    foreach ($chunk in (Get-WindowsToWalk -StartTime $window.Start -EndTime $window.End)) {
        $startStr = $chunk.Start.ToString('yyyy-MM-ddTHH:mm:ss')
        $endStr   = $chunk.End.ToString('yyyy-MM-ddTHH:mm:ss')
        $uri = "$baseUrl/subscriptions/content?contentType=$ContentType&startTime=$startStr&endTime=$endStr&$(Get-PublisherQueryArg)"

        # /content returns a list of blob pointers; follow NextPageUri for
        # busy windows (ms uses a header, not @odata.nextLink). Routed through
        # Invoke-MgmtApiRequest -WebRequest so paging calls share AF429 retry
        # + token-refresh-on-auth with the per-blob fetches below.
        do {
            $response = Invoke-MgmtApiRequest -Method GET -Uri $uri -WebRequest
            $blobs = $response.Content | ConvertFrom-Json

            foreach ($blob in $blobs) {
                $blobUri = "$($blob.contentUri)?$(Get-PublisherQueryArg)"
                $records = Invoke-MgmtApiRequest -Method GET -Uri $blobUri
                foreach ($record in $records) {
                    $Writer.WriteRecord($record)
                    if ($record.CreationTime) {
                        $ts = ConvertTo-UtcDateTime -Value $record.CreationTime
                        if ($null -eq $maxRecordTimestamp -or $ts -gt $maxRecordTimestamp) {
                            $maxRecordTimestamp = $ts
                        }
                    }
                }
            }

            $next = $response.Headers['NextPageUri']
            $uri = if ($next) { "$next&$(Get-PublisherQueryArg)" } else { $null }
        } while ($uri)
    }

    # HWM is per-(entity, tenant) and only advances on successful end-to-end
    # run. Backfill mode skips this so historical pulls don't drag normal HWM
    # forward (or backward).
    if (-not $window.IsBackfill -and $null -ne $maxRecordTimestamp) {
        Set-HighWaterMark `
            -StorageAccountUrl $env:STORAGE_ACCOUNT_URL `
            -Container $env:LANDING_CONTAINER `
            -Entity $Entity `
            -TenantKey $tenantKey `
            -Timestamp $maxRecordTimestamp `
            -RunId $env:RUN_ID
    }
}

Export-ModuleMember -Function ConvertTo-UtcDateTime, Get-MgmtApiBaseUrl,
    Get-PublisherQueryArg, Invoke-MgmtApiRequest, Initialize-MgmtSubscription,
    Get-WindowsToWalk, Resolve-IngestionWindow, Invoke-MgmtContentFetch
