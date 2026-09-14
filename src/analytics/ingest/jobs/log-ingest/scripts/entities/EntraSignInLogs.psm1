# Entra sign-in logs — relocated from graph-ingest as part of #296. Lives in
# log-ingest now so all log-shaped data ingests through one container and
# lands in caj-ma-toolkit-<env>-log-001 alongside the unified-audit content
# types.
#
# Pulled via Microsoft Graph /auditLogs/signIns. Different API surface from
# the 5 audit_* entities (Graph, not Mgmt API; @odata.nextLink, not
# NextPageUri header) so this module doesn't share MgmtApiCommon's fetch
# loop. It does share HighWaterMark and the BACKFILL_MODE env-var convention.
#
# Backfill mode: per the locked plan, no 24h chunking on Graph signIns —
# Graph paging handles arbitrary windows natively. The Mgmt API's 24h limit
# is a Microsoft constraint, not a backfill concept; imposing it here would
# add fan-out for no API-level reason.

function Get-ModuleStages {
    @{
        'signin_root' = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-EntraSignInLogs'
            ApiFamily           = 'log'
            MinimumSelectFields = @('id')
        }
    }
}

function Get-ModuleEntities {
    @{
        'entra_sign_in_logs' = @{
            Stage        = 'signin_root'
            WritesTo     = 'root'
            SelectFields = @(
                'id','createdDateTime','appDisplayName','appId','ipAddress','clientAppUsed',
                'conditionalAccessStatus','isInteractive','location','resourceDisplayName',
                'resourceId','riskDetail','riskLevelAggregated','riskLevelDuringSignIn',
                'riskState','riskEventTypes_v2','status','userDisplayName','userId',
                'userPrincipalName','deviceDetail'
            )
        }
    }
}

function Resolve-SignInWindow {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$TenantKey)

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

    $hwm = Get-HighWaterMark `
        -StorageAccountUrl $env:STORAGE_ACCOUNT_URL `
        -Container $env:LANDING_CONTAINER `
        -Entity 'entra_sign_in_logs' `
        -TenantKey $TenantKey

    $now = (Get-Date).ToUniversalTime()
    if ($null -eq $hwm) {
        return @{ Start = $now.AddHours(-24); End = $now; IsBackfill = $false }
    }

    # ConvertTo-UtcDateTime handles both [datetime] (ConvertFrom-Json) and
    # string inputs without the silent culture-sensitive round-trip that
    # `[datetime]::Parse(<DateTime>)` would do.
    $hwmTs = ConvertTo-UtcDateTime -Value $hwm.high_water_mark
    $start = $hwmTs.AddMinutes(-15)
    $earliest = $now.AddHours(-24)
    if ($start -lt $earliest) { $start = $earliest }
    return @{ Start = $start; End = $now; IsBackfill = $false }
}

function Get-EntraSignInLogs {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $tenantKey = $env:TENANT_KEY
    $window = Resolve-SignInWindow -TenantKey $tenantKey
    $select = $Context.SelectFields -join ','

    $startStr = $window.Start.ToString('yyyy-MM-ddTHH:mm:ssZ')
    $endStr   = $window.End.ToString('yyyy-MM-ddTHH:mm:ssZ')
    # Graph signIns supports arbitrary-length windows via @odata.nextLink — no
    # 24h chunking. $orderby + $top maximize page utilization.
    $filter = "createdDateTime ge $startStr and createdDateTime lt $endStr"
    $uri = "https://graph.microsoft.com/v1.0/auditLogs/signIns?`$filter=$([uri]::EscapeDataString($filter))&`$select=$select&`$top=999&`$orderby=createdDateTime"

    $maxCreated = $null

    do {
        # Read the token fresh inside each retry attempt (not captured), so an
        # auth-retry that fires Restore-ServiceConnection picks up the new
        # token. Long backfills cross the AAD ~1h token-lifetime boundary; a
        # captured-once token would loop on 401 until MaxRetries.
        $boundUri = $uri
        $req = {
            Invoke-RestMethod `
                -Method GET `
                -Uri $boundUri `
                -Headers @{ Authorization = "Bearer $(Get-CurrentGraphToken)" } `
                -ErrorAction Stop
        }
        $response = Invoke-WithRetry -ScriptBlock $req -ApiFamily 'log' `
            -OnAuthReconnect { Restore-ServiceConnection }
        foreach ($signIn in $response.value) {
            $Writer.WriteRecord($signIn)
            if ($signIn.createdDateTime) {
                $ts = ConvertTo-UtcDateTime -Value $signIn.createdDateTime
                if ($null -eq $maxCreated -or $ts -gt $maxCreated) {
                    $maxCreated = $ts
                }
            }
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)

    if (-not $window.IsBackfill -and $null -ne $maxCreated) {
        Set-HighWaterMark `
            -StorageAccountUrl $env:STORAGE_ACCOUNT_URL `
            -Container $env:LANDING_CONTAINER `
            -Entity 'entra_sign_in_logs' `
            -TenantKey $tenantKey `
            -Timestamp $maxCreated `
            -RunId $env:RUN_ID
    }
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-EntraSignInLogs
