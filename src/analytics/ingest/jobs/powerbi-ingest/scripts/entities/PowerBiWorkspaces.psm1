# Workspace family ingestion module for Power BI / Fabric.
#
# Stage graph:
#
#   workspaces_root        (inline; admin scan + /tmp cache + scanId emit)
#     │
#     │  Async-scan lifecycle, all inline:
#     │    1. Paginate GET /v1.0/myorg/admin/groups for active workspace IDs.
#     │    2. Chunk into 100-id batches (Microsoft's getInfo limit).
#     │    3. POST /v1.0/myorg/admin/workspaces/getInfo per batch (capture scanId).
#     │    4. Poll /scanStatus/{scanId} per scanId until 'Succeeded'
#     │       (Start-Sleep -Seconds 10 between polls).
#     │    5. Download /scanResult/{scanId} to /tmp/powerbi-scans/{scanId}.json.
#     │    6. Emit one IdEvent + one bronze audit row per scanId.
#     │
#     │  Bronze for powerbi_workspaces_root is the per-scan audit ledger
#     │  (small; one row per submission). Per-workspace data lives in the
#     │  pool child below.
#     │
#     └── workspaces        (pool; reads /tmp/{scanId}.json from disk)
#           Per-scanId pool fan-out. Each runspace opens the cached scanResult,
#           parses its slice, and writes per-workspace rows. Zero network I/O.
#           Lands at powerbi_workspaces (rich workspace metadata + nested
#           artifact arrays preserved as inline JSON).
#
# === Why the entire async lifecycle runs inline in workspaces_root ===
#
# Microsoft's `getInfo` API is asynchronous: POST returns a scanId, then you
# poll /scanStatus until status='Succeeded' (Microsoft caps scan runtime at
# ~1 hour). The scanResult payload only becomes available AFTER the scan
# completes. Two design choices:
#
# (a) Root submits + emits scanIds immediately; pool children poll +
#     download. Simpler stage graph but each child re-polls and re-downloads.
# (b) Root performs the entire submit+poll+download cycle, caches results
#     to /tmp, then emits scanIds. Pool children read from disk only.
#
# (b) is what we do. Justification:
#   - Each scanResult is downloaded ONCE, not N times across N pool children.
#   - The `/tmp` cache plus the per-scanId audit row in the bronze gives
#     downstream operators a clear picture of when each scan completed.
#   - Pool children are pure local-file parsers — restarts are cheap, no
#     network failures to retry mid-parse.
#
# === Restart caveat ===
#
# If the ACA Job container is killed mid-execution after workspaces_root
# completes but BEFORE pool children consume /tmp, the cache is gone and
# scanIds may have expired (Microsoft's TTL is 24 hours). ACA Job retry
# policy reruns the whole execution from scratch — workspaces_root will
# re-submit scans, repopulate /tmp, and the pool children pick up fresh
# scanIds. Bounded recovery cost. Don't "fix" by writing to ADLS: the right
# place IS /tmp because the recovery boundary is per-execution.
#
# === Throttle awareness ===
#
# Microsoft's `getInfo` submission caps at ~30/hour per tenant. A 3,000-
# workspace tenant takes 30 batches = exactly at ceiling. No batching
# protection beyond the 100-id chunk size; tenants exceeding 3,000 workspaces
# would need the dispatcher to spread runs across hours. Practical limit:
# we'll see this surface as a 429 from the getInfo POST in extreme cases.
# Per-resource admin endpoints (used by C5 children) burst around 200/min.
#
# === Per-tick visibility (#295-driven) ===
#
# The poll loop emits a Write-Host line per tick with elapsed time + count
# of scans still pending → ACA stdout → Azure Monitor → ingest telemetry
# workbook (#272 live-tail tile renders these in real time). Plus the
# audit row per scanId on completion captures submittedAt, completedAt,
# pollCount, durationSeconds for post-run forensics.

# === Per-fetcher constants ===

$PowerBiBaseUri = 'https://api.powerbi.com'
$AdminApiVer    = 'v1.0'

# Microsoft's documented limit for /getInfo's workspaces[] body array. Larger
# batches return 400 BadRequest.
$ScanBatchSize  = 100

# Conservative ceiling: scans that haven't reached Succeeded after this many
# polls are surfaced as failures. Microsoft's documented hard cap is ~1 hour;
# at 10s/poll, 360 polls = exactly 1 hour. Padding by 60 polls gives clear
# error rather than indefinite hang on edge cases.
$MaxPollsPerScan = 420
$PollIntervalSec = 10

$ScanCacheRoot   = '/tmp/powerbi-scans'

# === Stage + entity declarations ===

function Get-ModuleStages {
    # All children of workspaces_root run on the pool, take $InputId = scanId,
    # and read their slice from /tmp/powerbi-scans/{scanId}.json. Only
    # dataset_refresh_schedules calls a real REST endpoint (per-dataset, sync).
    # 'datasets' EmitIds → fans out to dataset_refresh_schedules (pool-of-pool).
    $rootStage = @{
        'workspaces_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerBiWorkspacesRoot'
            ApiFamily  = 'powerbi'
            EmitIds    = $true
            IdKey      = 'scanId'
        }
    }
    # Generic /tmp-reading pool children: workspaces foundation + 12 slices.
    $poolChildNames = @(
        'workspaces','reports','dashboards','dataflows',
        'datasource_instances',
        'dataset_datasources','dataflow_datasources',
        'workspace_users','dataset_users','report_users','dashboard_users',
        'dataset_schemas','dataset_expressions'
    )
    foreach ($name in $poolChildNames) {
        $fnSuffix = ($name -split '_' | ForEach-Object { $_.Substring(0,1).ToUpper() + $_.Substring(1) }) -join ''
        $rootStage[$name] = @{
            InputFrom  = 'workspaces_root'
            RunsOnPool = $true
            Function   = "Get-PowerBi$fnSuffix"
            ApiFamily  = 'powerbi'
        }
    }
    # 'datasets' is special: it emits datasetIds for the refresh-schedules
    # pool-of-pool below.
    $rootStage['datasets'] = @{
        InputFrom  = 'workspaces_root'
        RunsOnPool = $true
        Function   = 'Get-PowerBiDatasets'
        ApiFamily  = 'powerbi'
        EmitIds    = $true
        IdKey      = 'id'
    }
    # Pool-of-pool: per-dataset sync admin GET refreshSchedule. Datasets
    # without a configured schedule return 404, which RetryHelper classifies
    # as Skippable — the framework emits an item_failed LAW event and moves on.
    $rootStage['dataset_refresh_schedules'] = @{
        InputFrom      = 'datasets'
        RunsOnPool     = $true
        Function       = 'Get-PowerBiDatasetRefreshSchedules'
        ApiFamily      = 'powerbi'
    }
    return $rootStage
}

function Get-ModuleEntities {
    # All entities write to the family root folder (powerbi_workspaces_root/).
    $entityNames = @(
        'powerbi_workspaces_root',  # Stage = workspaces_root (audit ledger)
        'powerbi_workspaces',       # Stage = workspaces
        'powerbi_datasets',         # Stage = datasets
        'powerbi_reports',          # Stage = reports
        'powerbi_dashboards',       # Stage = dashboards
        'powerbi_dataflows',        # Stage = dataflows
        'powerbi_datasource_instances',
        'powerbi_dataset_datasources',
        'powerbi_dataflow_datasources',
        'powerbi_workspace_users',
        'powerbi_dataset_users',
        'powerbi_report_users',
        'powerbi_dashboard_users',
        'powerbi_dataset_schemas',
        'powerbi_dataset_expressions',
        'powerbi_dataset_refresh_schedules'
    )
    $entities = @{}
    foreach ($n in $entityNames) {
        # Strip 'powerbi_' prefix; that's the stage name (special-case the root).
        $stage = if ($n -eq 'powerbi_workspaces_root') { 'workspaces_root' } else { $n.Substring('powerbi_'.Length) }
        $entities[$n] = @{ Stage = $stage; WritesTo = 'root' }
    }
    return $entities
}

# Helpers (Invoke-PowerBiRequest, Invoke-PowerBiPagedFetch) live in Connect.psm1.

# === Fetchers ===

function Get-PowerBiWorkspacesRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # --- Step 1: enumerate active workspace IDs via /admin/groups ---
    # Filter to state='Active' to skip Deleted/Removing per Power BI norms.
    # We don't call Invoke-PowerBiPagedFetch here because /admin/groups rows
    # are transient input (used to drive scan submission), not bronze — they
    # never land. Walk @odata.nextLink inline and keep IDs in memory.
    $allGroups = [System.Collections.Generic.List[object]]::new()
    $uri = "$PowerBiBaseUri/$AdminApiVer/myorg/admin/groups?`$top=5000"
    do {
        $resp = Invoke-PowerBiRequest -Uri $uri
        foreach ($g in $resp.value) { $allGroups.Add($g) }
        $uri = $resp.'@odata.nextLink'
    } while ($uri)
    $activeIds = @($allGroups | Where-Object { $_.state -eq 'Active' } | ForEach-Object { $_.id })
    Write-Log "[workspaces_root] /admin/groups → $($allGroups.Count) total, $($activeIds.Count) Active"

    if ($activeIds.Count -eq 0) {
        Write-Log "[workspaces_root] No active workspaces; nothing to scan."
        return
    }

    # --- Step 2: ensure /tmp cache dir exists ---
    if (-not (Test-Path -Path $ScanCacheRoot)) {
        New-Item -ItemType Directory -Path $ScanCacheRoot -Force | Out-Null
    }

    # --- Step 3: chunk into batches and submit getInfo per batch ---
    # Use an explicit List<object[]> rather than the `,@(...)` pattern in a
    # for-expression. PowerShell unwraps single-element pipeline outputs in
    # ways that can flatten the batch array → the POST body becomes a string
    # instead of array-of-strings → 400 BadRequest with "requiredWorkspaces
    # .workspaces: Invalid value". Lesson learned the hard way.
    $batches = [System.Collections.Generic.List[object]]::new()
    for ($i = 0; $i -lt $activeIds.Count; $i += $ScanBatchSize) {
        $end = [Math]::Min($i + $ScanBatchSize, $activeIds.Count) - 1
        $slice = [string[]]@($activeIds[$i..$end])
        $batches.Add($slice)
    }
    Write-Log "[workspaces_root] Submitting $($batches.Count) getInfo batch(es)..."

    $scans = [System.Collections.Generic.List[hashtable]]::new()
    $batchIndex = 0
    foreach ($batch in $batches) {
        $batchArray = @($batch)
        Write-Log "[workspaces_root]   batch $batchIndex size=$($batchArray.Count)"
        $body = @{ workspaces = $batchArray } | ConvertTo-Json -Compress -Depth 5
        $submitUri = "$PowerBiBaseUri/$AdminApiVer/myorg/admin/workspaces/getInfo?lineage=true&datasourceDetails=true&datasetSchema=true&datasetExpressions=true&getArtifactUsers=true"
        $resp = Invoke-PowerBiRequest -Uri $submitUri -Method POST -Body $body
        $scanId = $resp.id
        if (-not $scanId) {
            throw "getInfo response missing 'id' field for batch $batchIndex (size=$($batchArray.Count)). Response: $($resp | ConvertTo-Json -Compress -Depth 3)"
        }
        $scans.Add(@{
            ScanId         = $scanId
            BatchIndex     = $batchIndex
            WorkspaceCount = $batchArray.Count
            SubmittedAt    = [DateTime]::UtcNow
            CompletedAt    = $null
            PollCount      = 0
            Status         = 'Submitted'
        })
        $batchIndex++
    }
    Write-Log "[workspaces_root] $($scans.Count) scan(s) submitted; polling for completion..."

    # --- Step 4: poll each scan to completion ---
    # Microsoft's /scanStatus returns one of: NotStarted, Running, Succeeded, Failed.
    # Poll all scans in parallel from a single thread (Invoke-RestMethod is
    # synchronous; we just iterate). Sleep between full-loop ticks so the poll
    # rate is bounded regardless of scan count.
    $pendingMask = [bool[]]::new($scans.Count)
    for ($i = 0; $i -lt $scans.Count; $i++) { $pendingMask[$i] = $true }
    $pollTick = 0
    while ($true) {
        $stillPending = 0
        for ($i = 0; $i -lt $scans.Count; $i++) {
            if (-not $pendingMask[$i]) { continue }
            $scan = $scans[$i]
            $statusUri = "$PowerBiBaseUri/$AdminApiVer/myorg/admin/workspaces/scanStatus/$($scan.ScanId)"
            $statusResp = Invoke-PowerBiRequest -Uri $statusUri
            $scan.PollCount++
            $scan.Status = $statusResp.status
            if ($statusResp.status -eq 'Succeeded' -or $statusResp.status -eq 'Failed') {
                $scan.CompletedAt = [DateTime]::UtcNow
                $pendingMask[$i] = $false
            } else {
                $stillPending++
            }
            if ($scan.PollCount -ge $MaxPollsPerScan -and $pendingMask[$i]) {
                $scan.CompletedAt = [DateTime]::UtcNow
                $scan.Status = "TimedOut(after=$($scan.PollCount * $PollIntervalSec)s)"
                $pendingMask[$i] = $false
                $stillPending--
            }
        }
        $elapsed = [int]([DateTime]::UtcNow - $scans[0].SubmittedAt).TotalSeconds
        Write-Log "[workspaces_root] poll tick=$pollTick elapsed=${elapsed}s pending=$stillPending/$($scans.Count)"
        if ($stillPending -eq 0) { break }
        Start-Sleep -Seconds $PollIntervalSec
        $pollTick++
    }

    # --- Step 5: download scanResult to /tmp for each succeeded scan ---
    $succeededCount = 0
    foreach ($scan in $scans) {
        if ($scan.Status -ne 'Succeeded') {
            Write-Log -Level WARN "[workspaces_root] scanId=$($scan.ScanId) batchIndex=$($scan.BatchIndex) FINAL status=$($scan.Status) — skipping download"
            continue
        }
        $resultUri = "$PowerBiBaseUri/$AdminApiVer/myorg/admin/workspaces/scanResult/$($scan.ScanId)"
        $resultResp = Invoke-PowerBiRequest -Uri $resultUri
        $cachePath = Join-Path -Path $ScanCacheRoot -ChildPath "$($scan.ScanId).json"
        $resultResp | ConvertTo-Json -Compress -Depth 100 | Set-Content -Path $cachePath -Encoding UTF8
        $succeededCount++
    }
    Write-Log "[workspaces_root] Downloaded $succeededCount scanResult(s) to $ScanCacheRoot"

    # --- Step 6: write audit ledger row + emit IdEvent per succeeded scan ---
    foreach ($scan in $scans) {
        $durationSec = if ($scan.CompletedAt) {
            [int]($scan.CompletedAt - $scan.SubmittedAt).TotalSeconds
        } else { $null }
        $auditRow = [pscustomobject]@{
            scanId          = $scan.ScanId
            batchIndex      = $scan.BatchIndex
            workspaceCount  = $scan.WorkspaceCount
            submittedAt     = $scan.SubmittedAt.ToString('o')
            completedAt     = if ($scan.CompletedAt) { $scan.CompletedAt.ToString('o') } else { $null }
            durationSeconds = $durationSec
            pollCount       = $scan.PollCount
            status          = $scan.Status
        }
        $Writer.WriteRecord($auditRow)
        # Only emit IdEvents for succeeded scans — failed/timed-out scans
        # have no /tmp file for the pool child to read, so dispatching to
        # them would just produce read-error noise.
        if ($scan.Status -eq 'Succeeded') {
            $Writer.EmitId($scan.ScanId, $null)
        }
    }
}

# Module-private helper. Pool runspaces import this module fresh, so
# this function is visible inside every C5 fetcher's scope.
function Get-PowerBiScanWorkspaces {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$ScanId)
    $cachePath = Join-Path -Path $ScanCacheRoot -ChildPath "$ScanId.json"
    if (-not (Test-Path -Path $cachePath)) {
        throw "Cached scanResult missing for scanId=$ScanId at $cachePath. Likely cause: container restart between workspaces_root and pool dispatch — rerun the execution. See module-header 'Restart caveat'."
    }
    $scanResult = Get-Content -Raw -Path $cachePath | ConvertFrom-Json -Depth 100
    if ($scanResult.workspaces) { return ,$scanResult.workspaces } else { return ,@() }
}

# Helper: copy all properties of $source onto an [ordered]@{} along with
# stamped FK fields, return as pscustomobject. Used by every slicer fetcher.
function ConvertTo-StampedRow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$Source,
        [Parameter(Mandatory)][hashtable]$Stamps
    )
    $row = [ordered]@{}
    foreach ($k in $Stamps.Keys) { $row[$k] = $Stamps[$k] }
    foreach ($prop in $Source.PSObject.Properties) {
        if (-not $row.Contains($prop.Name)) {
            $row[$prop.Name] = $prop.Value
        }
    }
    return [pscustomobject]$row
}

function Get-PowerBiWorkspaces {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # Pure local-file parser — InputId is the scanId, /tmp cache populated by
    # workspaces_root. /tmp is the same filesystem across runspaces in the
    # same OS process, so this works without cross-process plumbing.
    $scanId = $InputId
    $workspaces = Get-PowerBiScanWorkspaces -ScanId $scanId
    foreach ($ws in $workspaces) {
        $Writer.WriteRecord((ConvertTo-StampedRow -Source $ws -Stamps @{ scanId = $scanId }))
    }
}

function Get-PowerBiDatasets {
    # Emits dataset IDs for the dataset_refresh_schedules pool-of-pool. The
    # `EmitIds=true; IdKey='id'` declarations in Get-ModuleStages are
    # metadata-only — the framework requires an explicit $Writer.EmitId()
    # call (consistent with EntraGroups, PowerBiGateways, etc.).
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.datasets) { continue }
        foreach ($ds in $ws.datasets) {
            $Writer.WriteRecord((ConvertTo-StampedRow -Source $ds -Stamps @{ scanId = $scanId; workspaceId = $ws.id }))
            $Writer.EmitId($ds.id, $null)
        }
    }
}

function Get-PowerBiReports {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.reports) { continue }
        foreach ($r in $ws.reports) {
            $Writer.WriteRecord((ConvertTo-StampedRow -Source $r -Stamps @{ scanId = $scanId; workspaceId = $ws.id }))
        }
    }
}

function Get-PowerBiDashboards {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.dashboards) { continue }
        foreach ($d in $ws.dashboards) {
            $Writer.WriteRecord((ConvertTo-StampedRow -Source $d -Stamps @{ scanId = $scanId; workspaceId = $ws.id }))
        }
    }
}

function Get-PowerBiDataflows {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.dataflows) { continue }
        foreach ($df in $ws.dataflows) {
            $Writer.WriteRecord((ConvertTo-StampedRow -Source $df -Stamps @{ scanId = $scanId; workspaceId = $ws.id }))
        }
    }
}

function Get-PowerBiDatasourceInstances {
    # Workspace-level datasourceInstances[] array (populated when getInfo was
    # called with datasourceDetails=true, which workspaces_root does). These
    # are the migration-load-bearing connection records: connectionDetails
    # carries server names, SharePoint URLs, SAP systems, Azure SQL endpoints.
    # Empty in test tenants without on-prem / cloud-direct sources.
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.datasourceInstances) { continue }
        foreach ($dsi in $ws.datasourceInstances) {
            $Writer.WriteRecord((ConvertTo-StampedRow -Source $dsi -Stamps @{ scanId = $scanId; workspaceId = $ws.id }))
        }
    }
}

function Get-PowerBiDatasetDatasources {
    # FK link rows: dataset[].dataSourceUsages[] each contain a
    # datasourceInstanceId pointing at the workspace-level datasourceInstance.
    # Silver-layer joins powerbi_datasets ↔ powerbi_dataset_datasources ↔
    # powerbi_datasource_instances to resolve dataset → connection details.
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.datasets) { continue }
        foreach ($ds in $ws.datasets) {
            if (-not $ds.dataSourceUsages) { continue }
            foreach ($u in $ds.dataSourceUsages) {
                $Writer.WriteRecord((ConvertTo-StampedRow -Source $u -Stamps @{ scanId = $scanId; workspaceId = $ws.id; datasetId = $ds.id }))
            }
        }
    }
}

function Get-PowerBiDataflowDatasources {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.dataflows) { continue }
        foreach ($df in $ws.dataflows) {
            if (-not $df.dataSourceUsages) { continue }
            foreach ($u in $df.dataSourceUsages) {
                $Writer.WriteRecord((ConvertTo-StampedRow -Source $u -Stamps @{ scanId = $scanId; workspaceId = $ws.id; dataflowId = $df.id }))
            }
        }
    }
}

function Get-PowerBiWorkspaceUsers {
    # Workspace-level ACL: groupUserAccessRight per principal.
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.users) { continue }
        foreach ($u in $ws.users) {
            $Writer.WriteRecord((ConvertTo-StampedRow -Source $u -Stamps @{ scanId = $scanId; workspaceId = $ws.id }))
        }
    }
}

function Get-PowerBiDatasetUsers {
    # Per-dataset ACL: datasetUserAccessRight per principal (different from
    # workspace.users[].groupUserAccessRight — note the field-name shift).
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.datasets) { continue }
        foreach ($ds in $ws.datasets) {
            if (-not $ds.users) { continue }
            foreach ($u in $ds.users) {
                $Writer.WriteRecord((ConvertTo-StampedRow -Source $u -Stamps @{ scanId = $scanId; workspaceId = $ws.id; datasetId = $ds.id }))
            }
        }
    }
}

function Get-PowerBiReportUsers {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.reports) { continue }
        foreach ($r in $ws.reports) {
            if (-not $r.users) { continue }
            foreach ($u in $r.users) {
                $Writer.WriteRecord((ConvertTo-StampedRow -Source $u -Stamps @{ scanId = $scanId; workspaceId = $ws.id; reportId = $r.id }))
            }
        }
    }
}

function Get-PowerBiDashboardUsers {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.dashboards) { continue }
        foreach ($d in $ws.dashboards) {
            if (-not $d.users) { continue }
            foreach ($u in $d.users) {
                $Writer.WriteRecord((ConvertTo-StampedRow -Source $u -Stamps @{ scanId = $scanId; workspaceId = $ws.id; dashboardId = $d.id }))
            }
        }
    }
}

function Get-PowerBiDatasetSchemas {
    # workspace.datasets[].tables[] — populated when getInfo was called with
    # datasetSchema=true (which workspaces_root does). columns[] and measures[]
    # remain inline JSON; silver can flatten if needed.
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.datasets) { continue }
        foreach ($ds in $ws.datasets) {
            if (-not $ds.tables) { continue }
            foreach ($t in $ds.tables) {
                $Writer.WriteRecord((ConvertTo-StampedRow -Source $t -Stamps @{ scanId = $scanId; workspaceId = $ws.id; datasetId = $ds.id }))
            }
        }
    }
}

function Get-PowerBiDatasetExpressions {
    # workspace.datasets[].expressions[] — populated when getInfo was called
    # with datasetExpressions=true. Each expression carries DAX/M source code
    # for migration-time dependency analysis (cross-tenant references in M).
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $scanId = $InputId
    foreach ($ws in (Get-PowerBiScanWorkspaces -ScanId $scanId)) {
        if (-not $ws.datasets) { continue }
        foreach ($ds in $ws.datasets) {
            if (-not $ds.expressions) { continue }
            foreach ($e in $ds.expressions) {
                $Writer.WriteRecord((ConvertTo-StampedRow -Source $e -Stamps @{ scanId = $scanId; workspaceId = $ws.id; datasetId = $ds.id }))
            }
        }
    }
}

function Get-PowerBiDatasetRefreshSchedules {
    # Pool-of-pool: per-dataset sync admin GET. Calls the V1 admin variant
    # /admin/datasets/{datasetId}/refreshSchedule (works for SP auth despite
    # not being in Microsoft Learn's documented "44 supported admin SP APIs"
    # list — confirmed in branch-env smoke against madev1+madev2).
    #
    # Datasets without a configured schedule return 404, classified as
    # Skippable by RetryHelper. The framework emits an item_failed LAW event
    # and moves on — no row lands in JSONL for that dataset.
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InputId, [Parameter(Mandatory)][hashtable]$Context, [Parameter(Mandatory)]$Writer)
    $datasetId = $InputId
    $uri = "$PowerBiBaseUri/$AdminApiVer/myorg/admin/datasets/$datasetId/refreshSchedule"
    $resp = Invoke-PowerBiRequest -Uri $uri
    $Writer.WriteRecord((ConvertTo-StampedRow -Source $resp -Stamps @{ datasetId = $datasetId }))
}

Export-ModuleMember -Function `
    Get-ModuleStages, Get-ModuleEntities, `
    Get-PowerBiWorkspacesRoot, Get-PowerBiWorkspaces, `
    Get-PowerBiDatasets, Get-PowerBiReports, Get-PowerBiDashboards, Get-PowerBiDataflows, `
    Get-PowerBiDatasourceInstances, `
    Get-PowerBiDatasetDatasources, Get-PowerBiDataflowDatasources, `
    Get-PowerBiWorkspaceUsers, Get-PowerBiDatasetUsers, Get-PowerBiReportUsers, Get-PowerBiDashboardUsers, `
    Get-PowerBiDatasetSchemas, Get-PowerBiDatasetExpressions, `
    Get-PowerBiDatasetRefreshSchedules
