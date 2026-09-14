using module ./EntityRollup.psm1

# In-container run-state aggregator. See issue #314 for the original heartbeat
# design, #318 for the single-threaded shape, and #385 for the consolidation
# that made this the single dispatcher-facing artifact (formerly the
# heartbeat-plus-manifest-summary pair).
#
# Responsibility: maintain in-memory per-entity state, fold
# worker-runspace progress through a synchronized hashtable, and overwrite
# the run-state blob at stage transitions and during pool waits. The terminal
# upload from Stop-Heartbeat is now load-bearing — the dispatcher reads
# run_status from the blob to drive finalization (previously did this via the
# manifest summary at _dispatcher/manifests/, which #385 removed).
#
# Threading: this module is entirely main-thread. Stage-transition mutators
# (Set-StageRunning / Set-StageCompleted / Set-EntityCompleted) flush
# synchronously when called. Intra-stage progress during long pool stages
# is driven cooperatively from WorkerPool.psm1's wait loop, which calls
# Invoke-HeartbeatFlush every poll iteration; the flush self-throttles to
# at most one upload per $FlushSeconds.
#
# Mid-run flushes are best-effort. Stop-Heartbeat's final upload is bounded-
# retry (5 attempts, exponential backoff, ~30s cap) because dispatcher
# finalization depends on it.
#
# Path: _dispatcher/run_state/{runId}/{tenantKey}/{containerType}.json
# (Pre-#385 the path was _dispatcher/progress/ and the blob was deleted at
# finalization; the new path is permanent until aged out by the dispatcher's
# 30d sweep.)
#
# Cross-runspace progress: pwsh runspaces don't share script-scope state,
# so pool-stage progress arrives through a `[hashtable]::Synchronized`
# accessed via Get-ProgressShared. Worker runspaces in WorkerPool.psm1
# write slot entries; the main thread reads + folds during flush.

$script:State = $null
$script:UploadFunction = $null
$script:ConsecutiveFailures = 0
$script:LastWarnedAt = [DateTime]::MinValue
$script:FlushSeconds = 5
$script:MaxConsecutiveFailures = 12
$script:BlobPath = $null
$script:StorageAccountUrl = $null
$script:ContainerName = $null
$script:RunStartTimeUtc = $null

# Stop-Heartbeat terminal-flush retry tunables (#385). Production values give
# 5 attempts with exponential backoff capped near ~30s total. Test helper
# Set-TerminalRetryForTesting overrides these to skip the sleeps.
$script:TerminalMaxAttempts = 5
$script:TerminalBackoffSeconds = { param($attempt) [Math]::Min(8, [Math]::Pow(2, $attempt)) }

# Last time the cooperative flush gate let a call through. Distinct from
# State.LastUploadedAt — that one only advances on a successful upload, so
# during quiet periods (no state changes → upload skipped) or storage
# outages (upload failing) the rate limit never re-arms and the WorkerPool
# wait loop would refold + retry every 500ms. This timestamp advances
# whenever the gate passes, regardless of upload outcome.
$script:LastFlushAttemptAt = [DateTime]::MinValue

# Synchronized hashtable accessed by worker runspaces via Get-ProgressShared.
# Slot key = "<stageName>/<chunkNum>"; value = @{ stage; slice_index;
# records_so_far; updated_at }. Workers overwrite slot entries; the main
# thread folds with sum-of-max(records_so_far) per slice during flush.
# Enumeration locks on .SyncRoot (per-op atomicity isn't enough) — workers
# are still writing concurrently from their own runspaces.
$script:ProgressShared = [hashtable]::Synchronized(@{})
$script:StageToEntityMap = @{}

function Start-Heartbeat {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$RunId,
        [Parameter(Mandatory)][string]$TenantKey,
        [Parameter(Mandatory)][string]$ContainerType,
        [Parameter(Mandatory)][string]$StorageAccountUrl,
        [Parameter(Mandatory)][string]$ContainerName,
        [Parameter(Mandatory)][string[]]$WantedEntities,
        [Parameter(Mandatory)][scriptblock]$UploadFunction,
        [int]$FlushSeconds = 5
    )

    $script:FlushSeconds       = [Math]::Max(1, [int]$FlushSeconds)
    $script:UploadFunction     = $UploadFunction
    $script:StorageAccountUrl  = $StorageAccountUrl
    $script:ContainerName      = $ContainerName
    $script:BlobPath           = "_dispatcher/run_state/$RunId/$TenantKey/$ContainerType.json"
    $script:RunStartTimeUtc    = [DateTime]::UtcNow
    $script:ConsecutiveFailures = 0
    $script:StageToEntityMap   = @{}
    # Start the gate clock from run-start. The forced initial upload below
    # establishes the baseline; the next cooperative flush has to wait
    # FlushSeconds. Without this, the first cooperative call would always
    # slip through because MinValue makes the gate trivially pass.
    $script:LastFlushAttemptAt  = $script:RunStartTimeUtc

    # Per-entity state. Order preserved so the heartbeat blob lists entities
    # in the same order ENTITY_NAMES does — operator-friendly.
    $entityState = [ordered]@{}
    foreach ($ename in $WantedEntities) {
        $entityState[$ename] = @{
            Rollup         = New-EntityRollup -Name $ename -Status 'pending' -RecordCount $null -StartedAt $null -CompletedAt $null -Errors @()
            InputCount     = $null
            ItemsProcessed = $null
            ItemsFailed    = $null
            ItemsSkipped   = $null
            RecordsSoFar   = 0
            DurationMs     = $null
        }
    }

    $script:State = @{
        # Bumped from 3 to 4 for ProgressHeartbeat v4 flatten — entities
        # are now flat (no nested stages array). Each entity carries its
        # own input_count / items_* / records_so_far / duration_ms directly.
        SchemaVersion       = 4
        RunId               = $RunId
        TenantKey           = $TenantKey
        ContainerType       = $ContainerType
        ContainerStartedAt  = $script:RunStartTimeUtc.ToString('o')
        RunStatus           = 'running'
        RunError            = $null
        Entities            = $entityState
        # Entities required to run but not in WantedEntities
        # (subset/partial-ingest case from #362). Created on first transition to
        # 'running' by Set-PrerequisiteStageRunning, surfaced as a top-level
        # prerequisite_entities array in the blob so operators can see prerequisite
        # work progressing during partial-ingest runs.
        PrerequisiteEntities = [ordered]@{}
        # Bookkeeping for the rate-limited cooperative flush.
        LastChangedAt       = [DateTime]::UtcNow
        LastUploadedAt      = [DateTime]::MinValue
    }

    # Initial best-effort write so the dispatcher can see the run before any
    # stage starts. Forced (bypasses rate limit) — we want this on the wire
    # immediately even if a stage transition fires within FlushSeconds.
    Invoke-HeartbeatUpload -Force | Out-Null
}

function Stop-Heartbeat {
    [CmdletBinding()]
    param(
        [string]$FinalStatus = 'completed'
    )
    if ($null -eq $script:State) { return }
    $script:State.RunStatus = $FinalStatus
    $script:State.LastChangedAt = [DateTime]::UtcNow
    # Final upload — load-bearing post-#385. The dispatcher's RunTracker
    # reads run_status from this blob to drive finalization; if the terminal
    # upload never lands, RunTracker falls back to ACA exit code only and
    # loses per-entity outcomes. Bounded retry mirrors what the dispatcher
    # summary used to have (5 attempts, exponential backoff, ~30s cap).
    # On terminal failure the dispatcher still has the ACA exit fallback.
    for ($attempt = 1; $attempt -le $script:TerminalMaxAttempts; $attempt++) {
        if (Invoke-HeartbeatUpload -Force) { return }
        if ($attempt -ge $script:TerminalMaxAttempts) {
            try { Write-Log "Terminal run-state upload failed after $($script:TerminalMaxAttempts) attempts; dispatcher will fall back to ACA exit code" -Level ERROR } catch { Write-Verbose $_.Exception.Message }
            return
        }
        $delay = & $script:TerminalBackoffSeconds $attempt
        try { Write-Log "Terminal run-state upload attempt $attempt failed, retrying in ${delay}s" -Level WARN } catch { Write-Verbose $_.Exception.Message }
        if ($delay -gt 0) { Start-Sleep -Seconds $delay }
    }
}

# Test seam: shrink the terminal-flush backoff so retry tests don't have to
# wait the production cap (~30s). Production callers never invoke this.
function Set-TerminalRetryForTesting {
    [CmdletBinding()]
    param(
        [int]$MaxAttempts = 5,
        [scriptblock]$BackoffSeconds = { param($attempt) 0 }
    )
    $script:TerminalMaxAttempts = $MaxAttempts
    $script:TerminalBackoffSeconds = $BackoffSeconds
}

# Returns the synchronized hashtable used as the cross-runspace channel for
# pool-stage progress. WorkerPool dispatch sites thread this through
# AddArgument; worker runspaces write slot entries inside the FlushInterval
# block; the main thread reads + folds during Invoke-HeartbeatFlush.
function Get-ProgressShared {
    return $script:ProgressShared
}

function Set-StageRunning {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Entity,
        [Parameter(Mandatory)][string]$Stage,
        # InputCount is nullable to match the wire-format schema. Pool stages
        # know the input ID count up front and pass it; inline stages don't
        # have a meaningful count (the fetch streams records without an
        # input set) and pass $null. Storing 0 instead of null would read
        # as "no inputs" downstream, which is misleading.
        [object]$InputCount = $null
    )
    if ($null -eq $script:State) { return }
    if (-not $script:State.Entities.Contains($Entity)) { return }
    $now = [DateTime]::UtcNow
    $entityState = $script:State.Entities[$Entity]

    # Populate stage→entity map so the fold logic knows which entity each
    # ProgressShared slot belongs to.
    $script:StageToEntityMap[$Stage] = $Entity

    # Flat entity-level fields — v4 no longer nests per-stage hashtables.
    # For multi-stage entities (e.g. exo_group_members with dg_members +
    # ug_members), accumulate InputCount across stages rather than resetting.
    if ($null -ne $InputCount) {
        $ic = [int]$InputCount
        $entityState.InputCount = if ($null -eq $entityState.InputCount) { $ic } else { $entityState.InputCount + $ic }
    }

    if ($entityState.Rollup.status -eq 'pending') {
        $entityState.Rollup.status     = 'running'
        $entityState.Rollup.started_at = $now.ToString('o')
    }
    $script:State.LastChangedAt = $now

    # Stage transitions are rare and high-signal — flush immediately so the
    # dispatcher API reflects the new stage within ms, not within FlushSeconds.
    Invoke-HeartbeatUpload -Force | Out-Null
}

function Set-StageCompleted {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Entity,
        [Parameter(Mandatory)][string]$Stage,
        [int]$RecordsSoFar = 0,
        [int]$DurationMs = 0,
        [object]$ItemsProcessed = $null,
        [object]$ItemsFailed = $null,
        [object]$ItemsSkipped = $null
    )
    if ($null -eq $script:State) { return }
    if (-not $script:State.Entities.Contains($Entity)) { return }
    $now = [DateTime]::UtcNow
    $entityState = $script:State.Entities[$Entity]

    # DurationMs is NOT folded (it comes from the pool result's wall-clock
    # timer), so we accumulate it here across stages.
    $entityState.DurationMs = if ($null -eq $entityState.DurationMs) { $DurationMs } else { $entityState.DurationMs + $DurationMs }

    # For pool stages, RecordsSoFar and Items* are owned by the
    # ProgressShared fold (workers write slots, fold sums them). Writing
    # them here would double-count. For inline stages, there are no
    # ProgressShared slots — the counters passed here are the only source.
    # Detect which case by checking whether any ProgressShared slot maps
    # to this entity via StageToEntityMap.
    $hasFoldData = $false
    foreach ($key in $script:ProgressShared.Keys) {
        $slotStage = ($key -split '/')[0]
        if ($script:StageToEntityMap[$slotStage] -eq $Entity) {
            $hasFoldData = $true
            break
        }
    }
    if (-not $hasFoldData) {
        $entityState.RecordsSoFar += $RecordsSoFar
        if ($null -ne $ItemsProcessed) {
            $entityState.ItemsProcessed = if ($null -eq $entityState.ItemsProcessed) { [int]$ItemsProcessed } else { $entityState.ItemsProcessed + [int]$ItemsProcessed }
        }
        if ($null -ne $ItemsFailed) {
            $entityState.ItemsFailed = if ($null -eq $entityState.ItemsFailed) { [int]$ItemsFailed } else { $entityState.ItemsFailed + [int]$ItemsFailed }
        }
        if ($null -ne $ItemsSkipped) {
            $entityState.ItemsSkipped = if ($null -eq $entityState.ItemsSkipped) { [int]$ItemsSkipped } else { $entityState.ItemsSkipped + [int]$ItemsSkipped }
        }
    }

    $script:State.LastChangedAt = $now
    # Fold ProgressShared before uploading so the snapshot reflects the
    # latest per-slice values for pool stages. For inline stages (no fold
    # data), the counters were written directly above.
    Invoke-HeartbeatProjectAndUpload -Force | Out-Null
}

function Set-EntityCompleted {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Entity,
        [Parameter(Mandatory)][hashtable]$Rollup
    )
    if ($null -eq $script:State) { return }
    if (-not $script:State.Entities.Contains($Entity)) { return }
    # Replace the rollup wholesale — the caller already produced the canonical
    # shape via New-EntityRollup.
    $script:State.Entities[$Entity].Rollup = $Rollup
    $script:State.LastChangedAt = [DateTime]::UtcNow
    Invoke-HeartbeatUpload -Force | Out-Null
}

function Set-PrerequisiteStageRunning {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Stage,
        [object]$InputCount = $null
    )
    if ($null -eq $script:State) { return }
    $now = [DateTime]::UtcNow

    # Populate stage→entity map (prereq stages map to themselves).
    $script:StageToEntityMap[$Stage] = $Stage

    # Lazy first-touch creation. Prereq entities don't appear in the blob
    # until they transition to 'running', matching the entity-side behavior.
    if (-not $script:State.PrerequisiteEntities.Contains($Stage)) {
        $script:State.PrerequisiteEntities[$Stage] = @{
            name            = $Stage
            status          = 'pending'
            input_count     = $null
            items_processed = $null
            items_failed    = $null
            items_skipped   = $null
            records_so_far  = 0
            started_at      = $null
            completed_at    = $null
            duration_ms     = $null
            errors          = @()
        }
    }
    $st = $script:State.PrerequisiteEntities[$Stage]
    $st.status          = 'running'
    $st.input_count     = if ($null -eq $InputCount) { $null } else { [int]$InputCount }
    $st.records_so_far  = 0
    $st.items_processed = $null
    $st.items_failed    = $null
    $st.items_skipped   = $null
    $st.errors          = @()
    $st.started_at      = $now.ToString('o')
    $script:State.LastChangedAt = $now
    Invoke-HeartbeatUpload -Force | Out-Null
}

function Set-PrerequisiteStageCompleted {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Stage,
        [int]$RecordsSoFar = 0,
        [int]$DurationMs = 0,
        [ValidateSet('completed', 'partial', 'failed', 'skipped')]
        [string]$Status = 'completed',
        [object]$ItemsProcessed = $null,
        [object]$ItemsFailed = $null,
        [object]$ItemsSkipped = $null,
        [object]$Errors = @()
    )
    if ($null -eq $script:State) { return }
    if (-not $script:State.PrerequisiteEntities.Contains($Stage)) { return }
    $now = [DateTime]::UtcNow
    $st = $script:State.PrerequisiteEntities[$Stage]
    # Map stage vocabulary to entity vocabulary — prereqs are entities in v4.
    $st.status          = if ($Status -eq 'completed') { 'success' } else { $Status }
    $st.records_so_far  = $RecordsSoFar
    if ($null -ne $ItemsProcessed) { $st.items_processed = [int]$ItemsProcessed }
    if ($null -ne $ItemsFailed)    { $st.items_failed    = [int]$ItemsFailed }
    if ($null -ne $ItemsSkipped)   { $st.items_skipped   = [int]$ItemsSkipped }
    if ($null -ne $Errors)         { $st.errors          = @($Errors | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) }
    $st.completed_at    = $now.ToString('o')
    $st.duration_ms     = $DurationMs
    $script:State.LastChangedAt = $now
    Invoke-HeartbeatUpload -Force | Out-Null
}

function Set-RunCompleted {
    [CmdletBinding()]
    param(
        [string]$Status = 'completed'
    )
    if ($null -eq $script:State) { return }
    $script:State.RunStatus = $Status
    $script:State.LastChangedAt = [DateTime]::UtcNow
    # Don't force a flush here — the immediate caller is Stop-Heartbeat
    # (or close to it) which does the final forced upload. Avoids a double
    # PUT at shutdown.
}

function Set-RunError {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Message
    )
    if ($null -eq $script:State) { return }
    $script:State.RunError = $Message
    $script:State.LastChangedAt = [DateTime]::UtcNow
}

# Cooperative flush — called from WorkerPool's poll loop on every iteration
# (~500ms). Self-throttles to one upload per FlushSeconds, with the change-
# detection short-circuit folded in. No-op if heartbeat isn't initialized.
#
# This is the *only* mechanism for surfacing intra-stage records_so_far
# updates between Set-StageRunning and Set-StageCompleted. Without
# WorkerPool calling this during pool waits, long pool stages would freeze
# the heartbeat blob at "running, records_so_far: 0" until the stage ends.
function Invoke-HeartbeatFlush {
    [CmdletBinding()]
    param()
    if ($null -eq $script:State) { return }

    # Rate-limit: gate on $script:LastFlushAttemptAt (advanced on every gate
    # pass) rather than State.LastUploadedAt (advanced only on success).
    # The WorkerPool wait loop calls us every 500ms; we must not refold +
    # retry that often during quiet periods (no state changes → upload
    # skipped) or storage outages (upload failing) — both leave
    # LastUploadedAt stale.
    $now = [DateTime]::UtcNow
    if (($now - $script:LastFlushAttemptAt).TotalSeconds -lt $script:FlushSeconds) {
        return
    }
    $script:LastFlushAttemptAt = $now

    Invoke-HeartbeatProjectAndUpload | Out-Null
}

# Internal: fold $script:ProgressShared into per-entity progress counters
# on running entities, then upload if state has changed (or always, if -Force).
function script:Invoke-HeartbeatProjectAndUpload {
    [CmdletBinding()]
    param([switch]$Force)

    if ($null -eq $script:State) { return $false }

    # Fold: pool stages aggregate sum-of-max(records_so_far) by slice.
    # Inline stages don't write to the synchronized hashtable; their state
    # is updated synchronously via Set-StageCompleted.
    #
    # Snapshot ProgressShared under SyncRoot first — Hashtable.Synchronized
    # only guarantees per-op atomicity; enumeration races worker writes and
    # would intermittently throw. Workers are in their own runspaces and
    # are still writing concurrently — this lock is the only cross-runspace
    # synchronization in the module.
    # Fold every per-slice counter in parallel — records_so_far,
    # items_processed, items_failed, items_skipped. Same shape for each:
    # stageName -> slice_index -> max(value). Sum across slices on
    # project; max within a slice handles the worker overwrite-the-slot
    # cadence.
    $byStage             = @{}
    $byStageItems        = @{}
    $byStageItemsFail    = @{}
    $byStageItemsSkip    = @{}
    $snapshot = $null
    [System.Threading.Monitor]::Enter($script:ProgressShared.SyncRoot)
    try {
        $snapshot = @($script:ProgressShared.GetEnumerator() | ForEach-Object {
            [pscustomobject]@{ Key = $_.Key; Slot = $_.Value }
        })
    }
    finally { [System.Threading.Monitor]::Exit($script:ProgressShared.SyncRoot) }

    foreach ($entry in $snapshot) {
        $slot = $entry.Slot
        if ($null -eq $slot) { continue }
        $st = [string]$slot.stage
        $si = [int]$slot.slice_index
        $rs = [int]$slot.records_so_far
        if (-not $byStage.ContainsKey($st)) { $byStage[$st] = @{} }
        $cur = $byStage[$st][$si]
        if ($null -eq $cur -or $rs -gt $cur) { $byStage[$st][$si] = $rs }

        # items_* slots only present on schema_version=2+ writers (#383);
        # check via $null-vs-indexer lookup rather than .ContainsKey for
        # forward compatibility — today WorkerPool writes the slot as a
        # plain @{} (Hashtable) but if a future contributor switches to
        # [ordered]@{} for stable JSON order in the heartbeat blob, the
        # OrderedDictionary type doesn't expose .ContainsKey (only Contains
        # is on IDictionary), so .ContainsKey would throw silently inside
        # WorkerPool's cooperative-flush try/catch. The indexer returns
        # null on missing keys for both dict types — same effect as the
        # original guard. Mixed-version ProgressShared during a rolling
        # deploy: schema-1 slots have no key → indexer returns $null →
        # branch skipped.
        $ipVal = $slot['items_processed']
        if ($null -ne $ipVal) {
            $ip = [int]$ipVal
            if (-not $byStageItems.ContainsKey($st)) { $byStageItems[$st] = @{} }
            $curIP = $byStageItems[$st][$si]
            if ($null -eq $curIP -or $ip -gt $curIP) { $byStageItems[$st][$si] = $ip }
        }
        $ifVal = $slot['items_failed']
        if ($null -ne $ifVal) {
            $if_ = [int]$ifVal
            if (-not $byStageItemsFail.ContainsKey($st)) { $byStageItemsFail[$st] = @{} }
            $curIF = $byStageItemsFail[$st][$si]
            if ($null -eq $curIF -or $if_ -gt $curIF) { $byStageItemsFail[$st][$si] = $if_ }
        }
        $isVal = $slot['items_skipped']
        if ($null -ne $isVal) {
            $is_ = [int]$isVal
            if (-not $byStageItemsSkip.ContainsKey($st)) { $byStageItemsSkip[$st] = @{} }
            $curIS = $byStageItemsSkip[$st][$si]
            if ($null -eq $curIS -or $is_ -gt $curIS) { $byStageItemsSkip[$st][$si] = $is_ }
        }
    }

    # Project the fold onto flat entity-level fields using StageToEntityMap.
    # Accumulate per-entity sums across all stages that map to that entity.
    $byEntity = @{}  # entityName -> @{ records_so_far; items_processed; items_failed; items_skipped; has_items }
    foreach ($stageName in $byStage.Keys) {
        $entityName = $script:StageToEntityMap[$stageName]
        if ($null -eq $entityName) { continue }
        if (-not $byEntity.ContainsKey($entityName)) {
            $byEntity[$entityName] = @{ records_so_far = 0; items_processed = 0; items_failed = 0; items_skipped = 0; has_items = $false }
        }
        $sum = 0; foreach ($v in $byStage[$stageName].Values) { $sum += [int]$v }
        $byEntity[$entityName].records_so_far += $sum
    }
    foreach ($stageName in $byStageItems.Keys) {
        $entityName = $script:StageToEntityMap[$stageName]
        if ($null -eq $entityName) { continue }
        if (-not $byEntity.ContainsKey($entityName)) {
            $byEntity[$entityName] = @{ records_so_far = 0; items_processed = 0; items_failed = 0; items_skipped = 0; has_items = $false }
        }
        $sum = 0; foreach ($v in $byStageItems[$stageName].Values) { $sum += [int]$v }
        $byEntity[$entityName].items_processed += $sum
        $byEntity[$entityName].has_items = $true
    }
    foreach ($stageName in $byStageItemsFail.Keys) {
        $entityName = $script:StageToEntityMap[$stageName]
        if ($null -eq $entityName) { continue }
        if (-not $byEntity.ContainsKey($entityName)) {
            $byEntity[$entityName] = @{ records_so_far = 0; items_processed = 0; items_failed = 0; items_skipped = 0; has_items = $false }
        }
        $sum = 0; foreach ($v in $byStageItemsFail[$stageName].Values) { $sum += [int]$v }
        $byEntity[$entityName].items_failed += $sum
        $byEntity[$entityName].has_items = $true
    }
    foreach ($stageName in $byStageItemsSkip.Keys) {
        $entityName = $script:StageToEntityMap[$stageName]
        if ($null -eq $entityName) { continue }
        if (-not $byEntity.ContainsKey($entityName)) {
            $byEntity[$entityName] = @{ records_so_far = 0; items_processed = 0; items_failed = 0; items_skipped = 0; has_items = $false }
        }
        $sum = 0; foreach ($v in $byStageItemsSkip[$stageName].Values) { $sum += [int]$v }
        $byEntity[$entityName].items_skipped += $sum
        $byEntity[$entityName].has_items = $true
    }

    # Apply accumulated sums to entities. No status filter — multi-stage
    # entities (e.g. exo_group_members) can have their rollup set to a
    # terminal status by ManifestSink after the first stage completes while
    # the second stage is still writing ProgressShared slots (#504).
    foreach ($entityName in $script:State.Entities.Keys) {
        $entityState = $script:State.Entities[$entityName]
        if (-not $byEntity.ContainsKey($entityName)) { continue }
        $acc = $byEntity[$entityName]
        $changed = $false
        if ($acc.records_so_far -ne [int]$entityState.RecordsSoFar) {
            $entityState.RecordsSoFar = $acc.records_so_far
            $changed = $true
        }
        if ($acc.has_items) {
            if ($null -eq $entityState.ItemsProcessed -or $acc.items_processed -ne [int]$entityState.ItemsProcessed) {
                $entityState.ItemsProcessed = $acc.items_processed
                $changed = $true
            }
            if ($null -eq $entityState.ItemsFailed -or $acc.items_failed -ne [int]$entityState.ItemsFailed) {
                $entityState.ItemsFailed = $acc.items_failed
                $changed = $true
            }
            if ($null -eq $entityState.ItemsSkipped -or $acc.items_skipped -ne [int]$entityState.ItemsSkipped) {
                $entityState.ItemsSkipped = $acc.items_skipped
                $changed = $true
            }
        }
        if ($changed) { $script:State.LastChangedAt = [DateTime]::UtcNow }
    }

    # Same fold for prerequisite entities — pool workers don't know whether
    # they're feeding a requested entity or a prereq; they just write slot
    # entries keyed by stage name.
    foreach ($stageName in @($script:State.PrerequisiteEntities.Keys)) {
        $stage = $script:State.PrerequisiteEntities[$stageName]
        if ($stage.status -ne 'running') { continue }
        if (-not $byEntity.ContainsKey($stageName)) { continue }
        $acc = $byEntity[$stageName]
        $changed = $false
        if ($acc.records_so_far -ne [int]$stage.records_so_far) {
            $stage.records_so_far = $acc.records_so_far
            $changed = $true
        }
        if ($acc.has_items) {
            if ($null -eq $stage.items_processed -or $acc.items_processed -ne [int]$stage.items_processed) {
                $stage.items_processed = $acc.items_processed
                $changed = $true
            }
            if ($null -eq $stage.items_failed -or $acc.items_failed -ne [int]$stage.items_failed) {
                $stage.items_failed = $acc.items_failed
                $changed = $true
            }
            if ($null -eq $stage.items_skipped -or $acc.items_skipped -ne [int]$stage.items_skipped) {
                $stage.items_skipped = $acc.items_skipped
                $changed = $true
            }
        }
        if ($changed) { $script:State.LastChangedAt = [DateTime]::UtcNow }
    }

    return Invoke-HeartbeatUpload -Force:$Force
}

# Best-effort upload. Returns $true on success, $false otherwise. Log throttle:
# first failure WARN with class; subsequent failures DEBUG; recovery INFO.
# Skips upload when state hasn't changed since the last successful one,
# unless -Force is set (stage transitions, Stop-Heartbeat).
function script:Invoke-HeartbeatUpload {
    [CmdletBinding()]
    param([switch]$Force)

    if ($null -eq $script:State) { return $false }

    # Backoff gate: after N consecutive failures, only attempt when state
    # changed (no point pounding storage that's down with stale projections).
    if ($script:ConsecutiveFailures -ge $script:MaxConsecutiveFailures -and
        $script:State.LastChangedAt -le $script:State.LastUploadedAt -and
        -not $Force) {
        return $false
    }

    # Skip if no change since last successful upload (and not forced). Cheap
    # short-circuit — saves a PUT every flush during quiet periods.
    if (-not $Force -and $script:State.LastChangedAt -le $script:State.LastUploadedAt) {
        return $false
    }

    $now = [DateTime]::UtcNow
    $script:State.LastHeartbeatAt = $now.ToString('o')

    # Build the wire-shape payload. v4 flat: each entity is a single ordered
    # hashtable with all fields at the top level (no nested stages array).
    $entitiesArr = @()
    foreach ($entityName in $script:State.Entities.Keys) {
        $es = $script:State.Entities[$entityName]
        $entitiesArr += [ordered]@{
            name            = $es.Rollup.name
            status          = $es.Rollup.status
            record_count    = $es.Rollup.record_count
            input_count     = $es.InputCount
            items_processed = $es.ItemsProcessed
            items_failed    = $es.ItemsFailed
            items_skipped   = $es.ItemsSkipped
            records_so_far  = $es.RecordsSoFar
            started_at      = $es.Rollup.started_at
            completed_at    = $es.Rollup.completed_at
            duration_ms     = $es.DurationMs
            errors          = $es.Rollup.errors
        }
    }

    $prereqArr = @()
    foreach ($stageName in $script:State.PrerequisiteEntities.Keys) {
        $pr = $script:State.PrerequisiteEntities[$stageName]
        $prereqArr += [ordered]@{
            name            = $pr.name
            status          = $pr.status
            record_count    = $null
            input_count     = $pr.input_count
            items_processed = $pr.items_processed
            items_failed    = $pr.items_failed
            items_skipped   = $pr.items_skipped
            records_so_far  = $pr.records_so_far
            started_at      = $pr.started_at
            completed_at    = $pr.completed_at
            duration_ms     = $pr.duration_ms
            errors          = @($pr.errors)
        }
    }

    $payload = [ordered]@{
        schema_version         = $script:State.SchemaVersion
        run_id                 = $script:State.RunId
        tenant_key             = $script:State.TenantKey
        container_type         = $script:State.ContainerType
        container_started_at   = $script:State.ContainerStartedAt
        last_heartbeat_at      = $script:State.LastHeartbeatAt
        run_status             = $script:State.RunStatus
        run_error              = $script:State.RunError
        entities               = $entitiesArr
        prerequisite_entities  = $prereqArr
    }

    $tempFile = $null
    try {
        $json = $payload | ConvertTo-Json -Depth 6 -Compress
        # Upload helper is file-based (Write-ToAdlsRest); write JSON to a
        # temp file once per call. Cleanup in finally.
        $tempFile = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(),
            "heartbeat_$($script:State.RunId)_$([Guid]::NewGuid().ToString('N').Substring(0,8)).json")
        [System.IO.File]::WriteAllText($tempFile, $json, [System.Text.UTF8Encoding]::new($false))

        & $script:UploadFunction `
            -StorageAccountUrl $script:StorageAccountUrl `
            -ContainerName     $script:ContainerName `
            -BlobPath          $script:BlobPath `
            -LocalFile         $tempFile

        if ($script:ConsecutiveFailures -gt 0) {
            $recoveredAfter = [int](($now - $script:LastWarnedAt).TotalSeconds)
            try { Write-Log "Heartbeat upload recovered after ${recoveredAfter}s ($($script:ConsecutiveFailures) failures)" -Level INFO } catch { Write-Verbose $_.Exception.Message }
            $script:ConsecutiveFailures = 0
        }
        $script:State.LastUploadedAt = $now
        return $true
    }
    catch {
        $script:ConsecutiveFailures++
        if ($script:ConsecutiveFailures -eq 1) {
            $script:LastWarnedAt = $now
            try { Write-Log "Heartbeat upload failed: $($_.Exception.GetType().FullName): $($_.Exception.Message)" -Level WARN } catch { Write-Verbose $_.Exception.Message }
        } else {
            try { Write-Log "Heartbeat upload failed (#$($script:ConsecutiveFailures)): $($_.Exception.Message)" -Level DEBUG } catch { Write-Verbose $_.Exception.Message }
        }
        return $false
    }
    finally {
        if ($tempFile -and (Test-Path $tempFile)) {
            Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
        }
    }
}

Export-ModuleMember -Function `
    Start-Heartbeat, Stop-Heartbeat, `
    Set-StageRunning, Set-StageCompleted, Set-EntityCompleted, Set-RunCompleted, Set-RunError, `
    Set-PrerequisiteStageRunning, Set-PrerequisiteStageCompleted, `
    Get-ProgressShared, Invoke-HeartbeatFlush, `
    Set-TerminalRetryForTesting
