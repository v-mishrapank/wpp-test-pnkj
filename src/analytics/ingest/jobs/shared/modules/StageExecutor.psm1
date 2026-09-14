using module ./RecordEnvelope.psm1
using module ./StageWriter.psm1

# Import WorkerPool.psm1 so Invoke-StagePool is available when this module
# is used standalone (tests, future scripts). Invoke-Ingestion.ps1 already
# imports it, so this is a no-op there — but keeps the module self-contained.
#
# DO NOT add an Import-Module LogHelper here — see issue #189. Re-importing
# LogHelper from inside StageExecutor detaches Write-Log from the calling
# script's scope when StageExecutor is later Import-Module'd with -Force,
# silently breaking every Write-Log call in Invoke-Ingestion.ps1 from that
# point on. Callers must import LogHelper themselves before importing this
# module.
Import-Module (Join-Path $PSScriptRoot 'WorkerPool.psm1') -Force -DisableNameChecking
# DO NOT Import-Module RetryHelper here. Invoke-Ingestion.ps1 loads
# StageExecutor via `using module` (parse-time for class deps), which
# triggers this top-level code before Invoke-Ingestion's own runtime
# Import-Module RetryHelper runs.  The parse-time import steals the
# module into StageExecutor's scope, and the later runtime -Force
# re-import fails to restore global visibility — breaking every entity
# fetcher that calls Invoke-WithRetry.  Same shape as the LogHelper
# issue (#189) above.  Tests import RetryHelper in their own BeforeAll.
#   See: #507 (added), #458 (removed after branch-env breakage).

# StageExecutor — planner and orchestrator for the stage-DAG ingestion framework.
#
# A consolidated entity module exports two hashtables describing its work:
#
#   Get-ModuleStages   — stage-name -> stage-spec:
#       InputFrom            parent-stage-name or $null (root)
#       RunsOnPool           $true for pool-fanout stages; $false (default) for inline stages
#       Function             name of the Get-* function in the module
#       IdKey                composite-key template for emitted IDs (e.g. 'id', 'teamId:::channelId').
#                            The composite delimiter is `:::` (triple colon) — this
#                            is the literal token the runtime checks via -notmatch ':::'
#                            to gate auto-extract. Single-colon forms like 'a:b' are
#                            treated as a single-field IdKey by the framework, so
#                            don't use them for composites.
#                            Names a field on the record AS PASSED TO WriteRecord
#                            (post-projection schema), not on the source-API object.
#                            For entities that declare SelectFields, IdKey must be
#                            in the projected field set or it will be invisible to
#                            the framework's auto-extract.
#       EmitIds              $true if this stage feeds a downstream stage. When IdKey
#                            is single-field (does not contain ':::'), the framework
#                            auto-emits IDs from each WriteRecord output as a safety
#                            net — fetchers that forget the explicit $Writer.EmitId()
#                            call still feed downstream stages instead of silently
#                            landing record_count=0. Composite-IdKey stages still
#                            require explicit EmitId because the composite has to
#                            be assembled by the fetcher. Stages that need to emit
#                            with IdTags also call EmitId explicitly; the explicit
#                            calls win and the auto-extract is dropped on the floor.
#                            See #297.
#       IdTags               fields carried alongside each emitted ID (e.g. membershipType).
#                            Used by child InputFilter at the parent->child boundary AND
#                            propagated into the pool worker's $Context.InputTags so the
#                            fetch can read per-input metadata without composite-ID parsing.
#                            See "When to use IdTags vs composite IdKey" below.
#       InputFilter          optional scriptblock { param($tag) ... } — subsets parent IDs
#       MinimumSelectFields  fields this stage always needs from its API
#       JsonDepth            optional int; envelope Depth for records (default 5). Bump for
#                            deeply-nested payloads like SPO roleAssignments / sharingLinks.
#       AutoFlushThreshold   optional int; pool-only. When >0, the per-item StageWriter flushes
#                            every N records to disk during the fetch instead of buffering all
#                            records before the post-fetch dump. Use for stages where a single
#                            item can produce huge result sets (Dataverse solution_components on
#                            a large tenant, etc.). Default 0 keeps the buffer-then-dump
#                            behavior. Inline stages already auto-flush at 1000 in StageExecutor.
#
#   Get-ModuleEntities — entity-name -> @{ Stage; WritesTo; SelectFields }
#       Stage           which stage's records satisfy this entity
#       WritesTo        'root' (parent-folder file) or subdir name
#       SelectFields    fields this entity wants in its written output.
#                       Optional. Three valid shapes:
#                         - absent/omitted: writer emits records as-is (raw
#                           pass-through). Use when the endpoint doesn't
#                           accept $select and the module wants to land the
#                           server's full payload.
#                         - string[]: applies to every stage this entity
#                           maps to. URL $select is built from the union
#                           of these with the stage's MinimumSelectFields,
#                           AND the writer projects records to exactly this
#                           list (missing keys materialized as $null).
#                         - hashtable keyed by stage name: per-stage lists,
#                           mirroring the WritesTo pattern. Omit an entry
#                           to opt that specific stage out of projection
#                           (raw pass-through for that stage only).
#                       Empty arrays and hashtable entries are rejected at
#                       load time so a dev can't accidentally declare a
#                       filter that the framework would silently ignore.
#
# When to use IdTags vs composite IdKey (see #257)
#
# Use a composite IdKey ('a:b' or 'a:::b') only when both parts are true peer
# identifiers — typically URL components consumed together by the child API
# (teamId:channelId, Identity:::ObjectId, envName:::name).
#
# Use IdTags for per-input *metadata* the worker needs (instanceUrl, audience,
# membershipType, etc.). Smuggling metadata into the InputId leaks ~200 chars
# into every error log line and gets uglier with each additional field; tags
# ride alongside the ID and stay out of log strings.
#
# Heuristic: composite for true multi-key cases; tags for metadata.
#
# Given those two maps plus an orchestrator-supplied RequestedEntities list,
# the planner answers three questions:
#   1. Which stages need to run? (transitive closure through InputFrom)
#   2. In what order? (topological sort, parents before children)
#   3. What $select should each stage pass to its API?
#
# These resolvers are pure — no network, filesystem, or runspace interaction.
# Invoke-ModuleRun (added in a follow-up commit) uses them to drive execution.

function Resolve-RequiredStages {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Stages,
        [Parameter(Mandatory)][hashtable]$Entities,
        [Parameter(Mandatory)][string[]]$RequestedEntities
    )

    $required = [System.Collections.Generic.HashSet[string]]::new()
    $queue    = [System.Collections.Generic.Queue[string]]::new()

    foreach ($entityName in $RequestedEntities) {
        if (-not $Entities.ContainsKey($entityName)) {
            throw "Entity '$entityName' is not declared by this module (no matching entry in Get-ModuleEntities)"
        }
        # Entity.Stage can be a single string or an array. An entity with two
        # stages models a producer with multiple sources (e.g. ExoGroups, where
        # exo_group_members is fed by both DG and UG root stages).
        $entityStages = @($Entities[$entityName].Stage)
        foreach ($stageName in $entityStages) {
            if (-not $Stages.ContainsKey($stageName)) {
                throw "Entity '$entityName' maps to stage '$stageName', but that stage is not declared in Get-ModuleStages"
            }
            if ($required.Add($stageName)) { $queue.Enqueue($stageName) }
        }
    }

    while ($queue.Count -gt 0) {
        $current = $queue.Dequeue()
        $parent = $Stages[$current].InputFrom
        if ($null -ne $parent) {
            if (-not $Stages.ContainsKey($parent)) {
                throw "Stage '$current' references missing parent stage '$parent'"
            }
            if ($required.Add($parent)) { $queue.Enqueue($parent) }
        }
    }

    return @($required)
}

function Resolve-ExecutionOrder {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Stages,
        [Parameter(Mandatory)][string[]]$RequiredStages
    )

    # Kahn's algorithm restricted to the required subset, emitted in
    # depth-blocked passes. A stage is ready when its InputFrom is null,
    # outside the required set, or already emitted in a PRIOR pass. Each
    # pass collects all currently-ready stages, then commits them grouped
    # by InputFrom (with a deterministic secondary sort by stage name).
    #
    # Two invariants together (#352):
    #   1. Depth-blocking — a child cannot see its parent in $emitted
    #      until the next pass begins, so it cannot slot between its
    #      parent's same-depth siblings.
    #   2. Same-parent grouping within a pass — when a single pass
    #      contains ready children of multiple parents (e.g. a module
    #      with two roots), grouping by InputFrom keeps each sibling
    #      group contiguous in the output.
    #
    # Both are needed for the pool stage-batch assembly in Invoke-ModuleRun,
    # which walks the resolved order forward and breaks on the first
    # different InputFrom or ApiFamily it sees.
    $remaining = [System.Collections.Generic.HashSet[string]]::new($RequiredStages)
    $required  = [System.Collections.Generic.HashSet[string]]::new($RequiredStages)
    $emitted   = [System.Collections.Generic.HashSet[string]]::new()
    $order     = [System.Collections.Generic.List[string]]::new()

    while ($remaining.Count -gt 0) {
        $readyThisPass = [System.Collections.Generic.List[string]]::new()
        foreach ($name in @($remaining)) {
            $parent = $Stages[$name].InputFrom
            $parentReady = ($null -eq $parent) -or (-not $required.Contains($parent)) -or $emitted.Contains($parent)
            if ($parentReady) {
                $readyThisPass.Add($name)
            }
        }
        if ($readyThisPass.Count -eq 0) {
            throw "Cycle detected in stage graph among: $($remaining -join ', ')"
        }
        # Group by InputFrom (null parents first, sorted by stage name within
        # each group) so same-parent siblings are guaranteed adjacent.
        $sorted = $readyThisPass | Sort-Object `
            @{ Expression = { if ($null -eq $Stages[$_].InputFrom) { '' } else { [string]$Stages[$_].InputFrom } } }, `
            @{ Expression = { $_ } }
        foreach ($name in $sorted) {
            $order.Add($name)
            $emitted.Add($name) | Out-Null
            $remaining.Remove($name) | Out-Null
        }
    }

    return @($order)
}

function Resolve-RootEntities {
    <#
    Given a module's stages and entities, returns a hashtable mapping each
    entity name to the set of root-entity names under which its output lands.
    For a single-stage entity this is always exactly one name. For a
    multi-stage entity (e.g. exo_group_members fed by dg_root + ug_root) it
    can be two, each corresponding to the landing folder its stage writes to.

    The tie-breaking rule (single-stage entities claim root folders first) is
    the same as used by Invoke-ModuleRun, and is the fix for bug #2 from the
    second review.

    Used by Invoke-Ingestion's failure path to synthesize manifest BasePaths
    that match where the live run would have landed records. Mirrors the
    runtime gate in Invoke-ModuleRun (#156): a root stage's owning entity
    is the one that maps directly to it AND declares WritesTo='root'.
    Without these gates, a sibling entity (e.g. teams_team_details) could
    hashtable-iteration-order itself into the root-entity slot of
    teams_root and the failure-path BasePath would point at the
    wrong folder.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Stages,
        [Parameter(Mandatory)][hashtable]$Entities
    )

    $writesRootAt = {
        param($EntityName, $StageName)
        $wt = $Entities[$EntityName].WritesTo
        $resolved = if ($wt -is [hashtable]) { $wt[$StageName] } else { $wt }
        return $resolved -eq 'root'
    }

    $rootStageToEntity = @{}
    foreach ($ename in $Entities.Keys) {
        $entityStages = @($Entities[$ename].Stage)
        if ($entityStages.Count -ne 1) { continue }
        $s = $entityStages[0]
        if (-not $Stages.ContainsKey($s)) { continue }
        if ($null -ne $Stages[$s].InputFrom) { continue }
        if (-not (& $writesRootAt $ename $s)) { continue }
        if (-not $rootStageToEntity.ContainsKey($s)) { $rootStageToEntity[$s] = $ename }
    }
    foreach ($ename in $Entities.Keys) {
        $entityStages = @($Entities[$ename].Stage)
        if ($entityStages.Count -le 1) { continue }
        foreach ($s in $entityStages) {
            if (-not $Stages.ContainsKey($s)) { continue }
            if ($null -ne $Stages[$s].InputFrom) { continue }
            if (-not (& $writesRootAt $ename $s)) { continue }
            if (-not $rootStageToEntity.ContainsKey($s)) { $rootStageToEntity[$s] = $ename }
        }
    }

    $result = @{}
    foreach ($ename in $Entities.Keys) {
        $entityStages = @($Entities[$ename].Stage)
        $rootEntities = [System.Collections.Generic.HashSet[string]]::new()
        foreach ($s in $entityStages) {
            if (-not $Stages.ContainsKey($s)) { continue }
            $cur = $s
            while ($null -ne $Stages[$cur].InputFrom) { $cur = $Stages[$cur].InputFrom }
            if ($rootStageToEntity.ContainsKey($cur)) {
                $rootEntities.Add($rootStageToEntity[$cur]) | Out-Null
            }
        }
        $result[$ename] = @($rootEntities)
    }
    return $result
}

function Get-EntitySelectFieldsForStage {
    <#
    Returns the entity's declared SelectFields list applicable to a given
    stage, or $null if the entity doesn't declare fields for that stage.
    Centralizes the flat-array-vs-per-stage-hashtable handling so Resolve-
    SelectFields and Resolve-ProjectionFields agree on what an entity
    contributes.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Entity,
        [Parameter(Mandatory)][string]$StageName
    )
    if (-not $Entity.ContainsKey('SelectFields')) { return $null }
    $sf = $Entity.SelectFields
    if ($null -eq $sf) { return $null }
    if ($sf -is [System.Collections.IDictionary]) {
        if (-not $sf.Contains($StageName)) { return $null }
        $v = @($sf[$StageName])
        if ($v.Count -eq 0) { return $null }
        return $v
    }
    $a = @($sf)
    if ($a.Count -eq 0) { return $null }
    return $a
}

function Resolve-SelectFields {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$StageName,
        [Parameter(Mandatory)][hashtable]$Stages,
        [Parameter(Mandatory)][hashtable]$Entities,
        [Parameter(Mandatory)][string[]]$RequestedEntities
    )

    $stage = $Stages[$StageName]
    $fields = [System.Collections.Generic.HashSet[string]]::new()

    if ($null -ne $stage.MinimumSelectFields) {
        foreach ($f in $stage.MinimumSelectFields) { $fields.Add($f) | Out-Null }
    }

    foreach ($entityName in $RequestedEntities) {
        $entity = $Entities[$entityName]
        if ($null -eq $entity) { continue }
        $entityStages = @($entity.Stage)
        if (-not ($entityStages -contains $StageName)) { continue }
        $entityFields = Get-EntitySelectFieldsForStage -Entity $entity -StageName $StageName
        if ($null -eq $entityFields) { continue }
        foreach ($f in $entityFields) { $fields.Add($f) | Out-Null }
    }

    return @($fields)
}

function Resolve-ProjectionFields {
    <#
    Returns the field list the writer should project records to for the
    given stage, or $null if no requested entity declares SelectFields for
    this stage (writer passes records through unprojected).

    Distinct from Resolve-SelectFields: does NOT include a stage's
    MinimumSelectFields, which are operational (fields the fetch needs to
    do its work, like 'id' for EmitId) rather than wanted in the landed
    record. Including them would add internal scaffolding to every file.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$StageName,
        [Parameter(Mandatory)][hashtable]$Entities,
        [Parameter(Mandatory)][string[]]$RequestedEntities
    )

    $fields = [System.Collections.Generic.List[string]]::new()
    $seen   = [System.Collections.Generic.HashSet[string]]::new()
    $any    = $false

    foreach ($entityName in $RequestedEntities) {
        $entity = $Entities[$entityName]
        if ($null -eq $entity) { continue }
        $entityStages = @($entity.Stage)
        if (-not ($entityStages -contains $StageName)) { continue }
        $entityFields = Get-EntitySelectFieldsForStage -Entity $entity -StageName $StageName
        if ($null -eq $entityFields) { continue }
        $any = $true
        foreach ($f in $entityFields) {
            if ($f -and $seen.Add($f)) { $fields.Add($f) }
        }
    }

    if (-not $any) { return $null }
    return @($fields)
}

# Records-so-far source for an inline stage's completion. For writing
# stages, the metric is records persisted (TotalWritten). For prereqs
# (#362, no requested entity), production fetchers gate WriteRecord on
# $Context.WriteRecords so TotalWritten stays 0 even when the fetch
# iterated thousands of rows — EmittedIds.Count is the unconditional
# work signal. Pre-#373 the inline path used TotalWritten unconditionally,
# which made prereq inline stages (e.g. teams_root) report records_so_far=0
# on the heartbeat even though they had successfully emitted IDs for
# downstream pool stages to consume.
function Select-InlineRecordsSoFar {
    [CmdletBinding()]
    [OutputType([int])]
    param(
        [Parameter(Mandatory)][bool]$WriteRecords,
        [Parameter(Mandatory)][int]$TotalWritten,
        [Parameter(Mandatory)][int]$EmittedIdsCount
    )
    if ($WriteRecords) { return $TotalWritten } else { return $EmittedIdsCount }
}

# --- Invoke-ModuleRun: plan and execute one consolidated module's work ---
#
# Called once per module by Invoke-Ingestion with the subset of requested
# entities belonging to that module. Loads the module, plans via the
# resolvers above, then walks stages in topological order:
#   - inline stages execute in-process (like today's Phase 1)
#   - pool stages dispatch through Invoke-StagePool (replaces Phase 2)
# Between stages, emitted IDs flow downstream; InputFilter scriptblocks on
# child stages subset the parent's IDs before fan-out.
#
# Context hashtable fields consumed:
#   RunId          — 8-char invocation ID, used as batch_id and filename suffix
#   TenantKey      — source_key for envelope + landing-path segment
#   Date           — yyyy-MM-dd, landing-path segment
#   SourceType     — envelope source_type
#   SourceKey      — envelope source_key (usually TenantKey)
#   PoolSize       — runspace pool size for pool stages
#   AuthConfig     — hashtable passed to pool-stage first-dispatch auth.
#                    Carries CertificateBase64 so worker runspaces can decode
#                    the cert locally without a separate byte[] channel
#                    (post-#145 — see WorkerPool.psm1 "Auth model" comment).
#   AuthModulePath — absolute path to the container's Connect.psm1; imported
#                    into each worker runspace's InitialSessionState so
#                    Connect-Service / Restore-ServiceConnection are callable
#   TempRoot       — base temp directory (per-run chunk dirs and files land here)
#   Extra          — optional hashtable of module-specific fetch context (merged into $fetchContext)
#
# Returns a list of per-entity result hashtables:
#   @{ EntityName; StageName; Status; RecordCount; ChunkCount; SkippedCount;
#      FailedCount; LocalPaths; BlobPaths; Errors }
# The caller uploads each (LocalPath -> BlobPath) pair and writes the manifest.

function Invoke-ModuleRun {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ModulePath,
        [Parameter(Mandatory)][string[]]$RequestedEntities,
        [Parameter(Mandatory)][hashtable]$Context
    )

    # -DisableNameChecking is defensive: if an entity module ever ships an
    # unapproved verb the warning is caught by the repo-wide CI lint
    # (PSScriptAnalyzerSettings.psd1) and doesn't need to surface
    # at runtime.
    $module = Import-Module $ModulePath -PassThru -Force -DisableNameChecking -ErrorAction Stop
    # Pool cache lives outside the try-block so the finally can always
    # dispose pools regardless of where in the try the unwind starts.
    $poolCache = @{}
    try {
        $stages   = & "$($module.Name)\Get-ModuleStages"
        $entities = & "$($module.Name)\Get-ModuleEntities"

        # Validate module contract before planning. Inline stages always write
        # one file at the base path — they can't land in a subdir. Catching
        # this here gives a clear error instead of the stage silently writing
        # to the wrong location.
        foreach ($ename in $entities.Keys) {
            $entity = $entities[$ename]
            $entityStages = @($entity.Stage)
            foreach ($s in $entityStages) {
                if (-not $stages.ContainsKey($s)) { continue }
                if ($stages[$s].RunsOnPool) { continue }
                $wt = if ($entity.WritesTo -is [hashtable]) { $entity.WritesTo[$s] } else { $entity.WritesTo }
                if ($wt -ne 'root') {
                    throw "Inline stage '$s' (entity '$ename') declares WritesTo='$wt' — inline stages must write to 'root'."
                }
            }
        }

        # Fail loud on a SelectFields declaration that the framework would
        # silently ignore. An empty array or an empty per-stage entry is the
        # classic drift trap: a dev adds fields expecting them to take
        # effect, but the fetch was hardcoded and the writer had no list to
        # project to. With projection now wired into the writer, a declared
        # field MUST resolve to a non-empty list — or the entity should omit
        # SelectFields entirely and emit the raw payload.
        foreach ($ename in $entities.Keys) {
            $entity = $entities[$ename]
            if (-not $entity.ContainsKey('SelectFields')) { continue }
            $sf = $entity.SelectFields
            if ($null -eq $sf) {
                throw "Entity '$ename' declares SelectFields = `$null. Remove the key to emit raw records, or declare a non-empty field list."
            }
            if ($sf -is [System.Collections.IDictionary]) {
                if ($sf.Count -eq 0) {
                    throw "Entity '$ename' declares SelectFields = @{} (empty hashtable). Remove the key to emit raw records, or declare per-stage field lists."
                }
                foreach ($k in $sf.Keys) {
                    $v = $sf[$k]
                    if ($null -eq $v -or @($v).Count -eq 0) {
                        throw "Entity '$ename' declares SelectFields['$k'] = empty/null. Remove the key to emit raw records from stage '$k', or declare a non-empty field list."
                    }
                }
            }
            elseif (@($sf).Count -eq 0) {
                throw "Entity '$ename' declares SelectFields = @() (empty array). Remove the key to emit raw records, or declare a non-empty field list."
            }
        }

        $requiredStages  = Resolve-RequiredStages -Stages $stages -Entities $entities -RequestedEntities $RequestedEntities
        $executionOrder  = Resolve-ExecutionOrder -Stages $stages -RequiredStages $requiredStages
        $requestedSet    = [System.Collections.Generic.HashSet[string]]::new($RequestedEntities)
        # Heartbeat prereq stages (#362) are created on first transition by
        # Set-PrerequisiteStageRunning rather than eagerly here, so stages
        # skipped via the ancestor-failed path don't surface as ghost
        # 'pending' entries that never resolve. Mirrors how per-stage entries
        # under each entity behave: entity rollups are emitted up-front by
        # Start-Heartbeat, but the stages[] list inside each rollup only
        # grows as Set-StageRunning fires for each stage.

        # Map each stage to its root-stage ancestor (used to pick the landing folder).
        $stageToRoot       = @{}
        $rootStageToEntity = @{}
        foreach ($stageName in $executionOrder) {
            $cur = $stageName
            while ($null -ne $stages[$cur].InputFrom) { $cur = $stages[$cur].InputFrom }
            $stageToRoot[$stageName] = $cur
        }
        # A root stage's landing folder is the name of the entity that
        # declares WritesTo='root' for it. Prior logic populated this map by
        # iterating $entities.Keys, whose enumeration order is undefined in
        # PowerShell — so when several entities share a root stage (e.g. the
        # teams family's five children of teams_root), a sibling
        # could claim the root folder and downstream writes landed under the
        # wrong directory (#156).
        foreach ($ename in $entities.Keys) {
            $entity = $entities[$ename]
            foreach ($s in @($entity.Stage)) {
                if (-not $stages.ContainsKey($s)) { continue }
                if ($null -ne $stages[$s].InputFrom) { continue }
                $wt = if ($entity.WritesTo -is [hashtable]) { $entity.WritesTo[$s] } else { $entity.WritesTo }
                if ($wt -ne 'root') { continue }
                if ($rootStageToEntity.ContainsKey($s) -and $rootStageToEntity[$s] -ne $ename) {
                    throw "Root stage '$s' is claimed by multiple entities with WritesTo='root': '$($rootStageToEntity[$s])' and '$ename'."
                }
                $rootStageToEntity[$s] = $ename
            }
        }
        foreach ($r in ($stageToRoot.Values | Select-Object -Unique)) {
            if (-not $rootStageToEntity.ContainsKey($r)) {
                throw "Root stage '$r' has no entity declaring WritesTo='root'; cannot resolve landing folder."
            }
        }

        $stageEmitted  = @{}   # stageName -> [hashtable[]] of @{ Id; Tags }
        $failedStages  = [System.Collections.Generic.HashSet[string]]::new()
        $results       = [System.Collections.Generic.List[hashtable]]::new()

        # #345 — route every result-row append through this helper so the
        # caller's optional UploadSink / ManifestSink fire as each stage
        # finishes. Pre-#345 the orchestrator collected $results, then ran
        # a single upload+manifest pass at module-end; any hang or
        # replicaTimeout mid-module lost every completed stage's data.
        # Both sinks are optional ($null tolerated for ad-hoc tests).
        $emitResult = {
            param([hashtable]$r)
            $results.Add($r)
            if ($Context.UploadSink -and $r.LocalPaths -and $r.LocalPaths.Count -gt 0) {
                for ($i = 0; $i -lt $r.LocalPaths.Count; $i++) {
                    & $Context.UploadSink $r.LocalPaths[$i] $r.BlobPaths[$i] $r.EntityName
                }
                $r['Uploaded'] = $true
            }
            if ($Context.ManifestSink) {
                & $Context.ManifestSink $r
            }
        }

        # $poolCache (#145 item 1) is initialized above the try-block so the
        # finally can always dispose pools. Stages dispatched as part of a
        # sibling tier are tracked here so the foreach skips them on the
        # outer-iteration pass.
        $stagesHandled = [System.Collections.Generic.HashSet[string]]::new()

        foreach ($stageName in $executionOrder) {
            if ($stagesHandled.Contains($stageName)) { continue }
            $stage = $stages[$stageName]
            $selectFields     = Resolve-SelectFields     -StageName $stageName -Stages $stages -Entities $entities -RequestedEntities $RequestedEntities
            $projectionFields = Resolve-ProjectionFields -StageName $stageName                 -Entities $entities -RequestedEntities $RequestedEntities

            # Entities mapped to this stage AND in the requested set.
            $allStageEntities = @()
            foreach ($ename in $entities.Keys) {
                $entityStages = @($entities[$ename].Stage)
                if ($entityStages -contains $stageName) {
                    $allStageEntities += $ename
                }
            }
            $stageEntities = @($allStageEntities | Where-Object { $requestedSet.Contains($_) })
            $stageEntity = if ($stageEntities.Count -ge 1) { $stageEntities[0] } else { $null }
            $writeRecords = ($stageEntities.Count -gt 0)
            $rootEntity = $rootStageToEntity[$stageToRoot[$stageName]]
            $basePath = "$rootEntity/$($Context.TenantKey)/$($Context.Date)"

            # Update EventEmitter's current scope so RetryHelper-emitted
            # throttle events (which don't receive stage/entity through the
            # normal fetch context) inherit attribution.
            $scopeEntity = if ($stageEntity) { $stageEntity } else { '' }
            Set-EventScope -Stage $stageName -Entity $scopeEntity

            # Skip if any ancestor stage failed. Without this, a failed root
            # produces empty EmittedIds and descendants silently report
            # status=success record_count=0 — misleading. Explicitly mark
            # them 'skipped' with the root cause attributed.
            $failedAncestor = $null
            $ancestor = $stage.InputFrom
            while ($null -ne $ancestor) {
                if ($failedStages.Contains($ancestor)) { $failedAncestor = $ancestor; break }
                $ancestor = $stages[$ancestor].InputFrom
            }
            if ($null -ne $failedAncestor) {
                $failedStages.Add($stageName) | Out-Null
                $stageEmitted[$stageName] = @()
                Write-Event -EventType stage_skipped -Stage $stageName -Entity $stageEntity -Properties @{
                    reason         = 'ancestor_failed'
                    ancestor_stage = $failedAncestor
                }
                if ($writeRecords) {
                    foreach ($en in $stageEntities) {
                        & $emitResult @{
                            EntityName  = $en
                            StageName   = $stageName
                            Status      = 'skipped'
                            RecordCount = 0
                            ChunkCount  = 0
                            LocalPaths  = @()
                            BlobPaths   = @()
                            BasePath    = $basePath
                            Errors      = @("Skipped: ancestor stage '$failedAncestor' failed")
                        }
                    }
                }
                continue
            }

            # Build per-stage fetch context. AuthConfig is passed through for
            # Get-* fetchers that need per-invocation auth (mostly SPO site-level
            # Connect-PnPOnline calls). Graph and EXO inline stages use the pre-
            # established session and ignore it.
            #
            # SelectFields is what the fetch should use to build URL $select
            # (union of stage minimums + entity fields). ProjectionFields is
            # what the writer projects records to (entity fields only, or
            # $null to skip projection). Keeping them separate lets an
            # entity land exactly its declared fields without exposing
            # operational scaffolding like the id that roots emit for
            # child stages.
            $fetchContext = @{
                RunId            = $Context.RunId
                SelectFields     = $selectFields
                ProjectionFields = $projectionFields
                WriteRecords     = $writeRecords
                SourceType       = $Context.SourceType
                SourceKey        = $Context.SourceKey
                AuthConfig       = $Context.AuthConfig
            }
            if ($Context.Extra) {
                foreach ($k in $Context.Extra.Keys) { $fetchContext[$k] = $Context.Extra[$k] }
            }

            if (-not $stage.RunsOnPool) {
                if ($null -ne $stage.InputFrom) {
                    throw "Inline stages with InputFrom are not yet supported (stage: $stageName)"
                }

                $stageStartTime = Get-Date
                Write-Event -EventType stage_started -Stage $stageName -Entity $stageEntity -Properties @{
                    input_count = $null
                }
                # Heartbeat: per-entity stage transitions feed the aggregator.
                # Calls are best-effort; ProgressHeartbeat returns early when
                # not initialized (CONTAINER_TYPE unset for manual `docker run`).
                # Inline stages have no meaningful input count — match the
                # stage_started event's null and let the heartbeat blob also
                # surface input_count: null (vs 0 = "no inputs").
                # Prereq stages (no requested entity claims this stage) fall
                # through to the parallel prerequisite_entities bucket (#362).
                if ($stageEntity) {
                    try { Set-StageRunning -Entity $stageEntity -Stage $stageName } catch { Write-Verbose $_.Exception.Message }
                } else {
                    try { Set-PrerequisiteStageRunning -Stage $stageName } catch { Write-Verbose $_.Exception.Message }
                }

                # Streaming output for inline stages: the Get-* fetcher calls
                # WriteRecord per-record, writer auto-flushes to the JsonlRecordWriter
                # every N records via the callback. Prevents unbounded memory growth
                # for high-volume root stages (entra_users, entra_sign_in_logs).
                # Auto-emit safety net (#297): when EmitIds=$true and IdKey
                # is single-field, the writer auto-extracts ID-per-record.
                # Composite IdKey templates ('a:::b') opt out — those still
                # require the fetcher to assemble the composite via explicit
                # EmitId. $null disables.
                $autoEmitField = $null
                if ($stage.EmitIds -and $stage.IdKey -and $stage.IdKey -notmatch ':::') {
                    $autoEmitField = [string]$stage.IdKey
                }

                $localFile   = $null
                $jsonlWriter = $null
                $sw          = $null
                if ($writeRecords) {
                    $localFile = Join-Path $Context.TempRoot "${stageEntity}_$($Context.RunId).jsonl"
                    # Inline-stage JsonDepth knob: same +1 envelope math the pool
                    # path applies in WorkerPool.psm1. When unset, the writer's
                    # 4-arg constructor falls through to New-EnvelopedJsonLine's
                    # default. See issue #162.
                    $jsonlWriter = if ($null -ne $stage.JsonDepth) {
                        [JsonlRecordWriter]::new($localFile, $Context.SourceType, $Context.SourceKey, $Context.RunId, ([int]$stage.JsonDepth + 1))
                    } else {
                        [JsonlRecordWriter]::new($localFile, $Context.SourceType, $Context.SourceKey, $Context.RunId)
                    }
                    # GetNewClosure binds $jsonlWriter into the scriptblock so
                    # it's still reachable when the writer invokes the callback.
                    $flushCb = {
                        param($records)
                        foreach ($r in $records) { $jsonlWriter.WriteRecord($r) }
                    }.GetNewClosure()
                    $sw = New-StageWriter -FlushCallback $flushCb -AutoFlushThreshold 1000 -SelectFields $projectionFields -AutoEmitIdField $autoEmitField
                } else {
                    # Non-writing inline stage (only emits IDs for descendants).
                    # We pass a discard FlushCallback with a buffer threshold
                    # so if a Get-* fetcher accidentally calls WriteRecord for
                    # a non-writing stage (e.g., forgets the SelectFields gate),
                    # records are drained to /dev/null rather than buffered
                    # unboundedly. TotalWritten still reflects the intent for
                    # diagnostic/logging purposes.
                    $sw = New-StageWriter -FlushCallback { param($records) } -AutoFlushThreshold 1000 -AutoEmitIdField $autoEmitField
                }

                try {
                    # Retry loop for inline stages — mirrors the pool dispatch
                    # block's classification cascade in WorkerPool.psm1. When a
                    # long-running pool stage pushes elapsed time past the token
                    # lifetime, later inline stages hit 401 on their first
                    # cmdlet call. A cmdlet like Get-EXOMailbox -ResultSize
                    # Unlimited can also hit 429 throttle mid-enumeration.
                    #
                    # Retry is only safe when the fetch wrote zero records
                    # (the error hit before any data streamed) — partial
                    # output cannot be retried without producing duplicates.
                    #
                    # Classification order matches the pool:
                    #   NonRetryable/Skippable → throw immediately
                    #   RetryExhausted → throw immediately — the fetcher's
                    #                    inner Invoke-WithRetry already spent
                    #                    its budget on this error; re-retrying
                    #                    here would nest 5 × 5 (#526)
                    #   partial output         → throw (can't retry safely)
                    #   exhaustion             → throw (retries spent)
                    #   Auth         → Restore-ServiceConnection, recreate writer, retry
                    #   Throttle     → backoff, retry
                    #   Unknown      → backoff, retry
                    $inlineMaxRetries = 5
                    $inlineAttempt    = 0
                    $inlineApiFamily  = $stage.ApiFamily ?? 'graph'
                    while ($true) {
                        try {
                            & "$($module.Name)\$($stage.Function)" -Context $fetchContext -Writer $sw
                            $sw.Flush()
                            if ($jsonlWriter) { $jsonlWriter.Flush() }
                            [void]$sw.PromoteAutoEmittedIds()
                            if ($stage.EmitIds -and $sw.TotalWritten -gt 0 -and $sw.EmittedIds.Count -eq 0) {
                                throw "Stage '$stageName' wrote $($sw.TotalWritten) record(s) but emitted zero IDs. EmitIds=`$true with IdKey='$($stage.IdKey)' — either no record carries that field, or a composite-IdKey fetcher forgot to call `$Writer.EmitId(). Downstream stages would silently land record_count=0."
                            }
                            break
                        }
                        catch {
                            $inlineClass = Get-ErrorClassification -ErrorRecord $_ -ApiFamily $inlineApiFamily

                            if ($inlineClass.Category -in @('NonRetryable', 'Skippable', 'RetryExhausted')) { throw }
                            if ($sw.TotalWritten -gt 0 -or $sw.EmittedIds.Count -gt 0) { throw }
                            if ($inlineAttempt -ge $inlineMaxRetries) { throw }

                            $inlineAttempt++

                            if ($inlineClass.Category -eq 'Auth') {
                                Write-Log "Inline stage '$stageName' hit auth error, reconnecting (attempt $inlineAttempt/$inlineMaxRetries): $($inlineClass.Message)" -Level WARN -Entity $stageEntity -TenantKey $Context.TenantKey
                                try { $null = Restore-ServiceConnection }
                                catch {
                                    Write-Warning "Reconnect failed for inline stage '$stageName' (attempt $inlineAttempt): $($_.Exception.Message)"
                                }
                                # Auth requires a fresh session — recreate the writer
                                # so stale internal state from the old connection
                                # doesn't carry over.
                                if ($jsonlWriter) {
                                    $jsonlWriter.Dispose()
                                    $jsonlWriter = $null
                                }
                                if ($writeRecords) {
                                    $localFile = Join-Path $Context.TempRoot "${stageEntity}_$($Context.RunId).jsonl"
                                    $jsonlWriter = if ($null -ne $stage.JsonDepth) {
                                        [JsonlRecordWriter]::new($localFile, $Context.SourceType, $Context.SourceKey, $Context.RunId, ([int]$stage.JsonDepth + 1))
                                    } else {
                                        [JsonlRecordWriter]::new($localFile, $Context.SourceType, $Context.SourceKey, $Context.RunId)
                                    }
                                    $flushCb = {
                                        param($records)
                                        foreach ($r in $records) { $jsonlWriter.WriteRecord($r) }
                                    }.GetNewClosure()
                                    $sw = New-StageWriter -FlushCallback $flushCb -AutoFlushThreshold 1000 -SelectFields $projectionFields -AutoEmitIdField $autoEmitField
                                } else {
                                    $sw = New-StageWriter -FlushCallback { param($records) } -AutoFlushThreshold 1000 -AutoEmitIdField $autoEmitField
                                }
                            } elseif ($inlineClass.Category -eq 'Throttle') {
                                $delay = Get-RetryDelay -Classification $inlineClass -Attempt $inlineAttempt
                                Write-Log "Inline stage '$stageName' throttled, backing off ${delay}s (attempt $inlineAttempt/$inlineMaxRetries): $($inlineClass.Message)" -Level WARN -Entity $stageEntity -TenantKey $Context.TenantKey
                                Write-ThrottleEvent -Stage $stageName -Entity $stageEntity `
                                    -RetryAfterSeconds ([int]$delay) -Attempt $inlineAttempt `
                                    -StatusCode ([int]($inlineClass.StatusCode ?? 0)) -Message $inlineClass.Message
                                Start-Sleep -Seconds $delay
                            } else {
                                $delay = Get-RetryDelay -Classification $inlineClass -Attempt $inlineAttempt
                                $exType = $_.Exception.GetType().FullName
                                $innerType = $exType
                                $cur = $_.Exception.InnerException
                                while ($cur) { $innerType = $cur.GetType().FullName; $cur = $cur.InnerException }
                                Write-Log "Inline stage '$stageName' hit unknown error, backing off ${delay}s (attempt $inlineAttempt/$inlineMaxRetries): ${exType}: $($inlineClass.Message)" -Level WARN -Entity $stageEntity -TenantKey $Context.TenantKey
                                Write-UnknownRetryEvent -Stage $stageName -Entity $stageEntity `
                                    -ApiFamily $inlineApiFamily -Attempt $inlineAttempt -DelaySeconds ([int]$delay) `
                                    -StatusCode ([int]($inlineClass.StatusCode ?? 0)) `
                                    -ExceptionType $exType -InnerExceptionType $innerType -Message $inlineClass.Message
                                Start-Sleep -Seconds $delay
                            }
                        }
                    }
                }
                catch {
                    $stageErrMsg = $_.Exception.Message
                    Write-Log "Inline stage '$stageName' threw: $stageErrMsg" -Level ERROR -Entity $stageEntity -TenantKey $Context.TenantKey
                    Write-Log "Stack: $($_.ScriptStackTrace)" -Level ERROR -Entity $stageEntity -TenantKey $Context.TenantKey
                    $failedStages.Add($stageName) | Out-Null
                    $stageDurationMs = [int]((Get-Date) - $stageStartTime).TotalMilliseconds
                    Write-Event -EventType stage_failed -Stage $stageName -Entity $stageEntity -Properties @{
                        error_class   = $_.Exception.GetType().FullName
                        error_message = $stageErrMsg
                        duration_ms   = $stageDurationMs
                    }
                    if ($stageEntity) {
                        try { Set-StageCompleted -Entity $stageEntity -Stage $stageName -RecordsSoFar 0 -DurationMs $stageDurationMs } catch { Write-Verbose $_.Exception.Message }
                    } else {
                        try { Set-PrerequisiteStageCompleted -Stage $stageName -RecordsSoFar 0 -DurationMs $stageDurationMs -Status 'failed' -Errors @($stageErrMsg) } catch { Write-Verbose $_.Exception.Message }
                    }
                    if ($writeRecords) {
                        & $emitResult @{
                            EntityName  = $stageEntity
                            StageName   = $stageName
                            Status      = 'failed'
                            RecordCount = 0
                            ChunkCount  = 0
                            LocalPaths  = @()
                            BlobPaths   = @()
                            BasePath    = $basePath
                            Errors      = @($_.Exception.Message)
                        }
                    }
                    $stageEmitted[$stageName] = @()
                    continue
                }
                finally {
                    if ($jsonlWriter) { $jsonlWriter.Dispose() }
                }

                $stageEmitted[$stageName] = @($sw.EmittedIds)

                $stageDurationMs = [int]((Get-Date) - $stageStartTime).TotalMilliseconds
                # See Select-InlineRecordsSoFar above for the rationale on
                # why the source flips on $writeRecords (#373).
                $recordsSoFar = Select-InlineRecordsSoFar -WriteRecords $writeRecords -TotalWritten ([int]$sw.TotalWritten) -EmittedIdsCount ([int]$sw.EmittedIds.Count)
                Write-Event -EventType stage_completed -Stage $stageName -Entity $stageEntity -Properties @{
                    records_so_far = $recordsSoFar
                    duration_ms    = $stageDurationMs
                }
                if ($stageEntity) {
                    try { Set-StageCompleted -Entity $stageEntity -Stage $stageName -RecordsSoFar $recordsSoFar -DurationMs $stageDurationMs } catch { Write-Verbose $_.Exception.Message }
                } else {
                    try { Set-PrerequisiteStageCompleted -Stage $stageName -RecordsSoFar $recordsSoFar -DurationMs $stageDurationMs } catch { Write-Verbose $_.Exception.Message }
                }

                if ($writeRecords) {
                    # When the fetch emitted zero records the StreamWriter still
                    # created a 0-byte JSONL. Don't claim a chunk or a blob path
                    # for it — upload is skipped downstream (ADLS APPEND rejects
                    # zero-byte bodies), and the manifest should read chunk_count=0
                    # to reflect what actually lands.
                    # Intermediate vars (not inline `if { @(...) }`) because
                    # PowerShell's if-expression return unwraps single-element
                    # arrays, which would turn LocalPaths into a bare string.
                    $localPaths = @()
                    $blobPaths  = @()
                    $chunkCount = 0
                    if ($sw.TotalWritten -gt 0) {
                        $localPaths = @($localFile)
                        $blobPaths  = @("$basePath/${stageEntity}_$($Context.RunId).jsonl")
                        $chunkCount = 1
                    }
                    & $emitResult @{
                        EntityName  = $stageEntity
                        StageName   = $stageName
                        Status      = 'success'
                        RecordCount = $sw.TotalWritten
                        ChunkCount  = $chunkCount
                        LocalPaths  = $localPaths
                        BlobPaths   = $blobPaths
                        BasePath    = $basePath
                        Errors      = @()
                    }
                }
            }
            else {
                # --- Pool stage path with sibling-tier batching (#145 items 1, 2) ---
                #
                # We accumulate consecutive pool stages in $executionOrder that
                # share the same parent + ApiFamily into one "tier" and dispatch
                # them concurrently into a shared, cached pool. For modules
                # like EntraServicePrincipals (6 sibling pool stages) and
                # PowerPlatEnvironments (19 Dataverse data entities under
                # dataverse_onboardings), this turns N sequential pool setups
                # + N pre-auth rounds into 1 setup + 1 first-dispatch auth-
                # latch per runspace.
                #
                # Tier eligibility: a stage immediately after the current one
                # in $executionOrder joins the tier when it (a) is also a
                # pool stage, (b) has the same InputFrom, and (c) has the same
                # ApiFamily. Differing parent or family breaks the tier — the
                # next iteration of this foreach starts a new one.

                if ($null -eq $stage.InputFrom) {
                    throw "Pool stage '$stageName' must have an InputFrom (cannot be a root stage)"
                }
                if (-not $stageEmitted.ContainsKey($stage.InputFrom)) {
                    throw "Pool stage '$stageName' requires parent '$($stage.InputFrom)' to have run first"
                }

                # Walk $executionOrder forward to assemble the tier. The
                # iteration variable $stageName + the per-stage prep above
                # already gave us the FIRST tier member; we just need to
                # find any siblings that follow.
                $tierNames = @($stageName)
                $orderArray = @($executionOrder)
                $myIndex = $orderArray.IndexOf($stageName)
                $next = $myIndex + 1
                while ($next -lt $orderArray.Count) {
                    $candName = $orderArray[$next]
                    if ($stagesHandled.Contains($candName)) { $next++; continue }
                    $candStage = $stages[$candName]
                    if (-not $candStage.RunsOnPool) { break }
                    if ($candStage.InputFrom -ne $stage.InputFrom) { break }
                    if ($candStage.ApiFamily -ne $stage.ApiFamily) { break }
                    # Failed-ancestor check: if the candidate's parent failed,
                    # it'll be skipped by the existing path on its own
                    # iteration — don't pull it into the tier here. (The
                    # outer foreach handles the skip event + result entry.)
                    $cAncestor = $candStage.InputFrom
                    $cFailed = $false
                    while ($null -ne $cAncestor) {
                        if ($failedStages.Contains($cAncestor)) { $cFailed = $true; break }
                        $cAncestor = $stages[$cAncestor].InputFrom
                    }
                    if ($cFailed) { break }
                    $tierNames += $candName
                    $next++
                }

                # Per-stage state for every tier member. The first member's
                # prep was done at the top of this foreach iteration; build
                # equivalent state for the remaining members. Capture into
                # an array of hashtables keyed by stage name so the dispatch
                # + result-processing loops can index uniformly.
                $tierStates = [ordered]@{}
                $tierStates[$stageName] = @{
                    Stage              = $stage
                    StageEntities      = $stageEntities
                    StageEntity        = $stageEntity
                    AllStageEntities   = $allStageEntities
                    WriteRecords       = $writeRecords
                    BasePath           = $basePath
                    FetchContext       = $fetchContext
                }
                for ($k = 1; $k -lt $tierNames.Count; $k++) {
                    $tn = $tierNames[$k]
                    $ts = $stages[$tn]
                    $tSelectFields     = Resolve-SelectFields     -StageName $tn -Stages $stages -Entities $entities -RequestedEntities $RequestedEntities
                    $tProjectionFields = Resolve-ProjectionFields -StageName $tn                 -Entities $entities -RequestedEntities $RequestedEntities

                    $tAllEntities = @()
                    foreach ($ename in $entities.Keys) {
                        $entityStages = @($entities[$ename].Stage)
                        if ($entityStages -contains $tn) { $tAllEntities += $ename }
                    }
                    $tStageEntities = @($tAllEntities | Where-Object { $requestedSet.Contains($_) })
                    $tStageEntity = if ($tStageEntities.Count -ge 1) { $tStageEntities[0] } else { $null }
                    $tWriteRecords = ($tStageEntities.Count -gt 0)
                    $tRootEntity = $rootStageToEntity[$stageToRoot[$tn]]
                    $tBasePath = "$tRootEntity/$($Context.TenantKey)/$($Context.Date)"

                    $tFetchContext = @{
                        RunId            = $Context.RunId
                        SelectFields     = $tSelectFields
                        ProjectionFields = $tProjectionFields
                        WriteRecords     = $tWriteRecords
                        SourceType       = $Context.SourceType
                        SourceKey        = $Context.SourceKey
                        AuthConfig       = $Context.AuthConfig
                    }
                    if ($Context.Extra) {
                        foreach ($ek in $Context.Extra.Keys) { $tFetchContext[$ek] = $Context.Extra[$ek] }
                    }

                    $tierStates[$tn] = @{
                        Stage              = $ts
                        StageEntities      = $tStageEntities
                        StageEntity        = $tStageEntity
                        AllStageEntities   = $tAllEntities
                        WriteRecords       = $tWriteRecords
                        BasePath           = $tBasePath
                        FetchContext       = $tFetchContext
                    }
                }

                # Per-stage parent-input resolution. Sibling stages share
                # the same parent's emittedIds but may declare different
                # InputFilters, so each gets its own filtered inputIds +
                # inputTags.
                $tierInputs = @{}
                foreach ($tn in $tierNames) {
                    $ts = $tierStates[$tn].Stage
                    $pe = $stageEmitted[$ts.InputFrom]
                    if ($ts.InputFilter) {
                        $pe = @($pe | Where-Object { & $ts.InputFilter $_.Tags })
                    }
                    $tIds = @($pe | ForEach-Object { $_.Id })
                    $tTags = @{}
                    foreach ($p in $pe) {
                        if ($null -ne $p.Tags) { $tTags[$p.Id] = $p.Tags }
                    }
                    $tierInputs[$tn] = @{ InputIds = $tIds; InputTags = $tTags }
                }

                # Stage-started events fire for every tier member (whether
                # or not they have inputs) so the event stream stays at
                # parity with the manifest.
                $tierStartTimes = @{}
                foreach ($tn in $tierNames) {
                    $tierStartTimes[$tn] = Get-Date
                    $count = $tierInputs[$tn].InputIds.Count
                    Write-Event -EventType stage_started -Stage $tn -Entity $tierStates[$tn].StageEntity -Properties @{
                        input_count = $count
                    }
                    # Heartbeat: pool stages mark running here. Worker
                    # runspaces feed records_so_far via the synchronized
                    # hashtable; aggregator timer folds it into this stage's
                    # entry every $HEARTBEAT_FLUSH_SECONDS. Prereq stages
                    # (#362) route to the prerequisite_entities bucket — the
                    # worker fold-projection in ProgressHeartbeat treats
                    # both buckets uniformly since slot keys are stage-only.
                    if ($tierStates[$tn].StageEntity) {
                        try { Set-StageRunning -Entity $tierStates[$tn].StageEntity -Stage $tn -InputCount $count } catch { Write-Verbose $_.Exception.Message }
                    } else {
                        try { Set-PrerequisiteStageRunning -Stage $tn -InputCount $count } catch { Write-Verbose $_.Exception.Message }
                    }
                }

                # Stages with no inputs emit zero results immediately and
                # are dropped from the dispatch set. Stages with inputs go
                # into $batchUnits and get an output dir.
                $batchUnits = @()
                $tierOutputDirs = @{}
                foreach ($tn in $tierNames) {
                    $tState = $tierStates[$tn]
                    $inputIdsT = $tierInputs[$tn].InputIds

                    if ($inputIdsT.Count -eq 0) {
                        # Empty input — emit completed/0 and result/0 entries
                        # without dispatching. Distinct from stage_skipped
                        # (ancestor failed); this stage was considered and
                        # ran cleanly with no work.
                        $emptyDurationMs = [int]((Get-Date) - $tierStartTimes[$tn]).TotalMilliseconds
                        Write-Event -EventType stage_completed -Stage $tn -Entity $tState.StageEntity -Properties @{
                            records_so_far = 0
                            duration_ms    = $emptyDurationMs
                            skipped_count  = 0
                            failed_count   = 0
                            error_count    = 0
                        }
                        # Heartbeat: close out the stage so the blob doesn't
                        # leave it 'running' after the tier finished. Set-Stage
                        # Running fired earlier (around stage_started), and
                        # without a matching Completed call the prereq stage
                        # bucket — and the entity bucket too — would carry
                        # status='running' forever.
                        if ($tState.StageEntity) {
                            try { Set-StageCompleted -Entity $tState.StageEntity -Stage $tn -RecordsSoFar 0 -DurationMs $emptyDurationMs } catch { Write-Verbose $_.Exception.Message }
                        } else {
                            try { Set-PrerequisiteStageCompleted -Stage $tn -RecordsSoFar 0 -DurationMs $emptyDurationMs } catch { Write-Verbose $_.Exception.Message }
                        }
                        $stageEmitted[$tn] = @()
                        if ($tState.WriteRecords) {
                            foreach ($en in $tState.StageEntities) {
                                & $emitResult @{
                                    EntityName  = $en
                                    StageName   = $tn
                                    Status      = 'success'
                                    RecordCount = 0
                                    ChunkCount  = 0
                                    LocalPaths  = @()
                                    BlobPaths   = @()
                                    BasePath    = $tState.BasePath
                                    Errors      = @()
                                }
                            }
                        }
                        continue
                    }

                    $outputDirT = Join-Path $Context.TempRoot "stage_${tn}_$($Context.RunId)"
                    New-Item -ItemType Directory -Path $outputDirT -Force | Out-Null
                    $tierOutputDirs[$tn] = $outputDirT

                    $unit = @{
                        StageName        = $tn
                        FunctionName     = $tState.Stage.Function
                        InputIds         = $inputIdsT
                        OutputDirectory  = $outputDirT
                        Entity           = if ($tState.StageEntity) { $tState.StageEntity } else { '' }
                        WriteRecords     = $tState.WriteRecords
                    }
                    if ($null -ne $tState.Stage.JsonDepth) { $unit['JsonDepth'] = [int]$tState.Stage.JsonDepth }
                    if ($tierInputs[$tn].InputTags.Count -gt 0) { $unit['InputTags'] = $tierInputs[$tn].InputTags }
                    if ($null -ne $tState.Stage.AutoFlushThreshold -and [int]$tState.Stage.AutoFlushThreshold -gt 0) {
                        $unit['AutoFlushThreshold'] = [int]$tState.Stage.AutoFlushThreshold
                    }
                    # Auto-emit safety net (#297): single-field IdKey only.
                    # Composite IdKey ('a:::b') still requires explicit EmitId
                    # because the fetcher assembles the composite.
                    if ($tState.Stage.EmitIds -and $tState.Stage.IdKey -and $tState.Stage.IdKey -notmatch ':::') {
                        $unit['AutoEmitIdField'] = [string]$tState.Stage.IdKey
                    }
                    $batchUnits += $unit
                }

                # If every tier member had empty inputs, mark all handled and
                # advance — nothing to dispatch.
                if ($batchUnits.Count -eq 0) {
                    foreach ($tn in $tierNames) { $stagesHandled.Add($tn) | Out-Null }
                    continue
                }

                # Get-or-create the cached pool for (ApiFamily, ModulePath).
                # All tier members share the same key by construction. The
                # pool is reused across pool stages within this Invoke-
                # ModuleRun so first-dispatch auth state on each runspace
                # carries forward instead of being re-paid per stage.
                #
                # Skip pool creation entirely when WORKER_POOL_MODE=outofprocess
                # — the OOP dispatchers ignore the $Pool arg, and creating a
                # RunspacePool here would load every entity module + the auth
                # module into N runspaces in the parent process. That's
                # significant memory (~50 MB/runspace × PoolSize) in the very
                # scenario meant to reduce parent memory pressure.
                $poolKey = "$($stage.ApiFamily)|$ModulePath"
                $passPool = $null
                if ($env:WORKER_POOL_MODE -ne 'outofprocess') {
                    if (-not $poolCache.ContainsKey($poolKey)) {
                        $poolCache[$poolKey] = New-StagePool `
                            -ApiFamily      $stage.ApiFamily `
                            -ModulePath     $ModulePath `
                            -AuthModulePath $Context.AuthModulePath `
                            -PoolSize       $Context.PoolSize
                    }
                    $passPool = $poolCache[$poolKey]
                }

                # Dispatch.
                $batchResults = $null
                $batchError = $null
                # Heartbeat (#314): worker runspaces stream per-slice progress
                # through this synchronized hashtable. Threaded through via
                # $Context.ProgressShared from Invoke-Ingestion.ps1 — null when
                # ProgressHeartbeat isn't running (manual `docker run`, tests).
                $progressShared = if ($Context.ContainsKey('ProgressShared')) { $Context.ProgressShared } else { $null }

                # OOP dispatch routing (#484 spike). When WORKER_POOL_MODE is
                # set to "outofprocess", switch to the chunk-per-process
                # variants that recycle the pwsh process between chunks —
                # the only mechanism that reliably reclaims EXO module state.
                # Default unchanged for every container that doesn't set the
                # env var, so graph/spo/etc keep the in-process path.
                $useOop = ($env:WORKER_POOL_MODE -eq 'outofprocess')
                $dispatchSingle = if ($useOop) { 'Invoke-StagePoolOutOfProcess' } else { 'Invoke-StagePool' }
                $dispatchBatch  = if ($useOop) { 'Invoke-StagePoolBatchOutOfProcess' } else { 'Invoke-StagePoolBatch' }

                try {
                    if ($batchUnits.Count -eq 1) {
                        $unit = $batchUnits[0]
                        $singleParams = @{
                            StageName       = $unit.StageName
                            ModulePath      = $ModulePath
                            FunctionName    = $unit.FunctionName
                            InputIds        = $unit.InputIds
                            Context         = $tierStates[$unit.StageName].FetchContext
                            OutputDirectory = $unit.OutputDirectory
                            RunId           = $Context.RunId
                            Tenant          = $Context.TenantKey
                            Entity          = $unit.Entity
                            AuthModulePath  = $Context.AuthModulePath
                            PoolSize        = $Context.PoolSize
                            ApiFamily       = $stage.ApiFamily
                            SourceType      = $Context.SourceType
                            SourceKey       = $Context.SourceKey
                            WriteRecords    = $unit.WriteRecords
                            Pool            = $passPool
                            ProgressShared  = $progressShared
                        }
                        if ($null -ne $unit.JsonDepth) { $singleParams['JsonDepth']         = $unit.JsonDepth }
                        if ($unit.InputTags)          { $singleParams['InputTags']          = $unit.InputTags }
                        if ($unit.AutoFlushThreshold) { $singleParams['AutoFlushThreshold'] = $unit.AutoFlushThreshold }
                        if ($unit.AutoEmitIdField)    { $singleParams['AutoEmitIdField']    = $unit.AutoEmitIdField }
                        $singleResult = & $dispatchSingle @singleParams
                        $batchResults = @{ ($unit.StageName) = $singleResult }
                    }
                    else {
                        # Each sibling needs its own Context because per-stage
                        # ProjectionFields / SelectFields differ; attach the
                        # tier member's FetchContext to its unit so the dispatch
                        # template (which reads $Context.ProjectionFields) sees
                        # the right one per chunk. Invoke-StagePoolBatch's
                        # function-level -Context is the fallback; per-unit
                        # .Context wins when present.
                        foreach ($u in $batchUnits) {
                            $u['Context'] = $tierStates[$u.StageName].FetchContext
                        }
                        $batchResults = & $dispatchBatch `
                            -StageUnits     $batchUnits `
                            -ModulePath     $ModulePath `
                            -Context        $tierStates[$batchUnits[0].StageName].FetchContext `
                            -RunId          $Context.RunId `
                            -Tenant         $Context.TenantKey `
                            -AuthModulePath $Context.AuthModulePath `
                            -SourceType     $Context.SourceType `
                            -SourceKey      $Context.SourceKey `
                            -PoolSize       $Context.PoolSize `
                            -ApiFamily      $stage.ApiFamily `
                            -Pool           $passPool `
                            -ProgressShared $progressShared
                    }
                }
                catch {
                    $batchError = $_
                }

                if ($batchError) {
                    # Mark every tier-member-with-inputs as failed.
                    foreach ($u in $batchUnits) {
                        $tn = $u.StageName
                        $tState = $tierStates[$tn]
                        $failedStages.Add($tn) | Out-Null
                        $batchDurationMs = [int]((Get-Date) - $tierStartTimes[$tn]).TotalMilliseconds
                        Write-Event -EventType stage_failed -Stage $tn -Entity $tState.StageEntity -Properties @{
                            error_class   = $batchError.Exception.GetType().FullName
                            error_message = $batchError.Exception.Message
                            duration_ms   = $batchDurationMs
                        }
                        # Heartbeat: mark the stage failed (not completed).
                        if ($tState.StageEntity) {
                            try { Set-StageCompleted -Entity $tState.StageEntity -Stage $tn -RecordsSoFar 0 -DurationMs $batchDurationMs } catch { Write-Verbose $_.Exception.Message }
                        } else {
                            try { Set-PrerequisiteStageCompleted -Stage $tn -RecordsSoFar 0 -DurationMs $batchDurationMs -Status 'failed' -Errors @($batchError.Exception.Message) } catch { Write-Verbose $_.Exception.Message }
                        }
                        if ($tState.WriteRecords) {
                            foreach ($en in $tState.StageEntities) {
                                & $emitResult @{
                                    EntityName  = $en
                                    StageName   = $tn
                                    Status      = 'failed'
                                    RecordCount = 0
                                    ChunkCount  = 0
                                    LocalPaths  = @()
                                    BlobPaths   = @()
                                    BasePath    = $tState.BasePath
                                    Errors      = @($batchError.Exception.Message)
                                }
                            }
                        }
                        $stageEmitted[$tn] = @()
                    }
                    foreach ($tn in $tierNames) { $stagesHandled.Add($tn) | Out-Null }
                    continue
                }

                # Per-tier-member result processing. Same shape as the
                # pre-#145 single-stage path; runs in a loop because we
                # have N results (one per dispatched unit).
                foreach ($u in $batchUnits) {
                    $tn = $u.StageName
                    $tState = $tierStates[$tn]
                    $poolResult = $batchResults[$tn]
                    $inputIdsT = $u.InputIds
                    $outputDirT = $tierOutputDirs[$tn]

                    $stageEmitted[$tn] = @($poolResult.EmittedIds)

                    # #356: classify total-failure modes for the stage_failed
                    # event. Two distinct shapes:
                    #
                    #   AllItemsExhausted — every input ran through the dispatch
                    #     block and exited via Skipped (404) or Failed
                    #     (NonRetryable / max-retries-exhausted). Includes the
                    #     100%-NonRetryable channel_members case AND the
                    #     100%-Skippable auth-scope-regression case AND any mix.
                    #     The per-item counters cover the full input.
                    #
                    #   TotalFailure — chunk-fatal errors (e.g. first-dispatch
                    #     auth throw — see #342) that prevented per-item
                    #     processing entirely, so SkippedCount + FailedCount
                    #     does NOT cover InputCount, but Errors.Count > 0
                    #     because the aggregator captured chunk-level failures.
                    #
                    # AllItemsExhausted is checked first so 100%-NonRetryable
                    # (which populates both FailedCount and Errors) lands in
                    # the more-specific class. KQL alerts on AllItemsExhausted
                    # catch the per-item-failure mode; alerts on TotalFailure
                    # catch the chunk-fatal mode.
                    $totalFailure = $false
                    $allExhausted = $false
                    if ($poolResult.RecordCount -eq 0 -and
                        $poolResult.EmittedIds.Count -eq 0 -and
                        $inputIdsT.Count -gt 0) {
                        $exhaustedCount = [int]$poolResult.SkippedCount + [int]$poolResult.FailedCount
                        if ($exhaustedCount -ge $inputIdsT.Count) {
                            $totalFailure = $true
                            $allExhausted = $true
                        }
                    }
                    if (-not $totalFailure -and
                        $poolResult.RecordCount -eq 0 -and
                        $poolResult.EmittedIds.Count -eq 0 -and
                        $inputIdsT.Count -gt 0 -and
                        $poolResult.Errors.Count -gt 0) {
                        $totalFailure = $true
                    }

                    # #297 stage-level guard: stage wrote records across the
                    # pool but emitted zero IDs. The single-field IdKey path
                    # is already caught per-item inside the dispatch block;
                    # this catches composite-IdKey ('a:::b') stages where the
                    # fetcher forgot EmitId on every item — auto-extract can't
                    # help because the composite must be assembled by the
                    # fetcher. Without this, downstream stages silently land
                    # record_count=0. Treat as failure to halt the cascade.
                    if (-not $totalFailure -and $tState.Stage.EmitIds -and
                        $poolResult.RecordCount -gt 0 -and
                        $poolResult.EmittedIds.Count -eq 0) {
                        $totalFailure = $true
                        $poolResult.Errors = @($poolResult.Errors) + @(
                            "Stage '$tn' wrote $($poolResult.RecordCount) record(s) across the pool but emitted zero IDs. EmitIds=`$true with IdKey='$($tState.Stage.IdKey)' — composite-IdKey stages must call `$Writer.EmitId() explicitly. Halting cascade to prevent silent record_count=0 downstream."
                        )
                    }
                    $tierStageDurationMs = [int]((Get-Date) - $tierStartTimes[$tn]).TotalMilliseconds
                    if ($totalFailure) {
                        $failedStages.Add($tn) | Out-Null
                        # #356: error_class='AllItemsExhausted' specifically
                        # tags the every-item-Skippable / every-item-NonRetryable
                        # case so KQL alerts can target it. Existing TotalFailure
                        # path (Errors.Count > 0 with no skippable rollover)
                        # keeps its name for back-compat with any alert wired
                        # against the prior shape.
                        $errClass = if ($allExhausted) { 'AllItemsExhausted' } else { 'TotalFailure' }
                        $errMsg = if ($allExhausted) {
                            "All $($inputIdsT.Count) inputs exhausted: skipped=$([int]$poolResult.SkippedCount) failed=$([int]$poolResult.FailedCount) records=0"
                        } else {
                            "All $($inputIdsT.Count) inputs failed: $($poolResult.Errors[0])"
                        }
                        Write-Event -EventType stage_failed -Stage $tn -Entity $tState.StageEntity -Properties @{
                            error_class   = $errClass
                            error_message = $errMsg
                            duration_ms   = $tierStageDurationMs
                        }
                    } else {
                        $failedCount = [int]$poolResult.FailedCount
                        $skippedCount = [int]$poolResult.SkippedCount
                        $errorCount = @($poolResult.Errors).Count
                        # Plain @{} — Write-Event's -Properties param is
                        # typed [hashtable] (EventEmitter.psm1), so an
                        # [ordered]@{} is coerced and the order is lost
                        # anyway. KQL queries read fields by name and don't
                        # care about JSON property order. See #383 PR
                        # discussion.
                        Write-Event -EventType stage_completed -Stage $tn -Entity $tState.StageEntity -Properties @{
                            # #383: per-input-item totals from the pool result.
                            # Dashboard divides items_processed / input_count
                            # for true % complete.
                            items_processed = [int]$poolResult.ItemsProcessed
                            items_failed    = [int]$poolResult.ItemsFailed
                            items_skipped   = [int]$poolResult.ItemsSkipped
                            records_so_far  = $poolResult.RecordCount
                            skipped_count   = $skippedCount
                            failed_count    = $failedCount
                            # error_count stays for back-compat with any
                            # existing KQL — it equals @($errors).Count, which
                            # in turn equals the chunk-level error string list
                            # (one entry per failed item plus chunk-fatal entries).
                            # failed_count is the new canonical for per-item
                            # failures; prefer it in new queries.
                            error_count     = $errorCount
                            duration_ms     = $tierStageDurationMs
                        }
                        # #356: surface non-zero failed/error counts to plain
                        # console logs so an operator skimming
                        # ContainerAppConsoleLogs_CL without parsing the _event:
                        # sentinel still sees that something went wrong. WARN
                        # level (not ERROR) — the run as a whole didn't fail,
                        # individual items did. Skipped-only (legitimate 404s)
                        # stays silent to avoid noise.
                        if ($failedCount -gt 0 -or $errorCount -gt 0) {
                            Write-Log "Stage '$tn' completed with failures: records=$($poolResult.RecordCount) failed=$failedCount skipped=$skippedCount errors=$errorCount" -Level WARN -Entity $tState.StageEntity
                        }
                    }
                    # Heartbeat: stage status mirrors the WriteRecords=TRUE
                    # $status below so a prereq with per-item failures (but no
                    # total failure) reports 'partial', not a misleading
                    # 'completed'/'success', to /runs/{id} observers.
                    $hbStatus = if ($allExhausted -and [int]$poolResult.FailedCount -eq 0) { 'skipped' }
                                elseif ($totalFailure) { 'failed' }
                                elseif ([int]$poolResult.FailedCount -gt 0 -or @($poolResult.Errors).Count -gt 0) { 'partial' }
                                else { 'completed' }
                    # #383: pool result carries per-input-item totals. Thread
                    # them into the heartbeat so /runs/{id} has the final
                    # items_processed / items_failed / items_skipped after
                    # the stage's last fold (the ProgressShared fold can lag
                    # by up to FlushSeconds; the explicit Set-*Completed
                    # values are authoritative).
                    $itemsProcessed = [int]$poolResult.ItemsProcessed
                    $itemsFailed    = [int]$poolResult.ItemsFailed
                    $itemsSkipped   = [int]$poolResult.ItemsSkipped
                    if ($tState.StageEntity) {
                        try { Set-StageCompleted -Entity $tState.StageEntity -Stage $tn -RecordsSoFar ([int]$poolResult.RecordCount) -DurationMs $tierStageDurationMs -ItemsProcessed $itemsProcessed -ItemsFailed $itemsFailed -ItemsSkipped $itemsSkipped } catch { Write-Verbose $_.Exception.Message }
                    } else {
                        try { Set-PrerequisiteStageCompleted -Stage $tn -RecordsSoFar ([int]$poolResult.RecordCount) -DurationMs $tierStageDurationMs -Status $hbStatus -ItemsProcessed $itemsProcessed -ItemsFailed $itemsFailed -ItemsSkipped $itemsSkipped -Errors @($poolResult.Errors) } catch { Write-Verbose $_.Exception.Message }
                    }

                    if ($tState.WriteRecords) {
                        $status = if ($allExhausted -and [int]$poolResult.FailedCount -eq 0) { 'skipped' }
                                  elseif ($totalFailure) { 'failed' }
                                  elseif ($poolResult.Errors.Count -eq 0) { 'success' }
                                  else { 'partial' }

                        $chunkFiles = Get-ChildItem $outputDirT -Filter '*.jsonl' -ErrorAction SilentlyContinue
                        $localPaths = @()
                        $blobPaths  = @()
                        $writesTo = $entities[$tState.StageEntity].WritesTo
                        $subDir = if ($writesTo -is [hashtable]) { $writesTo[$tn] } else { $writesTo }
                        foreach ($c in $chunkFiles) {
                            $localPaths += $c.FullName
                            $blobPaths  += "$($tState.BasePath)/$subDir/$($c.Name)"
                        }
                        & $emitResult @{
                            EntityName   = $tState.StageEntity
                            StageName    = $tn
                            Status       = $status
                            RecordCount  = $poolResult.RecordCount
                            ChunkCount   = @($chunkFiles).Count
                            SkippedCount = $poolResult.SkippedCount
                            FailedCount  = $poolResult.FailedCount
                            LocalPaths   = $localPaths
                            BlobPaths    = $blobPaths
                            BasePath     = $tState.BasePath
                            Errors       = @($poolResult.Errors)
                        }
                    }
                }

                # Mark every tier member as handled so the foreach skips them
                # on subsequent iterations.
                foreach ($tn in $tierNames) { $stagesHandled.Add($tn) | Out-Null }
            }
        }

        # Unary comma prevents PowerShell from unwrapping a single-element array
        # on return, so callers always get an array they can index into.
        return ,$results.ToArray()
    }
    finally {
        # Dispose any reused runspace pools opened during this module run.
        # Each pool's runspaces hold open Microsoft.Graph / EXO / PnP
        # sessions plus the cached AuthConfig (with CertificateBase64) in
        # Connect.psm1's $script: scope; closing the pool releases all of
        # those at once.
        foreach ($p in $poolCache.Values) {
            try { $p.Close(); $p.Dispose() } catch { Write-Verbose $_.Exception.Message }
        }
        Remove-Module $module -Force -ErrorAction SilentlyContinue
    }
}

Export-ModuleMember -Function Resolve-RequiredStages, Resolve-ExecutionOrder, Resolve-SelectFields, Resolve-ProjectionFields, Resolve-RootEntities, Invoke-ModuleRun, Select-InlineRecordsSoFar
