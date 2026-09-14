#Requires -Version 7.4
using module ./modules/RecordEnvelope.psm1
using module ./modules/StageExecutor.psm1

[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidGlobalVars', '', Justification = 'Set by SIGTERM event handler; $script: writes to the event scope, not this scope. Intentional cross-scope flag.')]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseDeclaredVarsMoreThanAssignments', '', Justification = 'IngestRunning assignment lives in the Register-EngineEvent action scriptblock; PSSA cannot see the read in the main entity-dispatch loop across scriptblock boundaries.')]
param()

# --- Graceful cancellation ---
# $global: (not $script:) — event action scriptblocks run in their own scope,
# and a $script: assignment inside the action writes to the event's scope
# rather than this script's. $global: is reliably visible from both sides.
$global:IngestRunning = $true
[Console]::TreatControlCAsInput = $false
$null = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
    $global:IngestRunning = $false
}

# --- Load shared modules ---
# StorageHelperRest lives here (not in each container's Connect.psm1) because
# the upload target is identical across Graph/EXO/SPO — it was duplicated
# three times until this refactor.
$modulesPath = Join-Path $PSScriptRoot 'modules'
Import-Module (Join-Path $modulesPath 'LogHelper.psm1')         -Force
Import-Module (Join-Path $modulesPath 'EventEmitter.psm1')      -Force
Import-Module (Join-Path $modulesPath 'RetryHelper.psm1')       -Force
Import-Module (Join-Path $modulesPath 'KeyVaultHelper.psm1')    -Force
Import-Module (Join-Path $modulesPath 'MsalTokenHelper.psm1')   -Force
Import-Module (Join-Path $modulesPath 'WorkerPool.psm1')        -Force
Import-Module (Join-Path $modulesPath 'StageWriter.psm1')       -Force
Import-Module (Join-Path $modulesPath 'StageExecutor.psm1')     -Force
Import-Module (Join-Path $modulesPath 'StorageHelperRest.psm1') -Force
Import-Module (Join-Path $modulesPath 'ScopeFilter.psm1')       -Force
Import-Module (Join-Path $modulesPath 'EntityRollup.psm1')      -Force
Import-Module (Join-Path $modulesPath 'ProgressHeartbeat.psm1') -Force
$uploadFunction = ${function:Write-ToAdlsRest}

# Each container ships a Connect.psm1 next to Invoke-Ingestion.ps1 (the
# Dockerfile COPYs the container's file into /app/). It exports Connect-Service
# (initial auth — used here AND by each worker runspace's pre-auth block) and
# Restore-ServiceConnection (mid-run auth recovery — used inside worker
# runspaces and by inline-stage retry in StageExecutor). The path is passed through to Invoke-StagePool so workers
# import the same module into their InitialSessionState.
$connectModulePath = Join-Path $PSScriptRoot 'Connect.psm1'
Import-Module $connectModulePath -Force

# --- ThreadPool min-worker floor (issue #269 defense-in-depth) ---
# Default min-worker count equals ProcessorCount (4 on cpu=2.0 ACA Linux
# containers). Sync-over-async MSAL token acquisition in Connect.psm1
# implementations (and Invoke-MgGraphRequest's internal handler chain)
# blocks runspace pool threads while waiting on Tasks whose continuations
# need ThreadPool workers to run. Raising the floor to 50 ensures the pool
# never starves under PoolSize=10 concurrent runspaces.
#
# Note: this was tested as the proposed fix for issue #269 and proved
# insufficient — the silent hang reproduced with min threads at 50, ruling
# out ThreadPool starvation as the mechanism. The actual fix is in
# graph-ingest/Connect.psm1 (worker piggyback on parent's GraphSession).
# We keep this as defense-in-depth: it's harmless on a healthy run and
# provides a small safety margin for future code paths that introduce
# concurrent sync-over-async patterns.
$tpWorkerBefore = 0; $tpIocpBefore = 0
[Threading.ThreadPool]::GetMinThreads([ref]$tpWorkerBefore, [ref]$tpIocpBefore) | Out-Null
[Threading.ThreadPool]::SetMinThreads(50, 50) | Out-Null
$tpWorkerAfter = 0; $tpIocpAfter = 0
[Threading.ThreadPool]::GetMinThreads([ref]$tpWorkerAfter, [ref]$tpIocpAfter) | Out-Null
Write-Log "ThreadPool min-threads: worker $tpWorkerBefore->$tpWorkerAfter iocp $tpIocpBefore->$tpIocpAfter; current ThreadCount=$([Threading.ThreadPool]::ThreadCount) PendingWorkItems=$([Threading.ThreadPool]::PendingWorkItemCount)"

# --- Read environment (all config passed by dispatcher as env vars) ---
# Validate required env vars up front: a missing TENANT_KEY or STORAGE_ACCOUNT_URL
# would otherwise surface as a confusing downstream failure (empty path
# segments, 401 on upload) or — worse — a silent partial success. Fail loudly
# here so the dispatcher marks the task Failed with a clear root cause.
$requiredEnv = @('TENANT_KEY', 'TENANT_ID', 'ORGANIZATION', 'CLIENT_ID',
                 'CERT_NAME', 'KEYVAULT_NAME', 'STORAGE_ACCOUNT_URL', 'ENTITY_NAMES')
# STORAGE_AUTH_METHOD defaults to 'managed_identity' in Write-ToAdlsRest when
# unset; demand the SP credential vars per mode when the dispatcher explicitly
# asked for SP auth against ADLS. Different cred name per mode (cert vs secret).
switch ($env:STORAGE_AUTH_METHOD) {
    'service_principal_cert' {
        $requiredEnv += @('STORAGE_SP_TENANT_ID', 'STORAGE_SP_CLIENT_ID', 'STORAGE_SP_CERT_NAME')
    }
    'service_principal_secret' {
        $requiredEnv += @('STORAGE_SP_TENANT_ID', 'STORAGE_SP_CLIENT_ID', 'STORAGE_SP_SECRET_NAME')
    }
}
$missing = @($requiredEnv | Where-Object { [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($_)) })
if ($missing.Count -gt 0) {
    Write-Error "Missing required environment variable(s): $($missing -join ', ')"
    exit 1
}

$tenantKey         = $env:TENANT_KEY
$tenantId          = $env:TENANT_ID
$organization      = $env:ORGANIZATION
$clientId          = $env:CLIENT_ID
$certName          = $env:CERT_NAME
$adminUrl          = $env:ADMIN_URL
$kvName            = $env:KEYVAULT_NAME
$maxConcurrency    = [int]($env:MAX_CONCURRENCY ?? '10')
$containerName     = $env:LANDING_CONTAINER ?? 'landing'
$storageAccountUrl = $env:STORAGE_ACCOUNT_URL
# 12 hex chars matches the dispatcher's run_id length (RunExecutor.cs).
# Used only as fallback when $env:RUN_ID is unset (manual `docker run`).
$runId             = [guid]::NewGuid().ToString('N').Substring(0, 12)

# Provenance envelope context — stamped on every record. source_type is hardcoded
# today (all sources are M365 tenants); when AD forests / other source kinds land
# (#70), drive this from the source registry instead.
$sourceType        = 'tenant'
$sourceKey         = $tenantKey

$wantedEntities    = ($env:ENTITY_NAMES -split ',') | ForEach-Object { $_.Trim() }

# Prefer the dispatcher-provided RUN_ID so a single ID correlates dispatcher
# RunRecord, customEvents, and manifests. Fall back to a local guid for manual
# `docker run` testing where the dispatcher isn't in the loop.
if (-not [string]::IsNullOrWhiteSpace($env:RUN_ID)) {
    $runId = $env:RUN_ID
}
Initialize-EventContext -RunId $runId -Tenant $tenantKey

$runStartTime    = Get-Date
$runTotalRecords = 0
$runHasFailure   = $false
$runHasPartial   = $false

# Severity rank shared between the per-module manifest aggregation (below) and
# the dispatcher summary write (after the outer module loop). Hoisted to run
# scope so the summary path doesn't redeclare it. failed > partial > skipped > success.
$statusRank = @{ success = 0; skipped = 1; partial = 2; failed = 3 }

# Entity-level rollup spanning every module in this run; written as the
# dispatcher summary after the outer foreach. Key = entity name; multiple
# BasePaths for the same entity collapse via severity-max here. See #164.
$summaryEntities = @{}
Write-Log "Starting ingestion run=$runId tenant=$tenantKey entities=$($wantedEntities.Count)"
Write-Event -EventType run_started -Properties @{ input_count = $wantedEntities.Count }

# --- Progress heartbeat (#314) ---
# Best-effort projection of in-memory state to a blob the dispatcher reads.
# Started here so the dispatcher's GET /runs/{runId} sees the run as "running"
# within a few seconds of dispatch. CONTAINER_TYPE is dispatcher-set; manual
# `docker run` testing leaves it unset and we skip the heartbeat (matching
# the manifest-summary skip below). HEARTBEAT_FLUSH_SECONDS overrides the
# default cadence (5s).
$heartbeatStarted = $false
if (-not [string]::IsNullOrWhiteSpace($env:CONTAINER_TYPE)) {
    try {
        # Parse inside the try/catch so a non-numeric HEARTBEAT_FLUSH_SECONDS
        # (operator typo, env-var injection bug) falls back to 5 instead of
        # throwing past the heartbeat boundary and tearing down the run.
        $flushSeconds = 5
        $parsed = 0
        if (-not [string]::IsNullOrWhiteSpace($env:HEARTBEAT_FLUSH_SECONDS) -and
            [int]::TryParse($env:HEARTBEAT_FLUSH_SECONDS, [ref]$parsed) -and $parsed -gt 0) {
            $flushSeconds = $parsed
        }
        Start-Heartbeat `
            -RunId             $runId `
            -TenantKey         $tenantKey `
            -ContainerType     $env:CONTAINER_TYPE `
            -StorageAccountUrl $storageAccountUrl `
            -ContainerName     $containerName `
            -WantedEntities    $wantedEntities `
            -UploadFunction    $uploadFunction `
            -FlushSeconds      $flushSeconds
        $heartbeatStarted = $true
    }
    catch {
        Write-Log "Failed to start heartbeat: $($_.Exception.Message)" -Level WARN
    }
}

# --- Authenticate to Azure (managed identity) ---
Write-Log "Connecting to Azure with managed identity"
Connect-AzAccount -Identity -WarningAction SilentlyContinue | Out-Null
Write-Log "Azure authentication successful"

# --- Per-run temp directory for all local files ---
$runTempRoot = Join-Path ([System.IO.Path]::GetTempPath()) "ingest_${runId}"
New-Item -ItemType Directory -Path $runTempRoot -Force | Out-Null

# --- Load certificate + connect to service ---
$certPath = $null
$authContext = $null
try {
    Write-Log "Loading certificate '$certName' from Key Vault"
    $certPath = Get-CertificateFromKeyVault -VaultName $kvName -CertName $certName
    # Read once, base64-encode immediately, drop the byte[] reference.
    # Strings are immutable in .NET, so the encoded form can be passed
    # through layers without the per-layer clone-and-zero choreography the
    # previous byte[] pipeline required. Each Connect.psm1 decodes back to
    # bytes only at the moment of use (see #145 item 3).
    $certBytesLocal = [System.IO.File]::ReadAllBytes($certPath)
    try {
        $certificateBase64 = [Convert]::ToBase64String($certBytesLocal)
    }
    finally {
        [Array]::Clear($certBytesLocal, 0, $certBytesLocal.Length)
        $certBytesLocal = $null
    }

    # Connect-Service is implemented by the container's Connect.psm1. It
    # takes the superset of possible config fields; each container's impl
    # uses the ones it needs (Graph ignores Organization/AdminUrl; EXO uses
    # Organization; SPO uses Organization + AdminUrl). CertificateBase64 is
    # required by every container.
    $connectConfig = @{
        ClientId          = $clientId
        TenantId          = $tenantId
        Organization      = $organization
        AdminUrl          = $adminUrl
        CertificateBase64 = $certificateBase64
    }
    $authContext = Connect-Service -Config $connectConfig
    $orgSuffix = if ($authContext.OrganizationName) { " (org $($authContext.OrganizationName))" } else { '' }
    Write-Log "Connected to upstream service for tenant '$tenantKey'$orgSuffix" -TenantKey $tenantKey

    # --- Discover entity modules ---
    # Each module exports Get-ModuleEntities (one module may own several entities).
    # Build a map from entity-name to its owning module path for grouping below.
    # Also cache each entity's root-entity names so the failure path can write
    # synthetic manifests to the correct co-located landing paths.
    $entitiesPath = Join-Path $PSScriptRoot 'entities'
    $entityToModulePath    = @{}
    $entityToRootEntities  = @{}
    $moduleCount = 0

    Get-ChildItem $entitiesPath -Filter '*.psm1' | ForEach-Object {
        $modFile = $_.FullName
        $mod = Import-Module $modFile -PassThru -Force -DisableNameChecking
        try {
            $moduleEntitiesMap = & "$($mod.Name)\Get-ModuleEntities"
            $moduleStagesMap   = & "$($mod.Name)\Get-ModuleStages"
            $moduleCount++
            foreach ($ename in $moduleEntitiesMap.Keys) {
                if ($entityToModulePath.ContainsKey($ename)) {
                    throw "Entity '$ename' declared by multiple modules: $($entityToModulePath[$ename]) and $modFile"
                }
                $entityToModulePath[$ename] = $modFile
            }
            $rootMap = Resolve-RootEntities -Stages $moduleStagesMap -Entities $moduleEntitiesMap
            foreach ($ename in $rootMap.Keys) {
                $entityToRootEntities[$ename] = $rootMap[$ename]
            }
        }
        finally {
            Remove-Module $mod -Force -ErrorAction SilentlyContinue
        }
    }

    Write-Log "Discovered $($entityToModulePath.Count) entities across $moduleCount modules"

    # --- Filter to requested entities and group by module ---
    $unknown = @($wantedEntities | Where-Object { -not $entityToModulePath.ContainsKey($_) })
    if ($unknown.Count -gt 0) {
        Write-Log "Unknown entities in ENTITY_NAMES (skipping): $($unknown -join ', ')" -Level WARN
    }

    $toRun = @($wantedEntities | Where-Object { $entityToModulePath.ContainsKey($_) })
    if ($toRun.Count -eq 0) {
        # No matching entities is a failure mode — a dispatcher misconfiguration
        # (ENTITY_NAMES listing only ghost entities) or a registry drift (container
        # image lags entity-registry.json). Throw rather than `exit 1` so the
        # outer catch block fires `run_completed status=failed` — otherwise the
        # event stream looks like a hung in-flight run with run_started but no
        # terminal event. ACA still marks the execution Failed because the catch
        # block exits 1.
        Write-Log "No matching entities to process — failing" -Level ERROR
        Write-Log "Requested: $($wantedEntities -join ', ') | Known: $($entityToModulePath.Keys -join ', ')" -Level ERROR
        throw "No matching entities to process. Requested entities not found in registry: $($wantedEntities -join ', ')"
    }

    $entitiesByModule = @{}
    foreach ($ename in $toRun) {
        $modPath = $entityToModulePath[$ename]
        if (-not $entitiesByModule.ContainsKey($modPath)) { $entitiesByModule[$modPath] = @() }
        $entitiesByModule[$modPath] += $ename
    }

    # Sort module paths so two runs with the same registry execute modules in
    # the same order. Hashtable .Keys enumeration is undefined in PowerShell —
    # iterating it directly produced a different module order between runs
    # and made the heaviest module's start time depend on a random toss
    # (#408). Sort by full path; all module files live under one entities/
    # directory so this is equivalent to sorting by file name.
    $orderedModulePaths = @($entitiesByModule.Keys | Sort-Object)
    $orderedModuleNames = ($orderedModulePaths | ForEach-Object { Split-Path -Leaf $_ }) -join ', '
    Write-Log "Will process $($toRun.Count) entities across $($entitiesByModule.Count) modules"
    Write-Log "Module execution order: $orderedModuleNames"

    # --- Process each module ---
    $date = Get-Date -Format 'yyyy-MM-dd'

    # Per-module manifest aggregation state. Reset at the top of each module
    # iteration. Populated by $manifestSink as each entity's contributing
    # stage completes (#345 — per-stage emission means a final manifest
    # lands the moment a stage finishes, even if a later stage in the same
    # module hangs or the container hits replicaTimeout).
    $manifestGroups = @{}

    function Add-ResultToManifestGroups {
        param(
            [Parameter(Mandatory)][hashtable]$Result,
            [Parameter(Mandatory)][hashtable]$Groups
        )
        $key = "$($Result.EntityName)|$($Result.BasePath)"
        if (-not $Groups.ContainsKey($key)) {
            $Groups[$key] = @{
                EntityName   = $Result.EntityName
                BasePath     = $Result.BasePath
                RecordCount  = 0
                ChunkCount   = 0
                SkippedCount = 0
                FailedCount  = 0
                Status       = 'success'
                Errors       = [System.Collections.Generic.List[string]]::new()
            }
        }
        $g = $Groups[$key]
        $g.RecordCount  += [int]$Result.RecordCount
        if ($null -ne $Result.ChunkCount)   { $g.ChunkCount   += [int]$Result.ChunkCount }
        if ($null -ne $Result.SkippedCount) { $g.SkippedCount += [int]$Result.SkippedCount }
        if ($null -ne $Result.FailedCount)  { $g.FailedCount  += [int]$Result.FailedCount }
        $rRank = if ($statusRank.ContainsKey($Result.Status)) { $statusRank[$Result.Status] } else { 3 }
        $gRank = $statusRank[$g.Status]
        if ($rRank -gt $gRank) { $g.Status = $Result.Status }
        foreach ($e in @($Result.Errors)) { if ($e) { $g.Errors.Add($e) } }
        return $g
    }

    function Write-EntityManifest {
        param(
            [Parameter(Mandatory)][hashtable]$Group,
            [Parameter(Mandatory)][string]$StartedAt,
            [Parameter(Mandatory)][string]$CompletedAt
        )
        $manifest = @{
            run_id        = $runId
            tenant_key    = $tenantKey
            tenant_id     = $tenantId
            entity_type   = $Group.EntityName
            record_count  = $Group.RecordCount
            chunk_count   = $Group.ChunkCount
            skipped_count = $Group.SkippedCount
            failed_count  = $Group.FailedCount
            started_at    = $StartedAt
            completed_at  = $CompletedAt
            status        = $Group.Status
            errors        = $Group.Errors.ToArray()
        }
        $manifestJson = $manifest | ConvertTo-Json -Depth 3
        # Include a short hash of BasePath in the local filename so multi-
        # BasePath entities (exo_group_members from DG and UG roots) don't
        # overwrite each other's manifest before upload. Upload path itself
        # is already unique because BasePath differs.
        $basePathHash = [Convert]::ToHexString(
            [System.Security.Cryptography.SHA256]::HashData(
                [Text.Encoding]::UTF8.GetBytes($Group.BasePath))).Substring(0, 8).ToLower()
        $manifestPath = Join-Path $runTempRoot "_manifest_$($Group.EntityName)_${basePathHash}_${runId}.json"
        # UTF-8 without BOM. [Encoding]::UTF8 prepends one, which breaks strict
        # JSON parsers (Python stdlib, some Spark readers) at line 1 col 1.
        [System.IO.File]::WriteAllText($manifestPath, $manifestJson, [System.Text.UTF8Encoding]::new($false))

        $manifestBlobPath = "$($Group.BasePath)/_manifest_$($Group.EntityName)_${runId}.json"
        & $uploadFunction `
            -StorageAccountUrl $storageAccountUrl `
            -ContainerName $containerName `
            -BlobPath $manifestBlobPath `
            -LocalFile $manifestPath
    }

    # $uploadSink fires per chunk inside StageExecutor.Invoke-ModuleRun
    # immediately after each stage's result row is appended to $results.
    # Pre-#345 behavior: no chunks reached ADLS until the whole module
    # returned, so any hang or replicaTimeout mid-module lost every
    # completed stage's data. With the sink, worst-case loss is the single
    # in-flight stage's chunks.
    $uploadSink = {
        param($LocalPath, $BlobPath, $Entity)
        if (-not (Test-Path $LocalPath)) {
            Write-Log "Skipping upload of missing file $BlobPath" -Level WARN -Entity $Entity
            return
        }
        if ((Get-Item $LocalPath).Length -eq 0) {
            Write-Log "Skipping upload of empty file $BlobPath (0 bytes)" -Level WARN -Entity $Entity
            return
        }
        Write-Log "Uploading $BlobPath" -Entity $Entity
        & $uploadFunction `
            -StorageAccountUrl $storageAccountUrl `
            -ContainerName $containerName `
            -BlobPath $BlobPath `
            -LocalFile $LocalPath
        # Delete the local chunk after a successful upload to bound disk
        # pressure across long-running modules (site_permissions can pile
        # multi-GB across 6 sibling entities). Best-effort: a leftover file
        # only causes Test-Path skip on retry — ADLS flush is atomic, so
        # by the time we get here the chunk is durable.
        Remove-Item $LocalPath -Force -ErrorAction SilentlyContinue
    }

    # $manifestSink fires per entity-result row inside Invoke-ModuleRun,
    # just after $uploadSink for that row's chunks finishes. Each call
    # writes a final manifest for one (EntityName, BasePath). Safe because
    # no two stages in the current codebase contribute to the same
    # (EntityName, BasePath) group: exo_group_members is the only multi-
    # stage entity and its two stages land in different BasePaths.
    $manifestSink = {
        param($r)
        $g = Add-ResultToManifestGroups -Result $r -Groups $script:manifestGroups
        $endTime = Get-Date -Format 'o'
        Write-EntityManifest -Group $g -StartedAt $script:moduleStartTime -CompletedAt $endTime
        Write-Log "Entity '$($g.EntityName)' completed status=$($g.Status) records=$($g.RecordCount)" -Entity $g.EntityName
        $script:runTotalRecords += [int]$r.RecordCount
        if ($g.Status -eq 'failed')  { $script:runHasFailure = $true }
        if ($g.Status -eq 'partial') { $script:runHasPartial = $true }

        # Roll into the run-wide entity summary. Multi-BasePath entities
        # (e.g. exo_group_members across DG and UG roots) collapse here via
        # severity-max so the dispatcher sees one row per entity. New-
        # EntityRollup is the single shape factory shared with the
        # heartbeat module — same fields, same names, same nullability.
        if (-not $summaryEntities.ContainsKey($g.EntityName)) {
            $summaryEntities[$g.EntityName] = New-EntityRollup `
                -Name $g.EntityName `
                -Status $g.Status `
                -RecordCount ([int]$g.RecordCount) `
                -StartedAt $script:moduleStartTime `
                -CompletedAt $endTime `
                -Errors @($g.Errors)
        } else {
            $se = $summaryEntities[$g.EntityName]
            $seRank = $statusRank[$se.status]
            $gRank  = if ($statusRank.ContainsKey($g.Status)) { $statusRank[$g.Status] } else { 3 }
            if ($gRank -gt $seRank) { $se.status = $g.Status }
            $se.record_count += [int]$g.RecordCount
            $se.errors = @($se.errors) + @($g.Errors)
            # Span the timestamps across multiple BasePath landings —
            # earliest start wins, latest end wins.
            if ($null -ne $script:moduleStartTime -and ($null -eq $se.started_at -or $script:moduleStartTime -lt $se.started_at)) {
                $se.started_at = $script:moduleStartTime
            }
            if ($null -ne $endTime -and ($null -eq $se.completed_at -or $endTime -gt $se.completed_at)) {
                $se.completed_at = $endTime
            }
        }

        # Heartbeat: project this entity's terminal rollup into the
        # in-memory state so observers see status flip from running to
        # success/partial/skipped/failed.
        if ($heartbeatStarted) {
            try { Set-EntityCompleted -Entity $g.EntityName -Rollup $summaryEntities[$g.EntityName] } catch { Write-Verbose $_.Exception.Message }
        }
    }

    foreach ($modulePath in $orderedModulePaths) {
        if (-not $global:IngestRunning) {
            Write-Log "Cancellation requested, stopping" -Level WARN
            break
        }

        $moduleRequested = $entitiesByModule[$modulePath]
        $moduleFile = Split-Path -Leaf $modulePath
        Write-Log "Running module $moduleFile entities=$($moduleRequested -join ',')"

        $moduleStartTime = Get-Date -Format 'o'
        $moduleTempRoot  = Join-Path $runTempRoot ($moduleFile -replace '\.psm1$', '')
        New-Item -ItemType Directory -Path $moduleTempRoot -Force | Out-Null

        $ctx = @{
            RunId          = $runId
            TenantKey      = $tenantKey
            Date           = $date
            SourceType     = $sourceType
            SourceKey      = $sourceKey
            PoolSize       = $maxConcurrency
            # AuthConfig carries CertificateBase64 (post-#145). Worker runspaces
            # call Connect-Service on first dispatch with this same hashtable
            # and decode the cert locally — no byte[] clone-chain through the
            # pool plumbing.
            AuthConfig     = $authContext.AuthConfig
            # Worker runspaces import this Connect.psm1 into their ISS so the
            # same Connect-Service / Restore-ServiceConnection live in both
            # the main process and every runspace.
            AuthModulePath = $connectModulePath
            TempRoot       = $moduleTempRoot
            # Container's Connect-Service may return run-wide context that
            # every module needs (powerplat-ingest stashes the BAP env list
            # here so per-env modules don't each re-fetch it). Shallow-clone
            # per module so module A can't mutate the shared dict and affect
            # module B; values inside (record arrays etc.) are immutable in
            # practice. Containers that don't use Extra return $null and we
            # fall through to the empty hashtable.
            Extra          = if ($authContext.Extra) { @{} + $authContext.Extra } else { @{} }
            # ProgressShared (#314) — threaded through to StageExecutor and
            # WorkerPool so worker runspaces can stream per-slice
            # records_so_far snapshots into the heartbeat aggregator.
            # $null when the heartbeat isn't running.
            ProgressShared = if ($heartbeatStarted) { Get-ProgressShared } else { $null }
            # Per-stage upload + manifest sinks (#345). StageExecutor fires
            # these from inside Invoke-ModuleRun the instant each stage's
            # result row is finalized, instead of waiting for the whole
            # module to return. $null is tolerated (test contexts that
            # don't supply them) — StageExecutor.$emitResult short-circuits.
            UploadSink     = $uploadSink
            ManifestSink   = $manifestSink
        }

        # Reset the per-module manifest aggregator. Synthesized failed
        # results from the catch block are routed through $manifestSink
        # below, so this hashtable is the single source of truth for
        # what's been written this module.
        $manifestGroups = @{}

        $moduleResults = @()
        try {
            $moduleResults = Invoke-ModuleRun -ModulePath $modulePath -RequestedEntities $moduleRequested -Context $ctx
        }
        catch {
            $errMsg = $_.Exception.Message
            Write-Log "Module $moduleFile failed: $errMsg" -Level ERROR
            [Console]::Error.WriteLine("MODULE_ERROR [$moduleFile]: $errMsg")
            [Console]::Error.WriteLine("MODULE_ERROR [$moduleFile]: $($_.ScriptStackTrace)")
            # Synthetic failed results so every requested entity gets a manifest.
            # Use the entity->rootEntities mapping cached during discovery so
            # manifests land under the co-located parent folder (e.g.
            # entra_group_members under entra_groups/), not the entity name.
            # Multi-stage entities (exo_group_members across DG+UG) get one
            # synthetic result per root, matching the live-run manifest layout.
            #
            # Skip-if-emitted: with per-stage emission (#345) Invoke-ModuleRun
            # can route some result rows through $manifestSink BEFORE
            # throwing — successful stages have already uploaded chunks and
            # written their final manifests. The catch handler must NOT
            # re-emit synthetic failed results for those (entity, BasePath)
            # keys, or it would (a) overwrite the success manifest at the
            # same blob path with a failed-status one, and (b) double-count
            # the entity's record_count in the run-wide summary rollup
            # (since $manifestSink folds the group's cumulative RecordCount
            # into $summaryEntities each time it fires).
            $moduleResults = [System.Collections.Generic.List[hashtable]]::new()
            foreach ($ename in $moduleRequested) {
                $roots = if ($entityToRootEntities.ContainsKey($ename) -and $entityToRootEntities[$ename].Count -gt 0) {
                    $entityToRootEntities[$ename]
                } else {
                    # Fallback for entities whose stage graph couldn't be
                    # resolved at discovery (shouldn't happen in practice since
                    # discovery would have thrown earlier).
                    @($ename)
                }
                foreach ($root in $roots) {
                    $basePath = "$root/$tenantKey/$date"
                    $key = "$ename|$basePath"
                    if ($manifestGroups.ContainsKey($key)) {
                        Write-Log "Module failure: '$ename' at '$basePath' already emitted before the throw; preserving existing manifest" -Level WARN -Entity $ename
                        continue
                    }
                    $failedResult = @{
                        EntityName  = $ename
                        StageName   = $null
                        Status      = 'failed'
                        RecordCount = 0
                        ChunkCount  = 0
                        LocalPaths  = @()
                        BlobPaths   = @()
                        BasePath    = $basePath
                        Errors      = @($errMsg)
                    }
                    $moduleResults.Add($failedResult)
                    # Reset EventEmitter's scope to the synthetic entity
                    # before the sink fires. Without this, any
                    # chunk_upload_retry events triggered by the manifest
                    # upload below would inherit the scope of whichever
                    # stage was running before the throw — mis-attributing
                    # retries to an unrelated entity in LAW.
                    Set-EventScope -Stage '' -Entity $ename
                    # Route the synthetic failed result through the manifest
                    # sink so every entity that had NOT already emitted
                    # still gets a manifest. Upload sink is a no-op for
                    # empty LocalPaths.
                    & $manifestSink $failedResult
                }
            }
        }
    }

    $runStatus = if ($runHasFailure) { 'failed' } elseif ($runHasPartial) { 'partial' } else { 'success' }

    # Per-task dispatcher signal lives on the run-state blob now (#385).
    # ProgressHeartbeat.psm1 owns the upload (Stop-Heartbeat does the
    # terminal flush with bounded retry); the dispatcher reads run_status
    # + per-entity rollups from there. The old _dispatcher/manifests/...
    # writer was removed in #385.

    $runDurationMs = [int]((Get-Date) - $runStartTime).TotalMilliseconds
    Write-Log "Ingestion run=$runId completed"
    # Heartbeat: stop the timer before run_completed so no late tick fires
    # after the manifest write completes (matches the design's "stop the
    # aggregator timer before the final manifest write" guidance).
    if ($heartbeatStarted) {
        try { Set-RunCompleted -Status $runStatus } catch { Write-Verbose $_.Exception.Message }
        try { Stop-Heartbeat -FinalStatus $runStatus } catch { Write-Verbose $_.Exception.Message }
        $heartbeatStarted = $false
    }
    Write-Event -EventType run_completed -Properties @{
        status        = $runStatus
        total_records = $runTotalRecords
        duration_ms   = $runDurationMs
    }
}
catch {
    Write-Log "FATAL: $($_.Exception.Message)" -Level ERROR
    Write-Log "Stack trace: $($_.ScriptStackTrace)" -Level ERROR
    $runDurationMs = [int]((Get-Date) - $runStartTime).TotalMilliseconds
    if ($heartbeatStarted) {
        try { Set-RunError -Message $_.Exception.Message } catch { Write-Verbose $_.Exception.Message }
        try { Set-RunCompleted -Status 'failed' } catch { Write-Verbose $_.Exception.Message }
        try { Stop-Heartbeat -FinalStatus 'failed' } catch { Write-Verbose $_.Exception.Message }
        $heartbeatStarted = $false
    }
    Write-Event -EventType run_completed -Properties @{
        status        = 'failed'
        total_records = $runTotalRecords
        duration_ms   = $runDurationMs
        error_class   = $_.Exception.GetType().FullName
        error_message = $_.Exception.Message
    }
    exit 1
}
finally {
    # Defensive: if neither try nor catch fired Stop-Heartbeat (PS shutdown
    # mid-stream), tear it down here. Idempotent — Stop-Heartbeat is a no-op
    # when state is null.
    if ($heartbeatStarted) {
        try { Stop-Heartbeat -FinalStatus 'failed' } catch { Write-Verbose $_.Exception.Message }
    }
    if ($certPath) { Remove-CertificateFile -Path $certPath }
    # Post-#145: no $global:IngestCertBytes / $global:IngestAuthConfig to
    # clear. Auth state lives in $script: scope inside each Connect.psm1
    # (runspace-local for workers, main-process-local in the driver) and
    # is reclaimed when the runspace pool / process tears down. The base64
    # cert string is immutable so cannot be zeroed; defense-in-depth is the
    # tightened scope rather than zero-on-clear.
    if (Test-Path $runTempRoot) {
        Remove-Item $runTempRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}
