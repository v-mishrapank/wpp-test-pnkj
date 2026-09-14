function New-WorkerPool {
    param(
        [Parameter(Mandatory)][string]$ModuleName,
        [Parameter(Mandatory)][int]$PoolSize,
        [string[]]$AdditionalModules = @()
    )

    $iss = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
    $iss.ImportPSModule($ModuleName)
    $iss.ImportPSModule((Join-Path $PSScriptRoot 'RecordEnvelope.psm1'))
    $iss.ImportPSModule((Join-Path $PSScriptRoot 'RetryHelper.psm1'))
    $iss.ImportPSModule((Join-Path $PSScriptRoot 'LogHelper.psm1'))
    # EventEmitter must be in scope inside the runspace for stage_progress
    # and throttle_event emissions; LogHelper is a transitive dep (Write-Event
    # calls Write-Log).
    $iss.ImportPSModule((Join-Path $PSScriptRoot 'EventEmitter.psm1'))
    # MsalTokenHelper is needed by container Connect.psm1 implementations
    # that call Get-IngestAccessToken (powerplat-ingest does; graph/exo/spo
    # bundle their own MSAL flows). Importing here is a no-op for containers
    # that don't call it — the function just enters runspace scope.
    $iss.ImportPSModule((Join-Path $PSScriptRoot 'MsalTokenHelper.psm1'))
    foreach ($modPath in $AdditionalModules) {
        $iss.ImportPSModule($modPath)
    }

    $pool = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspacePool(
        $PoolSize, $PoolSize, $iss, (Get-Host)
    )
    $pool.Open()

    return $pool
}

function Split-WorkItems {
    param(
        [Parameter(Mandatory)][string[]]$Items,
        [Parameter(Mandatory)][int]$SliceCount
    )

    $slices = @()
    for ($i = 0; $i -lt $SliceCount; $i++) {
        $slices += , [System.Collections.Generic.List[string]]::new()
    }

    for ($i = 0; $i -lt $Items.Count; $i++) {
        $sliceIndex = $i % $SliceCount
        $slices[$sliceIndex].Add($Items[$i])
    }

    return $slices
}

function Get-PoolErrorInfo {
    # Extract structured diagnostic info from a runspace ErrorRecord captured
    # in $ps.Streams.Error. Mirrors the walk-to-innermost pattern used by
    # Get-ErrorClassification in RetryHelper.psm1, but doesn't classify —
    # the aggregator paths in Invoke-StagePool / Invoke-StagePoolBatch
    # already discarded everything except the message. This restores the
    # type info so chunk_failed events (see EventEmitter.psm1, issue #342)
    # carry the actual exception class — without it, hunting #342's PnP
    # "Tenant null" throw site required reading per-tenant manifests in
    # ADLS instead of grepping LAW.
    #
    # ScriptStackTrace is the PowerShell runspace stack trace (line + scope),
    # capped at 500 chars to match Get-ErrorClassification's response-body
    # truncation (RetryHelper.psm1:73) — keeps events readable in LAW.
    param(
        [Parameter(Mandatory)]$ErrorRecord
    )
    $ex = $ErrorRecord.Exception
    $inner = $ex
    while ($inner.InnerException) { $inner = $inner.InnerException }
    $message = if (-not [string]::IsNullOrWhiteSpace($inner.Message)) { $inner.Message } else { $ex.Message }
    $stack = if ($ErrorRecord.ScriptStackTrace) { [string]$ErrorRecord.ScriptStackTrace } else { '' }
    if ($stack.Length -gt 500) { $stack = $stack.Substring(0, 500) + '…' }
    return @{
        Message            = $message
        ExceptionType      = $ex.GetType().FullName
        InnerExceptionType = $inner.GetType().FullName
        ScriptStackTrace   = $stack
    }
}

# --- Which Microsoft-provided module each API family's pre-auth needs ---
#
# The container's Connect.psm1 owns the actual auth flow. WorkerPool only
# needs to know which Microsoft-supplied module (if any) to import into the
# runspace ISS so that Connect.psm1's call sites resolve. powerplat / powerbi
# / spo are REST-only and have no provider PS module — they point at a
# built-in module purely to satisfy the mandatory ImportPSModule call.
# (spo was 'PnP.PowerShell' until the REST rewrite in #464 dropped PnP from
# the spo-ingest image; the runspace can't ImportPSModule a module that
# isn't installed, so the family is now REST-only-style.)
$script:ModuleNames = @{
    graph     = 'Microsoft.Graph.Authentication'
    exo       = 'ExchangeOnlineManagement'
    spo       = 'Microsoft.PowerShell.Utility'
    powerplat = 'Microsoft.PowerShell.Utility'
    powerbi   = 'Microsoft.PowerShell.Utility'
}

# --- Runspace templates (scriptblocks serialized at dispatch time) ---
#
# !!! READ BEFORE EDITING !!!
#
# Every scriptblock in this file is converted to its text form via .ToString()
# and fed to [PowerShell].AddScript() on a worker runspace. That's how we
# guarantee no closure baggage travels across runspace boundaries: the text
# is re-parsed in the target runspace, and bare variable references resolve
# there, not here.
#
# Consequences for anyone editing these blocks:
#
#   1. Do NOT reference $script: or $global: variables defined in THIS module.
#      They will be `$null` at dispatch time. Use the param(...) block for
#      every input and pass it via .AddArgument() in the dispatch site.
#
#   2. Do NOT call helper functions defined in THIS module unless the helper
#      is exported AND the containing module is imported into the runspace
#      ISS via New-WorkerPool. The runspace can only see commands that were
#      loaded into its InitialSessionState.
#
#   3. $PSScriptRoot inside a stringified scriptblock resolves to empty (the
#      text has no source file). Don't use it. If you need a path, pass it
#      via param(...).
#
#   4. Editor/IntelliSense WILL make these blocks look like they have access
#      to outer-module state, because the parser sees them in-context. That
#      convenience is a lie at runtime. Treat the block body as an island.
#
# The payoff: editor, linter, and formatter see real PowerShell instead of
# strings-of-PowerShell. The cost is the discipline above.

# --- Auth model: first-dispatch self-auth ---
#
# Before #145: Invoke-StagePool ran a separate pre-auth BeginInvoke loop that
# tried to seed each runspace with a session by submitting N auth scripts
# into an N-runspace pool, hoping one landed per runspace. A RunspacePool is
# a work queue, not a fan-out — fast-completing auth scripts could free their
# runspace before later auth scripts dispatched, leaving some runspaces
# unauthenticated. Empirically reproducible with sub-10ms auth scripts; in
# production the ~200-500ms cert-auth latency masked the race.
#
# After #145: each runspace authenticates itself on its first dispatch via
# the latch below. $global: inside a runspace IS that runspace's globals,
# and runspace globals persist across BeginInvoke calls on the same
# runspace until the pool disposes — so the first chunk dispatched to a
# runspace runs auth, sets the latch, and every subsequent chunk on that
# runspace skips it. No race possible: the dispatch lands on whichever
# runspace was actually free, and whatever runspace got the work is the
# one that authenticated.
#
# The flag is just a boolean; per-Connect.psm1 auth state (AuthConfig +
# CertificateBase64 used by Restore-ServiceConnection / Get-*Token) lives
# in $script: scope inside Connect.psm1 itself, no longer in $global:.

$script:AuthScriptBlock = {
    param($Config)
    Connect-Service -Config $Config
}

# Universal worker-runspace reconnect scriptblock. Runs inside the dispatch
# template's retry loop when an item is classified Auth. For SPO this is a
# no-op; for Graph and EXO it re-establishes the session.
$script:ReconnectScriptBlock = {
    Restore-ServiceConnection
}

# --- Invoke-StagePool: dispatches a pool stage via a named Get-* function ---
#
# Imports the entity module AND the container's Connect.psm1 into the pool's
# InitialSessionState so both the named Get-* fetcher AND Connect-Service /
# Restore-ServiceConnection are directly callable in every runspace. Each
# runspace constructs a fresh New-StageWriter per item; its .Records become
# envelope-wrapped chunk output (when -WriteRecords), and its .EmittedIds
# aggregate back to the caller for downstream stages.

[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidGlobalVars', '', Justification = "Runspace-local auth-once latch — `$global: inside a runspace writes to that runspace's own globals (see comment block above).")]
$script:StageDispatchBlock = {
    param($InputIds, $OutputDir, $ChunkNum, $RunId, $ApiFamily,
        $FunctionName, $Context, $AuthScriptStr, $ReconnectScriptStr,
        $FlushInterval, $JsonDepth,
        $SourceType, $SourceKey, $WriteRecords,
        $InputTags,
        $AutoFlushThreshold,
        # Telemetry context. Passed in via .AddArgument because runspaces
        # cannot see EventEmitter's $script:EventContext from the outer
        # process (same constraint as the comment block above). The dispatch
        # block reseeds the runspace's own context at top.
        $Tenant, $StageName, $Entity,
        # Synchronized hashtable from ProgressHeartbeat (#314). Cross-runspace
        # channel for per-slice records_so_far. Slot key = "<stageName>/<chunkNum>";
        # value gets overwritten on each FlushInterval boundary. Aggregator
        # timer in the main process folds these into the heartbeat blob.
        # $null when the heartbeat isn't running (manual `docker run`).
        $ProgressShared,
        # Auto-emit safety net (#297). Field name on the WriteRecord-input
        # record from which the writer auto-extracts ID-per-record. $null
        # disables (composite-IdKey stages or stages
        # with EmitIds=$false).
        $AutoEmitIdField)

    # Auth-on-first-dispatch latch. See "Auth model" comment in WorkerPool.psm1.
    # $global:IngestAuthDone is runspace-local: $global: inside a runspace
    # writes to that runspace's own globals, which persist across BeginInvoke
    # calls on the same runspace until the pool disposes. First dispatch
    # auths, every subsequent dispatch on this runspace skips.
    if (-not $global:IngestAuthDone) {
        $authFn = [scriptblock]::Create($AuthScriptStr)
        # $null = suppresses Connect-Service's @{AuthConfig=...; OrganizationName=...}
        # return value from leaking into the dispatch block's pipeline output.
        # Without this, `& $dispatchScript` in the OOP child returns an array of
        # 2 elements (the auth result + the chunk-result hashtable), which the
        # child has to defend against with $output[-1]. See #484 spike probe.
        $null = & $authFn $Context.AuthConfig
        $global:IngestAuthDone = $true
    }

    # Initialize this runspace's EventEmitter context. Runspace-local —
    # every runspace must call this independently. See EventEmitter.psm1.
    Initialize-EventContext -RunId $RunId -Tenant $Tenant
    # Set the stage/entity scope so any RetryHelper.Invoke-WithRetry calls
    # made from inside the fetch function (single-API-call retry path) emit
    # throttle_events attributed to this stage. WorkerPool's pool-level
    # throttle path always passes Stage/Entity explicitly to Write-Throttle-
    # Event, but inline retries inside the fetch don't.
    Set-EventScope -Stage $StageName -Entity $Entity

    # Invoke-StagePool's dispatch loop hands every runspace the SAME $Context
    # hashtable reference via .AddArgument($Context). Runspaces share memory
    # (same process), so mutating $Context.InputTags below would race across
    # sibling chunks running in parallel. Clone once per runspace before
    # entering the per-item loop — within a runspace, items process
    # sequentially, so mutations on the clone are safe. Shallow clone is
    # enough; only top-level .InputTags is written. Nested fields
    # (.AuthConfig etc.) remain shared but read-only.
    $Context = $Context.Clone()

    $MaxRetries = 5
    $reconnectFn = if ($ReconnectScriptStr) { [scriptblock]::Create($ReconnectScriptStr) } else { $null }

    # Lazy chunk writer — a zero-record slice would otherwise leave a
    # 0-byte file that Invoke-Ingestion tries to upload, and ADLS Gen2's
    # PUT ?resource=file rejects PowerShell 7's zero-body request with
    # InvalidHeaderValue on Content-Length. Each write site below creates
    # the StreamWriter on first record; if no record is ever emitted, the
    # chunk file never exists and the landing folder stays clean.
    #
    # The writer is wrapped in a hashtable so an auto-flush callback can
    # mutate the reference (closures capture by value; .Writer mutation
    # propagates because hashtable contents are by-reference).
    $chunkFile = if ($WriteRecords) { Join-Path $OutputDir "chunk-$($ChunkNum.ToString('000'))_${RunId}.jsonl" } else { $null }
    $writerRef = @{ Writer = $null }
    # UTF-8 without BOM (see RecordEnvelope.psm1 for rationale). Keep the
    # encoding at a stable reference so both open sites produce identical files.
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)

    # Auto-flush callback — when a stage declares AutoFlushThreshold>0, the
    # StageWriter calls this every N records during the fetch, streaming
    # them to disk instead of buffering in memory. For high-volume per-env
    # fetches (Dataverse solution_components, etc.), this caps per-item
    # memory at ~N records instead of letting the StageWriter accumulate
    # the entire response set before the post-fetch dump.
    $useAutoFlush = ($WriteRecords -and $AutoFlushThreshold -gt 0)
    $autoFlushCb = if ($useAutoFlush) {
        {
            param($records)
            if ($null -eq $writerRef.Writer) {
                $writerRef.Writer = [System.IO.StreamWriter]::new($chunkFile, $false, $utf8NoBom)
            }
            foreach ($rec in $records) {
                $writerRef.Writer.WriteLine((New-EnvelopedJsonLine -Record $rec -SourceType $SourceType -SourceKey $SourceKey -BatchId $RunId -Depth ($JsonDepth + 1)))
            }
        }.GetNewClosure()
    } else { $null }

    $processed = 0
    # Records count at the most recent flush/progress emit. Used to drive
    # threshold-based emission instead of `$processed % $FlushInterval -eq 0`,
    # which silently skips boundaries when an item's record count straddles
    # a multiple (e.g. 80 -> 110 misses the 100 boundary). With $lastFlushAt
    # we emit on the first iteration where $processed crosses the next
    # threshold, regardless of how lumpy per-item counts are.
    $lastFlushAt = 0
    # Parallel to $lastFlushAt but tracks $itemsProcessed at the last emit.
    # Both the periodic-flush gate AND the final tick read this. The periodic
    # gate fires on EITHER counter crossing $FlushInterval since the last
    # emit (#399): an all-Skippable / all-NonRetryable chunk leaves
    # $processed stuck at 0 forever, so a records-only gate never fires and
    # ProgressShared receives no slot writes for the whole chunk duration
    # (40+ min observed for entra_user_managers, 60+ min for exo_unified_groups
    # ug_root) — the heartbeat fold then short-circuits its upload because
    # LastChangedAt <= LastUploadedAt and the run-state blob freezes.
    # Adding the items-leg fixes that without spamming normal chunks:
    # advancing $lastItemsFlushAt inside the gate means the items leg only
    # fires when the records leg has been silent for an items-interval,
    # which is exactly the all-skip / all-fail shape.
    $lastItemsFlushAt = 0
    # $skipped vs $failed are deliberately distinct (#356). Skippable carries
    # the semantic "expected absence" (404, locked SPO site) — operators
    # shouldn't alert on a steady-state non-zero skipped_count. $failed covers
    # NonRetryable (HTTP 400) and Auth/Unknown-after-MaxRetries — every
    # increment is a genuine fetch failure that warrants attention. Before
    # the split they shared one counter, which is why #341's all-400 stage
    # looked indistinguishable from a stage with legitimate 404s.
    $skipped = 0
    $failed = 0
    # Per-input-item counters, parallel to $processed (which counts records,
    # not items). $itemsProcessed ticks once per foreach itemId iteration
    # when the per-item retry loop reaches any terminal state (success,
    # NonRetryable, Skippable, RetryExhausted, or MaxRetries-exhausted).
    # $itemsFailed and
    # $itemsSkipped mirror $failed/$skipped but at the input-item granularity
    # the operator dashboard needs to compute "% of input chewed through".
    # See #383 — with these we can answer "we're 10% vs 90% through the
    # 25078 input teams" instead of staring at a records_so_far counter
    # whose denominator we never know up front.
    $itemsProcessed = 0
    $itemsFailed = 0
    $itemsSkipped = 0
    $errors = [System.Collections.Generic.List[string]]::new()
    $emittedIds = [System.Collections.Generic.List[hashtable]]::new()

    try {
        foreach ($itemId in $InputIds) {
            $attempt = 0
            $itemDone = $false

            # Attach this item's parent IdTags (if any) into $Context.InputTags
            # so the fetch can read per-input metadata. Reset each iteration —
            # $Context is shared across items in the slice, and we don't want
            # tags from a previous item leaking forward.
            if ($InputTags -and $InputTags.ContainsKey($itemId)) {
                $Context.InputTags = $InputTags[$itemId]
            } else {
                $Context.InputTags = $null
            }

            while (-not $itemDone) {
                $attempt++
                try {
                    $sw = if ($useAutoFlush) {
                        New-StageWriter -SelectFields $Context.ProjectionFields -FlushCallback $autoFlushCb -AutoFlushThreshold $AutoFlushThreshold -AutoEmitIdField $AutoEmitIdField
                    } else {
                        New-StageWriter -SelectFields $Context.ProjectionFields -AutoEmitIdField $AutoEmitIdField
                    }
                    & (Get-Command $FunctionName) -InputId $itemId -Context $Context -Writer $sw
                    # Promote auto-extracted IDs (no-op when fetcher called
                    # EmitId explicitly). Then guard: per-item fetch wrote
                    # records but emitted nothing → IdKey is missing on
                    # every record in this slice (typo, projection drop,
                    # or composite-IdKey fetcher forgot EmitId). Fail
                    # this item rather than silently land record_count=0
                    # downstream. See #297.
                    [void]$sw.PromoteAutoEmittedIds()
                    if ($AutoEmitIdField -and $sw.TotalWritten -gt 0 -and $sw.EmittedIds.Count -eq 0) {
                        $msg = "Per-item fetch for '$itemId' wrote $($sw.TotalWritten) record(s) but emitted zero IDs. AutoEmitIdField='$AutoEmitIdField' resolved no value on any record."
                        throw [System.Net.Http.HttpRequestException]::new($msg, $null, [System.Net.HttpStatusCode]::BadRequest)
                    }

                    if ($WriteRecords) {
                        if ($useAutoFlush) {
                            # Drain the residual records (last partial batch
                            # under the threshold) through the same callback.
                            # $sw.TotalWritten is fresh per item (writer is
                            # constructed inside this while loop), so it gives
                            # the per-item count.
                            $sw.Flush()
                            $processed += $sw.TotalWritten
                        }
                        elseif ($sw.Records.Count -gt 0) {
                            if ($null -eq $writerRef.Writer) {
                                $writerRef.Writer = [System.IO.StreamWriter]::new($chunkFile, $false, $utf8NoBom)
                            }
                            foreach ($rec in $sw.Records) {
                                $writerRef.Writer.WriteLine((New-EnvelopedJsonLine -Record $rec -SourceType $SourceType -SourceKey $SourceKey -BatchId $RunId -Depth ($JsonDepth + 1)))
                                $processed++
                            }
                        }
                    }
                    else {
                        # Stage is running as a #362 prereq — no requested
                        # entity claims it, so we don't materialize records to
                        # disk. But $processed still has to tick, or the
                        # "Periodic flush" check below never fires,
                        # ProgressShared never gets a slot, and the heartbeat
                        # aggregator shows records_so_far=0 for the whole run
                        # (the #373 symptom — a prereq stage stuck at 0 for
                        # 30 min while processing 25k inputs).
                        #
                        # Why EmittedIds.Count and not TotalWritten: production
                        # fetchers gate $Writer.WriteRecord on $Context.WriteRecords
                        # (see e.g. Get-TeamChannels, Get-TeamsRoot), so when
                        # WriteRecords=$false the WriteRecord call is skipped
                        # and TotalWritten stays 0. EmitId is always called —
                        # a prereq's whole purpose is to emit IDs for
                        # descendants — so EmittedIds.Count is the right
                        # per-item work count.
                        $processed += $sw.EmittedIds.Count
                    }

                    foreach ($emitted in $sw.EmittedIds) {
                        $emittedIds.Add($emitted)
                    }

                    $itemDone = $true
                }
                catch {
                    $class = Get-ErrorClassification -ErrorRecord $_ -ApiFamily $ApiFamily

                    if ($class.Category -eq 'NonRetryable') {
                        # 400 Bad Request — client-side malformed query or
                        # body — won't get better with retries. Record the
                        # error and move on to the next item rather than
                        # burning the chunk on a malformed query.
                        $errors.Add("item=$itemId NonRetryable: $($class.Message)")
                        $failed++
                        $itemsFailed++
                        # #356: emit one structured event per silent terminal
                        # failure. Without this, an entity returning 400 on
                        # every call leaves no LAW signal beyond the
                        # aggregated error_count on stage_completed — which
                        # is exactly how #341 stayed hidden for months. Best-
                        # effort: a downstream emitter failure must not mask
                        # the original error or fail the chunk.
                        try {
                            Write-ItemFailedEvent -Stage $StageName -Entity $Entity `
                                -Category NonRetryable -ItemId $itemId -Attempt $attempt `
                                -StatusCode ([int]($class.StatusCode ?? 0)) `
                                -ExceptionType $_.Exception.GetType().FullName `
                                -Message $class.Message
                        } catch { Write-Verbose $_.Exception.Message }
                        $itemDone = $true
                        continue
                    }

                    if ($class.Category -eq 'RetryExhausted') {
                        # The fetch's inner Invoke-WithRetry already spent its
                        # full retry budget on this error (#526). Re-retrying
                        # here would multiply the two budgets (5 × 5 ≈ 30 HTTP
                        # attempts, ~40 min observed on one persistently-500ing
                        # flow_metadata item) — fail the item on the first
                        # outer attempt instead. Fetchers without an inner
                        # Invoke-WithRetry never carry the marker, so they
                        # keep the Unknown/Auth retry paths below.
                        $errors.Add("item=$itemId RetryExhausted: $($class.Message)")
                        $failed++
                        $itemsFailed++
                        # Innermost exception type, not the wrapper's: the
                        # RETRY_EXHAUSTED wrapper is always InvalidOperation-
                        # Exception, so recording it would carry zero signal —
                        # walk to the original error's type for the event.
                        $exhaustedType = $_.Exception.GetType().FullName
                        $cur = $_.Exception.InnerException
                        while ($cur) { $exhaustedType = $cur.GetType().FullName; $cur = $cur.InnerException }
                        try {
                            Write-ItemFailedEvent -Stage $StageName -Entity $Entity `
                                -Category RetryExhausted -ItemId $itemId -Attempt $attempt `
                                -StatusCode ([int]($class.StatusCode ?? 0)) `
                                -ExceptionType $exhaustedType `
                                -Message $class.Message
                        } catch { Write-Verbose $_.Exception.Message }
                        $itemDone = $true
                        continue
                    }

                    if ($class.Category -eq 'Skippable') {
                        $skipped++
                        $itemsSkipped++
                        # #356: emit item_failed for Skippable too. These
                        # often have legitimate cause (404 on a since-deleted
                        # resource, locked SPO site), so they're not surfaced
                        # to the stage-level WARN log — but per-item visibility
                        # still matters when an entity *suddenly* starts hitting
                        # 100% Skippable (auth scope regression, endpoint move).
                        try {
                            Write-ItemFailedEvent -Stage $StageName -Entity $Entity `
                                -Category Skippable -ItemId $itemId -Attempt $attempt `
                                -StatusCode ([int]($class.StatusCode ?? 0)) `
                                -ExceptionType $_.Exception.GetType().FullName `
                                -Message $class.Message
                        } catch { Write-Verbose $_.Exception.Message }
                        $itemDone = $true
                        continue
                    }

                    if ($attempt -ge $MaxRetries) {
                        $errors.Add("item=$itemId attempt=${attempt}: $($class.Message)")
                        $failed++
                        $itemsFailed++
                        # #356: distinguish Auth-exhausted from Unknown-
                        # exhausted in the event payload. The per-attempt
                        # Auth retries themselves stay silent (5x WARN +
                        # event per token refresh would 5x LAW volume); only
                        # the terminal exhaustion is reported.
                        $exhaustCategory = if ($class.Category -eq 'Auth') { 'AuthMaxRetries' } else { 'UnknownMaxRetries' }
                        try {
                            Write-ItemFailedEvent -Stage $StageName -Entity $Entity `
                                -Category $exhaustCategory -ItemId $itemId -Attempt $attempt `
                                -StatusCode ([int]($class.StatusCode ?? 0)) `
                                -ExceptionType $_.Exception.GetType().FullName `
                                -Message $class.Message
                        } catch { Write-Verbose $_.Exception.Message }
                        $itemDone = $true
                        continue
                    }

                    if ($class.Category -eq 'Auth') {
                        if ($reconnectFn) {
                            # $null = suppresses Restore-ServiceConnection's
                            # return (Connect-ExchangeOnline's ConnectionInformation
                            # object) from leaking into the dispatch block's pipeline.
                            # Same rationale as the auth-latch site above.
                            try { $null = & $reconnectFn }
                            catch { Write-Warning "Reconnect failed (attempt $attempt): $($_.Exception.Message)" }
                        }
                        continue
                    }

                    if ($class.Category -eq 'Throttle') {
                        $delay = Get-RetryDelay -Classification $class -Attempt $attempt
                        Write-Log "Throttled on $ApiFamily, backing off ${delay}s (attempt $attempt) item=$itemId : $($class.Message)" -Level WARN -Entity $Entity
                        Write-ThrottleEvent -Stage $StageName -Entity $Entity `
                            -RetryAfterSeconds ([int]$delay) -Attempt $attempt `
                            -StatusCode ([int]($class.StatusCode ?? 0)) -Message $class.Message
                        Start-Sleep -Seconds $delay
                        continue
                    }

                    # Unknown — retry with backoff (many transient server-side errors
                    # from EXO/Graph surface as untyped exceptions whose message text
                    # doesn't match any classification pattern). Previously these
                    # terminated after a single attempt, producing the "1-2 random
                    # items failed per run" pattern in #156 bug #8.
                    # #327: emit WARN + unknown_retry_event so the storm is
                    # observable instead of indistinguishable from a wedge.
                    $delay = Get-RetryDelay -Classification $class -Attempt $attempt
                    $exType = $_.Exception.GetType().FullName
                    $innerType = $exType
                    $cur = $_.Exception.InnerException
                    while ($cur) { $innerType = $cur.GetType().FullName; $cur = $cur.InnerException }
                    Write-Log "Retrying on $ApiFamily after unknown error, backing off ${delay}s (attempt $attempt) item=$itemId : ${exType}: $($class.Message)" -Level WARN -Entity $Entity
                    Write-UnknownRetryEvent -Stage $StageName -Entity $Entity -ApiFamily $ApiFamily `
                        -Attempt $attempt -DelaySeconds ([int]$delay) `
                        -StatusCode ([int]($class.StatusCode ?? 0)) -ExceptionType $exType `
                        -InnerExceptionType $innerType -Message $class.Message
                    Start-Sleep -Seconds $delay
                    continue
                }
            }

            # Item-level tick: every foreach iteration that exits the
            # while loop has reached a terminal state ($itemDone=$true on
            # success, NonRetryable, Skippable, RetryExhausted, or
            # MaxRetries-exhausted), so $itemsProcessed advances exactly
            # once per input item.
            # This counter is what the dashboard divides by input_count to
            # compute true % complete (see #383).
            $itemsProcessed++

            # Periodic flush: fires on the first per-item iteration where
            # EITHER $processed OR $itemsProcessed has crossed at least one
            # FlushInterval threshold since the last flush. Threshold-based
            # (not modulo) so a lumpy 80 -> 110 doesn't silently skip the
            # 100 boundary, and a very lumpy 80 -> 320 still fires once
            # (with $lastFlushAt jumping straight to 320). When an item
            # spans multiple thresholds the in-between boundaries are NOT
            # emitted as separate progress events — synthesizing 100/200/300
            # markers would be misleading since we only observe at item-end.
            # The single emit captures the actual counters at the boundary;
            # the dashboard treats stage_progress as a snapshot, not a
            # fixed-cadence marker.
            # The two legs are OR'd, and BOTH watermarks advance on any
            # emit, so the items leg only fires when the records leg has
            # been silent for an items-interval. Normal mixed-records chunks
            # cross the records boundary first and never trip the items leg
            # (every records-leg fire resets $lastItemsFlushAt to current).
            # The items leg exists for the all-Skippable / all-NonRetryable
            # case where $processed is stuck at 0 forever; without it, the
            # in-chunk heartbeat was silent for the entire chunk duration
            # (#399 — 40+ min on entra_user_managers, 60+ min on ug_root).
            # The final-tick gate in finally still also catches a tail
            # boundary crossed mid-item (#328).
            # Flush the StreamWriter so long-running pool slices don't keep
            # a slice's worth of buffered records at risk if a runspace crashes.
            if ($FlushInterval -gt 0 -and (
                    $processed      -ge ($lastFlushAt      + $FlushInterval) -or
                    $itemsProcessed -ge ($lastItemsFlushAt + $FlushInterval))) {
                if ($writerRef.Writer) {
                    $writerRef.Writer.Flush()
                }

                # Each runspace emits stage_progress when it crosses each
                # FlushInterval boundary. slice_index lets the dashboard
                # aggregate cross-runspace: max(records_so_far) per slice
                # then sum per (run_id, stage). Without it, individual slice
                # snapshots can't be reconstructed into a run total.
                # Plain @{} here — Write-Event's -Properties is typed
                # [hashtable], so an [ordered]@{} would be coerced and lose
                # the ordering anyway. The slot dictionary stays @{} for
                # the same reason: the heartbeat fold reads keys by name,
                # not by enumeration order, and the public field-order
                # contract is the C# HeartbeatEntity record. See #383 PR
                # discussion (Copilot's #384 review comments).
                Write-Event -EventType stage_progress -Stage $StageName -Entity $Entity -Properties @{
                    slice_index     = $ChunkNum
                    items_processed = $itemsProcessed
                    items_failed    = $itemsFailed
                    items_skipped   = $itemsSkipped
                    records_so_far  = $processed
                }
                # Heartbeat (#314): write the same snapshot into the cross-
                # runspace synchronized hashtable. Aggregator timer folds
                # sum-of-max(records_so_far) per slice into the heartbeat
                # blob. Best-effort — failure is non-fatal.
                if ($null -ne $ProgressShared) {
                    try {
                        $ProgressShared["$StageName/$ChunkNum"] = @{
                            stage           = $StageName
                            slice_index     = $ChunkNum
                            items_processed = $itemsProcessed
                            items_failed    = $itemsFailed
                            items_skipped   = $itemsSkipped
                            records_so_far  = $processed
                            updated_at      = [DateTime]::UtcNow
                        }
                    } catch { Write-Verbose $_.Exception.Message }
                }
                $lastFlushAt = $processed
                $lastItemsFlushAt = $itemsProcessed
            }
        }
    }
    finally {
        if ($writerRef.Writer) {
            $writerRef.Writer.Flush()
            $writerRef.Writer.Dispose()
        }
        # Final tick (#328): emit the chunk's actual record count so a tail
        # batch (< FlushInterval records) doesn't leave records_so_far frozen
        # at the prior boundary. Without this, long-tail entities look wedged
        # to /runs/{id} observers for the duration of their tail. Gate on
        # "either counter advanced since the last emit" so we don't
        # duplicate-emit when the final boundary already fired with the same
        # values, but we also catch all-failed slices (#383) where $processed
        # stayed at 0 while $itemsProcessed climbed — without the items leg
        # of this gate, the dashboard would show 0% complete on a 100%-
        # failure stage instead of "100% chewed through, all failed".
        if ($processed -gt $lastFlushAt -or $itemsProcessed -gt $lastItemsFlushAt) {
            # Best-effort — wrap Write-Event so a downstream serialization or
            # log-sink failure doesn't override an in-flight exception from
            # the try body or escape as a new uncaught throw from finally.
            # Mirrors the swallow on the ProgressShared write below.
            try {
                Write-Event -EventType stage_progress -Stage $StageName -Entity $Entity -Properties @{
                    slice_index     = $ChunkNum
                    items_processed = $itemsProcessed
                    items_failed    = $itemsFailed
                    items_skipped   = $itemsSkipped
                    records_so_far  = $processed
                }
            } catch { Write-Verbose $_.Exception.Message }
            if ($null -ne $ProgressShared) {
                try {
                    $ProgressShared["$StageName/$ChunkNum"] = @{
                        stage           = $StageName
                        slice_index     = $ChunkNum
                        items_processed = $itemsProcessed
                        items_failed    = $itemsFailed
                        items_skipped   = $itemsSkipped
                        records_so_far  = $processed
                        updated_at      = [DateTime]::UtcNow
                    }
                } catch { Write-Verbose $_.Exception.Message }
            }
        }
        # NB: don't clear runspace-local auth state here. It's needed by
        # subsequent chunks dispatched onto this runspace (pool reuse, sibling
        # parallelism). State is reclaimed when the pool disposes.
    }

    return @{
        ChunkIndex        = $ChunkNum
        StageName         = $StageName
        Processed         = $processed
        Skipped           = $skipped
        Failed            = $failed
        # Per-input-item totals for this chunk (#383). Always equals the
        # slice's input count when the chunk completed without a fatal
        # runspace error; less when a chunk-level throw aborted the
        # foreach. ItemsFailed + ItemsSkipped <= ItemsProcessed by
        # construction (every failed/skipped item also increments
        # ItemsProcessed via the post-while-loop tick).
        ItemsProcessed    = $itemsProcessed
        ItemsFailed       = $itemsFailed
        ItemsSkipped      = $itemsSkipped
        Errors            = $errors.ToArray()
        EmittedIds        = $emittedIds.ToArray()
    }
}

function Invoke-StagePool {
    param(
        [Parameter(Mandatory)][string]$StageName,
        [Parameter(Mandatory)][string]$ModulePath,
        [Parameter(Mandatory)][string]$FunctionName,
        [Parameter(Mandatory)][string[]]$InputIds,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)][string]$OutputDirectory,
        [Parameter(Mandatory)][string]$RunId,
        [Parameter(Mandatory)][string]$Tenant,
        # Primary entity name for telemetry. Empty/$null is acceptable for
        # stages that only emit IDs (no requested entity).
        [string]$Entity = '',
        [Parameter(Mandatory)][string]$AuthModulePath,
        [Parameter(Mandatory)][string]$SourceType,
        [Parameter(Mandatory)][string]$SourceKey,
        [int]$PoolSize = 10,
        [Parameter(Mandatory)][ValidateSet('graph', 'exo', 'spo', 'powerplat', 'powerbi')][string]$ApiFamily,
        [int]$FlushInterval = 100,
        [int]$JsonDepth = 5,
        [switch]$WriteRecords,
        # Optional id->tags map. When the parent stage emitted IdTags, the
        # dispatch template attaches the per-item tags hashtable to
        # $Context.InputTags before invoking the fetch. Lets workers read
        # parent-emitted metadata without composite-ID parsing. See #257.
        [hashtable]$InputTags,
        # Optional auto-flush threshold for high-volume per-item fetches.
        # When >0, the StageWriter inside the dispatch template flushes
        # every N records to disk instead of buffering all records for an
        # item before the post-fetch dump. Default 0 keeps the original
        # buffer-then-dump behavior (matches the per-item flush boundary
        # documented in StageWriter.psm1). Stages declare this via the
        # AutoFlushThreshold field in Get-ModuleStages.
        [int]$AutoFlushThreshold = 0,
        # Optional pre-built pool. When provided, dispatch into it without
        # creating or disposing — lets a caller (StageExecutor) reuse one
        # pool across multiple Invoke-StagePool calls within a module run,
        # so runspace auth state from the first stage carries into later
        # stages. When $null, this function creates and disposes its own.
        $Pool = $null,
        # Optional cross-runspace progress hashtable from ProgressHeartbeat
        # (#314). Threaded into the dispatch block as a final AddArgument.
        # Workers write per-slice records_so_far snapshots; main-process
        # aggregator timer folds them into the heartbeat blob. $null when
        # the heartbeat isn't running.
        [hashtable]$ProgressShared = $null,
        # Auto-emit safety net (#297). When the parent stage declares
        # EmitIds=$true with single-field IdKey, StageExecutor passes the
        # field name here so per-item writers auto-extract ID-per-record.
        # $null for composite IdKey (fetcher assembles 'a:::b' explicitly)
        # and for stages that don't emit downstream.
        [string]$AutoEmitIdField = $null
    )

    $ownsPool = ($null -eq $Pool)
    $resolvedPool = $Pool

    if ($ownsPool) {
        $moduleName = $script:ModuleNames[$ApiFamily]
        $stageWriterPath = Join-Path $PSScriptRoot 'StageWriter.psm1'
        $resolvedPool = New-WorkerPool -ModuleName $moduleName -PoolSize $PoolSize `
            -AdditionalModules @($stageWriterPath, $AuthModulePath, $ModulePath)
    }

    try {
        # --- Dispatch work chunks ---
        # Cap slice count at InputIds.Count to avoid creating empty chunks
        # (and uploading empty JSONL files) when the input is smaller than
        # the pool size.
        $sliceCount = [math]::Min($InputIds.Count, $PoolSize)
        $slices = Split-WorkItems -Items $InputIds -SliceCount $sliceCount
        $handles = @()

        $authScriptStr      = $script:AuthScriptBlock.ToString()
        $reconnectScriptStr = $script:ReconnectScriptBlock.ToString()
        $dispatchScriptStr  = $script:StageDispatchBlock.ToString()
        for ($chunkIndex = 0; $chunkIndex -lt $slices.Count; $chunkIndex++) {
            $ps = [PowerShell]::Create().AddScript(
                $dispatchScriptStr
            ).AddArgument(
                $slices[$chunkIndex]          # $InputIds
            ).AddArgument(
                $OutputDirectory              # $OutputDir
            ).AddArgument(
                $chunkIndex                   # $ChunkNum
            ).AddArgument(
                $RunId                        # $RunId
            ).AddArgument(
                $ApiFamily                    # $ApiFamily
            ).AddArgument(
                $FunctionName                 # $FunctionName
            ).AddArgument(
                $Context                      # $Context (carries AuthConfig + CertificateBase64)
            ).AddArgument(
                $authScriptStr                # $AuthScriptStr (first-dispatch self-auth)
            ).AddArgument(
                $reconnectScriptStr           # $ReconnectScriptStr
            ).AddArgument(
                $FlushInterval                # $FlushInterval
            ).AddArgument(
                $JsonDepth                    # $JsonDepth
            ).AddArgument(
                $SourceType                   # $SourceType
            ).AddArgument(
                $SourceKey                    # $SourceKey
            ).AddArgument(
                [bool]$WriteRecords           # $WriteRecords
            ).AddArgument(
                $InputTags                    # $InputTags (null when parent emitted no tags)
            ).AddArgument(
                $AutoFlushThreshold           # $AutoFlushThreshold (0 = no auto-flush)
            ).AddArgument(
                $Tenant                       # $Tenant (telemetry — runspace EventContext)
            ).AddArgument(
                $StageName                    # $StageName (telemetry — stage_progress / throttle_event)
            ).AddArgument(
                $Entity                       # $Entity (telemetry — primary entity name)
            ).AddArgument(
                $ProgressShared               # $ProgressShared (#314 cross-runspace channel)
            ).AddArgument(
                $AutoEmitIdField              # $AutoEmitIdField (#297 auto-emit safety net)
            )

            $ps.RunspacePool = $resolvedPool
            $handles += @{ PowerShell = $ps; Handle = $ps.BeginInvoke(); ChunkIndex = $chunkIndex }
        }

        # --- Collect results ---
        $completed = [System.Collections.Generic.HashSet[int]]::new()
        $totalProcessed = 0
        $totalSkipped = 0
        $totalFailed = 0
        $totalItemsProcessed = 0
        $totalItemsFailed = 0
        $totalItemsSkipped = 0
        $allErrors = [System.Collections.Generic.List[string]]::new()
        $allEmittedIds = [System.Collections.Generic.List[hashtable]]::new()

        # Resolve the heartbeat flush command once before entering the wait
        # loop. ProgressHeartbeat is loaded by Invoke-Ingestion at process
        # start; presence won't change mid-stage. Tests skip the import.
        $heartbeatFlush = Get-Command -Name Invoke-HeartbeatFlush -ErrorAction SilentlyContinue

        while ($completed.Count -lt $handles.Count) {
            foreach ($item in $handles) {
                if ($completed.Contains($item.ChunkIndex)) { continue }
                if ($item.Handle.IsCompleted) {
                    try {
                        $output = $item.PowerShell.EndInvoke($item.Handle)

                        if ($item.PowerShell.HadErrors) {
                            foreach ($err in $item.PowerShell.Streams.Error) {
                                $info = Get-PoolErrorInfo -ErrorRecord $err
                                $allErrors.Add("chunk=$($item.ChunkIndex): $($info.Message)")
                                # Surface to LAW alongside the manifest string. Best-effort —
                                # a failed event emit must not mask the original chunk error.
                                try {
                                    Write-ChunkFailedEvent -Stage $StageName -Entity $Entity `
                                        -ChunkIndex $item.ChunkIndex `
                                        -ExceptionType $info.ExceptionType `
                                        -InnerExceptionType $info.InnerExceptionType `
                                        -Message $info.Message `
                                        -ScriptStackTrace $info.ScriptStackTrace
                                } catch { Write-Verbose $_.Exception.Message }
                            }
                        }

                        $result = if ($output -and $output.Count -gt 0) { $output[-1] } else { $null }
                        if ($result) {
                            if ($result.Processed) { $totalProcessed += $result.Processed }
                            if ($result.Skipped)   { $totalSkipped += $result.Skipped }
                            if ($result.Failed)    { $totalFailed += $result.Failed }
                            if ($result.ItemsProcessed) { $totalItemsProcessed += [int]$result.ItemsProcessed }
                            if ($result.ItemsFailed)    { $totalItemsFailed += [int]$result.ItemsFailed }
                            if ($result.ItemsSkipped)   { $totalItemsSkipped += [int]$result.ItemsSkipped }
                            if ($result.Errors -and $result.Errors.Count -gt 0) {
                                $allErrors.AddRange([string[]]$result.Errors)
                            }
                            if ($result.EmittedIds) {
                                foreach ($eid in $result.EmittedIds) {
                                    $allEmittedIds.Add($eid)
                                }
                            }
                        }
                    }
                    catch {
                        $info = Get-PoolErrorInfo -ErrorRecord $_
                        $allErrors.Add("chunk=$($item.ChunkIndex) fatal: $($info.Message)")
                        try {
                            Write-ChunkFailedEvent -Stage $StageName -Entity $Entity `
                                -ChunkIndex $item.ChunkIndex `
                                -ExceptionType $info.ExceptionType `
                                -InnerExceptionType $info.InnerExceptionType `
                                -Message $info.Message `
                                -ScriptStackTrace $info.ScriptStackTrace
                        } catch { Write-Verbose $_.Exception.Message }
                    }
                    finally {
                        $item.PowerShell.Dispose()
                        $completed.Add($item.ChunkIndex) | Out-Null
                    }
                }
            }
            if ($completed.Count -lt $handles.Count) {
                Start-Sleep -Milliseconds 500
                # Cooperative heartbeat flush during pool wait (#318). Folds
                # $script:ProgressShared from worker runspaces into the
                # heartbeat blob; self-throttles to FlushSeconds. Best-effort —
                # cached null when ProgressHeartbeat isn't loaded (tests).
                if ($heartbeatFlush) {
                    try { & $heartbeatFlush } catch { Write-Verbose $_.Exception.Message }
                }
            }
        }

        return @{
            RecordCount         = $totalProcessed
            SliceCount          = $slices.Count
            SkippedCount        = $totalSkipped
            FailedCount         = $totalFailed
            ItemsProcessed      = $totalItemsProcessed
            ItemsFailed         = $totalItemsFailed
            ItemsSkipped        = $totalItemsSkipped
            Errors              = $allErrors.ToArray()
            EmittedIds          = $allEmittedIds.ToArray()
        }
    }
    finally {
        if ($ownsPool) {
            $resolvedPool.Close()
            $resolvedPool.Dispose()
        }
    }
}

# --- Invoke-StagePoolBatch: dispatch sibling pool stages concurrently ---
#
# When two or more pool stages share the same parent + ApiFamily + ModulePath
# (e.g. team_settings + team_installed_apps under teams_root, or
# the 19 Dataverse data entities under dataverse_onboardings), they can fan
# out into one shared runspace pool concurrently instead of opening 6 pools
# in sequence and re-paying the per-pool runspace + auth setup cost each
# time.
#
# Each $StageUnits entry is a hashtable with the same single-stage params
# Invoke-StagePool accepts (StageName, FunctionName, InputIds, OutputDirectory,
# Entity, JsonDepth,
# AutoFlushThreshold, InputTags, WriteRecords). Pool/auth/Context are shared
# across all units. The function returns a hashtable keyed by stage name,
# each value the same shape Invoke-StagePool returns.
function Invoke-StagePoolBatch {
    param(
        [Parameter(Mandatory)][hashtable[]]$StageUnits,
        [Parameter(Mandatory)][string]$ModulePath,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)][string]$RunId,
        [Parameter(Mandatory)][string]$Tenant,
        [Parameter(Mandatory)][string]$AuthModulePath,
        [Parameter(Mandatory)][string]$SourceType,
        [Parameter(Mandatory)][string]$SourceKey,
        [int]$PoolSize = 10,
        [Parameter(Mandatory)][ValidateSet('graph', 'exo', 'spo', 'powerplat', 'powerbi')][string]$ApiFamily,
        [int]$FlushInterval = 100,
        $Pool = $null,
        # Optional cross-runspace progress hashtable from ProgressHeartbeat (#314).
        [hashtable]$ProgressShared = $null
    )

    $ownsPool = ($null -eq $Pool)
    $resolvedPool = $Pool

    if ($ownsPool) {
        $moduleName = $script:ModuleNames[$ApiFamily]
        $stageWriterPath = Join-Path $PSScriptRoot 'StageWriter.psm1'
        $resolvedPool = New-WorkerPool -ModuleName $moduleName -PoolSize $PoolSize `
            -AdditionalModules @($stageWriterPath, $AuthModulePath, $ModulePath)
    }

    try {
        $authScriptStr      = $script:AuthScriptBlock.ToString()
        $reconnectScriptStr = $script:ReconnectScriptBlock.ToString()
        $dispatchScriptStr  = $script:StageDispatchBlock.ToString()

        # Per-unit slice plan: each stage's InputIds split across PoolSize
        # workers. Slice indexes within a unit are 0-based per stage so each
        # unit's chunk files land at chunk-000_<run>.jsonl, chunk-001_..., etc.
        # without colliding with other sibling units.
        $unitPlans = @()
        foreach ($unit in $StageUnits) {
            $sliceCount = [math]::Min($unit.InputIds.Count, $PoolSize)
            if ($sliceCount -le 0) {
                # Empty input — skip dispatch but produce a zero result so
                # callers get a complete keyed map.
                $unitPlans += @{
                    Unit       = $unit
                    Slices     = @()
                    SliceCount = 0
                }
                continue
            }
            $slices = Split-WorkItems -Items $unit.InputIds -SliceCount $sliceCount
            $unitPlans += @{
                Unit       = $unit
                Slices     = $slices
                SliceCount = $slices.Count
            }
        }

        # Dispatch every (unit, slice) pair into the shared pool. Each handle
        # tags itself with the unit's StageName so the result aggregator can
        # bucket per stage. Each unit may carry its own Context (with stage-
        # specific ProjectionFields / SelectFields); fall back to the shared
        # batch Context when no per-unit one is provided.
        $handles = @()
        foreach ($plan in $unitPlans) {
            $unit = $plan.Unit
            $unitContext = if ($unit.Context) { $unit.Context } else { $Context }
            for ($chunkIndex = 0; $chunkIndex -lt $plan.SliceCount; $chunkIndex++) {
                $ps = [PowerShell]::Create().AddScript(
                    $dispatchScriptStr
                ).AddArgument(
                    $plan.Slices[$chunkIndex]
                ).AddArgument(
                    $unit.OutputDirectory
                ).AddArgument(
                    $chunkIndex
                ).AddArgument(
                    $RunId
                ).AddArgument(
                    $ApiFamily
                ).AddArgument(
                    $unit.FunctionName
                ).AddArgument(
                    $unitContext
                ).AddArgument(
                    $authScriptStr
                ).AddArgument(
                    $reconnectScriptStr
                ).AddArgument(
                    $FlushInterval
                ).AddArgument(
                    $(if ($null -ne $unit.JsonDepth) { [int]$unit.JsonDepth } else { 5 })
                ).AddArgument(
                    $SourceType
                ).AddArgument(
                    $SourceKey
                ).AddArgument(
                    [bool]$unit.WriteRecords
                ).AddArgument(
                    $unit.InputTags
                ).AddArgument(
                    $(if ($null -ne $unit.AutoFlushThreshold) { [int]$unit.AutoFlushThreshold } else { 0 })
                ).AddArgument(
                    $Tenant
                ).AddArgument(
                    $unit.StageName
                ).AddArgument(
                    $(if ($unit.Entity) { $unit.Entity } else { '' })
                ).AddArgument(
                    $ProgressShared               # $ProgressShared (#314)
                ).AddArgument(
                    $unit.AutoEmitIdField         # $AutoEmitIdField (#297) — $null when composite or not emitting
                )

                $ps.RunspacePool = $resolvedPool
                $handles += @{
                    PowerShell = $ps
                    Handle     = $ps.BeginInvoke()
                    StageName  = $unit.StageName
                    ChunkIndex = $chunkIndex
                }
            }
        }

        # Aggregate per-stage as handles complete. A unit with empty input
        # (SliceCount=0) gets a zero result seeded up front so callers see
        # an entry for every requested unit. Also build a StageName -> Entity
        # lookup so the error paths below can attribute chunk_failed events
        # without a Where-Object scan of $unitPlans per error.
        $perStage = @{}
        $entityByStage = @{}
        foreach ($plan in $unitPlans) {
            $unit = $plan.Unit
            $perStage[$unit.StageName] = @{
                RecordCount         = 0
                SliceCount          = $plan.SliceCount
                SkippedCount        = 0
                FailedCount         = 0
                ItemsProcessed      = 0
                ItemsFailed         = 0
                ItemsSkipped        = 0
                Errors              = [System.Collections.Generic.List[string]]::new()
                EmittedIds          = [System.Collections.Generic.List[hashtable]]::new()
            }
            $entityByStage[$unit.StageName] = if ($unit.Entity) { [string]$unit.Entity } else { '' }
        }

        # Resolve heartbeat flush once before the wait loop (#318).
        $heartbeatFlush = Get-Command -Name Invoke-HeartbeatFlush -ErrorAction SilentlyContinue

        $completed = [System.Collections.Generic.HashSet[int]]::new()
        while ($completed.Count -lt $handles.Count) {
            for ($i = 0; $i -lt $handles.Count; $i++) {
                if ($completed.Contains($i)) { continue }
                $item = $handles[$i]
                if (-not $item.Handle.IsCompleted) { continue }
                $bucket = $perStage[$item.StageName]
                try {
                    $output = $item.PowerShell.EndInvoke($item.Handle)

                    if ($item.PowerShell.HadErrors) {
                        $bucketEntity = $entityByStage[$item.StageName]
                        foreach ($err in $item.PowerShell.Streams.Error) {
                            $info = Get-PoolErrorInfo -ErrorRecord $err
                            $bucket.Errors.Add("chunk=$($item.ChunkIndex): $($info.Message)")
                            try {
                                Write-ChunkFailedEvent -Stage $item.StageName -Entity $bucketEntity `
                                    -ChunkIndex $item.ChunkIndex `
                                    -ExceptionType $info.ExceptionType `
                                    -InnerExceptionType $info.InnerExceptionType `
                                    -Message $info.Message `
                                    -ScriptStackTrace $info.ScriptStackTrace
                            } catch { Write-Verbose $_.Exception.Message }
                        }
                    }

                    $result = if ($output -and $output.Count -gt 0) { $output[-1] } else { $null }
                    if ($result) {
                        if ($result.Processed) { $bucket.RecordCount += $result.Processed }
                        if ($result.Skipped)   { $bucket.SkippedCount += $result.Skipped }
                        if ($result.Failed)    { $bucket.FailedCount += $result.Failed }
                        if ($result.ItemsProcessed) { $bucket.ItemsProcessed += [int]$result.ItemsProcessed }
                        if ($result.ItemsFailed)    { $bucket.ItemsFailed += [int]$result.ItemsFailed }
                        if ($result.ItemsSkipped)   { $bucket.ItemsSkipped += [int]$result.ItemsSkipped }
                        if ($result.Errors -and $result.Errors.Count -gt 0) {
                            $bucket.Errors.AddRange([string[]]$result.Errors)
                        }
                        if ($result.EmittedIds) {
                            foreach ($eid in $result.EmittedIds) {
                                $bucket.EmittedIds.Add($eid)
                            }
                        }
                    }
                }
                catch {
                    $info = Get-PoolErrorInfo -ErrorRecord $_
                    $bucket.Errors.Add("chunk=$($item.ChunkIndex) fatal: $($info.Message)")
                    $bucketEntity = $entityByStage[$item.StageName]
                    try {
                        Write-ChunkFailedEvent -Stage $item.StageName -Entity $bucketEntity `
                            -ChunkIndex $item.ChunkIndex `
                            -ExceptionType $info.ExceptionType `
                            -InnerExceptionType $info.InnerExceptionType `
                            -Message $info.Message `
                            -ScriptStackTrace $info.ScriptStackTrace
                    } catch { Write-Verbose $_.Exception.Message }
                }
                finally {
                    $item.PowerShell.Dispose()
                    $completed.Add($i) | Out-Null
                }
            }
            if ($completed.Count -lt $handles.Count) {
                Start-Sleep -Milliseconds 500
                # Cooperative heartbeat flush during pool wait (#318). Folds
                # $script:ProgressShared from worker runspaces into the
                # heartbeat blob; self-throttles to FlushSeconds. Best-effort —
                # cached null when ProgressHeartbeat isn't loaded (tests).
                if ($heartbeatFlush) {
                    try { & $heartbeatFlush } catch { Write-Verbose $_.Exception.Message }
                }
            }
        }

        # Convert each bucket's mutable accumulators to the shape Invoke-StagePool
        # returns so callers can treat single-stage and batch results uniformly.
        $resultsByStage = @{}
        foreach ($name in $perStage.Keys) {
            $b = $perStage[$name]
            $resultsByStage[$name] = @{
                RecordCount         = $b.RecordCount
                SliceCount          = $b.SliceCount
                SkippedCount        = $b.SkippedCount
                FailedCount         = $b.FailedCount
                ItemsProcessed      = $b.ItemsProcessed
                ItemsFailed         = $b.ItemsFailed
                ItemsSkipped        = $b.ItemsSkipped
                Errors              = $b.Errors.ToArray()
                EmittedIds          = $b.EmittedIds.ToArray()
            }
        }
        return $resultsByStage
    }
    finally {
        if ($ownsPool) {
            $resolvedPool.Close()
            $resolvedPool.Dispose()
        }
    }
}

# Convenience wrapper used by StageExecutor for pool reuse — opens a pool
# wired with the same module imports Invoke-StagePool / Invoke-StagePoolBatch
# would create internally, so a caller can hand the pool back into either of
# those functions across multiple stages without re-paying setup cost.
function New-StagePool {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateSet('graph', 'exo', 'spo', 'powerplat', 'powerbi')][string]$ApiFamily,
        [Parameter(Mandatory)][string]$ModulePath,
        [Parameter(Mandatory)][string]$AuthModulePath,
        [int]$PoolSize = 10
    )

    $moduleName = $script:ModuleNames[$ApiFamily]
    $stageWriterPath = Join-Path $PSScriptRoot 'StageWriter.psm1'
    return New-WorkerPool -ModuleName $moduleName -PoolSize $PoolSize `
        -AdditionalModules @($stageWriterPath, $AuthModulePath, $ModulePath)
}

# =============================================================================
# Out-of-process worker pool (OOP) — #484
# =============================================================================
#
# WHEN THIS RUNS
# StageExecutor routes each pool stage's dispatch through Invoke-StagePool /
# Invoke-StagePoolBatch by default. When the container sets
# WORKER_POOL_MODE=outofprocess as an env var, routing flips to
# Invoke-StagePoolOutOfProcess / Invoke-StagePoolBatchOutOfProcess instead.
# Set by exo-ingest's Dockerfile; other containers leave it unset and keep
# the in-process RunspacePool path.
#
# WHY IT EXISTS
# The ExchangeOnlineManagement V3 module accumulates per-call state (cmdlet
# cache, MSAL token cache, tmpEXO_* temp files, REST connection retain)
# that Disconnect-ExchangeOnline does not reliably release in the same
# process. On a long ingest (e.g. 14h on a 100K-mailbox tenant) the leak
# climbs ~300 MB/h and OOMs the 4 GiB cgroup. Microsoft's only documented
# reclaim mechanism is to "close the PowerShell process" — so OOP isolates
# each chunk's EXO work into a fresh pwsh child that exits when the chunk
# completes. Process exit triggers OS-level memory reclaim and the leak
# state dies with the child.
#
# MEMORY MODEL
#     peak ≈ orchestrator_parent + PoolSize × (child_baseline + per_chunk_leak)
#
# v6 empirical (madev2, 100K mailboxes, cmdlet allowlist active):
#   - parent (post-inline EXO stages):    ~1.3 GB
#   - child baseline at chunk start:      ~130 MB
#   - per-chunk leak (ChunkSize=1000):    ~30-40 MB by chunk end
#   - PoolSize=5:  ~1.3 + 5 × ~170 MB    ≈ 2.0 GB peak
#   - PoolSize=10: ~1.3 + 10 × ~170 MB   ≈ 3.0 GB peak
#
# CONFIGURATION
#   WORKER_POOL_MODE=outofprocess    — flip routing for the container
#   WORKER_POOL_OOP_MAX=N            — per-container concurrency ceiling
#                                      (default 5). Tenants.json
#                                      MAX_PARALLELISM is the per-customer
#                                      knob; this is the architectural one.
#                                      Effective PoolSize = min of both.
#
# TRADE-OFFS vs in-process RunspacePool
#   + No leak. Memory bounded regardless of tenant size or run duration.
#   + Survives EXO module regressions (we hit GetResponseHeader during the
#     #484 spike) without rewriting WorkerPool.
#   - ~5-10s cold-start per chunk (pwsh boot + module imports + auth).
#     On a 100-chunk run that's ~10 min overhead total — ~3% on a long
#     stage, negligible.
#   - Per-child baseline is per-process not per-runspace, so concurrent
#     workers cost more memory than the equivalent in-process pool. Caps
#     at ~5 children in 4 GiB. Move to a larger ACA workload profile to
#     raise that limit.
#
# IPC
#   Each chunk dispatch writes params-NNN.json (parent → child) and reads
#   result-NNN.json (child → parent) in a per-stage-invocation temp dir.
#   Structured events and per-chunk progress events flow via the child's
#   inherited stdout (Start-Process / ProcessStartInfo with
#   UseShellExecute=false), which lands in container console and from
#   there in LAW exactly the way in-process events do. The heartbeat
#   blob ($ProgressShared) updates only when a chunk completes —
#   sub-chunk granularity is in the LAW stream, not the heartbeat.
#
# LIVENESS
#   Children are reaped via Test-Path /proc/$pid (POSIX-canonical) rather
#   than Process.HasExited — .NET's HasExited on Linux can fire prematurely
#   due to the runtime's SIGCHLD handler consuming exit notifications
#   before the Process object observes them, which caused PoolSize=5 to
#   silently spawn 10+ children during the #484 spike.
# =============================================================================

# --- Invoke-StagePoolOutOfProcess: single-stage OOP dispatch ---
#
# Signature mirrors Invoke-StagePool plus -ChunkSize. The $Pool parameter
# is accepted but ignored (signature compatibility lets StageExecutor swap
# dispatchers via an env var without changing call-site code).
function Invoke-StagePoolOutOfProcess {
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSReviewUnusedParameter', 'Pool', Justification = "Signature-compat with Invoke-StagePool so StageExecutor can route between dispatchers via env var. The OOP path doesn't reuse pools.")]
    param(
        [Parameter(Mandatory)][string]$StageName,
        [Parameter(Mandatory)][string]$ModulePath,
        [Parameter(Mandatory)][string]$FunctionName,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]]$InputIds,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)][string]$OutputDirectory,
        [Parameter(Mandatory)][string]$RunId,
        [Parameter(Mandatory)][string]$Tenant,
        [string]$Entity = '',
        [Parameter(Mandatory)][string]$AuthModulePath,
        [Parameter(Mandatory)][string]$SourceType,
        [Parameter(Mandatory)][string]$SourceKey,
        [int]$PoolSize = 5,
        [Parameter(Mandatory)][ValidateSet('graph', 'exo', 'spo', 'powerplat', 'powerbi')][string]$ApiFamily,
        [int]$FlushInterval = 100,
        [int]$JsonDepth = 5,
        [switch]$WriteRecords,
        [hashtable]$InputTags,
        [int]$AutoFlushThreshold = 0,
        $Pool = $null,
        [hashtable]$ProgressShared = $null,
        [string]$AutoEmitIdField = $null,
        [int]$ChunkSize = 1000
    )

    # Empty input → zero result, no children. Matches Invoke-StagePool's
    # zero-record behavior.
    if ($InputIds.Count -eq 0) {
        return @{
            RecordCount         = 0
            SliceCount          = 0
            SkippedCount        = 0
            FailedCount         = 0
            ItemsProcessed      = 0
            ItemsFailed         = 0
            ItemsSkipped        = 0
            Errors              = @()
            EmittedIds          = @()
        }
    }

    # Split into fixed-size chunks. Last chunk may be short.
    $chunks = [System.Collections.Generic.List[object]]::new()
    for ($i = 0; $i -lt $InputIds.Count; $i += $ChunkSize) {
        $end = [Math]::Min($i + $ChunkSize - 1, $InputIds.Count - 1)
        $chunks.Add(@($InputIds[$i..$end]))
    }

    # Per-stage-invocation IPC dir. Random suffix so concurrent batches
    # (different stages within the same run) don't collide.
    $workDir = Join-Path ([System.IO.Path]::GetTempPath()) "oopworker-$RunId-$StageName-$([Guid]::NewGuid().ToString('N').Substring(0,8))"
    [void][System.IO.Directory]::CreateDirectory($workDir)

    # Locate Invoke-WorkerChunk.ps1. Two candidates depending on
    # deployment shape:
    #   - In-container (Dockerfile copies it to /app/): /app/Invoke-WorkerChunk.ps1
    #   - Repo / Pester: src/analytics/ingest/jobs/shared/scripts/Invoke-WorkerChunk.ps1
    $workerScript = $null
    $repoCandidate = Join-Path $PSScriptRoot '..' 'scripts' 'Invoke-WorkerChunk.ps1'
    if (Test-Path $repoCandidate) {
        $workerScript = (Resolve-Path $repoCandidate).Path
    } elseif (Test-Path '/app/Invoke-WorkerChunk.ps1') {
        $workerScript = '/app/Invoke-WorkerChunk.ps1'
    } else {
        throw "Invoke-WorkerChunk.ps1 not found at '$repoCandidate' or '/app/Invoke-WorkerChunk.ps1'."
    }

    # SharedModulesPath: where the child Imports LogHelper / EventEmitter /
    # RetryHelper / RecordEnvelope / StageWriter / MsalTokenHelper from.
    # WorkerPool.psm1 lives in that directory.
    $sharedModulesPath = $PSScriptRoot

    # Stringify the auth/reconnect/dispatch scripts once. The dispatch
    # script is the same $script:StageDispatchBlock the in-process path
    # runs in each runspace — we deliberately don't fork the per-item
    # retry/throttle/error logic.
    $authScriptStr      = $script:AuthScriptBlock.ToString()
    $reconnectScriptStr = $script:ReconnectScriptBlock.ToString()
    $dispatchScriptStr  = $script:StageDispatchBlock.ToString()

    # Resolve heartbeat flush once. Loaded by Invoke-Ingestion at process
    # start; presence won't change mid-stage. Same pattern as Invoke-StagePool.
    $heartbeatFlush = Get-Command -Name Invoke-HeartbeatFlush -ErrorAction SilentlyContinue

    # --- Spawn-and-reap loop ---
    # Cap effective concurrency via container env var (default 5). Lets
    # each container family set its own architectural ceiling without
    # code changes — tenants.json MAX_PARALLELISM stays the per-customer
    # tuning knob; this is the per-container "regardless of customer
    # config, this container can't safely run more than N concurrent
    # workers" limit.
    #
    # Memory math (validated by #484 spike v5/v6 at madev2 scale, 100K
    # mailboxes): per-child memory with the #496 cmdlet allowlist is
    # ~130MB at runtime; parent after inline EXO stages is ~1.3GB
    # (holds EXO module + mailbox refs from mailboxes_root). PoolSize=5
    # lands at ~2GB total — comfortable headroom under the 4GB cgroup
    # ceiling on ACA Consumption.
    #
    # Going higher than 5 hits diminishing returns: EXO API latency is
    # the bottleneck, not local parallelism. Parent slim to push past 5
    # isn't viable on the current architecture — inline and pool stages
    # interleave across modules in StageExecutor (mailboxes_root and
    # mail_users_root are inline EXO calls that run after earlier pool
    # batches), so the parent must keep EXO loaded throughout. To raise
    # this cap meaningfully, move the container to an ACA Dedicated
    # workload profile (E4 = 32 GiB, comfortable up to PoolSize=10+).
    # Safe-parse WORKER_POOL_OOP_MAX: non-numeric / 0 / negative would
    # either throw on [int] cast or deadlock the scheduler (no chunk
    # would ever be spawned while plan items remain). Fall back to the
    # default and warn loudly on bad config rather than silently break.
    $containerCap = 5
    if ($env:WORKER_POOL_OOP_MAX) {
        $parsed = 0
        if ([int]::TryParse($env:WORKER_POOL_OOP_MAX, [ref]$parsed) -and $parsed -gt 0) {
            $containerCap = $parsed
        } else {
            Write-Warning "WORKER_POOL_OOP_MAX='$($env:WORKER_POOL_OOP_MAX)' is not a positive integer; falling back to default 5"
        }
    }
    # Math.Max(1, ...) guards against $PoolSize <= 0 from upstream config.
    $effectivePoolSize = [Math]::Max(1, [Math]::Min($PoolSize, $containerCap))
    $inFlight = @{}   # chunkIdx -> @{Process, ParamsFile, ResultFile, StartedAt}
    $completedResults = [System.Collections.Generic.List[hashtable]]::new()
    $nextChunkIdx = 0
    # Track whether any chunk exited non-zero so we know whether to
    # preserve $workDir for post-mortem after the run.
    $anyChunkFailed = $false

    while ($nextChunkIdx -lt $chunks.Count -or $inFlight.Count -gt 0) {
        # Spawn up to effectivePoolSize concurrent children.
        while ($nextChunkIdx -lt $chunks.Count -and $inFlight.Count -lt $effectivePoolSize) {
            $chunkIdx = $nextChunkIdx
            $chunkItems = $chunks[$chunkIdx]

            $childParams = @{
                ChunkNum            = $chunkIdx
                InputIds            = $chunkItems
                OutputDir           = $OutputDirectory
                RunId               = $RunId
                ApiFamily           = $ApiFamily
                FunctionName        = $FunctionName
                Context             = $Context
                AuthScriptStr       = $authScriptStr
                ReconnectScriptStr  = $reconnectScriptStr
                FlushInterval       = $FlushInterval
                JsonDepth           = $JsonDepth
                SourceType          = $SourceType
                SourceKey           = $SourceKey
                WriteRecords        = [bool]$WriteRecords
                InputTags           = $InputTags
                AutoFlushThreshold  = $AutoFlushThreshold
                Tenant              = $Tenant
                StageName           = $StageName
                Entity              = $Entity
                AutoEmitIdField     = $AutoEmitIdField
                ModulePath          = $ModulePath
                AuthModulePath      = $AuthModulePath
                SharedModulesPath   = $sharedModulesPath
                DispatchScriptStr   = $dispatchScriptStr
            }
            $paramsFile = Join-Path $workDir "params-$($chunkIdx.ToString('0000')).json"
            $resultFile = Join-Path $workDir "result-$($chunkIdx.ToString('0000')).json"
            $childParams | ConvertTo-Json -Depth 20 -Compress | Set-Content -Path $paramsFile -Encoding UTF8
            # $Context.AuthConfig carries CertificateBase64 (private key
            # material). Tighten file perms to owner-only on Linux so the
            # secret isn't world-readable for the chunk's lifetime in /tmp.
            # Linux-only because [IO.File]::SetUnixFileMode is a no-op on
            # Windows; Dockerfile + ACA always run Linux containers.
            if ($IsLinux) {
                try { [System.IO.File]::SetUnixFileMode($paramsFile, 'UserRead, UserWrite') } catch { Write-Verbose $_.Exception.Message }
            }

            # ProcessStartInfo + Process.Start directly. Start-Process
            # -PassThru on Linux can return a shell-wrapper Process whose
            # HasExited fires the moment the wrapper exec's pwsh — the real
            # subprocess keeps running but the reap loop treats it as done,
            # spawning more children past PoolSize and ballooning memory
            # well beyond ceiling. Verified on the first madev2 run (10+
            # children at 4 GiB cgroup max). Direct Process.Start gives a
            # handle bound to the actual pwsh PID.
            $psi = [System.Diagnostics.ProcessStartInfo]::new()
            $psi.FileName = 'pwsh'
            [void]$psi.ArgumentList.Add('-NoProfile')
            [void]$psi.ArgumentList.Add('-File')
            [void]$psi.ArgumentList.Add($workerScript)
            [void]$psi.ArgumentList.Add('-ParamsFile')
            [void]$psi.ArgumentList.Add($paramsFile)
            [void]$psi.ArgumentList.Add('-ResultFile')
            [void]$psi.ArgumentList.Add($resultFile)
            $psi.UseShellExecute = $false
            $psi.RedirectStandardInput = $false
            $psi.RedirectStandardOutput = $false
            $psi.RedirectStandardError = $false
            $proc = [System.Diagnostics.Process]::Start($psi)

            $inFlight[$chunkIdx] = @{
                Process    = $proc
                ParamsFile = $paramsFile
                ResultFile = $resultFile
                StartedAt  = [DateTime]::UtcNow
            }
            $nextChunkIdx++
        }

        # Reap completed children. Liveness via Test-Path /proc/$pid —
        # NOT Process.HasExited. On Linux, .NET's global SIGCHLD handler
        # can consume child-exit notifications before our Process object
        # observes them, leaving HasExited stuck or returning true while
        # the child is still alive. Verified against the first OOP madev2
        # run where Start-Process + HasExited reported children as exited
        # immediately and the parent kept spawning past PoolSize until
        # ~10 children were live and we hit the 4 GiB cgroup ceiling. The
        # /proc-based check is the POSIX-canonical "is this PID alive?"
        # and doesn't depend on whether anyone has waitpid'd the child.
        $finishedKeys = @($inFlight.Keys | Where-Object {
            $childPid = $inFlight[$_].Process.Id
            -not (Test-Path "/proc/$childPid")
        })
        foreach ($k in $finishedKeys) {
            $entry = $inFlight[$k]
            $exitCode = $entry.Process.ExitCode
            $result = $null

            if (Test-Path $entry.ResultFile) {
                try {
                    $result = Get-Content -Path $entry.ResultFile -Raw | ConvertFrom-Json -AsHashtable -Depth 20
                } catch { $result = $null }
            }

            if (-not $result) {
                # Child died without a parseable result. Synthesize a
                # failure record so the aggregator below treats it
                # uniformly; emit chunk_failed for LAW.
                $result = @{
                    ChunkIndex        = $k
                    StageName         = $StageName
                    Processed         = 0
                    Skipped           = 0
                    Failed            = 0
                    ItemsProcessed    = 0
                    ItemsFailed       = 0
                    ItemsSkipped      = 0
                    Errors            = @("chunk=$k child exited with code $exitCode without producing a parseable result")
                    EmittedIds        = @()
                }
                try {
                    Write-ChunkFailedEvent -Stage $StageName -Entity $Entity `
                        -ChunkIndex $k -ExceptionType 'System.Diagnostics.Process' `
                        -InnerExceptionType 'System.Diagnostics.Process' `
                        -Message "child exit=$exitCode no result file" `
                        -ScriptStackTrace ''
                } catch { Write-Verbose $_.Exception.Message }
            }

            $completedResults.Add($result)

            # Heartbeat fold (#484 spike, Option 1): update ProgressShared
            # at chunk completion. Sub-chunk progress events still stream
            # to LAW in real time via the child's inherited stdout — this
            # update is only for the heartbeat blob the dashboard reads.
            # Staleness bounded by chunk duration (~3-5 min at ChunkSize=1000).
            if ($null -ne $ProgressShared) {
                try {
                    $ProgressShared["$StageName/$k"] = @{
                        stage           = $StageName
                        slice_index     = $k
                        items_processed = [int]($result.ItemsProcessed)
                        items_failed    = [int]($result.ItemsFailed)
                        items_skipped   = [int]($result.ItemsSkipped)
                        records_so_far  = [int]($result.Processed)
                        updated_at      = [DateTime]::UtcNow
                    }
                } catch { Write-Verbose $_.Exception.Message }
            }

            # Cleanup. Keep the result file on non-zero exit so an
            # operator can post-mortem; cleanup params either way.
            try { Remove-Item -Path $entry.ParamsFile -Force -ErrorAction SilentlyContinue } catch { Write-Verbose $_.Exception.Message }
            if ($exitCode -eq 0) {
                try { Remove-Item -Path $entry.ResultFile -Force -ErrorAction SilentlyContinue } catch { Write-Verbose $_.Exception.Message }
            } else {
                $anyChunkFailed = $true
            }
            $inFlight.Remove($k)
        }

        # Idle-tick: flush heartbeat and brief sleep when nothing reaped.
        if ($finishedKeys.Count -eq 0 -and $inFlight.Count -gt 0) {
            if ($heartbeatFlush) {
                try { & $heartbeatFlush } catch { Write-Verbose $_.Exception.Message }
            }
            Start-Sleep -Milliseconds 500
        }
    }

    # Workdir cleanup: only when ALL chunks exited cleanly. If anything
    # failed, leave the directory for post-mortem — operators need the
    # result files (which contain the synth-error stack traces) that the
    # per-chunk cleanup above intentionally preserved.
    if (-not $anyChunkFailed) {
        try { Remove-Item -Path $workDir -Recurse -Force -ErrorAction SilentlyContinue } catch { Write-Verbose $_.Exception.Message }
    } else {
        Write-Warning "Some chunks in stage '$StageName' exited non-zero; preserving '$workDir' for post-mortem."
    }

    # --- Aggregate ---
    $totalProcessed = 0; $totalSkipped = 0; $totalFailed = 0
    $totalItemsProcessed = 0; $totalItemsFailed = 0; $totalItemsSkipped = 0
    $allErrors = [System.Collections.Generic.List[string]]::new()
    $allEmittedIds = [System.Collections.Generic.List[hashtable]]::new()

    foreach ($r in $completedResults) {
        if ($r.Processed) { $totalProcessed += [int]$r.Processed }
        if ($r.Skipped)   { $totalSkipped   += [int]$r.Skipped }
        if ($r.Failed)    { $totalFailed    += [int]$r.Failed }
        if ($r.ItemsProcessed) { $totalItemsProcessed += [int]$r.ItemsProcessed }
        if ($r.ItemsFailed)    { $totalItemsFailed    += [int]$r.ItemsFailed }
        if ($r.ItemsSkipped)   { $totalItemsSkipped   += [int]$r.ItemsSkipped }
        if ($r.Errors -and $r.Errors.Count -gt 0) {
            $allErrors.AddRange([string[]]$r.Errors)
        }
        if ($r.EmittedIds) {
            foreach ($eid in $r.EmittedIds) {
                if ($eid -is [hashtable]) {
                    $allEmittedIds.Add($eid)
                } else {
                    # ConvertFrom-Json -AsHashtable should give us hashtables,
                    # but defend against PSCustomObject leakage just in case.
                    $h = @{}
                    foreach ($p in $eid.PSObject.Properties) { $h[$p.Name] = $p.Value }
                    $allEmittedIds.Add($h)
                }
            }
        }
    }

    return @{
        RecordCount         = $totalProcessed
        SliceCount          = $chunks.Count
        SkippedCount        = $totalSkipped
        FailedCount         = $totalFailed
        ItemsProcessed      = $totalItemsProcessed
        ItemsFailed         = $totalItemsFailed
        ItemsSkipped        = $totalItemsSkipped
        Errors              = $allErrors.ToArray()
        EmittedIds          = $allEmittedIds.ToArray()
    }
}

# --- Invoke-StagePoolBatchOutOfProcess: concurrent sibling-stage dispatch ---
#
# Mirrors Invoke-StagePoolBatch's outer signature. All units' chunks share
# ONE chunk-level scheduler (same WORKER_POOL_OOP_MAX cap applies across
# all units combined), so two sibling pool stages (e.g. EXO's mailbox_perms
# and mailbox_stats both feeding off mailboxes_root) run truly concurrent
# under a unified concurrency budget — not serial-per-unit.
#
# Per-unit results are routed back via a StageName tag on each in-flight
# tracker. Per-chunk heartbeat updates fold into $ProgressShared keyed by
# "<stageName>/<chunkIdxInUnit>", matching the single-stage shape so the
# heartbeat aggregator doesn't need to know whether a stage ran solo or
# as part of a batch.
function Invoke-StagePoolBatchOutOfProcess {
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSReviewUnusedParameter', 'Pool', Justification = "Signature-compat with Invoke-StagePoolBatch so StageExecutor can route between dispatchers via env var. The OOP path doesn't reuse pools.")]
    param(
        [Parameter(Mandatory)][hashtable[]]$StageUnits,
        [Parameter(Mandatory)][string]$ModulePath,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)][string]$RunId,
        [Parameter(Mandatory)][string]$Tenant,
        [Parameter(Mandatory)][string]$AuthModulePath,
        [Parameter(Mandatory)][string]$SourceType,
        [Parameter(Mandatory)][string]$SourceKey,
        [int]$PoolSize = 5,
        [Parameter(Mandatory)][ValidateSet('graph', 'exo', 'spo', 'powerplat', 'powerbi')][string]$ApiFamily,
        [int]$FlushInterval = 100,
        $Pool = $null,
        [hashtable]$ProgressShared = $null,
        [int]$ChunkSize = 1000
    )

    # Same cap logic as Invoke-StagePoolOutOfProcess — applies across ALL
    # units' chunks combined.
    $containerCap = if ($env:WORKER_POOL_OOP_MAX) {
        [int]$env:WORKER_POOL_OOP_MAX
    } else {
        5
    }
    $effectivePoolSize = [Math]::Min($PoolSize, $containerCap)

    # Per-unit state: keeps the StageUnit hashtable + a list to collect
    # chunk results as they come back from the shared spawn pool.
    $unitState = @{}
    foreach ($unit in $StageUnits) {
        $unitContext = if ($unit.Context) { $unit.Context } else { $Context }
        $unitState[$unit.StageName] = @{
            Unit         = $unit
            UnitContext  = $unitContext
            ChunkResults = [System.Collections.Generic.List[hashtable]]::new()
            TotalChunks  = 0
        }
    }

    # Build unified chunk plan across all units. Each entry tagged with the
    # owning StageName so reaping knows where to deposit the result.
    $chunkPlan = [System.Collections.Generic.List[hashtable]]::new()
    foreach ($unit in $StageUnits) {
        $inputIds = [string[]]$unit.InputIds
        if ($null -eq $inputIds -or $inputIds.Count -eq 0) { continue }
        $chunkIdx = 0
        for ($i = 0; $i -lt $inputIds.Count; $i += $ChunkSize) {
            $end = [Math]::Min($i + $ChunkSize - 1, $inputIds.Count - 1)
            $chunkPlan.Add(@{
                StageName      = $unit.StageName
                ChunkIdxInUnit = $chunkIdx
                ChunkItems     = @($inputIds[$i..$end])
            })
            $chunkIdx++
        }
        $unitState[$unit.StageName].TotalChunks = $chunkIdx
    }

    # If no unit had any input, short-circuit to zero results.
    if ($chunkPlan.Count -eq 0) {
        $resultsByStage = @{}
        foreach ($name in $unitState.Keys) {
            $unit = $unitState[$name].Unit
            $resultsByStage[$name] = @{
                RecordCount         = 0
                SliceCount          = 0
                SkippedCount        = 0; FailedCount = 0
                ItemsProcessed      = 0; ItemsFailed = 0; ItemsSkipped = 0
                Errors              = @()
                EmittedIds          = @()
            }
        }
        return $resultsByStage
    }

    # Per-batch IPC dir.
    $workDir = Join-Path ([System.IO.Path]::GetTempPath()) "oopbatch-$RunId-$([Guid]::NewGuid().ToString('N').Substring(0,8))"
    [void][System.IO.Directory]::CreateDirectory($workDir)

    # Locate the child worker script (same logic as Invoke-StagePoolOutOfProcess).
    $workerScript = $null
    $repoCandidate = Join-Path $PSScriptRoot '..' 'scripts' 'Invoke-WorkerChunk.ps1'
    if (Test-Path $repoCandidate) {
        $workerScript = (Resolve-Path $repoCandidate).Path
    } elseif (Test-Path '/app/Invoke-WorkerChunk.ps1') {
        $workerScript = '/app/Invoke-WorkerChunk.ps1'
    } else {
        throw "Invoke-WorkerChunk.ps1 not found at '$repoCandidate' or '/app/Invoke-WorkerChunk.ps1'."
    }
    $sharedModulesPath = $PSScriptRoot

    $authScriptStr      = $script:AuthScriptBlock.ToString()
    $reconnectScriptStr = $script:ReconnectScriptBlock.ToString()
    $dispatchScriptStr  = $script:StageDispatchBlock.ToString()

    $heartbeatFlush = Get-Command -Name Invoke-HeartbeatFlush -ErrorAction SilentlyContinue

    # Unified spawn-and-reap loop. $inFlight is keyed by globalChunkId
    # (sequential across all units' chunks) so cleanup is trivial.
    $inFlight = @{}
    $nextPlanIdx = 0
    $anyChunkFailed = $false  # preserve $workDir if any chunk fails
    $globalChunkId = 0

    while ($nextPlanIdx -lt $chunkPlan.Count -or $inFlight.Count -gt 0) {
        # Spawn up to effectivePoolSize concurrent children, drawing the
        # next chunk from the unified plan regardless of owning unit.
        while ($nextPlanIdx -lt $chunkPlan.Count -and $inFlight.Count -lt $effectivePoolSize) {
            $work = $chunkPlan[$nextPlanIdx]
            $state = $unitState[$work.StageName]
            $unit = $state.Unit
            $unitContext = $state.UnitContext

            $childParams = @{
                ChunkNum            = $work.ChunkIdxInUnit
                InputIds            = $work.ChunkItems
                OutputDir           = $unit.OutputDirectory
                RunId               = $RunId
                ApiFamily           = $ApiFamily
                FunctionName        = $unit.FunctionName
                Context             = $unitContext
                AuthScriptStr       = $authScriptStr
                ReconnectScriptStr  = $reconnectScriptStr
                FlushInterval       = $FlushInterval
                JsonDepth           = if ($null -ne $unit.JsonDepth) { [int]$unit.JsonDepth } else { 5 }
                SourceType          = $SourceType
                SourceKey           = $SourceKey
                WriteRecords        = [bool]$unit.WriteRecords
                InputTags           = $unit.InputTags
                AutoFlushThreshold  = if ($unit.AutoFlushThreshold) { [int]$unit.AutoFlushThreshold } else { 0 }
                Tenant              = $Tenant
                StageName           = $work.StageName
                Entity              = if ($unit.Entity) { [string]$unit.Entity } else { '' }
                AutoEmitIdField     = $unit.AutoEmitIdField
                ModulePath          = $ModulePath
                AuthModulePath      = $AuthModulePath
                SharedModulesPath   = $sharedModulesPath
                DispatchScriptStr   = $dispatchScriptStr
            }
            $paramsFile = Join-Path $workDir "params-g$($globalChunkId.ToString('0000')).json"
            $resultFile = Join-Path $workDir "result-g$($globalChunkId.ToString('0000')).json"
            $childParams | ConvertTo-Json -Depth 20 -Compress | Set-Content -Path $paramsFile -Encoding UTF8
            # $Context.AuthConfig carries CertificateBase64 (private key
            # material). Tighten file perms to owner-only on Linux so the
            # secret isn't world-readable for the chunk's lifetime in /tmp.
            # Linux-only because [IO.File]::SetUnixFileMode is a no-op on
            # Windows; Dockerfile + ACA always run Linux containers.
            if ($IsLinux) {
                try { [System.IO.File]::SetUnixFileMode($paramsFile, 'UserRead, UserWrite') } catch { Write-Verbose $_.Exception.Message }
            }

            $psi = [System.Diagnostics.ProcessStartInfo]::new()
            $psi.FileName = 'pwsh'
            [void]$psi.ArgumentList.Add('-NoProfile')
            [void]$psi.ArgumentList.Add('-File')
            [void]$psi.ArgumentList.Add($workerScript)
            [void]$psi.ArgumentList.Add('-ParamsFile')
            [void]$psi.ArgumentList.Add($paramsFile)
            [void]$psi.ArgumentList.Add('-ResultFile')
            [void]$psi.ArgumentList.Add($resultFile)
            $psi.UseShellExecute = $false
            $proc = [System.Diagnostics.Process]::Start($psi)

            $inFlight[$globalChunkId] = @{
                Process        = $proc
                ParamsFile     = $paramsFile
                ResultFile     = $resultFile
                StageName      = $work.StageName
                ChunkIdxInUnit = $work.ChunkIdxInUnit
                StartedAt      = [DateTime]::UtcNow
            }
            $nextPlanIdx++
            $globalChunkId++
        }

        # Reap via /proc/$pid liveness — same rationale as single-stage path.
        $finishedKeys = @($inFlight.Keys | Where-Object {
            $childPid = $inFlight[$_].Process.Id
            -not (Test-Path "/proc/$childPid")
        })
        foreach ($k in $finishedKeys) {
            $entry = $inFlight[$k]
            $exitCode = $entry.Process.ExitCode
            $result = $null

            if (Test-Path $entry.ResultFile) {
                try {
                    $result = Get-Content -Path $entry.ResultFile -Raw | ConvertFrom-Json -AsHashtable -Depth 20
                } catch { $result = $null }
            }

            if (-not $result) {
                $result = @{
                    ChunkIndex        = $entry.ChunkIdxInUnit
                    StageName         = $entry.StageName
                    Processed         = 0; Skipped = 0; Failed = 0
                    ItemsProcessed    = 0; ItemsFailed = 0; ItemsSkipped = 0
                    Errors            = @("chunk=$($entry.ChunkIdxInUnit) child exited with code $exitCode without producing a parseable result")
                    EmittedIds        = @()
                }
                $entityForLog = $unitState[$entry.StageName].Unit.Entity
                if (-not $entityForLog) { $entityForLog = '' }
                try {
                    Write-ChunkFailedEvent -Stage $entry.StageName -Entity $entityForLog `
                        -ChunkIndex $entry.ChunkIdxInUnit -ExceptionType 'System.Diagnostics.Process' `
                        -InnerExceptionType 'System.Diagnostics.Process' `
                        -Message "child exit=$exitCode no result file" -ScriptStackTrace ''
                } catch { Write-Verbose $_.Exception.Message }
            }

            # Route to owning unit's bucket.
            $unitState[$entry.StageName].ChunkResults.Add($result)

            # Heartbeat fold — same key format as single-stage path.
            if ($null -ne $ProgressShared) {
                try {
                    $ProgressShared["$($entry.StageName)/$($entry.ChunkIdxInUnit)"] = @{
                        stage           = $entry.StageName
                        slice_index     = $entry.ChunkIdxInUnit
                        items_processed = [int]($result.ItemsProcessed)
                        items_failed    = [int]($result.ItemsFailed)
                        items_skipped   = [int]($result.ItemsSkipped)
                        records_so_far  = [int]($result.Processed)
                        updated_at      = [DateTime]::UtcNow
                    }
                } catch { Write-Verbose $_.Exception.Message }
            }

            try { Remove-Item -Path $entry.ParamsFile -Force -ErrorAction SilentlyContinue } catch { Write-Verbose $_.Exception.Message }
            if ($exitCode -eq 0) {
                try { Remove-Item -Path $entry.ResultFile -Force -ErrorAction SilentlyContinue } catch { Write-Verbose $_.Exception.Message }
            } else {
                $anyChunkFailed = $true
            }
            $inFlight.Remove($k)
        }

        if ($finishedKeys.Count -eq 0 -and $inFlight.Count -gt 0) {
            if ($heartbeatFlush) {
                try { & $heartbeatFlush } catch { Write-Verbose $_.Exception.Message }
            }
            Start-Sleep -Milliseconds 500
        }
    }

    # Preserve $workDir for post-mortem if any chunk exited non-zero.
    # Matches the single-stage path's behavior.
    if (-not $anyChunkFailed) {
        try { Remove-Item -Path $workDir -Recurse -Force -ErrorAction SilentlyContinue } catch { Write-Verbose $_.Exception.Message }
    } else {
        Write-Warning "Some chunks in stage batch exited non-zero; preserving '$workDir' for post-mortem."
    }

    # Per-unit aggregation, mirroring Invoke-StagePoolOutOfProcess's
    # aggregation shape so callers see uniform per-stage results regardless
    # of whether dispatch was single-stage or batched.
    $resultsByStage = @{}
    foreach ($name in $unitState.Keys) {
        $state = $unitState[$name]
        $unit = $state.Unit

        $totalProcessed = 0; $totalSkipped = 0; $totalFailed = 0
        $totalItemsProcessed = 0; $totalItemsFailed = 0; $totalItemsSkipped = 0
        $allErrors = [System.Collections.Generic.List[string]]::new()
        $allEmittedIds = [System.Collections.Generic.List[hashtable]]::new()

        foreach ($r in $state.ChunkResults) {
            if ($r.Processed) { $totalProcessed += [int]$r.Processed }
            if ($r.Skipped)   { $totalSkipped   += [int]$r.Skipped }
            if ($r.Failed)    { $totalFailed    += [int]$r.Failed }
            if ($r.ItemsProcessed) { $totalItemsProcessed += [int]$r.ItemsProcessed }
            if ($r.ItemsFailed)    { $totalItemsFailed    += [int]$r.ItemsFailed }
            if ($r.ItemsSkipped)   { $totalItemsSkipped   += [int]$r.ItemsSkipped }
            if ($r.Errors -and $r.Errors.Count -gt 0) {
                $allErrors.AddRange([string[]]$r.Errors)
            }
            if ($r.EmittedIds) {
                foreach ($eid in $r.EmittedIds) {
                    if ($eid -is [hashtable]) {
                        $allEmittedIds.Add($eid)
                    } else {
                        $h = @{}
                        foreach ($p in $eid.PSObject.Properties) { $h[$p.Name] = $p.Value }
                        $allEmittedIds.Add($h)
                    }
                }
            }
        }

        $resultsByStage[$name] = @{
            RecordCount         = $totalProcessed
            SliceCount          = $state.TotalChunks
            SkippedCount        = $totalSkipped
            FailedCount         = $totalFailed
            ItemsProcessed      = $totalItemsProcessed
            ItemsFailed         = $totalItemsFailed
            ItemsSkipped        = $totalItemsSkipped
            Errors              = $allErrors.ToArray()
            EmittedIds          = $allEmittedIds.ToArray()
        }
    }

    return $resultsByStage
}

Export-ModuleMember -Function New-WorkerPool, New-StagePool, Invoke-StagePool, Invoke-StagePoolBatch, Invoke-StagePoolOutOfProcess, Invoke-StagePoolBatchOutOfProcess
