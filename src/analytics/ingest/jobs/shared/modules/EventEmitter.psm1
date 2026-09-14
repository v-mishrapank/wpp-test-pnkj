# Structured-event emitter. See EVENT_SCHEMA.md for the v1 contract.
#
# Every Write-Event produces one Write-Log line of the form
#   "<prose> _event:<json>"
# A LogHelper guard rejects non-emitter messages containing the literal
# `_event:` token, so the sentinel is unambiguous to KQL parsers reading
# ContainerAppConsoleLogs_CL.
#
# Stdout is the only transport. PR2 originally added an Application
# Insights customEvents mirror; it was dropped in favour of LAW-only
# because the workload is batch-shaped (no AI SDK auto-correlation to
# benefit from), workspace-based AI lands in the same LAW workspace
# anyway, and hand-rolled REST POSTs to v2.1/track introduced a class of
# silent failures without a reliability win. If a future requirement
# (Live Metrics stream, AI portal UI) demands AI as a transport, the
# cleanest path is a downstream forwarder reading ContainerAppConsoleLogs_CL.
#
# Runspace note: the script-scoped EventContext below does NOT cross runspace
# boundaries (same constraint as the WorkerPool tombstone in WorkerPool.psm1).
# Each runspace must call Initialize-EventContext at the top of its dispatch
# block to seed its own context. WorkerPool.psm1 threads RunId/Tenant/etc.
# through the dispatch params for exactly this reason.

$script:EventContext = @{
    RunId          = $null
    Tenant         = $null
    # Current stage/entity scope. Set-EventScope writes these as each stage
    # enters; RetryHelper's Write-ThrottleEvent (called from inline fetch
    # paths that don't take stage context) reads them so throttle events
    # are attributed even when the immediate caller can't pass the context
    # through. Pool stages also call Set-EventScope at the top of the
    # dispatch block — their stage/entity is already known from the
    # dispatch params, but the call ensures any RetryHelper invocations
    # from inside fetch functions pick them up too.
    CurrentStage   = ''
    CurrentEntity  = ''
}

$script:ValidEventTypes = @(
    'run_started',
    'stage_started',
    'stage_progress',
    'stage_completed',
    'stage_failed',
    'stage_skipped',
    'run_completed',
    'throttle_event',
    'unknown_retry_event',
    'chunk_failed',
    'item_failed',
    'chunk_upload_retry'
)

# The valid categories for item_failed are enforced via the ValidateSet on
# Write-ItemFailedEvent's -Category parameter; that's the single source of
# truth at the runtime boundary. EVENT_SCHEMA.md documents the same list for
# external consumers.

function Initialize-EventContext {
    param(
        [Parameter(Mandatory)][string]$RunId,
        [Parameter(Mandatory)][string]$Tenant
    )
    $script:EventContext.RunId         = $RunId
    $script:EventContext.Tenant        = $Tenant
    $script:EventContext.CurrentStage  = ''
    $script:EventContext.CurrentEntity = ''
}

function Set-EventScope {
    # Set the current stage/entity scope for events emitted from helpers
    # that don't take stage context (notably RetryHelper.Invoke-WithRetry).
    # Last-write-wins; not cleared between stages — the next Set-EventScope
    # call overwrites. Run-level events (run_started / run_completed) pass
    # empty Stage/Entity explicitly and don't read this fallback.
    param(
        [string]$Stage = '',
        [string]$Entity = ''
    )
    $script:EventContext.CurrentStage  = $Stage
    $script:EventContext.CurrentEntity = $Entity
}

function script:Protect-Prose {
    # Strip the `_event:` sentinel from caller-supplied strings so the
    # prose half of an event line can't accidentally contain a second
    # sentinel. Without this, a remote error message containing `_event:`
    # would make KQL's `extract(@'_event:(.+)$', ...)` capture from the
    # wrong start, returning malformed JSON. The JSON payload itself uses
    # ConvertTo-Json which escapes the colon-bearing string safely; this
    # only protects the human prose.
    param([string]$Text)
    if ([string]::IsNullOrEmpty($Text)) { return $Text }
    return $Text -replace '_event:', '_evnt_:'
}

function Format-EventProse {
    param(
        [string]$EventType,
        [string]$Stage,
        [hashtable]$Properties
    )
    switch ($EventType) {
        'run_started'     { "Run started entities=$($Properties.input_count)" }
        'stage_started'   {
            $ic = if ($null -eq $Properties.input_count) { 'inline' } else { $Properties.input_count }
            "Stage '$Stage' started input_count=$ic"
        }
        'stage_progress'  {
            $si = if ($null -ne $Properties.slice_index) { " slice=$($Properties.slice_index)" } else { '' }
            "Stage '$Stage' progress records=$($Properties.records_so_far)$si"
        }
        'stage_completed' { "Stage '$Stage' completed records=$($Properties.records_so_far) duration_ms=$($Properties.duration_ms)" }
        'stage_failed'    { "Stage '$Stage' failed: $(Protect-Prose $Properties.error_message)" }
        'stage_skipped'   { "Stage '$Stage' skipped: $(Protect-Prose $Properties.reason)" }
        'run_completed'   { "Run completed status=$($Properties.status) records=$($Properties.total_records) duration_ms=$($Properties.duration_ms)" }
        'throttle_event'  { "Throttled attempt=$($Properties.attempt) retry_after=$($Properties.retry_after_seconds)s status=$($Properties.status_code) signal='$(Protect-Prose $Properties.throttle_signal_text)'" }
        'unknown_retry_event' { "Unknown-retry attempt=$($Properties.attempt) delay=$($Properties.delay_seconds)s status=$($Properties.status_code) type='$($Properties.exception_type)' msg='$(Protect-Prose $Properties.error_message)'" }
        'chunk_failed'    { "Chunk $($Properties.chunk_index) failed in stage '$Stage' type='$($Properties.exception_type)' msg='$(Protect-Prose $Properties.error_message)'" }
        'item_failed'     { "Item '$(Protect-Prose $Properties.item_id)' failed in stage '$Stage' category=$($Properties.category) status=$($Properties.status_code) attempt=$($Properties.attempt) msg='$(Protect-Prose $Properties.error_message)'" }
        'chunk_upload_retry' { "Chunk upload retry attempt=$($Properties.attempt) delay=$($Properties.delay_seconds)s status=$($Properties.status_code) blob='$($Properties.blob_path)' msg='$(Protect-Prose $Properties.error_message)'" }
        default           { "Event $EventType" }
    }
}

function Write-Event {
    param(
        [Parameter(Mandatory)][string]$EventType,
        [string]$Entity = '',
        [string]$Stage = '',
        [hashtable]$Properties = @{},
        # Override run_id/tenant when the script-scope context isn't set
        # (e.g. inside a fresh runspace before Initialize-EventContext runs).
        # Normal callers don't pass these.
        [string]$RunIdOverride = $null,
        [string]$TenantOverride = $null
    )

    if ($script:ValidEventTypes -notcontains $EventType) {
        throw "Write-Event: unknown event_type '$EventType'. Valid: $($script:ValidEventTypes -join ', ')"
    }

    $runId  = if ($RunIdOverride)  { $RunIdOverride }  else { $script:EventContext.RunId }
    $tenant = if ($TenantOverride) { $TenantOverride } else { $script:EventContext.Tenant }

    $eventObj = [ordered]@{
        schema_version = 1
        # UtcNow — the `Z` suffix in the format string is just a literal
        # character; Get-Date returns local time. See LogHelper.psm1.
        ts             = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
        run_id         = $runId
        tenant         = $tenant
        entity         = $Entity
        stage          = $Stage
        event_type     = $EventType
    }
    foreach ($k in $Properties.Keys) {
        $eventObj[$k] = $Properties[$k]
    }

    # Compact one-line JSON — KQL parses on a per-row basis, so newlines in
    # the sentinel would break the `_event:(.+)$` extract.
    $json = $eventObj | ConvertTo-Json -Compress -Depth 5

    $prose = Format-EventProse -EventType $EventType -Stage $Stage -Properties $Properties
    $line = "$prose _event:$json"

    # AllowEventSentinel bypasses the LogHelper guard that protects normal
    # prose from accidentally containing `_event:`.
    Write-Log $line -Entity $Entity -TenantKey $tenant -AllowEventSentinel
}

function Write-ThrottleEvent {
    param(
        # When Stage/Entity are blank, fall back to whatever Set-EventScope
        # last set. Lets RetryHelper.Invoke-WithRetry (which doesn't take
        # stage context) still emit attributed throttle events as long as
        # the orchestrator declared scope at stage entry.
        [string]$Entity = '',
        [string]$Stage = '',
        [Parameter(Mandatory)][int]$RetryAfterSeconds,
        [Parameter(Mandatory)][int]$Attempt,
        [int]$StatusCode = 0,
        [string]$Message = '',
        [string]$RunIdOverride = $null,
        [string]$TenantOverride = $null
    )
    if ([string]::IsNullOrEmpty($Stage))  { $Stage  = $script:EventContext.CurrentStage }
    if ([string]::IsNullOrEmpty($Entity)) { $Entity = $script:EventContext.CurrentEntity }
    Write-Event -EventType throttle_event -Entity $Entity -Stage $Stage `
        -RunIdOverride $RunIdOverride -TenantOverride $TenantOverride `
        -Properties @{
            retry_after_seconds  = $RetryAfterSeconds
            attempt              = $Attempt
            status_code          = $StatusCode
            throttle_signal_text = $Message
        }
}

function Write-UnknownRetryEvent {
    # Sibling of Write-ThrottleEvent for the Unknown retry branch in
    # RetryHelper / WorkerPool. Without this signal, the Unknown sleep is
    # indistinguishable from a wedged container — the diagnostic ambiguity
    # that initially mis-classified the #321 wedge as silent retry. See #327.
    # Falls back to Set-EventScope's last Stage/Entity when caller doesn't
    # pass them (matches Write-ThrottleEvent).
    param(
        [string]$Entity = '',
        [string]$Stage = '',
        [Parameter(Mandatory)][int]$Attempt,
        [Parameter(Mandatory)][int]$DelaySeconds,
        [int]$StatusCode = 0,
        [string]$ExceptionType = '',
        [string]$InnerExceptionType = '',
        [string]$ApiFamily = '',
        [string]$Message = '',
        [string]$RunIdOverride = $null,
        [string]$TenantOverride = $null
    )
    if ([string]::IsNullOrEmpty($Stage))  { $Stage  = $script:EventContext.CurrentStage }
    if ([string]::IsNullOrEmpty($Entity)) { $Entity = $script:EventContext.CurrentEntity }
    Write-Event -EventType unknown_retry_event -Entity $Entity -Stage $Stage `
        -RunIdOverride $RunIdOverride -TenantOverride $TenantOverride `
        -Properties @{
            attempt              = $Attempt
            delay_seconds        = $DelaySeconds
            status_code          = $StatusCode
            exception_type       = $ExceptionType
            inner_exception_type = $InnerExceptionType
            api_family           = $ApiFamily
            error_message        = $Message
        }
}

function Write-ChunkFailedEvent {
    # Per-chunk uncaught exception from a pool dispatch. Sibling of
    # Write-ThrottleEvent / Write-UnknownRetryEvent. Surfaces what previously
    # only ever reached the per-tenant manifest's .errors list — the
    # aggregators in WorkerPool capture $ps.Streams.Error but never wrote
    # those strings to host stdout, so they didn't reach LAW. See #342.
    #
    # Each call corresponds to one error record on one chunk's
    # $ps.Streams.Error stream. A chunk can emit multiple if PowerShell
    # decides to wrap; ChunkIndex stays the same.
    param(
        [string]$Entity = '',
        [string]$Stage = '',
        [Parameter(Mandatory)][int]$ChunkIndex,
        [string]$ExceptionType = '',
        [string]$InnerExceptionType = '',
        [string]$Message = '',
        [string]$ScriptStackTrace = '',
        [string]$RunIdOverride = $null,
        [string]$TenantOverride = $null
    )
    if ([string]::IsNullOrEmpty($Stage))  { $Stage  = $script:EventContext.CurrentStage }
    if ([string]::IsNullOrEmpty($Entity)) { $Entity = $script:EventContext.CurrentEntity }

    # Self-enforce the contract: cap diagnostic strings before they enter the
    # event JSON. EVENT_SCHEMA.md advertises a 500-char cap on
    # script_stack_trace and the Get-PoolErrorInfo helper truncates there too,
    # but a future caller (or a code path we add later) could bypass the
    # helper and emit a 50KB stack trace into LAW. Capping in the emitter
    # keeps the contract single-sourced. error_message is also capped — most
    # exception messages are short, but a wrapped C# exception with embedded
    # context can run long. 500 chars matches RetryHelper.psm1's body cap.
    if ($null -ne $ScriptStackTrace -and $ScriptStackTrace.Length -gt 500) {
        $ScriptStackTrace = $ScriptStackTrace.Substring(0, 500) + '…'
    }
    if ($null -ne $Message -and $Message.Length -gt 500) {
        $Message = $Message.Substring(0, 500) + '…'
    }

    Write-Event -EventType chunk_failed -Entity $Entity -Stage $Stage `
        -RunIdOverride $RunIdOverride -TenantOverride $TenantOverride `
        -Properties @{
            chunk_index          = $ChunkIndex
            exception_type       = $ExceptionType
            inner_exception_type = $InnerExceptionType
            error_message        = $Message
            script_stack_trace   = $ScriptStackTrace
        }
}

function Write-ItemFailedEvent {
    # Per-item terminal failure inside a pool slice. Emitted from each of the
    # four previously-silent dispatch paths in WorkerPool.psm1:
    #   - NonRetryable (e.g. HTTP 400 — bad $select / malformed body)
    #   - Skippable (e.g. HTTP 404 — entity gone)
    #   - Auth-after-MaxRetries (5x reconnect failed)
    #   - Unknown-after-MaxRetries (5x backoff failed)
    # Without this, all four advanced the per-chunk counters without leaving
    # any LAW signal — the silent-skip hole that let #341 sit unnoticed for
    # months. See #356.
    # RetryExhausted (#526): the fetch's inner Invoke-WithRetry spent its
    # full budget, so the dispatch loop failed the item on its first outer
    # attempt instead of multiplying the two retry budgets.
    param(
        [string]$Entity = '',
        [string]$Stage = '',
        [Parameter(Mandatory)][ValidateSet('NonRetryable','Skippable','AuthMaxRetries','UnknownMaxRetries','RetryExhausted')][string]$Category,
        [Parameter(Mandatory)][string]$ItemId,
        [int]$Attempt = 1,
        [int]$StatusCode = 0,
        [string]$ExceptionType = '',
        [string]$Message = '',
        [string]$RunIdOverride = $null,
        [string]$TenantOverride = $null
    )
    if ([string]::IsNullOrEmpty($Stage))  { $Stage  = $script:EventContext.CurrentStage }
    if ([string]::IsNullOrEmpty($Entity)) { $Entity = $script:EventContext.CurrentEntity }

    # 500-char cap on Message — mirrors Write-ChunkFailedEvent so a wrapped
    # exception body or stack-y message can't blow up LAW row sizes.
    if ($null -ne $Message -and $Message.Length -gt 500) {
        $Message = $Message.Substring(0, 500) + '…'
    }

    Write-Event -EventType item_failed -Entity $Entity -Stage $Stage `
        -RunIdOverride $RunIdOverride -TenantOverride $TenantOverride `
        -Properties @{
            category       = $Category
            item_id        = $ItemId
            attempt        = $Attempt
            status_code    = $StatusCode
            exception_type = $ExceptionType
            error_message  = $Message
        }
}

function Write-ChunkUploadRetryEvent {
    # Per-attempt retry signal for the ADLS data-plane upload wrapper
    # (StorageHelperRest.Invoke-AdlsDataPlaneWithRetry). Mirrors
    # Write-ThrottleEvent / Write-UnknownRetryEvent — without this signal,
    # the new per-stage upload's retry sleeps are indistinguishable from a
    # wedged container. See #345.
    # Falls back to Set-EventScope's last Stage/Entity when caller doesn't
    # pass them — Write-ToAdlsRest is called from $uploadSink after
    # stage_completed, and StageExecutor.Invoke-ModuleRun calls Set-EventScope
    # at stage entry so the running stage's scope is visible at upload time.
    # The module-failure catch path in Invoke-Ingestion.ps1 explicitly resets
    # scope per synthetic entity before invoking $manifestSink so manifest-
    # upload retries attribute to the entity being failed, not the entity
    # that was last running before the throw.
    param(
        [string]$Entity = '',
        [string]$Stage = '',
        [Parameter(Mandatory)][string]$BlobPath,
        [Parameter(Mandatory)][int]$Attempt,
        [Parameter(Mandatory)][int]$DelaySeconds,
        [int]$StatusCode = 0,
        [string]$Message = '',
        [string]$RunIdOverride = $null,
        [string]$TenantOverride = $null
    )
    if ([string]::IsNullOrEmpty($Stage))  { $Stage  = $script:EventContext.CurrentStage }
    if ([string]::IsNullOrEmpty($Entity)) { $Entity = $script:EventContext.CurrentEntity }
    if ($null -ne $Message -and $Message.Length -gt 500) {
        $Message = $Message.Substring(0, 500) + '…'
    }
    Write-Event -EventType chunk_upload_retry -Entity $Entity -Stage $Stage `
        -RunIdOverride $RunIdOverride -TenantOverride $TenantOverride `
        -Properties @{
            blob_path     = $BlobPath
            attempt       = $Attempt
            delay_seconds = $DelaySeconds
            status_code   = $StatusCode
            error_message = $Message
        }
}

Export-ModuleMember -Function Initialize-EventContext, Set-EventScope, Write-Event, Write-ThrottleEvent, Write-UnknownRetryEvent, Write-ChunkFailedEvent, Write-ItemFailedEvent, Write-ChunkUploadRetryEvent
