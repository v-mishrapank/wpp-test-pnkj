#!/usr/bin/env pwsh
# Out-of-process worker entry point. One invocation = one chunk of work.
#
# Spawned by Invoke-StagePoolOutOfProcess / Invoke-StagePoolBatchOutOfProcess
# (WorkerPool.psm1) via [System.Diagnostics.Process]::Start with a
# ProcessStartInfo whose UseShellExecute=false and no stream redirection
# — the child inherits the parent's stdout/stderr, so structured event
# lines from Write-Event flow directly to container stdout -> ACA -> LAW,
# identical to the in-process runspace path. IPC for params and results
# is via two JSON files whose paths the parent passes as arguments.
#
# Why this exists: ExchangeOnlineManagement V3 accumulates process-global
# state (REST connection cache, MSAL token cache, tmpEXO_* temp files) that
# Disconnect-ExchangeOnline does not reliably release in the same process.
# Recycling the entire pwsh process between chunks is the only mechanism
# Microsoft documents as reclaiming that state. See #484.

[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingWriteHost', '', Justification = "Mirrors LogHelper.psm1 pattern — Write-Host is the print primitive for ACA stdout capture; chunk-failure diagnostics must reach container stdout (and LAW) even when the result file is unwritable.")]
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ParamsFile,
    [Parameter(Mandatory)][string]$ResultFile
)

# Match the in-process runspace's default. Setting 'Stop' here turns
# benign non-terminating errors (cmdlet warnings, EXO module noise) into
# fatal exceptions that the dispatch block doesn't expect — the in-process
# path runs under 'Continue' inherited from ISS default. Diverging would
# cause stage-completing-but-throwing failure modes.
$ErrorActionPreference = 'Continue'

# Read params written by the parent. ConvertFrom-Json with -AsHashtable
# (pwsh 6+) round-trips nested hashtables faithfully — the dispatch block
# below treats $Context as a hashtable with .AuthConfig etc.
$params = Get-Content -Path $ParamsFile -Raw | ConvertFrom-Json -AsHashtable -Depth 20

# Module imports. Mirrors the InitialSessionState set up in New-WorkerPool:
# LogHelper (transitive dep of Write-Event), EventEmitter, RetryHelper,
# RecordEnvelope, StageWriter, MsalTokenHelper (no-op for exo/graph/spo,
# needed by powerplat/powerbi), plus the container's auth module and the
# entity module that defines the FunctionName fetcher.
$sharedModules = $params.SharedModulesPath
Import-Module (Join-Path $sharedModules 'LogHelper.psm1')       -Force -DisableNameChecking
Import-Module (Join-Path $sharedModules 'EventEmitter.psm1')    -Force -DisableNameChecking
Import-Module (Join-Path $sharedModules 'RetryHelper.psm1')     -Force -DisableNameChecking
Import-Module (Join-Path $sharedModules 'RecordEnvelope.psm1')  -Force -DisableNameChecking
Import-Module (Join-Path $sharedModules 'StageWriter.psm1')     -Force -DisableNameChecking
Import-Module (Join-Path $sharedModules 'MsalTokenHelper.psm1') -Force -DisableNameChecking -ErrorAction SilentlyContinue
Import-Module $params.AuthModulePath                            -Force -DisableNameChecking
Import-Module $params.ModulePath                                -Force -DisableNameChecking

# The provider module (ExchangeOnlineManagement for exo, etc.) is loaded
# by the auth module's Connect-Service on first use; no explicit import here.

# Recreate the dispatch scriptblock from its stringified form. This is the
# same $script:StageDispatchBlock that runs in-process today — the parent
# passed it as a string so we don't have to fork the logic. Inside the
# script, $global:IngestAuthDone is undefined at first invocation, so the
# auth latch fires exactly once per child (which is correct: each child
# is a fresh process and must auth).
$dispatchScript = [scriptblock]::Create($params.DispatchScriptStr)

try {
    # `& $scriptblock args` collects EVERY pipeline emission of the
    # scriptblock. The dispatch block's `return @{...}` is the intended
    # chunk-result hashtable, but in OOP mode the in-process auth latch
    # site uses `$null = & $authFn ...` to suppress Connect-Service's
    # return value (see WorkerPool.psm1 dispatch block). Defensive
    # $output[-1] below still picks the right element even if a future
    # fetcher accidentally emits — mirrors what the in-process aggregator
    # does (Invoke-StagePool reads $output[-1] from PowerShell.EndInvoke).
    $output = & $dispatchScript `
        $params.InputIds `
        $params.OutputDir `
        $params.ChunkNum `
        $params.RunId `
        $params.ApiFamily `
        $params.FunctionName `
        $params.Context `
        $params.AuthScriptStr `
        $params.ReconnectScriptStr `
        $params.FlushInterval `
        $params.JsonDepth `
        $params.SourceType `
        $params.SourceKey `
        $params.WriteRecords `
        $params.InputTags `
        $params.AutoFlushThreshold `
        $params.Tenant `
        $params.StageName `
        $params.Entity `
        $null `
        $params.AutoEmitIdField

    $result = if ($output -is [array]) { $output[-1] } else { $output }

    # Verbose-only chunk completion marker — useful with -Verbose for ad-hoc
    # debugging but doesn't pollute LAW with one row per chunk in normal runs.
    Write-Verbose "[WorkerChunk] Chunk $($params.ChunkNum) of stage '$($params.StageName)' completed; result keys: $(($result.Keys | Sort-Object) -join ',')"

    $result | ConvertTo-Json -Depth 20 -Compress | Set-Content -Path $ResultFile -Encoding UTF8
    exit 0
}
catch {
    # Child threw before producing a result. Write a synthetic failure
    # record so the parent gets a structured handle on what went wrong;
    # without this the parent only sees a non-zero exit code and a missing
    # result file. Mirror the chunk-result shape the dispatch block returns
    # so the aggregator can fold it uniformly.
    #
    # Pack the full PowerShell stack trace + inner-exception walk into the
    # error string so post-mortems don't need access to the child's stdout.
    # The dispatch block's diagnostics in errors only carry a one-line
    # message; for chunk-fatal failures we want the whole stack.
    $errRec = $_
    $traceStr = if ($errRec.ScriptStackTrace) { [string]$errRec.ScriptStackTrace } else { '' }
    $innerWalk = @()
    $cur = $errRec.Exception
    while ($cur) {
        $innerWalk += "$($cur.GetType().FullName): $($cur.Message)"
        $cur = $cur.InnerException
    }
    $errMsg = "chunk fatal: $($innerWalk -join ' <- ')`nScriptStackTrace:`n$traceStr"

    $errInfo = @{
        ChunkIndex        = $params.ChunkNum
        StageName         = $params.StageName
        Processed         = 0
        Skipped           = 0
        Failed            = 0
        ItemsProcessed    = 0
        ItemsFailed       = 0
        ItemsSkipped      = 0
        Errors            = @($errMsg)
        EmittedIds        = @()
    }
    $errInfo | ConvertTo-Json -Depth 10 -Compress | Set-Content -Path $ResultFile -Encoding UTF8

    # Also echo the diagnostic to stdout (inherits to container console -> LAW)
    # so we can find this even if the result file is unreachable.
    Write-Host "[WorkerChunk] Chunk $($params.ChunkNum) of stage '$($params.StageName)' failed: $errMsg"
    exit 1
}
