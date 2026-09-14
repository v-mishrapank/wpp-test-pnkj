#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Unit tests for WorkerPool.psm1 helpers. Split-WorkItems and New-WorkerPool
# are exercised directly. Invoke-StagePool is exercised end-to-end with a
# fake entity module + StubConnect — the existing InvokeModuleRun tests
# cover only inline stages, so this file fills the pool-dispatch gap.

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    Import-Module (Join-Path $modulesPath 'LogHelper.psm1')         -Force
    Import-Module (Join-Path $modulesPath 'EventEmitter.psm1')      -Force
    Import-Module (Join-Path $modulesPath 'StageWriter.psm1')       -Force
    Import-Module (Join-Path $modulesPath 'RecordEnvelope.psm1')    -Force
    Import-Module (Join-Path $modulesPath 'RetryHelper.psm1')       -Force
    Import-Module (Join-Path $modulesPath 'WorkerPool.psm1')        -Force
}

Describe 'Split-WorkItems' {
    # Split-WorkItems is module-internal; Export-ModuleMember exposes only
    # Invoke-StagePool. Use InModuleScope to call it directly.
    It 'distributes items round-robin across slices' {
        InModuleScope WorkerPool {
            $slices = Split-WorkItems -Items @('a','b','c','d','e') -SliceCount 2
            @($slices).Count | Should -Be 2
            @($slices[0]) | Should -Be @('a','c','e')
            @($slices[1]) | Should -Be @('b','d')
        }
    }

    It 'returns SliceCount slices even when input is smaller' {
        InModuleScope WorkerPool {
            $slices = Split-WorkItems -Items @('only') -SliceCount 4
            @($slices).Count | Should -Be 4
            @($slices[0]) | Should -Be @('only')
            @($slices[1]).Count | Should -Be 0
            @($slices[2]).Count | Should -Be 0
            @($slices[3]).Count | Should -Be 0
        }
    }

    It 'returns a single full slice when SliceCount is 1' {
        InModuleScope WorkerPool {
            # Use leading-comma wrap to defeat PowerShell's single-element
            # array unwrap on function output (the inner List<string> would
            # otherwise be flattened directly into the caller's pipeline).
            $slices = ,(Split-WorkItems -Items @('a','b','c') -SliceCount 1)
            @($slices).Count | Should -Be 1
            @($slices[0]) | Should -Be @('a','b','c')
        }
    }

    It 'preserves the original order within each slice (stable round-robin)' {
        InModuleScope WorkerPool {
            $items = 1..10 | ForEach-Object { "item-$_" }
            $slices = Split-WorkItems -Items $items -SliceCount 3
            @($slices[0]) | Should -Be @('item-1','item-4','item-7','item-10')
            @($slices[1]) | Should -Be @('item-2','item-5','item-8')
            @($slices[2]) | Should -Be @('item-3','item-6','item-9')
        }
    }
}

Describe 'New-WorkerPool' {
    It 'creates a runspace pool that opens and closes cleanly' {
        $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
        $stageWriter = Join-Path $modulesPath 'StageWriter.psm1'
        InModuleScope WorkerPool -Parameters @{ p = $stageWriter } {
            param($p)
            $pool = New-WorkerPool -ModuleName $p -PoolSize 1
            try {
                $pool | Should -Not -BeNullOrEmpty
                $pool.GetType().Name | Should -Be 'RunspacePool'
                $pool.RunspacePoolStateInfo.State.ToString() | Should -Be 'Opened'
            }
            finally {
                $pool.Close()
                $pool.Dispose()
            }
        }
    }

    It 'imports the requested AdditionalModules into the runspace ISS' {
        # New-WorkerPool always imports RecordEnvelope/RetryHelper/LogHelper
        # plus -ModuleName, so picking any of those four to validate
        # AdditionalModules wouldn't actually prove anything. Build a temp
        # module with a uniquely-named function and assert it appears in the
        # runspace only when supplied via -AdditionalModules.
        $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
        $stageWriter = Join-Path $modulesPath 'StageWriter.psm1'

        $tempDir = New-Item -ItemType Directory -Path (
            Join-Path ([System.IO.Path]::GetTempPath()) "WorkerPoolAddModuleTest_$(Get-Random)"
        ) -Force
        $tempModule = Join-Path $tempDir.FullName 'AdditionalTestModule.psm1'
        Set-Content -Path $tempModule -Value @'
function Invoke-AdditionalModuleMarker { 'loaded' }
Export-ModuleMember -Function Invoke-AdditionalModuleMarker
'@

        try {
            InModuleScope WorkerPool -Parameters @{ sw = $stageWriter; am = $tempModule } {
                param($sw, $am)
                $pool = New-WorkerPool -ModuleName $sw -PoolSize 1 -AdditionalModules @($am)
                try {
                    $ps = [PowerShell]::Create()
                    try {
                        $ps.RunspacePool = $pool
                        $ps.AddScript({
                            $hasExtra = $null -ne (Get-Command Invoke-AdditionalModuleMarker -ErrorAction SilentlyContinue)
                            return @{ HasExtra = $hasExtra }
                        }) | Out-Null
                        $output = $ps.Invoke()
                        $output[-1].HasExtra | Should -Be $true
                    }
                    finally {
                        $ps.Dispose()
                    }
                }
                finally {
                    $pool.Close()
                    $pool.Dispose()
                }
            }
        }
        finally {
            Remove-Item -Path $tempDir.FullName -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}

Describe 'Invoke-StagePool (single-entity, inline auth stub)' {
    BeforeAll {
        # Stub out the Graph SDK module name. Default is
        # 'Microsoft.Graph.Authentication', which CI's pester job doesn't
        # install (only Pester is on the runner). Swap to a built-in so
        # New-WorkerPool's $iss.ImportPSModule is a no-op cost. StubConnect
        # provides Connect-Service / Restore-ServiceConnection, so the real
        # Graph SDK is never actually called.
        InModuleScope WorkerPool {
            $script:OriginalGraphModule = $script:ModuleNames['graph']
            $script:ModuleNames['graph'] = 'Microsoft.PowerShell.Utility'
        }

        # Build a fake entity module on disk that emits a known number of
        # records per item id. Mirrors the InvokeModuleRun fixture pattern
        # but sized for pool dispatch (which Invoke-StagePool drives).
        $script:tempDir = New-Item -ItemType Directory -Path (
            Join-Path ([System.IO.Path]::GetTempPath()) "WorkerPoolTest_$(Get-Random)"
        ) -Force
        $script:fakeModule = Join-Path $tempDir.FullName 'FakeEntity.psm1'

        $moduleContent = @'
function Get-Items {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # Emit two records per id for predictable record counts.
    $Writer.WriteRecord(@{ id = "$InputId-r1"; payload = 'a' })
    $Writer.WriteRecord(@{ id = "$InputId-r2"; payload = 'b' })
    $Writer.EmitId("$InputId-r1", $null)
}

function Get-Lumpy {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # Treat InputId as a literal record count — `200` → emit 200 records.
    # Lets a test drive lumpy per-item counts to exercise the FlushInterval
    # threshold tracker without an auth/network round-trip.
    $count = [int]$InputId
    for ($i = 0; $i -lt $count; $i++) {
        $Writer.WriteRecord(@{ id = "rec-$InputId-$i"; payload = 'x' })
    }
    $Writer.EmitId("done-$InputId", $null)
}

function Get-Skippable {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # Throw a Skippable-classified message (graph family Request_ResourceNotFound).
    throw "Request_ResourceNotFound: item $InputId is gone"
}

function Get-NoRecords {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # Successful fetch that legitimately has nothing to emit (e.g. a tenant
    # where the queried collection is empty). No WriteRecord, no EmitId.
    # The dispatch loop's lazy writer must NOT create a chunk file.
}

function Get-LumpyPrereq {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # Production-shaped prereq fetcher (mirrors Get-TeamChannels /
    # Get-TeamsRoot): WriteRecord is skipped because for a prereq nothing
    # claims the records, but EmitId is always called so descendants get
    # their input IDs. Treats InputId as a literal count to drive lumpy
    # per-item batches the same way Get-Lumpy does, except no records
    # are persisted and $sw.TotalWritten stays 0 — exactly the production
    # shape that the original #373 fix missed.
    $count = [int]$InputId
    for ($i = 0; $i -lt $count; $i++) {
        $Writer.EmitId("emit-$InputId-$i", $null)
    }
}

function Get-Bad400 {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # Throw a 400-shaped HTTP exception. Get-ErrorClassification reads
    # .Response.StatusCode and classifies as NonRetryable; the pool
    # dispatch must record one error per item and not retry.
    $resp = [PSCustomObject]@{ StatusCode = 400; Headers = $null }
    $ex   = [System.Exception]::new("400 Bad Request: bad query for $InputId")
    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
    throw $ex
}

function Get-UnknownThenSucceed {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # Per-item attempt counter on disk so the second invocation in the
    # dispatch's per-item retry loop sees the previous attempt's state.
    $f = Join-Path $Context.AttemptDir "$InputId.count"
    $n = 0
    if (Test-Path $f) { $n = [int](Get-Content $f) }
    $n++
    Set-Content -Path $f -Value $n
    if ($n -lt 2) {
        # 'transient hiccup' has no classification-matching pattern in the
        # graph family → Get-ErrorClassification returns Unknown. Forces the
        # dispatch block down the #327 emission path.
        throw "transient hiccup id=$InputId attempt=$n"
    }
    $Writer.WriteRecord(@{ id = $InputId; ok = $true })
    $Writer.EmitId($InputId, $null)
}

function Get-RetryExhausted {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # Simulate a fetcher whose inner Invoke-WithRetry already exhausted its
    # budget on a persistent 500 (#526): the rethrow carries the
    # RETRY_EXHAUSTED marker with the original HTTP error as InnerException.
    # Attempt counter on disk so the test can assert the dispatch loop made
    # exactly ONE outer attempt instead of re-running its own retry cycle.
    $f = Join-Path $Context.AttemptDir "$InputId.count"
    $n = 0
    if (Test-Path $f) { $n = [int](Get-Content $f) }
    $n++
    Set-Content -Path $f -Value $n
    $resp = [PSCustomObject]@{ StatusCode = 500; Headers = $null }
    $orig = [System.Exception]::new("500 AzureResourceManagerServerError id=$InputId")
    Add-Member -InputObject $orig -MemberType NoteProperty -Name Response -Value $resp
    throw [System.InvalidOperationException]::new("RETRY_EXHAUSTED: 500 AzureResourceManagerServerError id=$InputId", $orig)
}

Export-ModuleMember -Function Get-Items, Get-Skippable, Get-Bad400, Get-Lumpy, Get-LumpyPrereq, Get-NoRecords, Get-UnknownThenSucceed, Get-RetryExhausted
'@
        [System.IO.File]::WriteAllText($fakeModule, $moduleContent)

        $script:outDir = Join-Path $tempDir.FullName 'out'
        New-Item -ItemType Directory -Path $script:outDir -Force | Out-Null
    }

    AfterAll {
        if ($tempDir -and (Test-Path $tempDir.FullName)) {
            Remove-Item $tempDir.FullName -Recurse -Force
        }
        InModuleScope WorkerPool {
            if ($script:OriginalGraphModule) {
                $script:ModuleNames['graph'] = $script:OriginalGraphModule
            }
        }
    }

    It 'dispatches across the pool and aggregates Processed correctly' {
        # 4 input ids, pool size 2 → 2 slices, 2 items each, 2 records per item = 8 total.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'

        $result = Invoke-StagePool `
            -StageName       'fake_root' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-Items' `
            -InputIds        @('i1','i2','i3','i4') `
            -Context         @{ ProjectionFields = @('id','payload'); AuthConfig = @{} } `
            -OutputDirectory $script:outDir `
            -RunId           'runX' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        2 `
            -ApiFamily       'graph' `
            -WriteRecords

        $result.RecordCount  | Should -Be 8
        $result.SkippedCount | Should -Be 0
        $result.SliceCount   | Should -Be 2
        $result.EmittedIds.Count | Should -Be 4
        @($result.Errors).Count | Should -Be 0
    }

    It 'caps slice count at InputIds count when pool is larger than input' {
        # 1 input id, pool size 8 → just 1 slice (no empty chunk files).
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'

        $result = Invoke-StagePool `
            -StageName       'fake_root' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-Items' `
            -InputIds        @('only') `
            -Context         @{ ProjectionFields = @('id','payload'); AuthConfig = @{} } `
            -OutputDirectory $script:outDir `
            -RunId           'runY' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        8 `
            -ApiFamily       'graph' `
            -WriteRecords

        $result.RecordCount | Should -Be 2
        $result.SliceCount  | Should -Be 1
    }

    It 'writes no chunk files when every dispatched slice produces zero records (#253)' {
        # 5 inputs, pool 5 → 5 slices each carrying 1 input. Fetcher returns
        # nothing for every item. The lazy StreamWriter must never open,
        # so the output dir stays empty even though SliceCount is 5.
        # StageExecutor's manifest reads chunk_count from the on-disk file
        # count, not SliceCount — this asserts the precondition that lets
        # that downstream check land at chunk_count=0.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $emptyOut = Join-Path $script:tempDir.FullName 'empty_out'
        New-Item -ItemType Directory -Path $emptyOut -Force | Out-Null

        $result = Invoke-StagePool `
            -StageName       'fake_empty' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-NoRecords' `
            -InputIds        @('e1','e2','e3','e4','e5') `
            -Context         @{ ProjectionFields = @('id'); AuthConfig = @{} } `
            -OutputDirectory $emptyOut `
            -RunId           'runEmpty' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        5 `
            -ApiFamily       'graph' `
            -WriteRecords

        $result.RecordCount | Should -Be 0
        $result.SliceCount  | Should -Be 5
        @($result.Errors).Count | Should -Be 0
        @(Get-ChildItem $emptyOut -Filter '*.jsonl' -ErrorAction SilentlyContinue).Count | Should -Be 0
    }

    It 'classifies thrown items as Skippable and reports them in SkippedCount (not FailedCount)' {
        # #356: Skippable goes to $skipped++, NOT $failed++. The split lets
        # KQL distinguish "legitimate 404s" from "actual failures" without
        # cross-referencing error_count.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'

        $result = Invoke-StagePool `
            -StageName       'fake_skip' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-Skippable' `
            -InputIds        @('s1','s2','s3') `
            -Context         @{ ProjectionFields = @('id'); AuthConfig = @{} } `
            -OutputDirectory $script:outDir `
            -RunId           'runZ' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        2 `
            -ApiFamily       'graph' `
            -WriteRecords

        $result.RecordCount  | Should -Be 0
        $result.SkippedCount | Should -Be 3
        $result.FailedCount  | Should -Be 0
    }

    It 'fails fast on NonRetryable (HTTP 400): records error, increments FailedCount (not SkippedCount), no retry loop' {
        # Validates the fix from PR #261 — before NonRetryable existed, a 400
        # was Unknown and burned 5×5 retry attempts per item before giving up,
        # hanging the chunk for ~5 minutes. Now: one error per item, one fail
        # per item, no retries.
        # #356: counter split moves 400s out of SkippedCount into FailedCount.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'

        $result = Invoke-StagePool `
            -StageName       'fake_400' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-Bad400' `
            -InputIds        @('b1','b2','b3') `
            -Context         @{ ProjectionFields = @('id'); AuthConfig = @{} } `
            -OutputDirectory $script:outDir `
            -RunId           'run400' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        2 `
            -ApiFamily       'graph' `
            -WriteRecords

        $result.RecordCount   | Should -Be 0
        $result.SkippedCount  | Should -Be 0
        $result.FailedCount   | Should -Be 3
        @($result.Errors).Count | Should -Be 3
        @($result.Errors)[0]  | Should -Match 'NonRetryable'
        @($result.Errors)[0]  | Should -Match 'item=b'
    }

    It 'fails fast on RetryExhausted: one outer attempt per item, FailedCount incremented, no re-retry (#526)' {
        # The nested-retry amplification fix: an error carrying the
        # RETRY_EXHAUSTED marker (inner Invoke-WithRetry already spent its
        # 5-attempt budget) must be terminal at the dispatch loop — before
        # this, the outer Unknown branch re-ran the whole inner cycle up to
        # 5 more times (~40 min observed on one persistently-500ing item).
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $attemptDir = New-Item -ItemType Directory -Path (
            Join-Path $script:tempDir.FullName 'attempts_exhausted'
        ) -Force

        $result = Invoke-StagePool `
            -StageName       'fake_exhausted' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-RetryExhausted' `
            -InputIds        @('x1','x2') `
            -Context         @{ ProjectionFields = @('id'); AuthConfig = @{}; AttemptDir = $attemptDir.FullName } `
            -OutputDirectory $script:outDir `
            -RunId           'runExh' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        2 `
            -ApiFamily       'graph' `
            -WriteRecords

        $result.RecordCount   | Should -Be 0
        $result.SkippedCount  | Should -Be 0
        $result.FailedCount   | Should -Be 2
        $result.ItemsFailed   | Should -Be 2
        @($result.Errors).Count | Should -Be 2
        @($result.Errors)[0]  | Should -Match 'RetryExhausted'
        # Exactly one outer attempt per item — the dispatch loop must not
        # have re-entered the fetch for an already-exhausted error.
        foreach ($id in @('x1','x2')) {
            [int](Get-Content (Join-Path $attemptDir.FullName "$id.count")) | Should -Be 1
        }
    }

    It 'emits unknown_retry_event from dispatch on Unknown-classifying exception (#327)' {
        # Pre-#327 the dispatch's Unknown branch was a bare Start-Sleep — no
        # WARN, no telemetry. From the outside it was indistinguishable from
        # a wedged container, which is the diagnostic ambiguity that initially
        # mis-classified #321. This test shadows EventEmitter's
        # Write-UnknownRetryEvent with a spy module imported AFTER it via
        # AdditionalModules (last-import wins for command resolution inside
        # the runspace) and asserts the dispatch block calls it on retry.
        $stubConnect    = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $modulesPath    = Join-Path $PSScriptRoot '..' 'modules'
        $stageWriterPath = Join-Path $modulesPath 'StageWriter.psm1'

        $spyDir = New-Item -ItemType Directory -Path (
            Join-Path $script:tempDir.FullName "spy_$(Get-Random)"
        ) -Force
        $sentinelFile = Join-Path $spyDir.FullName 'unknown_retry_calls.txt'
        $attemptDir   = New-Item -ItemType Directory -Path (Join-Path $spyDir.FullName 'attempts') -Force
        $spyModule    = Join-Path $spyDir.FullName 'UnknownRetrySpy.psm1'

        # Single-quoted heredoc: $Stage / $Entity / $Attempt etc. stay as
        # PowerShell variables inside the spy. Only $sentinelFile needs to
        # be substituted in, so do that with -replace after the fact.
        $spyBody = @'
function Write-UnknownRetryEvent {
    param(
        [string]$Entity = '',
        [string]$Stage = '',
        [int]$Attempt,
        [int]$DelaySeconds,
        [int]$StatusCode = 0,
        [string]$ExceptionType = '',
        [string]$InnerExceptionType = '',
        [string]$ApiFamily = '',
        [string]$Message = '',
        [string]$RunIdOverride = $null,
        [string]$TenantOverride = $null
    )
    Add-Content -Path '__SENTINEL__' -Value "stage=$Stage entity=$Entity attempt=$Attempt api=$ApiFamily type=$ExceptionType"
}
Export-ModuleMember -Function Write-UnknownRetryEvent
'@
        # Literal string Replace, not -replace — the latter is regex-based and
        # could mis-interpret `$` or `\` sequences in $sentinelFile (e.g. on
        # Windows). Per Copilot review on PR #332.
        Set-Content -Path $spyModule -Value ($spyBody.Replace('__SENTINEL__', $sentinelFile))

        # Build the pool directly so we can include the spy in
        # AdditionalModules — Invoke-StagePool's hardcoded New-WorkerPool
        # call doesn't expose AdditionalModules. Pass the prebuilt pool via
        # -Pool to bypass internal pool creation. PoolSize=1 puts both items
        # on the same runspace so the spy's Add-Content writes are
        # sequential — concurrent appends from two runspaces race on Linux
        # CI runners and silently drop one.
        $pool = New-WorkerPool -ModuleName 'Microsoft.PowerShell.Utility' -PoolSize 1 `
            -AdditionalModules @($stageWriterPath, $stubConnect, $script:fakeModule, $spyModule)

        try {
            $result = Invoke-StagePool `
                -StageName       'fake_unknown' `
                -ModulePath      $script:fakeModule `
                -FunctionName    'Get-UnknownThenSucceed' `
                -InputIds        @('u1','u2') `
                -Context         @{ ProjectionFields = @('id','ok'); AuthConfig = @{}; AttemptDir = $attemptDir.FullName } `
                -OutputDirectory $script:outDir `
                -RunId           'run327' `
                -Tenant          'fab' `
                -Entity          'fake_unknown_entity' `
                -AuthModulePath  $stubConnect `
                -SourceType      'tenant' `
                -SourceKey       'fab' `
                -PoolSize        1 `
                -ApiFamily       'graph' `
                -Pool            $pool `
                -WriteRecords
        }
        finally {
            $pool.Close()
            $pool.Dispose()
        }

        # Both items must succeed on attempt 2 (one Unknown retry each).
        $result.RecordCount  | Should -Be 2
        $result.SkippedCount | Should -Be 0
        @($result.Errors).Count | Should -Be 0

        # Spy file should exist with one line per Unknown retry. Two items,
        # one retry each = two lines.
        Test-Path $sentinelFile | Should -BeTrue
        $lines = @(Get-Content $sentinelFile)
        $lines.Count | Should -Be 2
        $lines[0] | Should -Match 'stage=fake_unknown'
        $lines[0] | Should -Match 'entity=fake_unknown_entity'
        $lines[0] | Should -Match 'attempt=1'
        $lines[0] | Should -Match 'api=graph'
    }

    It 'emits chunk_failed when first-dispatch self-auth throws (#342)' {
        # Pre-#342 the aggregators added "chunk=N: <msg>" to $allErrors but
        # never wrote it to host stdout, so chunk-level uncaught failures
        # were invisible in LAW — only readable via per-tenant ADLS manifests.
        # Provoke that path with a Connect-Service that throws a typed
        # exception: first-dispatch sits outside the inner per-item try/catch
        # (WorkerPool.psm1:175), so the throw propagates to $ps.Streams.Error.
        # The aggregator code runs in the MAIN process (not the pool
        # runspace), so we can Mock Write-Log inside EventEmitter scope to
        # catch the emission — same pattern as EventEmitter.Tests.ps1.
        $modulesPath     = Join-Path $PSScriptRoot '..' 'modules'
        $stageWriterPath = Join-Path $modulesPath 'StageWriter.psm1'

        $failDir = New-Item -ItemType Directory -Path (
            Join-Path $script:tempDir.FullName "chunkfail_$(Get-Random)"
        ) -Force
        $failingConnect = Join-Path $failDir.FullName 'FailingConnect.psm1'

        # Stub Connect-Service that throws an ArgumentNullException — mirrors
        # the real shape of a PnP Connect-PnPOnline -Tenant $null binding
        # error closely enough that the captured exception class is testable.
        $connectBody = @'
function Connect-Service {
    [CmdletBinding()]
    param([Parameter(Mandatory)][hashtable]$Config)
    throw [System.ArgumentNullException]::new('Tenant',
        "Cannot bind argument to parameter 'Tenant' because it is null.")
}
function Restore-ServiceConnection { param() }
Export-ModuleMember -Function Connect-Service, Restore-ServiceConnection
'@
        Set-Content -Path $failingConnect -Value $connectBody

        # Capture Write-Log calls from the EventEmitter module scope — this
        # is where Write-ChunkFailedEvent ultimately routes. Pool runspaces
        # have their own EventEmitter instance (loaded by New-WorkerPool's
        # ISS), but the aggregator's Write-ChunkFailedEvent call runs in the
        # main process, so this mock catches it.
        $script:chunkFailedLines = [System.Collections.Generic.List[string]]::new()
        Mock -ModuleName EventEmitter Write-Log {
            if ($Message -match '"event_type":"chunk_failed"') {
                $script:chunkFailedLines.Add($Message)
            }
        }

        $pool = New-WorkerPool -ModuleName 'Microsoft.PowerShell.Utility' -PoolSize 1 `
            -AdditionalModules @($stageWriterPath, $failingConnect, $script:fakeModule)

        try {
            $result = Invoke-StagePool `
                -StageName       'fake_chunkfail' `
                -ModulePath      $script:fakeModule `
                -FunctionName    'Get-Items' `
                -InputIds        @('a','b') `
                -Context         @{ ProjectionFields = @('id','payload'); AuthConfig = @{} } `
                -OutputDirectory $script:outDir `
                -RunId           'run342' `
                -Tenant          'fab' `
                -Entity          'fake_chunkfail_entity' `
                -AuthModulePath  $failingConnect `
                -SourceType      'tenant' `
                -SourceKey       'fab' `
                -PoolSize        1 `
                -ApiFamily       'graph' `
                -Pool            $pool `
                -WriteRecords
        }
        finally {
            $pool.Close()
            $pool.Dispose()
        }

        # Back-compat: the manifest-shape "chunk=N: <msg>" string still lands
        # in $result.Errors so existing manifest readers don't break.
        @($result.Errors).Count | Should -BeGreaterOrEqual 1
        @($result.Errors)[0]    | Should -Match 'chunk='
        @($result.Errors)[0]    | Should -Match "Cannot bind argument to parameter 'Tenant'"

        # Aggregator emitted at least one chunk_failed event carrying the
        # exception class — the data point that pre-#342 was missing in LAW.
        $script:chunkFailedLines.Count | Should -BeGreaterOrEqual 1
        $first = $script:chunkFailedLines[0]
        $first | Should -Match '"event_type":"chunk_failed"'
        $first | Should -Match '"stage":"fake_chunkfail"'
        $first | Should -Match '"chunk_index":0'
        # The outer exception is MethodInvocationException because PowerShell
        # wraps the throw from inside a scriptblock invocation; the innermost
        # is the actual ArgumentNullException from our stub. Both must be
        # captured so the operator can see the real cause.
        $first | Should -Match '"inner_exception_type":"System.ArgumentNullException"'
    }

    It 'emits item_failed from dispatch with the correct Category on NonRetryable and Skippable (#356)' {
        # Pre-#356 the dispatch's NonRetryable / Skippable / MaxRetries-exhausted
        # paths were all silent — no LAW signal beyond the aggregated error_count
        # on stage_completed. This test shadows EventEmitter's
        # Write-ItemFailedEvent with a spy and asserts the dispatch calls it
        # with the right Category on the two single-attempt terminal paths.
        # AuthMaxRetries / UnknownMaxRetries fire from the same code site
        # (the `if ($attempt -ge $MaxRetries)` branch, just with a different
        # Category-by-original-class lookup) and are exercised at the helper
        # level by EventEmitter.Tests.ps1's "accepts each of the four valid
        # Category values" case. Provoking real 5×-retry exhaustion in a
        # dispatch e2e test would add ~10s to the suite for marginal extra
        # signal. Mirrors the #327 unknown_retry_event spy pattern.
        $stubConnect    = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $modulesPath    = Join-Path $PSScriptRoot '..' 'modules'
        $stageWriterPath = Join-Path $modulesPath 'StageWriter.psm1'

        $spyDir = New-Item -ItemType Directory -Path (
            Join-Path $script:tempDir.FullName "itemspy_$(Get-Random)"
        ) -Force
        $sentinelFile = Join-Path $spyDir.FullName 'item_failed_calls.txt'
        $spyModule    = Join-Path $spyDir.FullName 'ItemFailedSpy.psm1'

        $spyBody = @'
function Write-ItemFailedEvent {
    param(
        [string]$Entity = '',
        [string]$Stage = '',
        [string]$Category,
        [string]$ItemId,
        [int]$Attempt = 1,
        [int]$StatusCode = 0,
        [string]$ExceptionType = '',
        [string]$Message = '',
        [string]$RunIdOverride = $null,
        [string]$TenantOverride = $null
    )
    Add-Content -Path '__SENTINEL__' -Value "stage=$Stage entity=$Entity category=$Category item=$ItemId attempt=$Attempt status=$StatusCode"
}
Export-ModuleMember -Function Write-ItemFailedEvent
'@
        Set-Content -Path $spyModule -Value ($spyBody.Replace('__SENTINEL__', $sentinelFile))

        # PoolSize=1 keeps spy appends serialized (concurrent Add-Content
        # races on Linux CI — same reason as the #327 spy test).
        $pool = New-WorkerPool -ModuleName 'Microsoft.PowerShell.Utility' -PoolSize 1 `
            -AdditionalModules @($stageWriterPath, $stubConnect, $script:fakeModule, $spyModule)

        try {
            # NonRetryable (400): one item, one event with Category=NonRetryable.
            $null = Invoke-StagePool `
                -StageName       'item_failed_400' `
                -ModulePath      $script:fakeModule `
                -FunctionName    'Get-Bad400' `
                -InputIds        @('b1') `
                -Context         @{ ProjectionFields = @('id'); AuthConfig = @{} } `
                -OutputDirectory $script:outDir `
                -RunId           'run_item_400' `
                -Tenant          'fab' `
                -Entity          'fake_entity' `
                -AuthModulePath  $stubConnect `
                -SourceType      'tenant' `
                -SourceKey       'fab' `
                -PoolSize        1 `
                -ApiFamily       'graph' `
                -Pool            $pool `
                -WriteRecords

            # Skippable (Request_ResourceNotFound on graph): one item, one event
            # with Category=Skippable.
            $null = Invoke-StagePool `
                -StageName       'item_failed_skip' `
                -ModulePath      $script:fakeModule `
                -FunctionName    'Get-Skippable' `
                -InputIds        @('s1') `
                -Context         @{ ProjectionFields = @('id'); AuthConfig = @{} } `
                -OutputDirectory $script:outDir `
                -RunId           'run_item_skip' `
                -Tenant          'fab' `
                -Entity          'fake_entity' `
                -AuthModulePath  $stubConnect `
                -SourceType      'tenant' `
                -SourceKey       'fab' `
                -PoolSize        1 `
                -ApiFamily       'graph' `
                -Pool            $pool `
                -WriteRecords
        }
        finally {
            $pool.Close()
            $pool.Dispose()
        }

        Test-Path $sentinelFile | Should -BeTrue
        $lines = @(Get-Content $sentinelFile)
        $lines.Count | Should -Be 2

        $nonRetry = $lines | Where-Object { $_ -match 'category=NonRetryable' }
        $nonRetry | Should -Not -BeNullOrEmpty
        $nonRetry | Should -Match 'stage=item_failed_400'
        $nonRetry | Should -Match 'entity=fake_entity'
        $nonRetry | Should -Match 'item=b1'
        $nonRetry | Should -Match 'status=400'

        $skippable = $lines | Where-Object { $_ -match 'category=Skippable' }
        $skippable | Should -Not -BeNullOrEmpty
        $skippable | Should -Match 'stage=item_failed_skip'
        $skippable | Should -Match 'item=s1'
    }

    It 'items_failed and items_skipped tick on NonRetryable / Skippable terminal paths (#383)' {
        # Companion to the #356 item_failed-events test above. Here we check
        # the pool-aggregated ItemsFailed / ItemsSkipped counts that drive
        # the items_failed / items_skipped fields on the heartbeat blob. The
        # dispatch ticks items_processed once per foreach iteration that
        # exits via $itemDone=$true, and adds to items_failed (NonRetryable,
        # MaxRetries-exhausted) or items_skipped (Skippable) at the same
        # time. ItemsFailed + ItemsSkipped <= ItemsProcessed by construction.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'

        $bad = Invoke-StagePool `
            -StageName       'items_383_400' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-Bad400' `
            -InputIds        @('b1','b2','b3') `
            -Context         @{ ProjectionFields = @('id'); AuthConfig = @{} } `
            -OutputDirectory $script:outDir `
            -RunId           'run_383_400' `
            -Tenant          'fab' `
            -Entity          'fake_entity' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        1 `
            -ApiFamily       'graph' `
            -WriteRecords

        $bad.ItemsProcessed | Should -Be 3
        $bad.ItemsFailed | Should -Be 3
        $bad.ItemsSkipped | Should -Be 0
        $bad.RecordCount | Should -Be 0

        $skip = Invoke-StagePool `
            -StageName       'items_383_skip' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-Skippable' `
            -InputIds        @('s1','s2') `
            -Context         @{ ProjectionFields = @('id'); AuthConfig = @{} } `
            -OutputDirectory $script:outDir `
            -RunId           'run_383_skip' `
            -Tenant          'fab' `
            -Entity          'fake_entity' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        1 `
            -ApiFamily       'graph' `
            -WriteRecords

        $skip.ItemsProcessed | Should -Be 2
        $skip.ItemsFailed | Should -Be 0
        $skip.ItemsSkipped | Should -Be 2
    }

    It 'records_so_far accumulates across items and matches stage_completed total' {
        # End-to-end behavioral check: 3 items × 200 records each = 600 total
        # records returned, validating that the threshold-tracking flush
        # cadence doesn't drop records or miscount even when items each
        # cross multiple FlushInterval boundaries. (Stage_progress event
        # cadence itself is exercised on a real LAW deployment per the
        # branch-env smoke test on PR #270 — runspace Write-Host output
        # doesn't reliably surface to Pester here, so we assert on the
        # aggregate result instead.)
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'

        $result = Invoke-StagePool `
            -StageName       'fake_lumpy' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-Lumpy' `
            -InputIds        @('200','200','200') `
            -Context         @{ ProjectionFields = @('id','payload'); AuthConfig = @{} } `
            -OutputDirectory $script:outDir `
            -RunId           'lumpy-run' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        1 `
            -ApiFamily       'graph' `
            -FlushInterval   100 `
            -WriteRecords

        $result.RecordCount | Should -Be 600
        @($result.Errors).Count | Should -Be 0
        # #383: per-input-item totals. 3 successful inputs in a single slice.
        $result.ItemsProcessed | Should -Be 3
        $result.ItemsFailed | Should -Be 0
        $result.ItemsSkipped | Should -Be 0
    }

    It 'final tick: tail batch (<FlushInterval records) lands the true count in ProgressShared (#328)' {
        # Pre-#328: tail records (those after the last 100-boundary) never
        # ticked, so ProgressShared froze on the last full hundred until
        # Set-StageCompleted ran. This drove the false-wedge observation on
        # team_details (25000 stuck for 23min while processing the final 78).
        # Now: the runspace's finally block emits one more snapshot when
        # $processed > $lastFlushAt, capturing the chunk's true total.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $shared = [hashtable]::Synchronized(@{})

        $result = Invoke-StagePool `
            -StageName       'fake_lumpy' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-Lumpy' `
            -InputIds        @('178') `
            -Context         @{ ProjectionFields = @('id','payload'); AuthConfig = @{} } `
            -OutputDirectory $script:outDir `
            -RunId           'tail-run' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        1 `
            -ApiFamily       'graph' `
            -FlushInterval   100 `
            -ProgressShared  $shared `
            -WriteRecords

        $result.RecordCount | Should -Be 178
        @($result.Errors).Count | Should -Be 0

        # Slice 0 (single runspace, single chunk) should reflect the true
        # final count, not the last 100-boundary (100). Pre-fix this would
        # be 100; post-fix it's 178.
        $shared.ContainsKey('fake_lumpy/0') | Should -BeTrue
        [int]$shared['fake_lumpy/0'].records_so_far | Should -Be 178
        # #383: single input '178' = 1 item processed.
        [int]$shared['fake_lumpy/0'].items_processed | Should -Be 1
        [int]$shared['fake_lumpy/0'].items_failed | Should -Be 0
        [int]$shared['fake_lumpy/0'].items_skipped | Should -Be 0
    }

    It 'final tick gate: round-multiple chunk does not over-emit, slot still equals total (#328)' {
        # Regression guard for the gate `$processed -gt $lastFlushAt`.
        # When N % FlushInterval == 0 the boundary tick already fired with
        # the true total, so the final tick must NOT fire (would be a
        # redundant duplicate emit). The slot value is identical either way
        # — this case asserts the gate doesn't crash or off-by-one when
        # processed == lastFlushAt at end of loop.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $shared = [hashtable]::Synchronized(@{})

        $result = Invoke-StagePool `
            -StageName       'fake_lumpy' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-Lumpy' `
            -InputIds        @('200') `
            -Context         @{ ProjectionFields = @('id','payload'); AuthConfig = @{} } `
            -OutputDirectory $script:outDir `
            -RunId           'round-run' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        1 `
            -ApiFamily       'graph' `
            -FlushInterval   100 `
            -ProgressShared  $shared `
            -WriteRecords

        $result.RecordCount | Should -Be 200
        @($result.Errors).Count | Should -Be 0
        $shared.ContainsKey('fake_lumpy/0') | Should -BeTrue
        [int]$shared['fake_lumpy/0'].records_so_far | Should -Be 200
        # #383: single input '200' = 1 item processed.
        [int]$shared['fake_lumpy/0'].items_processed | Should -Be 1
    }

    It 'final tick: zero-record chunk still emits slot when items_processed advanced (#383)' {
        # Pre-#383: gate was `$processed -gt $lastFlushAt`, so a slice with N
        # inputs that produced zero records (legitimate empty fetches, or
        # 100% Skippable / NonRetryable) wrote no slot at all — leaving the
        # dashboard unable to distinguish "no inputs" from "all inputs done,
        # zero results". Post-#383: gate also fires when $itemsProcessed
        # advanced, so the slot lands with records_so_far=0 but
        # items_processed=<input count for the slice>. That's strictly more
        # information for the operator.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $shared = [hashtable]::Synchronized(@{})
        $emptyOut = Join-Path $script:tempDir.FullName "empty_shared_$(Get-Random)"
        New-Item -ItemType Directory -Path $emptyOut -Force | Out-Null

        $result = Invoke-StagePool `
            -StageName       'fake_empty_shared' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-NoRecords' `
            -InputIds        @('e1','e2','e3','e4','e5') `
            -Context         @{ ProjectionFields = @('id'); AuthConfig = @{} } `
            -OutputDirectory $emptyOut `
            -RunId           'empty-shared-run' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        5 `
            -ApiFamily       'graph' `
            -FlushInterval   100 `
            -ProgressShared  $shared `
            -WriteRecords

        $result.RecordCount | Should -Be 0
        @($result.Errors).Count | Should -Be 0
        # 5 inputs × pool 5 = 5 slices, each with 1 input. Every slice's
        # final tick fires (items_processed=1 > lastItemsFlushAt=0), so 5
        # slots land — each with records=0, items_processed=1.
        $slotKeys = @($shared.Keys | Where-Object { $_ -like 'fake_empty_shared/*' })
        $slotKeys.Count | Should -Be 5
        foreach ($k in $slotKeys) {
            [int]$shared[$k].records_so_far | Should -Be 0
            [int]$shared[$k].items_processed | Should -Be 1
        }
        # And the aggregate: 5 items processed total, 0 records.
        $result.ItemsProcessed | Should -Be 5
        $result.ItemsFailed | Should -Be 0
        $result.ItemsSkipped | Should -Be 0
    }

    It 'all-Skippable chunk: final slot lands records=0, items_skipped=N (end-to-end wire, #399)' {
        # 250 Skippable items split across 2 slices (125 each via round-
        # robin), FlushInterval=100. Records stay at 0 for the entire loop
        # because Get-Skippable throws on every item (caught and classified
        # as Skippable, $skipped/$itemsSkipped++, $processed stays 0).
        #
        # SCOPE: this asserts ONLY on the final ProgressShared slot value,
        # which is also what the chunk-end `finally` tick produces — so
        # this test does not on its own prove the periodic items-leg gate
        # fires mid-chunk. That property is locked down by the logic-level
        # predicate test in the 'FlushInterval threshold tracker' Describe
        # block below ('fires on the items leg when records leg is silent
        # (all-Skippable / all-NonRetryable, #399)'). What this test does
        # cover is the full end-to-end wire for the all-skip shape: fetcher
        # throw → Skippable classification → $itemsSkipped++ → slot writer
        # populates items_* fields correctly → slot reflects the chunk's
        # actual counts (not the silent-slot pre-#383 regression). Runspace
        # Write-Event output isn't reliably visible to Pester (per the
        # lumpy-records test caveat above), so an "observed periodic emit"
        # assertion isn't feasible here.
        #
        # PoolSize must be >= 2 here: Split-WorkItems with SliceCount=1
        # silently unwraps the single inner List (PowerShell array-return
        # quirk), producing N single-item dispatches instead of 1 chunk of
        # N items, so the periodic gate would never have a chance to fire.
        # The unwrap is a pre-existing slicer behavior, out of scope for
        # this fix; the assertion below relies on PoolSize=2 dodging it.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $shared = [hashtable]::Synchronized(@{})
        $skipIds = 1..250 | ForEach-Object { "s$_" }

        $result = Invoke-StagePool `
            -StageName       'fake_skip_periodic' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-Skippable' `
            -InputIds        $skipIds `
            -Context         @{ ProjectionFields = @('id'); AuthConfig = @{} } `
            -OutputDirectory $script:outDir `
            -RunId           'skip-periodic-run' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        2 `
            -ApiFamily       'graph' `
            -FlushInterval   100 `
            -ProgressShared  $shared `
            -WriteRecords

        $result.RecordCount  | Should -Be 0
        $result.SkippedCount | Should -Be 250
        $result.FailedCount  | Should -Be 0
        @($result.Errors).Count | Should -Be 0
        # Two slices (125 items each via round-robin). Each slot's final
        # value reflects that slice's input count; aggregate skipped=250.
        $slotKeys = @($shared.Keys | Where-Object { $_ -like 'fake_skip_periodic/*' })
        $slotKeys.Count | Should -Be 2
        foreach ($k in $slotKeys) {
            [int]$shared[$k].records_so_far  | Should -Be 0
            [int]$shared[$k].items_processed | Should -Be 125
            [int]$shared[$k].items_skipped   | Should -Be 125
            [int]$shared[$k].items_failed    | Should -Be 0
        }
    }

    It 'prereq stage (WriteRecords=$false): boundary tick advances ProgressShared via EmittedIds (#373)' {
        # Pre-#373: $processed only incremented inside `if ($WriteRecords)`, so
        # pool stages running as #362 prereqs (no requested entity claims them)
        # never crossed a FlushInterval boundary and never wrote to
        # ProgressShared. Live evidence: team_channels prereq stuck at
        # records_so_far=0 for 30 min on madev2 run 5e2f7e5db443. Production
        # fetchers (Get-TeamChannels, Get-TeamsRoot) gate $Writer.WriteRecord
        # on $Context.WriteRecords but always call $Writer.EmitId, so for the
        # prereq path TotalWritten stays 0 and the right per-item work signal
        # is EmittedIds.Count. Get-LumpyPrereq mirrors that shape.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $shared = [hashtable]::Synchronized(@{})
        $prereqOut = Join-Path $script:tempDir.FullName "prereq_$(Get-Random)"
        New-Item -ItemType Directory -Path $prereqOut -Force | Out-Null

        $result = Invoke-StagePool `
            -StageName       'fake_prereq' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-LumpyPrereq' `
            -InputIds        @('250') `
            -Context         @{ ProjectionFields = @('id','payload'); AuthConfig = @{} } `
            -OutputDirectory $prereqOut `
            -RunId           'prereq-run' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        1 `
            -ApiFamily       'graph' `
            -FlushInterval   100 `
            -ProgressShared  $shared

        @($result.Errors).Count | Should -Be 0
        # Per item: Get-LumpyPrereq calls EmitId 250 times, never WriteRecord.
        # $sw.TotalWritten = 0; $sw.EmittedIds.Count = 250. The else-branch
        # advances $processed by 250; the periodic flush (250 >= 0+100) fires
        # and writes the slot with the full count.
        $shared.ContainsKey('fake_prereq/0') | Should -BeTrue
        [int]$shared['fake_prereq/0'].records_so_far | Should -Be 250
        # #383: single input '250' = 1 item processed.
        [int]$shared['fake_prereq/0'].items_processed | Should -Be 1
    }

    It 'prereq stage (WriteRecords=$false): final tick fires for sub-boundary chunk (#373)' {
        # Final-tick coverage on the prereq code path. With $processed=78 and
        # FlushInterval=100, the periodic flush condition (78 >= 0+100) is
        # false → no boundary tick. The finally block's `$processed -gt
        # $lastFlushAt` (78 > 0) then fires and lands the true count. Pre-fix
        # $processed stayed at 0 and BOTH gates rejected → no slot at all.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $shared = [hashtable]::Synchronized(@{})
        $prereqOut = Join-Path $script:tempDir.FullName "prereq_tail_$(Get-Random)"
        New-Item -ItemType Directory -Path $prereqOut -Force | Out-Null

        $result = Invoke-StagePool `
            -StageName       'fake_prereq_tail' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-LumpyPrereq' `
            -InputIds        @('78') `
            -Context         @{ ProjectionFields = @('id','payload'); AuthConfig = @{} } `
            -OutputDirectory $prereqOut `
            -RunId           'prereq-tail-run' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        1 `
            -ApiFamily       'graph' `
            -FlushInterval   100 `
            -ProgressShared  $shared

        @($result.Errors).Count | Should -Be 0
        $shared.ContainsKey('fake_prereq_tail/0') | Should -BeTrue
        [int]$shared['fake_prereq_tail/0'].records_so_far | Should -Be 78
        # #383: single input '78' = 1 item processed.
        [int]$shared['fake_prereq_tail/0'].items_processed | Should -Be 1
    }

    It 'prereq stage (WriteRecords=$false): zero-record fetch still emits slot when items_processed advanced (#383)' {
        # See the parallel #383 case in the WriteRecords path above. Same gate
        # change: a prereq slice that ran but emitted nothing now writes a
        # slot with records=0, items_processed=1 instead of staying invisible.
        $stubConnect = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $shared = [hashtable]::Synchronized(@{})
        $prereqOut = Join-Path $script:tempDir.FullName "prereq_empty_$(Get-Random)"
        New-Item -ItemType Directory -Path $prereqOut -Force | Out-Null

        $result = Invoke-StagePool `
            -StageName       'fake_prereq_empty' `
            -ModulePath      $script:fakeModule `
            -FunctionName    'Get-NoRecords' `
            -InputIds        @('e1','e2','e3') `
            -Context         @{ ProjectionFields = @('id'); AuthConfig = @{} } `
            -OutputDirectory $prereqOut `
            -RunId           'prereq-empty-run' `
            -Tenant          'fab' `
            -AuthModulePath  $stubConnect `
            -SourceType      'tenant' `
            -SourceKey       'fab' `
            -PoolSize        3 `
            -ApiFamily       'graph' `
            -FlushInterval   100 `
            -ProgressShared  $shared

        @($result.Errors).Count | Should -Be 0
        $slotKeys = @($shared.Keys | Where-Object { $_ -like 'fake_prereq_empty/*' })
        $slotKeys.Count | Should -Be 3
        foreach ($k in $slotKeys) {
            [int]$shared[$k].records_so_far | Should -Be 0
            [int]$shared[$k].items_processed | Should -Be 1
        }
        $result.ItemsProcessed | Should -Be 3
    }
}

Describe 'End-of-chunk final tick gate (logic-level, #328)' {
    # Direct unit test of the gate inside the new finally-block emit:
    #   if ($processed -gt $lastFlushAt) { ...emit... }
    # Mirrors the FlushInterval threshold tracker pattern above. Same caveat
    # applies — runspace Write-Event output isn't reliably visible to Pester,
    # so the end-to-end cases above assert on ProgressShared while this case
    # locks down the gate predicate.
    BeforeAll {
        function script:Test-EmitFinalTick {
            param([int]$Processed, [int]$LastFlushAt)
            return ($Processed -gt $LastFlushAt)
        }
    }

    It 'fires when tail records advanced processed past the last boundary' {
        # 25078 processed, last boundary tick at 25000 (250 × 100) → tail of
        # 78 records. Pre-#328 this was the freeze; now the final tick fires.
        Test-EmitFinalTick -Processed 25078 -LastFlushAt 25000 | Should -BeTrue
    }

    It 'does not fire on a round-multiple chunk where boundary tick already covered the total' {
        # FlushInterval=100, processed=200 → boundary at 200 already advanced
        # lastFlushAt to 200; the gate must reject the redundant final emit.
        Test-EmitFinalTick -Processed 200 -LastFlushAt 200 | Should -BeFalse
    }

    It 'does not fire on an empty chunk (zero processed, zero lastFlushAt)' {
        Test-EmitFinalTick -Processed 0 -LastFlushAt 0 | Should -BeFalse
    }
}

Describe 'FlushInterval threshold tracker (logic-level)' {
    # Direct unit test of the cadence rule. Mirrors the inline check in
    # WorkerPool.psm1's StageDispatchBlock: emit when EITHER counter
    # crosses $lastFlushAt/$lastItemsFlushAt + $FlushInterval. Locking it
    # down here gives us regression coverage even though we can't easily
    # intercept Write-Host calls from runspaces in Pester.
    BeforeAll {
        function script:Test-EmitFlush {
            param(
                [int]$Processed,
                [int]$LastFlushAt,
                [int]$FlushInterval,
                [int]$ItemsProcessed = 0,
                [int]$LastItemsFlushAt = 0
            )
            return ($FlushInterval -gt 0 -and (
                $Processed      -ge ($LastFlushAt      + $FlushInterval) -or
                $ItemsProcessed -ge ($LastItemsFlushAt + $FlushInterval)))
        }
    }

    It 'fires when processed first crosses the next threshold' {
        Test-EmitFlush -Processed 100 -LastFlushAt 0 -FlushInterval 100 | Should -BeTrue
    }

    It 'does not fire when processed has not yet reached the next threshold' {
        Test-EmitFlush -Processed 99 -LastFlushAt 0 -FlushInterval 100 | Should -BeFalse
    }

    It 'still fires when an item lumps processed past multiple thresholds at once' {
        # The original modulo-based check would have been false here
        # (320 % 100 != 0) even though we've crossed 100/200/300.
        Test-EmitFlush -Processed 320 -LastFlushAt 0 -FlushInterval 100 | Should -BeTrue
    }

    It 'fires again on the next threshold crossing after lastFlushAt advances' {
        # After emitting at 320, lastFlushAt becomes 320 — next emit
        # requires processed >= 420, NOT >= 400 (modulo behavior).
        Test-EmitFlush -Processed 410 -LastFlushAt 320 -FlushInterval 100 | Should -BeFalse
        Test-EmitFlush -Processed 420 -LastFlushAt 320 -FlushInterval 100 | Should -BeTrue
    }

    It 'is disabled when FlushInterval is 0' {
        Test-EmitFlush -Processed 100 -LastFlushAt 0 -FlushInterval 0 | Should -BeFalse
    }

    It 'fires on the items leg when records leg is silent (all-Skippable / all-NonRetryable, #399)' {
        # Records leg: 0 < 0+100 → false. Items leg: 100 >= 0+100 → true.
        # Pre-#399 the items leg didn't exist on the periodic gate so this
        # case never emitted mid-chunk; the run-state blob froze for the
        # entire chunk duration (40+ min on entra_user_managers).
        Test-EmitFlush -Processed 0 -LastFlushAt 0 -FlushInterval 100 `
            -ItemsProcessed 100 -LastItemsFlushAt 0 | Should -BeTrue
    }

    It 'items leg does not fire under the threshold (#399)' {
        Test-EmitFlush -Processed 0 -LastFlushAt 0 -FlushInterval 100 `
            -ItemsProcessed 99 -LastItemsFlushAt 0 | Should -BeFalse
    }

    It 'items leg does not fire when a prior emit already advanced lastItemsFlushAt past current items (#399)' {
        # After a records-leg fire at processed=100, items=20: both
        # watermarks advance (lastFlushAt=100, lastItemsFlushAt=20).
        # Now at processed=110, items=30 — both legs should be false
        # because we're under both thresholds. Validates "advancing
        # both watermarks together prevents the items leg from double-
        # firing on normal records-driven chunks".
        Test-EmitFlush -Processed 110 -LastFlushAt 100 -FlushInterval 100 `
            -ItemsProcessed 30 -LastItemsFlushAt 20 | Should -BeFalse
    }

    It 'fires when both legs cross simultaneously (single fire, not double) (#399)' {
        # records=100, items=100, both watermarks at 0 — gate is OR, so
        # the single if-block fires once. The block advances both
        # watermarks to current; next call would need processed >= 200
        # OR items >= 200. Encoded here as the predicate evaluating to
        # true exactly once for these inputs.
        Test-EmitFlush -Processed 100 -LastFlushAt 0 -FlushInterval 100 `
            -ItemsProcessed 100 -LastItemsFlushAt 0 | Should -BeTrue
    }
}

# --- OOP worker-pool tests (#484) ---------------------------------------
#
# Invoke-StagePoolOutOfProcess and Invoke-StagePoolBatchOutOfProcess spawn
# real pwsh child processes. Tests below exercise the empty-input fast
# path (no children spawned) and the env-var cap logic without paying the
# multi-second child spawn cost; end-to-end with real children is covered
# by branch-env smoke runs. The cap-via-env-var is the key correctness
# property to test deterministically because it gates how memory scales.

Describe 'Invoke-StagePoolOutOfProcess - empty input' {
    It 'returns zero result and spawns no children when InputIds is empty' {
        $authPath = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $modulePath = Join-Path $PSScriptRoot 'StubConnect.psm1'  # any module path; not loaded for empty input

        $result = Invoke-StagePoolOutOfProcess `
            -StageName 'test_stage' `
            -ModulePath $modulePath `
            -FunctionName 'Connect-Service' `
            -InputIds @() `
            -Context @{} `
            -OutputDirectory $TestDrive `
            -RunId 'test-run' `
            -Tenant 'testtenant' `
            -AuthModulePath $authPath `
            -SourceType 'tenant' `
            -SourceKey 'testkey' `
            -ApiFamily 'exo'

        $result.RecordCount   | Should -Be 0
        $result.SliceCount    | Should -Be 0
        $result.SkippedCount  | Should -Be 0
        $result.FailedCount   | Should -Be 0
        @($result.Errors).Count     | Should -Be 0
        @($result.EmittedIds).Count | Should -Be 0
    }
}

Describe 'Invoke-StagePoolBatchOutOfProcess - empty input across multiple units' {
    It 'returns zero results per stage when all units have empty input' {
        $authPath   = Join-Path $PSScriptRoot 'StubConnect.psm1'
        $modulePath = Join-Path $PSScriptRoot 'StubConnect.psm1'

        $units = @(
            @{ StageName = 'stage_a'; FunctionName = 'Connect-Service'; InputIds = @(); OutputDirectory = $TestDrive; Entity = 'entity_a'; WriteRecords = $true }
            @{ StageName = 'stage_b'; FunctionName = 'Connect-Service'; InputIds = @(); OutputDirectory = $TestDrive; Entity = 'entity_b'; WriteRecords = $true }
        )

        $results = Invoke-StagePoolBatchOutOfProcess `
            -StageUnits $units `
            -ModulePath $modulePath `
            -Context @{} `
            -RunId 'test-run' `
            -Tenant 'testtenant' `
            -AuthModulePath $authPath `
            -SourceType 'tenant' `
            -SourceKey 'testkey' `
            -ApiFamily 'exo'

        $results.Keys | Should -Contain 'stage_a'
        $results.Keys | Should -Contain 'stage_b'
        $results['stage_a'].RecordCount | Should -Be 0
        $results['stage_a'].SliceCount  | Should -Be 0
        $results['stage_b'].RecordCount | Should -Be 0
        $results['stage_b'].SliceCount  | Should -Be 0
    }
}

Describe 'WORKER_POOL_OOP_MAX env var cap logic' {
    # The cap is computed inline inside the function; expose the same
    # logic via InModuleScope so we can test the integer-coercion and
    # fallback default without spawning children.
    It 'caps effective PoolSize at the env var value when set' {
        InModuleScope WorkerPool {
            $env:WORKER_POOL_OOP_MAX = '3'
            try {
                $containerCap = if ($env:WORKER_POOL_OOP_MAX) { [int]$env:WORKER_POOL_OOP_MAX } else { 5 }
                $containerCap | Should -Be 3
                [Math]::Min(10, $containerCap) | Should -Be 3
                [Math]::Min(2,  $containerCap) | Should -Be 2  # smaller PoolSize still wins
            } finally {
                Remove-Item Env:WORKER_POOL_OOP_MAX -ErrorAction SilentlyContinue
            }
        }
    }

    It 'falls back to 5 when env var is not set' {
        InModuleScope WorkerPool {
            Remove-Item Env:WORKER_POOL_OOP_MAX -ErrorAction SilentlyContinue
            $containerCap = if ($env:WORKER_POOL_OOP_MAX) { [int]$env:WORKER_POOL_OOP_MAX } else { 5 }
            $containerCap | Should -Be 5
        }
    }

    It 'allows raising the cap above 5 for larger workload profiles' {
        InModuleScope WorkerPool {
            $env:WORKER_POOL_OOP_MAX = '10'
            try {
                $containerCap = if ($env:WORKER_POOL_OOP_MAX) { [int]$env:WORKER_POOL_OOP_MAX } else { 5 }
                [Math]::Min(15, $containerCap) | Should -Be 10
            } finally {
                Remove-Item Env:WORKER_POOL_OOP_MAX -ErrorAction SilentlyContinue
            }
        }
    }
}

Describe 'OOP chunk-plan math' {
    # The chunk-splitter inside Invoke-StagePoolOutOfProcess is inline.
    # Reproduce the same logic here to assert boundary behavior — guards
    # against off-by-one regressions in the for-loop bounds.
    It 'splits N items into ceil(N/ChunkSize) chunks of correct sizes' {
        function Test-ChunkSplit {
            param([string[]]$Items, [int]$ChunkSize)
            $chunks = [System.Collections.Generic.List[object]]::new()
            for ($i = 0; $i -lt $Items.Count; $i += $ChunkSize) {
                $end = [Math]::Min($i + $ChunkSize - 1, $Items.Count - 1)
                $chunks.Add(@($Items[$i..$end]))
            }
            return $chunks
        }

        # 5 items, chunk 2 → @(2, 2, 1)
        $chunks = Test-ChunkSplit -Items @('a','b','c','d','e') -ChunkSize 2
        @($chunks).Count | Should -Be 3
        @($chunks[0]).Count | Should -Be 2
        @($chunks[1]).Count | Should -Be 2
        @($chunks[2]).Count | Should -Be 1

        # 4 items, chunk 2 → @(2, 2) — no short tail
        $chunks = Test-ChunkSplit -Items @('a','b','c','d') -ChunkSize 2
        @($chunks).Count | Should -Be 2
        @($chunks[0]).Count | Should -Be 2
        @($chunks[1]).Count | Should -Be 2

        # 1 item, chunk 5 → @(1) — small input
        $chunks = Test-ChunkSplit -Items @('only') -ChunkSize 5
        @($chunks).Count | Should -Be 1
        @($chunks[0]).Count | Should -Be 1
    }
}
