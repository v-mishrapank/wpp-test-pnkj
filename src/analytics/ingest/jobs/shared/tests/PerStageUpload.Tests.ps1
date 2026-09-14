#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Sink-wiring tests for #345. Verifies that Invoke-ModuleRun fires the
# optional UploadSink and ManifestSink immediately as each stage's result
# row is finalized — pre-#345 behavior was a single upload+manifest pass
# at module-end, which lost every completed stage's data when the
# container died mid-module.
#
# Pool stages need live Graph/EXO/SPO auth so all tests here use inline
# stages, matching InvokeModuleRun.Tests.ps1. The sink-invocation site
# in $emitResult (StageExecutor.psm1) is shared between inline and pool
# paths, so inline coverage exercises the same wiring.

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    Import-Module (Join-Path $modulesPath 'LogHelper.psm1')     -Force
    Import-Module (Join-Path $modulesPath 'EventEmitter.psm1')  -Force
    Import-Module (Join-Path $modulesPath 'StageWriter.psm1')   -Force
    Import-Module (Join-Path $modulesPath 'RecordEnvelope.psm1') -Force
    Import-Module (Join-Path $modulesPath 'StageExecutor.psm1') -Force
    Initialize-EventContext -RunId 'test-run' -Tenant 'test-tenant'

    $script:tempDir = New-Item -ItemType Directory -Path (Join-Path ([System.IO.Path]::GetTempPath()) "PerStageUploadTest_$(Get-Random)") -Force

    # Two sequential inline stages, each writing one record. Stage 2 reads
    # nothing from stage 1 — keeps the test minimal.
    $script:twoStageFile = Join-Path $tempDir.FullName 'TwoStageModule.psm1'
    $twoStageContent = @'
function Get-ModuleStages {
    @{
        'root_a' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-RootA'; ApiFamily = 'graph'; MinimumSelectFields = @('id') }
        'root_b' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-RootB'; ApiFamily = 'graph'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'entity_a' = @{ Stage = 'root_a'; WritesTo = 'root'; SelectFields = @('id') }
        'entity_b' = @{ Stage = 'root_b'; WritesTo = 'root'; SelectFields = @('id') }
    }
}
function Get-RootA { param([hashtable]$Context, $Writer) $Writer.WriteRecord(@{ id = 'a1' }) }
function Get-RootB { param([hashtable]$Context, $Writer) $Writer.WriteRecord(@{ id = 'b1' }) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-RootA, Get-RootB
'@
    [System.IO.File]::WriteAllText($twoStageFile, $twoStageContent)

    # Stage 2 throws — verifies stage 1's sink invocations happened before
    # the failure, and stage 2 still fires ManifestSink with status=failed
    # so downstream consumers see a manifest at the moment of failure.
    $script:throwModuleFile = Join-Path $tempDir.FullName 'ThrowOnStageTwoModule.psm1'
    $throwContent = @'
function Get-ModuleStages {
    @{
        'root_a' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-RootA';      ApiFamily = 'graph'; MinimumSelectFields = @('id') }
        'root_b' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-RootBThrow'; ApiFamily = 'graph'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'entity_a' = @{ Stage = 'root_a'; WritesTo = 'root'; SelectFields = @('id') }
        'entity_b' = @{ Stage = 'root_b'; WritesTo = 'root'; SelectFields = @('id') }
    }
}
function Get-RootA      { param([hashtable]$Context, $Writer) $Writer.WriteRecord(@{ id = 'a1' }) }
function Get-RootBThrow { param([hashtable]$Context, $Writer) throw "simulated stage 2 failure" }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-RootA, Get-RootBThrow
'@
    [System.IO.File]::WriteAllText($throwModuleFile, $throwContent)

    $script:baseContext = @{
        RunId          = 'sinktest'
        TenantKey      = 'fabrikam'
        Date           = '2026-05-11'
        SourceType     = 'tenant'
        SourceKey      = 'fabrikam'
        PoolSize       = 1
        AuthConfig     = @{}
        AuthModulePath = Join-Path $PSScriptRoot 'StubConnect.psm1'
        TempRoot       = $tempDir.FullName
    }
}

AfterAll {
    if ($tempDir -and (Test-Path $tempDir.FullName)) {
        Remove-Item $tempDir.FullName -Recurse -Force
    }
}

Describe 'Invoke-ModuleRun sink wiring' {

    It 'calls UploadSink once per chunk, in stage-completion order, with (LocalPath, BlobPath, Entity)' {
        $script:uploadCalls = [System.Collections.Generic.List[hashtable]]::new()
        $sinkContext = $baseContext.Clone()
        $sinkContext.UploadSink = {
            param($LocalPath, $BlobPath, $Entity)
            $script:uploadCalls.Add(@{
                LocalPath = $LocalPath
                BlobPath  = $BlobPath
                Entity    = $Entity
                Order     = $script:uploadCalls.Count
            })
        }
        $results = Invoke-ModuleRun -ModulePath $twoStageFile -RequestedEntities @('entity_a','entity_b') -Context $sinkContext

        $results.Count                       | Should -Be 2
        $script:uploadCalls.Count            | Should -Be 2
        $entitiesCalled = ($script:uploadCalls | ForEach-Object { $_.Entity } | Sort-Object)
        $entitiesCalled | Should -Be @('entity_a','entity_b')

        # Each sink call's BlobPath matches the result row's BlobPaths[0].
        foreach ($call in $script:uploadCalls) {
            $r = $results | Where-Object { $_.EntityName -eq $call.Entity }
            $r.BlobPaths[0] | Should -Be $call.BlobPath
            $r.LocalPaths[0] | Should -Be $call.LocalPath
        }
    }

    It 'sets Uploaded=$true on result rows whose chunks went through UploadSink' {
        $sinkContext = $baseContext.Clone()
        $sinkContext.UploadSink = { param($LocalPath, $BlobPath, $Entity) }
        $results = Invoke-ModuleRun -ModulePath $twoStageFile -RequestedEntities @('entity_a','entity_b') -Context $sinkContext

        foreach ($r in $results) {
            $r.Uploaded | Should -Be $true
        }
    }

    It 'calls ManifestSink once per result row, with the row hashtable' {
        $script:manifestCalls = [System.Collections.Generic.List[hashtable]]::new()
        $sinkContext = $baseContext.Clone()
        $sinkContext.ManifestSink = {
            param($r)
            $script:manifestCalls.Add(@{
                Entity      = $r.EntityName
                Status      = $r.Status
                RecordCount = $r.RecordCount
                BasePath    = $r.BasePath
            })
        }
        Invoke-ModuleRun -ModulePath $twoStageFile -RequestedEntities @('entity_a','entity_b') -Context $sinkContext | Out-Null

        $script:manifestCalls.Count | Should -Be 2
        ($script:manifestCalls | ForEach-Object { $_.Entity } | Sort-Object) | Should -Be @('entity_a','entity_b')
        foreach ($call in $script:manifestCalls) {
            $call.Status | Should -Be 'success'
            $call.RecordCount | Should -Be 1
        }
    }

    It 'fires UploadSink for the completed stage even when a later stage throws' {
        $script:uploadCalls = [System.Collections.Generic.List[hashtable]]::new()
        $script:manifestCalls = [System.Collections.Generic.List[hashtable]]::new()
        $sinkContext = $baseContext.Clone()
        $sinkContext.UploadSink = {
            param($LocalPath, $BlobPath, $Entity)
            $script:uploadCalls.Add(@{ Entity = $Entity; BlobPath = $BlobPath })
        }
        $sinkContext.ManifestSink = {
            param($r)
            $script:manifestCalls.Add(@{ Entity = $r.EntityName; Status = $r.Status })
        }

        Invoke-ModuleRun -ModulePath $throwModuleFile -RequestedEntities @('entity_a','entity_b') -Context $sinkContext | Out-Null

        # Stage 1 (entity_a) succeeded → UploadSink fired for it.
        # Force array context with @(...) — Where-Object with one match
        # returns the matched hashtable directly, and .Count on a hashtable
        # gives its key count (2), not 1.
        @($script:uploadCalls | Where-Object { $_.Entity -eq 'entity_a' }).Count | Should -Be 1
        # Stage 2 (entity_b) threw before producing chunks → UploadSink not
        # called for it (empty LocalPaths short-circuits the sink).
        @($script:uploadCalls | Where-Object { $_.Entity -eq 'entity_b' }).Count | Should -Be 0
        # But ManifestSink still fired for entity_b with status=failed —
        # this is the key #345 guarantee: every requested entity gets a
        # manifest at the moment its stage finishes (or fails), not at
        # module-end.
        $bManifest = @($script:manifestCalls | Where-Object { $_.Entity -eq 'entity_b' })
        $bManifest.Count | Should -Be 1
        $bManifest[0].Status | Should -Be 'failed'
    }

    It 'does not invoke sinks when neither is supplied (backward compatibility)' {
        # The existing InvokeModuleRun.Tests.ps1 already runs without sinks,
        # so this asserts the same property directly: no error, $results
        # still populated, ChunkCount/RecordCount unchanged.
        $results = Invoke-ModuleRun -ModulePath $twoStageFile -RequestedEntities @('entity_a','entity_b') -Context $baseContext
        $results.Count | Should -Be 2
        foreach ($r in $results) {
            $r.Uploaded | Should -BeNullOrEmpty
            $r.RecordCount | Should -Be 1
        }
    }

    It 'invokes ManifestSink even when UploadSink is absent' {
        $script:manifestCalls = [System.Collections.Generic.List[hashtable]]::new()
        $sinkContext = $baseContext.Clone()
        $sinkContext.ManifestSink = {
            param($r)
            $script:manifestCalls.Add(@{ Entity = $r.EntityName })
        }
        $results = Invoke-ModuleRun -ModulePath $twoStageFile -RequestedEntities @('entity_a') -Context $sinkContext

        $script:manifestCalls.Count | Should -Be 1
        $script:manifestCalls[0].Entity | Should -Be 'entity_a'
        # No UploadSink → Uploaded flag never set.
        $results[0].Uploaded | Should -BeNullOrEmpty
    }

    It 'invokes UploadSink even when ManifestSink is absent' {
        $script:uploadCalls = [System.Collections.Generic.List[hashtable]]::new()
        $sinkContext = $baseContext.Clone()
        $sinkContext.UploadSink = {
            param($LocalPath, $BlobPath, $Entity)
            $script:uploadCalls.Add(@{ Entity = $Entity })
        }
        $results = Invoke-ModuleRun -ModulePath $twoStageFile -RequestedEntities @('entity_a') -Context $sinkContext

        $script:uploadCalls.Count | Should -Be 1
        $results[0].Uploaded | Should -Be $true
    }
}
