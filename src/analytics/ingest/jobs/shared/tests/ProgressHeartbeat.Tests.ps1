#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Regression coverage for #318: the original timer-driven design crashed the
# pwsh process on the first tick because the timer callback ran on a .NET
# ThreadPool thread that had no DefaultRunspace. The fix went a step further:
# we removed the timer entirely and made the heartbeat single-threaded —
# stage transitions flush synchronously from the main thread, and intra-pool-
# stage progress is driven cooperatively from WorkerPool's wait loop.
#
# These tests cover the new design's contract:
#   - Stage transitions force a synchronous upload
#   - Invoke-HeartbeatFlush rate-limits to FlushSeconds
#   - Pool progress in $ProgressShared folds into stage records_so_far
#   - Stop-Heartbeat performs a forced final upload with terminal status

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    Import-Module (Join-Path $modulesPath 'LogHelper.psm1')         -Force
    Import-Module (Join-Path $modulesPath 'EntityRollup.psm1')      -Force
    Import-Module (Join-Path $modulesPath 'ProgressHeartbeat.psm1') -Force

    # Skip Stop-Heartbeat's exponential-backoff sleeps in tests. Production
    # values (5 attempts, ~30s cap) would make the retry tests painfully slow.
    Set-TerminalRetryForTesting

    function script:New-CapturingUpload {
        param([System.Collections.Generic.List[hashtable]]$Sink)
        return {
            param($StorageAccountUrl, $ContainerName, $BlobPath, $LocalFile)
            $Sink.Add(@{
                BlobPath = $BlobPath
                Json     = [System.IO.File]::ReadAllText($LocalFile)
            })
        }.GetNewClosure()
    }
}

Describe 'ProgressHeartbeat (single-threaded design, #318)' {

    Context 'stage-transition forced uploads' {
        It 'flushes synchronously on Set-StageRunning, Set-StageCompleted, Stop-Heartbeat' {
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-318-trans' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('graph_users') `
                -UploadFunction $upload `
                -FlushSeconds 5

            # 1: initial forced upload from Start-Heartbeat
            $sink.Count | Should -Be 1

            Set-StageRunning -Entity 'graph_users' -Stage 'fetch' -InputCount 100
            # 2: forced upload from Set-StageRunning
            $sink.Count | Should -Be 2

            Set-StageCompleted -Entity 'graph_users' -Stage 'fetch' -RecordsSoFar 100 -DurationMs 1234
            # 3: forced upload from Set-StageCompleted
            $sink.Count | Should -Be 3

            Stop-Heartbeat -FinalStatus 'completed'
            # 4: forced upload from Stop-Heartbeat
            $sink.Count | Should -Be 4

            $finalPayload = $sink[$sink.Count - 1].Json | ConvertFrom-Json
            $finalPayload.run_status                          | Should -Be 'completed'
            $finalPayload.entities[0].status        | Should -Be 'running'
            $finalPayload.entities[0].records_so_far | Should -Be 100
            # #385: schema bumped to 3 (durable past finalization, load-bearing).
            $finalPayload.schema_version | Should -Be 4
        }

        It 'writes the run-state blob under _dispatcher/run_state/ (#385)' {
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-385-path' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('graph_users') `
                -UploadFunction $upload `
                -FlushSeconds 5

            try {
                $sink[0].BlobPath | Should -Be '_dispatcher/run_state/rt-385-path/tenant-a/graph-ingest.json'
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }
    }

    Context 'Stop-Heartbeat terminal flush (#385)' {
        It 'retries the terminal upload up to 5 times on transient failure' {
            # Stop-Heartbeat is now load-bearing — the dispatcher reads
            # run_status from the final blob. Mirror the bounded-retry
            # contract that the deleted manifest-summary writer used to
            # provide. Counter goes through a List so the closure and the
            # test scope share state (closure's $script: is its own scope).
            $counter = [System.Collections.Generic.List[int]]::new()
            $upload = {
                param($StorageAccountUrl, $ContainerName, $BlobPath, $LocalFile)
                $counter.Add($counter.Count + 1)
                if ($counter.Count -lt 3) {
                    throw [System.Net.WebException]::new("transient 503 on attempt $($counter.Count)")
                }
            }.GetNewClosure()

            Start-Heartbeat `
                -RunId 'rt-385-retry' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('graph_users') `
                -UploadFunction $upload `
                -FlushSeconds 5

            # Reset after Start-Heartbeat's initial upload so we measure only
            # Stop-Heartbeat's retries.
            $counter.Clear()
            Stop-Heartbeat -FinalStatus 'completed'

            # 1 failed + 1 failed + 1 succeeded = 3 attempts.
            $counter.Count | Should -Be 3
        }

        It 'gives up after 5 failed terminal-upload attempts and does not throw' {
            $counter = [System.Collections.Generic.List[int]]::new()
            $upload = {
                param($StorageAccountUrl, $ContainerName, $BlobPath, $LocalFile)
                $counter.Add($counter.Count + 1)
                throw [System.Net.WebException]::new("persistent failure on attempt $($counter.Count)")
            }.GetNewClosure()

            Start-Heartbeat `
                -RunId 'rt-385-giveup' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('graph_users') `
                -UploadFunction $upload `
                -FlushSeconds 5

            $counter.Clear()
            { Stop-Heartbeat -FinalStatus 'failed' } | Should -Not -Throw
            $counter.Count | Should -Be 5
        }
    }

    Context 'Invoke-HeartbeatFlush rate limiting' {
        It 'collapses rapid calls to at most one upload per FlushSeconds' {
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-318-rate' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('graph_users') `
                -UploadFunction $upload `
                -FlushSeconds 2

            try {
                # 1 initial forced upload from Start-Heartbeat. Set-StageRunning
                # also forces (so observers see the new stage immediately).
                Set-StageRunning -Entity 'graph_users' -Stage 'fetch' -InputCount 100
                $countAfterStart = $sink.Count    # 2

                # Hammer the cooperative flush — should be rate-limited to 0
                # additional uploads within the FlushSeconds window since
                # Set-StageRunning just uploaded.
                $progress = Get-ProgressShared
                for ($i = 0; $i -lt 20; $i++) {
                    $progress["fetch/0"] = @{
                        stage = 'fetch'; slice_index = 0; records_so_far = $i + 1
                        updated_at = [DateTime]::UtcNow
                    }
                    Invoke-HeartbeatFlush
                    Start-Sleep -Milliseconds 50    # ~1s of hammering
                }
                $sink.Count | Should -Be $countAfterStart

                # Wait past the rate-limit window and try once more — now it
                # should fold ProgressShared and upload.
                Start-Sleep -Seconds 2
                $progress["fetch/0"] = @{
                    stage = 'fetch'; slice_index = 0; records_so_far = 99
                    updated_at = [DateTime]::UtcNow
                }
                Invoke-HeartbeatFlush
                $sink.Count | Should -Be ($countAfterStart + 1)

                $latest = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                $latest.entities[0].records_so_far | Should -Be 99
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }
    }

    Context 'pool progress fold' {
        It 'sums max(records_so_far) across slices for the active stage' {
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-318-fold' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('graph_users') `
                -UploadFunction $upload `
                -FlushSeconds 1

            try {
                Set-StageRunning -Entity 'graph_users' -Stage 'fetch' -InputCount 1000
                $progress = Get-ProgressShared
                $progress["fetch/0"] = @{ stage='fetch'; slice_index=0; records_so_far=120; items_processed=80;  items_failed=2; items_skipped=1; updated_at=[DateTime]::UtcNow }
                $progress["fetch/1"] = @{ stage='fetch'; slice_index=1; records_so_far=250; items_processed=130; items_failed=0; items_skipped=4; updated_at=[DateTime]::UtcNow }
                $progress["fetch/2"] = @{ stage='fetch'; slice_index=2; records_so_far=80;  items_processed=40;  items_failed=1; items_skipped=0; updated_at=[DateTime]::UtcNow }

                # Wait past the rate-limit window then flush.
                Start-Sleep -Seconds 1
                Invoke-HeartbeatFlush
                $latest = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                $latest.entities[0].records_so_far | Should -Be 450
                # #383: items_* fold the same way — sum-of-max per slice.
                $latest.entities[0].items_processed | Should -Be 250
                $latest.entities[0].items_failed    | Should -Be 3
                $latest.entities[0].items_skipped   | Should -Be 5
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }

        It 'leaves items_* null on inline stages whose ProgressShared slots omit them (#383)' {
            # Schema-1 worker compatibility leg: if a slot in ProgressShared
            # carries records_so_far but no items_* keys (e.g. an older
            # container running concurrently mid-rolling-deploy), the fold
            # must update records_so_far and leave items_* alone. This
            # exercises the ContainsKey guard in Invoke-HeartbeatProjectAndUpload.
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-383-schema1-compat' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('graph_users') `
                -UploadFunction $upload `
                -FlushSeconds 1

            try {
                # Use a stage name unique to this test — the synchronized
                # ProgressShared hashtable is module-scoped and persists
                # across tests, so reusing 'fetch' would fold in leftover
                # slots from the previous case.
                Set-StageRunning -Entity 'graph_users' -Stage 'schema1_compat_fetch' -InputCount 100
                $progress = Get-ProgressShared
                # Schema-1 slot: no items_* keys.
                $progress["schema1_compat_fetch/0"] = @{ stage='schema1_compat_fetch'; slice_index=0; records_so_far=42; updated_at=[DateTime]::UtcNow }

                Start-Sleep -Seconds 1
                Invoke-HeartbeatFlush
                $latest = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                $latest.entities[0].records_so_far | Should -Be 42
                $latest.entities[0].items_processed | Should -BeNullOrEmpty
                $latest.entities[0].items_failed    | Should -BeNullOrEmpty
                $latest.entities[0].items_skipped   | Should -BeNullOrEmpty
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }

        It 'tolerates [ordered]@{} slot writes from WorkerPool without throwing (#383 regression)' {
            # Live-incident regression on branch run ddf3ad9d1b77 (2026-05-12):
            # WorkerPool writes ProgressShared slots as [ordered]@{} for
            # stable JSON serialization order in the heartbeat blob. The
            # fold's `$slot.ContainsKey('items_processed')` threw silently
            # (OrderedDictionary has Contains but not ContainsKey), the
            # exception was swallowed by WorkerPool's cooperative-flush
            # try/catch, and the blob froze at Set-StageRunning's snapshot
            # for the entire run. Existing tests used @{} (Hashtable) which
            # masked the bug — Hashtable has both Contains and ContainsKey.
            # This test uses [ordered]@{} to match production writers.
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-383-ordered-slot' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('graph_users') `
                -UploadFunction $upload `
                -FlushSeconds 1

            try {
                Set-StageRunning -Entity 'graph_users' -Stage 'ordered_slot_fetch' -InputCount 1000
                $progress = Get-ProgressShared
                # Production shape: [ordered]@{} not @{}. Different field
                # order than other tests to catch ordering bugs too.
                $progress["ordered_slot_fetch/0"] = [ordered]@{
                    stage           = 'ordered_slot_fetch'
                    slice_index     = 0
                    items_processed = 50
                    items_failed    = 1
                    items_skipped   = 2
                    records_so_far  = 600
                    updated_at      = [DateTime]::UtcNow
                }
                $progress["ordered_slot_fetch/1"] = [ordered]@{
                    stage           = 'ordered_slot_fetch'
                    slice_index     = 1
                    items_processed = 70
                    items_failed    = 0
                    items_skipped   = 0
                    records_so_far  = 800
                    updated_at      = [DateTime]::UtcNow
                }

                Start-Sleep -Seconds 1
                Invoke-HeartbeatFlush
                $latest = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                $latest.entities[0].records_so_far  | Should -Be 1400
                $latest.entities[0].items_processed | Should -Be 120
                $latest.entities[0].items_failed    | Should -Be 1
                $latest.entities[0].items_skipped   | Should -Be 2
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }

        It 'Set-StageCompleted with explicit items_* overrides the fold (#383)' {
            # The pool result's aggregated counts are authoritative at stage
            # close — they reflect every chunk's contribution including the
            # final tick. Set-StageCompleted with explicit -ItemsProcessed
            # writes those values to the stage hashtable; subsequent flushes
            # don't reach the running-only fold path because the stage is
            # now 'completed'.
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-383-complete' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('graph_users') `
                -UploadFunction $upload `
                -FlushSeconds 5

            try {
                # Use a unique stage name to avoid cross-test contamination
                # from the module-scoped ProgressShared hashtable.
                Set-StageRunning -Entity 'graph_users' -Stage 'complete_test_fetch' -InputCount 100
                Set-StageCompleted -Entity 'graph_users' -Stage 'complete_test_fetch' `
                    -RecordsSoFar 9999 -DurationMs 1234 `
                    -ItemsProcessed 100 -ItemsFailed 3 -ItemsSkipped 2

                $latest = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                # No ProgressShared slots for this stage (inline stage),
                # so Set-StageCompleted applies counters directly.
                $latest.entities[0].duration_ms     | Should -Be 1234
                $latest.entities[0].records_so_far  | Should -Be 9999
                $latest.entities[0].items_processed | Should -Be 100
                $latest.entities[0].items_failed    | Should -Be 3
                $latest.entities[0].items_skipped   | Should -Be 2
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }

        It 'multi-stage entity accumulates fold data after Set-EntityCompleted (#504)' {
            # exo_group_members is fed by dg_members and ug_members. When
            # dg_members completes first, ManifestSink calls Set-EntityCompleted
            # which sets the rollup to a terminal status. The fold must still
            # apply ug_members ProgressShared data to the entity's counters.
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-504-multistage' `
                -TenantKey 'tenant-a' `
                -ContainerType 'exo-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('exo_group_members') `
                -UploadFunction $upload `
                -FlushSeconds 1

            try {
                $progress = Get-ProgressShared

                # Stage 1: dg_members runs and completes (small, 4 items).
                Set-StageRunning -Entity 'exo_group_members' -Stage 'dg_504' -InputCount 4
                $progress["dg_504/0"] = @{ stage='dg_504'; slice_index=0; records_so_far=6; items_processed=4; items_failed=0; items_skipped=0; updated_at=[DateTime]::UtcNow }
                Set-StageCompleted -Entity 'exo_group_members' -Stage 'dg_504' `
                    -RecordsSoFar 6 -DurationMs 500 `
                    -ItemsProcessed 4 -ItemsFailed 0 -ItemsSkipped 0

                # ManifestSink would call Set-EntityCompleted here, flipping the
                # rollup from running to success.
                Set-EntityCompleted -Entity 'exo_group_members' -Rollup (
                    New-EntityRollup -Name 'exo_group_members' -Status 'success' `
                        -RecordCount 6 -StartedAt (Get-Date -Format 'o') `
                        -CompletedAt (Get-Date -Format 'o'))

                # Stage 2: ug_members runs (large, 25k items). Rollup is now
                # terminal — pre-fix, the fold would skip this entity.
                Set-StageRunning -Entity 'exo_group_members' -Stage 'ug_504' -InputCount 25082
                $progress["ug_504/0"] = @{ stage='ug_504'; slice_index=0; records_so_far=160000; items_processed=12000; items_failed=0; items_skipped=0; updated_at=[DateTime]::UtcNow }
                $progress["ug_504/1"] = @{ stage='ug_504'; slice_index=1; records_so_far=159923; items_processed=13082; items_failed=1; items_skipped=0; updated_at=[DateTime]::UtcNow }

                Start-Sleep -Seconds 1
                Invoke-HeartbeatFlush

                $latest = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                $entity = $latest.entities[0]
                # Must sum both stages: dg(6) + ug(160000+159923) = 319929
                $entity.records_so_far  | Should -Be 319929
                # dg(4) + ug(12000+13082) = 25086
                $entity.items_processed | Should -Be 25086
                $entity.items_failed    | Should -Be 1
                $entity.items_skipped   | Should -Be 0
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }
    }

    Context 'prerequisite entities (#362)' {
        It 'Set-PrerequisiteStageRunning/Completed transition status and force a flush' {
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-362-transition' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('channel_members') `
                -UploadFunction $upload `
                -FlushSeconds 5

            try {
                $countBefore = $sink.Count

                Set-PrerequisiteStageRunning -Stage 'team_channels' -InputCount 63000
                $sink.Count | Should -Be ($countBefore + 1)
                $running = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                ($running.prerequisite_entities | Where-Object { $_.name -eq 'team_channels' }).status | Should -Be 'running'
                ($running.prerequisite_entities | Where-Object { $_.name -eq 'team_channels' }).input_count | Should -Be 63000

                Set-PrerequisiteStageCompleted -Stage 'team_channels' -RecordsSoFar 63000 -DurationMs 840000
                $done = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                $entry = $done.prerequisite_entities | Where-Object { $_.name -eq 'team_channels' }
                $entry.status         | Should -Be 'success'
                $entry.records_so_far | Should -Be 63000
                $entry.duration_ms    | Should -Be 840000
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }

        It 'Set-PrerequisiteStageCompleted with per-item failures reports partial and surfaces errors' {
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-362-partial' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('channel_members') `
                -UploadFunction $upload `
                -FlushSeconds 5

            try {
                Set-PrerequisiteStageRunning -Stage 'team_channels' -InputCount 100

                Set-PrerequisiteStageCompleted -Stage 'team_channels' -RecordsSoFar 90 -DurationMs 1000 `
                    -Status 'partial' -ItemsProcessed 90 -ItemsFailed 10 -ItemsSkipped 0 `
                    -Errors @('item=abc NonRetryable: HTTP 400', 'item=def RetryExhausted: timeout')

                $done = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                $entry = $done.prerequisite_entities | Where-Object { $_.name -eq 'team_channels' }
                $entry.status       | Should -Be 'partial'
                $entry.items_failed | Should -Be 10
                @($entry.errors).Count | Should -Be 2
                $entry.errors[0]    | Should -Match 'NonRetryable'
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }

        It 'Set-PrerequisiteStageRunning lazily creates an entry on first transition' {
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-362-autoreg' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('channel_members') `
                -UploadFunction $upload `
                -FlushSeconds 5

            try {
                Set-PrerequisiteStageRunning -Stage 'team_channels' -InputCount 10
                $latest = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                $entry = $latest.prerequisite_entities | Where-Object { $_.name -eq 'team_channels' }
                $entry        | Should -Not -BeNullOrEmpty
                $entry.status | Should -Be 'running'
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }

        It 'folds pool ProgressShared into prereq stage records_so_far' {
            $sink = [System.Collections.Generic.List[hashtable]]::new()
            $upload = New-CapturingUpload -Sink $sink

            Start-Heartbeat `
                -RunId 'rt-362-fold' `
                -TenantKey 'tenant-a' `
                -ContainerType 'graph-ingest' `
                -StorageAccountUrl 'https://example.dfs.core.windows.net' `
                -ContainerName 'analytics' `
                -WantedEntities @('channel_members') `
                -UploadFunction $upload `
                -FlushSeconds 1

            try {
                Set-PrerequisiteStageRunning -Stage 'team_channels' -InputCount 100
                $progress = Get-ProgressShared
                $progress["team_channels/0"] = @{ stage='team_channels'; slice_index=0; records_so_far=300; items_processed=50; items_failed=0; items_skipped=0; updated_at=[DateTime]::UtcNow }
                $progress["team_channels/1"] = @{ stage='team_channels'; slice_index=1; records_so_far=500; items_processed=50; items_failed=0; items_skipped=0; updated_at=[DateTime]::UtcNow }

                Start-Sleep -Seconds 1
                Invoke-HeartbeatFlush
                $latest = $sink[$sink.Count - 1].Json | ConvertFrom-Json
                $entry = $latest.prerequisite_entities | Where-Object { $_.name -eq 'team_channels' }
                $entry.records_so_far | Should -Be 800
                # #383: items_* fold the same way for prereq stages.
                $entry.items_processed | Should -Be 100
                $entry.items_failed    | Should -Be 0
                $entry.items_skipped   | Should -Be 0
            }
            finally {
                Stop-Heartbeat -FinalStatus 'completed'
            }
        }
    }

    Context 'no-op when not initialized' {
        It 'Invoke-HeartbeatFlush is a no-op before Start-Heartbeat' {
            # Force a clean module state by re-importing.
            $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
            Remove-Module ProgressHeartbeat -Force -ErrorAction SilentlyContinue
            Import-Module (Join-Path $modulesPath 'ProgressHeartbeat.psm1') -Force

            { Invoke-HeartbeatFlush } | Should -Not -Throw
            { Set-StageRunning -Entity 'graph_users' -Stage 'fetch' } | Should -Not -Throw
            { Set-PrerequisiteStageRunning -Stage 'team_channels' } | Should -Not -Throw
            { Set-PrerequisiteStageCompleted -Stage 'team_channels' } | Should -Not -Throw
            { Stop-Heartbeat } | Should -Not -Throw
        }
    }
}
