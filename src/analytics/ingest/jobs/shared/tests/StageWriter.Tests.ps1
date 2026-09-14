#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot '..' 'modules' 'StageWriter.psm1') -Force
}

Describe 'New-StageWriter basic behavior' {
    It 'exposes the documented properties and methods' {
        $w = New-StageWriter
        $w.PSObject.Properties.Name | Should -Contain 'Records'
        $w.PSObject.Properties.Name | Should -Contain 'EmittedIds'
        $w.PSObject.Properties.Name | Should -Contain 'TotalWritten'
        $w.PSObject.Methods.Name    | Should -Contain 'WriteRecord'
        $w.PSObject.Methods.Name    | Should -Contain 'EmitId'
        $w.PSObject.Methods.Name    | Should -Contain 'Flush'
    }

    It 'appends to Records on WriteRecord' {
        $w = New-StageWriter
        $w.WriteRecord(@{ id = '1' })
        $w.WriteRecord(@{ id = '2' })
        $w.Records.Count | Should -Be 2
        $w.TotalWritten  | Should -Be 2
    }

    It 'appends to EmittedIds on EmitId with tags' {
        $w = New-StageWriter
        $w.EmitId('a', @{ kind = 'x' })
        $w.EmitId('b', $null)
        $w.EmittedIds.Count | Should -Be 2
        $w.EmittedIds[0].Id | Should -Be 'a'
        $w.EmittedIds[0].Tags.kind | Should -Be 'x'
        $w.EmittedIds[1].Tags | Should -Be $null
    }

    It 'does not cross-count Records and EmittedIds' {
        $w = New-StageWriter
        $w.WriteRecord(@{ id = 1 })
        $w.EmitId('a', $null)
        $w.Records.Count    | Should -Be 1
        $w.EmittedIds.Count | Should -Be 1
        $w.TotalWritten     | Should -Be 1
    }

    It 'Flush is a no-op when no FlushCallback is supplied' {
        $w = New-StageWriter
        $w.WriteRecord(@{ id = 1 })
        $w.WriteRecord(@{ id = 2 })
        $w.Flush()
        $w.Records.Count | Should -Be 2
        $w.TotalWritten  | Should -Be 2
    }

    It 'Flush drains Records through the FlushCallback and clears the buffer' {
        $captured = [System.Collections.Generic.List[object]]::new()
        $w = New-StageWriter -FlushCallback {
            param($records)
            foreach ($r in $records) { $captured.Add($r) }
        }.GetNewClosure()
        $w.WriteRecord(@{ id = 1 })
        $w.WriteRecord(@{ id = 2 })
        $w.Flush()
        $captured.Count  | Should -Be 2
        $w.Records.Count | Should -Be 0
        $w.TotalWritten  | Should -Be 2
    }

    It 'Flush on empty buffer is a no-op' {
        $calls = [System.Collections.Generic.List[int]]::new()
        $w = New-StageWriter -FlushCallback { param($records) $calls.Add(1) }.GetNewClosure()
        $w.Flush()
        $calls.Count | Should -Be 0
    }
}

Describe 'New-StageWriter auto-flush' {
    It 'auto-flushes exactly at the threshold' {
        $batches = [System.Collections.Generic.List[int]]::new()
        $w = New-StageWriter -AutoFlushThreshold 3 -FlushCallback {
            param($records)
            $batches.Add($records.Count)
        }.GetNewClosure()

        1..2 | ForEach-Object { $w.WriteRecord(@{ i = $_ }) }
        $batches.Count | Should -Be 0   # not yet at threshold

        $w.WriteRecord(@{ i = 3 })
        $batches.Count | Should -Be 1
        $batches[0]    | Should -Be 3
    }

    It 'TotalWritten keeps running across multiple auto-flushes' {
        $batches = [System.Collections.Generic.List[int]]::new()
        $w = New-StageWriter -AutoFlushThreshold 2 -FlushCallback {
            param($records)
            $batches.Add($records.Count)
        }.GetNewClosure()

        1..7 | ForEach-Object { $w.WriteRecord(@{ i = $_ }) }
        $w.Flush()   # final drain
        ($batches | Measure-Object -Sum).Sum | Should -Be 7
        $w.TotalWritten | Should -Be 7
    }

    It 'auto-flush is disabled when AutoFlushThreshold is 0 (default)' {
        $batches = [System.Collections.Generic.List[int]]::new()
        $w = New-StageWriter -FlushCallback { param($records) $batches.Add(1) }.GetNewClosure()
        1..1000 | ForEach-Object { $w.WriteRecord(@{ i = $_ }) }
        $batches.Count | Should -Be 0
        $w.Records.Count | Should -Be 1000
    }

    It 'auto-flush without FlushCallback does not lose records' {
        # AutoFlushThreshold with no callback means Flush is a no-op; buffer keeps growing
        $w = New-StageWriter -AutoFlushThreshold 3
        1..5 | ForEach-Object { $w.WriteRecord(@{ i = $_ }) }
        $w.Records.Count | Should -Be 5
        $w.TotalWritten  | Should -Be 5
    }
}

Describe 'New-StageWriter SelectFields projection' {
    It 'passes records through unchanged when SelectFields is omitted' {
        $w = New-StageWriter
        $rec = @{ a = 1; b = 2; c = 3 }
        $w.WriteRecord($rec)
        $w.Records[0] | Should -Be $rec
    }

    It 'projects hashtable records to exactly the declared fields' {
        $w = New-StageWriter -SelectFields @('a','b')
        $w.WriteRecord(@{ a = 1; b = 2; c = 3 })
        $w.Records[0].Keys.Count | Should -Be 2
        $w.Records[0]['a']       | Should -Be 1
        $w.Records[0]['b']       | Should -Be 2
        $w.Records[0].Contains('c') | Should -Be $false
    }

    It 'materializes missing keys as $null so a dev-added field is visible, not dropped' {
        $w = New-StageWriter -SelectFields @('a','b','missing')
        $w.WriteRecord(@{ a = 1; b = 2 })
        $w.Records[0].Contains('missing') | Should -Be $true
        $w.Records[0]['missing']          | Should -Be $null
    }

    It 'preserves declared field order in the projected record' {
        $w = New-StageWriter -SelectFields @('c','a','b')
        $w.WriteRecord(@{ a = 1; b = 2; c = 3 })
        @($w.Records[0].Keys) | Should -Be @('c','a','b')
    }

    It 'projects PSCustomObject records by property name' {
        $w = New-StageWriter -SelectFields @('a','c')
        $w.WriteRecord([PSCustomObject]@{ a = 1; b = 2; c = 3 })
        $w.Records[0]['a'] | Should -Be 1
        $w.Records[0]['c'] | Should -Be 3
        $w.Records[0].Contains('b') | Should -Be $false
    }

    It 'treats an empty SelectFields array as no projection (pass-through)' {
        $w = New-StageWriter -SelectFields @()
        $rec = @{ a = 1; b = 2 }
        $w.WriteRecord($rec)
        $w.Records[0] | Should -Be $rec
    }
}

Describe 'New-StageWriter auto-emit ID safety net (#297)' {
    It 'auto-extracts the IdKey field from hashtable records on WriteRecord' {
        $w = New-StageWriter -AutoEmitIdField 'id'
        $w.WriteRecord(@{ id = 'a'; name = 'Alice' })
        $w.WriteRecord(@{ id = 'b'; name = 'Bob' })
        $w.EmittedIds.Count | Should -Be 0
        [void]$w.PromoteAutoEmittedIds()
        $w.EmittedIds.Count | Should -Be 2
        $w.EmittedIds[0].Id | Should -Be 'a'
        $w.EmittedIds[1].Id | Should -Be 'b'
        $w.EmittedIds[0].Tags | Should -Be $null
    }

    It 'auto-extracts from PSCustomObject records by property name' {
        $w = New-StageWriter -AutoEmitIdField 'id'
        $w.WriteRecord([pscustomobject]@{ id = 'x'; payload = 'p' })
        [void]$w.PromoteAutoEmittedIds()
        $w.EmittedIds.Count | Should -Be 1
        $w.EmittedIds[0].Id | Should -Be 'x'
    }

    It 'reads from the projected record when SelectFields is set (post-projection schema)' {
        # IdKey is on the projected schema. Source has different shape; the
        # writer projects to {id} on the way in, and auto-extract reads `id`
        # from the projected hashtable.
        $w = New-StageWriter -SelectFields @('id') -AutoEmitIdField 'id'
        $w.WriteRecord(@{ id = '42'; ignored = 'extra' })
        [void]$w.PromoteAutoEmittedIds()
        $w.EmittedIds.Count | Should -Be 1
        $w.EmittedIds[0].Id | Should -Be '42'
    }

    It 'PromoteAutoEmittedIds drops the auto list when EmitId was called explicitly' {
        # Explicit always wins — no duplicates with tag-bearing fetches.
        $w = New-StageWriter -AutoEmitIdField 'id'
        $w.WriteRecord(@{ id = 'auto-1' })
        $w.EmitId('explicit-1', @{ kind = 'tagged' })
        $promoted = $w.PromoteAutoEmittedIds()
        $promoted | Should -Be $false
        $w.EmittedIds.Count | Should -Be 1
        $w.EmittedIds[0].Id | Should -Be 'explicit-1'
        $w.EmittedIds[0].Tags.kind | Should -Be 'tagged'
    }

    It 'first explicit EmitId disables auto-extract for subsequent WriteRecords' {
        # Memory hygiene: high-volume root stages (entra_users, sign_in_logs)
        # can produce millions of records. Accumulating _AutoEmittedIds for
        # the whole run when the fetcher emits explicitly is wasted memory.
        # First EmitId clears _AutoEmittedIds and stops further capture.
        $w = New-StageWriter -AutoEmitIdField 'id'
        $w.WriteRecord(@{ id = 'auto-1' })
        $w.WriteRecord(@{ id = 'auto-2' })
        $w._AutoEmittedIds.Count | Should -Be 2
        $w.EmitId('explicit-1', $null)
        $w._AutoEmittedIds.Count | Should -Be 0   # cleared
        $w.WriteRecord(@{ id = 'auto-3' })        # should NOT be captured
        $w._AutoEmittedIds.Count | Should -Be 0
        $w.PromoteAutoEmittedIds() | Should -Be $false
        $w.EmittedIds.Count | Should -Be 1
        $w.EmittedIds[0].Id | Should -Be 'explicit-1'
    }

    It 'skips records where the IdKey field is missing or null' {
        $w = New-StageWriter -AutoEmitIdField 'id'
        $w.WriteRecord(@{ id = 'present' })
        $w.WriteRecord(@{ id = $null })
        $w.WriteRecord(@{ other = 'no id field at all' })
        [void]$w.PromoteAutoEmittedIds()
        $w.EmittedIds.Count | Should -Be 1
        $w.EmittedIds[0].Id | Should -Be 'present'
    }

    It 'skips records where the IdKey value is an empty string' {
        $w = New-StageWriter -AutoEmitIdField 'id'
        $w.WriteRecord(@{ id = '' })
        $w.WriteRecord(@{ id = 'real' })
        [void]$w.PromoteAutoEmittedIds()
        $w.EmittedIds.Count | Should -Be 1
        $w.EmittedIds[0].Id | Should -Be 'real'
    }

    It 'coerces non-string IdKey values via ToString (Guid, int)' {
        $w = New-StageWriter -AutoEmitIdField 'id'
        $g = [Guid]::NewGuid()
        $w.WriteRecord(@{ id = $g })
        $w.WriteRecord(@{ id = 17 })
        [void]$w.PromoteAutoEmittedIds()
        $w.EmittedIds.Count | Should -Be 2
        $w.EmittedIds[0].Id | Should -Be $g.ToString()
        $w.EmittedIds[1].Id | Should -Be '17'
    }

    It 'PromoteAutoEmittedIds returns false and is a no-op when nothing to promote' {
        $w = New-StageWriter -AutoEmitIdField 'id'
        $promoted = $w.PromoteAutoEmittedIds()
        $promoted | Should -Be $false
        $w.EmittedIds.Count | Should -Be 0
    }

    It 'PromoteAutoEmittedIds clears _AutoEmittedIds and disables further capture after promotion' {
        # Memory hygiene: the auto list holds the same hashtable references as
        # EmittedIds after promotion, so dropping the source slot is cheap and
        # avoids two large lists living for the rest of the stage run.
        $w = New-StageWriter -AutoEmitIdField 'id'
        $w.WriteRecord(@{ id = '1' })
        $w.WriteRecord(@{ id = '2' })
        $w._AutoEmittedIds.Count | Should -Be 2
        [void]$w.PromoteAutoEmittedIds()
        $w.EmittedIds.Count | Should -Be 2
        $w._AutoEmittedIds.Count | Should -Be 0   # cleared post-promote
        $null -eq $w._AutoEmitIdField | Should -Be $true   # capture disabled
    }

    It 'PromoteAutoEmittedIds clears _AutoEmittedIds even when explicit wins (no promotion)' {
        # When EmitId already fired and cleared mid-stream, the post-clear
        # WriteRecords don't capture; this case asserts the no-promote branch
        # also leaves nothing dangling in the auto list.
        $w = New-StageWriter -AutoEmitIdField 'id'
        $w.WriteRecord(@{ id = 'auto-1' })   # captured
        # Manually re-enable capture and add a stray entry to simulate a
        # contrived path where _AutoEmittedIds has content alongside an
        # explicit EmitId (shouldn't happen via WriteRecord, but the clear
        # logic should still cover it).
        $w.EmitId('explicit-1', $null)
        # _AutoEmittedIds was cleared by EmitId. Re-add to simulate residual.
        $w._AutoEmittedIds.Add(@{ Id = 'stale'; Tags = $null })
        [void]$w.PromoteAutoEmittedIds()
        $w._AutoEmittedIds.Count | Should -Be 0
        $w.EmittedIds.Count | Should -Be 1
        $w.EmittedIds[0].Id | Should -Be 'explicit-1'
    }

    It 'is inert when AutoEmitIdField is null (default)' {
        $w = New-StageWriter
        $w.WriteRecord(@{ id = 'a' })
        [void]$w.PromoteAutoEmittedIds()
        $w.EmittedIds.Count | Should -Be 0
    }

    It 'auto-extract survives auto-flush boundaries' {
        # The auto-extract reads the projected/raw record before Records is
        # cleared by Flush, so an auto-flushed buffer doesn't lose ID context.
        $captured = [System.Collections.Generic.List[object]]::new()
        $cb = { param($recs) foreach ($r in $recs) { $captured.Add($r) } }
        $w = New-StageWriter -FlushCallback $cb -AutoFlushThreshold 2 -AutoEmitIdField 'id'
        $w.WriteRecord(@{ id = '1' })
        $w.WriteRecord(@{ id = '2' })  # triggers auto-flush
        $w.WriteRecord(@{ id = '3' })
        $w.Flush()
        [void]$w.PromoteAutoEmittedIds()
        $w.EmittedIds.Count | Should -Be 3
        $w.EmittedIds[0].Id | Should -Be '1'
        $w.EmittedIds[2].Id | Should -Be '3'
    }
}

