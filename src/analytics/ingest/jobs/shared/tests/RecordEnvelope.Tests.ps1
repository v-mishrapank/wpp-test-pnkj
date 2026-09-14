#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Unit tests for RecordEnvelope.psm1 — the envelope shape contract and the
# Depth ceiling that gated #162. Written deliberately to fail when the
# default depth ever changes silently: depth-related assertions reference
# the function default explicitly, so a drift in the default flips them.

# `using module` (not Import-Module) so the [JsonlRecordWriter] class is
# visible to the test scriptblocks. Classes don't cross Import-Module
# boundaries; this is the same constraint documented in RecordEnvelope.psm1.
using module ../modules/RecordEnvelope.psm1

BeforeAll {
    # Helper: build a hashtable with N levels of own nesting around a string
    # leaf. Depth 1 = @{ k = 'leaf' }; depth 3 = @{ k = @{ k = @{ k = 'leaf' } } }.
    # Defined inside BeforeAll so it lands in the test scriptblock scope —
    # script-top function definitions aren't visible to It blocks under Pester.
    function New-NestedHash {
        param([int]$Depth)
        if ($Depth -le 0) { return 'leaf' }
        return @{ k = (New-NestedHash -Depth ($Depth - 1)) }
    }

    # Truncation marker emitted by ConvertTo-Json at the depth ceiling.
    # Hashtables render as their .ToString() type name when the serializer
    # can't recurse further.
    $script:HashtableTruncationMarker = 'System.Collections.Hashtable'
}

Describe 'New-EnvelopedJsonLine envelope shape' {
    It 'wraps the record under _record alongside the four metadata fields' {
        $line = New-EnvelopedJsonLine -Record @{ id = 'x' } -SourceType 'tenant' -SourceKey 't1' -BatchId 'run-1'
        $obj  = $line | ConvertFrom-Json -AsHashtable
        $obj.Keys | Sort-Object | Should -Be @('_record','batch_id','ingested_at','source_key','source_type')
        $obj.source_type | Should -Be 'tenant'
        $obj.source_key  | Should -Be 't1'
        $obj.batch_id    | Should -Be 'run-1'
        $obj._record.id  | Should -Be 'x'
    }

    It 'emits ingested_at as a parseable ISO-8601 timestamp' {
        $line = New-EnvelopedJsonLine -Record @{} -SourceType 'tenant' -SourceKey 't1' -BatchId 'run-1'
        $obj  = $line | ConvertFrom-Json
        # Round-trip through DateTimeOffset to assert the format is parseable;
        # exact precision is platform-dependent so we don't string-match.
        { [System.DateTimeOffset]::Parse($obj.ingested_at) } | Should -Not -Throw
    }

    It 'returns a single line of compressed JSON (no embedded newlines)' {
        $line = New-EnvelopedJsonLine -Record @{ id = 'x'; arr = @(1,2,3) } -SourceType 'tenant' -SourceKey 't1' -BatchId 'run-1'
        $line | Should -Not -Match "`n"
        $line | Should -Not -Match "`r"
    }
}

Describe 'New-EnvelopedJsonLine depth ceiling — pins current default' {
    # These tests pin the depth contract as it stands at HEAD. The empirical
    # ConvertTo-Json behavior is "envelope -Depth N fits exactly N record
    # wraps; the (N+1)th wrap stringifies as a type-name placeholder". So at
    # the default Depth=6: 6 wraps fit, 7 wraps truncate. (The issue body's
    # description as 'Depth=6 envelope gives the record 5 levels of budget'
    # is a hair off — verified by the empirical-boundary tests below.)

    It 'serializes a 6-wrap-deep record cleanly at the default Depth (6)' {
        $rec  = New-NestedHash -Depth 6
        $line = New-EnvelopedJsonLine -Record $rec -SourceType 'tenant' -SourceKey 't1' -BatchId 'run-1'
        $line | Should -Not -Match $script:HashtableTruncationMarker
    }

    It 'truncates a 7-wrap-deep record at the default Depth (6) — the #162 bug' {
        $rec  = New-NestedHash -Depth 7
        $line = New-EnvelopedJsonLine -Record $rec -SourceType 'tenant' -SourceKey 't1' -BatchId 'run-1' -WarningAction SilentlyContinue
        $line | Should -Match $script:HashtableTruncationMarker
    }

    It 'honors an explicit -Depth override: 12 fits a 12-wrap-deep record' {
        $rec  = New-NestedHash -Depth 12
        $line = New-EnvelopedJsonLine -Record $rec -SourceType 'tenant' -SourceKey 't1' -BatchId 'run-1' -Depth 12
        $line | Should -Not -Match $script:HashtableTruncationMarker
    }

    It 'honors an explicit -Depth override: 4 truncates a 5-wrap-deep record' {
        $rec  = New-NestedHash -Depth 5
        $line = New-EnvelopedJsonLine -Record $rec -SourceType 'tenant' -SourceKey 't1' -BatchId 'run-1' -Depth 4 -WarningAction SilentlyContinue
        $line | Should -Match $script:HashtableTruncationMarker
    }
}

Describe 'JsonlRecordWriter file round-trip' {
    BeforeEach {
        $script:tmpFile = Join-Path ([System.IO.Path]::GetTempPath()) "RecordEnvelopeTest_$(Get-Random).jsonl"
    }

    AfterEach {
        if (Test-Path $script:tmpFile) { Remove-Item $script:tmpFile -Force }
    }

    It 'writes one envelope-wrapped line per WriteRecord call' {
        $w = [JsonlRecordWriter]::new($script:tmpFile, 'tenant', 't1', 'run-1')
        try {
            $w.WriteRecord(@{ id = 'a' })
            $w.WriteRecord(@{ id = 'b' })
            $w.Flush()
        } finally { $w.Dispose() }

        $lines = Get-Content -LiteralPath $script:tmpFile
        $lines.Count | Should -Be 2
        ($lines[0] | ConvertFrom-Json)._record.id | Should -Be 'a'
        ($lines[1] | ConvertFrom-Json)._record.id | Should -Be 'b'
    }

    It 'writes UTF-8 without a BOM (matches the documented encoding contract)' {
        $w = [JsonlRecordWriter]::new($script:tmpFile, 'tenant', 't1', 'run-1')
        try { $w.WriteRecord(@{ id = 'x' }); $w.Flush() } finally { $w.Dispose() }

        $bytes = [System.IO.File]::ReadAllBytes($script:tmpFile)
        # UTF-8 BOM is 0xEF 0xBB 0xBF; assert first byte is the literal '{'.
        $bytes[0] | Should -Be ([byte][char]'{')
    }

    It 'truncates a 7-wrap-deep record at the default Depth — confirms the bug surfaces through the writer too' {
        $w = [JsonlRecordWriter]::new($script:tmpFile, 'tenant', 't1', 'run-1')
        try {
            $w.WriteRecord((New-NestedHash -Depth 7))
            $w.Flush()
        } finally { $w.Dispose() }

        $line = Get-Content -LiteralPath $script:tmpFile -Raw -WarningAction SilentlyContinue
        $line | Should -Match $script:HashtableTruncationMarker
    }

    It 'honors the 5-arg constructor Depth override at the boundary — Depth 12 fits exactly 12 wraps' {
        # Mirrors the function-level "Depth N fits N wraps" boundary that the
        # ceiling tests above pin. A 12-wrap record at envelope Depth 12
        # round-trips clean; a 13-wrap one at the same Depth would truncate.
        $w = [JsonlRecordWriter]::new($script:tmpFile, 'tenant', 't1', 'run-1', 12)
        try {
            $w.WriteRecord((New-NestedHash -Depth 12))
            $w.Flush()
        } finally { $w.Dispose() }

        $line = Get-Content -LiteralPath $script:tmpFile -Raw
        $line | Should -Not -Match $script:HashtableTruncationMarker
        # Sanity: all 12 levels of nesting are present in the output.
        ($line -split '"k":').Count - 1 | Should -Be 12
    }
}
