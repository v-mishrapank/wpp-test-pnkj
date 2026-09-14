# New-StageWriter — factory for the accumulator passed to Get-* stage
# functions. Returned object has the same shape whether constructed in the
# orchestrator process or inside a worker runspace.
#
# Why a factory (not a class): PowerShell classes don't cross runspace
# boundaries via Import-Module — only functions do (see RecordEnvelope.psm1
# for the same constraint). Pool-stage runspaces construct their writer
# fresh per invocation using this helper, with identical shape to the one
# the orchestrator constructs for inline stages.
#
# The writer accumulates records bound to one entity in a flat .Records
# list. The Get-* fetcher calls WriteRecord($rec).
#   $writer.Records        [List[object]]     buffer (cleared on flush)
#   $writer.EmittedIds     [List[hashtable]]  each @{ Id; Tags }
#   $writer.TotalWritten   [int]              cumulative count
#   $writer.WriteRecord($record)
#   $writer.EmitId($id, $tags)
#   $writer.Flush()        drains Records through FlushCallback if supplied
#
# When constructed with -AutoFlushThreshold N (and a FlushCallback), the
# writer self-flushes every N records. Inline stages for high-volume
# entities (entra_users, entra_sign_in_logs, intune_managed_devices) need
# this to avoid accumulating millions of records in memory before writing.
# Pool stages already stream per-item via the dispatch template, so they
# can leave AutoFlushThreshold=0.
#
# When constructed with -SelectFields @('a','b','c'), each WriteRecord
# projects the record down to exactly that field set (ordered hashtable,
# missing keys become $null). This is the enforcement point for the
# entity's declared SelectFields contract: the fetcher can't silently emit
# extra fields or miss declared ones — the writer reshapes on the way in.
# A $null or empty field list skips projection (raw pass-through).
#
# When constructed with -AutoEmitIdField <fieldName>, every WriteRecord
# captures record[<fieldName>] into _AutoEmittedIds as a safety net for
# fetchers that forget the explicit $Writer.EmitId() call. After the fetch
# returns, the caller invokes PromoteAutoEmittedIds():
# - If the fetch made any explicit EmitId calls, the auto-list is dropped
#   on the floor (explicit always wins; no duplicate emissions).
# - If EmittedIds is empty, the auto-list is promoted into EmittedIds so
#   downstream stages still receive their inputs.
# StageExecutor / WorkerPool wire this to single-field IdKey stages
# (EmitIds=$true with IdKey not containing ':::'); composite-IdKey stages
# pass $null and continue to require explicit EmitId calls because the
# composite has to be assembled by the fetcher. IdKey names a field on
# the record AS PASSED TO WriteRecord (post-projection schema), not on
# the source-API object.

function New-StageWriter {
    [CmdletBinding()]
    param(
        [scriptblock]$FlushCallback,
        [int]$AutoFlushThreshold = 0,
        [string[]]$SelectFields,
        [string]$AutoEmitIdField
    )

    $resolvedAutoEmit = if ([string]::IsNullOrEmpty($AutoEmitIdField)) { $null } else { $AutoEmitIdField }

    $writer = [PSCustomObject]@{
        Records                = [System.Collections.Generic.List[object]]::new()
        EmittedIds             = [System.Collections.Generic.List[hashtable]]::new()
        TotalWritten           = 0
        _FlushCb               = $FlushCallback
        _AutoFlushAt           = $AutoFlushThreshold
        _SelectFields          = if ($SelectFields -and $SelectFields.Count -gt 0) { $SelectFields } else { $null }
        _AutoEmitIdField       = $resolvedAutoEmit
        _AutoEmittedIds        = [System.Collections.Generic.List[hashtable]]::new()
    }

    $writer | Add-Member -MemberType ScriptMethod -Name WriteRecord -Value {
        param([object]$record)
        if ($null -ne $this._SelectFields) {
            $projected = [ordered]@{}
            foreach ($f in $this._SelectFields) {
                if ($record -is [System.Collections.IDictionary]) {
                    $projected[$f] = $record[$f]
                } else {
                    $projected[$f] = $record.$f
                }
            }
            $this.Records.Add($projected)
            $autoSource = $projected
        } else {
            $this.Records.Add($record)
            $autoSource = $record
        }
        $this.TotalWritten++
        # Auto-emit ID safety net. Read from the same shape the record was
        # stored in (projected hashtable when projection applied, raw
        # record otherwise). Cast to string so non-string IdKey values
        # (Guid, int) match EmitId's [string]$id contract. Skip null /
        # empty so a record missing the field doesn't pollute the auto
        # list — promotion will still fire if any other record had it.
        if ($null -ne $this._AutoEmitIdField) {
            $idVal = if ($autoSource -is [System.Collections.IDictionary]) {
                $autoSource[$this._AutoEmitIdField]
            } else {
                $autoSource.($this._AutoEmitIdField)
            }
            if ($null -ne $idVal) {
                $idStr = [string]$idVal
                if (-not [string]::IsNullOrEmpty($idStr)) {
                    $this._AutoEmittedIds.Add(@{ Id = $idStr; Tags = $null })
                }
            }
        }
        if ($this._AutoFlushAt -gt 0 -and $this.Records.Count -ge $this._AutoFlushAt) {
            $this.Flush()
        }
    }

    $writer | Add-Member -MemberType ScriptMethod -Name EmitId -Value {
        param([string]$id, [hashtable]$tags)
        # First explicit call disables auto-extract for the rest of the run:
        # explicit always wins at promote time, so accumulating auto entries
        # past this point is wasted memory (high-volume root stages can
        # produce millions of records). Drop what we've collected and stop
        # capturing on subsequent WriteRecord calls.
        if ($null -ne $this._AutoEmitIdField) {
            $this._AutoEmitIdField = $null
            $this._AutoEmittedIds.Clear()
        }
        $this.EmittedIds.Add(@{ Id = $id; Tags = $tags })
    }

    # Promote auto-extracted IDs into EmittedIds when the fetcher made no
    # explicit calls. Idempotent (returns $false if no promotion happened).
    # Caller invokes this once after the fetch returns and before reading
    # EmittedIds. Tags-bearing fetches that call EmitId explicitly take
    # precedence; the auto-list is dropped on the floor.
    $writer | Add-Member -MemberType ScriptMethod -Name PromoteAutoEmittedIds -Value {
        if ($this.EmittedIds.Count -gt 0) {
            # Explicit emits won — drop the auto list to release its slot in
            # memory. Cheap insurance for high-volume stages where the auto
            # path captured records before the first explicit EmitId cleared.
            if ($this._AutoEmittedIds.Count -gt 0) { $this._AutoEmittedIds.Clear() }
            return $false
        }
        if ($this._AutoEmittedIds.Count -eq 0) { return $false }
        foreach ($e in $this._AutoEmittedIds) { $this.EmittedIds.Add($e) }
        # Promoted — clear the source list and disable further capture.
        # The hashtables themselves are now referenced by EmittedIds so the
        # underlying objects survive; only the auto-list slot is released.
        $this._AutoEmittedIds.Clear()
        $this._AutoEmitIdField = $null
        return $true
    }

    $writer | Add-Member -MemberType ScriptMethod -Name Flush -Value {
        if ($null -eq $this._FlushCb -or $this.Records.Count -eq 0) { return }
        & $this._FlushCb $this.Records.ToArray()
        $this.Records.Clear()
    }

    return $writer
}

Export-ModuleMember -Function New-StageWriter
