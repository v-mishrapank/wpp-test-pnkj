# Provenance envelope for ingested records. Wire-shape per JSONL line:
#
#   {
#     "source_type": "tenant",
#     "source_key":  "<tenant_key | forest_key | ...>",
#     "batch_id":    "<runId>",
#     "ingested_at": "<ISO-8601>",
#     "_record":     { ...source-API record... }
#   }
#
# The _record nesting prevents collisions between metadata fields and
# source-API properties (e.g. Entra schema extensions, LDAP attrs, etc.).
#
# New-EnvelopedJsonLine is the single source of truth for envelope shape.
# Phase 1 callers use the JsonlRecordWriter class, which wraps a StreamWriter
# and delegates serialization to the function. Phase 2 runspaces (in
# WorkerPool.psm1) import this module via InitialSessionState and call the
# function directly — classes don't cross runspace boundaries via Import-Module
# but functions do.

function New-EnvelopedJsonLine {
    param(
        [Parameter(Mandatory)]$Record,
        [Parameter(Mandatory)][string]$SourceType,
        [Parameter(Mandatory)][string]$SourceKey,
        [Parameter(Mandatory)][string]$BatchId,
        [int]$Depth = 6
    )
    $envelope = [ordered]@{
        source_type = $SourceType
        source_key  = $SourceKey
        batch_id    = $BatchId
        ingested_at = (Get-Date -Format 'o')
        _record     = $Record
    }
    return ConvertTo-Json -InputObject $envelope -Compress -Depth $Depth
}

class JsonlRecordWriter {
    hidden [System.IO.StreamWriter]$_writer
    hidden [string]$_sourceType
    hidden [string]$_sourceKey
    hidden [string]$_batchId
    # Envelope-level Depth passed through to New-EnvelopedJsonLine.
    # Stage-spec JsonDepth is the *record* depth budget; the inline path
    # (StageExecutor) converts to envelope depth with the same +1 the pool
    # path applies in WorkerPool.psm1. See issue #162.
    hidden [int]$_depth

    # 4-arg form: preserves existing call sites by deferring to the function
    # default. The 5-arg form is what stages with deeply-nested payloads use.
    JsonlRecordWriter([string]$path, [string]$sourceType, [string]$sourceKey, [string]$batchId)
        : base() {
        $this._init($path, $sourceType, $sourceKey, $batchId, 0)
    }
    JsonlRecordWriter([string]$path, [string]$sourceType, [string]$sourceKey, [string]$batchId, [int]$depth)
        : base() {
        $this._init($path, $sourceType, $sourceKey, $batchId, $depth)
    }

    hidden [void] _init([string]$path, [string]$sourceType, [string]$sourceKey, [string]$batchId, [int]$depth) {
        # UTF-8 without BOM. [Encoding]::UTF8 prepends one, which breaks strict
        # JSON parsers at the first record of every chunk file.
        $this._writer     = [System.IO.StreamWriter]::new($path, $false, [System.Text.UTF8Encoding]::new($false))
        $this._sourceType = $sourceType
        $this._sourceKey  = $sourceKey
        $this._batchId    = $batchId
        $this._depth      = $depth
    }

    [void] WriteRecord([object]$record) {
        # The field uses 0 as an internal sentinel for "no override — use
        # New-EnvelopedJsonLine's default". The 4-arg ctor sets it; the 5-arg
        # ctor's $depth parameter is expected to be a real envelope-Depth
        # value (>= 1), never a literal 0 — passing 0 would be a meaningless
        # serialization budget and is not a supported configuration.
        if ($this._depth -gt 0) {
            $this._writer.WriteLine((New-EnvelopedJsonLine `
                -Record $record `
                -SourceType $this._sourceType `
                -SourceKey $this._sourceKey `
                -BatchId $this._batchId `
                -Depth $this._depth))
        } else {
            $this._writer.WriteLine((New-EnvelopedJsonLine `
                -Record $record `
                -SourceType $this._sourceType `
                -SourceKey $this._sourceKey `
                -BatchId $this._batchId))
        }
    }

    [void] Flush()   { $this._writer.Flush() }
    [void] Dispose() { $this._writer.Dispose() }
}

Export-ModuleMember -Function New-EnvelopedJsonLine
