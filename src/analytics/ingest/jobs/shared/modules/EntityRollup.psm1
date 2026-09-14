# Shared per-entity rollup factory. One function = one place to change the
# wire shape of an entity row in either the manifest summary or the heartbeat
# blob. The C# side mirrors this contract via Models/EntityRollup.cs; a
# round-trip test in CI fails on drift.
#
# Nullable semantics:
#   - StartedAt / CompletedAt are progressively known. Pending entities pass
#     $null; running entities pass StartedAt only; terminal entities pass both.
#   - RecordCount is null until terminal — pending/running entities don't have
#     a count yet. Pass $null when not known; numeric values are cast to int.
#   - Errors is always present; empty array when no errors. Forced to an array
#     so single-string payloads don't deserialize as a bare string downstream.
#
# Status vocabulary: pending | running | success | partial | skipped | failed.
# Manifest writes only the four terminal values; heartbeat writes the full
# union.

function New-EntityRollup {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Status,
        [object]$RecordCount = $null,
        [object]$StartedAt = $null,
        [object]$CompletedAt = $null,
        [object]$Errors = @(),
        [object]$InputCount = $null,
        [object]$ItemsProcessed = $null,
        [object]$ItemsFailed = $null,
        [object]$ItemsSkipped = $null,
        [object]$RecordsSoFar = $null,
        [object]$DurationMs = $null
    )

    # Normalize nullables: pwsh hashtable serialization treats $null as missing
    # in some paths; force explicit null fields so System.Text.Json on the C#
    # side sees the property and reads it as null rather than defaulting.
    $rc = if ($null -eq $RecordCount) { $null } else { [int]$RecordCount }
    $sa = if ($null -eq $StartedAt -or $StartedAt -eq '') { $null } else { [string]$StartedAt }
    $ca = if ($null -eq $CompletedAt -or $CompletedAt -eq '') { $null } else { [string]$CompletedAt }
    # Force-array — a single-element @($_) collapses to a bare string under
    # ConvertTo-Json without the explicit cast.
    $errs = @()
    if ($null -ne $Errors) { $errs = @($Errors | Where-Object { $_ -ne $null }) }

    $ic = if ($null -eq $InputCount) { $null } else { [int]$InputCount }
    $ip = if ($null -eq $ItemsProcessed) { $null } else { [int]$ItemsProcessed }
    $ifail = if ($null -eq $ItemsFailed) { $null } else { [int]$ItemsFailed }
    $iskip = if ($null -eq $ItemsSkipped) { $null } else { [int]$ItemsSkipped }
    $rsf = if ($null -eq $RecordsSoFar) { $null } else { [int]$RecordsSoFar }
    $dm = if ($null -eq $DurationMs) { $null } else { [int]$DurationMs }

    return [ordered]@{
        name            = $Name
        status          = $Status
        record_count    = $rc
        input_count     = $ic
        items_processed = $ip
        items_failed    = $ifail
        items_skipped   = $iskip
        records_so_far  = $rsf
        started_at      = $sa
        completed_at    = $ca
        duration_ms     = $dm
        errors          = $errs
    }
}

Export-ModuleMember -Function New-EntityRollup
