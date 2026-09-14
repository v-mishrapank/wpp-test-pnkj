function Write-Log {
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidOverwritingBuiltInCmdlets', '', Justification = 'Write-Log is a project logger, not a PowerShell built-in. PSSA stale built-ins list.')]
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingWriteHost', '', Justification = 'Write-Log is the print primitive; ACA containers capture host output to stdout.')]
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Message,
        [ValidateSet('INFO', 'WARN', 'ERROR', 'DEBUG')][string]$Level = 'INFO',
        [string]$Entity = '',
        [string]$TenantKey = '',
        # Bypass the structured-event sentinel guard. Set only by EventEmitter's
        # Write-Event, which legitimately appends `_event:<json>` to its prose
        # line. Any other caller slipping `_event:` into a message would corrupt
        # KQL parsing on `ContainerAppConsoleLogs_CL`.
        [switch]$AllowEventSentinel
    )

    # Sanitize the reserved `_event:` sentinel out of caller-supplied prose
    # so a stray substring (e.g. an upstream API error body that happens to
    # contain `_event:`) doesn't poison KQL parsing of structured events.
    # Replacing rather than throwing — losing a log line because the upstream
    # error string contained a token would mask the real failure path.
    if (-not $AllowEventSentinel -and $Message -like '*_event:*') {
        $Message = $Message -replace '_event:', '_evnt_:'
    }

    # A single logical log call must land as one LAW row. ACA's stdout
    # capture splits on \n, so a multi-line $Message turns into N
    # ContainerAppConsoleLogs_CL rows — the N-1 continuation rows arrive
    # without our [timestamp] [LEVEL] prefix and break KQL parsing. Replace
    # embedded CR/LF with the literal two-char \n so the structure is
    # preserved for downstream extraction without splitting rows. Primary
    # offender: HTTP wire-format text leaking through ErrorDetails.Message;
    # see #405.
    if ($Message -match '[\r\n]') {
        $Message = $Message -replace "`r`n", '\n' -replace "`n", '\n' -replace "`r", '\n'
    }

    # UtcNow.ToString — the literal `Z` in the format string is just a
    # character, not a UTC indicator. `Get-Date` returns local time by
    # default, which would mislabel non-UTC hosts. Containers run in UTC so
    # production was always correct, but tests and dev hosts could emit
    # stamps tagged Z that aren't UTC.
    $timestamp = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
    $context = @()
    if ($TenantKey) { $context += "tenant=$TenantKey" }
    if ($Entity) { $context += "entity=$Entity" }
    $contextStr = if ($context.Count -gt 0) { " [$($context -join ', ')]" } else { '' }

    $line = "[$timestamp] [$Level]$contextStr $Message"

    # -ErrorAction / -WarningAction Continue guarantee the log line is emitted
    # without terminating the caller, regardless of the caller's
    # $ErrorActionPreference / $WarningPreference. A logging call must never
    # propagate.
    switch ($Level) {
        'ERROR' { Write-Error $line -ErrorAction Continue }
        'WARN'  { Write-Warning $line -WarningAction Continue }
        'DEBUG' { Write-Verbose $line }
        default { Write-Host $line }
    }
}

Export-ModuleMember -Function Write-Log
