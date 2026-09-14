#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Unit tests for LogHelper.psm1 — primarily the invariant that a single
# Write-Log call must land as a single ACA stdout row, even when the caller
# passes embedded CR/LF. See #405 for the leak this defends against.

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    Import-Module (Join-Path $modulesPath 'LogHelper.psm1') -Force
}

Describe 'Write-Log newline normalization (#405)' {
    It 'replaces a single embedded LF with literal \n' {
        $out = Write-Log -Message "first`nsecond" 6>&1 | Out-String
        # Trim the trailing terminator the host appends so we can assert on
        # the count of newlines that survived inside the message itself.
        $body = $out.TrimEnd("`r", "`n")
        $body.Contains([char]10) | Should -BeFalse
        $body | Should -Match 'first\\nsecond'
    }

    It 'replaces CRLF with a single literal \n (no doubling)' {
        $out = Write-Log -Message "a`r`nb" 6>&1 | Out-String
        $body = $out.TrimEnd("`r", "`n")
        $body | Should -Match 'a\\nb'
        $body | Should -Not -Match 'a\\n\\nb'
    }

    It 'leaves single-line messages untouched' {
        $out = Write-Log -Message 'one liner' 6>&1 | Out-String
        $out | Should -Match 'one liner'
        $out | Should -Not -Match '\\n'
    }

    # The Graph 4xx leak pattern: an item_failed prose line carrying a
    # body= chunk with HTTP wire format. Even though Get-ErrorClassification
    # now strips wire headers (#405), Write-Log is the last line of defense
    # — any future caller slipping in a multi-line message must still land
    # as one row.
    It 'collapses a wire-format-shaped multi-line message to one row' {
        $multi = "Item failed msg='HTTP/2.0 404 Not Found`r`nVary: Accept-Encoding`r`nrequest-id: abc'"
        $out = Write-Log -Message $multi 6>&1 | Out-String
        $body = $out.TrimEnd("`r", "`n")
        # One terminating newline at most — Write-Host adds one.
        ($body -split "`n").Count | Should -Be 1
        $body | Should -Match 'HTTP/2\.0 404 Not Found\\nVary'
    }

    It 'still scrubs the _event: sentinel before normalizing newlines' {
        $out = Write-Log -Message "before _event:bad`nafter" 6>&1 | Out-String
        $body = $out.TrimEnd("`r", "`n")
        $body | Should -Match '_evnt_:bad'
        $body | Should -Not -Match '_event:'
        $body | Should -Match 'before _evnt_:bad\\nafter'
    }
}
