#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Schema-shape tests for EventEmitter.psm1. Each Write-Event must produce a
# Write-Log line containing exactly one `_event:` sentinel followed by valid
# JSON with all base fields populated. See EVENT_SCHEMA.md for the v1 contract.

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    Import-Module (Join-Path $modulesPath 'LogHelper.psm1')    -Force
    Import-Module (Join-Path $modulesPath 'EventEmitter.psm1') -Force

    function script:CaptureEvent {
        param([scriptblock]$EmitBlock)
        # Write-Event funnels through Write-Log, which routes INFO via
        # Write-Host. Pester 5's Mock works at module scope, so mock Write-Log
        # in LogHelper to capture the line without touching real stdout.
        $script:capturedLine = $null
        InModuleScope LogHelper {
            Mock Write-Log {
                $script:capturedLine = $Message
            } -ModuleName EventEmitter
        }
        & $EmitBlock
        return $script:capturedLine
    }

    function script:ParseEventJson {
        param([string]$Line)
        if (-not $Line) { return $null }
        if ($Line -notmatch '_event:(.+)$') { return $null }
        return ($Matches[1] | ConvertFrom-Json -AsHashtable)
    }
}

Describe 'Initialize-EventContext' {
    It 'sets run_id and tenant on subsequent emits' {
        Initialize-EventContext -RunId 'abc123' -Tenant 'madev1'
        $line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
        Write-Event -EventType run_started -Properties @{ input_count = 5 }
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.run_id | Should -Be 'abc123'
        $e.tenant | Should -Be 'madev1'
    }
}

Describe 'Write-Event base schema' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'emits all base fields on every event_type' {
        Write-Event -EventType stage_started -Stage 'xyz' -Entity 'ent' -Properties @{ input_count = 10 }
        # Check ts shape on the raw line — ConvertFrom-Json auto-parses ISO8601
        # strings to [DateTime] and re-stringifies with 7-digit fractional
        # seconds, hiding the actual emitted format.
        $script:line | Should -Match '"ts":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z"'
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.schema_version | Should -Be 1
        $e.run_id         | Should -Be 'r1'
        $e.tenant         | Should -Be 't1'
        $e.entity         | Should -Be 'ent'
        $e.stage          | Should -Be 'xyz'
        $e.event_type     | Should -Be 'stage_started'
        $e.input_count    | Should -Be 10
    }

    It 'rejects unknown event_type' {
        { Write-Event -EventType not_real } | Should -Throw '*unknown event_type*'
    }

    It 'allows null input_count for inline stage_started' {
        Write-Event -EventType stage_started -Stage 's' -Entity 'e' -Properties @{ input_count = $null }
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.input_count | Should -BeNullOrEmpty
    }

    It 'merges per-event extra properties into the event body' {
        Write-Event -EventType stage_completed -Stage 's' -Entity 'e' -Properties @{
            records_so_far = 1500
            duration_ms    = 4321
        }
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.records_so_far | Should -Be 1500
        $e.duration_ms    | Should -Be 4321
    }

    It 'emits one-line JSON with no embedded newlines' {
        Write-Event -EventType run_completed -Properties @{
            status = 'success'; total_records = 100; duration_ms = 200
        }
        # Whole line must contain no newlines — KQL extract relies on per-row parse.
        $script:line | Should -Not -Match "`n"
        # And the JSON sentinel must be the literal one-line shape.
        $script:line | Should -Match '_event:\{.+\}$'
    }
}

Describe 'Write-Event prose' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'puts prose before the _event: sentinel' {
        Write-Event -EventType run_started -Properties @{ input_count = 7 }
        $script:line | Should -Match '^Run started entities=7 _event:'
    }

    It 'never produces prose containing the _event sentinel before the actual one' {
        Write-Event -EventType stage_started -Stage 's' -Entity 'e' -Properties @{ input_count = 5 }
        # Exactly one occurrence of `_event:`.
        ([regex]::Matches($script:line, '_event:')).Count | Should -Be 1
    }
}

Describe 'stage_skipped event type' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'is a recognized event_type' {
        { Write-Event -EventType stage_skipped -Stage 's' -Entity 'e' -Properties @{
            reason = 'ancestor_failed'; ancestor_stage = 'parent'
        } } | Should -Not -Throw
    }

    It 'carries reason and ancestor_stage in the JSON body' {
        Write-Event -EventType stage_skipped -Stage 'child' -Entity 'e' -Properties @{
            reason         = 'ancestor_failed'
            ancestor_stage = 'parent'
        }
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.event_type     | Should -Be 'stage_skipped'
        $e.reason         | Should -Be 'ancestor_failed'
        $e.ancestor_stage | Should -Be 'parent'
    }
}

Describe 'stage_progress slice_index' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'records slice_index in the body and prose' {
        Write-Event -EventType stage_progress -Stage 's' -Entity 'e' -Properties @{
            records_so_far = 100
            slice_index    = 3
        }
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.slice_index | Should -Be 3
        $script:line   | Should -Match 'slice=3'
    }
}

Describe 'UTC timestamps' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'emits a ts that is within 5s of [DateTime]::UtcNow' {
        $before = [DateTime]::UtcNow
        Write-Event -EventType run_started -Properties @{ input_count = 1 }
        $after = [DateTime]::UtcNow

        # Pull the raw ts string out of the JSON, parse as UTC.
        $tsMatch = [regex]::Match($script:line, '"ts":"([^"]+)"')
        $tsMatch.Success | Should -BeTrue
        $tsStr = $tsMatch.Groups[1].Value
        $ts = [DateTime]::ParseExact($tsStr, 'yyyy-MM-ddTHH:mm:ss.fffZ',
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::AssumeUniversal -bor
            [System.Globalization.DateTimeStyles]::AdjustToUniversal)

        # Allow 5s either side of the call window (clock skew + Pester slowness).
        ($ts - $before).TotalSeconds | Should -BeGreaterOrEqual -5
        ($ts - $after).TotalSeconds  | Should -BeLessOrEqual 5
    }
}

Describe 'Write-ThrottleEvent' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'emits a throttle_event with all required throttle properties' {
        Write-ThrottleEvent -Stage 'st' -Entity 'en' `
            -RetryAfterSeconds 30 -Attempt 3 -StatusCode 429 -Message 'TooManyRequests'
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.event_type           | Should -Be 'throttle_event'
        $e.retry_after_seconds  | Should -Be 30
        $e.attempt              | Should -Be 3
        $e.status_code          | Should -Be 429
        $e.throttle_signal_text | Should -Be 'TooManyRequests'
        $e.stage                | Should -Be 'st'
        $e.entity               | Should -Be 'en'
    }
}

Describe 'Write-UnknownRetryEvent (#327)' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'emits an unknown_retry_event with all required diagnostic properties' {
        Write-UnknownRetryEvent -Stage 'st' -Entity 'en' `
            -Attempt 2 -DelaySeconds 8 -StatusCode 0 `
            -ExceptionType 'System.Net.Http.HttpRequestException' `
            -InnerExceptionType 'System.IO.IOException' `
            -ApiFamily 'graph' -Message 'Connection reset'
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.event_type           | Should -Be 'unknown_retry_event'
        $e.attempt              | Should -Be 2
        $e.delay_seconds        | Should -Be 8
        $e.status_code          | Should -Be 0
        $e.exception_type       | Should -Be 'System.Net.Http.HttpRequestException'
        $e.inner_exception_type | Should -Be 'System.IO.IOException'
        $e.api_family           | Should -Be 'graph'
        $e.error_message        | Should -Be 'Connection reset'
        $e.stage                | Should -Be 'st'
        $e.entity               | Should -Be 'en'
    }

    It 'falls back to Set-EventScope when caller does not pass Stage/Entity' {
        Set-EventScope -Stage 'fallback_stage' -Entity 'fallback_entity'
        Write-UnknownRetryEvent -Attempt 1 -DelaySeconds 2 -ApiFamily 'exo' -Message 'x'
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.stage  | Should -Be 'fallback_stage'
        $e.entity | Should -Be 'fallback_entity'
    }

    It 'sanitizes _event: out of caller-supplied error_message in the prose half' {
        Write-UnknownRetryEvent -Stage 's' -Entity 'e' -Attempt 1 -DelaySeconds 2 `
            -ApiFamily 'graph' -ExceptionType 'X' -Message 'remote said _event: bad'
        $proseHalf = ($script:line -split '_event:', 2)[0]
        $proseHalf | Should -Not -Match '_event:'
        $proseHalf | Should -Match '_evnt_:'
    }
}

Describe 'Write-ChunkFailedEvent (#342)' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'emits a chunk_failed event with all required diagnostic properties' {
        Write-ChunkFailedEvent -Stage 'site_details' -Entity 'spo_site_details' `
            -ChunkIndex 3 `
            -ExceptionType 'System.Management.Automation.ParameterBindingException' `
            -InnerExceptionType 'System.ArgumentNullException' `
            -Message "Cannot bind argument to parameter 'Tenant' because it is null." `
            -ScriptStackTrace 'at Connect-Service, Connect.psm1: line 40'
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.event_type           | Should -Be 'chunk_failed'
        $e.chunk_index          | Should -Be 3
        $e.exception_type       | Should -Be 'System.Management.Automation.ParameterBindingException'
        $e.inner_exception_type | Should -Be 'System.ArgumentNullException'
        $e.error_message        | Should -Be "Cannot bind argument to parameter 'Tenant' because it is null."
        $e.script_stack_trace   | Should -Be 'at Connect-Service, Connect.psm1: line 40'
        $e.stage                | Should -Be 'site_details'
        $e.entity               | Should -Be 'spo_site_details'
    }

    It 'falls back to Set-EventScope when caller does not pass Stage/Entity' {
        Set-EventScope -Stage 'fb_stage' -Entity 'fb_entity'
        Write-ChunkFailedEvent -ChunkIndex 0 -ExceptionType 'X' -Message 'm'
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.stage  | Should -Be 'fb_stage'
        $e.entity | Should -Be 'fb_entity'
    }

    It 'sanitizes _event: out of caller-supplied error_message in the prose half' {
        Write-ChunkFailedEvent -Stage 's' -Entity 'e' -ChunkIndex 0 `
            -ExceptionType 'X' -Message 'remote said _event: bad'
        $proseHalf = ($script:line -split '_event:', 2)[0]
        $proseHalf | Should -Not -Match '_event:'
        $proseHalf | Should -Match '_evnt_:'
    }
}

Describe 'Write-ItemFailedEvent (#356)' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'emits an item_failed event with all required properties' {
        Write-ItemFailedEvent -Stage 'channel_members' -Entity 'teams_channel_members' `
            -Category 'NonRetryable' -ItemId 'team1::ch1' -Attempt 1 -StatusCode 400 `
            -ExceptionType 'Microsoft.PowerShell.Commands.HttpResponseException' `
            -Message "Could not find a property named 'email' on type 'microsoft.graph.conversationMember'."
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.event_type     | Should -Be 'item_failed'
        $e.category       | Should -Be 'NonRetryable'
        $e.item_id        | Should -Be 'team1::ch1'
        $e.attempt        | Should -Be 1
        $e.status_code    | Should -Be 400
        $e.exception_type | Should -Be 'Microsoft.PowerShell.Commands.HttpResponseException'
        $e.error_message  | Should -Be "Could not find a property named 'email' on type 'microsoft.graph.conversationMember'."
        $e.stage          | Should -Be 'channel_members'
        $e.entity         | Should -Be 'teams_channel_members'
    }

    It 'accepts each of the five valid Category values' {
        foreach ($cat in @('NonRetryable','Skippable','AuthMaxRetries','UnknownMaxRetries','RetryExhausted')) {
            { Write-ItemFailedEvent -Stage 's' -Entity 'e' -Category $cat -ItemId 'i1' } | Should -Not -Throw
        }
    }

    It 'rejects an invalid Category' {
        { Write-ItemFailedEvent -Stage 's' -Entity 'e' -Category 'NotARealCategory' -ItemId 'i1' } | Should -Throw
    }

    It 'truncates Message at 500 chars with an ellipsis (mirrors chunk_failed)' {
        $long = 'x' * 600
        Write-ItemFailedEvent -Stage 's' -Entity 'e' -Category Skippable -ItemId 'i1' -Message $long
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.error_message.Length | Should -Be 501  # 500 chars + '…'
        $e.error_message        | Should -Match '…$'
    }

    It 'falls back to Set-EventScope when caller does not pass Stage/Entity' {
        Set-EventScope -Stage 'fb_stage' -Entity 'fb_entity'
        Write-ItemFailedEvent -Category Skippable -ItemId 'i1'
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.stage  | Should -Be 'fb_stage'
        $e.entity | Should -Be 'fb_entity'
    }

    It 'sanitizes _event: out of caller-supplied error_message in the prose half' {
        Write-ItemFailedEvent -Stage 's' -Entity 'e' -Category NonRetryable -ItemId 'i1' `
            -Message 'remote said _event: bad'
        $proseHalf = ($script:line -split '_event:', 2)[0]
        $proseHalf | Should -Not -Match '_event:'
        $proseHalf | Should -Match '_evnt_:'
    }
}

Describe 'LogHelper _event: sentinel guard' {
    It 'sanitizes _event: out of a non-emitter Write-Log message (does not throw)' {
        # Throwing in a logging path masks the real failure if upstream
        # error text happens to contain `_event:`. Sanitize-and-emit is
        # safer — the log line still reaches LAW, with the sentinel defanged.
        # 6>&1 captures the Information stream that Write-Host writes to.
        $output = (Write-Log 'this prose contains _event: which gets sanitized' 6>&1) | Out-String
        $output | Should -Not -Match '_event:[^_]'
        $output | Should -Match '_evnt_:'
    }

    It 'allows Write-Log when -AllowEventSentinel is set (used by EventEmitter)' {
        $output = (Write-Log 'fake event _event:{}' -AllowEventSentinel 6>&1) | Out-String
        $output | Should -Match '_event:\{\}'
    }
}

Describe 'EventEmitter prose sanitizes user-controlled fields' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'replaces _event: in stage_failed error_message before interpolating into prose' {
        Write-Event -EventType stage_failed -Stage 's' -Entity 'e' -Properties @{
            error_class   = 'Whatever'
            error_message = 'upstream said _event: oops'
            duration_ms   = 5
        }
        # The prose half (everything before the sentinel) must not contain
        # `_event:` — that would split KQL's `extract(@"_event:(.+)$", ...)`.
        # The JSON half can legitimately contain `_event:` inside string
        # values (it's escaped properly inside JSON strings; KQL extract is
        # greedy and the `parse_json` over the full match handles in-string
        # colons fine).
        $proseHalf = ($script:line -split '_event:', 2)[0]
        $proseHalf | Should -Not -Match '_event:'
        $proseHalf | Should -Match '_evnt_:'
    }
}

Describe 'Set-EventScope fallback for Write-ThrottleEvent' {
    BeforeEach {
        Initialize-EventContext -RunId 'r1' -Tenant 't1'
        $script:line = $null
        Mock -ModuleName EventEmitter Write-Log { $script:line = $Message }
    }

    It 'uses CurrentStage and CurrentEntity from scope when caller does not pass them' {
        Set-EventScope -Stage 'inline_root' -Entity 'foo'
        Write-ThrottleEvent -RetryAfterSeconds 5 -Attempt 1 -StatusCode 429 -Message 'TooManyRequests'
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.stage  | Should -Be 'inline_root'
        $e.entity | Should -Be 'foo'
    }

    It 'explicit Stage/Entity overrides Set-EventScope' {
        Set-EventScope -Stage 'inline_root' -Entity 'foo'
        Write-ThrottleEvent -Stage 'explicit_stage' -Entity 'bar' `
            -RetryAfterSeconds 5 -Attempt 1 -StatusCode 429 -Message 'x'
        $e = ($script:line -replace '^.+?_event:', '') | ConvertFrom-Json -AsHashtable
        $e.stage  | Should -Be 'explicit_stage'
        $e.entity | Should -Be 'bar'
    }
}
