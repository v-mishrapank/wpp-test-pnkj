#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Unit tests for RetryHelper.psm1: error classification, backoff, and the
# Invoke-WithRetry wrapper. Get-HttpStatusCode is module-internal and is
# tested via InModuleScope.

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    Import-Module (Join-Path $modulesPath 'LogHelper.psm1')    -Force
    Import-Module (Join-Path $modulesPath 'EventEmitter.psm1') -Force
    Import-Module (Join-Path $modulesPath 'RetryHelper.psm1')  -Force
    Initialize-EventContext -RunId 'test-run' -Tenant 'test-tenant'

    function script:NewHttpException {
        # Build an exception with the .Response.StatusCode shape produced by
        # Invoke-RestMethod / Invoke-WebRequest in PS 7.4+.
        param([int]$StatusCode, [hashtable]$Headers = $null, [string]$Message = "simulated $StatusCode")
        $resp = [PSCustomObject]@{
            StatusCode = $StatusCode
            Headers    = $Headers
        }
        $ex = [System.Exception]::new($Message)
        Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
        return $ex
    }

    function script:NewErrorRecord {
        # Wrap an exception in an ErrorRecord so it matches the $_ shape
        # that catch blocks pass into Get-ErrorClassification.
        param([System.Exception]$Exception)
        return [System.Management.Automation.ErrorRecord]::new(
            $Exception, 'TestError',
            [System.Management.Automation.ErrorCategory]::NotSpecified, $null)
    }
}

Describe 'Get-HttpStatusCode' {
    It 'returns null for a plain exception with no response shape' {
        InModuleScope RetryHelper {
            Get-HttpStatusCode -Exception ([System.Exception]::new('plain')) | Should -BeNullOrEmpty
        }
    }

    It 'extracts StatusCode from .Response.StatusCode' {
        InModuleScope RetryHelper -Parameters @{ ex = (NewHttpException -StatusCode 429) } {
            param($ex)
            Get-HttpStatusCode -Exception $ex | Should -Be 429
        }
    }

    It 'extracts StatusCode directly off the exception (HttpRequestException shape)' {
        InModuleScope RetryHelper {
            $ex = [System.Exception]::new('direct')
            Add-Member -InputObject $ex -MemberType NoteProperty -Name StatusCode -Value 503
            Get-HttpStatusCode -Exception $ex | Should -Be 503
        }
    }

    It 'extracts ResponseStatusCode (Graph SDK ODataError shape)' {
        InModuleScope RetryHelper {
            $ex = [System.Exception]::new('odata')
            Add-Member -InputObject $ex -MemberType NoteProperty -Name ResponseStatusCode -Value 404
            Get-HttpStatusCode -Exception $ex | Should -Be 404
        }
    }

    It 'walks InnerException chain to find a typed status code' {
        InModuleScope RetryHelper -Parameters @{ inner = (NewHttpException -StatusCode 401) } {
            param($inner)
            $outer = [System.Exception]::new('wrapper', $inner)
            Get-HttpStatusCode -Exception $outer | Should -Be 401
        }
    }

    It 'returns null when no exception in the chain has a status code' {
        InModuleScope RetryHelper {
            $deep = [System.Exception]::new('inner')
            $mid  = [System.Exception]::new('mid', $deep)
            $top  = [System.Exception]::new('top', $mid)
            Get-HttpStatusCode -Exception $top | Should -BeNullOrEmpty
        }
    }
}

Describe 'Get-ErrorClassification by HTTP status code' {
    It 'classifies 400 as NonRetryable' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 400)
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph'
        $r.Category   | Should -Be 'NonRetryable'
        $r.StatusCode | Should -Be 400
    }

    It 'classifies 401 as Auth' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 401)
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph'
        $r.Category   | Should -Be 'Auth'
        $r.StatusCode | Should -Be 401
    }

    It 'classifies 429 as Throttle' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 429)
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'Throttle'
    }

    It 'classifies 503 as Throttle' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 503)
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'Throttle'
    }

    It 'classifies 404 as Skippable' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 404)
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'Skippable'
    }

    It 'classifies 403 as Auth for graph' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 403)
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'Auth'
    }

    It 'classifies 403 as Auth for exo' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 403)
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'exo').Category | Should -Be 'Auth'
    }

    It 'classifies 403 as Skippable for spo (issue #165)' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 403)
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'spo').Category | Should -Be 'Skippable'
    }

    It 'classifies 403 as Skippable for mde' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 403)
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'mde').Category | Should -Be 'Skippable'
    }

    It 'extracts Retry-After from response headers (typed dictionary)' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 429 -Headers @{ 'Retry-After' = '13' })
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').RetryAfter | Should -Be 13
    }

    It 'classifies 409 as Skippable for powerplat with NoPermissionsForEmbeddedApplications body (issue #344)' {
        # BAP returns this on SharepointFormApp / other Microsoft-internal embedded
        # apps when the role-assignments child hits /apps/{id}/permissions. The
        # body code lives in ErrorDetails (Invoke-RestMethod -ErrorAction Stop).
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 409 -Message '409 Conflict')
        $rec.ErrorDetails = [System.Management.Automation.ErrorDetails]::new(
            '{"error":{"code":"NoPermissionsForEmbeddedApplications","message":"No permission to the SharepointFormApp application that has ID = 9fa26049-b3e2-42d5-8e8c-64012493ec97."}}')
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'powerplat').Category | Should -Be 'Skippable'
    }

    It 'classifies 409 as NonRetryable for powerplat without the embedded-app body marker' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 409 -Message '409 Conflict')
        $rec.ErrorDetails = [System.Management.Automation.ErrorDetails]::new(
            '{"error":{"code":"ResourceVersionConflict","message":"etag mismatch"}}')
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'powerplat').Category | Should -Be 'NonRetryable'
    }

    It 'classifies 409 as NonRetryable for other API families regardless of body' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 409 -Message '409 Conflict')
        $rec.ErrorDetails = [System.Management.Automation.ErrorDetails]::new(
            '{"error":{"code":"NoPermissionsForEmbeddedApplications"}}')
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'NonRetryable'
    }
}

Describe 'Get-ErrorClassification RETRY_EXHAUSTED marker (#526)' {
    It 'classifies a RETRY_EXHAUSTED-marked wrapper as RetryExhausted, beating the inner status code' {
        # The wrapped original (persistent 500) sits on the InnerException
        # chain — without the early marker check it would classify Unknown
        # and outer retry layers would multiply on top of the inner budget.
        $inner = NewHttpException -StatusCode 500 -Message '500 AzureResourceManagerServerError'
        $wrapper = [System.InvalidOperationException]::new('RETRY_EXHAUSTED: 500 AzureResourceManagerServerError', $inner)
        $r = Get-ErrorClassification -ErrorRecord (NewErrorRecord -Exception $wrapper) -ApiFamily 'powerplat'
        $r.Category   | Should -Be 'RetryExhausted'
        # Status code from the wrapped original is preserved for telemetry.
        $r.StatusCode | Should -Be 500
    }

    It 'detects the marker anywhere on the InnerException chain — a catch-and-rethrow wrapper cannot bury it' {
        # A fetcher that catches the RETRY_EXHAUSTED wrapper and re-throws it
        # inside its own context exception must not silently revert the path
        # to nested 5 × 5 retries.
        $orig   = NewHttpException -StatusCode 500 -Message '500 server error'
        $marker = [System.InvalidOperationException]::new('RETRY_EXHAUSTED: 500 server error', $orig)
        $rewrap = [System.Exception]::new('fetch context: bootstrap failed', $marker)
        (Get-ErrorClassification -ErrorRecord (NewErrorRecord -Exception $rewrap) -ApiFamily 'powerplat').Category | Should -Be 'RetryExhausted'
    }

    It 'ignores RETRY_EXHAUSTED appearing mid-message — the per-level check is prefix-anchored' {
        $inner = [System.Exception]::new('server said: RETRY_EXHAUSTED: but this is just quoted text')
        $outer = [System.Exception]::new('plain wrapper', $inner)
        (Get-ErrorClassification -ErrorRecord (NewErrorRecord -Exception $outer) -ApiFamily 'graph').Category | Should -Be 'Unknown'
    }
}

Describe 'Get-ErrorClassification by message pattern (no status code)' {
    It 'matches Auth patterns regardless of family' {
        $ex = [System.Exception]::new('Access token has expired and cannot be used')
        $rec = NewErrorRecord -Exception $ex
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'Auth'
    }

    It 'matches base throttle patterns for any family' {
        $ex = [System.Exception]::new('Server Busy please retry later')
        $rec = NewErrorRecord -Exception $ex
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'Throttle'
    }

    It 'matches EXO-only throttle patterns when family=exo' {
        $ex = [System.Exception]::new('MicroDelay applied')
        $rec = NewErrorRecord -Exception $ex
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'exo').Category | Should -Be 'Throttle'
    }

    It 'leaves EXO-only throttle pattern Unknown when family=graph' {
        $ex = [System.Exception]::new('MicroDelay applied')
        $rec = NewErrorRecord -Exception $ex
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'Unknown'
    }

    It 'tags EXO Collection-was-modified as Unknown with #343 prefix (issue #343)' {
        $ex = [System.InvalidOperationException]::new('Collection was modified; enumeration operation may not execute.')
        $rec = NewErrorRecord -Exception $ex
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'exo'
        $r.Category | Should -Be 'Unknown'
        $r.Message  | Should -Match 'EXO_CONCURRENT_MUTATION'
    }

    It 'does not tag Collection-was-modified for non-exo families' {
        $ex = [System.InvalidOperationException]::new('Collection was modified; enumeration operation may not execute.')
        $rec = NewErrorRecord -Exception $ex
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph'
        $r.Category | Should -Be 'Unknown'
        $r.Message  | Should -Not -Match 'EXO_CONCURRENT_MUTATION'
    }

    It 'matches graph Skippable pattern (Request_ResourceNotFound)' {
        $ex = [System.Exception]::new('Request_ResourceNotFound: Resource does not exist')
        $rec = NewErrorRecord -Exception $ex
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'Skippable'
    }

    It "matches exo Skippable pattern (couldn't be found)" {
        $ex = [System.Exception]::new("The user couldn't be found in this tenant")
        $rec = NewErrorRecord -Exception $ex
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'exo').Category | Should -Be 'Skippable'
    }

    It 'matches spo Skippable pattern (locked site)' {
        # The spo Skippable regex includes 'Attempted to perform an unauthorized
        # operation' (issue #165), but the Auth pattern's 'Unauthorized' alt
        # short-circuits any message containing 'unauthorized'. In practice
        # those errors arrive with a 403 status (see status-code tests above);
        # this test exercises a different SPO-only pattern that doesn't collide.
        $ex = [System.Exception]::new('Site is locked')
        $rec = NewErrorRecord -Exception $ex
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'spo').Category | Should -Be 'Skippable'
    }

    It 'matches spo Skippable pattern (system site rejecting GetSitePropertiesByUrl with 500 "not supported for site")' {
        # contentTypeHub / CompliancePolicyCenter / search system sites reject
        # GetSitePropertiesByUrl with HTTP 500 + body containing "The requested
        # operation is not supported for site". The 500 falls through the status-
        # code switch into message matching, so the skippable pattern needs to
        # catch this here. See #471.
        $ex = [System.Exception]::new('Response status code does not indicate success: 500 (Internal Server Error). | body={"error":{"code":"-2147213239","message":{"value":"The requested operation is not supported for site: https://tenant.sharepoint.com/sites/contentTypeHub"}}}')
        $rec = NewErrorRecord -Exception $ex
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'spo').Category | Should -Be 'Skippable'
    }

    It 'returns Unknown for messages that match no pattern' {
        $ex = [System.Exception]::new('something inexplicable happened')
        $rec = NewErrorRecord -Exception $ex
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'Unknown'
    }

    It 'parses Retry-After from message text when no response object is available' {
        $ex = [System.Exception]::new('TooManyRequests Retry-After: 23 try again')
        $rec = NewErrorRecord -Exception $ex
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph'
        $r.Category   | Should -Be 'Throttle'
        $r.RetryAfter | Should -Be 23
    }

    It 'walks InnerException chain to find the deepest non-empty message' {
        $deep = [System.Exception]::new('Request_ResourceNotFound: deep')
        $top  = [System.Exception]::new('', $deep)
        $rec  = NewErrorRecord -Exception $top
        (Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph').Category | Should -Be 'Skippable'
    }

    It 'falls back to type name when message is whitespace and no inner exception' {
        $ex = [System.InvalidOperationException]::new('   ')
        $rec = NewErrorRecord -Exception $ex
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph'
        $r.Message | Should -Match 'InvalidOperationException'
    }
}

Describe 'Get-ErrorClassification response body enrichment' {
    # Invoke-RestMethod -ErrorAction Stop captures non-2xx response bodies
    # on $ErrorRecord.ErrorDetails — ODataError JSON ("Could not find a
    # property named 'foo'") lives there, not on the exception message.
    # Without this enrichment a 400 surfaces as just "400 Bad Request".
    It 'appends ErrorDetails.Message body to classification Message' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 400 -Message '400 Bad Request')
        $rec.ErrorDetails = [System.Management.Automation.ErrorDetails]::new(
            "Could not find a property named 'version' on type 'Microsoft.Dynamics.CRM.webresource'.")
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph'
        $r.Message | Should -Match '400 Bad Request'
        $r.Message | Should -Match "Could not find a property named 'version'"
        $r.Message | Should -Match 'body='
    }

    It 'truncates long bodies at 500 chars with ellipsis' {
        $longBody = 'X' * 800
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 400)
        $rec.ErrorDetails = [System.Management.Automation.ErrorDetails]::new($longBody)
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph'
        # Message = "<orig> | body=<500 X's>…"  → guard against unbounded growth
        $r.Message | Should -Match 'body=X{500}…'
        $r.Message.Length | Should -BeLessThan 600
    }

    It 'leaves Message unchanged when ErrorDetails is absent' {
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 400 -Message '400 Bad Request')
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph'
        $r.Message    | Should -Be '400 Bad Request'
        $r.Message    | Should -Not -Match 'body='
    }

    # Invoke-MgGraphRequest stores the full HTTP wire format (request line +
    # request headers + blank + response status + response headers + blank +
    # body) in ErrorDetails.Message via HttpMessageFormatter — see #405.
    # We pass that through to body= verbatim (the wire dump carries useful
    # diagnostic context like the request URL and request-id). The
    # newline-row-split problem this used to cause is handled at Write-Log,
    # not here. Two invariants for this path:
    #   - matchText still scans the body, so embedded codes (AF429,
    #     Request_ResourceNotFound) still classify correctly.
    #   - The 500-char cap still applies, so the line stays bounded.
    It 'classifies AF429 carried in wire-format ErrorDetails as Throttle (#405)' {
        $wire = @(
            'GET https://manage.office.com/api/v1.0/tenant/activity/feed/subscriptions/content?...'
            ''
            'HTTP/2.0 403 Forbidden'
            'Content-Type: application/json'
            ''
            '{"error":{"code":"AF429","message":"quota exceeded"}}'
        ) -join "`r`n"
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 403 -Message '403 Forbidden')
        $rec.ErrorDetails = [System.Management.Automation.ErrorDetails]::new($wire)
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'log'
        $r.Category  | Should -Be 'Throttle'
        $r.Message   | Should -Match 'AF429'
        $r.Message   | Should -Match 'body='
    }

    It 'classifies Request_ResourceNotFound carried in wire-format ErrorDetails as Skippable (#405)' {
        $wire = @(
            'GET https://graph.microsoft.com/v1.0/teams/21eb0e73-a63e-4496-bc4e-411f1e7f74e6/channels'
            ''
            'HTTP/2.0 404 Not Found'
            'Vary: Accept-Encoding'
            'request-id: 12174612-d069-4b08-b09b-b745189b7362'
            ''
            '{"error":{"code":"Request_ResourceNotFound","message":"Resource does not exist."}}'
        ) -join "`r`n"
        $rec = NewErrorRecord -Exception (NewHttpException -StatusCode 404 -Message '404 Not Found')
        $rec.ErrorDetails = [System.Management.Automation.ErrorDetails]::new($wire)
        $r = Get-ErrorClassification -ErrorRecord $rec -ApiFamily 'graph'
        $r.Category  | Should -Be 'Skippable'
        $r.Message   | Should -Match 'body='
        # We pass the wire dump through verbatim; the bytes survive into
        # body=. Write-Log normalizes the CR/LF to literal \n at emit time.
        $r.Message   | Should -Match 'HTTP/2\.0 404 Not Found'
    }
}

Describe 'Get-RetryDelay' {
    It 'returns RetryAfter when classification carries one' {
        $cls = @{ Category='Throttle'; RetryAfter=17 }
        Get-RetryDelay -Classification $cls -Attempt 1 | Should -Be 17
    }

    It 'ignores RetryAfter when 0 or negative' {
        $cls = @{ Category='Throttle'; RetryAfter=0 }
        $d = Get-RetryDelay -Classification $cls -Attempt 1 -BaseDelay 2 -MaxDelay 60
        $d | Should -BeGreaterOrEqual 2
    }

    It 'computes exponential backoff for early attempts' {
        $cls = @{ Category='Unknown'; RetryAfter=$null }
        # Attempt=1 -> exp = BaseDelay * 2^0 = 2; jitter ∈ [0, 0.6]; total ∈ [2, 2.6]
        $d = Get-RetryDelay -Classification $cls -Attempt 1 -BaseDelay 2 -MaxDelay 120
        $d | Should -BeGreaterOrEqual 2
        $d | Should -BeLessOrEqual 2.7
    }

    It 'caps at MaxDelay even with high attempts' {
        $cls = @{ Category='Unknown'; RetryAfter=$null }
        # Attempt=20 -> exp=base*2^19 which cap clamps to MaxDelay; jitter adds at most 30%
        $d = Get-RetryDelay -Classification $cls -Attempt 20 -BaseDelay 2 -MaxDelay 10
        $d | Should -BeLessOrEqual 13.1
    }
}

Describe 'Invoke-WithRetry' {
    BeforeAll {
        # Cheap stub for Start-Sleep so retries don't actually wait. Pester
        # 5's Mock works at module scope, which is what Invoke-WithRetry uses.
        # Write-Log + Write-UnknownRetryEvent are mocked so the #327 emission
        # test below can assert call counts; the other Invoke-WithRetry tests
        # don't care which way they're stubbed.
        InModuleScope RetryHelper {
            Mock Start-Sleep { }
            Mock Write-Log { }
            Mock Write-UnknownRetryEvent { }
        }
    }

    It 'returns the script block result on first success' {
        $r = Invoke-WithRetry -ScriptBlock { return 'ok' }
        $r | Should -Be 'ok'
    }

    It 'wraps Skippable errors in InvalidOperationException with SKIPPABLE prefix' {
        $script:Calls = 0
        try {
            Invoke-WithRetry -ScriptBlock {
                $script:Calls++
                throw 'Request_ResourceNotFound'
            } -ApiFamily 'graph' -MaxRetries 5
            throw 'should not reach'
        } catch [System.InvalidOperationException] {
            $_.Exception.Message | Should -Match 'SKIPPABLE'
        }
        $script:Calls | Should -Be 1   # no retry on Skippable
    }

    It 'retries on Throttle and eventually succeeds' {
        $script:Calls = 0
        $r = Invoke-WithRetry -ScriptBlock {
            $script:Calls++
            if ($script:Calls -lt 3) { throw 'TooManyRequests' }
            return 'ok'
        } -ApiFamily 'graph' -MaxRetries 5 -BaseDelay 1 -MaxDelay 1
        $r | Should -Be 'ok'
        $script:Calls | Should -Be 3
    }

    It 'invokes OnAuthReconnect on Auth errors before retrying' {
        $script:Calls = 0
        $script:Reconnects = 0
        $r = Invoke-WithRetry -ScriptBlock {
            $script:Calls++
            if ($script:Calls -lt 2) { throw 'Unauthorized' }
            return 'ok'
        } -ApiFamily 'graph' -MaxRetries 5 -OnAuthReconnect { $script:Reconnects++ }
        $r | Should -Be 'ok'
        $script:Reconnects | Should -Be 1
    }

    It 'rethrows after exceeding MaxRetries' {
        $script:Calls = 0
        { Invoke-WithRetry -ScriptBlock {
            $script:Calls++
            throw 'TooManyRequests'
        } -ApiFamily 'graph' -MaxRetries 2 -BaseDelay 1 -MaxDelay 1 } | Should -Throw
        $script:Calls | Should -Be 3   # initial + 2 retries
    }

    It 'does NOT mark Throttle exhaustion with RETRY_EXHAUSTED (#526)' {
        # 429/503 weather is about the API, not the item — outer layers may
        # legitimately retry a throttle-exhausted item later, so the bare
        # rethrow is preserved.
        try {
            Invoke-WithRetry -ScriptBlock {
                throw 'TooManyRequests'
            } -ApiFamily 'graph' -MaxRetries 1 -BaseDelay 1 -MaxDelay 1
            throw 'should not reach'
        } catch {
            $_.Exception.Message | Should -Not -Match 'RETRY_EXHAUSTED'
        }
    }

    It 'wraps Unknown exhaustion in InvalidOperationException with RETRY_EXHAUSTED prefix (#526)' {
        $script:Calls = 0
        try {
            Invoke-WithRetry -ScriptBlock {
                $script:Calls++
                throw 'unmapped server hiccup'
            } -ApiFamily 'graph' -MaxRetries 2 -BaseDelay 1 -MaxDelay 1
            throw 'should not reach'
        } catch [System.InvalidOperationException] {
            $_.Exception.Message | Should -Match '^RETRY_EXHAUSTED:'
            # Original error preserved on the chain for status/type telemetry.
            $_.Exception.InnerException.Message | Should -Match 'unmapped server hiccup'
        }
        $script:Calls | Should -Be 3   # initial + 2 retries, then marked throw
    }

    It 'wraps Auth exhaustion in RETRY_EXHAUSTED too (#526)' {
        $script:Reconnects = 0
        try {
            Invoke-WithRetry -ScriptBlock {
                throw 'Unauthorized'
            } -ApiFamily 'graph' -MaxRetries 2 -OnAuthReconnect { $script:Reconnects++ }
            throw 'should not reach'
        } catch [System.InvalidOperationException] {
            $_.Exception.Message | Should -Match '^RETRY_EXHAUSTED:'
        }
        $script:Reconnects | Should -Be 2
    }

    It 'rethrows an already-exhausted error immediately without re-retrying (#526)' {
        # Nested Invoke-WithRetry (helper wrapping a helper): the inner one
        # already spent its budget, so the outer must not multiply on top.
        $script:Calls = 0
        try {
            Invoke-WithRetry -ScriptBlock {
                $script:Calls++
                throw [System.InvalidOperationException]::new(
                    'RETRY_EXHAUSTED: inner gave up', [System.Exception]::new('boom'))
            } -ApiFamily 'graph' -MaxRetries 5 -BaseDelay 1 -MaxDelay 1
            throw 'should not reach'
        } catch [System.InvalidOperationException] {
            $_.Exception.Message | Should -Match '^RETRY_EXHAUSTED:'
        }
        $script:Calls | Should -Be 1   # no retry on an already-exhausted error
    }

    It 'retries Unknown errors with conservative backoff' {
        $script:Calls = 0
        $r = Invoke-WithRetry -ScriptBlock {
            $script:Calls++
            if ($script:Calls -lt 2) { throw 'unmapped server hiccup' }
            return 'ok'
        } -ApiFamily 'graph' -MaxRetries 3 -BaseDelay 1 -MaxDelay 1
        $r | Should -Be 'ok'
        $script:Calls | Should -Be 2
    }

    It 'emits WARN log + unknown_retry_event before sleeping on Unknown (#327)' {
        # Pre-#327 the Unknown branch was a bare Start-Sleep with no log
        # and no telemetry — workers in this state were indistinguishable
        # from a wedged container. Lock the emission down so the silent-
        # retry observability gap can't regress.
        InModuleScope RetryHelper {
            $script:Calls = 0
            $r = Invoke-WithRetry -ScriptBlock {
                $script:Calls++
                if ($script:Calls -lt 3) { throw 'unmapped server hiccup' }
                return 'ok'
            } -ApiFamily 'graph' -MaxRetries 5 -BaseDelay 1 -MaxDelay 1
            $r | Should -Be 'ok'
            # 3 attempts → 2 retries, so 2 WARN+event emits before the
            # successful third call.
            Should -Invoke -CommandName Write-Log -Times 2 -Exactly -ParameterFilter {
                $Level -eq 'WARN' -and $Message -match 'unknown error'
            }
            Should -Invoke -CommandName Write-UnknownRetryEvent -Times 2 -Exactly -ParameterFilter {
                $ApiFamily -eq 'graph'
            }
        }
    }

    It 'does not retry NonRetryable (400) and surfaces enriched message' {
        # Build an ErrorRecord-style throw: a 400 with a response body so the
        # classifier appends `| body=...` to Message. The wrap in
        # Invoke-WithRetry must surface that enriched text on Exception.Message
        # so callers logging $_.Exception.Message see the offending field name.
        $script:Calls = 0
        $thrown = $null
        try {
            Invoke-WithRetry -ScriptBlock {
                $script:Calls++
                $ex = NewHttpException -StatusCode 400 -Message '400 Bad Request'
                $rec = [System.Management.Automation.ErrorRecord]::new(
                    $ex, 'TestError',
                    [System.Management.Automation.ErrorCategory]::NotSpecified, $null)
                $rec.ErrorDetails = [System.Management.Automation.ErrorDetails]::new(
                    "Could not find a property named 'version'")
                throw $rec
            } -ApiFamily 'graph' -MaxRetries 5
            throw 'should not reach'
        } catch {
            $thrown = $_
        }
        $script:Calls                  | Should -Be 1
        $thrown.Exception              | Should -BeOfType ([System.InvalidOperationException])
        $thrown.Exception.Message      | Should -Match 'NONRETRYABLE'
        $thrown.Exception.Message      | Should -Match "Could not find a property named 'version'"
        $thrown.Exception.InnerException | Should -Not -BeNullOrEmpty
    }
}
