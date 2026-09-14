#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Unit tests for SpoRestClient.psm1 — the REST helpers used by every SP
# entity fetcher. Covers envelope unwrapping, pagination, and the /_api/$batch
# multipart composer + parser.

BeforeAll {
    $script:SharedModules = Join-Path $PSScriptRoot '..' '..' 'shared' 'modules'
    $script:ModulePath    = Join-Path $PSScriptRoot '..' 'scripts' 'SpoRestClient.psm1'
    Import-Module (Join-Path $script:SharedModules 'LogHelper.psm1')    -Force
    Import-Module (Join-Path $script:SharedModules 'EventEmitter.psm1') -Force
    Import-Module (Join-Path $script:SharedModules 'RetryHelper.psm1')  -Force
    Import-Module $script:ModulePath -Force -DisableNameChecking
}

AfterAll {
    Remove-Module SpoRestClient, RetryHelper, EventEmitter, LogHelper -Force -ErrorAction SilentlyContinue
}

Describe 'ConvertFrom-SpoEnvelope — verbose envelope unwrapping' {
    It 'unwraps {d:{results:[...]}} to the array' {
        InModuleScope 'SpoRestClient' {
            $resp = [pscustomobject]@{
                d = [pscustomobject]@{
                    results = @(
                        [pscustomobject]@{ Title = 'a' }
                        [pscustomobject]@{ Title = 'b' }
                    )
                }
            }
            $out = ConvertFrom-SpoEnvelope -Response $resp
            $out.Count | Should -Be 2
            $out[0].Title | Should -Be 'a'
        }
    }

    It 'unwraps {d:{...}} single-entity to the inner object' {
        InModuleScope 'SpoRestClient' {
            $resp = [pscustomobject]@{
                d = [pscustomobject]@{ Title = 'site'; ServerRelativeUrl = '/sites/x' }
            }
            $out = ConvertFrom-SpoEnvelope -Response $resp
            $out.Title | Should -Be 'site'
            $out.ServerRelativeUrl | Should -Be '/sites/x'
        }
    }

    It 'passes through non-envelope shapes unchanged' {
        InModuleScope 'SpoRestClient' {
            $resp = [pscustomobject]@{ value = @('x','y'); CustomField = 1 }
            $out = ConvertFrom-SpoEnvelope -Response $resp
            $out.value.Count | Should -Be 2
            $out.CustomField | Should -Be 1
        }
    }

    It 'returns $null for $null input' {
        InModuleScope 'SpoRestClient' {
            ConvertFrom-SpoEnvelope -Response $null | Should -BeNullOrEmpty
        }
    }
}

Describe 'Get-SpoNextLink — pagination cursor extraction' {
    It 'reads .d.__next (verbose envelope)' {
        InModuleScope 'SpoRestClient' {
            $resp = [pscustomobject]@{
                d = [pscustomobject]@{ results = @(); __next = 'https://t/_api/web?$skip=10' }
            }
            Get-SpoNextLink -Response $resp | Should -Be 'https://t/_api/web?$skip=10'
        }
    }

    It 'ignores @odata.nextLink (nometadata/Graph-style — verbose-only by design)' {
        InModuleScope 'SpoRestClient' {
            # Verbose-only contract: the SP REST client always requests
            # `Accept: application/json;odata=verbose`, so we never receive
            # `@odata.nextLink`. ConvertFrom-SpoEnvelope doesn't extract
            # `value`-shape rows either, so supporting one without the other
            # would silently zero-row-loop on a Graph-style response.
            $resp = [pscustomobject]@{ '@odata.nextLink' = 'https://t/page2' }
            Get-SpoNextLink -Response $resp | Should -BeNullOrEmpty
        }
    }

    It 'returns $null when no .d.__next cursor present' {
        InModuleScope 'SpoRestClient' {
            Get-SpoNextLink -Response ([pscustomobject]@{ d = [pscustomobject]@{ results = @() } }) | Should -BeNullOrEmpty
        }
    }
}

Describe 'Invoke-SpoRestPaged — pagination loop' {
    # Pester Mock scriptblocks run in a separate scope so neither $script:
    # nor $global: written inside them is visible to the It block. The
    # reliable pattern: keep state in a Pester-owned hashtable referenced
    # by name from inside the Mock — we use $global: for cross-scope
    # visibility, and clear it at the top of each It to avoid leaks
    # between tests.
    BeforeEach { $global:SpoTestState = @{ Calls = 0; Caught = @(); Reconnected = $false } }
    AfterEach  { Remove-Variable -Scope Global -Name SpoTestState -ErrorAction SilentlyContinue }

    It 'walks through __next pages until exhausted, calling OnRow per record' {
        InModuleScope 'SpoRestClient' {
            # Mock Invoke-WebRequest (not Invoke-RestMethod) because the
            # production code switched to Invoke-WebRequest + ConvertFrom-Json
            # -AsHashtable to work around Invoke-RestMethod's chunked-encoding
            # silent-string-return on SP's /Items endpoint. The mock returns a
            # response shape with .StatusCode and .Content (JSON string), and
            # the production helper parses it with ConvertFrom-Json -AsHashtable.
            Mock Invoke-WebRequest -MockWith {
                $global:SpoTestState.Calls++
                if ($global:SpoTestState.Calls -eq 1) {
                    $json = '{"d":{"results":[{"Id":1},{"Id":2}],"__next":"https://t/_api/web/SiteUsers?$skip=2"}}'
                } else {
                    $json = '{"d":{"results":[{"Id":3}],"__next":null}}'
                }
                return [pscustomobject]@{ StatusCode = 200; Content = $json }
            }

            $total = Invoke-SpoRestPaged -Url 'https://t/_api/web/SiteUsers' -Token 'tok' -OnRow {
                param($row)
                $global:SpoTestState.Caught += [int]$row.Id
            }
            $total | Should -Be 3
            $global:SpoTestState.Calls | Should -Be 2
            ($global:SpoTestState.Caught -join ',') | Should -Be '1,2,3'
        }
    }

    It 'retries on 429 with backoff (via Invoke-WithRetry classification)' {
        InModuleScope 'SpoRestClient' {
            Mock Start-Sleep -MockWith { }
            Mock Invoke-WebRequest -MockWith {
                $global:SpoTestState.Calls++
                if ($global:SpoTestState.Calls -eq 1) {
                    # .NET 8 HttpRequestException already has a native
                    # StatusCode property — set it via the constructor.
                    # Get-HttpStatusCode (RetryHelper) finds it via .Response.
                    $resp = [pscustomobject]@{ StatusCode = 429; Headers = @{ 'Retry-After' = '1' } }
                    $ex = [System.Net.Http.HttpRequestException]::new('Too Many Requests', $null, [System.Net.HttpStatusCode]::TooManyRequests)
                    $ex | Add-Member -NotePropertyName Response -NotePropertyValue $resp
                    throw $ex
                }
                return [pscustomobject]@{ StatusCode = 200; Content = '{"d":{"results":[{"Id":1}],"__next":null}}' }
            }

            $total = Invoke-SpoRestPaged -Url 'https://t/_api/web/SiteUsers' -Token 'tok' -OnRow {
                param($row) ; $global:SpoTestState.Caught += [int]$row.Id
            }
            $total | Should -Be 1
            $global:SpoTestState.Calls | Should -Be 2
        }
    }

    It 'reconnects on 401 then retries with new token' {
        InModuleScope 'SpoRestClient' {
            Mock Invoke-WebRequest -MockWith {
                $global:SpoTestState.Calls++
                if ($global:SpoTestState.Calls -eq 1) {
                    $resp = [pscustomobject]@{ StatusCode = 401; Headers = @{} }
                    $ex = [System.Net.Http.HttpRequestException]::new('Unauthorized', $null, [System.Net.HttpStatusCode]::Unauthorized)
                    $ex | Add-Member -NotePropertyName Response -NotePropertyValue $resp
                    throw $ex
                }
                return [pscustomobject]@{ StatusCode = 200; Content = '{"d":{"results":[{"Id":7}],"__next":null}}' }
            }

            $reconnect = { $global:SpoTestState.Reconnected = $true }
            # -OnAuthReconnect requires a refreshable token source. -Token is
            # frozen at call-entry, so reconnect would re-send the stale value
            # — the helper now enforces -GetToken in that combination.
            $getToken = { 'tok' }
            $total = Invoke-SpoRestPaged -Url 'https://t/_api/web/SiteUsers' -GetToken $getToken -OnAuthReconnect $reconnect -OnRow {
                param($row) ; $global:SpoTestState.Caught += [int]$row.Id
            }
            $total | Should -Be 1
            $global:SpoTestState.Calls | Should -Be 2
            $global:SpoTestState.Reconnected | Should -BeTrue
        }
    }
}

Describe 'Build-SpoBatchBody — multipart composition' {
    It 'composes one part per request with HTTP/1.1 envelope and Accept header' {
        InModuleScope 'SpoRestClient' {
            $body = Build-SpoBatchBody -Boundary 'batch_x' -Requests @(
                @{ Method = 'GET';  Url = '/_api/web' }
                @{ Method = 'POST'; Url = '/_api/web/SomeOp'; Body = '{}' }
            )
            $body | Should -Match '--batch_x\r\n'
            $body | Should -Match 'Content-Type: application/http'
            $body | Should -Match 'GET /_api/web HTTP/1.1'
            $body | Should -Match 'POST /_api/web/SomeOp HTTP/1.1'
            $body | Should -Match 'Accept: application/json;odata=verbose'
            $body | Should -Match '--batch_x--\r\n$'
            # POST body present
            $body | Should -Match '\{\}'
        }
    }

    It 'wraps write sub-requests (POST/PUT/MERGE/DELETE) in a ChangeSet, leaves reads (GET) flat' {
        # Regression guard for an SP REST $batch quirk we hit in production
        # against madev1 OneDrive doc libraries: 5 lists 400'd with
        # `Microsoft.Data.OData.ODataException` "An invalid HTTP method
        # 'POST' was detected for a query operation. Query operations only
        # support the HTTP 'GET' method." The misleading error obscures the
        # real cause — SP enforces the OData $batch spec strictly: write
        # sub-requests MUST be in a ChangeSet. A bare POST part is read as
        # a query operation. The fix wraps each write in its own ChangeSet,
        # GETs stay as flat batch parts. Confirmed against real SP via
        # cert-auth probe — both shapes return 200 with the expected
        # `SP.Sharing.SharingInformation` body when wrapped, both 400 when
        # left flat. (Per the SP $batch doc, multi-write ChangeSets are
        # not even guaranteed transactional, so the one-write-per-ChangeSet
        # granularity here costs nothing structurally.)
        InModuleScope 'SpoRestClient' {
            $body = Build-SpoBatchBody -Boundary 'batch_x' -Requests @(
                @{ Method = 'GET';  Url = '/_api/web' }
                @{ Method = 'POST'; Url = '/_api/web/SomeOp'; Body = '{}' }
                @{ Method = 'POST'; Url = '/_api/web/OtherOp'; Body = '{"k":"v"}' }
                @{ Method = 'MERGE'; Url = '/_api/web/lists(guid:x)'; Body = '{}' }
            )
            # GET is a flat batch part — no nested changeset wrapper around it.
            $body | Should -Match '(?ms)--batch_x\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\r\nGET /_api/web HTTP/1.1'
            # POST + MERGE each spawn their own ChangeSet wrapper.
            $changesets = [regex]::Matches($body, '--batch_x\r\nContent-Type: multipart/mixed; boundary=changeset_')
            $changesets.Count | Should -Be 3
            # Each ChangeSet has its own boundary, the POST/MERGE inside, then a closing marker.
            $body | Should -Match '(?ms)Content-Type: multipart/mixed; boundary=(changeset_[a-f0-9]+)\r\n\r\n--\1\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\r\nPOST /_api/web/SomeOp HTTP/1.1'
            $body | Should -Match '(?ms)--(changeset_[a-f0-9]+)\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\r\nMERGE /_api/web/lists\(guid:x\) HTTP/1.1'
            # POST bodies still land inside their respective ChangeSet wrappers.
            $body | Should -Match '\{"k":"v"\}'
            # Outer batch boundary closer comes last.
            $body | Should -Match '--batch_x--\r\n$'
        }
    }

    It 'throws when a sub-request is missing Url' {
        InModuleScope 'SpoRestClient' {
            { Build-SpoBatchBody -Boundary 'b' -Requests @(@{ Method = 'GET' }) } |
                Should -Throw "*missing 'Url'*"
        }
    }
}

Describe 'ConvertFrom-SpoBatchResponse — multipart response parsing' {
    It 'parses N sub-responses with status, headers, body in order' {
        InModuleScope 'SpoRestClient' {
            $raw = @"
--br
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 200 OK
Content-Type: application/json;odata=verbose

{"d":{"Title":"first"}}
--br
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 403 Forbidden
Content-Type: application/json

{"error":{"message":"AccessDenied"}}
--br--
"@
            $out = ConvertFrom-SpoBatchResponse -RawText $raw -Boundary 'br' -ExpectedCount 2
            $out.Count | Should -Be 2
            $out[0].Status | Should -Be 200
            $out[0].Body.Title | Should -Be 'first'
            $out[1].Status | Should -Be 403
            $out[1].Body.error.message | Should -Be 'AccessDenied'
        }
    }

    It 'throws when parsed count != expected' {
        InModuleScope 'SpoRestClient' {
            $raw = @"
--br
Content-Type: application/http

HTTP/1.1 200 OK
Content-Type: application/json

{"d":{}}
--br--
"@
            { ConvertFrom-SpoBatchResponse -RawText $raw -Boundary 'br' -ExpectedCount 5 } |
                Should -Throw '*parsed 1 sub-responses but sent 5*'
        }
    }

    It 'preserves array shape for a single-sub-response batch (no PS unwrap)' {
        InModuleScope 'SpoRestClient' {
            $raw = @"
--br
Content-Type: application/http

HTTP/1.1 200 OK
Content-Type: application/json

{"d":{"Title":"only"}}
--br--
"@
            $out = ConvertFrom-SpoBatchResponse -RawText $raw -Boundary 'br' -ExpectedCount 1
            # Without the `,` operator the function output stream would
            # unwrap to a bare hashtable here, and $out.Count would be the
            # hashtable's key count (3: Status, Headers, Body).
            ,$out -is [System.Array] | Should -BeTrue
            $out.Count   | Should -Be 1
            $out[0].Status | Should -Be 200
        }
    }
}

Describe 'Invoke-SpoBatch — sub-response retry policy (the #460 regression guard)' {
    BeforeEach {
        $global:SpoTestState = @{
            Calls    = 0
            Bodies   = @()  # captures raw text returned by each Invoke-WebRequest call
            Boundary = 'batchresponse_abc123'
        }
    }
    AfterEach { Remove-Variable -Scope Global -Name SpoTestState -ErrorAction SilentlyContinue }

    It 'retries the whole batch when any sub-response is 429, returns the eventual 200 array' {
        InModuleScope 'SpoRestClient' {
            Mock Start-Sleep -MockWith { }
            # On call 1: return a multipart body with one sub-response that's 429.
            # On call 2: return the same boundary but with a 200 sub-response.
            # Asserts the outer Invoke-WithRetry classifies the synthesized
            # exception as Throttle and re-sends the batch.
            $boundary = $global:SpoTestState.Boundary
            $throttledBody = @"
--$boundary
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 429 Too Many Requests
Retry-After: 1
Content-Type: application/json;odata=verbose

{"error":{"message":"throttled"}}
--$boundary--
"@
            $successBody = @"
--$boundary
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 200 OK
Content-Type: application/json;odata=verbose

{"d":{"Title":"unthrottled"}}
--$boundary--
"@

            Mock Invoke-WebRequest -MockWith {
                $global:SpoTestState.Calls++
                $bodyText = if ($global:SpoTestState.Calls -eq 1) { $throttledBody } else { $successBody }
                # Synthesize a response object with the same shape Invoke-WebRequest
                # produces — Headers is a dictionary-like with Content-Type carrying
                # the boundary, Content is the raw multipart bytes-as-string.
                $headers = @{ 'Content-Type' = "multipart/mixed; boundary=$($global:SpoTestState.Boundary)" }
                return [pscustomobject]@{ Headers = $headers; Content = $bodyText }
            }

            $resp = Invoke-SpoBatch -BatchUrl 'https://x/_api/$batch' `
                -Requests @(@{ Method = 'GET'; Url = '/_api/web' }) `
                -Token 'tok'

            $global:SpoTestState.Calls | Should -Be 2
            $resp.Count   | Should -Be 1
            $resp[0].Status | Should -Be 200
            $resp[0].Body.Title | Should -Be 'unthrottled'
        }
    }

    It 'handles byte[] Content from Invoke-WebRequest (multipart/mixed responses come back binary on success)' {
        # Regression guard: in production, after fixing the ChangeSet wrapping
        # bug, real SP started returning 200 multipart/mixed responses where
        # Invoke-WebRequest delivers `Content` as a `byte[]` rather than a
        # `string`. ConvertFrom-SpoBatchResponse's `-RawText` is declared
        # `[string]`, so the byte[] failed the param-binding cast with a
        # generic "Cannot convert value to type System.String" — no URL or
        # item context. Worse, RetryHelper classifies that as Unknown and
        # retried each item 5 times with exponential backoff before giving
        # up, masking the underlying parse failure entirely. The fix decodes
        # UTF-8 bytes to string before passing to ConvertFrom-SpoBatchResponse.
        InModuleScope 'SpoRestClient' {
            $boundary = $global:SpoTestState.Boundary
            $successBody = @"
--$boundary
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 200 OK
Content-Type: application/json;odata=verbose

{"d":{"Title":"from-bytes"}}
--$boundary--
"@
            Mock Invoke-WebRequest -MockWith {
                $headers = @{ 'Content-Type' = "multipart/mixed; boundary=$($global:SpoTestState.Boundary)" }
                # Real Invoke-WebRequest returns byte[] for multipart/mixed — mimic that.
                $bytes = [System.Text.Encoding]::UTF8.GetBytes($successBody)
                return [pscustomobject]@{ Headers = $headers; Content = $bytes }
            }

            $resp = Invoke-SpoBatch -BatchUrl 'https://x/_api/$batch' `
                -Requests @(@{ Method = 'POST'; Url = '/_api/web/SomeOp'; Body = '{}' }) `
                -Token 'tok'

            $resp.Count       | Should -Be 1
            $resp[0].Status   | Should -Be 200
            $resp[0].Body.Title | Should -Be 'from-bytes'
        }
    }

    It 'does NOT retry on sub-response 403 (permanent per-item denial)' {
        InModuleScope 'SpoRestClient' {
            $boundary = $global:SpoTestState.Boundary
            $forbiddenBody = @"
--$boundary
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 403 Forbidden
Content-Type: application/json

{"error":{"message":"AccessDenied"}}
--$boundary--
"@
            Mock Invoke-WebRequest -MockWith {
                $global:SpoTestState.Calls++
                $headers = @{ 'Content-Type' = "multipart/mixed; boundary=$($global:SpoTestState.Boundary)" }
                return [pscustomobject]@{ Headers = $headers; Content = $forbiddenBody }
            }

            $resp = Invoke-SpoBatch -BatchUrl 'https://x/_api/$batch' `
                -Requests @(@{ Method = 'GET'; Url = '/_api/web' }) `
                -Token 'tok'

            # Single call — no retry. The 403 surfaces in the returned array
            # so the caller can skip the item.
            $global:SpoTestState.Calls | Should -Be 1
            $resp.Count   | Should -Be 1
            $resp[0].Status | Should -Be 403
        }
    }

    It 'reconnects + retries when any sub-response is 401 (token expired mid-batch)' {
        InModuleScope 'SpoRestClient' {
            Mock Start-Sleep -MockWith { }
            $boundary = $global:SpoTestState.Boundary
            $authFailedBody = @"
--$boundary
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 401 Unauthorized
Content-Type: application/json

{"error":{"message":"Access token has expired"}}
--$boundary--
"@
            $successBody = @"
--$boundary
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 200 OK
Content-Type: application/json;odata=verbose

{"d":{"Title":"after-reconnect"}}
--$boundary--
"@
            Mock Invoke-WebRequest -MockWith {
                $global:SpoTestState.Calls++
                $bodyText = if ($global:SpoTestState.Calls -eq 1) { $authFailedBody } else { $successBody }
                $headers = @{ 'Content-Type' = "multipart/mixed; boundary=$($global:SpoTestState.Boundary)" }
                return [pscustomobject]@{ Headers = $headers; Content = $bodyText }
            }

            $global:SpoTestState.ReconnectCalls = 0
            $reconnect = { $global:SpoTestState.ReconnectCalls++ }

            $getToken = { 'tok' }
            $resp = Invoke-SpoBatch -BatchUrl 'https://x/_api/$batch' `
                -Requests @(@{ Method = 'GET'; Url = '/_api/web' }) `
                -GetToken $getToken `
                -OnAuthReconnect $reconnect

            $global:SpoTestState.Calls          | Should -Be 2
            $global:SpoTestState.ReconnectCalls | Should -BeGreaterOrEqual 1
            $resp.Count   | Should -Be 1
            $resp[0].Status | Should -Be 200
            $resp[0].Body.Title | Should -Be 'after-reconnect'
        }
    }

    It 'retries the whole batch when any sub-response is 500 (transient server error)' {
        InModuleScope 'SpoRestClient' {
            Mock Start-Sleep -MockWith { }
            $boundary = $global:SpoTestState.Boundary
            $transientBody = @"
--$boundary
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 500 Internal Server Error
Content-Type: application/json

{"error":{"message":"server hiccup"}}
--$boundary--
"@
            $successBody = @"
--$boundary
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 200 OK
Content-Type: application/json;odata=verbose

{"d":{"Title":"recovered"}}
--$boundary--
"@
            Mock Invoke-WebRequest -MockWith {
                $global:SpoTestState.Calls++
                $bodyText = if ($global:SpoTestState.Calls -eq 1) { $transientBody } else { $successBody }
                $headers = @{ 'Content-Type' = "multipart/mixed; boundary=$($global:SpoTestState.Boundary)" }
                return [pscustomobject]@{ Headers = $headers; Content = $bodyText }
            }

            $resp = Invoke-SpoBatch -BatchUrl 'https://x/_api/$batch' `
                -Requests @(@{ Method = 'GET'; Url = '/_api/web' }) `
                -Token 'tok'

            $global:SpoTestState.Calls | Should -Be 2
            $resp.Count   | Should -Be 1
            $resp[0].Status | Should -Be 200
            $resp[0].Body.Title | Should -Be 'recovered'
        }
    }

    It 'does NOT retry on sub-response 404 (permanent per-item denial)' {
        InModuleScope 'SpoRestClient' {
            $boundary = $global:SpoTestState.Boundary
            $notFoundBody = @"
--$boundary
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 404 Not Found
Content-Type: application/json

{"error":{"message":"item not found"}}
--$boundary--
"@
            Mock Invoke-WebRequest -MockWith {
                $global:SpoTestState.Calls++
                $headers = @{ 'Content-Type' = "multipart/mixed; boundary=$($global:SpoTestState.Boundary)" }
                return [pscustomobject]@{ Headers = $headers; Content = $notFoundBody }
            }

            $resp = Invoke-SpoBatch -BatchUrl 'https://x/_api/$batch' `
                -Requests @(@{ Method = 'GET'; Url = '/_api/web' }) `
                -Token 'tok'

            $global:SpoTestState.Calls | Should -Be 1
            $resp.Count   | Should -Be 1
            $resp[0].Status | Should -Be 404
        }
    }
}
