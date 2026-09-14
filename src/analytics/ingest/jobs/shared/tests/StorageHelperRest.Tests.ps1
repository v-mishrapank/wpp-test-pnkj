#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Unit tests for the Invoke-TokenRequest retry wrapper, the
# Get-RetryAfterSeconds header extractor, and the ConvertTo-RetryAfterSeconds
# parser in StorageHelperRest.psm1. All three are module-internal — tested
# via InModuleScope.

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    # EventEmitter loaded so Invoke-AdlsDataPlaneWithRetry's
    # Write-ChunkUploadRetryEvent call resolves on retry paths. LogHelper
    # is loaded for Write-Log used by the retry wrapper.
    Import-Module (Join-Path $modulesPath 'LogHelper.psm1')         -Force
    Import-Module (Join-Path $modulesPath 'EventEmitter.psm1')      -Force
    Import-Module (Join-Path $modulesPath 'StorageHelperRest.psm1') -Force
    Initialize-EventContext -RunId 'test-run' -Tenant 'test-tenant'
}

Describe 'Invoke-TokenRequest' {

    Context 'success path' {
        It 'returns immediately on first success without retry' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                $script:Calls = 0
                $result = Invoke-TokenRequest -RequestScript {
                    $script:Calls++
                    return @{ access_token = 'abc' }
                }
                $result.access_token | Should -Be 'abc'
                $script:Calls | Should -Be 1
                Should -Invoke Start-Sleep -Times 0
            }
        }
    }

    Context 'transient errors' {
        BeforeAll {
            # Define helper inside the module's scope so test scriptblocks
            # (which run in module scope under InModuleScope) can call it.
            InModuleScope StorageHelperRest {
                function script:NewFakeHttpException {
                    param([int]$StatusCode, $Headers = $null)
                    $resp = [PSCustomObject]@{ StatusCode = $StatusCode; Headers = $Headers }
                    $ex = [System.Exception]::new("simulated $StatusCode")
                    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                    return $ex
                }
            }
        }

        It 'retries on 5xx and eventually succeeds' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                $script:Calls = 0
                $result = Invoke-TokenRequest -RequestScript {
                    $script:Calls++
                    if ($script:Calls -lt 2) { throw (NewFakeHttpException -StatusCode 503) }
                    return @{ access_token = 'ok' }
                }
                $result.access_token | Should -Be 'ok'
                $script:Calls | Should -Be 2
                Should -Invoke Start-Sleep -Times 1
            }
        }

        It 'retries on 429 and honors Retry-After delta-seconds' {
            InModuleScope StorageHelperRest {
                $script:LastSleep = $null
                Mock Start-Sleep { $script:LastSleep = $Seconds }
                $script:Calls = 0
                $headers = @{ 'Retry-After' = '7' }
                { Invoke-TokenRequest -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 429 -Headers $headers)
                } -MaxAttempts 2 } | Should -Throw
                $script:Calls | Should -Be 2
                $script:LastSleep | Should -Be 7
            }
        }

        It 'retries when no status code is present (network/transport error)' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                $script:Calls = 0
                $result = Invoke-TokenRequest -RequestScript {
                    $script:Calls++
                    if ($script:Calls -lt 2) { throw [System.Exception]::new('connection reset') }
                    return @{ access_token = 'recovered' }
                }
                $result.access_token | Should -Be 'recovered'
                $script:Calls | Should -Be 2
            }
        }

        It 'gives up after MaxAttempts and rethrows the last error' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                $script:Calls = 0
                { Invoke-TokenRequest -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 503)
                } -MaxAttempts 3 } | Should -Throw
                $script:Calls | Should -Be 3
            }
        }
    }

    Context 'hard-fail path (no retry on 4xx)' {
        BeforeAll {
            InModuleScope StorageHelperRest {
                function script:NewFakeHttpException {
                    param([int]$StatusCode, $Headers = $null)
                    $resp = [PSCustomObject]@{ StatusCode = $StatusCode; Headers = $Headers }
                    $ex = [System.Exception]::new("simulated $StatusCode")
                    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                    return $ex
                }
            }
        }

        It 'does not retry on 401 (bad creds)' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                $script:Calls = 0
                { Invoke-TokenRequest -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 401)
                } -MaxAttempts 3 } | Should -Throw
                $script:Calls | Should -Be 1
                Should -Invoke Start-Sleep -Times 0
            }
        }

        It 'does not retry on 404 (bad endpoint)' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                $script:Calls = 0
                { Invoke-TokenRequest -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 404)
                } -MaxAttempts 3 } | Should -Throw
                $script:Calls | Should -Be 1
            }
        }

        It 'does not retry on 400 (bad request)' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                $script:Calls = 0
                { Invoke-TokenRequest -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 400)
                } -MaxAttempts 3 } | Should -Throw
                $script:Calls | Should -Be 1
            }
        }
    }
}

Describe 'Get-RetryAfterSeconds' {
    It 'returns null when response is null' {
        InModuleScope StorageHelperRest {
            Get-RetryAfterSeconds -Response $null | Should -BeNullOrEmpty
        }
    }

    It 'parses dict-style Headers["Retry-After"] with delta-seconds' {
        InModuleScope StorageHelperRest {
            $resp = [PSCustomObject]@{ Headers = @{ 'Retry-After' = '15' } }
            Get-RetryAfterSeconds -Response $resp | Should -Be 15
        }
    }

    It 'parses dict-style Headers["Retry-After"] with HTTP-date' {
        InModuleScope StorageHelperRest {
            $future = ([datetime]::UtcNow.AddSeconds(20)).ToString('R')
            $resp = [PSCustomObject]@{ Headers = @{ 'Retry-After' = $future } }
            $result = Get-RetryAfterSeconds -Response $resp
            # ±2s slop for clock drift between line execution
            $result | Should -BeGreaterOrEqual 18
            $result | Should -BeLessOrEqual 22
        }
    }

    It 'parses typed HttpResponseHeaders.RetryAfter (delta)' {
        InModuleScope StorageHelperRest {
            $resp = [PSCustomObject]@{
                Headers = [PSCustomObject]@{
                    RetryAfter = [PSCustomObject]@{
                        Delta = [System.TimeSpan]::FromSeconds(11)
                        Date  = $null
                    }
                }
            }
            Get-RetryAfterSeconds -Response $resp | Should -Be 11
        }
    }

    It 'returns null when Headers has no Retry-After' {
        InModuleScope StorageHelperRest {
            $resp = [PSCustomObject]@{ Headers = @{ 'Content-Type' = 'application/json' } }
            Get-RetryAfterSeconds -Response $resp | Should -BeNullOrEmpty
        }
    }

    It 'survives a header-extraction exception without throwing' {
        InModuleScope StorageHelperRest {
            # A Headers property that throws on access (the PS 7+
            # HttpResponseHeaders concern Copilot flagged) must not break the
            # retry loop.
            $headersProxy = [PSCustomObject]@{}
            $headersProxy.PSObject.Properties.Add(
                (New-Object System.Management.Automation.PSScriptProperty 'RetryAfter', { throw 'oops' }))
            $resp = [PSCustomObject]@{ Headers = $headersProxy }
            { Get-RetryAfterSeconds -Response $resp } | Should -Not -Throw
            Get-RetryAfterSeconds -Response $resp | Should -BeNullOrEmpty
        }
    }
}

Describe 'ConvertTo-RetryAfterSeconds' {
    It 'parses delta-seconds' {
        InModuleScope StorageHelperRest {
            ConvertTo-RetryAfterSeconds -Value '42' | Should -Be 42
        }
    }

    It 'parses HTTP-date' {
        InModuleScope StorageHelperRest {
            $future = ([datetime]::UtcNow.AddSeconds(30)).ToString('R')
            $r = ConvertTo-RetryAfterSeconds -Value $future
            $r | Should -BeGreaterOrEqual 28
            $r | Should -BeLessOrEqual 32
        }
    }

    It 'returns null on garbage input' {
        InModuleScope StorageHelperRest {
            ConvertTo-RetryAfterSeconds -Value 'not-a-number-or-date' | Should -BeNullOrEmpty
        }
    }

    It 'clamps past dates to 0' {
        InModuleScope StorageHelperRest {
            $past = ([datetime]::UtcNow.AddSeconds(-30)).ToString('R')
            ConvertTo-RetryAfterSeconds -Value $past | Should -Be 0
        }
    }
}

Describe 'Write-ToAdlsRest (managed identity path)' {
    BeforeAll {
        # Drop a small payload to disk so Write-ToAdlsRest has something to read.
        $script:tmpPayload = Join-Path ([System.IO.Path]::GetTempPath()) "adls-mi-$(Get-Random).bin"
        [System.IO.File]::WriteAllBytes($script:tmpPayload, [byte[]](1..64))
    }
    AfterAll {
        if (Test-Path $script:tmpPayload) { Remove-Item $script:tmpPayload -Force }
    }
    BeforeEach {
        # Wipe the module-scoped token cache so each test mints fresh — the fake
        # tokens used here aren't JWTs, so they'd otherwise cache to MaxValue and
        # leak the acquired token (and its call count) into the next test.
        InModuleScope StorageHelperRest { Clear-AdlsTokenCache }
    }

    It 'acquires a managed-identity token, then PUTs/PATCHes/PATCHes the blob' {
        $tmp = $script:tmpPayload
        InModuleScope StorageHelperRest -Parameters @{ payload = $tmp } {
            param($payload)

            $env:STORAGE_AUTH_METHOD = 'managed_identity'
            $env:IDENTITY_ENDPOINT   = 'http://169.254.169.254/metadata/identity/oauth2/token'
            $env:IDENTITY_HEADER     = 'fake-header-value'

            $script:Calls = [System.Collections.Generic.List[hashtable]]::new()
            Mock Invoke-WebRequest -ParameterFilter { $Uri -match 'metadata/identity' } {
                # ACA managed identity endpoint shape: response.Content is JSON string.
                return [PSCustomObject]@{ Content = '{"access_token":"mi-token-xyz"}' }
            }
            # Explicit -notmatch keeps mock resolution unambiguous if Pester's
            # specificity ordering ever changes — without the filter, the
            # general mock matches the MI token call too and clobbers .Content.
            Mock Invoke-WebRequest -ParameterFilter { $Uri -notmatch 'metadata/identity' } {
                $script:Calls.Add(@{
                    Uri    = $Uri
                    Method = $Method
                    Auth   = $Headers['Authorization']
                })
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            try {
                Write-ToAdlsRest `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName    'landing' `
                    -BlobPath         'tenant/2026-04-25/file.jsonl' `
                    -LocalFile        $payload

                $script:Calls.Count | Should -Be 3
                $script:Calls[0].Uri    | Should -Match 'resource=file$'
                $script:Calls[0].Method | Should -Be 'PUT'
                $script:Calls[1].Uri    | Should -Match 'action=append'
                $script:Calls[1].Method | Should -Be 'PATCH'
                $script:Calls[2].Uri    | Should -Match 'action=flush'
                $script:Calls[2].Uri    | Should -Match "position=64"
                $script:Calls[2].Method | Should -Be 'PATCH'
                $script:Calls[0].Auth   | Should -Be 'Bearer mi-token-xyz'
            }
            finally {
                Remove-Item env:STORAGE_AUTH_METHOD -ErrorAction SilentlyContinue
                Remove-Item env:IDENTITY_ENDPOINT   -ErrorAction SilentlyContinue
                Remove-Item env:IDENTITY_HEADER     -ErrorAction SilentlyContinue
            }
        }
    }

    It 'defaults to managed_identity when STORAGE_AUTH_METHOD is unset' {
        $tmp = $script:tmpPayload
        InModuleScope StorageHelperRest -Parameters @{ payload = $tmp } {
            param($payload)

            Remove-Item env:STORAGE_AUTH_METHOD -ErrorAction SilentlyContinue
            $env:IDENTITY_ENDPOINT = 'http://169.254.169.254/metadata/identity/oauth2/token'
            $env:IDENTITY_HEADER   = 'fake'

            $script:MICalled = 0
            Mock Invoke-WebRequest -ParameterFilter { $Uri -match 'metadata/identity' } {
                $script:MICalled++
                return [PSCustomObject]@{ Content = '{"access_token":"default-mi-token"}' }
            }
            Mock Invoke-WebRequest -ParameterFilter { $Uri -notmatch 'metadata/identity' } {
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            try {
                Write-ToAdlsRest -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing' -BlobPath 'p/f.jsonl' -LocalFile $payload
                $script:MICalled | Should -Be 1
            }
            finally {
                Remove-Item env:IDENTITY_ENDPOINT -ErrorAction SilentlyContinue
                Remove-Item env:IDENTITY_HEADER   -ErrorAction SilentlyContinue
            }
        }
    }
}

Describe 'Write-ToAdlsRest (service principal path)' {
    BeforeAll {
        # KeyVaultHelper exports Get-CertificateBytes and MsalTokenHelper
        # exports Get-IngestAccessToken — both called by Get-AdlsAccessToken
        # in the SP branch. In production they're imported by Invoke-Ingestion.ps1
        # alongside StorageHelperRest; tests need them visible so Pester's
        # Mock can target the commands.
        $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
        Import-Module (Join-Path $modulesPath 'KeyVaultHelper.psm1')  -Force
        Import-Module (Join-Path $modulesPath 'MsalTokenHelper.psm1') -Force

        $script:tmpPayload = Join-Path ([System.IO.Path]::GetTempPath()) "adls-sp-$(Get-Random).bin"
        [System.IO.File]::WriteAllBytes($script:tmpPayload, [byte[]](100..131))   # 32 bytes
    }
    AfterAll {
        if (Test-Path $script:tmpPayload) { Remove-Item $script:tmpPayload -Force }
    }
    BeforeEach {
        # Reset the token cache between tests so each SP path re-mints and the
        # bearer assertions see this test's token, not a cached one.
        InModuleScope StorageHelperRest { Clear-AdlsTokenCache }
    }

    It 'delegates SP-cert token acquisition to Get-IngestAccessToken with the Storage audience' {
        # Pre-#186 we hand-rolled the JWT assertion and POSTed to the AAD
        # token endpoint directly. That path tripped over the empty-password
        # PFX-load fragility documented in MsalTokenHelper.psm1 header (RSA-
        # wrapped-key issue on Linux pwsh) and the related OpenSslCrypto
        # error 10080002. The SP branch now delegates to Get-IngestAccessToken,
        # which already handles the Linux-pwsh cert rebind and uses MSAL to
        # build the assertion (so x5t Base64Url is MSAL's problem, not ours).
        $tmp = $script:tmpPayload
        $fakePfx = [byte[]](1..40)   # opaque bytes; helper is mocked

        InModuleScope StorageHelperRest -Parameters @{ payload = $tmp; pfx = $fakePfx } {
            param($payload, $pfx)

            $env:STORAGE_AUTH_METHOD    = 'service_principal_cert'
            $env:STORAGE_SP_TENANT_ID   = '11111111-1111-1111-1111-111111111111'
            $env:STORAGE_SP_CLIENT_ID   = '22222222-2222-2222-2222-222222222222'
            $env:STORAGE_SP_CERT_NAME   = 'sp-cert'
            $env:KEYVAULT_NAME          = 'kv-test'

            $script:CertCall = $null
            Mock Get-CertificateBytes {
                $script:CertCall = @{ VaultName = $VaultName; CertName = $CertName }
                return $pfx
            }

            $script:TokenCall = $null
            Mock Get-IngestAccessToken {
                $script:TokenCall = @{
                    CertBytes = $CertBytes
                    ClientId  = $ClientId
                    TenantId  = $TenantId
                    Audience  = $Audience
                }
                return 'sp-token-abc'
            }

            $script:WebCalls = [System.Collections.Generic.List[hashtable]]::new()
            Mock Invoke-WebRequest {
                $script:WebCalls.Add(@{ Uri = $Uri; Method = $Method; Auth = $Headers['Authorization'] })
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            try {
                Write-ToAdlsRest -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing' -BlobPath 'p/sp.jsonl' -LocalFile $payload

                # KV fetch uses the SP cert name + container KV
                $script:CertCall          | Should -Not -BeNullOrEmpty
                $script:CertCall.VaultName | Should -Be 'kv-test'
                $script:CertCall.CertName  | Should -Be 'sp-cert'

                # MSAL helper receives the SP identity + Storage audience
                $script:TokenCall          | Should -Not -BeNullOrEmpty
                $script:TokenCall.ClientId | Should -Be '22222222-2222-2222-2222-222222222222'
                $script:TokenCall.TenantId | Should -Be '11111111-1111-1111-1111-111111111111'
                $script:TokenCall.Audience | Should -Be 'https://storage.azure.com/.default'
                # Cert bytes are passed through verbatim
                ,$script:TokenCall.CertBytes | Should -BeOfType ([byte[]])
                $script:TokenCall.CertBytes.Length | Should -Be 40

                # ADLS data-plane calls carry the SP-issued bearer
                $script:WebCalls.Count   | Should -Be 3
                $script:WebCalls[0].Auth | Should -Be 'Bearer sp-token-abc'
            }
            finally {
                Remove-Item env:STORAGE_AUTH_METHOD  -ErrorAction SilentlyContinue
                Remove-Item env:STORAGE_SP_TENANT_ID -ErrorAction SilentlyContinue
                Remove-Item env:STORAGE_SP_CLIENT_ID -ErrorAction SilentlyContinue
                Remove-Item env:STORAGE_SP_CERT_NAME -ErrorAction SilentlyContinue
                Remove-Item env:KEYVAULT_NAME        -ErrorAction SilentlyContinue
            }
        }
    }

    It 'delegates SP-secret token acquisition to Get-IngestAccessToken via the Secret parameter set' {
        # Mirror of the cert test for the secret path: KV fetch goes through
        # Get-SecretValue (Secrets User role only, no Certificate User
        # required), and Get-IngestAccessToken is invoked with -ClientSecret
        # instead of -CertBytes. The downstream Storage audience is identical.
        $tmp = $script:tmpPayload

        InModuleScope StorageHelperRest -Parameters @{ payload = $tmp } {
            param($payload)

            $env:STORAGE_AUTH_METHOD    = 'service_principal_secret'
            $env:STORAGE_SP_TENANT_ID   = '33333333-3333-3333-3333-333333333333'
            $env:STORAGE_SP_CLIENT_ID   = '44444444-4444-4444-4444-444444444444'
            $env:STORAGE_SP_SECRET_NAME = 'sp-secret'
            $env:KEYVAULT_NAME          = 'kv-test'

            $script:SecretCall = $null
            Mock Get-SecretValue {
                $script:SecretCall = @{ VaultName = $VaultName; SecretName = $SecretName }
                return 'plaintext-secret-value'
            }

            $script:TokenCall = $null
            Mock Get-IngestAccessToken {
                $script:TokenCall = @{
                    ClientSecret = $ClientSecret
                    ClientId     = $ClientId
                    TenantId     = $TenantId
                    Audience     = $Audience
                }
                return 'sp-secret-token-xyz'
            }

            $script:WebCalls = [System.Collections.Generic.List[hashtable]]::new()
            Mock Invoke-WebRequest {
                $script:WebCalls.Add(@{ Uri = $Uri; Method = $Method; Auth = $Headers['Authorization'] })
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            try {
                Write-ToAdlsRest -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName 'landing' -BlobPath 'p/sp-secret.jsonl' -LocalFile $payload

                # KV fetch uses the SP secret name + container KV
                $script:SecretCall            | Should -Not -BeNullOrEmpty
                $script:SecretCall.VaultName  | Should -Be 'kv-test'
                $script:SecretCall.SecretName | Should -Be 'sp-secret'

                # MSAL helper receives the SP identity + Storage audience via the Secret param set
                $script:TokenCall              | Should -Not -BeNullOrEmpty
                $script:TokenCall.ClientId     | Should -Be '44444444-4444-4444-4444-444444444444'
                $script:TokenCall.TenantId     | Should -Be '33333333-3333-3333-3333-333333333333'
                $script:TokenCall.Audience     | Should -Be 'https://storage.azure.com/.default'
                $script:TokenCall.ClientSecret | Should -Be 'plaintext-secret-value'

                # ADLS data-plane calls carry the SP-issued bearer
                $script:WebCalls.Count   | Should -Be 3
                $script:WebCalls[0].Auth | Should -Be 'Bearer sp-secret-token-xyz'
            }
            finally {
                Remove-Item env:STORAGE_AUTH_METHOD    -ErrorAction SilentlyContinue
                Remove-Item env:STORAGE_SP_TENANT_ID   -ErrorAction SilentlyContinue
                Remove-Item env:STORAGE_SP_CLIENT_ID   -ErrorAction SilentlyContinue
                Remove-Item env:STORAGE_SP_SECRET_NAME -ErrorAction SilentlyContinue
                Remove-Item env:KEYVAULT_NAME          -ErrorAction SilentlyContinue
            }
        }
    }
}

Describe 'Invoke-AdlsDataPlaneWithRetry' {
    # Mirrors the Invoke-TokenRequest tests but for the data-plane wrapper
    # introduced by #345. Same classification rules (5xx + 429 + no-status
    # are transient; 4xx-other is hard-fail), 5 attempts by default, with
    # an additional chunk_upload_retry LAW event per retry.

    BeforeAll {
        InModuleScope StorageHelperRest {
            function script:NewFakeHttpException {
                param([int]$StatusCode, $Headers = $null)
                $resp = [PSCustomObject]@{ StatusCode = $StatusCode; Headers = $Headers }
                $ex = [System.Exception]::new("simulated $StatusCode")
                Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                return $ex
            }
        }
    }

    Context 'success path' {
        It 'returns immediately on first success without retry' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                Mock Write-Log { }
                $script:Calls = 0
                $result = Invoke-AdlsDataPlaneWithRetry -BlobPath 'p/x.jsonl' -RequestScript {
                    $script:Calls++
                    return 'ok'
                }
                $result | Should -Be 'ok'
                $script:Calls | Should -Be 1
                Should -Invoke Start-Sleep -Times 0
            }
        }
    }

    Context 'transient errors' {
        It 'retries on 503 and eventually succeeds' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                Mock Write-Log { }
                $script:Calls = 0
                $result = Invoke-AdlsDataPlaneWithRetry -BlobPath 'p/x.jsonl' -RequestScript {
                    $script:Calls++
                    if ($script:Calls -lt 3) { throw (NewFakeHttpException -StatusCode 503) }
                    return 'recovered'
                }
                $result | Should -Be 'recovered'
                $script:Calls | Should -Be 3
                Should -Invoke Start-Sleep -Times 2
            }
        }

        It 'retries on 429 and honors Retry-After delta-seconds (capped at MaxDelaySeconds)' {
            InModuleScope StorageHelperRest {
                $script:LastSleep = $null
                Mock Start-Sleep { $script:LastSleep = $Seconds }
                Mock Write-Log { }
                $headers = @{ 'Retry-After' = '7' }
                $script:Calls = 0
                { Invoke-AdlsDataPlaneWithRetry -BlobPath 'p/x.jsonl' -MaxAttempts 2 -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 429 -Headers $headers)
                } } | Should -Throw
                $script:Calls    | Should -Be 2
                $script:LastSleep | Should -Be 7
            }
        }

        It 'caps Retry-After at MaxDelaySeconds' {
            InModuleScope StorageHelperRest {
                $script:LastSleep = $null
                Mock Start-Sleep { $script:LastSleep = $Seconds }
                Mock Write-Log { }
                $headers = @{ 'Retry-After' = '999' }
                $script:Calls = 0
                { Invoke-AdlsDataPlaneWithRetry -BlobPath 'p/x.jsonl' -MaxAttempts 2 -MaxDelaySeconds 30 -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 503 -Headers $headers)
                } } | Should -Throw
                $script:LastSleep | Should -Be 30
            }
        }

        It 'retries when no status code is present (network/transport error)' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                Mock Write-Log { }
                $script:Calls = 0
                $result = Invoke-AdlsDataPlaneWithRetry -BlobPath 'p/x.jsonl' -RequestScript {
                    $script:Calls++
                    if ($script:Calls -lt 2) { throw [System.Exception]::new('connection reset') }
                    return 'recovered'
                }
                $result | Should -Be 'recovered'
                $script:Calls | Should -Be 2
            }
        }

        It 'gives up after MaxAttempts (default 5) and rethrows the last error' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                Mock Write-Log { }
                $script:Calls = 0
                { Invoke-AdlsDataPlaneWithRetry -BlobPath 'p/x.jsonl' -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 503)
                } } | Should -Throw
                $script:Calls | Should -Be 5
            }
        }
    }

    Context 'hard-fail path (no retry on 4xx-other)' {
        It 'does not retry on 401 (bad token)' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                Mock Write-Log { }
                $script:Calls = 0
                { Invoke-AdlsDataPlaneWithRetry -BlobPath 'p/x.jsonl' -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 401)
                } } | Should -Throw
                $script:Calls | Should -Be 1
                Should -Invoke Start-Sleep -Times 0
            }
        }

        It 'does not retry on 400 (bad path)' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                Mock Write-Log { }
                $script:Calls = 0
                { Invoke-AdlsDataPlaneWithRetry -BlobPath 'p/x.jsonl' -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 400)
                } } | Should -Throw
                $script:Calls | Should -Be 1
            }
        }

        It 'does not retry on 403 (forbidden)' {
            InModuleScope StorageHelperRest {
                Mock Start-Sleep { }
                Mock Write-Log { }
                $script:Calls = 0
                { Invoke-AdlsDataPlaneWithRetry -BlobPath 'p/x.jsonl' -RequestScript {
                    $script:Calls++
                    throw (NewFakeHttpException -StatusCode 403)
                } } | Should -Throw
                $script:Calls | Should -Be 1
            }
        }
    }
}

Describe 'Write-ToAdlsRest retry on transient errors' {
    # Verifies the wrapper is wired into Write-ToAdlsRest: a transient 5xx
    # from the data-plane PUT survives a retry. Mirrors the managed-identity
    # happy-path test above but injects one failure before success.
    BeforeAll {
        $script:tmpPayload = Join-Path ([System.IO.Path]::GetTempPath()) "adls-retry-$(Get-Random).bin"
        [System.IO.File]::WriteAllBytes($script:tmpPayload, [byte[]](1..16))
    }
    AfterAll {
        if (Test-Path $script:tmpPayload) { Remove-Item $script:tmpPayload -Force }
    }
    BeforeEach {
        InModuleScope StorageHelperRest { Clear-AdlsTokenCache }
    }

    It 'retries the create+append+flush sequence as one unit on 503' {
        $tmp = $script:tmpPayload
        InModuleScope StorageHelperRest -Parameters @{ payload = $tmp } {
            param($payload)

            $env:STORAGE_AUTH_METHOD = 'managed_identity'
            $env:IDENTITY_ENDPOINT   = 'http://169.254.169.254/metadata/identity/oauth2/token'
            $env:IDENTITY_HEADER     = 'fake'

            Mock Start-Sleep { }
            Mock Invoke-WebRequest -ParameterFilter { $Uri -match 'metadata/identity' } {
                return [PSCustomObject]@{ Content = '{"access_token":"tok"}' }
            }

            $script:Attempts = 0
            $script:DataCalls = [System.Collections.Generic.List[hashtable]]::new()
            Mock Invoke-WebRequest -ParameterFilter { $Uri -notmatch 'metadata/identity' } {
                # First create-file call throws 503; on retry the whole
                # sequence reruns and succeeds.
                if ($Uri -match 'resource=file') {
                    $script:Attempts++
                    if ($script:Attempts -eq 1) {
                        $resp = [PSCustomObject]@{ StatusCode = 503; Headers = $null }
                        $ex = [System.Exception]::new('simulated 503')
                        Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                        throw $ex
                    }
                }
                $script:DataCalls.Add(@{ Uri = $Uri; Method = $Method })
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            try {
                Write-ToAdlsRest `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName    'landing' `
                    -BlobPath         'tenant/2026-05-11/file.jsonl' `
                    -LocalFile        $payload

                # First attempt: create-file threw. Second attempt: create +
                # append + flush all succeed. So DataCalls holds three
                # entries (the second attempt's three calls).
                $script:DataCalls.Count | Should -Be 3
                $script:DataCalls[0].Uri | Should -Match 'resource=file$'
                $script:DataCalls[1].Uri | Should -Match 'action=append'
                $script:DataCalls[2].Uri | Should -Match 'action=flush'
                Should -Invoke Start-Sleep -Times 1
            }
            finally {
                Remove-Item env:STORAGE_AUTH_METHOD -ErrorAction SilentlyContinue
                Remove-Item env:IDENTITY_ENDPOINT   -ErrorAction SilentlyContinue
                Remove-Item env:IDENTITY_HEADER     -ErrorAction SilentlyContinue
            }
        }
    }
}

Describe 'Get-JwtExpiry' {
    # Builds a minimal JWT (header.payload.signature) whose payload carries the
    # given exp claim, so we can assert the parser without a real token.
    BeforeAll {
        InModuleScope StorageHelperRest {
            function script:New-TestJwt {
                param([long]$Exp)
                $payloadJson = "{""exp"":$Exp}"
                $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($payloadJson)).
                    TrimEnd('=').Replace('+', '-').Replace('/', '_')
                return "eyJhbGciOiJSUzI1NiJ9.$b64.sig"
            }
        }
    }

    It 'parses the exp claim into a UTC DateTime' {
        InModuleScope StorageHelperRest {
            $exp = [DateTimeOffset]::UtcNow.AddHours(1).ToUnixTimeSeconds()
            $expected = [DateTimeOffset]::FromUnixTimeSeconds($exp).UtcDateTime
            Get-JwtExpiry -Token (New-TestJwt -Exp $exp) | Should -Be $expected
        }
    }

    It 'returns MaxValue for a non-JWT opaque token' {
        InModuleScope StorageHelperRest {
            Get-JwtExpiry -Token 'not-a-jwt' | Should -Be ([DateTime]::MaxValue)
        }
    }

    It 'returns MaxValue when the payload has no exp claim' {
        InModuleScope StorageHelperRest {
            $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"foo":1}')).
                TrimEnd('=').Replace('+', '-').Replace('/', '_')
            Get-JwtExpiry -Token "hdr.$b64.sig" | Should -Be ([DateTime]::MaxValue)
        }
    }
}

Describe 'Get-AdlsAccessToken caching (managed identity path)' {
    BeforeAll {
        InModuleScope StorageHelperRest {
            function script:New-TestJwt {
                param([long]$Exp)
                $payloadJson = "{""exp"":$Exp}"
                $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($payloadJson)).
                    TrimEnd('=').Replace('+', '-').Replace('/', '_')
                return "eyJhbGciOiJSUzI1NiJ9.$b64.sig"
            }
        }
    }
    BeforeEach {
        InModuleScope StorageHelperRest { Clear-AdlsTokenCache }
    }

    It 'mints once and serves the cached token on subsequent calls' {
        InModuleScope StorageHelperRest {
            $env:STORAGE_AUTH_METHOD = 'managed_identity'
            $env:IDENTITY_ENDPOINT   = 'http://169.254.169.254/metadata/identity/oauth2/token'
            $env:IDENTITY_HEADER     = 'fake'
            $jwt = New-TestJwt -Exp ([DateTimeOffset]::UtcNow.AddHours(1).ToUnixTimeSeconds())

            $script:MintCalls = 0
            Mock Invoke-WebRequest -ParameterFilter { $Uri -match 'metadata/identity' } {
                $script:MintCalls++
                return [PSCustomObject]@{ Content = "{""access_token"":""$jwt""}" }
            }

            try {
                $first  = Get-AdlsAccessToken
                $second = Get-AdlsAccessToken
                $first  | Should -Be $jwt
                $second | Should -Be $jwt
                $script:MintCalls | Should -Be 1
            }
            finally {
                Remove-Item env:STORAGE_AUTH_METHOD, env:IDENTITY_ENDPOINT, env:IDENTITY_HEADER -ErrorAction SilentlyContinue
            }
        }
    }

    It 're-mints when -ForceRefresh is passed even on a warm cache' {
        InModuleScope StorageHelperRest {
            $env:STORAGE_AUTH_METHOD = 'managed_identity'
            $env:IDENTITY_ENDPOINT   = 'http://169.254.169.254/metadata/identity/oauth2/token'
            $env:IDENTITY_HEADER     = 'fake'
            $jwt = New-TestJwt -Exp ([DateTimeOffset]::UtcNow.AddHours(1).ToUnixTimeSeconds())

            $script:MintCalls = 0
            Mock Invoke-WebRequest -ParameterFilter { $Uri -match 'metadata/identity' } {
                $script:MintCalls++
                return [PSCustomObject]@{ Content = "{""access_token"":""$jwt""}" }
            }

            try {
                Get-AdlsAccessToken | Out-Null
                Get-AdlsAccessToken -ForceRefresh | Out-Null
                $script:MintCalls | Should -Be 2
            }
            finally {
                Remove-Item env:STORAGE_AUTH_METHOD, env:IDENTITY_ENDPOINT, env:IDENTITY_HEADER -ErrorAction SilentlyContinue
            }
        }
    }

    It 're-mints a token already inside the refresh skew window' {
        InModuleScope StorageHelperRest {
            $env:STORAGE_AUTH_METHOD = 'managed_identity'
            $env:IDENTITY_ENDPOINT   = 'http://169.254.169.254/metadata/identity/oauth2/token'
            $env:IDENTITY_HEADER     = 'fake'
            # exp only 60s out — inside the 300s skew, so never cached as fresh.
            $jwt = New-TestJwt -Exp ([DateTimeOffset]::UtcNow.AddSeconds(60).ToUnixTimeSeconds())

            $script:MintCalls = 0
            Mock Invoke-WebRequest -ParameterFilter { $Uri -match 'metadata/identity' } {
                $script:MintCalls++
                return [PSCustomObject]@{ Content = "{""access_token"":""$jwt""}" }
            }

            try {
                Get-AdlsAccessToken | Out-Null
                Get-AdlsAccessToken | Out-Null
                $script:MintCalls | Should -Be 2
            }
            finally {
                Remove-Item env:STORAGE_AUTH_METHOD, env:IDENTITY_ENDPOINT, env:IDENTITY_HEADER -ErrorAction SilentlyContinue
            }
        }
    }

    It 'mints again after Clear-AdlsTokenCache' {
        InModuleScope StorageHelperRest {
            $env:STORAGE_AUTH_METHOD = 'managed_identity'
            $env:IDENTITY_ENDPOINT   = 'http://169.254.169.254/metadata/identity/oauth2/token'
            $env:IDENTITY_HEADER     = 'fake'
            $jwt = New-TestJwt -Exp ([DateTimeOffset]::UtcNow.AddHours(1).ToUnixTimeSeconds())

            $script:MintCalls = 0
            Mock Invoke-WebRequest -ParameterFilter { $Uri -match 'metadata/identity' } {
                $script:MintCalls++
                return [PSCustomObject]@{ Content = "{""access_token"":""$jwt""}" }
            }

            try {
                Get-AdlsAccessToken | Out-Null
                Clear-AdlsTokenCache
                Get-AdlsAccessToken | Out-Null
                $script:MintCalls | Should -Be 2
            }
            finally {
                Remove-Item env:STORAGE_AUTH_METHOD, env:IDENTITY_ENDPOINT, env:IDENTITY_HEADER -ErrorAction SilentlyContinue
            }
        }
    }
}

Describe 'Write-ToAdlsRest 401 safety net' {
    BeforeAll {
        $script:tmpPayload = Join-Path ([System.IO.Path]::GetTempPath()) "adls-401-$(Get-Random).bin"
        [System.IO.File]::WriteAllBytes($script:tmpPayload, [byte[]](1..16))
    }
    AfterAll {
        if (Test-Path $script:tmpPayload) { Remove-Item $script:tmpPayload -Force }
    }
    BeforeEach {
        InModuleScope StorageHelperRest { Clear-AdlsTokenCache }
    }

    It 're-mints the token and retries the upload once on a 401' {
        $tmp = $script:tmpPayload
        InModuleScope StorageHelperRest -Parameters @{ payload = $tmp } {
            param($payload)

            $env:STORAGE_AUTH_METHOD = 'managed_identity'
            $env:IDENTITY_ENDPOINT   = 'http://169.254.169.254/metadata/identity/oauth2/token'
            $env:IDENTITY_HEADER     = 'fake'

            Mock Start-Sleep { }
            Mock Write-Log { }

            # Two distinct tokens: the first is "rejected" with a 401, the
            # second (post-refresh) is accepted.
            $script:MintCalls = 0
            Mock Invoke-WebRequest -ParameterFilter { $Uri -match 'metadata/identity' } {
                $script:MintCalls++
                $tok = if ($script:MintCalls -eq 1) { 'stale-token' } else { 'fresh-token' }
                return [PSCustomObject]@{ Content = "{""access_token"":""$tok""}" }
            }

            $script:Accepted = [System.Collections.Generic.List[hashtable]]::new()
            Mock Invoke-WebRequest -ParameterFilter { $Uri -notmatch 'metadata/identity' } {
                if ($Headers['Authorization'] -eq 'Bearer stale-token') {
                    $resp = [PSCustomObject]@{ StatusCode = 401; Headers = $null }
                    $ex = [System.Exception]::new('simulated 401')
                    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
                    throw $ex
                }
                $script:Accepted.Add(@{ Uri = $Uri; Auth = $Headers['Authorization'] })
                return [PSCustomObject]@{ StatusCode = 201 }
            }

            try {
                Write-ToAdlsRest `
                    -StorageAccountUrl 'https://acct.dfs.core.windows.net' `
                    -ContainerName    'landing' `
                    -BlobPath         'tenant/2026-08-08/file.jsonl' `
                    -LocalFile        $payload

                # Token minted again after the 401 forces a refresh, and the
                # accepted create/append/flush all carry the refreshed bearer.
                $script:MintCalls | Should -Be 2
                $script:Accepted.Count | Should -Be 3
                $script:Accepted[0].Auth | Should -Be 'Bearer fresh-token'
            }
            finally {
                Remove-Item env:STORAGE_AUTH_METHOD, env:IDENTITY_ENDPOINT, env:IDENTITY_HEADER -ErrorAction SilentlyContinue
            }
        }
    }
}

