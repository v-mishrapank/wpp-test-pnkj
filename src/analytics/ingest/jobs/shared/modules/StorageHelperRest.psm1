function Get-RetryAfterSeconds {
    # Best-effort extraction of Retry-After from a failed HTTP response object.
    # PS 7+ exposes the response in three different shapes depending on the exception:
    #   - Invoke-RestMethod / Invoke-WebRequest → HttpResponseMessage with
    #     .Headers = HttpResponseHeaders (typed; .RetryAfter is RetryConditionHeaderValue)
    #   - Legacy WebException → HttpWebResponse with .Headers = WebHeaderCollection (indexable)
    #   - BasicHtmlWebResponseObject (older PS shape) → .Headers is a dictionary
    # All three are wrapped in try/catch so a header-parse failure can never
    # break the surrounding retry loop.
    param($Response)

    if (-not $Response) { return $null }

    try {
        # Path 1: typed HttpResponseHeaders.RetryAfter (RetryConditionHeaderValue)
        $headers = $Response.Headers
        if ($headers -and $headers.PSObject.Properties['RetryAfter'] -and $headers.RetryAfter) {
            $ra = $headers.RetryAfter
            if ($ra.Delta) { return [int]$ra.Delta.TotalSeconds }
            if ($ra.Date)  { return [int]([math]::Max(0, ($ra.Date.UtcDateTime - [datetime]::UtcNow).TotalSeconds)) }
        }
    } catch { Write-Verbose $_.Exception.Message }

    try {
        # Path 2: TryGetValues on HttpResponseHeaders (returns IEnumerable<string>)
        $headers = $Response.Headers
        if ($headers -and $headers.PSObject.Methods['TryGetValues']) {
            [string[]]$values = $null
            if ($headers.TryGetValues('Retry-After', [ref]$values) -and $values.Count -gt 0) {
                $parsed = ConvertTo-RetryAfterSeconds -Value $values[0]
                if ($null -ne $parsed) { return $parsed }
            }
        }
    } catch { Write-Verbose $_.Exception.Message }

    try {
        # Path 3: dict/indexable Headers (WebHeaderCollection, BasicHtmlWebResponseObject)
        $val = $Response.Headers['Retry-After']
        if ($val) {
            $parsed = ConvertTo-RetryAfterSeconds -Value ([string]$val)
            if ($null -ne $parsed) { return $parsed }
        }
    } catch { Write-Verbose $_.Exception.Message }

    return $null
}

function ConvertTo-RetryAfterSeconds {
    # Parse a Retry-After header value. Accepts delta-seconds ("30") or HTTP-date
    # ("Wed, 21 Oct 2026 07:28:00 GMT"). Returns $null on parse failure.
    param([Parameter(Mandatory)][string]$Value)

    [int]$secs = 0
    if ([int]::TryParse($Value, [ref]$secs)) { return $secs }

    $dt = [datetime]::MinValue
    if ([datetime]::TryParse($Value, [ref]$dt)) {
        return [int]([math]::Max(0, ($dt.ToUniversalTime() - [datetime]::UtcNow).TotalSeconds))
    }

    return $null
}

function Invoke-AdlsDataPlaneWithRetry {
    # Retry wrapper for the ADLS data-plane create/append/flush sequence in
    # Write-ToAdlsRest. Mirrors Invoke-TokenRequest's shape but tuned for the
    # data plane:
    #   - 5 attempts (Invoke-WithRetry's default for graph/exo/spo). Each
    #     upload is on the per-stage critical path of Invoke-Ingestion as of
    #     #345, so resilience > speed.
    #   - Retry on 5xx and 429 (with Retry-After honored). Network errors
    #     (no status code) are also treated as transient.
    #   - Hard-fail on 4xx-other (400/401/403/404) — those don't fix on retry.
    #   - Emit chunk_upload_retry LAW event per retry so retry storms surface
    #     in dashboards. Without this, the new per-stage upload's retry
    #     sleeps are indistinguishable from a wedged container (#327 pattern).
    #   - Backoff capped at MaxDelaySeconds (default 60s); worst case per
    #     chunk is 4 × 60s ≈ 4 min.
    param(
        [Parameter(Mandatory)][scriptblock]$RequestScript,
        [Parameter(Mandatory)][string]$BlobPath,
        [int]$MaxAttempts = 5,
        [int]$MaxDelaySeconds = 60
    )

    $attempt = 0
    while ($true) {
        $attempt++
        try {
            return & $RequestScript
        }
        catch {
            $status = $null
            $resp = $_.Exception.Response
            if ($resp -and $resp.PSObject.Properties['StatusCode']) {
                $status = [int]$resp.StatusCode
            }

            $isTransient = ($null -eq $status) -or ($status -ge 500) -or ($status -eq 429)
            if (-not $isTransient -or $attempt -ge $MaxAttempts) { throw }

            $retryAfter = Get-RetryAfterSeconds -Response $resp
            $delay = if ($retryAfter -and $retryAfter -gt 0) {
                [math]::Min([int]$retryAfter, $MaxDelaySeconds)
            } else {
                [math]::Min([int][math]::Pow(2, $attempt), $MaxDelaySeconds)
            }

            $statusForEvent = if ($null -ne $status) { [int]$status } else { 0 }
            Write-Log "ADLS upload retry attempt $attempt for $BlobPath after status=$status, backing off ${delay}s: $($_.Exception.Message)" -Level WARN
            # Emit best-effort — if EventEmitter isn't loaded (e.g. ad-hoc
            # smoke test of this module), don't block the retry on a missing
            # observability sink.
            try {
                Write-ChunkUploadRetryEvent -BlobPath $BlobPath -Attempt $attempt `
                    -DelaySeconds ([int]$delay) -StatusCode $statusForEvent `
                    -Message $_.Exception.Message
            } catch { Write-Verbose $_.Exception.Message }
            Start-Sleep -Seconds $delay
        }
    }
}

function Invoke-TokenRequest {
    # Small retry wrapper for AAD/MI token endpoints.
    # Retries on 5xx and 429 (with Retry-After honored), up to 3 attempts.
    # Hard-fails on 4xx — bad creds or bad endpoint shouldn't burn retries.
    # Network exceptions (no status code) are treated as transient.
    param(
        [Parameter(Mandatory)][scriptblock]$RequestScript,
        [int]$MaxAttempts = 3
    )

    $attempt = 0
    while ($true) {
        $attempt++
        try {
            return & $RequestScript
        }
        catch {
            $status = $null
            $resp = $_.Exception.Response
            if ($resp -and $resp.PSObject.Properties['StatusCode']) {
                $status = [int]$resp.StatusCode
            }

            $isTransient = ($null -eq $status) -or ($status -ge 500) -or ($status -eq 429)
            if (-not $isTransient -or $attempt -ge $MaxAttempts) { throw }

            $retryAfter = Get-RetryAfterSeconds -Response $resp
            $delay = if ($retryAfter -and $retryAfter -gt 0) { $retryAfter } else { [int][math]::Pow(2, $attempt) }
            # #327: WARN-only here (no stage/entity context for an event).
            # Without this line, token-fetch retries are silent — the same
            # observability gap we close at the worker call sites.
            Write-Log "Token request retry attempt $attempt after status=$status, backing off ${delay}s: $($_.Exception.Message)" -Level WARN
            Start-Sleep -Seconds $delay
        }
    }
}

# Process-lifetime cache for the Storage data-plane access token. All ADLS
# callers share this one module-scoped entry, so a valid token is minted and
# reused for the rest of its lifetime instead of re-fetched on every operation.
# Shape: @{ Token = '...'; ExpiresAt = [DateTime] }.
$script:AdlsTokenCache = $null

# Re-mint a cached token this many seconds before its real expiry so it can't
# lapse mid-request. Covers a long create/append/flush plus the following
# retry's wall time. Storage tokens are ~1h-lived; 5 min of slack is ample.
$script:AdlsTokenRefreshSkewSeconds = 300

function Get-JwtExpiry {
    # Extract the `exp` claim from a JWT and return it as a UTC DateTime.
    # Returns [DateTime]::MaxValue when the token isn't a well-formed JWT —
    # an opaque token still works until Storage rejects it, and the 401 safety
    # net re-mints on rejection, so caching-to-max is safe.
    param([Parameter(Mandatory)][string]$Token)

    $parts = $Token -split '\.'
    if ($parts.Count -lt 2) { return [DateTime]::MaxValue }
    try {
        # JWT payload is URL-safe base64 without padding. Translate the
        # URL-safe chars and pad to a multiple of 4 before decoding.
        $payload = $parts[1].Replace('-', '+').Replace('_', '/')
        switch ($payload.Length % 4) {
            2 { $payload += '==' }
            3 { $payload += '=' }
        }
        $json = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($payload))
        $claims = $json | ConvertFrom-Json -ErrorAction Stop
        if ($null -eq $claims.exp) { return [DateTime]::MaxValue }
        return [DateTimeOffset]::FromUnixTimeSeconds([long]$claims.exp).UtcDateTime
    }
    catch {
        Write-Verbose "Get-JwtExpiry: failed to parse exp claim — $($_.Exception.Message)"
        return [DateTime]::MaxValue
    }
}

function Clear-AdlsTokenCache {
    # Drop the cached token so the next Get-AdlsAccessToken mints fresh. Used by
    # the 401 safety net (via -ForceRefresh) and available to callers that need
    # to force a clean re-auth after a credential rotation.
    $script:AdlsTokenCache = $null
}

function New-AdlsAccessToken {
    # Mints a Storage data-plane access token via one of three flows, selected
    # by STORAGE_AUTH_METHOD:
    #   managed_identity          (default) — ACA system-assigned MI
    #   service_principal_cert    — SP with a cert credential in our KV
    #   service_principal_secret  — SP with a client-secret credential in our KV
    # Centralizes the auth branch; callers go through the cached
    # Get-AdlsAccessToken wrapper rather than minting directly.
    $authMethod = $env:STORAGE_AUTH_METHOD ?? 'managed_identity'

    if ($authMethod -eq 'service_principal_cert') {
        # Delegate to the shared MSAL-cert helper that EXO and other containers
        # already rely on. It handles the Linux-pwsh PKCS12 fragility (RSA
        # rebind + CopyWithPrivateKey, [string]$null password, MSAL-native
        # assertion build with correct x5t Base64Url), which the prior
        # hand-rolled JWT path tripped over (issue #186). Audience is the only
        # Storage-specific bit; the helper takes arbitrary AAD audiences.
        $spCertBytes = Get-CertificateBytes -VaultName $env:KEYVAULT_NAME -CertName $env:STORAGE_SP_CERT_NAME
        try {
            return Get-IngestAccessToken `
                -CertBytes $spCertBytes `
                -ClientId  $env:STORAGE_SP_CLIENT_ID `
                -TenantId  $env:STORAGE_SP_TENANT_ID `
                -Audience  'https://storage.azure.com/.default'
        }
        finally {
            if ($spCertBytes) { [Array]::Clear($spCertBytes, 0, $spCertBytes.Length) }
        }
    }

    if ($authMethod -eq 'service_principal_secret') {
        # Same MSAL path as the cert branch, but the credential is a client
        # secret pulled from KV (Secrets User role only — no Certificate User
        # needed). Skips all the cert-rebind machinery since secrets don't
        # need PKCS12 handling.
        $spSecret = Get-SecretValue -VaultName $env:KEYVAULT_NAME -SecretName $env:STORAGE_SP_SECRET_NAME
        try {
            return Get-IngestAccessToken `
                -ClientSecret $spSecret `
                -ClientId     $env:STORAGE_SP_CLIENT_ID `
                -TenantId     $env:STORAGE_SP_TENANT_ID `
                -Audience     'https://storage.azure.com/.default'
        }
        finally {
            $spSecret = $null
        }
    }

    # ACA managed identity token — use Invoke-WebRequest + manual JSON parse
    # to avoid ExchangeOnlineManagement's Invoke-RestMethod proxy conflict.
    $response = Invoke-TokenRequest -RequestScript {
        Invoke-WebRequest `
            -Uri "$($env:IDENTITY_ENDPOINT)?resource=https://storage.azure.com/&api-version=2019-08-01" `
            -Headers @{ 'X-IDENTITY-HEADER' = $env:IDENTITY_HEADER } `
            -UseBasicParsing
    }
    return ($response.Content | ConvertFrom-Json).access_token
}

function Get-AdlsAccessToken {
    # Cache-fronted accessor for the Storage data-plane token. Returns the
    # cached bearer while it's still comfortably inside its lifetime, and only
    # mints a fresh one on a cold cache, near-expiry, or an explicit
    # -ForceRefresh (the 401 safety net). This is what keeps token fetches to
    # ~one per token lifetime no matter how many uploads / reads a run performs.
    param([switch]$ForceRefresh)

    if (-not $ForceRefresh -and $script:AdlsTokenCache) {
        $skew = [TimeSpan]::FromSeconds($script:AdlsTokenRefreshSkewSeconds)
        if ($script:AdlsTokenCache.ExpiresAt -and
            ([DateTime]::UtcNow + $skew) -lt $script:AdlsTokenCache.ExpiresAt) {
            return $script:AdlsTokenCache.Token
        }
    }

    $token = New-AdlsAccessToken
    $script:AdlsTokenCache = @{
        Token     = $token
        ExpiresAt = Get-JwtExpiry -Token $token
    }
    return $token
}

function Write-ToAdlsRest {
    param(
        [Parameter(Mandatory)][string]$StorageAccountUrl,
        [Parameter(Mandatory)][string]$ContainerName,
        [Parameter(Mandatory)][string]$BlobPath,
        [Parameter(Mandatory)][string]$LocalFile
    )

    $token = $null
    try {
        $baseUrl = "$StorageAccountUrl/$ContainerName/$BlobPath"
        $content = [System.IO.File]::ReadAllBytes($LocalFile)

        # 401 safety net wrapping the transient (5xx/429) retry below. A cached
        # token can be rejected if it lapses inside the refresh skew or the
        # storage credential is rotated in Key Vault mid-run; on a 401 we force
        # a fresh mint (bypassing the cache) and re-issue the upload. The inner
        # Invoke-AdlsDataPlaneWithRetry hard-fails 401 fast (non-transient), so
        # control returns here promptly to re-auth.
        $authAttempt = 0
        while ($true) {
            $authAttempt++
            $token = Get-AdlsAccessToken -ForceRefresh:($authAttempt -gt 1)

            $headers = @{
                'Authorization' = "Bearer $token"
                'x-ms-version'  = '2021-08-06'
            }

            try {
                # Use Invoke-WebRequest instead of Invoke-RestMethod to avoid
                # ExchangeOnlineManagement's Invoke-RestMethod proxy conflict.
                #
                # The whole create+append+flush triple is wrapped in one retry —
                # `PUT ?resource=file` overwrites by default, so re-running the
                # sequence on retry is idempotent. Wrapping per-step would have to
                # track which step failed and resume from there, with no real win
                # (network errors that interrupt mid-sequence are rare). See #345.
                Invoke-AdlsDataPlaneWithRetry -BlobPath $BlobPath -RequestScript {
                    # 1. Create file
                    Invoke-WebRequest -Uri "${baseUrl}?resource=file" `
                        -Method PUT -Headers $headers -UseBasicParsing | Out-Null

                    # 2. Append data
                    Invoke-WebRequest -Uri "${baseUrl}?action=append&position=0" `
                        -Method PATCH -Headers $headers `
                        -Body $content -ContentType 'application/octet-stream' -UseBasicParsing | Out-Null

                    # 3. Flush (finalize)
                    Invoke-WebRequest -Uri "${baseUrl}?action=flush&position=$($content.Length)" `
                        -Method PATCH -Headers $headers -UseBasicParsing | Out-Null
                }
                break
            }
            catch {
                $status = $null
                $resp = $_.Exception.Response
                if ($resp -and $resp.PSObject.Properties['StatusCode']) {
                    $status = [int]$resp.StatusCode
                }
                if ($status -eq 401 -and $authAttempt -eq 1) {
                    Write-Log "ADLS upload for $BlobPath returned 401; re-minting token and retrying once" -Level WARN
                    continue
                }
                throw
            }
        }
    }
    finally {
        $token = $null
    }
}

Export-ModuleMember -Function Write-ToAdlsRest, Get-AdlsAccessToken, Get-RetryAfterSeconds, Clear-AdlsTokenCache
