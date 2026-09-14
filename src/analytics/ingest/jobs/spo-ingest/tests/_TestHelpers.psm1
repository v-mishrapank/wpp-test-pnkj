# Shared test infrastructure for the SPO entity test files. Each per-entity
# *.Tests.ps1 imports this and uses New-CaptureWriter to receive
# $Writer.WriteRecord / EmitId calls in a hashtable the test can assert on.
#
# Imported via `Import-Module` from BeforeAll so the helper functions are
# available inside both the test scope and the InModuleScope 'SpoSites' blocks
# (this module is at "global" relative to the SpoSites module).

function New-CaptureWriter {
    <#
    Mimics the New-StageWriter shape that StageExecutor/WorkerPool would hand
    a fetcher in production. Single-entity, raw-passthrough — no SelectFields
    projection (the rewrite lands records verbatim from REST). Exposes the
    captured records as $writer.Records and emitted IDs as $writer.EmittedIds.
    #>
    $writer = [pscustomobject]@{
        Records             = [System.Collections.Generic.List[object]]::new()
        EmittedIds          = [System.Collections.Generic.List[hashtable]]::new()
        TotalWritten        = 0
    }
    $writer | Add-Member -MemberType ScriptMethod -Name WriteRecord -Value {
        param($record)
        [void]$this.Records.Add($record)
        $this.TotalWritten++
    }
    $writer | Add-Member -MemberType ScriptMethod -Name EmitId -Value {
        param([string]$id, $tags)
        [void]$this.EmittedIds.Add(@{ Id = $id; Tags = $tags })
    }
    return $writer
}

function New-MockContext {
    <#
    Builds the $Context hashtable that StageExecutor/WorkerPool would build
    for a fetcher. WriteRecords=$true is the default — the per-stage flag
    that gates record materialization at the Get-Spo*Root level.
    #>
    param(
        [hashtable]$AuthConfig,
        [hashtable]$InputTags
    )
    if (-not $AuthConfig) {
        $AuthConfig = @{
            ClientId          = 'cid'
            TenantId          = 'tid'
            CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
            AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
            TenantPrefix      = 'xtlab2'
            # Mirror the Hosts/Audiences shape Connect-Service populates so
            # fetchers that read $Context.AuthConfig.Hosts.My (e.g.
            # Get-SpoSitesRoot's scope branch) work the same in tests as
            # in production.
            Hosts             = @{
                Main  = 'xtlab2.sharepoint.com'
                My    = 'xtlab2-my.sharepoint.com'
                Admin = 'xtlab2-admin.sharepoint.com'
                Graph = 'graph.microsoft.com'
            }
        }
    }
    return @{
        AuthConfig    = $AuthConfig
        WriteRecords  = $true
        InputTags     = $InputTags
    }
}

function Initialize-MockConnect {
    <#
    Pre-populate the Connect module's $script:AuthConfig with pre-minted
    cached tokens for all four audiences (xtlab2 test tenant + Graph).
    Mocking Get-SpoToken from inside InModuleScope 'SpoSites' didn't intercept
    the cross-module call (the closure binds to the SpoSites scope but the
    function name resolves through Connect's command table). Pre-populating
    the cache bypasses the live-mint code path entirely — every Get-SpoToken
    call gets a cache hit.
    #>
    Get-Module Connect | ForEach-Object {
        & $_ {
            $script:AuthConfig = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                AdminUrl          = 'https://xtlab2-admin.sharepoint.com'
                TenantPrefix      = 'xtlab2'
                Hosts             = @{
                    Main  = 'xtlab2.sharepoint.com'
                    My    = 'xtlab2-my.sharepoint.com'
                    Admin = 'xtlab2-admin.sharepoint.com'
                    Graph = 'graph.microsoft.com'
                }
                Audiences         = @{
                    'xtlab2.sharepoint.com'       = 'https://xtlab2.sharepoint.com/.default'
                    'xtlab2-my.sharepoint.com'    = 'https://xtlab2-my.sharepoint.com/.default'
                    'xtlab2-admin.sharepoint.com' = 'https://xtlab2-admin.sharepoint.com/.default'
                    'graph.microsoft.com'         = 'https://graph.microsoft.com/.default'
                }
                TokenCache        = @{
                    'xtlab2.sharepoint.com'       = @{ Token = 'fake-main-token';  ExpiresAt = [DateTime]::UtcNow.AddHours(1) }
                    'xtlab2-my.sharepoint.com'    = @{ Token = 'fake-my-token';    ExpiresAt = [DateTime]::UtcNow.AddHours(1) }
                    'xtlab2-admin.sharepoint.com' = @{ Token = 'fake-admin-token'; ExpiresAt = [DateTime]::UtcNow.AddHours(1) }
                    'graph.microsoft.com'         = @{ Token = 'fake-graph-token'; ExpiresAt = [DateTime]::UtcNow.AddHours(1) }
                }
            }
        }
    }
}

# Helper — wraps a single PSCustomObject as a verbose-envelope row, as
# Invoke-RestMethod would deserialize from the wire.
function New-VerboseEnvelope {
    param(
        [Parameter(Mandatory)][array]$Results,
        [string]$NextLink
    )
    $d = [pscustomobject]@{
        results = $Results
        __next  = $NextLink
    }
    return [pscustomobject]@{ d = $d }
}

# Helper — wraps an array as the verbose-array shape Invoke-RestMethod
# returns for an $expand'd nested collection: { __metadata; results: [...] }.
# Used in mocks where the REST response carries nested collections under an
# expanded property.
function New-VerboseNestedArray {
    param([array]$Items)
    return [pscustomobject]@{
        __metadata = [pscustomobject]@{ type = 'Collection(Microsoft.SharePoint.Item)' }
        results    = $Items
    }
}

Export-ModuleMember -Function New-CaptureWriter, New-MockContext, New-VerboseEnvelope, New-VerboseNestedArray, Initialize-MockConnect
