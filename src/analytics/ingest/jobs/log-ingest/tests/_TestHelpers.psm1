# Shared test infrastructure for the log-ingest entity test files. Each
# per-entity *.Tests.ps1 imports this and uses New-CaptureWriter to receive
# $Writer.WriteRecord / EmitId calls in a hashtable the test can assert on.

function New-CaptureWriter {
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
    param([hashtable]$InputTags)
    return @{
        AuthConfig   = @{
            ClientId          = 'cid'
            TenantId          = 'tid'
            CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
        }
        WriteRecords = $true
        InputTags    = $InputTags
    }
}

function Initialize-MockConnect {
    Get-Module Connect | ForEach-Object {
        & $_ {
            $script:AuthConfig = @{
                ClientId          = 'cid'
                TenantId          = 'tid'
                CertificateBase64 = [Convert]::ToBase64String([byte[]]::new(8))
                GraphToken        = 'fake-graph-token'
                MgmtToken         = 'fake-mgmt-token'
            }
        }
    }
}

Export-ModuleMember -Function New-CaptureWriter, New-MockContext, Initialize-MockConnect
