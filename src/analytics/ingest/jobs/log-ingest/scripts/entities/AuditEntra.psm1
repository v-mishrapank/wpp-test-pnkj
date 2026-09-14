# Audit.AzureActiveDirectory — Entra-side audit events (sign-ins, app role
# assignments, conditional access changes, B2B invitations, role assignment
# changes, password resets, MFA enrollment, etc.).
#
# Pulled via the Office 365 Management Activity API. See MgmtApiCommon.psm1
# for the shared fetch loop, subscription bootstrap, HWM, and backfill mode.

function Get-ModuleStages {
    @{
        'audit_entra_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-AuditEntra'
            ApiFamily  = 'log'
        }
    }
}

function Get-ModuleEntities {
    # SelectFields is intentionally absent — the Mgmt API does not support
    # field projection (no $select), records are heterogeneous (schema varies
    # by event type), and the framework lands the full record envelope when
    # the key is omitted. DLT bronze handles schema-on-read.
    @{
        'audit_entra' = @{
            Stage    = 'audit_entra_root'
            WritesTo = 'root'
        }
    }
}

function Get-AuditEntra {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    Invoke-MgmtContentFetch -Entity 'audit_entra' -ContentType 'Audit.AzureActiveDirectory' -Writer $Writer
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-AuditEntra
