# Audit.Exchange — mailbox audit events (mailbox login, send-as, hard delete,
# inbox-rule create/modify, message-class changes, etc.) and admin audit
# (cmdlet executions tracked by the EXO admin audit log).
#
# Pulled via the Office 365 Management Activity API. See MgmtApiCommon.psm1.

function Get-ModuleStages {
    @{
        'audit_exchange_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-AuditExchange'
            ApiFamily  = 'log'
        }
    }
}

function Get-ModuleEntities {
    # SelectFields omitted intentionally — Mgmt API has no $select; we land
    # the full record envelope. See AuditEntra.psm1 for the rationale.
    @{
        'audit_exchange' = @{
            Stage    = 'audit_exchange_root'
            WritesTo = 'root'
        }
    }
}

function Get-AuditExchange {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    Invoke-MgmtContentFetch -Entity 'audit_exchange' -ContentType 'Audit.Exchange' -Writer $Writer
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-AuditExchange
