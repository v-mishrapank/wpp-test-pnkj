# Audit.General — catch-all bucket for workloads that don't have a dedicated
# content type in the Mgmt API. Per the API reference, the supported content
# types are: Audit.AzureActiveDirectory, Audit.Exchange, Audit.SharePoint,
# Audit.General, DLP.All. Anything else (Power BI, Power Apps, Power Automate,
# Microsoft Forms, Stream, Yammer, Defender, Purview, Microsoft 365 Copilot)
# routes through here.
#
# Power BI events specifically (RecordType=PowerBIAudit, Workload=PowerBI) are
# gated by the Fabric Admin Portal "Create audit logs for internal activity
# auditing and compliance" toggle. With it on, PowerBI events appear in this
# stream; without it, they don't. See docs/tenant-onboarding.md section 2d.
#
# Pulled via the Office 365 Management Activity API. See MgmtApiCommon.psm1.

function Get-ModuleStages {
    @{
        'audit_general_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-AuditGeneral'
            ApiFamily  = 'log'
        }
    }
}

function Get-ModuleEntities {
    # SelectFields omitted intentionally — see AuditEntra.psm1.
    @{
        'audit_general' = @{
            Stage    = 'audit_general_root'
            WritesTo = 'root'
        }
    }
}

function Get-AuditGeneral {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    Invoke-MgmtContentFetch -Entity 'audit_general' -ContentType 'Audit.General' -Writer $Writer
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-AuditGeneral
