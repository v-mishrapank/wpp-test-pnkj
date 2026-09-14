# Audit.SharePoint — SPO and OneDrive activity (file accesses, sharing,
# external sharing invites, site-collection admin operations, etc.). High
# volume on busy tenants — typical largest content type by record count.
#
# Pulled via the Office 365 Management Activity API. See MgmtApiCommon.psm1.

function Get-ModuleStages {
    @{
        'audit_sharepoint_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-AuditSharePoint'
            ApiFamily  = 'log'
        }
    }
}

function Get-ModuleEntities {
    # SelectFields omitted intentionally — see AuditEntra.psm1.
    @{
        'audit_sharepoint' = @{
            Stage    = 'audit_sharepoint_root'
            WritesTo = 'root'
        }
    }
}

function Get-AuditSharePoint {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    Invoke-MgmtContentFetch -Entity 'audit_sharepoint' -ContentType 'Audit.SharePoint' -Writer $Writer
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-AuditSharePoint
