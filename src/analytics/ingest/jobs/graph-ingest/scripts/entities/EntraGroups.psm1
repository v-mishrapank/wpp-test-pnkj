# Consolidated ingestion module for the Entra groups family.
#
# Stage graph:
#   groups_root (inline, /v1.0/groups)
#     |- group_members (pool, /v1.0/groups/{id}/members)
#     |- group_owners  (pool, /v1.0/groups/{id}/owners)
#
# The root stage fetches once and emits group IDs to both child stages.
# Its $select is computed by the planner from Resolve-SelectFields — if
# entra_groups is requested, full field list; if only members/owners are
# requested, just 'id'.

function Get-ModuleStages {
    @{
        'groups_root'   = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-EntraGroupsRoot'
            ApiFamily           = 'graph'
            IdKey               = 'id'
            EmitIds             = $true
            MinimumSelectFields = @('id')
        }
        'group_members' = @{
            InputFrom  = 'groups_root'
            RunsOnPool = $true
            Function   = 'Get-EntraGroupMembers'
            ApiFamily  = 'graph'
        }
        'group_owners'  = @{
            InputFrom  = 'groups_root'
            RunsOnPool = $true
            Function   = 'Get-EntraGroupOwners'
            ApiFamily  = 'graph'
        }
    }
}

function Get-ModuleEntities {
    @{
        'entra_groups'        = @{
            Stage        = 'groups_root'
            WritesTo     = 'root'
            SelectFields = @(
                'id','displayName','description','mail','mailEnabled','mailNickname',
                'securityEnabled','groupTypes','membershipRule','membershipRuleProcessingState',
                'onPremisesSyncEnabled','onPremisesLastSyncDateTime','onPremisesDomainName',
                'onPremisesNetBiosName','onPremisesProvisioningErrors','onPremisesSamAccountName',
                'onPremisesSecurityIdentifier','serviceProvisioningErrors','createdDateTime',
                'proxyAddresses','visibility','resourceProvisioningOptions'
            )
        }
        'entra_group_members' = @{
            Stage        = 'group_members'
            WritesTo     = 'members'
            SelectFields = @('groupId','memberType','id','displayName','userPrincipalName','mail')
        }
        'entra_group_owners'  = @{
            Stage        = 'group_owners'
            WritesTo     = 'owners'
            SelectFields = @('groupId','id','displayName','userPrincipalName','mail')
        }
    }
}

function Get-EntraGroupsRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $select = $Context.SelectFields -join ','
    $uri = "/v1.0/groups?`$select=$select&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($group in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($group) }
            $Writer.EmitId($group.id, $null)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-EntraGroupMembers {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # memberType is synthetic (derived from @odata.type, not a Graph field),
    # so it must not be sent to Graph in $select.
    $select = ($Context.SelectFields | Where-Object { $_ -notin @('groupId','memberType') }) -join ','
    $uri = "/v1.0/groups/$InputId/members?`$select=$select&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($member in $response.value) {
            $member['groupId'] = $InputId
            # memberType: strip Graph's "#microsoft.graph." namespace prefix
            # from @odata.type. Fallback 'unknown' (rather than $null) keeps
            # the field categorical across all rows — matches Resolve-SpPrincipal's
            # convention in SpoSites for consistent downstream treatment.
            $odataType = $member['@odata.type']
            $member['memberType'] = if ($odataType -like '#microsoft.graph.*') {
                $odataType.Substring('#microsoft.graph.'.Length)
            } else {
                'unknown'
            }
            $Writer.WriteRecord($member)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-EntraGroupOwners {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $select = ($Context.SelectFields | Where-Object { $_ -ne 'groupId' }) -join ','
    $uri = "/v1.0/groups/$InputId/owners?`$select=$select&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($owner in $response.value) {
            $owner['groupId'] = $InputId
            $Writer.WriteRecord($owner)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-EntraGroupsRoot, Get-EntraGroupMembers, Get-EntraGroupOwners
