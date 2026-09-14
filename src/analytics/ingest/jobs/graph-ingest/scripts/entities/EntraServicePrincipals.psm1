# Consolidated ingestion module for the Entra service-principal family.
#
# Stage graph:
#   sps_root (inline, /v1.0/servicePrincipals)
#     |- sp_owners                  (pool, /v1.0/servicePrincipals/{id}/owners)
#     |- sp_role_assignees          (pool, /v1.0/servicePrincipals/{id}/appRoleAssignedTo)
#     |- sp_claims_mapping_policies (pool, /v1.0/servicePrincipals/{id}/claimsMappingPolicies)
#     |- sp_app_role_assignments    (pool, /v1.0/servicePrincipals/{id}/appRoleAssignments)
#     |- sp_perm_classifications    (pool, /v1.0/servicePrincipals/{id}/delegatedPermissionClassifications)
#     |- sp_provisioning_jobs       (pool, /v1.0/servicePrincipals/{id}/synchronization/jobs)

function Get-ModuleStages {
    @{
        'sps_root'                   = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-EntraSpsRoot'
            ApiFamily           = 'graph'
            IdKey               = 'id'
            EmitIds             = $true
            MinimumSelectFields = @('id')
        }
        'sp_owners'                  = @{
            InputFrom  = 'sps_root'
            RunsOnPool = $true
            Function   = 'Get-EntraSpOwners'
            ApiFamily  = 'graph'
        }
        'sp_role_assignees'          = @{
            InputFrom  = 'sps_root'
            RunsOnPool = $true
            Function   = 'Get-EntraSpRoleAssignees'
            ApiFamily  = 'graph'
        }
        'sp_claims_mapping_policies' = @{
            InputFrom  = 'sps_root'
            RunsOnPool = $true
            Function   = 'Get-EntraSpClaimsMappingPolicies'
            ApiFamily  = 'graph'
        }
        'sp_app_role_assignments'    = @{
            InputFrom  = 'sps_root'
            RunsOnPool = $true
            Function   = 'Get-EntraSpAppRoleAssignments'
            ApiFamily  = 'graph'
        }
        'sp_perm_classifications'    = @{
            InputFrom  = 'sps_root'
            RunsOnPool = $true
            Function   = 'Get-EntraSpPermClassifications'
            ApiFamily  = 'graph'
        }
        'sp_provisioning_jobs'       = @{
            InputFrom  = 'sps_root'
            RunsOnPool = $true
            Function   = 'Get-EntraSpProvisioningJobs'
            ApiFamily  = 'graph'
        }
    }
}

function Get-ModuleEntities {
    @{
        'entra_service_principals'         = @{
            Stage        = 'sps_root'
            WritesTo     = 'root'
            SelectFields = @(
                'id','appId','appDisplayName','displayName','servicePrincipalType',
                'appOwnerOrganizationId','accountEnabled','appRoleAssignmentRequired',
                'appRoles','oauth2PermissionScopes','tags','servicePrincipalNames',
                'homepage','loginUrl','logoutUrl','replyUrls','keyCredentials',
                'passwordCredentials','preferredSingleSignOnMode','samlSingleSignOnSettings',
                'signInAudience','notes','notificationEmailAddresses','info',
                'applicationTemplateId','verifiedPublisher','alternativeNames',
                'tokenEncryptionKeyId','resourceSpecificApplicationPermissions','description',
                'disabledByMicrosoftStatus','preferredTokenSigningKeyThumbprint'
            )
        }
        'entra_sp_owners'                  = @{
            Stage        = 'sp_owners'
            WritesTo     = 'owners'
            SelectFields = @('servicePrincipalId','id','displayName','userPrincipalName','mail')
        }
        # The five sub-resource entities below omit SelectFields because the
        # Graph endpoints backing them don't accept $select and the fetch
        # lands the server's full default payload (plus the synthesized
        # servicePrincipalId parent key). Downstream bronze/silver consumes
        # those native shapes directly. Declaring SelectFields here would
        # either lose fields (via writer projection) or lie about what the
        # fetch actually emits. If a future contributor wants a narrower
        # schema, add SelectFields AND verify each pipeline consumer is OK
        # losing the omitted fields.
        'entra_sp_role_assignees'          = @{
            Stage    = 'sp_role_assignees'
            WritesTo = 'role_assignees'
        }
        'entra_sp_claims_mapping_policies' = @{
            Stage    = 'sp_claims_mapping_policies'
            WritesTo = 'claims_mapping_policies'
        }
        'entra_sp_app_role_assignments'    = @{
            Stage    = 'sp_app_role_assignments'
            WritesTo = 'app_role_assignments'
        }
        'entra_sp_perm_classifications'    = @{
            Stage    = 'sp_perm_classifications'
            WritesTo = 'perm_classifications'
        }
        'entra_sp_provisioning_jobs'       = @{
            Stage    = 'sp_provisioning_jobs'
            WritesTo = 'provisioning_jobs'
        }
    }
}

function Get-EntraSpsRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $select = $Context.SelectFields -join ','
    $uri = "/v1.0/servicePrincipals?`$select=$select&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($sp in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($sp) }
            $Writer.EmitId($sp.id, $null)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-EntraSpOwners {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $select = ($Context.SelectFields | Where-Object { $_ -ne 'servicePrincipalId' }) -join ','
    $uri = "/v1.0/servicePrincipals/$InputId/owners?`$select=$select&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($owner in $response.value) {
            $owner['servicePrincipalId'] = $InputId
            $Writer.WriteRecord($owner)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-EntraSpRoleAssignees {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $uri = "/v1.0/servicePrincipals/$InputId/appRoleAssignedTo?`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($assignment in $response.value) {
            $assignment['servicePrincipalId'] = $InputId
            $Writer.WriteRecord($assignment)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-EntraSpClaimsMappingPolicies {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $uri = "/v1.0/servicePrincipals/$InputId/claimsMappingPolicies"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($policy in $response.value) {
            $policy['servicePrincipalId'] = $InputId
            $Writer.WriteRecord($policy)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-EntraSpAppRoleAssignments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $uri = "/v1.0/servicePrincipals/$InputId/appRoleAssignments?`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($grant in $response.value) {
            $grant['servicePrincipalId'] = $InputId
            $Writer.WriteRecord($grant)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-EntraSpPermClassifications {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $uri = "/v1.0/servicePrincipals/$InputId/delegatedPermissionClassifications"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($item in $response.value) {
            $item['servicePrincipalId'] = $InputId
            $Writer.WriteRecord($item)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-EntraSpProvisioningJobs {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $uri = "/v1.0/servicePrincipals/$InputId/synchronization/jobs"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($item in $response.value) {
            $item['servicePrincipalId'] = $InputId
            $Writer.WriteRecord($item)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-EntraSpsRoot, Get-EntraSpOwners, Get-EntraSpRoleAssignees, Get-EntraSpClaimsMappingPolicies, Get-EntraSpAppRoleAssignments, Get-EntraSpPermClassifications, Get-EntraSpProvisioningJobs
