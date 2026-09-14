# Consolidated ingestion module for the Entra users family.
#
# Stage graph:
#   users_root    (inline, /v1.0/users?$expand=manager)
#
# Manager is a 0..1 relationship per user, folded into the user record via
# $expand=manager on the list call. Users without a manager simply have a
# null/absent `manager` property — no separate per-user fetch, no 404 storm.
# Previously a separate user_managers pool stage made one /users/{id}/manager
# call per user; on tenants where most users have no manager configured,
# 100% of those calls 404'd and the resulting item_failed events
# stdout-contended the worker pool to ~1 item/sec aggregate. See #407.
# DLT migration to consume the inline manager tracked in #417.

function Get-ModuleStages {
    @{
        'users_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-EntraUsersRoot'
            ApiFamily  = 'graph'
        }
    }
}

function Get-ModuleEntities {
    @{
        'entra_users' = @{
            Stage        = 'users_root'
            WritesTo     = 'root'
            SelectFields = @(
                # Identity
                'id','userPrincipalName','mail','displayName','givenName','surname','mailNickname'
                # Organization
                'jobTitle','department','officeLocation','city','state','country','companyName'
                'streetAddress','postalCode','usageLocation','preferredLanguage','preferredDataLocation'
                # Contact
                'businessPhones','mobilePhone','faxNumber','otherMails'
                # Employee
                'employeeId','employeeType','employeeHireDate','employeeOrgData'
                # Account status
                'accountEnabled','userType','creationType','createdDateTime'
                'lastPasswordChangeDateTime','passwordPolicies'
                'securityIdentifier'
                # Guest / external
                'externalUserState','externalUserStateChangeDateTime','identities'
                # Licensing & sync
                'assignedLicenses','proxyAddresses'
                'onPremisesSyncEnabled','onPremisesLastSyncDateTime','onPremisesDomainName'
                'onPremisesDistinguishedName','onPremisesExtensionAttributes','onPremisesImmutableId'
                'onPremisesProvisioningErrors','onPremisesSamAccountName','onPremisesSecurityIdentifier'
                'onPremisesUserPrincipalName','serviceProvisioningErrors'
                # Manager — folded inline via $expand=manager in Get-EntraUsersRoot.
                # MUST be listed here so StageWriter's projection keeps the
                # nested manager object on the landed record; without this the
                # writer drops every key not in SelectFields. Shape on landed
                # record: { manager: { id, displayName, userPrincipalName, mail } | null }.
                'manager'
            )
        }
    }
}

function Get-EntraUsersRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Scope filter (issue #260): if SCOPE_ROOT is set, load the UPN set and
    # filter records before WriteRecord. We keep the full paginated /users
    # pull because per-Identity /users/{UPN} calls for a 55K-user scope are
    # more expensive than one paginated full enumeration; the savings here
    # are landing volume, not API calls.
    $scopeUpns = $null
    if ($env:SCOPE_ROOT) {
        $scopeUpns = Get-ScopeKeySet `
            -ScopeRoot $env:SCOPE_ROOT `
            -Dimension 'users' `
            -KeyField 'userPrincipalName' `
            -StorageAccountUrl $env:STORAGE_ACCOUNT_URL `
            -ContainerName $env:LANDING_CONTAINER
        if ($null -eq $scopeUpns) {
            Write-Log "entra_users: scope enabled but no scope file found; skipping all records" -Level WARN
            return
        }
        Write-Log "entra_users: scope enabled with $($scopeUpns.Count) UPN(s); filtering records"
    }

    $select = $Context.SelectFields -join ','
    # Manager $select kept to the same four-field denormalized shape the
    # standalone user_managers stage used to land. DLT (silver schema in
    # #417) flattens exactly these to manager_entra_object_id / display_name
    # / upn / mail. Adding fields here without a coordinated silver update
    # would be a no-op at best, schema drift at worst.
    $managerSelect = 'id,displayName,userPrincipalName,mail'
    $uri = "/v1.0/users?`$select=$select&`$expand=manager(`$select=$managerSelect)&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($user in $response.value) {
            if ($scopeUpns -and -not $scopeUpns.Contains([string]$user.userPrincipalName)) {
                continue
            }
            if ($Context.WriteRecords) { $Writer.WriteRecord($user) }
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-EntraUsersRoot
