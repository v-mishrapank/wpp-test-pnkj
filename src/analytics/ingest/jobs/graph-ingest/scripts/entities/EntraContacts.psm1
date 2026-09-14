function Get-ModuleStages {
    @{
        'contacts_root' = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-EntraContactsRoot'
            ApiFamily           = 'graph'
            MinimumSelectFields = @('id')
        }
    }
}

function Get-ModuleEntities {
    @{
        'entra_contacts' = @{
            Stage        = 'contacts_root'
            WritesTo     = 'root'
            SelectFields = @(
                'id','displayName','givenName','surname','mail','jobTitle','department',
                'companyName','phones','addresses','proxyAddresses','onPremisesSyncEnabled',
                'onPremisesLastSyncDateTime','onPremisesProvisioningErrors','serviceProvisioningErrors'
            )
        }
    }
}

function Get-EntraContactsRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $select = $Context.SelectFields -join ','
    $uri = "/v1.0/contacts?`$select=$select&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($contact in $response.value) {
            $Writer.WriteRecord($contact)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-EntraContactsRoot
