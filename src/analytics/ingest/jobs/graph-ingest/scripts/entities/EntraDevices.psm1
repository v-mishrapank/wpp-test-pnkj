function Get-ModuleStages {
    @{
        'devices_root' = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-EntraDevicesRoot'
            ApiFamily           = 'graph'
            MinimumSelectFields = @('id')
        }
    }
}

function Get-ModuleEntities {
    @{
        # Note: $expand=registeredOwners/registeredUsers is not supported on the
        # /devices collection endpoint (returns BadRequest). Owner correlation is
        # done via the silver layer using device.deviceId <-> user relationships.
        'entra_devices' = @{
            Stage        = 'devices_root'
            WritesTo     = 'root'
            SelectFields = @(
                'id','deviceId','displayName','operatingSystem','operatingSystemVersion',
                'trustType','isManaged','isCompliant','accountEnabled','approximateLastSignInDateTime',
                'createdDateTime','model','manufacturer','profileType','deviceCategory',
                'enrollmentProfileName','onPremisesSyncEnabled','onPremisesLastSyncDateTime',
                'onPremisesSecurityIdentifier','mdmAppId','registrationDateTime'
            )
        }
    }
}

function Get-EntraDevicesRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $select = $Context.SelectFields -join ','
    $uri = "/beta/devices?`$select=$select&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($device in $response.value) {
            $Writer.WriteRecord($device)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-EntraDevicesRoot
