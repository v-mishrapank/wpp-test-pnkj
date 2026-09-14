function Get-ModuleStages {
    @{
        'intune_root' = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-IntuneManagedDevicesRoot'
            ApiFamily           = 'graph'
            MinimumSelectFields = @('id')
        }
    }
}

function Get-ModuleEntities {
    @{
        'intune_managed_devices' = @{
            Stage        = 'intune_root'
            WritesTo     = 'root'
            SelectFields = @(
                'id','deviceName','managedDeviceOwnerType','enrolledDateTime','lastSyncDateTime',
                'operatingSystem','complianceState','jailBroken','managementAgent','osVersion',
                'azureADRegistered','deviceEnrollmentType','emailAddress','azureADDeviceId',
                'deviceRegistrationState','isEncrypted','userPrincipalName','model','manufacturer',
                'serialNumber','userId','userDisplayName','totalStorageSpaceInBytes',
                'freeStorageSpaceInBytes','managedDeviceName','partnerReportedThreatState',
                'autopilotEnrolled','isSupervised'
            )
        }
    }
}

function Get-IntuneManagedDevicesRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $select = $Context.SelectFields -join ','
    $uri = "/beta/deviceManagement/managedDevices?`$select=$select&`$top=1000"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($device in $response.value) {
            $Writer.WriteRecord($device)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-IntuneManagedDevicesRoot
