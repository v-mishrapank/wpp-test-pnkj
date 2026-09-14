function Get-ModuleStages {
    @{
        'grants_root' = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-EntraDelegatedPermissionGrantsRoot'
            ApiFamily           = 'graph'
            MinimumSelectFields = @('id')
        }
    }
}

function Get-ModuleEntities {
    @{
        'entra_delegated_permission_grants' = @{
            # oauth2PermissionGrants endpoint doesn't accept $select, so no
            # field filtering happens anywhere in the pipeline. SelectFields
            # is omitted to keep the metadata honest: there is no field list
            # for a dev to update that the framework would honor.
            Stage    = 'grants_root'
            WritesTo = 'root'
        }
    }
}

function Get-EntraDelegatedPermissionGrantsRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $uri = '/v1.0/oauth2PermissionGrants?$top=999'

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($grant in $response.value) {
            $Writer.WriteRecord($grant)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-EntraDelegatedPermissionGrantsRoot
