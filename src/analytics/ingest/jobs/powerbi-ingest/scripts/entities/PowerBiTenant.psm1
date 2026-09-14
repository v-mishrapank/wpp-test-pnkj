# Tenant-singleton ingestion module for Power BI / Fabric.
#
# Stage graph (one inline root per entity — no parent/child within this module):
#
#   capacities             (inline, GET /v1.0/myorg/admin/capacities)              [api.powerbi.com]
#   apps                   (inline, GET /v1.0/myorg/admin/apps)                    [api.powerbi.com]
#   deployment_pipelines   (inline, GET /v1.0/myorg/admin/pipelines)               [api.powerbi.com]
#   fabric_lakehouses      (inline, GET /v1/admin/items?type=Lakehouse)            [api.fabric.microsoft.com]
#   fabric_warehouses      (inline, GET /v1/admin/items?type=Warehouse)            [api.fabric.microsoft.com]
#   fabric_kql_databases   (inline, GET /v1/admin/items?type=KQLDatabase)          [api.fabric.microsoft.com]
#   fabric_notebooks       (inline, GET /v1/admin/items?type=Notebook)             [api.fabric.microsoft.com]
#
# === Why "tenant" naming ===
#
# Workspaces, gateways, and tenant-singletons are three orthogonal top-level
# dimensions in Power BI. Workspaces is a family with deep fan-out; gateways
# is a small family; tenant-singletons are independent leaves. Forcing them
# into one module would inflate the file without sharing structure. Three
# modules, each with its own Get-ModuleStages / Get-ModuleEntities.
#
# === Auth + endpoint surface ===
#
# Shared helpers (Invoke-PowerBiRequest, Invoke-PowerBiPagedFetch,
# Invoke-FabricPagedFetch, ConvertTo-PowerBiArray) live in Connect.psm1
# alongside the token cache — they need to be visible cross-entity-module
# (e.g., PowerBiGateways uses them too), and only Connect.psm1's exports
# resolve from arbitrary fetcher contexts at runtime.
#
# Two audiences passed via Invoke-PowerBiRequest's -Audience param:
#   - Power BI legacy admin (https://analysis.windows.net/powerbi/api/.default) —
#     V1 admin endpoints on api.powerbi.com. Default; capacities, apps,
#     deployment_pipelines.
#   - Fabric (https://api.fabric.microsoft.com/.default) — Fabric REST API on
#     api.fabric.microsoft.com. The four fabric_* entities via
#     /v1/admin/items?type=... Both audiences gated by the same per-tenant
#     Fabric admin SP toggle.

# === Per-fetcher constants ===

$PowerBiBaseUri = 'https://api.powerbi.com'
$AdminApiVer    = 'v1.0'

$FabricBaseUri  = 'https://api.fabric.microsoft.com'
$FabricApiVer   = 'v1'

# === Stage + entity declarations ===

function Get-ModuleStages {
    @{
        'capacities'           = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerBiCapacities'
            ApiFamily  = 'powerbi'
        }
        'apps'                 = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerBiApps'
            ApiFamily  = 'powerbi'
        }
        'deployment_pipelines' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerBiDeploymentPipelines'
            ApiFamily  = 'powerbi'
        }
        'fabric_lakehouses'    = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerBiFabricLakehouses'
            ApiFamily  = 'powerbi'
        }
        'fabric_warehouses'    = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerBiFabricWarehouses'
            ApiFamily  = 'powerbi'
        }
        'fabric_kql_databases' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerBiFabricKqlDatabases'
            ApiFamily  = 'powerbi'
        }
        'fabric_notebooks'     = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerBiFabricNotebooks'
            ApiFamily  = 'powerbi'
        }
    }
}

function Get-ModuleEntities {
    @{
        'powerbi_capacities'           = @{ Stage = 'capacities';           WritesTo = 'root' }
        'powerbi_apps'                 = @{ Stage = 'apps';                 WritesTo = 'root' }
        'powerbi_deployment_pipelines' = @{ Stage = 'deployment_pipelines'; WritesTo = 'root' }
        'powerbi_fabric_lakehouses'    = @{ Stage = 'fabric_lakehouses';    WritesTo = 'root' }
        'powerbi_fabric_warehouses'    = @{ Stage = 'fabric_warehouses';    WritesTo = 'root' }
        'powerbi_fabric_kql_databases' = @{ Stage = 'fabric_kql_databases'; WritesTo = 'root' }
        'powerbi_fabric_notebooks'     = @{ Stage = 'fabric_notebooks';     WritesTo = 'root' }
    }
}

# === Fetchers ===

function Get-PowerBiCapacities {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    Invoke-PowerBiPagedFetch -Uri "$PowerBiBaseUri/$AdminApiVer/myorg/admin/capacities" -Writer $Writer
}

function Get-PowerBiApps {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # /admin/apps requires $top to bound results (defaults to error otherwise);
    # Microsoft caps $top at 5000 per request and recommends paging via $skip.
    # For inventory at any scale, 5000 covers most tenants in one call; large
    # tenants we paginate via @odata.nextLink (the V1 endpoint emits it when
    # results exceed the page size).
    Invoke-PowerBiPagedFetch -Uri "$PowerBiBaseUri/$AdminApiVer/myorg/admin/apps?`$top=5000" -Writer $Writer
}

function Get-PowerBiDeploymentPipelines {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # Admin tenant-wide pipeline list. Cheaper than per-pipeline /pipelines/{id}
    # which we'd only need for stage details (deferred to a future entity).
    Invoke-PowerBiPagedFetch -Uri "$PowerBiBaseUri/$AdminApiVer/myorg/admin/pipelines" -Writer $Writer
}

function Get-PowerBiFabricLakehouses {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    Invoke-FabricPagedFetch -Uri "$FabricBaseUri/$FabricApiVer/admin/items?type=Lakehouse" -Writer $Writer
}

function Get-PowerBiFabricWarehouses {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    Invoke-FabricPagedFetch -Uri "$FabricBaseUri/$FabricApiVer/admin/items?type=Warehouse" -Writer $Writer
}

function Get-PowerBiFabricKqlDatabases {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    Invoke-FabricPagedFetch -Uri "$FabricBaseUri/$FabricApiVer/admin/items?type=KQLDatabase" -Writer $Writer
}

function Get-PowerBiFabricNotebooks {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    Invoke-FabricPagedFetch -Uri "$FabricBaseUri/$FabricApiVer/admin/items?type=Notebook" -Writer $Writer
}

Export-ModuleMember -Function `
    Get-ModuleStages, Get-ModuleEntities, `
    Get-PowerBiCapacities, Get-PowerBiApps, Get-PowerBiDeploymentPipelines, `
    Get-PowerBiFabricLakehouses, Get-PowerBiFabricWarehouses, `
    Get-PowerBiFabricKqlDatabases, Get-PowerBiFabricNotebooks
