# Tenant-wide gateway-cluster ingestion module for Power BI / Fabric.
#
# Stage graph:
#
#   gateway_clusters_root           (inline; admin GET /v2.0/myorg/gatewayClusters
#                                    ?$expand=permissions,memberGateways)
#     │  Single-entity → powerbi_gateway_clusters (full cluster row,
#     │  permissions[] + memberGateways[] inline as JSON arrays).
#     │  Emits clusterId for the data_sources pool child.
#     │
#     └── gateway_cluster_data_sources
#           (pool, GET /v2.0/myorg/gatewayClusters/{clusterId}/datasources?$expand=users)
#           Per-cluster data sources. users[] kept inline as JSON.
#           Lands at powerbi_gateway_cluster_data_sources.
#
#   gateway_cluster_permissions_root (inline; SAME endpoint as gateway_clusters_root)
#     │  Single-entity → powerbi_gateway_cluster_permissions (one flat row
#     │  per (cluster, principal, role)).
#
# === Why two roots calling the same endpoint ===
#
# Power BI V2 exposes cluster permissions only via $expand on the list
# endpoint — there's no per-cluster /permissions endpoint that returns
# permissions standalone. The framework constraint (StageExecutor.psm1:502)
# rejects inline multi-entity stages: "Multi-entity fan-out is pool-only".
# Splitting into two single-entity inline roots costs one extra tenant-wide
# call (cheap) and keeps each stage framework-compliant. The alternative —
# making this a pool child with a synthesized input — would add unnecessary
# fan-out machinery for what is fundamentally a tenant-singleton fetch.
#
# === Migration-relevance ===
#
# Gateways do not move tenant-to-tenant. The `connectionDetails` JSON on each
# data source enumerates the on-prem servers / SharePoint URLs / SAP systems
# / Azure SQL endpoints that must be reprovisioned in the destination tenant.
# Plus each cluster's memberGateways[] (inline in the bronze) lists the
# physical / VM hosts running gateway processes — useful for inventorying
# the destination-side install plan. Both data points feed the silver-layer
# migration scoping (#245).
#
# === Auth + endpoint surface ===
#
# Same Power BI audience as the rest of the family
# (https://analysis.windows.net/powerbi/api/.default). Same Fabric admin
# SP toggle authorizes ("Service principals can access read-only admin
# APIs"). No new per-tenant onboarding step relative to C1/C2.
#
# === V2 endpoint quirks ===
#
# Discovered via Fiddler against Get-DataGatewayCluster (PowerShell
# DataGateway module) — not documented on Microsoft Learn. Behavior captured
# in #242:
#
# - Response wrapper: `.value` array per OData convention. ConvertTo-PowerBiArray
#   below normalizes either `.value` or top-level array shape.
# - Pagination: V2 /gatewayClusters supports $skip-based offset paging.
#   First pass doesn't paginate — typical tenants have <100 clusters and
#   the endpoint returns all of them in one shot. Add $skip handling if a
#   real tenant exceeds the implicit page size.
# - $expand=permissions,memberGateways inlines both arrays into each cluster
#   record. Empty arrays are returned for orphaned clusters with no
#   permissions / members.

# === Per-fetcher constants ===

$PowerBiBaseUri = 'https://api.powerbi.com'
$V2ApiVer       = 'v2.0'

$ClustersUri    = "$PowerBiBaseUri/$V2ApiVer/myorg/gatewayClusters?`$expand=permissions,memberGateways"

# === Stage + entity declarations ===

function Get-ModuleStages {
    @{
        'gateway_clusters_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerBiGatewayClustersRoot'
            ApiFamily  = 'powerbi'
            EmitIds    = $true
            IdKey      = 'id'
        }
        'gateway_cluster_permissions_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerBiGatewayClusterPermissionsRoot'
            ApiFamily  = 'powerbi'
        }
        'gateway_cluster_data_sources' = @{
            InputFrom  = 'gateway_clusters_root'
            RunsOnPool = $true
            Function   = 'Get-PowerBiGatewayClusterDataSources'
            ApiFamily  = 'powerbi'
        }
    }
}

function Get-ModuleEntities {
    @{
        'powerbi_gateway_clusters' = @{
            Stage    = 'gateway_clusters_root'
            WritesTo = 'root'
        }
        'powerbi_gateway_cluster_permissions' = @{
            Stage    = 'gateway_cluster_permissions_root'
            WritesTo = 'root'
        }
        'powerbi_gateway_cluster_data_sources' = @{
            Stage    = 'gateway_cluster_data_sources'
            WritesTo = 'root'
        }
    }
}

# Helpers (Invoke-PowerBiRequest, ConvertTo-PowerBiArray) live in Connect.psm1.

# === Fetchers ===

function Get-PowerBiGatewayClustersRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Single fetch with $expand. Each cluster carries permissions[] and
    # memberGateways[] arrays inline. We write the cluster record verbatim
    # to powerbi_gateway_clusters (DLT silver-layer can explode the inline
    # arrays as needed) and EmitId(clusterId) so the data_sources pool
    # child runs per cluster. The permissions[] array is also flattened by
    # gateway_cluster_permissions_root via a separate call to the same URL
    # (cheap; one tenant-wide fetch).
    $response = Invoke-PowerBiRequest -Uri $ClustersUri
    $clusters = ConvertTo-PowerBiArray -Response $response

    foreach ($cluster in $clusters) {
        $Writer.WriteRecord($cluster)
        $Writer.EmitId($cluster.id, $null)
    }
}

function Get-PowerBiGatewayClusterPermissionsRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Same endpoint as gateway_clusters_root. We re-fetch (rather than share
    # state with the sibling root) to keep each stage independently
    # restartable and stay within the framework's single-entity-per-inline-
    # stage constraint (StageExecutor.psm1:502). Cost is one extra tenant-
    # wide call per run — negligible on the V2 admin API throttle budget.
    $response = Invoke-PowerBiRequest -Uri $ClustersUri
    $clusters = ConvertTo-PowerBiArray -Response $response

    foreach ($cluster in $clusters) {
        if (-not $cluster.permissions) { continue }
        foreach ($perm in $cluster.permissions) {
            # Flatten — keys mirror V2 cluster permission shape:
            #   { id, principalType, role, principalDisplayName, principalEmail,
            #     tenantId, allowedDataSourceTypes }
            # Stamp clusterId for FK back to powerbi_gateway_clusters.
            $row = [ordered]@{ clusterId = $cluster.id }
            foreach ($prop in $perm.PSObject.Properties) {
                $row[$prop.Name] = $prop.Value
            }
            $Writer.WriteRecord([pscustomobject]$row)
        }
    }
}

function Get-PowerBiGatewayClusterDataSources {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $clusterId = $InputId

    # Per-cluster data source list. $expand=users inlines per-data-source
    # role assignments (Owner / User / UserWithSharing) on each datasource
    # record. users[] stays inline as JSON in the bronze — typical clusters
    # have <20 data sources and <50 user assignments combined. If we need
    # to flatten later, add a sibling entity in a follow-up PR.
    $uri = "$PowerBiBaseUri/$V2ApiVer/myorg/gatewayClusters/$clusterId/datasources?`$expand=users"
    $response = Invoke-PowerBiRequest -Uri $uri
    $dataSources = ConvertTo-PowerBiArray -Response $response

    foreach ($ds in $dataSources) {
        # Stamp clusterId for the FK back to powerbi_gateway_clusters. The
        # response object itself doesn't carry it (it's in the URL).
        $row = [ordered]@{ clusterId = $clusterId }
        foreach ($prop in $ds.PSObject.Properties) {
            $row[$prop.Name] = $prop.Value
        }
        $Writer.WriteRecord([pscustomobject]$row)
    }
}

Export-ModuleMember -Function `
    Get-ModuleStages, Get-ModuleEntities, `
    Get-PowerBiGatewayClustersRoot, Get-PowerBiGatewayClusterPermissionsRoot, `
    Get-PowerBiGatewayClusterDataSources
