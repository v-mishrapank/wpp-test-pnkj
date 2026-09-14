# Consolidated ingestion module for the Entra applications family.
#
# Stage graph:
#   apps_root (inline, /v1.0/applications — emits is_app_proxy IdTag per app)
#     |- app_owners       (pool, /v1.0/applications/{id}/owners — all apps)
#     `- app_proxy_config (pool, /beta/applications/{id}?$select=onPremisesPublishing&$expand=connectorGroup
#                                — InputFilter to is_app_proxy=true only)

function Get-ModuleStages {
    @{
        'apps_root'        = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-EntraApplicationsRoot'
            ApiFamily           = 'graph'
            IdKey               = 'id'
            EmitIds             = $true
            # `tags` is already in the entra_applications SelectFields; the
            # fetcher derives is_app_proxy from it so app_proxy_config can
            # InputFilter the per-app fan-out to proxy-published apps only.
            # Eliminates the 100% 404 storm from #406 in tenants without
            # app proxy without losing per-item retry / Skippable semantics.
            IdTags              = @('is_app_proxy')
            MinimumSelectFields = @('id','tags')
        }
        'app_owners'       = @{
            InputFrom  = 'apps_root'
            RunsOnPool = $true
            Function   = 'Get-EntraAppOwners'
            ApiFamily  = 'graph'
        }
        'app_proxy_config' = @{
            InputFrom   = 'apps_root'
            RunsOnPool  = $true
            Function    = 'Get-EntraAppProxyConfig'
            ApiFamily   = 'graph'
            # Only fan out to apps that apps_root tagged as proxy-published.
            # Filtered apps don't dispatch — pool worker never sees them, so
            # no 404 and no per-item failure for the designed-absence case.
            InputFilter = { param($tags) $tags -and $tags.is_app_proxy }
        }
    }
}

function Get-ModuleEntities {
    @{
        'entra_applications'     = @{
            Stage        = 'apps_root'
            WritesTo     = 'root'
            SelectFields = @(
                'id','appId','displayName','signInAudience','identifierUris','appRoles',
                'requiredResourceAccess','keyCredentials','passwordCredentials','web',
                'spa','publicClient','api','optionalClaims','groupMembershipClaims',
                'tags','applicationTemplateId','createdDateTime','publisherDomain',
                'verifiedPublisher','info','notes','servicePrincipalLockConfiguration',
                'description','isFallbackPublicClient','tokenEncryptionKeyId',
                'certification','samlMetadataUrl','disabledByMicrosoftStatus'
            )
        }
        'entra_app_owners'       = @{
            Stage        = 'app_owners'
            WritesTo     = 'owners'
            SelectFields = @('applicationId','id','displayName','userPrincipalName','mail')
        }
        'entra_app_proxy_config' = @{
            Stage        = 'app_proxy_config'
            WritesTo     = 'proxy_config'
            # connectorGroup is a navigation property brought in via $expand
            # (not $select-able); listing it here makes the writer's projection
            # preserve the expanded sub-object on landed records.
            SelectFields = @('applicationId','id','displayName','onPremisesPublishing','connectorGroup')
        }
    }
}

function Get-EntraApplicationsRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $select = $Context.SelectFields -join ','
    $uri = "/v1.0/applications?`$select=$select&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($app in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($app) }
            # Emit is_app_proxy as an IdTag so app_proxy_config's InputFilter
            # can skip non-proxy apps without dispatching them to the pool.
            # `tags` is null on apps with no tags set; -contains on $null is
            # safe (returns false).
            $Writer.EmitId($app.id, @{
                is_app_proxy = ($app.tags -contains 'WindowsAzureActiveDirectoryOnPremApp')
            })
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-EntraAppOwners {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $select = ($Context.SelectFields | Where-Object { $_ -ne 'applicationId' }) -join ','
    $uri = "/v1.0/applications/$InputId/owners?`$select=$select&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($owner in $response.value) {
            $owner['applicationId'] = $InputId
            $Writer.WriteRecord($owner)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-EntraAppProxyConfig {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Per-app hydrate. apps_root tagged each app with is_app_proxy and
    # this stage's InputFilter dropped the non-proxy ones, so every
    # dispatched InputId is guaranteed to have onPremisesPublishing set.
    # No 404 storm — that's the #414/#406 fix.
    #
    # connectorGroup is a navigation property (not $select-able); $expand
    # pulls it inline so silver can join multi-forest routing context
    # without a second call.
    #
    # ${InputId} (not $InputId) — PowerShell allows `?` in unbraced
    # variable names, so "$InputId?" parses as the variable named
    # "InputId?" (empty), eating the URI's `?` separator. Braces
    # disambiguate.
    #
    # GSA Private Access apps use different SP-side tags
    # (PrivateAccessNonWebApplication, NetworkAccessManagedApplication)
    # and are NOT caught here — tracked separately.
    $select = ($Context.SelectFields | Where-Object { $_ -notin @('applicationId','connectorGroup') }) -join ','
    $response = Invoke-MgGraphRequest -Method GET -Uri "/beta/applications/${InputId}?`$select=$select&`$expand=connectorGroup" -ErrorAction Stop
    $response['applicationId'] = $InputId
    $Writer.WriteRecord($response)
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-EntraApplicationsRoot, Get-EntraAppOwners, Get-EntraAppProxyConfig
