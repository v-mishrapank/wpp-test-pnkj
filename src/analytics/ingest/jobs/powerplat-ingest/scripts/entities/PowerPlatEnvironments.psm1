# Consolidated ingestion module for the Power Platform environments family.
#
# Stage graph:
#
#   environments_root  (inline, BAP /scopes/admin/environments?$expand=...)
#     |- apps                      (pool, /scopes/admin/environments/{env}/apps)
#     |     `- app_role_assignments         (pool-of-pool, /apps/{id}/permissions)
#     |- flows                     (pool, /scopes/admin/environments/{env}/v2/flows)
#     |     |- flow_role_assignments        (pool-of-pool, /flows/{id}/permissions)
#     |     `- flow_metadata                (pool-of-pool, /flows/{id} — admin V1, non-Dataverse flows only)  # PR-7
#     |- connections               (pool, /scopes/admin/environments/{env}/connections)
#     |     `- connection_role_assignments  (pool-of-pool, /connections/{id}/permissions)
#     |- custom_connectors         (pool, /scopes/admin/environments/{env}/apis)
#     `- dataverse_onboardings     (pool, Dataverse Application User onboarding;
#                                   InputFilter excludes Teams-style envs)
#           |- solutions                  (pool, Dataverse /solutions)
#           |- solution_components        (pool, Dataverse /solutioncomponents)
#           |- connection_references      (pool, Dataverse /connectionreferences)
#           |- env_variable_definitions   (pool, Dataverse /environmentvariabledefinitions)
#           |- env_variable_values        (pool, Dataverse /environmentvariablevalues)
#           |- tables                     (pool, Dataverse /EntityDefinitions, $filter=IsCustomEntity eq true)
#           |- workflows                  (pool, Dataverse /workflows)
#           |     `- workflow_definitions          (pool-of-pool, Dataverse /workflows({id})?$select=clientdata,xaml,inputparameters)  # PR-7
#           |- plugin_assemblies          (pool, Dataverse /pluginassemblies)
#           |- plugin_steps               (pool, Dataverse /sdkmessageprocessingsteps)
#           |- web_resources              (pool, Dataverse /webresourceset)
#           |     `- web_resource_contents         (pool-of-pool, Dataverse /webresourceset({id})?$select=content)  # PR-7
#           |- app_modules                (pool, Dataverse /appmodules)
#           |     `- app_module_xml                (pool-of-pool, Dataverse /appmodules({id})?$select=appmodulexmlmanaged,descriptor)  # PR-7
#           |- bots                       (pool, Dataverse /bots)
#           |     `- bot_configurations            (pool-of-pool, Dataverse /bots({id})?$select=configuration)  # PR-7
#           |- bot_components             (pool, Dataverse /botcomponents)
#           |     `- bot_component_data            (pool-of-pool, Dataverse /botcomponents({id})?$select=data)  # PR-7
#           |- ai_models                  (pool, Dataverse /msdyn_aimodels)
#           |- powerpages_websites        (pool, Dataverse /mspp_websites)
#           |- powerpages_components      (pool, Dataverse /powerpagecomponents)
#           |     `- powerpages_component_contents (pool-of-pool, Dataverse /powerpagecomponents({id})?$select=content,filecontent)  # PR-7
#           |- systemusers                (pool, Dataverse /systemusers, $filter=applicationid ne null)  # PR-7
#           |- publishers                 (pool, Dataverse /publishers)  # PR-7
#           `- mailboxes                  (pool, Dataverse /mailboxes)  # PR-7
#
# === Why one module ===
#
# All powerplat entities are per-env — no tenant-wide endpoint exists for any
# of them. The natural top-level dimension is env, not resource type. PR-1/2/3/4
# split powerplat across five sibling modules (Apps/Flows/Connections/Dataverse/
# Environments) before this was clear; PR-5a collapses them into one canonical
# multi-stage family module matching the Teams / Entra precedent (one module
# per family, one root entity with WritesTo='root', children share the family
# folder via WritesTo subdirs).
#
# === Worker payload via $Context.InputTags ===
#
# environments_root emits InputId=envName plus IdTags={instanceUrl,
# instanceApiUrl, environmentType}. The framework propagates IdTags into each
# pool worker's $Context.InputTags (added in PR-5a — see #257). Workers that
# need env URL data (dataverse_onboardings + the Dataverse data children)
# read $Context.InputTags.instanceApiUrl directly. Workers that only need
# envName (apps/flows/connections/custom_connectors) read $InputId verbatim.
#
# dataverse_onboardings re-emits IdTags so the data children inherit the same
# URL data without re-deriving from environments_root.
#
# Pool-of-pool stages (app_role_assignments etc.) keep the 2-part composite
# 'envName:::resourceId' from their pool parent — both parts are URL components,
# matching Teams's 'teamId:channelId' precedent.
#
# === Auth + endpoint surface ===
#
# Two audience families:
#   - Legacy admin (https://service.powerapps.com/.default) — covers BAP +
#     PowerApps admin + Flow admin. Used by environments_root, apps, flows,
#     connections, custom_connectors, and dataverse_onboardings's /addAppUser.
#   - Per-env Dataverse Web API (https://{org}.crm.dynamics.com/.default) —
#     used by dataverse_onboardings for WhoAmI / role binds / role revokes,
#     and by the 19 Dataverse data entities (PR-5b: solutions,
#     solution_components, connection_references, env_variable_definitions,
#     env_variable_values; PR-6: tables, workflows, plugin_assemblies,
#     plugin_steps, web_resources, app_modules, bots, bot_components,
#     ai_models, powerpages_websites, powerpages_components; PR-7:
#     systemusers, publishers, mailboxes), plus the six PR-7 per-row
#     dependency-body children (workflow_definitions, app_module_xml,
#     bot_configurations, bot_component_data, web_resource_contents,
#     powerpages_component_contents). flow_metadata is the lone
#     admin-plane per-row child (non-Dataverse flows only; DV-backed
#     flows have their definitions in workflow.clientdata).
#     Audience derived from the env's instanceUrl (non-`api.` host); REST
#     calls go against instanceApiUrl (`api.` host). Region-agnostic —
#     handles crm.dynamics.com / crm4.dynamics.com (EMEA) etc. without
#     string surgery.
#
# Two in-module helpers wrap the reconnect-closure + Invoke-WithRetry boilerplate:
#   Invoke-AdminApiRequest    — legacy admin audience (constant)
#   Invoke-DataverseRequest   — per-env Dataverse audience (derived from InstanceUrl)
#
# === Application User onboarding (Path A / Path B) ===
#
# 1. WhoAmI pre-check on the env's Dataverse Web API (direct, NOT Invoke-WithRetry —
#    RetryHelper classifies powerplat 403 as Auth and would eat the
#    0x80072560 'user is not a member of the organization' signal that gates
#    Path B).
#      - 200 -> SP already onboarded. Path A: just (re-)bind Service Reader.
#      - 403 + body code '0x80072560' -> Path B: bootstrap.
#      - 404 -> SKIPPABLE; framework filters and the manifest reflects skip.
#      - else -> throw with context.
#
# 2. Path B (bootstrap):
#      - POST {bap}/scopes/admin/environments/{env}/addAppUser with
#        {servicePrincipalAppId: <clientId>}. Always grants System Admin.
#      - Resolve the new systemuser id by querying systemusers?$filter=
#        applicationid eq <clientId> against Dataverse (synchronous post-create,
#        avoids the WhoAmI propagation window).
#      - Resolve Service Reader + System Administrator role ids.
#      - POST systemuserroles_association/$ref to bind Service Reader.
#      - DELETE systemuserroles_association/$ref to revoke System Admin.
#
# /addAppUser is NOT idempotent on roles — re-running re-grants System Admin.
# The WhoAmI pre-check is what gates Path B vs A; without it every cycle would
# re-elevate the SP for the duration of steps 4-5.
#
# Service Reader bind runs in BOTH paths (idempotent — re-bind on existing
# assoc returns 204). System Admin revoke ONLY in Path B.
#
# === Crash-window protection and self-heal (issue #564) ===
#
# Path B uses try/finally so System Administrator is revoked after the
# Service Reader bind, even when the bind fails. Revoke is best-effort: failures
# are logged without hiding the original error. Path A also detects and removes
# stale System Administrator bindings left by an earlier crash.
#
# === Per-fetcher constants ===

$BapBaseUri          = 'https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform'
$BapApiVer           = '2020-10-01'
$DataverseApiVer     = 'v9.2'
$LegacyAdminAudience = 'https://service.powerapps.com/.default'

$PowerAppsHost              = 'https://api.powerapps.com/providers/Microsoft.PowerApps'
$AppsApiVer                 = '2017-05-01'
$AppPermissionsApiVer       = '2017-05-01'
$ConnectionsApiVer          = '2017-05-01'
$ConnectionPermissionsApiVer = '2017-05-01'
$CustomConnectorsApiVer     = '2017-05-01'

$FlowHost                = 'https://api.flow.microsoft.com/providers/Microsoft.ProcessSimple'
$FlowsApiVer             = '2016-11-01'
$FlowPermissionsApiVer   = '2016-11-01'

# === Stage + entity declarations ===

function Get-ModuleStages {
    @{
        'environments_root'           = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-PowerPlatEnvironments'
            ApiFamily  = 'powerplat'
            EmitIds    = $true
            IdKey      = 'name'
            IdTags     = @('instanceUrl','instanceApiUrl','environmentType')
        }
        'apps'                        = @{
            InputFrom  = 'environments_root'
            RunsOnPool = $true
            Function   = 'Get-PowerPlatApps'
            ApiFamily  = 'powerplat'
            EmitIds    = $true
            IdKey      = 'envName:::name'
            # embeddedAppType is null on non-embedded apps — used by
            # app_role_assignments' InputFilter to skip the 409-guaranteed
            # /permissions calls on embedded apps (#415).
            IdTags     = @('embeddedAppType')
        }
        'app_role_assignments'        = @{
            InputFrom   = 'apps'
            RunsOnPool  = $true
            Function    = 'Get-PowerPlatAppRoleAssignments'
            ApiFamily   = 'powerplat'
            # Embedded apps (SharepointFormApp etc.) always 409
            # NoPermissionsForEmbeddedApplications on /apps/{id}/permissions.
            # Pre-filter rather than pay per-item Skippable cost (#407, #415).
            InputFilter = { param($tags) -not $tags.embeddedAppType }
        }
        'flows'                       = @{
            InputFrom  = 'environments_root'
            RunsOnPool = $true
            Function   = 'Get-PowerPlatFlows'
            ApiFamily  = 'powerplat'
            EmitIds    = $true
            IdKey      = 'envName:::name'
            # workflowEntityId is null when the cloud flow lives outside
            # Dataverse — used by flow_metadata's InputFilter to skip
            # DV-backed flows (which are covered by workflow_definitions).
            IdTags     = @('workflowEntityId')
        }
        'flow_role_assignments'       = @{
            InputFrom  = 'flows'
            RunsOnPool = $true
            Function   = 'Get-PowerPlatFlowRoleAssignments'
            ApiFamily  = 'powerplat'
        }
        'connections'                 = @{
            InputFrom  = 'environments_root'
            RunsOnPool = $true
            Function   = 'Get-PowerPlatConnections'
            ApiFamily  = 'powerplat'
            EmitIds    = $true
            IdKey      = 'envName:::apiName:::name'
        }
        'connection_role_assignments' = @{
            InputFrom  = 'connections'
            RunsOnPool = $true
            Function   = 'Get-PowerPlatConnectionRoleAssignments'
            ApiFamily  = 'powerplat'
        }
        'custom_connectors'           = @{
            InputFrom  = 'environments_root'
            RunsOnPool = $true
            Function   = 'Get-PowerPlatCustomConnectors'
            ApiFamily  = 'powerplat'
        }
        'dataverse_onboardings'       = @{
            InputFrom          = 'environments_root'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatDataverseOnboardings'
            ApiFamily          = 'powerplat'
            # Excludes Teams-style envs (instanceApiUrl is null when the env
            # has no linkedEnvironmentMetadata). Filtered envs don't dispatch.
            InputFilter        = { param($tags) $tags -and $tags.instanceApiUrl }
            # Re-emits IdTags so the 5 Dataverse data children (solutions,
            # solution_components, connection_references, env_variable_*)
            # inherit instanceUrl/instanceApiUrl without re-deriving and only
            # dispatch on successfully-onboarded envs.
            EmitIds            = $true
            IdKey              = 'name'
            IdTags             = @('instanceUrl','instanceApiUrl','environmentType')
            # Audit ledger emits 1 row per env — auto-flush is a no-op here.
            # Set anyway for consistency with the family's data children.
            AutoFlushThreshold = 1000
        }
        'solutions'                   = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatSolutions'
            ApiFamily          = 'powerplat'
            # Per-env count: ~hundreds-low-thousands typical, can be much
            # larger on managed-solution-heavy tenants. Stream to disk to
            # avoid buffering the whole response.
            AutoFlushThreshold = 1000
        }
        'solution_components'         = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatSolutionComponents'
            ApiFamily          = 'powerplat'
            # Highest-volume entity in the family — a single env can hold
            # 100k+ rows (each solution × each component), tenants with
            # large managed solutions can exceed 1M. Streaming is critical
            # to stay within ACA memory at scale.
            AutoFlushThreshold = 1000
        }
        'connection_references'       = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatConnectionReferences'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
        'env_variable_definitions'    = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatEnvVariableDefinitions'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
        'env_variable_values'         = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatEnvVariableValues'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
        'tables'                      = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatTables'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
        'workflows'                   = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatWorkflows'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
            EmitIds            = $true
            IdKey              = 'envName:::workflowid'
            IdTags             = @('instanceUrl','instanceApiUrl')
        }
        'plugin_assemblies'           = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatPluginAssemblies'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
        'plugin_steps'                = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatPluginSteps'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
        'web_resources'               = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatWebResources'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
            EmitIds            = $true
            IdKey              = 'envName:::webresourceid'
            IdTags             = @('instanceUrl','instanceApiUrl')
        }
        'app_modules'                 = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatAppModules'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
            EmitIds            = $true
            IdKey              = 'envName:::appmoduleid'
            IdTags             = @('instanceUrl','instanceApiUrl')
        }
        'bots'                        = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatBots'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
            EmitIds            = $true
            IdKey              = 'envName:::botid'
            IdTags             = @('instanceUrl','instanceApiUrl')
        }
        'bot_components'              = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatBotComponents'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
            EmitIds            = $true
            IdKey              = 'envName:::botcomponentid'
            IdTags             = @('instanceUrl','instanceApiUrl')
        }
        'ai_models'                   = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatAiModels'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
        'powerpages_websites'         = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatPowerpagesWebsites'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
        'powerpages_components'       = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatPowerpagesComponents'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
            EmitIds            = $true
            IdKey              = 'envName:::powerpagecomponentid'
            IdTags             = @('instanceUrl','instanceApiUrl')
        }

        # === PR-7: per-row dependency-body fan-out (#262) ===
        # Each fetches the heavy column(s) of one parent row at a time, with
        # tight $select. Exists because the bulk-list calls in the parent
        # stages above exclude these columns to avoid blob-induced hangs at
        # scale (see project_dataverse_select_for_blobs). Silver layer (#245)
        # walks the bodies for cross-asset dependency parsing.
        'flow_metadata'                 = @{
            InputFrom          = 'flows'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatFlowMetadata'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 100
            # Non-Dataverse flows only. DV-backed flows (workflowEntityId set)
            # land via workflow_definitions (Dataverse workflow.clientdata),
            # and the admin V1 per-flow GET returns 403 InsufficientCdsPermissions
            # for them — it defers to row-level CDS perms our SP doesn't hold.
            InputFilter        = { param($tags) $tags -and -not $tags.workflowEntityId }
        }
        'workflow_definitions'          = @{
            InputFrom          = 'workflows'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatWorkflowDefinitions'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 100
        }
        'app_module_xml'                = @{
            InputFrom          = 'app_modules'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatAppModuleXml'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 100
        }
        'bot_configurations'            = @{
            InputFrom          = 'bots'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatBotConfigurations'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 100
        }
        'bot_component_data'            = @{
            InputFrom          = 'bot_components'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatBotComponentData'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 100
        }
        'web_resource_contents'         = @{
            InputFrom          = 'web_resources'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatWebResourceContents'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 100
        }
        'powerpages_component_contents' = @{
            InputFrom          = 'powerpages_components'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatPowerpagesComponentContents'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 100
        }

        # === PR-7: new tier-3 Dataverse bulk entities (#262) ===
        # Standalone tables that fill in cross-tenant migration coordination
        # gaps left by the existing bronze. systemusers filtered to SP-backed
        # rows only — every other systemuser is already covered by entra_users.
        'systemusers'                   = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatSystemUsers'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
        'publishers'                    = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatPublishers'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
        'mailboxes'                     = @{
            InputFrom          = 'dataverse_onboardings'
            RunsOnPool         = $true
            Function           = 'Get-PowerPlatMailboxes'
            ApiFamily          = 'powerplat'
            AutoFlushThreshold = 1000
        }
    }
}

function Get-ModuleEntities {
    # Subdir names strip the family prefix (powerplat_apps -> 'apps') to match
    # Teams's convention (teams_team_details -> 'details'). environments is
    # the inline root — its records land at the family-folder root.
    @{
        'powerplat_environments'                = @{ Stage = 'environments_root';           WritesTo = 'root' }
        'powerplat_apps'                        = @{ Stage = 'apps';                        WritesTo = 'apps' }
        'powerplat_app_role_assignments'        = @{ Stage = 'app_role_assignments';        WritesTo = 'app_role_assignments' }
        'powerplat_flows'                       = @{ Stage = 'flows';                       WritesTo = 'flows' }
        'powerplat_flow_role_assignments'       = @{ Stage = 'flow_role_assignments';       WritesTo = 'flow_role_assignments' }
        'powerplat_connections'                 = @{ Stage = 'connections';                 WritesTo = 'connections' }
        'powerplat_connection_role_assignments' = @{ Stage = 'connection_role_assignments'; WritesTo = 'connection_role_assignments' }
        'powerplat_custom_connectors'           = @{ Stage = 'custom_connectors';           WritesTo = 'custom_connectors' }
        'powerplat_dataverse_onboardings'       = @{ Stage = 'dataverse_onboardings';       WritesTo = 'dataverse_onboardings' }
        'powerplat_solutions'                   = @{ Stage = 'solutions';                   WritesTo = 'solutions' }
        'powerplat_solution_components'         = @{ Stage = 'solution_components';         WritesTo = 'solution_components' }
        'powerplat_connection_references'       = @{ Stage = 'connection_references';       WritesTo = 'connection_references' }
        'powerplat_env_variable_definitions'    = @{ Stage = 'env_variable_definitions';    WritesTo = 'env_variable_definitions' }
        'powerplat_env_variable_values'         = @{ Stage = 'env_variable_values';         WritesTo = 'env_variable_values' }
        'powerplat_tables'                      = @{ Stage = 'tables';                      WritesTo = 'tables' }
        'powerplat_workflows'                   = @{ Stage = 'workflows';                   WritesTo = 'workflows' }
        'powerplat_plugin_assemblies'           = @{ Stage = 'plugin_assemblies';           WritesTo = 'plugin_assemblies' }
        'powerplat_plugin_steps'                = @{ Stage = 'plugin_steps';                WritesTo = 'plugin_steps' }
        'powerplat_web_resources'               = @{ Stage = 'web_resources';               WritesTo = 'web_resources' }
        'powerplat_app_modules'                 = @{ Stage = 'app_modules';                 WritesTo = 'app_modules' }
        'powerplat_bots'                        = @{ Stage = 'bots';                        WritesTo = 'bots' }
        'powerplat_bot_components'              = @{ Stage = 'bot_components';              WritesTo = 'bot_components' }
        'powerplat_ai_models'                   = @{ Stage = 'ai_models';                   WritesTo = 'ai_models' }
        'powerplat_powerpages_websites'         = @{ Stage = 'powerpages_websites';         WritesTo = 'powerpages_websites' }
        'powerplat_powerpages_components'       = @{ Stage = 'powerpages_components';       WritesTo = 'powerpages_components' }
        # PR-7 (#262): per-row dependency-body fan-outs
        'powerplat_flow_metadata'                 = @{ Stage = 'flow_metadata';                 WritesTo = 'flow_metadata' }
        'powerplat_workflow_definitions'          = @{ Stage = 'workflow_definitions';          WritesTo = 'workflow_definitions' }
        'powerplat_app_module_xml'                = @{ Stage = 'app_module_xml';                WritesTo = 'app_module_xml' }
        'powerplat_bot_configurations'            = @{ Stage = 'bot_configurations';            WritesTo = 'bot_configurations' }
        'powerplat_bot_component_data'            = @{ Stage = 'bot_component_data';            WritesTo = 'bot_component_data' }
        'powerplat_web_resource_contents'         = @{ Stage = 'web_resource_contents';         WritesTo = 'web_resource_contents' }
        'powerplat_powerpages_component_contents' = @{ Stage = 'powerpages_component_contents'; WritesTo = 'powerpages_component_contents' }
        # PR-7 (#262): new tier-3 Dataverse bulk entities
        'powerplat_systemusers'                   = @{ Stage = 'systemusers';                   WritesTo = 'systemusers' }
        'powerplat_publishers'                    = @{ Stage = 'publishers';                    WritesTo = 'publishers' }
        'powerplat_mailboxes'                     = @{ Stage = 'mailboxes';                     WritesTo = 'mailboxes' }
    }
}

# === Helpers ===

function Get-DataverseAudience {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$InstanceUrl)
    return ($InstanceUrl.TrimEnd('/')) + '/.default'
}

function Invoke-AdminApiRequest {
    # Call the legacy admin plane (BAP / PowerApps admin / Flow admin). Audience
    # is the constant $LegacyAdminAudience. Each call gets its own $headers
    # hashtable so the reconnect closure's Authorization mutation is naturally
    # scoped — no cross-call stale-header risk.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Uri,
        [string]$Method = 'GET',
        $Body = $null
    )

    $audience = $LegacyAdminAudience
    $token = Get-PowerPlatToken -Audience $audience
    $headers = @{ Authorization = "Bearer $token" }

    $reconnect = {
        Restore-ServiceConnection
        $newToken = Get-PowerPlatToken -Audience $audience
        $headers['Authorization'] = "Bearer $newToken"
    }.GetNewClosure()

    Invoke-WithRetry -ApiFamily 'powerplat' -OnAuthReconnect $reconnect -ScriptBlock {
        if ($Body) {
            # Mutate-and-restore Content-Type on the caller's $headers. Cloning
            # would let reconnect's Authorization refresh miss the cloned copy
            # (PR-4 Copilot fix #1).
            try {
                $headers['Content-Type'] = 'application/json'
                Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers -Body $Body -ErrorAction Stop
            } finally {
                $headers.Remove('Content-Type')
            }
        } else {
            Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers -ErrorAction Stop
        }
    }
}

function Invoke-DataverseRequest {
    # Call the per-env Dataverse Web API. Audience is derived from InstanceUrl
    # (non-`api.` host); the URI itself uses the api. host (caller's
    # responsibility to construct from InstanceApiUrl).
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InstanceUrl,
        [Parameter(Mandatory)][string]$Uri,
        [string]$Method = 'GET',
        $Body = $null
    )

    $audience = Get-DataverseAudience -InstanceUrl $InstanceUrl
    $token = Get-PowerPlatToken -Audience $audience
    $headers = @{ Authorization = "Bearer $token"; Accept = 'application/json' }

    $reconnect = {
        Restore-ServiceConnection
        $newToken = Get-PowerPlatToken -Audience $audience
        $headers['Authorization'] = "Bearer $newToken"
    }.GetNewClosure()

    Invoke-WithRetry -ApiFamily 'powerplat' -OnAuthReconnect $reconnect -ScriptBlock {
        if ($Body) {
            try {
                $headers['Content-Type'] = 'application/json'
                Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers -Body $Body -ErrorAction Stop
            } finally {
                $headers.Remove('Content-Type')
            }
        } else {
            Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers -ErrorAction Stop
        }
    }
}

# === Onboarding helpers (private, used by Get-PowerPlatDataverseOnboardings) ===

function Get-DataverseRoleId {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InstanceUrl,
        [Parameter(Mandatory)][string]$DvWebApi,
        [Parameter(Mandatory)][string]$RoleName
    )

    $uri = "$DvWebApi/roles?`$filter=name eq '$RoleName'&`$select=roleid"
    $response = Invoke-DataverseRequest -InstanceUrl $InstanceUrl -Uri $uri
    if (-not $response.value -or $response.value.Count -eq 0) {
        throw "Dataverse role '$RoleName' not found at $DvWebApi/roles. Predefined roles should always be present on Dataverse-enabled envs."
    }
    return $response.value[0].roleid
}

function Set-DataverseUserRole {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InstanceUrl,
        [Parameter(Mandatory)][string]$DvWebApi,
        [Parameter(Mandatory)][string]$SystemUserId,
        [Parameter(Mandatory)][string]$RoleId
    )

    $bindUri = "$DvWebApi/systemusers($SystemUserId)/systemuserroles_association/`$ref"
    $body = @{ '@odata.id' = "$DvWebApi/roles($RoleId)" } | ConvertTo-Json -Compress
    Invoke-DataverseRequest -InstanceUrl $InstanceUrl -Uri $bindUri -Method POST -Body $body | Out-Null
}

function Remove-DataverseUserRole {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InstanceUrl,
        [Parameter(Mandatory)][string]$DvWebApi,
        [Parameter(Mandatory)][string]$SystemUserId,
        [Parameter(Mandatory)][string]$RoleId
    )

    $unbindUri = "$DvWebApi/systemusers($SystemUserId)/systemuserroles_association/`$ref?`$id=$DvWebApi/roles($RoleId)"
    Invoke-DataverseRequest -InstanceUrl $InstanceUrl -Uri $unbindUri -Method DELETE | Out-Null
}

function Test-DataverseUserHasRole {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InstanceUrl,
        [Parameter(Mandatory)][string]$DvWebApi,
        [Parameter(Mandatory)][string]$SystemUserId,
        [Parameter(Mandatory)][string]$RoleId
    )

    $uri = "$DvWebApi/systemusers($SystemUserId)/systemuserroles_association?`$select=roleid"
    $response = Invoke-DataverseRequest -InstanceUrl $InstanceUrl -Uri $uri
    return [bool]($response.value | Where-Object { $_.roleid -eq $RoleId })
}

function Invoke-EnvBootstrap {
    # POSTs /addAppUser via legacy admin audience, then queries Dataverse for
    # the freshly-created systemuser id. Returns the systemuser GUID.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$EnvName,
        [Parameter(Mandatory)][string]$InstanceUrl,
        [Parameter(Mandatory)][string]$DvWebApi,
        [Parameter(Mandatory)][string]$ClientId
    )

    $body = @{ servicePrincipalAppId = $ClientId } | ConvertTo-Json -Compress
    $bapUri = "$BapBaseUri/scopes/admin/environments/$EnvName/addAppUser?api-version=$BapApiVer"
    Invoke-AdminApiRequest -Uri $bapUri -Method POST -Body $body | Out-Null

    # Synchronous post-create query; avoids the WhoAmI propagation window.
    $sysUserUri = "$DvWebApi/systemusers?`$filter=applicationid eq $ClientId&`$select=systemuserid"
    $response = Invoke-DataverseRequest -InstanceUrl $InstanceUrl -Uri $sysUserUri
    if (-not $response.value -or $response.value.Count -eq 0) {
        throw "Bootstrap on env '$EnvName' succeeded (/addAppUser 200) but systemuser query returned 0 rows for applicationid=$ClientId. Verify Power Platform tenant management app registration in this tenant — see CLAUDE.md §Per-target-tenant onboarding."
    }
    return $response.value[0].systemuserid
}

function Invoke-EnvOnboarding {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$EnvName,
        [Parameter(Mandatory)][string]$InstanceUrl,
        [Parameter(Mandatory)][string]$InstanceApiUrl,
        # Post-#145: ClientId comes through $Context.AuthConfig (not the
        # removed $global:IngestAuthConfig). Caller threads it in explicitly.
        [Parameter(Mandatory)][string]$ClientId
    )

    $dvWebApi = $InstanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $audience = Get-DataverseAudience -InstanceUrl $InstanceUrl

    # WhoAmI pre-check — direct call, manual classification (see header).
    $userId = $null
    $createdNew = $false
    try {
        $token = Get-PowerPlatToken -Audience $audience
        $headers = @{ Authorization = "Bearer $token"; Accept = 'application/json' }
        $whoAmI = Invoke-RestMethod -Method GET -Uri "$dvWebApi/WhoAmI" -Headers $headers -ErrorAction Stop
        $userId = $whoAmI.UserId
    } catch {
        $statusCode = $null
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
            $statusCode = [int]$_.Exception.Response.StatusCode
        }
        $errorBody = $_.ErrorDetails.Message

        if ($statusCode -eq 403 -and $errorBody -match '0x80072560') {
            $createdNew = $true
            $userId = Invoke-EnvBootstrap -EnvName $EnvName -InstanceUrl $InstanceUrl -DvWebApi $dvWebApi -ClientId $clientId
        } elseif ($statusCode -eq 404) {
            throw "SKIPPABLE: Dataverse Web API returned 404 for env '$EnvName' WhoAmI (env may have been deleted or instanceApiUrl is stale)"
        } else {
            throw "WhoAmI failed for env '$EnvName' (status=$statusCode): $($_.Exception.Message). Body: $errorBody"
        }
    }

    if ($createdNew) {
        # Path B: /addAppUser granted System Administrator. The revoke MUST run on
        # every exit path (throw or container kill), else the SP stays SysAdmin (#564).
        try {
            $serviceReaderRoleId = Get-DataverseRoleId -InstanceUrl $InstanceUrl -DvWebApi $dvWebApi -RoleName 'Service Reader'
            Set-DataverseUserRole -InstanceUrl $InstanceUrl -DvWebApi $dvWebApi -SystemUserId $userId -RoleId $serviceReaderRoleId
        } finally {
            # Best-effort — must never mask an in-flight failure. Path A self-heals next run.
            try {
                $sysAdminRoleId = Get-DataverseRoleId -InstanceUrl $InstanceUrl -DvWebApi $dvWebApi -RoleName 'System Administrator'
                Remove-DataverseUserRole -InstanceUrl $InstanceUrl -DvWebApi $dvWebApi -SystemUserId $userId -RoleId $sysAdminRoleId
            } catch {
                Write-Warning "Failed to revoke System Administrator for SP on env '$EnvName' after bootstrap: $($_.Exception.Message). Env left elevated; Path A will retry the revoke on next run."
            }
        }
    } else {
        # Path A: re-bind Service Reader, then retroactively revoke a stale SysAdmin
        # left by a crashed prior Path B run so already-affected envs self-heal (#564).
        $serviceReaderRoleId = Get-DataverseRoleId -InstanceUrl $InstanceUrl -DvWebApi $dvWebApi -RoleName 'Service Reader'
        Set-DataverseUserRole -InstanceUrl $InstanceUrl -DvWebApi $dvWebApi -SystemUserId $userId -RoleId $serviceReaderRoleId

        $sysAdminRoleId = Get-DataverseRoleId -InstanceUrl $InstanceUrl -DvWebApi $dvWebApi -RoleName 'System Administrator'
        if (Test-DataverseUserHasRole -InstanceUrl $InstanceUrl -DvWebApi $dvWebApi -SystemUserId $userId -RoleId $sysAdminRoleId) {
            Write-Warning "STALE_SYSADMIN_REVOKED: env '$EnvName' SP had a stale System Administrator binding (likely a crashed prior bootstrap) — revoking to restore least privilege."
            Remove-DataverseUserRole -InstanceUrl $InstanceUrl -DvWebApi $dvWebApi -SystemUserId $userId -RoleId $sysAdminRoleId
        }
    }

    return @{
        env_id         = $EnvName
        application_id = $clientId
        systemuser_id  = $userId
        role_id        = $serviceReaderRoleId
        role_name      = 'Service Reader'
        onboarded_at   = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
        created_new    = $createdNew
    }
}

# === Fetchers ===

function Get-PowerPlatEnvironments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # $expand: capacity (storage/file usage), addons (premium SKU consumption).
    # Both are migration-relevant signals flattened into the bronze record.
    $uri = "$BapBaseUri/scopes/admin/environments?api-version=$BapApiVer&`$expand=properties.capacity,properties.addons"
    do {
        $response = Invoke-AdminApiRequest -Uri $uri
        foreach ($envRecord in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($envRecord) }

            $envName = $envRecord.name
            $linked = $envRecord.properties.linkedEnvironmentMetadata
            $instanceUrl    = if ($linked) { $linked.instanceUrl }    else { $null }
            $instanceApiUrl = if ($linked) { $linked.instanceApiUrl } else { $null }
            $environmentType = $envRecord.properties.environmentType

            # Tags carry env URL data into pool workers via $Context.InputTags
            # (#257). InputFilter on dataverse_onboardings uses instanceApiUrl
            # to exclude Teams-style envs. apps/flows/connections/custom_connectors
            # only need envName and ignore the tags.
            $Writer.EmitId($envName, @{
                instanceUrl     = $instanceUrl
                instanceApiUrl  = $instanceApiUrl
                environmentType = $environmentType
            })
        }
        $uri = $response.nextLink
    } while ($uri)
}

function Get-PowerPlatApps {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $envName = $InputId
    # 404s on Teams-style envs (no /apps endpoint) classify as Skippable —
    # framework's per-item skip handling drops the env without failing the
    # stage. See RetryHelper.psm1 for classification rules.
    #
    # embeddedAppType is emitted as a tag so app_role_assignments' InputFilter
    # can skip embedded apps (SharepointFormApp etc.), which always 409 on
    # /apps/{id}/permissions. Null on non-embedded apps.
    $uri = "$PowerAppsHost/scopes/admin/environments/$envName/apps?api-version=$AppsApiVer"
    do {
        $response = Invoke-AdminApiRequest -Uri $uri
        foreach ($app in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($app) }
            $Writer.EmitId("$envName:::$($app.name)", @{
                embeddedAppType = $app.properties.embeddedApp.type
            })
        }
        $uri = $response.nextLink
    } while ($uri)
}

function Get-PowerPlatAppRoleAssignments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $parts = $InputId -split ':::', 2
    $envName = $parts[0]
    $appId   = $parts[1]

    $uri = "$PowerAppsHost/scopes/admin/environments/$envName/apps/$appId/permissions?api-version=$AppPermissionsApiVer"
    do {
        $response = Invoke-AdminApiRequest -Uri $uri
        foreach ($perm in $response.value) {
            $perm | Add-Member -NotePropertyName 'envName' -NotePropertyValue $envName -Force
            $perm | Add-Member -NotePropertyName 'appId'   -NotePropertyValue $appId   -Force
            $Writer.WriteRecord($perm)
        }
        $uri = $response.nextLink
    } while ($uri)
}

function Get-PowerPlatFlows {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # V2 endpoint mandatory — V1 bulk-list (/scopes/admin/environments/{env}/flows)
    # is hard-deprecated (CannotListFlowsAsAdminWithDefinition). V2 metadata-only;
    # the executable WDL body lives in `workflow.clientdata` (Dataverse) for
    # DV-backed flows — landed by workflow_definitions. The flow_metadata child
    # picks up the admin V1 per-flow payload for non-Dataverse flows only.
    #
    # workflowEntityId is emitted as a tag so flow_metadata's InputFilter can
    # skip DV-backed flows (where it would 403 InsufficientCdsPermissions).
    # flow_role_assignments ignores the tag.
    $envName = $InputId
    $uri = "$FlowHost/scopes/admin/environments/$envName/v2/flows?api-version=$FlowsApiVer"
    do {
        $response = Invoke-AdminApiRequest -Uri $uri
        foreach ($flow in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($flow) }
            $Writer.EmitId("$envName:::$($flow.name)", @{
                workflowEntityId = $flow.properties.workflowEntityId
            })
        }
        $uri = $response.nextLink
    } while ($uri)
}

function Get-PowerPlatFlowRoleAssignments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Flow /permissions returns explicit shares only (NOT the implicit Owner
    # row — asymmetric with app /permissions). 0-record manifests on tenants
    # without explicit sharing are expected. PR-3 validation confirmed.
    $parts = $InputId -split ':::', 2
    $envName = $parts[0]
    $flowId  = $parts[1]

    $uri = "$FlowHost/scopes/admin/environments/$envName/flows/$flowId/permissions?api-version=$FlowPermissionsApiVer"
    do {
        $response = Invoke-AdminApiRequest -Uri $uri
        foreach ($perm in $response.value) {
            $perm | Add-Member -NotePropertyName 'envName' -NotePropertyValue $envName -Force
            $perm | Add-Member -NotePropertyName 'flowId'  -NotePropertyValue $flowId  -Force
            $Writer.WriteRecord($perm)
        }
        $uri = $response.nextLink
    } while ($uri)
}

function Get-PowerPlatConnections {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Raw bronze: full record incl. connectionParameters. Silver layer
    # extracts gateway IDs / SP site URLs / etc. from the JSON.
    $envName = $InputId
    $uri = "$PowerAppsHost/scopes/admin/environments/$envName/connections?api-version=$ConnectionsApiVer"
    do {
        $response = Invoke-AdminApiRequest -Uri $uri
        foreach ($conn in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($conn) }
            # apiName (connector slug, e.g. shared_commondataservice) is required
            # in the /permissions URL for connection_role_assignments — the
            # admin endpoint routes connections under /apis/{apiName}/, unlike
            # flows/apps which are env-rooted. Carry it through the composite
            # InputId so the child stage can reconstruct the canonical path.
            $apiName = ($conn.properties.apiId -split '/')[-1]
            $Writer.EmitId("$envName:::$apiName:::$($conn.name)", $null)
        }
        $uri = $response.nextLink
    } while ($uri)
}

function Get-PowerPlatConnectionRoleAssignments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Connection /permissions lives under /apis/{apiName}/connections/{id}/,
    # not at the env root — omitting the apiName segment returns an empty-body
    # 404 that the framework (rightly) classifies Skippable (see #401). Unlike
    # flow /permissions, this endpoint returns the implicit Owner row in
    # addition to explicit shares.
    $parts = $InputId -split ':::', 3
    $envName      = $parts[0]
    $apiName      = $parts[1]
    $connectionId = $parts[2]

    $uri = "$PowerAppsHost/scopes/admin/environments/$envName/apis/$apiName/connections/$connectionId/permissions?api-version=$ConnectionPermissionsApiVer"
    do {
        $response = Invoke-AdminApiRequest -Uri $uri
        foreach ($perm in $response.value) {
            $perm | Add-Member -NotePropertyName 'envName'      -NotePropertyValue $envName      -Force
            $perm | Add-Member -NotePropertyName 'apiName'      -NotePropertyValue $apiName      -Force
            $perm | Add-Member -NotePropertyName 'connectionId' -NotePropertyValue $connectionId -Force
            $Writer.WriteRecord($perm)
        }
        $uri = $response.nextLink
    } while ($uri)
}

function Get-PowerPlatCustomConnectors {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # /scopes/admin/apis (tenant-wide) returns 404 for our SP. PR-2 validation
    # confirmed; switched to per-env /scopes/admin/environments/{env}/apis.
    # No role-assignments fan-out — /permissions endpoint isn't documented
    # for custom connectors.
    $envName = $InputId
    $uri = "$PowerAppsHost/scopes/admin/environments/$envName/apis?api-version=$CustomConnectorsApiVer"
    do {
        $response = Invoke-AdminApiRequest -Uri $uri
        foreach ($connector in $response.value) {
            $Writer.WriteRecord($connector)
        }
        $uri = $response.nextLink
    } while ($uri)
}

function Get-PowerPlatDataverseOnboardings {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # InputFilter on the stage already excluded Teams-style envs (null
    # instanceApiUrl). Worker only sees Dataverse-bearing envs.
    $envName        = $InputId
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl

    $row = Invoke-EnvOnboarding -EnvName $envName -InstanceUrl $instanceUrl -InstanceApiUrl $instanceApiUrl -ClientId $Context.AuthConfig.ClientId
    if ($row) {
        # Onboarding (WhoAmI / Path A re-bind / Path B bootstrap) runs
        # unconditionally — its side effects gate child dispatch. Only the
        # audit-row write is gated, matching the apps/flows/connections
        # convention so transitive-parent runs (entity_names asks only for
        # children) don't accumulate audit rows in memory that nothing
        # writes to disk.
        if ($Context.WriteRecords) { $Writer.WriteRecord($row) }
        # Re-emit env name + URL tags so the 16 Dataverse data children
        # (PR-5b's 5: solutions, solution_components, connection_references,
        # env_variable_definitions, env_variable_values; PR-6's 11: tables,
        # workflows, plugin_assemblies, plugin_steps, web_resources,
        # app_modules, bots, bot_components, ai_models, powerpages_websites,
        # powerpages_components) consume only successfully-onboarded envs
        # and inherit URL data.
        $Writer.EmitId($envName, @{
            instanceUrl     = $instanceUrl
            instanceApiUrl  = $instanceApiUrl
            environmentType = $Context.InputTags.environmentType
        })
    }
}

# === Dataverse data fetchers (children of dataverse_onboardings) ===
#
# Each fetches a single Dataverse OData entity-set verbatim. Bronze raw —
# no $select / $filter / $expand. Silver layer (#245) handles projection.
# @odata.nextLink (NOT BAP's nextLink) drives pagination; the dot-property
# accessor needs the quoted form '@odata.nextLink'.

function Get-PowerPlatSolutions {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/solutions"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatSolutionComponents {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/solutioncomponents"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatConnectionReferences {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/connectionreferences"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatEnvVariableDefinitions {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/environmentvariabledefinitions"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatEnvVariableValues {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/environmentvariablevalues"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatTables {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    # Filtered to custom entities only — system-table set inflates ~500-800
    # rows per env at the wire, all immutable Dataverse internals (audit,
    # asyncoperation, workflowlog, etc.) we never query. Silver-layer
    # filtering doesn't apply when the API itself is the cardinality driver.
    # Only PR-6 entity with a non-trivial query shape; others are bare lists.
    $uri = "$dvWebApi/EntityDefinitions?`$filter=IsCustomEntity eq true"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatWorkflows {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Bundles category 0 (classic), 4 (BPF), 5 (cloud), 6 (desktop). Silver
    # layer joins/filters by category and bridges with powerplat_flows
    # (admin-plane) on workflow id.
    #
    # $select excludes xaml / clientdata / inputparameters — three Memo
    # columns with MaxLength 1GB each that hold the full process/flow
    # definition body. clientdata in particular carries the full
    # Power Automate cloud flow JSON (often multi-MB per row) and is the
    # plug-in-assembly-content equivalent for workflows: not metadata,
    # not migration-analysis data, just the executable artifact.
    #
    # PR-7: Emits per-row IDs for the workflow_definitions child stage,
    # which fetches the excluded Memo columns one row at a time. Tags
    # carry the env's Dataverse URLs so the child can call /api/data
    # without re-deriving them.
    $envName        = $InputId
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/workflows?`$select=workflowid,name,description,type,category,mode,subprocess,ismanaged,componentstate,primaryentity,scope,businessprocesstype,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($r) }
            $Writer.EmitId("$envName:::$($r.workflowid)", @{
                instanceUrl    = $instanceUrl
                instanceApiUrl = $instanceApiUrl
            })
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatPluginAssemblies {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Deviation from the bronze-raw convention: explicit $select excludes the
    # `content` and `content2` columns, which carry base64-encoded DLL bytes
    # for the compiled assembly. On tenants with many large or complex
    # plugin assemblies, the default response is multiple MB per record and
    # many GB per env in aggregate — madev1 hangs the whole pool stage when
    # these blobs are returned (verified via diag instrumentation, 2026-04-30).
    # We're inventorying assemblies for migration, not backing them up;
    # silver layer (#245) doesn't need the bytes either.
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/pluginassemblies?`$select=pluginassemblyid,name,version,culture,publickeytoken,description,isolationmode,sourcetype,sourcehash,ismanaged,componentstate,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatPluginSteps {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/sdkmessageprocessingsteps"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatWebResources {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # EntitySetName is 'webresourceset' (irregular plural), NOT 'webresources'.
    #
    # $select excludes `content` (base64 file bytes — JS/HTML/CSS/images,
    # routinely multi-MB per row) and `dependencyxml` (raw dependency tree
    # XML, can be 100KB+). dependencyxml is technically migration-relevant
    # but silver layer (#245) parses dependencies from solution_components
    # cross-references; we don't need the per-resource XML in bronze.
    #
    # PR-7: Emits per-row IDs for web_resource_contents child stage.
    $envName        = $InputId
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/webresourceset?`$select=webresourceid,name,displayname,description,webresourcetype,languagecode,ismanaged,componentstate,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($r) }
            $Writer.EmitId("$envName:::$($r.webresourceid)", @{
                instanceUrl    = $instanceUrl
                instanceApiUrl = $instanceApiUrl
            })
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatAppModules {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Model-driven app metadata. Only path to model-driven apps —
    # admin-plane /apps returns canvas only.
    #
    # $select excludes appmodulexmlmanaged / configxml / descriptor /
    # eventhandlers (1GB Memo each, holding the full app definition XML/JSON
    # — sitemap, components, handlers) plus appgraph and aiappdescription
    # (1MB Memo each). Silver layer (#245) doesn't use these directly.
    #
    # PR-7: Emits per-row IDs for app_module_xml child stage, which fetches
    # the excluded sitemap XML one row at a time.
    $envName        = $InputId
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/appmodules?`$select=appmoduleid,name,uniquename,description,formfactor,isfeatured,ismanaged,componentstate,navigationtype,clienttype,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($r) }
            $Writer.EmitId("$envName:::$($r.appmoduleid)", @{
                instanceUrl    = $instanceUrl
                instanceApiUrl = $instanceApiUrl
            })
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatBots {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Copilot Studio agents + classic PVA.
    #
    # $select excludes iconbase64 (base64 PNG, up to 100KB) and the four
    # 1MB Memo blobs: configuration, applicationmanifestinformation,
    # authenticationconfiguration, synchronizationstatus. These hold the
    # full Teams manifest, auth config, sync state, and bot config —
    # operational artifacts, not migration-analysis metadata.
    #
    # PR-7: Emits per-row IDs for bot_configurations child stage, which
    # fetches the migration-relevant `configuration` blob per row.
    $envName        = $InputId
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/bots?`$select=botid,name,schemaname,language,ismanaged,componentstate,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($r) }
            $Writer.EmitId("$envName:::$($r.botid)", @{
                instanceUrl    = $instanceUrl
                instanceApiUrl = $instanceApiUrl
            })
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatBotComponents {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Topics, skills, knowledge, custom GPT, triggers.
    #
    # $select excludes data / content / dependencies — three 1MB Memo
    # columns. data holds the OBI-format topic/skill/dialog body (the
    # actual logic), content holds component metadata, dependencies
    # holds the inter-component dependency graph. On any real Copilot
    # Studio tenant these are the bulk of the response.
    #
    # PR-7: Emits per-row IDs for bot_component_data child stage, which
    # fetches the `data` blob per row — the highest-value silver target
    # after flow definitions (Copilot Studio agent topics carry full
    # connector-action graphs equivalent to flow actions).
    $envName        = $InputId
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/botcomponents?`$select=botcomponentid,name,schemaname,componenttype,ismanaged,componentstate,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($r) }
            $Writer.EmitId("$envName:::$($r.botcomponentid)", @{
                instanceUrl    = $instanceUrl
                instanceApiUrl = $instanceApiUrl
            })
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatAiModels {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # $select excludes msdyn_modelcreationcontext (1MB Memo). Actual model
    # weights / training data live in related msdyn_aibdatasetscontainer
    # and msdyn_aibfile rows — not on this row.
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/msdyn_aimodels?`$select=msdyn_aimodelid,msdyn_name,ismanaged,componentstate,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatPowerpagesWebsites {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/mspp_websites"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatPowerpagesComponents {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # powerpagecomponent is the unified replacement for the legacy mspp_*
    # component tables (mspp_webtemplate, mspp_webfile, mspp_contentsnippet,
    # mspp_webpage, ...). Each row carries the body of one of those types.
    #
    # $select excludes content / searchcontent / filecontent — three large
    # Memo/File fields. content holds the equivalent of mspp_source /
    # mspp_value / mspp_copy (Liquid templates, snippet bodies, page copy);
    # searchcontent denormalizes the search index; filecontent is the
    # File-typed payload (up to 128MB) for web-file rows.
    #
    # PR-7: Emits per-row IDs for powerpages_component_contents child
    # stage, which fetches `content` + `filecontent` per row.
    $envName        = $InputId
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/powerpagecomponents?`$select=powerpagecomponentid,name,ismanaged,componentstate,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($r) }
            $Writer.EmitId("$envName:::$($r.powerpagecomponentid)", @{
                instanceUrl    = $instanceUrl
                instanceApiUrl = $instanceApiUrl
            })
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

# === PR-7: per-row dependency-body fetchers (#262) ===
#
# Each child stage fetches one parent row's heavy column(s). InputId is the
# composite 'envName:::<entity>id'; URL tags re-propagate from the parent's
# IdTags. AutoFlushThreshold=100 (per-row payloads can be multi-MB; 1000
# would buffer GB-scale before flushing).
#
# Per-row 400 / 404 races (rows deleted between bulk-list and detail call)
# are handled by the PR-6 NonRetryable + ODataError body enrichment in the
# shared retry framework — no per-fetcher special-casing needed.

function Get-PowerPlatFlowMetadata {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Admin-plane child of `flows`. Lands the admin V1 per-flow payload:
    # connectionReferences (full apiDefinition per connector), definitionSummary
    # (server-rendered triggers + actions inventory with type + swaggerOperationId
    # + source api), referencedResources (concrete SharePoint sites/lists/etc.),
    # plus creator, provisioningMethod, templateName, flowOpenAiData, ...
    #
    # NOT the executable WDL document — that's workflow.clientdata, landed by
    # workflow_definitions. This entity covers the non-Dataverse flows that
    # have no workflow row (e.g., personal automations in default envs without
    # a Dataverse instance). The InputFilter on the stage gates on
    # workflowEntityId == null so DV-backed flows aren't called against this
    # endpoint (it returns 403 InsufficientCdsPermissions for them).
    #
    # Endpoint: matches Microsoft.PowerApps.Administration.PowerShell's
    # Get-AdminFlow -FlowName X cmdlet (V1 path, no /v2/, with $top=50).
    $parts    = $InputId -split ':::', 2
    $envName  = $parts[0]
    $flowName = $parts[1]
    $uri = "$FlowHost/scopes/admin/environments/$envName/flows/$flowName" +
           "?api-version=$FlowsApiVer&`$top=50"
    $response = Invoke-AdminApiRequest -Uri $uri
    $response | Add-Member -NotePropertyName 'envName' -NotePropertyValue $envName -Force
    $Writer.WriteRecord($response)
}

function Get-PowerPlatWorkflowDefinitions {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Dataverse child of `workflows`. Pulls the three 1GB Memo columns the
    # parent excludes: clientdata (cloud flow JSON for cat 5), xaml (classic
    # XAML for cat 0/4/6), inputparameters (workflow input bindings).
    $parts          = $InputId -split ':::', 2
    $envName        = $parts[0]
    $workflowId     = $parts[1]
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/workflows($workflowId)?`$select=workflowid,clientdata,xaml,inputparameters"
    $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
    $response | Add-Member -NotePropertyName 'envName' -NotePropertyValue $envName -Force
    $Writer.WriteRecord($response)
}

function Get-PowerPlatAppModuleXml {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Dataverse child of `app_modules`. Pulls appmodulexmlmanaged (sitemap
    # + form/web-resource/business-rule refs) and descriptor (manifest).
    # Other 1GB Memo cols on appmodule (configxml, eventhandlers) deferred
    # — silver doesn't parse them today.
    $parts          = $InputId -split ':::', 2
    $envName        = $parts[0]
    $appModuleId    = $parts[1]
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/appmodules($appModuleId)?`$select=appmoduleid,appmodulexmlmanaged,descriptor"
    $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
    $response | Add-Member -NotePropertyName 'envName' -NotePropertyValue $envName -Force
    $Writer.WriteRecord($response)
}

function Get-PowerPlatBotConfigurations {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Dataverse child of `bots`. Pulls the migration-relevant `configuration`
    # blob (channel bindings, OAuth providers, AAD app refs). Other excluded
    # bot Memos (applicationmanifestinformation, authenticationconfiguration,
    # synchronizationstatus) are operational — silver doesn't need them.
    $parts          = $InputId -split ':::', 2
    $envName        = $parts[0]
    $botId          = $parts[1]
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/bots($botId)?`$select=botid,configuration"
    $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
    $response | Add-Member -NotePropertyName 'envName' -NotePropertyValue $envName -Force
    $Writer.WriteRecord($response)
}

function Get-PowerPlatBotComponentData {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Dataverse child of `bot_components`. Pulls `data` — the OBI-format
    # topic/skill/dialog body. Carries Power Automate-style action graphs
    # for Copilot Studio agents: HTTP actions, knowledge sources, connector
    # invocations. Highest silver value after workflow_definitions.
    $parts           = $InputId -split ':::', 2
    $envName         = $parts[0]
    $botComponentId  = $parts[1]
    $instanceUrl     = $Context.InputTags.instanceUrl
    $instanceApiUrl  = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/botcomponents($botComponentId)?`$select=botcomponentid,data"
    $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
    $response | Add-Member -NotePropertyName 'envName' -NotePropertyValue $envName -Force
    $Writer.WriteRecord($response)
}

function Get-PowerPlatWebResourceContents {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Dataverse child of `web_resources`. Pulls `content` (base64-encoded
    # body of the JS / HTML / CSS / image / etc.). Silver decodes per
    # webresourcetype; binary types (PNG/JPG/GIF/ICO/XAP) are skipped at
    # silver layer — landing all types in bronze keeps the fetcher uniform.
    $parts           = $InputId -split ':::', 2
    $envName         = $parts[0]
    $webResourceId   = $parts[1]
    $instanceUrl     = $Context.InputTags.instanceUrl
    $instanceApiUrl  = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/webresourceset($webResourceId)?`$select=webresourceid,content"
    $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
    $response | Add-Member -NotePropertyName 'envName' -NotePropertyValue $envName -Force
    $Writer.WriteRecord($response)
}

function Get-PowerPlatPowerpagesComponentContents {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Dataverse child of `powerpages_components`. Pulls `content` (Liquid
    # templates / snippet bodies / page copy / OAuth provider config) and
    # `filecontent` (web-file binary payload, up to 128MB per row).
    # `searchcontent` skipped — denormalized search index, redundant.
    $parts                = $InputId -split ':::', 2
    $envName              = $parts[0]
    $powerpageComponentId = $parts[1]
    $instanceUrl          = $Context.InputTags.instanceUrl
    $instanceApiUrl       = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/powerpagecomponents($powerpageComponentId)?`$select=powerpagecomponentid,content,filecontent"
    $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
    $response | Add-Member -NotePropertyName 'envName' -NotePropertyValue $envName -Force
    $Writer.WriteRecord($response)
}

# === PR-7: new tier-3 Dataverse bulk fetchers (#262) ===

function Get-PowerPlatSystemUsers {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # SP-backed application users. $filter=applicationid ne null drops
    # human users (covered by entra_users). These are the systemusers that
    # plugins / flows actually run as on cross-tenant migration: the
    # destination tenant must onboard the same SP for them to authenticate.
    #
    # $select avoids lookup `_value` fields per project_dataverse_select_for_blobs
    # — businessunitid / mobileofflineprofileid / etc. can return 400 silently
    # under some tenants and the framework's retry loop hides the error. Add
    # them in a follow-up after first-pass validation if silver needs them.
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/systemusers?`$filter=applicationid ne null&`$select=systemuserid,applicationid,fullname,domainname,internalemailaddress,isdisabled,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatPublishers {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Solution publishers. customizationprefix is the load-bearing field —
    # it determines the logical-name prefix for every custom entity / column
    # that publisher creates (e.g. cr_invoice). Cross-tenant migration has
    # to predict prefix collisions on import.
    #
    # $select excludes `entityimage` (Image type, ~10MB cap) — not needed
    # for migration and skips lookup _value fields per saved memory.
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/publishers?`$select=publisherid,uniquename,friendlyname,customizationprefix,customizationoptionvalueprefix,description,emailaddress,supportingwebsiteurl,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

function Get-PowerPlatMailboxes {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Server-Side Sync mailbox bindings. Each row binds a Dataverse user /
    # queue to an Exchange mailbox. Migration query: "if I retire mailbox X,
    # which Dataverse principals lose mail?"
    #
    # $select excludes `exchangesyncstatexml` and `folderhierarchy` — both
    # 1GB Memo cols holding sync state / folder tree. Operational, not
    # migration-analysis. Lookup _value fields (regardingobjectid_value,
    # emailserverprofile_value) deferred per saved memory; follow-up PR
    # adds them after first-pass validation.
    $instanceUrl    = $Context.InputTags.instanceUrl
    $instanceApiUrl = $Context.InputTags.instanceApiUrl
    $dvWebApi = $instanceApiUrl.TrimEnd('/') + "/api/data/$DataverseApiVer"
    $uri = "$dvWebApi/mailboxes?`$select=mailboxid,name,emailaddress,statecode,statuscode,enabledforincomingemail,enabledforoutgoingemail,enabledforact,allowemailconnectortousecredentials,createdon,modifiedon"
    do {
        $response = Invoke-DataverseRequest -InstanceUrl $instanceUrl -Uri $uri
        foreach ($r in $response.value) {
            $Writer.WriteRecord($r)
        }
        $uri = $response.'@odata.nextLink'
    } while ($uri)
}

Export-ModuleMember -Function `
    Get-ModuleStages, Get-ModuleEntities, `
    Get-PowerPlatEnvironments, `
    Get-PowerPlatApps, Get-PowerPlatAppRoleAssignments, `
    Get-PowerPlatFlows, Get-PowerPlatFlowRoleAssignments, `
    Get-PowerPlatConnections, Get-PowerPlatConnectionRoleAssignments, `
    Get-PowerPlatCustomConnectors, `
    Get-PowerPlatDataverseOnboardings, `
    Get-PowerPlatSolutions, Get-PowerPlatSolutionComponents, `
    Get-PowerPlatConnectionReferences, `
    Get-PowerPlatEnvVariableDefinitions, Get-PowerPlatEnvVariableValues, `
    Get-PowerPlatTables, Get-PowerPlatWorkflows, `
    Get-PowerPlatPluginAssemblies, Get-PowerPlatPluginSteps, `
    Get-PowerPlatWebResources, Get-PowerPlatAppModules, `
    Get-PowerPlatBots, Get-PowerPlatBotComponents, `
    Get-PowerPlatAiModels, `
    Get-PowerPlatPowerpagesWebsites, Get-PowerPlatPowerpagesComponents, `
    Get-PowerPlatFlowMetadata, Get-PowerPlatWorkflowDefinitions, `
    Get-PowerPlatAppModuleXml, Get-PowerPlatBotConfigurations, `
    Get-PowerPlatBotComponentData, Get-PowerPlatWebResourceContents, `
    Get-PowerPlatPowerpagesComponentContents, `
    Get-PowerPlatSystemUsers, Get-PowerPlatPublishers, `
    Get-PowerPlatMailboxes
