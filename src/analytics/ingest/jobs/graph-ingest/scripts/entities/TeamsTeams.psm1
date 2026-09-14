# Consolidated ingestion module for the Teams family.
#
# Stage graph (three tiers):
#
#   teams_root (inline, /v1.0/teams)
#     |- team_details        (pool, /v1.0/teams/{id})
#     |- team_installed_apps (pool, /v1.0/teams/{id}/installedApps)
#     |- team_channels       (pool, /v1.0/teams/{id}/channels) -> emits composite IDs
#         |- channel_members (pool, filtered to private/shared)
#         |- channel_tabs    (pool, all channels)
#
# Sibling entities (each single-stage) under the teams_teams/ family folder:
#   teams_teams            (teams_root)
#   teams_team_details     (team_details)
#   teams_installed_apps   (team_installed_apps)
#   teams_channels         (team_channels)
#   teams_channel_members  (channel_members)
#   teams_channel_tabs     (channel_tabs)
#
# team_channels is both a child of teams and a parent of channel-level stages —
# its composite ID emit "teamId:::channelId" with tagged membershipType drives
# the level-3 fan-out. The channel_members InputFilter excludes only `standard`
# channels (standard channels inherit team membership), so private, shared, and
# any future non-standard channel type Graph adds all fan out to /members.

function Get-ModuleStages {
    @{
        'teams_root' = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-TeamsRoot'
            ApiFamily           = 'graph'
            IdKey               = 'id'
            EmitIds             = $true
            MinimumSelectFields = @('id')
        }
        'team_details'        = @{
            InputFrom  = 'teams_root'
            RunsOnPool = $true
            Function   = 'Get-TeamDetails'
            ApiFamily  = 'graph'
        }
        'team_installed_apps' = @{
            InputFrom  = 'teams_root'
            RunsOnPool = $true
            Function   = 'Get-TeamInstalledApps'
            ApiFamily  = 'graph'
        }
        'team_channels'       = @{
            InputFrom           = 'teams_root'
            RunsOnPool          = $true
            Function            = 'Get-TeamChannels'
            ApiFamily           = 'graph'
            IdKey               = 'teamId:::channelId'
            EmitIds             = $true
            IdTags              = @('membershipType')
            MinimumSelectFields = @('id','membershipType')
        }
        'channel_members'     = @{
            InputFrom   = 'team_channels'
            RunsOnPool  = $true
            Function    = 'Get-ChannelMembers'
            ApiFamily   = 'graph'
            InputFilter = { param($tags) $tags -and $tags.membershipType -ne 'standard' }
        }
        'channel_tabs'        = @{
            InputFrom  = 'team_channels'
            RunsOnPool = $true
            Function   = 'Get-ChannelTabs'
            ApiFamily  = 'graph'
        }
    }
}

function Get-ModuleEntities {
    @{
        'teams_teams'           = @{
            Stage        = 'teams_root'
            WritesTo     = 'root'
            SelectFields = @('id','displayName','description')
        }
        # team_details emits the raw /v1.0/teams/{id} response. Graph doesn't
        # accept $select on this endpoint and the schema is wide and evolving
        # (memberSettings, guestSettings, messagingSettings, funSettings,
        # discoverySettings, summary counts, specialization, archive state).
        # Landing it raw preserves the full shape for bronze — no SelectFields.
        'teams_team_details'    = @{
            Stage    = 'team_details'
            WritesTo = 'details'
        }
        'teams_installed_apps'  = @{
            Stage        = 'team_installed_apps'
            WritesTo     = 'installed_apps'
            SelectFields = @('teamId','appId','displayName','teamsAppId','version','publishingState')
        }
        'teams_channels'        = @{
            Stage        = 'team_channels'
            WritesTo     = 'channels'
            SelectFields = @('teamId','id','displayName','description','membershipType','createdDateTime','webUrl','email','isArchived')
        }
        'teams_channel_members' = @{
            Stage        = 'channel_members'
            WritesTo     = 'channel_members'
            SelectFields = @('teamId','channelId','id','displayName','userId','roles')
        }
        'teams_channel_tabs'    = @{
            Stage        = 'channel_tabs'
            WritesTo     = 'channel_tabs'
            SelectFields = @('teamId','channelId','id','displayName','webUrl','configuration','appDisplayName','teamsAppId')
        }
    }
}

function Get-TeamsRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $select = $Context.SelectFields -join ','
    $uri = "/v1.0/teams?`$select=$select&`$top=999"

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($team in $response.value) {
            if ($Context.WriteRecords) { $Writer.WriteRecord($team) }
            $Writer.EmitId($team.id, $null)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-TeamDetails {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $response = Invoke-MgGraphRequest -Method GET -Uri "/v1.0/teams/$InputId" -ErrorAction Stop
    $Writer.WriteRecord($response)
}

function Get-TeamInstalledApps {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $uri = "/v1.0/teams/$InputId/installedApps?`$expand=teamsAppDefinition"
    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($app in $response.value) {
            $Writer.WriteRecord(@{
                teamId          = $InputId
                appId           = $app.id
                displayName     = $app.teamsAppDefinition.displayName
                teamsAppId      = $app.teamsAppDefinition.teamsAppId
                version         = $app.teamsAppDefinition.version
                publishingState = $app.teamsAppDefinition.publishingState
            })
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-TeamChannels {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # teamId is synthesized post-fetch (see below) — Graph would reject it
    # in $select. Filter it out of the URL; the writer projection will
    # still enforce it in the landed record.
    $select = ($Context.SelectFields | Where-Object { $_ -ne 'teamId' }) -join ','
    $uri = "/v1.0/teams/$InputId/channels?`$select=$select"

    # Prefer header surfaces the `shared` enum literal in the landed record;
    # without it Graph downgrades shared channels to `unknownFutureValue` and
    # downstream consumers (silver/gold/PBI) can't tell shared from genuinely
    # unknown without displayName heuristics (#491). The channel_members
    # InputFilter is independent — it excludes only `standard` — so fan-out
    # would still happen even without the header.
    $headers = @{ Prefer = 'include-unknown-enum-members' }

    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -Headers $headers -ErrorAction Stop
        foreach ($channel in $response.value) {
            if ($Context.WriteRecords) {
                $channel['teamId'] = $InputId
                $Writer.WriteRecord($channel)
            }
            $Writer.EmitId("${InputId}:::$($channel.id)", @{ membershipType = $channel.membershipType })
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-ChannelMembers {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $parts = $InputId -split ':::', 2
    $teamId    = $parts[0]
    $channelId = $parts[1]

    # No $select. /members returns polymorphic microsoft.graph.conversationMember
    # subtypes: microsoft.graph.aadUserConversationMember for in-tenant members,
    # microsoft.graph.microsoftEntraUserConversationMember for cross-tenant
    # shared-channel members. OData rejects $select against the base
    # conversationMember for any subtype-only field (userId, email, tenantId) —
    # that's #341, where a bare `email` in the SelectFields list 400'd every
    # call and the framework's NonRetryable path swallowed it silently. Casting
    # (e.g. microsoft.graph.aadUserConversationMember/userId) works for one
    # subtype but mismatches the other. Skipping $select lets the server return
    # all fields and the writer's client-side projection trims to SelectFields —
    # safe across subtypes, no schema drift to track.
    $uri = "/v1.0/teams/$teamId/channels/$channelId/members"
    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($member in $response.value) {
            $member['teamId']    = $teamId
            $member['channelId'] = $channelId
            $Writer.WriteRecord($member)
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

function Get-ChannelTabs {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $parts = $InputId -split ':::', 2
    $teamId    = $parts[0]
    $channelId = $parts[1]

    $uri = "/v1.0/teams/$teamId/channels/$channelId/tabs?`$expand=teamsApp&`$select=id,displayName,webUrl,configuration"
    do {
        $response = Invoke-MgGraphRequest -Method GET -Uri $uri -ErrorAction Stop
        foreach ($tab in $response.value) {
            $Writer.WriteRecord(@{
                teamId         = $teamId
                channelId      = $channelId
                id             = $tab.id
                displayName    = $tab.displayName
                webUrl         = $tab.webUrl
                configuration  = $tab.configuration
                appDisplayName = $tab.teamsApp.displayName
                teamsAppId     = $tab.teamsApp.id
            })
        }
        $uri = $response['@odata.nextLink']
    } while ($uri)
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-TeamsRoot, Get-TeamDetails, Get-TeamInstalledApps, Get-TeamChannels, Get-ChannelMembers, Get-ChannelTabs
