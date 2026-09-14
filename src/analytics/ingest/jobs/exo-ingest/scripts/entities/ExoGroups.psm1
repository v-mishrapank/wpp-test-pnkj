# Consolidated ingestion module for the EXO groups family.
#
# Stage graph (two-stage: enumerate via Get-EXORecipient, enrich per-item):
#
#   dg_root (inline, Get-EXORecipient -RecipientTypeDetails MailUniversalDistributionGroup,MailUniversalSecurityGroup)
#     |- dg_details (pool, Get-DistributionGroup -Identity)
#     |- dg_members (pool, Get-DistributionGroupMember)
#   ug_root (inline, Get-EXORecipient -RecipientTypeDetails GroupMailbox)
#     |- ug_details (pool, Get-UnifiedGroup -Identity -IncludeAllProperties)
#     |- ug_members (pool, Get-UnifiedGroupLinks -LinkType Members)
#
# === Why two stages (issues #368, #492, #494) ===
#
# Get-UnifiedGroup -ResultSize Unlimited silently caps at ~6k records on
# large tenants (#494) and crashes with GetResponseHeader on throttle
# responses (#368), making it unreliable for bulk enumeration. The same
# risk applies to Get-DistributionGroup. Get-EXORecipient is the V3 REST
# cmdlet with proper @odata.nextLink pagination -- proven at scale for
# exo_mail_users and exo_contacts.
#
# The root stages enumerate via Get-EXORecipient (fast, reliable, tier 1).
# The details stages call the type-specific cmdlet per-Identity (no bulk
# pagination bugs on single-record lookups) to capture the full property
# surface that Get-EXORecipient can't return (DG: MemberJoinRestriction,
# RequireSenderAuthenticationEnabled, GroupType, etc. UG: AccessType,
# ResourceProvisioningOptions, SharePointSiteUrl, GroupMemberCount, etc.).
#
# exo_group_members is a single entity fed by both dg_members and ug_members.
# Its records land under each parent's folder (see issue #143).
#
# Parent Identity and ExternalDirectoryObjectID are both needed downstream:
# Identity drives the EXO lookup; ExternalDirectoryObjectID is the AAD
# reference used in emitted member records. The root stages encode both as
# a composite InputId "Identity:::ExternalDirectoryObjectID" that the member
# and detail Fetch functions parse.

function Get-ModuleStages {
    @{
        'dg_root'    = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-ExoDistributionGroupsRoot'
            ApiFamily           = 'exo'
            IdKey               = 'Identity:::ExternalDirectoryObjectID'
            EmitIds             = $true
            MinimumSelectFields = @('id')
        }
        'dg_details' = @{
            InputFrom  = 'dg_root'
            RunsOnPool = $true
            Function   = 'Get-ExoDistributionGroupDetails'
            ApiFamily  = 'exo'
        }
        'dg_members' = @{
            InputFrom  = 'dg_root'
            RunsOnPool = $true
            Function   = 'Get-ExoDistributionGroupMembers'
            ApiFamily  = 'exo'
        }
        'ug_root'    = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-ExoUnifiedGroupsRoot'
            ApiFamily           = 'exo'
            IdKey               = 'Identity:::ExternalDirectoryObjectID'
            EmitIds             = $true
            MinimumSelectFields = @('id')
        }
        'ug_details' = @{
            InputFrom  = 'ug_root'
            RunsOnPool = $true
            Function   = 'Get-ExoUnifiedGroupDetails'
            ApiFamily  = 'exo'
        }
        'ug_members' = @{
            InputFrom  = 'ug_root'
            RunsOnPool = $true
            Function   = 'Get-ExoUnifiedGroupMembers'
            ApiFamily  = 'exo'
        }
    }
}

function Get-ModuleEntities {
    @{
        'exo_distribution_groups' = @{
            Stage    = 'dg_root'
            WritesTo = 'root'
        }
        'exo_distribution_group_details' = @{
            Stage    = 'dg_details'
            WritesTo = 'details'
        }
        'exo_unified_groups'      = @{
            Stage    = 'ug_root'
            WritesTo = 'root'
        }
        'exo_unified_group_details' = @{
            Stage    = 'ug_details'
            WritesTo = 'details'
        }
        # Fed by both dg_members and ug_members -- output lands under each
        # parent's root folder. Two per-run manifests, one per path.
        # The fetch hand-builds a fixed 7-key record, so SelectFields
        # is the enforced landing shape.
        'exo_group_members'       = @{
            Stage        = @('dg_members', 'ug_members')
            WritesTo     = 'members'
            SelectFields = @('groupIdentity','groupObjectId','groupType','memberName','memberObjectId','memberType','primarySmtp')
        }
    }
}

function Get-ExoDistributionGroupsRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $splat = @{
        RecipientTypeDetails = 'MailUniversalDistributionGroup','MailUniversalSecurityGroup'
        ResultSize           = 'Unlimited'
        ErrorAction          = 'Stop'
    }

    if ($Context.WriteRecords) {
        $splat['PropertySets'] = @('Archive','Custom','MailboxMove','Policy')
        $splat['Properties']   = @(
            'DisplayName','Alias','PrimarySmtpAddress','EmailAddresses',
            'ExternalEmailAddress','HiddenFromAddressListsEnabled',
            'WhenCreated','WhenChanged',
            'Identity','DistinguishedName',
            'ManagedBy','Notes'
        )
    } else {
        $splat['Properties'] = @('Identity')
    }

    Get-EXORecipient @splat | ForEach-Object {
        if ($Context.WriteRecords) { $Writer.WriteRecord($_) }
        $Writer.EmitId("$($_.Identity):::$($_.ExternalDirectoryObjectID)", $null)
    }
}

function Get-ExoUnifiedGroupsRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $splat = @{
        RecipientTypeDetails = 'GroupMailbox'
        ResultSize           = 'Unlimited'
        ErrorAction          = 'Stop'
    }

    if ($Context.WriteRecords) {
        $splat['PropertySets'] = @('Archive','Custom','MailboxMove','Policy')
        $splat['Properties']   = @(
            'DisplayName','Alias','PrimarySmtpAddress','EmailAddresses',
            'ExternalEmailAddress','HiddenFromAddressListsEnabled',
            'WhenCreated','WhenChanged',
            'Identity','DistinguishedName',
            'ManagedBy','Notes'
        )
    } else {
        $splat['Properties'] = @('Identity')
    }

    Get-EXORecipient @splat | ForEach-Object {
        if ($Context.WriteRecords) { $Writer.WriteRecord($_) }
        $Writer.EmitId("$($_.Identity):::$($_.ExternalDirectoryObjectID)", $null)
    }
}

function Get-ExoDistributionGroupDetails {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $parts = $InputId -split ':::', 2
    $identity = $parts[0]

    $group = Get-DistributionGroup -Identity $identity -ErrorAction Stop
    if ($null -eq $group) {
        throw [System.Management.Automation.ItemNotFoundException]::new(
            "Distribution group $identity not found (Get-DistributionGroup returned null).")
    }
    $Writer.WriteRecord($group)
}

function Get-ExoUnifiedGroupDetails {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $parts = $InputId -split ':::', 2
    $identity = $parts[0]

    $group = Get-UnifiedGroup -Identity $identity -IncludeAllProperties -ErrorAction Stop
    if ($null -eq $group) {
        throw [System.Management.Automation.ItemNotFoundException]::new(
            "Unified group $identity not found (Get-UnifiedGroup returned null).")
    }
    # Language is a System.Globalization.CultureInfo whose self-recursive
    # .Parent chain blows past ConvertTo-Json's Depth ceiling. Project to
    # the bare IETF tag ("en-US"). See issue #162.
    $langName = if ($group.Language) { $group.Language.Name } else { $null }
    $projected = $group | Select-Object -Property * -ExcludeProperty Language
    $projected | Add-Member -NotePropertyName Language -NotePropertyValue $langName
    $Writer.WriteRecord($projected)
}

function Get-ExoDistributionGroupMembers {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $parts = $InputId -split ':::', 2
    $identity  = $parts[0]
    $objectId  = $parts[1]

    # Snapshot into an array before iterating. A live pipeline iterates the
    # EXO module's shared state, which can mutate mid-foreach when a sibling
    # runspace reconnects EXO, throwing "Collection was modified" and poisoning
    # the chunk. See #343 (and #165 for the SPO parallel).
    $members = @(Get-DistributionGroupMember -Identity $identity -ResultSize Unlimited -ErrorAction Stop |
        Select-Object Name, ExternalDirectoryObjectId, RecipientType, PrimarySmtpAddress)
    foreach ($m in $members) {
        $Writer.WriteRecord(@{
            groupIdentity  = $identity
            groupObjectId  = $objectId
            groupType      = 'DistributionGroup'
            memberName     = $m.Name
            memberObjectId = $m.ExternalDirectoryObjectId
            memberType     = $m.RecipientType
            primarySmtp    = $m.PrimarySmtpAddress
        })
    }
}

function Get-ExoUnifiedGroupMembers {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $parts = $InputId -split ':::', 2
    $identity  = $parts[0]
    $objectId  = $parts[1]

    # Snapshot into an array before iterating -- see Get-ExoDistributionGroupMembers
    # for the rationale (#343).
    # Get-ExoUnifiedGroupMembers — project during the snapshot so retained objects are tiny
    $members = @(Get-UnifiedGroupLinks -Identity $identity -LinkType Members -ResultSize Unlimited -ErrorAction Stop |
        Select-Object Name, ExternalDirectoryObjectId, RecipientType, PrimarySmtpAddress)
    foreach ($m in $members) {
        $Writer.WriteRecord(@{
            groupIdentity  = $identity
            groupObjectId  = $objectId
            groupType      = 'UnifiedGroup'
            memberName     = $m.Name
            memberObjectId = $m.ExternalDirectoryObjectId
            memberType     = $m.RecipientType
            primarySmtp    = $m.PrimarySmtpAddress
        })
    }
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-ExoDistributionGroupsRoot, Get-ExoUnifiedGroupsRoot, Get-ExoDistributionGroupDetails, Get-ExoUnifiedGroupDetails, Get-ExoDistributionGroupMembers, Get-ExoUnifiedGroupMembers
