# Consolidated ingestion module for the EXO mailboxes family.
#
# Stage graph:
#   mailboxes_root (inline, Get-EXOMailbox)
#     |- mailbox_stats (pool, Get-EXOMailboxStatistics per ExchangeGuid)
#     |- mailbox_perms (pool, Get-MailboxPermission + Get-RecipientPermission)
#
# EXO uses PropertySets rather than $select. The root Fetch picks
# -PropertySets All when $Context.WriteRecords (the entity is being
# landed); otherwise StatisticsSeed, which is the cheapest set that
# still carries ExchangeGuid for downstream children. Prior revisions
# keyed off a sentinel SelectFields list, but that conflated field
# selection with request-tracking.

function Get-ModuleStages {
    @{
        'mailboxes_root' = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-ExoMailboxesRoot'
            ApiFamily           = 'exo'
            IdKey               = 'ExchangeGuid'
            EmitIds             = $true
            MinimumSelectFields = @('id')
        }
        'mailbox_stats'  = @{
            InputFrom  = 'mailboxes_root'
            RunsOnPool = $true
            # Wrapper deliberately named Get-ExoMailboxStats — Get-ExoMailboxStatistics
            # would collide case-insensitively with the EXO cmdlet
            # Get-EXOMailboxStatistics, and since PowerShell command lookup
            # prefers Function > Cmdlet, the wrapper would shadow the cmdlet
            # and recurse on every call. See #402.
            Function   = 'Get-ExoMailboxStats'
            ApiFamily  = 'exo'
        }
        'mailbox_perms'  = @{
            InputFrom  = 'mailboxes_root'
            RunsOnPool = $true
            Function   = 'Get-ExoMailboxPermissions'
            ApiFamily  = 'exo'
        }
    }
}

function Get-ModuleEntities {
    @{
        # Root emits PS objects from Get-EXOMailbox (PropertySets picks the
        # schema but leaves per-field selection to the cmdlet). Statistics
        # is likewise a raw Get-EXOMailboxStatistics object. No field
        # filter in either — SelectFields omitted.
        'exo_mailboxes'           = @{
            Stage    = 'mailboxes_root'
            WritesTo = 'root'
        }
        'exo_mailbox_statistics'  = @{
            Stage    = 'mailbox_stats'
            WritesTo = 'statistics'
        }
        # Permissions hand-builds a fixed 13-key record combining
        # Get-MailboxPermission / Get-RecipientPermission / GrantSendOnBehalfTo.
        # The writer projects to this exact set.
        'exo_mailbox_permissions' = @{
            Stage        = 'mailbox_perms'
            WritesTo     = 'permissions'
            SelectFields = @(
                'exchangeGuid','userPrincipalName','displayName','recipientTypeDetails','primarySmtpAddress',
                'mailboxPermissions','sendAsPermissions','sendOnBehalfTo',
                'hasDelegates','hasFullAccess','hasSendAs','hasSendOnBehalf','permissionCount'
            )
        }
    }
}

function Get-ExoMailboxesRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $propertySet = if ($Context.WriteRecords) { 'All' } else { 'StatisticsSeed' }

    # Scope filter (issue #260): same shape as Get-EntraUsersRoot and
    # Get-SpoSitesRoot — keep the single -ResultSize Unlimited enumeration
    # (server-side pagination) and filter records by UPN membership in the
    # pipeline. A per-Identity loop over a 55K-UPN scope is 55K sequential
    # REST round-trips (~3-8 hours) vs ~25-40 min for full enumeration on
    # the same tenant; the unscoped-style enum is the right primitive
    # regardless of scope size. Descendant stats/perms pool stages still
    # benefit because EmitId only fires for in-scope ExchangeGuids.
    $scopeUpns = $null
    if ($env:SCOPE_ROOT) {
        $scopeUpns = Get-ScopeKeySet `
            -ScopeRoot $env:SCOPE_ROOT `
            -Dimension 'users' `
            -KeyField 'userPrincipalName' `
            -StorageAccountUrl $env:STORAGE_ACCOUNT_URL `
            -ContainerName $env:LANDING_CONTAINER
        if ($null -eq $scopeUpns) {
            Write-Log "exo_mailboxes: scope enabled but no scope file found; skipping all records and downstream stages" -Level WARN
            return
        }
        Write-Log "exo_mailboxes: scope enabled with $($scopeUpns.Count) UPN(s); filtering records"
    }

    Get-EXOMailbox -PropertySets $propertySet -ResultSize Unlimited -ErrorAction Stop | ForEach-Object {
        if ($scopeUpns -and -not $scopeUpns.Contains([string]$_.UserPrincipalName)) {
            return
        }
        if ($Context.WriteRecords) {
            # Languages comes back as System.Globalization.CultureInfo[].
            # ConvertTo-Json walks each CultureInfo's self-recursive .Parent
            # chain to its Depth ceiling and emits 30 levels of {"Parent":...}
            # garbage per entry. Project to bare IETF tags ("en-US") before
            # WriteRecord. See issue #162.
            $projected = $_ | Select-Object -Property * -ExcludeProperty Languages
            $projected | Add-Member -NotePropertyName Languages -NotePropertyValue @(
                $_.Languages | ForEach-Object { $_.Name } | Where-Object { $_ }
            )
            $Writer.WriteRecord($projected)
        }
        $Writer.EmitId($_.ExchangeGuid.ToString(), $null)
    }
}

function Get-ExoMailboxStats {
    # NOT Get-ExoMailboxStatistics — that name collides case-insensitively
    # with the EXO cmdlet Get-EXOMailboxStatistics (PowerShell command lookup
    # is case-insensitive and prefers Function > Cmdlet), so the wrapper
    # would shadow the cmdlet and recurse, failing to bind -Identity. #402.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $stats = Get-EXOMailboxStatistics -Identity $InputId -PropertySets All -ErrorAction Stop
    # Get-EXOMailboxStatistics can return nothing (not throw) for a mailbox
    # in a transitional state — recently converted, disconnected, or soft-
    # deleted. Surface as a Skippable classification so the pool dispatcher
    # increments skipped_count without terminating the chunk (#156 bug #9).
    if ($null -eq $stats) {
        throw [System.Management.Automation.ItemNotFoundException]::new(
            "Mailbox $InputId couldn't be found (Get-EXOMailboxStatistics returned null).")
    }
    $Writer.WriteRecord($stats)
}

function Get-ExoMailboxPermissions {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Per-runspace cache — evaluated once per runspace on first invocation.
    # $script: is module-scoped, which in a worker runspace is one instance per runspace.
    if ($null -eq $script:HasSendAsCmdlet) {
        $script:HasSendAsCmdlet = $null -ne (Get-Command 'Get-RecipientPermission' -ErrorAction SilentlyContinue)
    }

    $mbx = Get-EXOMailbox -Identity $InputId -PropertySets Minimum `
        -Properties RecipientTypeDetails, GrantSendOnBehalfTo -ErrorAction Stop

    $mbxPerms = Get-MailboxPermission -Identity $InputId -ErrorAction Stop |
        Where-Object { $_.User -ne 'NT AUTHORITY\SELF' -and $_.User -ne 'SELF' }

    $sendAsPerms = @()
    if ($script:HasSendAsCmdlet) {
        try {
            $sendAsPerms = @(Get-RecipientPermission -Identity $InputId -ErrorAction Stop |
                Where-Object { $_.Trustee -ne 'NT AUTHORITY\SELF' -and $_.Trustee -ne 'SELF' })
        }
        catch {
            Write-Warning "SendAs lookup failed for mailbox $InputId : $($_.Exception.Message)"
        }
    }

    # List+Add, not array += : a shared mailbox can carry hundreds of
    # delegates, and += rebuilds the whole array per permission (O(n^2)).
    $mailboxPermissions = [System.Collections.Generic.List[object]]::new()
    foreach ($p in $mbxPerms) {
        $mailboxPermissions.Add(@{
            trustee      = $p.User
            accessRights = @($p.AccessRights)
            isInherited  = [bool]$p.IsInherited
            deny         = [bool]$p.Deny
        })
    }

    $sendAsPermissions = [System.Collections.Generic.List[object]]::new()
    foreach ($p in $sendAsPerms) {
        $sendAsPermissions.Add(@{
            trustee      = $p.Trustee
            accessRights = @($p.AccessRights)
            isInherited  = [bool]$p.IsInherited
        })
    }

    $sendOnBehalfTo = @()
    if ($mbx.GrantSendOnBehalfTo) {
        $sendOnBehalfTo = @($mbx.GrantSendOnBehalfTo)
    }

    $record = @{
        exchangeGuid         = $InputId
        userPrincipalName    = $mbx.UserPrincipalName
        displayName          = $mbx.DisplayName
        recipientTypeDetails = $mbx.RecipientTypeDetails
        primarySmtpAddress   = $mbx.PrimarySmtpAddress
        mailboxPermissions   = $mailboxPermissions
        sendAsPermissions    = $sendAsPermissions
        sendOnBehalfTo       = $sendOnBehalfTo
        hasDelegates         = ($mailboxPermissions.Count -gt 0 -or $sendAsPermissions.Count -gt 0 -or $sendOnBehalfTo.Count -gt 0)
        hasFullAccess        = ($mailboxPermissions | Where-Object { $_.accessRights -contains 'FullAccess' }).Count -gt 0
        hasSendAs            = $sendAsPermissions.Count -gt 0
        hasSendOnBehalf      = $sendOnBehalfTo.Count -gt 0
        permissionCount      = $mailboxPermissions.Count + $sendAsPermissions.Count + $sendOnBehalfTo.Count
    }
    $Writer.WriteRecord($record)
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-ExoMailboxesRoot, Get-ExoMailboxStats, Get-ExoMailboxPermissions
