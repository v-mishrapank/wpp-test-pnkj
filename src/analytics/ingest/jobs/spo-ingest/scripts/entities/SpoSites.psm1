# Consolidated ingestion module for the SharePoint sites family.
#
# Nine entities, nine stages. Site enumeration uses Graph; everything else is
# SP REST (no PnP). The naming reflects SharePoint's natural two-level scope:
#
#   spo_sites                (Graph /v1.0/sites/getAllSites — URL list + 9 fields)
#   spo_site_details         (SP admin REST GetSitePropertiesByUrl — 134 fields)
#   spo_site_users           (/_api/web/SiteUsers from each root web)
#   spo_site_groups          (/_api/web/SiteGroups?$expand=Users)
#   spo_webs                 (recursive /_api/web/Webs walk per site collection)
#   spo_web_role_definitions (/_api/web/RoleDefinitions per web)
#   spo_web_role_assignments (two REST calls per web, zipped on PrincipalId)
#   spo_web_lists            (/_api/web/Lists per web)
#   spo_web_item_permissions (per-item GetSharingInformation via /_api/$batch)
#
# === Why site enumeration is on Graph and detail is on SP REST (#471) ===
#
# SP admin REST's GetSitePropertiesFromSharePointByFilters has a hard ~1,497-row
# ceiling on large tenants — the StartIndex continuation token is opaque and SP
# refuses to issue one past the first page on tenants with 60K+ sites. Eleven
# filter/parameter variations probed; all return exactly 1,497 then empty.
# PnP's Get-PnPTenantSite works because it paginates via CSOM (server-held
# state); REST paging on the same data is capped. Graph /sites/getAllSites
# paginates cleanly via @odata.nextLink at any scale but only carries 9 fields
# (no Template, no LockState, no governance flags). So Graph enumerates, then
# spo_site_details fans out per-URL to GetSitePropertiesByUrl for the full
# 134-field SiteProperties shape — admin REST is fine when addressed per-URL.
#
# Stage DAG:
#
#   spo_sites_root     (inline, Graph /sites/getAllSites — paginates @odata.nextLink)
#     └─ site_details  (pool, per webUrl → GetSitePropertiesByUrl)
#          │           (filters Template -like 'REDIRECTSITE*' from descendant
#          │            fan-out; row still landed in spo_site_details)
#          ├─ site_users      (pool, per site collection)
#          ├─ site_groups     (pool, per site collection)
#          └─ webs_root       (pool, per site collection → emits N web URLs as IDs)
#               ├─ web_role_defs        (pool, per web)
#               ├─ web_role_assignments (pool, per web — 2 REST calls zipped)
#               └─ web_lists            (pool, per web → emits list IDs)
#                    └─ web_item_perms  (pool, InputFilter BaseType=1)
#
# === Why one module file for the whole family ===
# The shared framework loads each entities/*.psm1 independently and runs each
# as its own stage DAG (Invoke-Ingestion.ps1 line 229; Invoke-ModuleRun loads
# one module at a time). Issue #464's eight-file layout would have required
# either a framework change to merge per-module Get-ModuleStages across files,
# or each module re-running the admin tenant enumeration (which is the
# expensive throttle-prone bit the rewrite is meant to fix). Mirrors how
# graph-ingest's TeamsTeams.psm1 keeps six related stages in one file.
#
# === Field naming ===
# PascalCase verbatim from the REST response. We do not rename, snake-case, or
# camelCase any source field. The single transform we apply is stripping
# `__metadata` and unwrapping nested `{ __metadata; results: [...] }` envelopes
# (see Resolve-SpoVerboseRecord) — those are odata=verbose wire artifacts, not
# data. Foreign-key columns (SiteUrl, WebUrl, ListId, ItemId) are added as
# context for the relational model — explicitly endorsed by #464.

# Per-stage REST audience routing happens in Connect.psm1's Get-SpoToken via
# host parsing. The fetchers below just call Get-SpoToken -SiteUrl <url>;
# the cache picks the right audience.

# === Get-ModuleStages / Get-ModuleEntities ===

function Get-ModuleStages {
    @{
        'spo_sites_root'        = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-SpoSitesRoot'
            # Graph enumeration. Retry/throttle classification routes through
            # the 'graph' family in RetryHelper — 429/Retry-After is parsed
            # there, not the spo family.
            ApiFamily  = 'graph'
            IdKey      = 'webUrl'
            EmitIds    = $true
            # Inline stages auto-flush at 1000 records by default (StageExecutor).
            # Tenants with 100k+ sites would otherwise hold the whole inventory
            # in memory before the first chunk ships — Graph pages at 200 so
            # steady state is fine, but a big tenant's chunk dump is what we
            # cap.
        }
        'site_details'          = @{
            # Gating stage between spo_sites_root and the per-site descendants.
            # Calls GetSitePropertiesByUrl per webUrl, lands the 134-field
            # SiteProperties shape, then emits InputId for the descendant
            # fan-out — but ONLY for non-REDIRECTSITE templates. The redirect
            # filter lives here (not at spo_sites_root) because Graph's
            # getAllSites doesn't carry the Template field; GetSitePropertiesByUrl
            # is the first place we see it. See #471.
            #
            # No IdKey: Get-SpoSiteDetails owns EmitId explicitly and suppresses
            # it for REDIRECTSITE shells. If we set IdKey='webUrl', the framework's
            # auto-emit safety net (#297) would treat the suppressed-emit case as
            # a "fetcher forgot EmitId" bug and fail the redirect items. With no
            # IdKey, EmittedIds is driven only by explicit EmitId calls — exactly
            # the contract we want for a conditionally-emitting stage. Composite
            # IdKey stages use the same no-auto-extract path; this is the same
            # shape with a more deliberate semantic.
            InputFrom      = 'spo_sites_root'
            RunsOnPool     = $true
            Function       = 'Get-SpoSiteDetails'
            ApiFamily      = 'spo'
            EmitIds        = $true
        }
        'site_users'            = @{
            # Rechained through site_details (#471) so REDIRECTSITE shells
            # never reach per-site descendants — on a tenant-renamed customer
            # tenant, redirect-shell descendants would each 403-Skippable and
            # multiply the manifest noise by 3x.
            InputFrom      = 'site_details'
            RunsOnPool     = $true
            Function       = 'Get-SpoSiteUsers'
            ApiFamily      = 'spo'
        }
        'site_groups'           = @{
            InputFrom      = 'site_details'
            RunsOnPool     = $true
            Function       = 'Get-SpoSiteGroups'
            ApiFamily      = 'spo'
            # Nested permissionsInformation arrays in expanded Users push the
            # natural depth past the default of 5. Bump to land them intact.
            JsonDepth      = 8
        }
        'webs_root'             = @{
            InputFrom  = 'site_details'
            RunsOnPool = $true
            Function   = 'Get-SpoWebsRoot'
            ApiFamily  = 'spo'
            IdKey      = 'Url'
            EmitIds    = $true
            IdTags     = @('SiteUrl')
        }
        'web_role_defs'         = @{
            InputFrom      = 'webs_root'
            RunsOnPool     = $true
            Function       = 'Get-SpoWebRoleDefinitions'
            ApiFamily      = 'spo'
        }
        'web_role_assignments'  = @{
            InputFrom      = 'webs_root'
            RunsOnPool     = $true
            Function       = 'Get-SpoWebRoleAssignments'
            ApiFamily      = 'spo'
            JsonDepth      = 8
        }
        'web_lists'             = @{
            InputFrom  = 'webs_root'
            RunsOnPool = $true
            Function   = 'Get-SpoWebLists'
            ApiFamily  = 'spo'
            IdKey      = 'Id'
            EmitIds    = $true
            IdTags     = @('SiteUrl','WebUrl','BaseTemplate','BaseType')
        }
        'web_item_perms'        = @{
            InputFrom      = 'web_lists'
            RunsOnPool     = $true
            Function       = 'Get-SpoWebItemPermissions'
            ApiFamily      = 'spo'
            JsonDepth      = 10
            # Filter on the parent's emitted BaseType — `1` = DocumentLibrary,
            # which captures every doc-library variant regardless of the
            # underlying BaseTemplate. Filtering by BaseTemplate=101 alone
            # drops OneDrive personal `Documents` libs (BaseTemplate=700) AND
            # picture libraries (109) AND site-page libraries (119) — and
            # OneDrive in particular accounts for nearly all of the sharing-
            # link signal in a real tenant. Validated against a prior madev1
            # run where 100% of landed sharing links were on OneDrive.
            InputFilter    = { param($tags) $tags -and ([int]$tags.BaseType) -eq 1 }
            # Stream output rows to chunk files during the fetch so the
            # StageWriter buffer stays bounded. A large OneDrive library
            # with thousands of items where every one has unique perms
            # would otherwise buffer all of GetSharingInformation's nested
            # permissionsInformation graphs in memory before the list's
            # fetch returns. The input-side $pendingBatch already caps
            # in-memory items at SpoBatchSize (50); 1000 matches the
            # power-platform precedent for high-volume per-list output.
            AutoFlushThreshold = 1000
        }
    }
}

function Get-ModuleEntities {
    # No SelectFields anywhere — the rewrite lands every source REST field
    # verbatim. The writer passes records through unchanged (raw mode). FK
    # columns are added by the fetchers themselves, not enforced by the
    # writer's projection. This is the explicit design from #464: bronze
    # mirrors the REST shape, silver/gold pick the columns downstream.
    #
    # Landing layout — every entity in this module is owned by the spo_sites
    # root stage (the admin tenant enumeration), so the framework places
    # them under `spo_sites/{tenant}/{date}/` (root_entity = the entity that
    # declares WritesTo='root' for the root stage). Non-root entities land
    # in per-entity subdirs named after the entity itself, replacing the
    # OLD multi-entity subdir names (`details`, `admins`, `groups`, ...) —
    # see #464 rollout step 3 for the cleanup that drops the old subdirs.
    @{
        'spo_sites'                 = @{ Stage = 'spo_sites_root';       WritesTo = 'root'                     }
        'spo_site_details'          = @{ Stage = 'site_details';         WritesTo = 'spo_site_details'         }
        'spo_site_users'            = @{ Stage = 'site_users';           WritesTo = 'spo_site_users'           }
        'spo_site_groups'           = @{ Stage = 'site_groups';          WritesTo = 'spo_site_groups'          }
        'spo_webs'                  = @{ Stage = 'webs_root';            WritesTo = 'spo_webs'                 }
        'spo_web_role_definitions'  = @{ Stage = 'web_role_defs';        WritesTo = 'spo_web_role_definitions' }
        'spo_web_role_assignments'  = @{ Stage = 'web_role_assignments'; WritesTo = 'spo_web_role_assignments' }
        'spo_web_lists'             = @{ Stage = 'web_lists';            WritesTo = 'spo_web_lists'            }
        'spo_web_item_permissions'  = @{ Stage = 'web_item_perms';       WritesTo = 'spo_web_item_permissions' }
    }
}

# ============================================================================
# Verbose-envelope record cleanup
# ============================================================================
# Invoke-SpoRest unwraps the TOP-LEVEL .d / .d.results envelope. Records
# themselves still carry per-row `__metadata` and nested verbose-array
# wrappers (`{ __metadata; results: [...] }`) on every $expand'd child
# collection. This helper normalizes the leftover envelope artifacts so the
# landed JSON is the lean shape the silver/gold layer expects.
#
# Concretely, four transforms happen here:
#  1. `__metadata` keys are stripped from every dict (carries the OData type
#     discriminator we don't need in bronze).
#  2. Verbose-array wrappers `{ results: [...] }` (with optional `__metadata`)
#     are flattened to their inner array.
#  3. Deferred navigation-property stubs `{ __deferred: { uri: ... } }` are
#     dropped entirely (replaced with $null) instead of leaving an empty `{}`
#     for every unexpanded child collection — without this, every /_api/web
#     row carries ~25 noisy nullable-struct columns into silver.
#  4. Naked ISO datetime strings (no timezone suffix) get `Z` appended. SP
#     REST returns datetime fields inconsistently — some with Z, some naked.
#     For SPO, server-side dates are UTC; normalizing here means downstream
#     parsers don't drift on session timezone.

function Resolve-SpoVerboseRecord {
    param($Value)
    if ($null -eq $Value) { return $null }

    # SP returns `$expand`'d child collections as `{"results": [...]}` with no
    # sibling `__metadata` on the wrapper — `__metadata` lives on the inner row
    # objects. Detect a verbose-array wrapper as: `results` is the only
    # meaningful key (with `__metadata` allowed but not required) AND `results`
    # is an enumerable. Anything else with a `results` field is a real record.
    $isVerboseWrapper = $false
    $resultsValue = $null
    if ($Value -is [System.Collections.IDictionary]) {
        if ($Value.Contains('results')) {
            $hasOtherKey = $false
            foreach ($k in $Value.Keys) {
                if ($k -ne 'results' -and $k -ne '__metadata') { $hasOtherKey = $true; break }
            }
            if (-not $hasOtherKey) {
                $candidate = $Value['results']
                if ($candidate -is [System.Collections.IList]) {
                    $isVerboseWrapper = $true
                    $resultsValue = $candidate
                }
            }
        }
    } elseif ($Value -is [PSCustomObject]) {
        $resultsProp = $Value.PSObject.Properties['results']
        if ($resultsProp) {
            $hasOtherKey = $false
            foreach ($p in $Value.PSObject.Properties) {
                if ($p.Name -ne 'results' -and $p.Name -ne '__metadata') { $hasOtherKey = $true; break }
            }
            if (-not $hasOtherKey -and $resultsProp.Value -is [System.Collections.IList]) {
                $isVerboseWrapper = $true
                $resultsValue = $resultsProp.Value
            }
        }
    }
    if ($isVerboseWrapper) {
        # List accumulation (O(n)) instead of array += (O(n^2)); AddRange when a
        # child resolves to a list so nested wrappers stay flattened like +=.
        $arr = [System.Collections.Generic.List[object]]::new()
        foreach ($r in $resultsValue) {
            $resolved = Resolve-SpoVerboseRecord $r
            if ($resolved -is [System.Collections.IList]) { $arr.AddRange($resolved) }
            else { $arr.Add($resolved) }
        }
        return ,$arr.ToArray()
    }

    if ($Value -is [System.Collections.IDictionary]) {
        # Deferred navigation-property stub. SP's verbose response embeds every
        # unexpanded child collection as `{"__deferred":{"uri":"..."}}`. Stripping
        # the `__deferred` key while keeping the parent dict leaves an empty
        # `{}` per stub — hundreds of those per record on /_api/web. Returning
        # $null here drops the stub entirely so the silver schema doesn't carry
        # a noisy nullable-struct column for every unexpanded navigation property.
        if ($Value.Contains('__deferred')) { return $null }
        $clean = [ordered]@{}
        foreach ($k in $Value.Keys) {
            if ($k -eq '__metadata') { continue }
            $clean[$k] = Resolve-SpoVerboseRecord $Value[$k]
        }
        return $clean
    }

    if ($Value -is [PSCustomObject]) {
        if ($Value.PSObject.Properties['__deferred']) { return $null }
        $clean = [ordered]@{}
        foreach ($p in $Value.PSObject.Properties) {
            if ($p.Name -eq '__metadata') { continue }
            $clean[$p.Name] = Resolve-SpoVerboseRecord $p.Value
        }
        return $clean
    }

    if ($Value -is [DateTime]) {
        # SP REST returns date-typed values; `Invoke-RestMethod` parses JSON
        # date strings into [DateTime] with `Kind=Unspecified` (the offset is
        # discarded). For SharePoint Online every server-side date is UTC, so
        # treat Unspecified as Utc (no TZ conversion — the wall-clock value
        # is already UTC). The default round-trip serializer writes naked
        # ISO; force the `Z` form so downstream parsers (Spark, DLT, ADF,
        # Power BI) don't drift on session timezone.
        $dt = if ($Value.Kind -eq [System.DateTimeKind]::Unspecified) {
            [DateTime]::SpecifyKind($Value, [System.DateTimeKind]::Utc)
        } else {
            $Value.ToUniversalTime()
        }
        return $dt.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", [System.Globalization.CultureInfo]::InvariantCulture)
    }
    if ($Value -is [DateTimeOffset]) {
        return $Value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", [System.Globalization.CultureInfo]::InvariantCulture)
    }
    if ($Value -is [string]) {
        # Defensive: also normalize naked-ISO strings if they ever flow through
        # without being typed as DateTime (some SP responses or future code paths
        # might pass raw strings). Strict-anchor regex so URLs / UPNs / etc are
        # not touched.
        if ($Value -match '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$') {
            return "${Value}Z"
        }
        return $Value
    }
    if ($Value -is [System.Collections.IList]) {
        # List accumulation (O(n)) instead of array += (O(n^2)); AddRange when a
        # child resolves to a list so nested arrays stay flattened like +=.
        $arr = [System.Collections.Generic.List[object]]::new()
        foreach ($r in $Value) {
            $resolved = Resolve-SpoVerboseRecord $r
            if ($resolved -is [System.Collections.IList]) { $arr.AddRange($resolved) }
            else { $arr.Add($resolved) }
        }
        return ,$arr.ToArray()
    }
    return $Value
}

function Add-FkColumns {
    <#
    Take a cleaned record (Resolve-SpoVerboseRecord output) and append
    FK columns. FKs land at the end of the column list so the verbatim SP
    fields stay together at the top — easier to diff against a probe capture.
    #>
    param(
        [Parameter(Mandatory)]$Record,
        [Parameter(Mandatory)][hashtable]$Fks
    )
    $rec = [ordered]@{}
    if ($Record -is [System.Collections.IDictionary]) {
        foreach ($k in $Record.Keys) { $rec[$k] = $Record[$k] }
    } elseif ($Record -is [PSCustomObject]) {
        foreach ($p in $Record.PSObject.Properties) { $rec[$p.Name] = $p.Value }
    }
    foreach ($k in $Fks.Keys) { $rec[$k] = $Fks[$k] }
    return $rec
}

function Get-PerRunspaceReconnect {
    <#
    Builds a closure that wipes the runspace's SPO token cache so the next
    Get-SpoToken call re-mints. Passed as -OnAuthReconnect to Invoke-SpoRest
    so a 401 mid-fetch routes through Restore-ServiceConnection rather than
    Invoke-WithRetry's default (which would just sleep + retry the stale
    bearer indefinitely until MaxRetries).
    #>
    return {
        Restore-ServiceConnection
    }.GetNewClosure()
}

# ============================================================================
# Section 1: spo_sites  (Graph /v1.0/sites/getAllSites)
# ============================================================================
# Endpoint: GET https://graph.microsoft.com/v1.0/sites/getAllSites?$top=200
# Pagination: response carries `@odata.nextLink` — follow until absent.
#
# Why Graph instead of SP admin REST: the admin endpoint we used previously
# (GetSitePropertiesFromSharePointByFilters) caps at ~1,497 rows on tenants
# with 60K+ sites — the StartIndex token is opaque and SP refuses to issue a
# continuation past the first page beyond that point. Eleven filter shapes
# probed; all returned the same 1,497-and-empty contract. See #471.
#
# Trade-off: Graph carries only 9 fields per site (id, name, displayName,
# webUrl, description, createdDateTime, lastModifiedDateTime, root,
# siteCollection) plus isPersonalSite. The 134-field SiteProperties shape
# (Template, LockState, governance flags, sharing settings, storage, owner,
# etc.) moves to a per-URL fan-out stage `spo_site_details` that calls SP
# admin REST's GetSitePropertiesByUrl — that endpoint is fine when addressed
# per-URL, only the bulk enumeration was broken.
#
# RedirectSite handling: Graph's site resource has no Template field, so we
# can't filter redirect shells (REDIRECTSITE#0 templates created during site
# renames / tenant renames / cross-geo moves) at this stage. Every site is
# emitted as an InputId for the site_details stage; site_details reads
# Template back from GetSitePropertiesByUrl and prunes redirects from the
# descendant fan-out there.

function Get-SpoSitesRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    # Scope filter (issue #260). Two independent scope dimensions:
    #   users — set of UPNs whose personal OneDrive (isPersonalSite=true) sites
    #           should be included. URLs are derived via the standard M365
    #           pattern <admin_url stripped of -admin / replaced with -my>
    #           /personal/<upn_munged>. Not authoritative (the OwnerEmail at
    #           spo_site_details would be, but that's the per-URL fan-out we're
    #           trying to scope). Vanity domains, GCC-High, and post-rename
    #           hosts may miss-match — accepted Phase 0 trade-off.
    #   sites — explicit set of non-personal site webUrls to include
    #           (team/group/hub sites). Required because team sites aren't
    #           user-keyed; the only way to scope them for a 500K+-site tenant
    #           without re-listing the cascade is by explicit URL allowlist.
    #
    # Missing-file semantics are independent:
    #   users  scope file missing → ALL personal sites skipped
    #   sites  scope file missing → ALL non-personal sites skipped
    #   both   missing            → root effectively a no-op (no enrichment runs)
    #
    # SCOPE_ROOT unset means scope is off entirely → full enumeration as today.
    $usersScope = $null
    $sitesScope = $null
    $personalUrlSet = $null
    if ($env:SCOPE_ROOT) {
        $usersScope = Get-ScopeKeySet `
            -ScopeRoot $env:SCOPE_ROOT `
            -Dimension 'users' `
            -KeyField 'userPrincipalName' `
            -StorageAccountUrl $env:STORAGE_ACCOUNT_URL `
            -ContainerName $env:LANDING_CONTAINER
        $sitesScope = Get-ScopeKeySet `
            -ScopeRoot $env:SCOPE_ROOT `
            -Dimension 'sites' `
            -KeyField 'webUrl' `
            -StorageAccountUrl $env:STORAGE_ACCOUNT_URL `
            -ContainerName $env:LANDING_CONTAINER

        if ($null -eq $usersScope -and $null -eq $sitesScope) {
            Write-Log "spo_sites: scope enabled but neither users nor sites scope file found; skipping all records and downstream stages" -Level WARN
            return
        }

        if ($null -ne $usersScope) {
            # Use the -my host already derived by Connect.psm1 (Hosts.My on
            # the AuthConfig hashtable, e.g. 'xtlab1-my.sharepoint.com').
            # Connect-Service computed it once at session start with the
            # same regex / strip-admin logic — duplicating it here would
            # drift the day Connect.psm1's host parsing gets fixed for a
            # new tenant shape (vanity domain, GCC-High, etc.).
            $myHost = $Context.AuthConfig.Hosts.My
            if ([string]::IsNullOrEmpty($myHost)) {
                throw "spo_sites: scope enabled but AuthConfig.Hosts.My is not populated — Connect-Service should have set it from AdminUrl"
            }
            $myBase = "https://$myHost"

            $personalUrlSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
            foreach ($upn in $usersScope) {
                $munged = ($upn -replace '@', '_') -replace '\.', '_'
                [void]$personalUrlSet.Add("$myBase/personal/$munged")
            }
            Write-Log "spo_sites: users scope present ($($usersScope.Count) UPN(s) → $($personalUrlSet.Count) personal-site URL(s) under $myBase)"
        } else {
            Write-Log "spo_sites: users scope absent; all personal sites will be skipped" -Level WARN
        }

        if ($null -ne $sitesScope) {
            Write-Log "spo_sites: sites scope present ($($sitesScope.Count) team-site URL(s))"
        } else {
            Write-Log "spo_sites: sites scope absent; all non-personal sites will be skipped" -Level WARN
        }
    }

    $reconnect = Get-PerRunspaceReconnect
    $getGraphToken = { Get-SpoToken -SiteUrl 'https://graph.microsoft.com' }.GetNewClosure()

    $next = 'https://graph.microsoft.com/v1.0/sites/getAllSites?$top=200'
    $page = 0
    while ($next) {
        $page++
        $resp = Invoke-WithRetry -ApiFamily 'graph' -OnAuthReconnect $reconnect -ScriptBlock {
            $bearer = & $getGraphToken
            Invoke-RestMethod `
                -Uri $next -Method GET `
                -Headers @{ Authorization = "Bearer $bearer"; Accept = 'application/json'; 'User-Agent' = (Get-SpoUserAgent) } `
                -TimeoutSec 120 -ErrorAction Stop
        }
        if (-not $resp -or -not $resp.value) {
            Write-Log "spo_sites_root Graph page $page returned no value array — assuming exhausted" -Level WARN
            break
        }

        foreach ($site in $resp.value) {
            $webUrl = if ($site.webUrl) { ([string]$site.webUrl).TrimEnd('/') } else { $null }
            if ([string]::IsNullOrEmpty($webUrl)) {
                Write-Log "spo_sites_root: skipping Graph row with null/empty webUrl" -Level WARN
                continue
            }
            # Scope gate: when scope is active, personal sites are filtered by
            # personalUrlSet (derived from users dimension); non-personal sites
            # are filtered by sitesScope (explicit webUrl set). Either set being
            # $null means "skip this category entirely". Tested via the four
            # cases (both, users-only, sites-only, neither) in SpoSitesRoot tests.
            if ($env:SCOPE_ROOT) {
                $isPersonal = [bool]($site.PSObject.Properties['isPersonalSite'] -and $site.isPersonalSite)
                if ($isPersonal) {
                    if ($null -eq $personalUrlSet -or -not $personalUrlSet.Contains($webUrl)) { continue }
                } else {
                    if ($null -eq $sitesScope -or -not $sitesScope.Contains($webUrl)) { continue }
                }
            }

            # Copy Graph fields verbatim into an ordered dictionary so the
            # cleaned record has predictable column ordering (id, name, ...)
            # and a normalized webUrl that matches the emitted InputId.
            $rec = [ordered]@{}
            foreach ($prop in $site.PSObject.Properties) {
                $rec[$prop.Name] = $prop.Value
            }
            $rec['webUrl'] = $webUrl

            if ($Context.WriteRecords) {
                $Writer.WriteRecord($rec)
            }
            $Writer.EmitId($webUrl, $null)
        }

        $next = if ($resp.PSObject.Properties['@odata.nextLink']) { [string]$resp.'@odata.nextLink' } else { $null }
        if ([string]::IsNullOrWhiteSpace($next)) { break }
    }
}

# ============================================================================
# Section 1b: spo_site_details  (admin REST GetSitePropertiesByUrl per-URL)
# ============================================================================
# Endpoint: POST {adminUrl}/_api/Microsoft.Online.SharePoint.TenantAdministration.
#             Tenant/GetSitePropertiesByUrl
# Body:     { "url": "<siteUrl>", "includeDetail": true }
# Returns:  the same 134-field SiteProperties shape we landed in spo_sites
#           pre-#471 (Url, Template, Owner, OwnerEmail, LockState, GroupId,
#           SharingCapability, ConditionalAccessPolicy, all governance /
#           sharing / capacity / archive / template / classification fields).
#
# Two responsibilities:
#  1. Land the per-site detail row in spo_site_details with FK SiteUrl.
#  2. Filter REDIRECTSITE shells (created by tenant rename / site address
#     change / cross-geo move) from the descendant fan-out. Their root web
#     returns LockState=ReadOnly and per-site descendants would each 403/
#     Skippable, generating O(N redirects × N descendants) manifest noise.
#     The Template field is the authoritative signal — and this is the
#     first stage that sees it (Graph's getAllSites doesn't carry Template).
#
# Match is case-insensitive prefix: MS docs reference 'REDIRECTSITE#0' but
# we've seen 'RedirectSite#1' in probe captures. `-like 'REDIRECTSITE*'`
# survives a template-version bump.

function Get-SpoSiteDetails {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    $siteUrl  = $InputId.TrimEnd('/')
    $adminUrl = $Context.AuthConfig.AdminUrl
    $reconnect = Get-PerRunspaceReconnect
    $getToken  = { Get-SpoToken -SiteUrl $adminUrl }.GetNewClosure()

    $endpoint = "$adminUrl/_api/Microsoft.Online.SharePoint.TenantAdministration.Tenant/GetSitePropertiesByUrl"
    $body = @{ url = $siteUrl; includeDetail = $true } | ConvertTo-Json -Compress

    # Invoke-SpoRest unwraps the verbose .d envelope to the single inner
    # entity (single-entity response — GetSitePropertiesByUrl returns one
    # SiteProperties object, not a results[] array).
    $raw = Invoke-SpoRest -Url $endpoint -Method POST -Body $body `
        -GetToken $getToken -OnAuthReconnect $reconnect

    if ($null -eq $raw) {
        Write-Log "spo_site_details: GetSitePropertiesByUrl returned null for $siteUrl" -Level WARN
        return
    }

    $cleaned = Resolve-SpoVerboseRecord $raw
    if ($Context.WriteRecords) {
        $rec = Add-FkColumns -Record $cleaned -Fks @{ SiteUrl = $siteUrl }
        $Writer.WriteRecord($rec)
    } else {
        $rec = $cleaned
    }

    # Redirect-shell filter — see section header. Land the detail row above
    # (so the redirect is accounted for in spo_site_details), then return
    # without emitting InputId for descendants.
    $template = if ($rec -is [System.Collections.IDictionary] -and $rec.Contains('Template')) {
        [string]$rec['Template']
    } else { $null }
    if ($template -and $template -like 'REDIRECTSITE*') {
        return
    }

    $Writer.EmitId($siteUrl, $null)
}

# ============================================================================
# Section 2: spo_site_users  (/_api/web/SiteUsers)
# ============================================================================
# One row per (site collection, user) — the SiteUsers endpoint is site-
# collection-scoped regardless of which web you call it from, so we call it
# from the root web. IsSiteAdmin is a column on the row (no separate admins
# table — consumers filter WHERE IsSiteAdmin=true).

function Get-SpoSiteUsers {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    $siteUrl = $InputId.TrimEnd('/')
    $url = "$siteUrl/_api/web/SiteUsers"
    $reconnect = Get-PerRunspaceReconnect
    $getToken  = { Get-SpoToken -SiteUrl $siteUrl }.GetNewClosure()

    [void](Invoke-SpoRestPaged -Url $url -GetToken $getToken -OnAuthReconnect $reconnect -OnRow {
        param($row)
        $cleaned = Resolve-SpoVerboseRecord $row
        $rec = Add-FkColumns -Record $cleaned -Fks @{ SiteUrl = $siteUrl }
        $Writer.WriteRecord($rec)
    })
}

# ============================================================================
# Section 3: spo_site_groups  (/_api/web/SiteGroups?$expand=Users)
# ============================================================================
# One row per (site collection, SP group), with Users[] nested as an array of
# user objects. Replaces both the old spo_site_groups and spo_site_group_members
# tables — silver/gold queries lateral-flatten Users when they need per-member
# rows.
# Probe-confirmed shape (CogSite1): Users comes back as a verbose-array shape
# { __metadata; results: [...] } which Resolve-SpoVerboseRecord flattens
# to a plain array of user objects (each with its own __metadata stripped).

function Get-SpoSiteGroups {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    $siteUrl = $InputId.TrimEnd('/')
    $url = "$siteUrl/_api/web/SiteGroups?`$expand=Users"
    $reconnect = Get-PerRunspaceReconnect
    $getToken  = { Get-SpoToken -SiteUrl $siteUrl }.GetNewClosure()

    [void](Invoke-SpoRestPaged -Url $url -GetToken $getToken -OnAuthReconnect $reconnect -OnRow {
        param($row)
        $cleaned = Resolve-SpoVerboseRecord $row
        $rec = Add-FkColumns -Record $cleaned -Fks @{ SiteUrl = $siteUrl }
        $Writer.WriteRecord($rec)
    })
}

# ============================================================================
# Section 4: spo_webs  (root + recursive subweb walk)
# ============================================================================
# For each site collection, emit one row per web — the root web plus every
# subweb walked recursively via /_api/web/Webs. Subwebs are emitted with FK
# columns SiteUrl (the parent site collection) and ParentWebUrl (the immediate
# parent web), plus IsRootWeb (bool).
# No depth cap — throttle is the natural backpressure. If a pathological
# tenant trips that, the cap is the place to add a knob.

function Get-SpoWebsRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    $siteUrl = $InputId.TrimEnd('/')
    $reconnect = Get-PerRunspaceReconnect
    $getToken  = { Get-SpoToken -SiteUrl $siteUrl }.GetNewClosure()

    # Root web first. Write-WebRow's return is the cleaned record — handy
    # for future callers but unused here, so discard via [void] to silence
    # the unused-variable analyzer.
    $rootWeb = Invoke-SpoRest -Url "$siteUrl/_api/web" -GetToken $getToken -OnAuthReconnect $reconnect
    [void](Write-WebRow -Writer $Writer -SiteUrl $siteUrl -ParentWebUrl $null -IsRootWeb $true -RawWeb $rootWeb -Context $Context)

    # Recursive walk. Queue of webs to expand; each pop fetches its /Webs
    # (paged — a web with more subwebs than fit on one SP REST page would
    # otherwise have descendants silently dropped) and emits each child as a
    # row, queuing children for further descent.
    $queue = [System.Collections.Generic.Queue[hashtable]]::new()
    $queue.Enqueue(@{ Url = $siteUrl; })
    while ($queue.Count -gt 0) {
        $current = $queue.Dequeue()
        $parentUrl = $current.Url
        $childrenUrl = "$parentUrl/_api/web/Webs"
        [void](Invoke-SpoRestPaged -Url $childrenUrl -GetToken $getToken -OnAuthReconnect $reconnect -OnRow {
            param($child)
            if ($null -eq $child) { return }
            $childUrl = if ($child -is [System.Collections.IDictionary]) { $child['Url'] } else { $child.Url }
            if ([string]::IsNullOrEmpty($childUrl)) { return }
            $childUrl = $childUrl.TrimEnd('/')
            [void](Write-WebRow -Writer $Writer -SiteUrl $siteUrl -ParentWebUrl $parentUrl -IsRootWeb $false -RawWeb $child -Context $Context)
            $queue.Enqueue(@{ Url = $childUrl })
        })
    }
}

function Write-WebRow {
    param(
        $Writer,
        [string]$SiteUrl,
        [string]$ParentWebUrl,
        [bool]$IsRootWeb,
        $RawWeb,
        [hashtable]$Context
    )
    if ($null -eq $RawWeb -or [string]::IsNullOrEmpty($RawWeb.Url)) { return $null }
    $webUrl = $RawWeb.Url.TrimEnd('/')
    $cleaned = Resolve-SpoVerboseRecord $RawWeb
    if ($cleaned -is [System.Collections.IDictionary]) {
        $cleaned['Url'] = $webUrl
    }
    $rec = Add-FkColumns -Record $cleaned -Fks @{
        SiteUrl      = $SiteUrl
        ParentWebUrl = $ParentWebUrl
        IsRootWeb    = $IsRootWeb
    }
    if ($Context.WriteRecords) {
        $Writer.WriteRecord($rec)
    }
    # Emit web URL as the next-stage InputId, with SiteUrl as a tag so the
    # per-web descendants can derive both the audience host and the FK column
    # without re-parsing the URL.
    $Writer.EmitId($webUrl, @{ SiteUrl = $SiteUrl })
    return $rec
}

# ============================================================================
# Section 5: spo_web_role_definitions  (/_api/web/RoleDefinitions)
# ============================================================================
# One row per (web, role-definition). The custom-vs-built-in signal lives in
# BasePermissions.High/.Low (bitmask) and RoleTypeKind — impossible to express
# in the old PnP model.

function Get-SpoWebRoleDefinitions {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    $webUrl = $InputId.TrimEnd('/')
    $siteUrl = if ($Context.InputTags -and $Context.InputTags.SiteUrl) { $Context.InputTags.SiteUrl } else { $webUrl }
    $reconnect = Get-PerRunspaceReconnect
    $getToken  = { Get-SpoToken -SiteUrl $siteUrl }.GetNewClosure()

    $url = "$webUrl/_api/web/RoleDefinitions"
    [void](Invoke-SpoRestPaged -Url $url -GetToken $getToken -OnAuthReconnect $reconnect -OnRow {
        param($row)
        $cleaned = Resolve-SpoVerboseRecord $row
        $rec = Add-FkColumns -Record $cleaned -Fks @{ SiteUrl = $siteUrl; WebUrl = $webUrl }
        $Writer.WriteRecord($rec)
    })
}

# ============================================================================
# Section 6: spo_web_role_assignments  (two REST calls, zipped on PrincipalId)
# ============================================================================
# /_api/web/RoleAssignments?$expand=Member,RoleDefinitionBindings is rejected
# with 406 on most tenants (probe-confirmed against madev2 and CogSite1).
# Workaround: two separate calls, one with $expand=Member, one with
# $expand=RoleDefinitionBindings, zip on PrincipalId. The PrincipalId is the
# RoleAssignment's primary key.

function Get-SpoWebRoleAssignments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    $webUrl = $InputId.TrimEnd('/')
    $siteUrl = if ($Context.InputTags -and $Context.InputTags.SiteUrl) { $Context.InputTags.SiteUrl } else { $webUrl }
    $reconnect = Get-PerRunspaceReconnect
    $getToken  = { Get-SpoToken -SiteUrl $siteUrl }.GetNewClosure()

    $membersByPid = @{}
    $bindingsByPid = @{}

    [void](Invoke-SpoRestPaged -Url "$webUrl/_api/web/RoleAssignments?`$expand=Member" -GetToken $getToken -OnAuthReconnect $reconnect -OnRow {
        param($row)
        $cleaned = Resolve-SpoVerboseRecord $row
        # cleaned.PrincipalId — int. Land the cleaned row keyed by it; we'll
        # merge in bindings from the second call before writing.
        if ($cleaned -is [System.Collections.IDictionary] -and $cleaned.Contains('PrincipalId')) {
            $membersByPid[[string]$cleaned['PrincipalId']] = $cleaned
        }
    })

    [void](Invoke-SpoRestPaged -Url "$webUrl/_api/web/RoleAssignments?`$expand=RoleDefinitionBindings" -GetToken $getToken -OnAuthReconnect $reconnect -OnRow {
        param($row)
        $cleaned = Resolve-SpoVerboseRecord $row
        if ($cleaned -is [System.Collections.IDictionary] -and $cleaned.Contains('PrincipalId')) {
            $principalIdStr = [string]$cleaned['PrincipalId']
            # Avoid an `if`-expression assignment here: PowerShell unwraps a
            # 1-element-array result of an if-expression to its single element,
            # which would turn the [RoleDefinition] one-row case into a bare
            # role-def object and break the nested-array contract. Direct
            # hashtable assignment in each branch preserves the array shape.
            if ($cleaned.Contains('RoleDefinitionBindings')) {
                $bindingsByPid[$principalIdStr] = $cleaned['RoleDefinitionBindings']
            } else {
                $bindingsByPid[$principalIdStr] = @()
            }
        }
    })

    foreach ($principalIdStr in $membersByPid.Keys) {
        $rec = $membersByPid[$principalIdStr]
        # Attach the bindings array from the second call. If the second call
        # didn't see this PrincipalId, attach an empty array — happens rarely
        # but conceivable if a role assignment was deleted between calls.
        # Same array-unwrap avoidance: direct hashtable read assigned without
        # an intervening if-expression.
        if ($bindingsByPid.ContainsKey($principalIdStr)) {
            $rec['RoleDefinitionBindings'] = $bindingsByPid[$principalIdStr]
        } else {
            $rec['RoleDefinitionBindings'] = @()
        }
        $rec = Add-FkColumns -Record $rec -Fks @{ SiteUrl = $siteUrl; WebUrl = $webUrl }
        $Writer.WriteRecord($rec)
    }
}

# ============================================================================
# Section 7: spo_web_lists  (/_api/web/Lists)
# ============================================================================
# One row per list. Emits (siteUrl, webUrl, listId, BaseTemplate, BaseType)
# IdTags so the downstream web_item_perms stage can InputFilter on BaseType=1
# (every document-library variant, including OneDrive personal `Documents`
# libs that live at BaseTemplate=700).

function Get-SpoWebLists {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    $webUrl = $InputId.TrimEnd('/')
    $siteUrl = if ($Context.InputTags -and $Context.InputTags.SiteUrl) { $Context.InputTags.SiteUrl } else { $webUrl }
    $reconnect = Get-PerRunspaceReconnect
    $getToken  = { Get-SpoToken -SiteUrl $siteUrl }.GetNewClosure()

    [void](Invoke-SpoRestPaged -Url "$webUrl/_api/web/Lists" -GetToken $getToken -OnAuthReconnect $reconnect -OnRow {
        param($row)
        $cleaned = Resolve-SpoVerboseRecord $row
        $rec = Add-FkColumns -Record $cleaned -Fks @{ SiteUrl = $siteUrl; WebUrl = $webUrl }
        if ($Context.WriteRecords) { $Writer.WriteRecord($rec) }
        # Emit (web, list) composite ID for the per-item perms descendant.
        # The list GUID alone is NOT unique across sites: OneDrive personal
        # `Documents` libraries (and several other site-template lists like
        # "Style Library", "Site Pages", "Master Page Gallery") use
        # deterministic GUIDs that repeat across every site instance. The
        # framework (StageExecutor.psm1) keys input-tag lookup by ID, so
        # duplicate IDs collapse — the dispatch for list X would get whatever
        # SiteUrl/WebUrl was last emitted for that ID, then 400 against every
        # OTHER site that also owns a list-with-that-ID.
        #
        # The composite `<webUrl>:::<listId>` makes each emission unique. The
        # consumer reads the actual listId from the ListId tag (so it doesn't
        # have to know about the composite encoding).
        $listId = if ($rec.Contains('Id')) { [string]$rec['Id'] } else { $null }
        $baseTemplate = if ($rec.Contains('BaseTemplate')) { [int]$rec['BaseTemplate'] } else { -1 }
        $baseType     = if ($rec.Contains('BaseType'))     { [int]$rec['BaseType'] }     else { -1 }
        if ($listId) {
            $compositeId = "${webUrl}:::${listId}"
            $Writer.EmitId($compositeId, @{
                SiteUrl      = $siteUrl
                WebUrl       = $webUrl
                ListId       = $listId
                BaseTemplate = $baseTemplate
                BaseType     = $baseType
            })
        }
    })
}

# ============================================================================
# Section 8: spo_web_item_permissions  (per-item GetSharingInformation via $batch)
# ============================================================================
# Per (web, doc-library): enumerate /Items with $select including
# HasUniqueRoleAssignments and filter client-side to the rows where the flag
# is true (server-side $filter on this property 500s — see comment at the
# itemsUrl assignment). For surviving items, POST GetSharingInformation in
# $batch chunks of 50. Items where the response has both
# `permissionsInformation.links` empty AND `permissionsInformation.principals`
# empty are dropped — they materially have no sharing info to land.
# Pagination on /Items uses verbose __next.

# Max items per /_api/$batch POST. SP recommends ≤100; 50 keeps each multipart
# body under ~1MB on observed shapes and limits damage from a single 500.
$script:SpoBatchSize = 50

function Get-SpoWebItemPermissions {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$InputId,
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    # The InputId is a composite "<webUrl>:::<listId>" — see Get-SpoWebLists
    # for why. The real listId comes from the ListId tag so this function
    # doesn't have to know about the composite encoding.
    $siteUrl     = if ($Context.InputTags) { $Context.InputTags.SiteUrl } else { $null }
    $webUrl      = if ($Context.InputTags) { $Context.InputTags.WebUrl  } else { $null }
    $listId      = if ($Context.InputTags) { $Context.InputTags.ListId  } else { $null }
    if ([string]::IsNullOrEmpty($siteUrl) -or [string]::IsNullOrEmpty($webUrl) -or [string]::IsNullOrEmpty($listId)) {
        throw "Get-SpoWebItemPermissions: required IdTags missing (SiteUrl=$siteUrl WebUrl=$webUrl ListId=$listId) for InputId=$InputId"
    }

    $reconnect = Get-PerRunspaceReconnect
    $getToken  = { Get-SpoToken -SiteUrl $siteUrl }.GetNewClosure()

    # Step 1: enumerate items with unique permissions. $select keeps the wire
    # payload light — we only need Id + FileRef + FileSystemObjectType to
    # build the GetSharingInformation request URL and to tag the landed row
    # with file path context.
    # HasUniqueRoleAssignments is a computed/runtime property of SPListItem,
    # NOT a queryable column — putting it in $filter returns 500 from SP's
    # QueryParser because the predicate can't be pushed down to the list
    # storage engine. Same restriction CAML's <Where> hits, and the same
    # reason PnP's old code went via Graph /drives/.../permissions instead of
    # SP REST. We enumerate all items with $select including the flag, then
    # filter client-side. Cost is "all doc-library items per list scanned" —
    # for OneDrive personal sites with millions of items this needs the
    # search-API or CAML path; we'll add that when a customer needs it.
    $itemsUrl = "$webUrl/_api/web/lists(guid'$listId')/Items?`$select=Id,FileRef,FileSystemObjectType,HasUniqueRoleAssignments&`$top=5000"
    $batchUrl = "$siteUrl/_api/`$batch"
    $expandQs = "`$Expand=permissionsInformation,pickerSettings,sharingLinkTemplates"

    # Stream items into fixed-size batches as they enumerate. A naive
    # accumulate-then-batch grows the in-memory list to (# items with
    # unique perms) per library — for a million-item OneDrive that's
    # ~100MB per worker, ~1GB across the pool. Flushing every
    # $SpoBatchSize items caps memory at the batch size per worker.
    $pendingBatch = [System.Collections.Generic.List[hashtable]]::new()

    $flushBatch = {
        if ($pendingBatch.Count -eq 0) { return }
        $batch = @($pendingBatch.ToArray())
        $pendingBatch.Clear()

        $requests = @()
        foreach ($it in $batch) {
            $requests += @{
                Method      = 'POST'
                Url         = "$webUrl/_api/web/lists(guid'$listId')/Items($($it.Id))/GetSharingInformation?$expandQs"
                Body        = '{}'
                ContentType = 'application/json;odata=verbose'
                Accept      = 'application/json;odata=verbose'
            }
        }
        $responses = Invoke-SpoBatch -BatchUrl $batchUrl -Requests $requests -GetToken $getToken -OnAuthReconnect $reconnect

        for ($j = 0; $j -lt $responses.Count; $j++) {
            $resp = $responses[$j]
            $it   = $batch[$j]
            if ($resp.Status -lt 200 -or $resp.Status -ge 300) {
                # Permanent per-item denial — only 403 (forbidden) and 404
                # (not found) reach this branch. Invoke-SpoBatch re-throws on
                # 401/429/503/500/502/504 sub-responses so RetryHelper retries
                # the whole batch with reconnect or backoff. No row lands in
                # JSONL for denied items.
                try {
                    Write-ItemFailedEvent -Category Skippable `
                        -ItemId "$webUrl/lists/$listId/items/$($it.Id)" `
                        -StatusCode ([int]$resp.Status) `
                        -Message "GetSharingInformation HTTP $($resp.Status) on $($it.FileRef)"
                } catch { Write-Verbose $_.Exception.Message }
                continue
            }
            $body = $resp.Body
            if ($null -eq $body) { continue }

            # GetSharingInformation returns a single-entity verbose envelope:
            #   { d: { permissionsInformation: {...}, pickerSettings: {...}, ... } }
            # ConvertFrom-SpoEnvelope (called from ConvertFrom-SpoBatchResponse)
            # already unwrapped .d, so $body is the inner object. Drop empties:
            # an item with no links AND no principals has no sharing data to
            # land — it would be a noise row in bronze.
            $cleaned = Resolve-SpoVerboseRecord $body
            $pi = if ($cleaned -is [System.Collections.IDictionary] -and $cleaned.Contains('permissionsInformation')) {
                $cleaned['permissionsInformation']
            } else { $null }
            $hasLinks      = $pi -and $pi -is [System.Collections.IDictionary] -and $pi.Contains('links')      -and @($pi['links']).Count -gt 0
            $hasPrincipals = $pi -and $pi -is [System.Collections.IDictionary] -and $pi.Contains('principals') -and @($pi['principals']).Count -gt 0
            if (-not $hasLinks -and -not $hasPrincipals) { continue }

            $rec = Add-FkColumns -Record $cleaned -Fks @{
                SiteUrl              = $siteUrl
                WebUrl               = $webUrl
                ListId               = $listId
                ItemId               = $it.Id
                FileRef              = $it.FileRef
                FileSystemObjectType = $it.FileSystemObjectType
            }
            $Writer.WriteRecord($rec)
        }
    }
    # NOTE: do not call `.GetNewClosure()` here. Closures freeze the lexical
    # bindings at creation, which breaks Pester's `Mock Invoke-SpoBatch`
    # interception. PowerShell's dynamic scoping means the script block
    # still sees $pendingBatch, $webUrl, etc. from Get-SpoWebItemPermissions'
    # active call frame when invoked via `& $flushBatch`.

    [void](Invoke-SpoRestPaged -Url $itemsUrl -GetToken $getToken -OnAuthReconnect $reconnect -OnRow {
        param($row)
        $cleaned = Resolve-SpoVerboseRecord $row
        if ($cleaned -isnot [System.Collections.IDictionary] -or -not $cleaned.Contains('Id')) { return }
        # Client-side filter: only items with unique role assignments have
        # something to land in GetSharingInformation. Items inheriting from
        # the list/web have no per-item permissions information; calling
        # GetSharingInformation on them returns the inherited set which we
        # already capture at the web/list level.
        if (-not $cleaned.Contains('HasUniqueRoleAssignments') -or -not [bool]$cleaned['HasUniqueRoleAssignments']) { return }
        $pendingBatch.Add(@{
            Id                   = [int]$cleaned['Id']
            FileRef              = if ($cleaned.Contains('FileRef')) { [string]$cleaned['FileRef'] } else { $null }
            FileSystemObjectType = if ($cleaned.Contains('FileSystemObjectType')) { [int]$cleaned['FileSystemObjectType'] } else { $null }
        })
        if ($pendingBatch.Count -ge $script:SpoBatchSize) {
            & $flushBatch
        }
    })
    # Flush the final partial batch (if any).
    & $flushBatch
}

Export-ModuleMember -Function `
    Get-ModuleStages, Get-ModuleEntities, `
    Get-SpoSitesRoot, `
    Get-SpoSiteDetails, `
    Get-SpoSiteUsers, `
    Get-SpoSiteGroups, `
    Get-SpoWebsRoot, `
    Get-SpoWebRoleDefinitions, `
    Get-SpoWebRoleAssignments, `
    Get-SpoWebLists, `
    Get-SpoWebItemPermissions