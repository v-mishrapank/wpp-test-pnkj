#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Integration tests for Invoke-ModuleRun using a fake entity module with only
# inline stages. Pool stages require live Graph/EXO/SPO auth and are covered
# by real-world usage rather than unit tests.

BeforeAll {
    $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
    Import-Module (Join-Path $modulesPath 'LogHelper.psm1')     -Force
    Import-Module (Join-Path $modulesPath 'EventEmitter.psm1')  -Force
    Import-Module (Join-Path $modulesPath 'StageWriter.psm1')   -Force
    Import-Module (Join-Path $modulesPath 'RecordEnvelope.psm1') -Force
    Import-Module (Join-Path $modulesPath 'RetryHelper.psm1')    -Force
    Import-Module (Join-Path $modulesPath 'StageExecutor.psm1') -Force
    # Seed runspace-local context so Write-Event calls inside Invoke-ModuleRun
    # carry a non-null run_id/tenant for any test that inspects emitted lines.
    Initialize-EventContext -RunId 'test-run' -Tenant 'test-tenant'

    $script:tempDir    = New-Item -ItemType Directory -Path (Join-Path ([System.IO.Path]::GetTempPath()) "InvokeModuleRunTest_$(Get-Random)") -Force
    $script:moduleFile = Join-Path $tempDir.FullName 'TestModule.psm1'

    # Fake entity module. Two independent root stages, each written as its
    # own entity, so we can verify per-entity file output and skip-when-not-requested.
    $moduleContent = @'
function Get-ModuleStages {
    @{
        'root_a' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-RootA'; ApiFamily = 'graph'; MinimumSelectFields = @('id') }
        'root_b' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-RootB'; ApiFamily = 'graph'; MinimumSelectFields = @('id') }
    }
}

function Get-ModuleEntities {
    @{
        'entity_a' = @{ Stage = 'root_a'; WritesTo = 'root'; SelectFields = @('id','name') }
        'entity_b' = @{ Stage = 'root_b'; WritesTo = 'root'; SelectFields = @('id') }
    }
}

function Get-RootA {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    $Writer.WriteRecord(@{ id = 'a1'; name = 'record1'; extra = 'x' })
    $Writer.WriteRecord(@{ id = 'a2'; name = 'record2'; extra = 'y' })
    $Writer.EmitId('a1', @{ kind = 'primary' })
    $Writer.EmitId('a2', $null)
}

function Get-RootB {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )
    $Writer.WriteRecord(@{ id = 'b1' })
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-RootA, Get-RootB
'@
    [System.IO.File]::WriteAllText($moduleFile, $moduleContent)

    $script:baseContext = @{
        RunId          = 'test1234'
        TenantKey      = 'fabrikam'
        Date           = '2026-04-19'
        SourceType     = 'tenant'
        SourceKey      = 'fabrikam'
        PoolSize       = 1
        AuthConfig     = @{}
        AuthModulePath = Join-Path $PSScriptRoot 'StubConnect.psm1'
        TempRoot       = $tempDir.FullName
    }
}

AfterAll {
    if ($tempDir -and (Test-Path $tempDir.FullName)) {
        Remove-Item $tempDir.FullName -Recurse -Force
    }
}

Describe 'Invoke-ModuleRun (inline stages)' {
    It 'returns one result per requested entity' {
        $results = Invoke-ModuleRun -ModulePath $moduleFile -RequestedEntities @('entity_a','entity_b') -Context $baseContext
        $results.Count | Should -Be 2
        ($results | ForEach-Object { $_.EntityName } | Sort-Object) | Should -Be @('entity_a','entity_b')
    }

    It 'writes record counts that match what the fetcher emitted' {
        $results = Invoke-ModuleRun -ModulePath $moduleFile -RequestedEntities @('entity_a','entity_b') -Context $baseContext
        $a = $results | Where-Object { $_.EntityName -eq 'entity_a' }
        $b = $results | Where-Object { $_.EntityName -eq 'entity_b' }
        $a.RecordCount | Should -Be 2
        $b.RecordCount | Should -Be 1
    }

    It 'produces a local file at the expected path with envelope-wrapped JSONL' {
        $results = Invoke-ModuleRun -ModulePath $moduleFile -RequestedEntities @('entity_a') -Context $baseContext
        $result = $results[0]
        Test-Path $result.LocalPaths[0] | Should -Be $true

        $lines = Get-Content $result.LocalPaths[0]
        $lines.Count | Should -Be 2
        $first = $lines[0] | ConvertFrom-Json -AsHashtable
        $first.source_type | Should -Be 'tenant'
        $first.source_key  | Should -Be 'fabrikam'
        $first.batch_id    | Should -Be 'test1234'
        $first._record.id  | Should -Be 'a1'
    }

    It 'computes BlobPath as <rootEntity>/<tenant>/<date>/<entity>_<runId>.jsonl' {
        $results = Invoke-ModuleRun -ModulePath $moduleFile -RequestedEntities @('entity_a') -Context $baseContext
        $results[0].BlobPaths[0] | Should -Be 'entity_a/fabrikam/2026-04-19/entity_a_test1234.jsonl'
    }

    It 'does not produce results for entities that were not requested' {
        $results = Invoke-ModuleRun -ModulePath $moduleFile -RequestedEntities @('entity_a') -Context $baseContext
        ($results | Where-Object { $_.EntityName -eq 'entity_b' }).Count | Should -Be 0
    }

    It 'tags status=success when the fetcher returns without error' {
        $results = Invoke-ModuleRun -ModulePath $moduleFile -RequestedEntities @('entity_a') -Context $baseContext
        $results[0].Status | Should -Be 'success'
    }

    It 'returns BasePath for manifest upload' {
        $results = Invoke-ModuleRun -ModulePath $moduleFile -RequestedEntities @('entity_a') -Context $baseContext
        $results[0].BasePath | Should -Be 'entity_a/fabrikam/2026-04-19'
    }
}

Describe 'Invoke-ModuleRun (inline stages, zero-record output)' {
    BeforeAll {
        # Fetcher that never calls WriteRecord. Mirrors the real-world
        # case where an MDE-style API returns 0 rows for a tenant. StageExecutor
        # should produce a success result with ChunkCount=0 and no
        # LocalPaths/BlobPaths — any downstream upload loop must be a no-op.
        $script:emptyFile = Join-Path $tempDir.FullName 'EmptyFetchModule.psm1'
        $content = @'
function Get-ModuleStages {
    @{
        'empty_root' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-Empty'; ApiFamily = 'graph'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'entity_empty' = @{ Stage = 'empty_root'; WritesTo = 'root'; SelectFields = @('id') }
    }
}
function Get-Empty { param([hashtable]$Context, $Writer) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-Empty
'@
        [System.IO.File]::WriteAllText($emptyFile, $content)
    }

    It 'emits RecordCount=0 with status=success when the fetch writes nothing' {
        $results = Invoke-ModuleRun -ModulePath $emptyFile -RequestedEntities @('entity_empty') -Context $baseContext
        $results[0].Status      | Should -Be 'success'
        $results[0].RecordCount | Should -Be 0
    }

    It 'emits ChunkCount=0 and empty LocalPaths/BlobPaths so the upload loop is a no-op' {
        $results = Invoke-ModuleRun -ModulePath $emptyFile -RequestedEntities @('entity_empty') -Context $baseContext
        $results[0].ChunkCount       | Should -Be 0
        $results[0].LocalPaths.Count | Should -Be 0
        $results[0].BlobPaths.Count  | Should -Be 0
    }
}

Describe 'Invoke-ModuleRun (multi-stage entity BasePath resolution)' {
    BeforeAll {
        # ExoGroups-shaped module: two inline roots (dg_root, ug_root), each
        # owned by its own single-stage entity, plus a multi-stage entity fed
        # by the pool stages under both roots. The multi-stage entity's output
        # must land under each parent root's folder — it does not own the root
        # folder itself.
        $script:multiFile = Join-Path $tempDir.FullName 'MultiStageTestModule.psm1'
        $content = @'
function Get-ModuleStages {
    @{
        'dg_root'    = @{ InputFrom = $null;      RunsOnPool = $false; Function = 'Get-DgRoot';    ApiFamily = 'graph'; IdKey = 'id'; EmitIds = $true; MinimumSelectFields = @('id') }
        'dg_members' = @{ InputFrom = 'dg_root';  RunsOnPool = $true;  Function = 'Get-DgMembers'; ApiFamily = 'graph' }
        'ug_root'    = @{ InputFrom = $null;      RunsOnPool = $false; Function = 'Get-UgRoot';    ApiFamily = 'graph'; IdKey = 'id'; EmitIds = $true; MinimumSelectFields = @('id') }
        'ug_members' = @{ InputFrom = 'ug_root';  RunsOnPool = $true;  Function = 'Get-UgMembers'; ApiFamily = 'graph' }
    }
}

function Get-ModuleEntities {
    @{
        'entity_a'     = @{ Stage = 'dg_root';                  WritesTo = 'root';    SelectFields = @('id','a') }
        'entity_b'     = @{ Stage = 'ug_root';                  WritesTo = 'root';    SelectFields = @('id','b') }
        'entity_multi' = @{ Stage = @('dg_members','ug_members'); WritesTo = 'members'; SelectFields = @('id','m') }
    }
}

function Get-DgRoot    { param([hashtable]$Context, $Writer) $Writer.WriteRecord(@{ id = 'a1' }); $Writer.EmitId('a1', $null) }
function Get-UgRoot    { param([hashtable]$Context, $Writer) $Writer.WriteRecord(@{ id = 'b1' }); $Writer.EmitId('b1', $null) }
function Get-DgMembers { param([string]$InputId, [hashtable]$Context, $Writer) $Writer.WriteRecord(@{ parentId = $InputId }) }
function Get-UgMembers { param([string]$InputId, [hashtable]$Context, $Writer) $Writer.WriteRecord(@{ parentId = $InputId }) }

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-DgRoot, Get-UgRoot, Get-DgMembers, Get-UgMembers
'@
        [System.IO.File]::WriteAllText($multiFile, $content)
    }

    It 'multi-stage entity output lands under the single-stage entities root folders' {
        # Request only the multi-stage entity. Landing folders are the
        # single-stage entities at each root (entity_a, entity_b), resolved
        # deterministically via WritesTo='root'. The multi-stage entity's
        # WritesTo='members' only supplies the subdirectory under each root.
        $results = Invoke-ModuleRun -ModulePath $multiFile -RequestedEntities @('entity_multi') -Context $baseContext
        $results.Count | Should -Be 2
        $basePaths = ($results | ForEach-Object { $_.BasePath } | Sort-Object)
        $basePaths[0] | Should -Be 'entity_a/fabrikam/2026-04-19'
        $basePaths[1] | Should -Be 'entity_b/fabrikam/2026-04-19'
    }

    It 'routes single-stage entities to their own folders when only those are requested (no multi-stage entity in play)' {
        $results = Invoke-ModuleRun -ModulePath $multiFile -RequestedEntities @('entity_a','entity_b') -Context $baseContext
        ($results | Where-Object { $_.EntityName -eq 'entity_a' }).BasePath | Should -Be 'entity_a/fabrikam/2026-04-19'
        ($results | Where-Object { $_.EntityName -eq 'entity_b' }).BasePath | Should -Be 'entity_b/fabrikam/2026-04-19'
    }
}

Describe 'Invoke-ModuleRun (root-folder ownership with sibling single-stage entities)' {
    # Models the teams family: one inline root (teams_root) feeds several
    # single-stage child entities (installed_apps, channels, ...).
    # Every child's root resolves to the same root stage. Before the #156
    # fix, whichever child hashtable-enumeration hit first claimed that root
    # stage and the multi-stage 'root' entity's output landed under a
    # sibling's folder. Resolution now goes through WritesTo='root'.
    BeforeAll {
        $script:teamsFile = Join-Path $tempDir.FullName 'TeamsShapedModule.psm1'
        $content = @'
function Get-ModuleStages {
    @{
        'teams_root'     = @{ InputFrom = $null;         RunsOnPool = $false; Function = 'Get-TeamsRoot';     ApiFamily = 'graph'; IdKey = 'id'; EmitIds = $true; MinimumSelectFields = @('id') }
        'team_settings'  = @{ InputFrom = 'teams_root';  RunsOnPool = $true;  Function = 'Get-TeamSettings';  ApiFamily = 'graph' }
        'team_installed' = @{ InputFrom = 'teams_root';  RunsOnPool = $true;  Function = 'Get-TeamInstalled'; ApiFamily = 'graph' }
        'team_channels'  = @{ InputFrom = 'teams_root';  RunsOnPool = $true;  Function = 'Get-TeamChannels';  ApiFamily = 'graph' }
    }
}

function Get-ModuleEntities {
    @{
        'teams_teams'          = @{ Stage = @('teams_root','team_settings'); WritesTo = @{ teams_root = 'root'; team_settings = 'settings' }; SelectFields = @('id','displayName') }
        'teams_installed_apps' = @{ Stage = 'team_installed';                WritesTo = 'installed_apps' }
        'teams_channels'       = @{ Stage = 'team_channels';                 WritesTo = 'channels' }
    }
}

function Get-TeamsRoot     { param([hashtable]$Context, $Writer) $Writer.WriteRecord(@{ id = 't1'; displayName = 'Team 1' }); $Writer.EmitId('t1', $null) }
function Get-TeamSettings  { param([string]$InputId, [hashtable]$Context, $Writer) $Writer.WriteRecord(@{ teamId = $InputId; setting = 'x' }) }
function Get-TeamInstalled { param([string]$InputId, [hashtable]$Context, $Writer) $Writer.WriteRecord(@{ teamId = $InputId; appId = 'a' }) }
function Get-TeamChannels  { param([string]$InputId, [hashtable]$Context, $Writer) $Writer.WriteRecord(@{ teamId = $InputId; channelId = 'c' }) }

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-TeamsRoot, Get-TeamSettings, Get-TeamInstalled, Get-TeamChannels
'@
        [System.IO.File]::WriteAllText($teamsFile, $content)
    }

    It 'routes a single-stage child entity''s output under the root entity''s folder' {
        $results = Invoke-ModuleRun -ModulePath $teamsFile -RequestedEntities @('teams_installed_apps') -Context $baseContext
        $installed = $results | Where-Object { $_.EntityName -eq 'teams_installed_apps' }
        $installed.BasePath | Should -Be 'teams_teams/fabrikam/2026-04-19'
    }

    It 'routes every sibling child entity under the root entity''s folder (not under another sibling)' {
        $results = Invoke-ModuleRun -ModulePath $teamsFile -RequestedEntities @('teams_installed_apps','teams_channels') -Context $baseContext
        foreach ($r in $results) {
            $r.BasePath | Should -Be 'teams_teams/fabrikam/2026-04-19'
        }
    }

    It 'throws when no entity claims a root stage via WritesTo=root' {
        $badFile = Join-Path $tempDir.FullName 'NoRootClaim.psm1'
        $content = @'
function Get-ModuleStages {
    @{
        'r'     = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-R'; ApiFamily = 'graph'; IdKey = 'id'; EmitIds = $true; MinimumSelectFields = @('id') }
        'child' = @{ InputFrom = 'r';   RunsOnPool = $true;  Function = 'Get-C'; ApiFamily = 'graph' }
    }
}
function Get-ModuleEntities {
    @{
        'only_child' = @{ Stage = 'child'; WritesTo = 'sub' }
    }
}
function Get-R { param([hashtable]$Context, $Writer) }
function Get-C { param([string]$InputId, [hashtable]$Context, $Writer) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-R, Get-C
'@
        [System.IO.File]::WriteAllText($badFile, $content)
        { Invoke-ModuleRun -ModulePath $badFile -RequestedEntities @('only_child') -Context $baseContext } |
            Should -Throw "*no entity declaring WritesTo='root'*"
    }

    It 'throws when two entities both claim the same root stage via WritesTo=root' {
        $badFile = Join-Path $tempDir.FullName 'DoubleRootClaim.psm1'
        $content = @'
function Get-ModuleStages {
    @{
        'r' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-R'; ApiFamily = 'graph'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'one' = @{ Stage = 'r'; WritesTo = 'root'; SelectFields = @('id') }
        'two' = @{ Stage = 'r'; WritesTo = 'root'; SelectFields = @('id') }
    }
}
function Get-R { param([hashtable]$Context, $Writer) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-R
'@
        [System.IO.File]::WriteAllText($badFile, $content)
        { Invoke-ModuleRun -ModulePath $badFile -RequestedEntities @('one','two') -Context $baseContext } |
            Should -Throw "*claimed by multiple entities*"
    }
}

Describe 'Invoke-ModuleRun (inline stage WritesTo validation)' {
    BeforeAll {
        $script:badFile = Join-Path $tempDir.FullName 'InlineSubdirBadModule.psm1'
        $content = @'
function Get-ModuleStages {
    @{
        'bad_root' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-Bad'; ApiFamily = 'graph' }
    }
}
function Get-ModuleEntities {
    @{
        'bad_entity' = @{ Stage = 'bad_root'; WritesTo = 'some_subdir' }
    }
}
function Get-Bad { param([hashtable]$Context, $Writer) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-Bad
'@
        [System.IO.File]::WriteAllText($badFile, $content)
    }

    It 'throws when an inline stage declares a non-root WritesTo' {
        { Invoke-ModuleRun -ModulePath $badFile -RequestedEntities @('bad_entity') -Context $baseContext } |
            Should -Throw "*Inline stage*must write to 'root'*"
    }
}

Describe 'Invoke-ModuleRun (SelectFields validation)' {
    It 'throws when an entity declares SelectFields = @()' {
        $f = Join-Path $tempDir.FullName 'EmptySelectFields.psm1'
        $content = @'
function Get-ModuleStages   { @{ 'r' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-R'; ApiFamily = 'graph' } } }
function Get-ModuleEntities { @{ 'e' = @{ Stage = 'r'; WritesTo = 'root'; SelectFields = @() } } }
function Get-R { param([hashtable]$Context, $Writer) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-R
'@
        [System.IO.File]::WriteAllText($f, $content)
        { Invoke-ModuleRun -ModulePath $f -RequestedEntities @('e') -Context $baseContext } |
            Should -Throw "*empty array*"
    }

    It 'throws when an entity declares SelectFields = $null' {
        $f = Join-Path $tempDir.FullName 'NullSelectFields.psm1'
        $content = @'
function Get-ModuleStages   { @{ 'r' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-R'; ApiFamily = 'graph' } } }
function Get-ModuleEntities { @{ 'e' = @{ Stage = 'r'; WritesTo = 'root'; SelectFields = $null } } }
function Get-R { param([hashtable]$Context, $Writer) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-R
'@
        [System.IO.File]::WriteAllText($f, $content)
        { Invoke-ModuleRun -ModulePath $f -RequestedEntities @('e') -Context $baseContext } |
            Should -Throw "*SelectFields = `$null*"
    }

    It 'throws when a per-stage SelectFields hashtable has an empty entry' {
        $f = Join-Path $tempDir.FullName 'EmptyPerStageEntry.psm1'
        $content = @'
function Get-ModuleStages {
    @{
        'a' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-A'; ApiFamily = 'graph' }
        'b' = @{ InputFrom = 'a';   RunsOnPool = $true;  Function = 'Get-B'; ApiFamily = 'graph' }
    }
}
function Get-ModuleEntities {
    @{ 'e' = @{ Stage = @('a','b'); WritesTo = @{ a = 'root'; b = 'sub' }; SelectFields = @{ a = @('id'); b = @() } } }
}
function Get-A { param([hashtable]$Context, $Writer) }
function Get-B { param([string]$InputId, [hashtable]$Context, $Writer) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-A, Get-B
'@
        [System.IO.File]::WriteAllText($f, $content)
        { Invoke-ModuleRun -ModulePath $f -RequestedEntities @('e') -Context $baseContext } |
            Should -Throw "*empty/null*stage 'b'*"
    }

    It 'accepts an entity that omits SelectFields entirely (raw emission)' {
        $f = Join-Path $tempDir.FullName 'NoSelectFields.psm1'
        $content = @'
function Get-ModuleStages   { @{ 'r' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-R'; ApiFamily = 'graph' } } }
function Get-ModuleEntities { @{ 'e' = @{ Stage = 'r'; WritesTo = 'root' } } }
function Get-R { param([hashtable]$Context, $Writer) $Writer.WriteRecord(@{ id = 'x'; whatever = 'y' }) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-R
'@
        [System.IO.File]::WriteAllText($f, $content)
        $results = Invoke-ModuleRun -ModulePath $f -RequestedEntities @('e') -Context $baseContext
        ($results | Where-Object { $_.EntityName -eq 'e' }).Status | Should -Be 'success'
    }
}

Describe 'Invoke-ModuleRun (writer projection enforces declared SelectFields)' {
    It 'projects inline-stage records to exactly the declared fields' {
        $f = Join-Path $tempDir.FullName 'InlineProjection.psm1'
        $content = @'
function Get-ModuleStages   { @{ 'r' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-R'; ApiFamily = 'graph'; MinimumSelectFields = @('id') } } }
function Get-ModuleEntities { @{ 'e' = @{ Stage = 'r'; WritesTo = 'root'; SelectFields = @('id','kept') } } }
function Get-R {
    param([hashtable]$Context, $Writer)
    $Writer.WriteRecord(@{ id = '1'; kept = 'yes'; discarded = 'no' })
}
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-R
'@
        [System.IO.File]::WriteAllText($f, $content)
        $results = Invoke-ModuleRun -ModulePath $f -RequestedEntities @('e') -Context $baseContext
        $localPath = ($results | Where-Object { $_.EntityName -eq 'e' }).LocalPaths[0]
        $line = Get-Content $localPath -Raw
        $line | Should -Match '"kept"\s*:\s*"yes"'
        $line | Should -Not -Match '"discarded"'
    }
}

Describe 'Invoke-ModuleRun (inline stage JsonDepth — issue #162)' {
    # End-to-end: a stage spec with JsonDepth set should propagate the
    # +1 envelope adjustment all the way to the JsonlRecordWriter so a
    # deeply-nested record round-trips clean. Without the plumbing the
    # writer would default to Depth=6 and truncate.
    BeforeAll {
        $script:deepFile = Join-Path $tempDir.FullName 'DeepModule.psm1'
        $deepContent = @'
function Get-ModuleStages {
    @{
        'deep_root' = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-Deep'
            ApiFamily           = 'graph'
            MinimumSelectFields = @('id')
            JsonDepth           = 12
        }
        'shallow_root' = @{
            InputFrom           = $null
            RunsOnPool          = $false
            Function            = 'Get-Shallow'
            ApiFamily           = 'graph'
            MinimumSelectFields = @('id')
            # No JsonDepth — must inherit the function default.
        }
    }
}
function Get-ModuleEntities {
    @{
        'entity_deep'    = @{ Stage = 'deep_root';    WritesTo = 'root' }
        'entity_shallow' = @{ Stage = 'shallow_root'; WritesTo = 'root' }
    }
}
function Get-Deep {
    param([hashtable]$Context, $Writer)
    # 11-wrap-deep record. Truncates at envelope-Depth 6 (the default);
    # fits at envelope-Depth 13 (JsonDepth 12 + 1).
    $rec = 'leaf'
    for ($i = 0; $i -lt 11; $i++) { $rec = @{ k = $rec } }
    $Writer.WriteRecord($rec)
}
function Get-Shallow {
    param([hashtable]$Context, $Writer)
    $Writer.WriteRecord(@{ id = 's1' })
}
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-Deep, Get-Shallow
'@
        [System.IO.File]::WriteAllText($deepFile, $deepContent)
    }

    It 'serializes an 11-wrap-deep record cleanly when the stage declares JsonDepth=12' {
        $results = Invoke-ModuleRun -ModulePath $deepFile -RequestedEntities @('entity_deep') -Context $baseContext
        $line = Get-Content -LiteralPath $results[0].LocalPaths[0] -Raw
        $line | Should -Not -Match 'System\.Collections\.Hashtable'
        # Sanity: all 11 levels of nesting are present in the output.
        ($line -split '"k":').Count - 1 | Should -Be 11
    }

    It 'still truncates at the function default when the stage omits JsonDepth' {
        # Replace Get-Shallow with a deep-record version so we can verify
        # the default-path truncation. We rebuild the module file with one
        # field changed; reusing the existing $deepFile to keep the harness
        # surface area small.
        $defaultDepthModule = Join-Path $tempDir.FullName 'DefaultDepthModule.psm1'
        $content = @'
function Get-ModuleStages {
    @{
        'default_root' = @{
            InputFrom = $null; RunsOnPool = $false; Function = 'Get-Default'
            ApiFamily = 'graph'; MinimumSelectFields = @('id')
        }
    }
}
function Get-ModuleEntities {
    @{ 'entity_default' = @{ Stage = 'default_root'; WritesTo = 'root' } }
}
function Get-Default {
    param([hashtable]$Context, $Writer)
    $rec = 'leaf'
    for ($i = 0; $i -lt 11; $i++) { $rec = @{ k = $rec } }
    $Writer.WriteRecord($rec)
}
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-Default
'@
        [System.IO.File]::WriteAllText($defaultDepthModule, $content)
        $results = Invoke-ModuleRun -ModulePath $defaultDepthModule -RequestedEntities @('entity_default') -Context $baseContext -WarningAction SilentlyContinue
        $line = Get-Content -LiteralPath $results[0].LocalPaths[0] -Raw
        $line | Should -Match 'System\.Collections\.Hashtable'
    }
}

Describe 'Invoke-ModuleRun (stage failure handling)' {
    BeforeAll {
        $script:failFile = Join-Path $tempDir.FullName 'FailingModule.psm1'
        $content = @'
function Get-ModuleStages {
    @{
        'good_root' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-Good'; ApiFamily = 'graph'; MinimumSelectFields = @('id') }
        'bad_root'  = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-Bad';  ApiFamily = 'graph'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'entity_good' = @{ Stage = 'good_root'; WritesTo = 'root'; SelectFields = @('id','v') }
        'entity_bad'  = @{ Stage = 'bad_root';  WritesTo = 'root'; SelectFields = @('id','v') }
    }
}
function Get-Good { param([hashtable]$Context, $Writer) $Writer.WriteRecord(@{ id = 'g1' }) }
function Get-Bad  { param([hashtable]$Context, $Writer) throw [System.Net.Http.HttpRequestException]::new('simulated failure: 400 (BadRequest).', $null, [System.Net.HttpStatusCode]::BadRequest) }
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-Good, Get-Bad
'@
        [System.IO.File]::WriteAllText($failFile, $content)
    }

    It 'marks a failing stage as status=failed and continues executing other stages' {
        $results = Invoke-ModuleRun -ModulePath $failFile -RequestedEntities @('entity_good','entity_bad') -Context $baseContext
        $good = $results | Where-Object { $_.EntityName -eq 'entity_good' }
        $bad  = $results | Where-Object { $_.EntityName -eq 'entity_bad'  }
        $good.Status | Should -Be 'success'
        $good.RecordCount | Should -Be 1
        $bad.Status  | Should -Be 'failed'
        $bad.Errors[0] | Should -Match 'simulated failure'
    }
}

Describe 'Invoke-ModuleRun (non-terminating error promoted by -ErrorAction Stop, #481)' {
    # EXO cmdlets emit non-terminating errors on server-side 5xx. Without
    # -ErrorAction Stop the error writes to stderr and the pipeline yields
    # zero records — the function returns normally and StageExecutor reports
    # success. With -ErrorAction Stop the error becomes terminating and
    # StageExecutor's catch fires, marking the entity failed. This test
    # locks down that Write-Error -ErrorAction Stop propagates correctly.
    BeforeAll {
        $script:writeErrFile = Join-Path $tempDir.FullName 'WriteErrorModule.psm1'
        $content = @'
function Get-ModuleStages {
    @{
        'ok_root'    = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-Ok';    ApiFamily = 'graph'; MinimumSelectFields = @('id') }
        'err_root'   = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-Err';   ApiFamily = 'graph'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'entity_ok'  = @{ Stage = 'ok_root';  WritesTo = 'root'; SelectFields = @('id') }
        'entity_err' = @{ Stage = 'err_root'; WritesTo = 'root'; SelectFields = @('id') }
    }
}
function Get-Ok {
    param([hashtable]$Context, $Writer)
    $Writer.WriteRecord(@{ id = 'ok1' })
}
function Get-Err {
    param([hashtable]$Context, $Writer)
    $ex = [System.Net.Http.HttpRequestException]::new('A server side error has occurred because of which the operation could not be completed.', $null, [System.Net.HttpStatusCode]::BadRequest)
    Write-Error -Exception $ex -ErrorAction Stop
}
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-Ok, Get-Err
'@
        [System.IO.File]::WriteAllText($writeErrFile, $content)
    }

    It 'marks the entity as failed when a non-terminating error is promoted to terminating' {
        $results = Invoke-ModuleRun -ModulePath $writeErrFile -RequestedEntities @('entity_err') -Context $baseContext
        $err = $results | Where-Object { $_.EntityName -eq 'entity_err' }
        $err.Status | Should -Be 'failed'
        $err.Errors[0] | Should -Match 'server side error'
    }

    It 'carries the error message through to the result' {
        $results = Invoke-ModuleRun -ModulePath $writeErrFile -RequestedEntities @('entity_err') -Context $baseContext
        $err = $results | Where-Object { $_.EntityName -eq 'entity_err' }
        $err.Errors[0] | Should -Match 'could not be completed'
    }

    It 'does not affect sibling entities in the same module' {
        $results = Invoke-ModuleRun -ModulePath $writeErrFile -RequestedEntities @('entity_ok','entity_err') -Context $baseContext
        $ok  = $results | Where-Object { $_.EntityName -eq 'entity_ok'  }
        $err = $results | Where-Object { $_.EntityName -eq 'entity_err' }
        $ok.Status      | Should -Be 'success'
        $ok.RecordCount | Should -Be 1
        $err.Status     | Should -Be 'failed'
    }
}

Describe 'Invoke-ModuleRun (pool stage total-failure classification, #356)' {
    # End-to-end coverage for the AllItemsExhausted / TotalFailure classification
    # added in #356. Drives the full Invoke-ModuleRun → StageExecutor → WorkerPool
    # path with a fake module that fans out from an inline root to a pool child
    # whose fetch deterministically throws. Validates:
    #   - All-NonRetryable: result.Status='failed', FailedCount=N, SkippedCount=0,
    #     stage_failed event with error_class='AllItemsExhausted'.
    #   - All-Skippable: same shape, but SkippedCount=N, FailedCount=0.
    #   - Cascade: descendants of a failed pool stage get Status='skipped' with
    #     a stage_skipped event referencing the failed ancestor.
    #
    # Pre-#356 the existing $totalFailure cascade only fired on Errors.Count > 0,
    # which missed the 100%-Skippable case (Skippable doesn't populate $errors)
    # entirely, and conflated 100%-NonRetryable with chunk-fatal failures under
    # the same TotalFailure class. This test locks down the split.
    BeforeAll {
        $modulesPath = Join-Path $PSScriptRoot '..' 'modules'
        Import-Module (Join-Path $modulesPath 'RetryHelper.psm1') -Force
        Import-Module (Join-Path $modulesPath 'WorkerPool.psm1')  -Force

        # Same Graph-SDK-not-installed workaround as WorkerPool.Tests — swap to a
        # built-in module name so New-WorkerPool's ISS import is a no-op cost.
        # StubConnect supplies Connect-Service / Restore-ServiceConnection; the
        # real Graph SDK is never called.
        InModuleScope WorkerPool {
            $script:OriginalGraphModuleForClassificationTest = $script:ModuleNames['graph']
            $script:ModuleNames['graph'] = 'Microsoft.PowerShell.Utility'
        }

        $script:classTempDir = New-Item -ItemType Directory -Path (
            Join-Path ([System.IO.Path]::GetTempPath()) "ModuleRunClassTest_$(Get-Random)"
        ) -Force
        $script:classModuleFile = Join-Path $classTempDir.FullName 'ClassFakeEntity.psm1'

        # Three pool-child variants on a shared inline root:
        #   - child_400  : every item throws HTTP 400 → NonRetryable
        #   - child_skip : every item throws Request_ResourceNotFound → Skippable
        #   - grandchild : depends on child_400; should be cascade-skipped, never fetched
        $moduleContent = @'
function Get-ModuleStages {
    @{
        'root_emit3' = @{
            InputFrom = $null
            RunsOnPool = $false
            Function = 'Get-RootEmit3Ids'
            ApiFamily = 'graph'
            MinimumSelectFields = @('id')
        }
        'child_400' = @{
            InputFrom = 'root_emit3'
            RunsOnPool = $true
            Function = 'Get-Bad400'
            ApiFamily = 'graph'
            MinimumSelectFields = @('id')
        }
        'child_skip' = @{
            InputFrom = 'root_emit3'
            RunsOnPool = $true
            Function = 'Get-Skippable'
            ApiFamily = 'graph'
            MinimumSelectFields = @('id')
        }
        'grandchild' = @{
            InputFrom = 'child_400'
            RunsOnPool = $true
            Function = 'Get-Bad400'
            ApiFamily = 'graph'
            MinimumSelectFields = @('id')
        }
    }
}

function Get-ModuleEntities {
    @{
        'entity_root'      = @{ Stage = 'root_emit3'; WritesTo = 'root';       SelectFields = @('id') }
        'entity_child_400' = @{ Stage = 'child_400';  WritesTo = 'child_400';  SelectFields = @('id') }
        'entity_child_skip'= @{ Stage = 'child_skip'; WritesTo = 'child_skip'; SelectFields = @('id') }
        'entity_grand'     = @{ Stage = 'grandchild'; WritesTo = 'grandchild'; SelectFields = @('id') }
    }
}

function Get-RootEmit3Ids {
    param([hashtable]$Context, $Writer)
    # Inline root emits 3 IDs and no records (no WritesTo='root' record stream
    # is needed for this test — only the EmitId fan-out matters).
    $Writer.EmitId('i1', $null)
    $Writer.EmitId('i2', $null)
    $Writer.EmitId('i3', $null)
}

function Get-Bad400 {
    param([string]$InputId, [hashtable]$Context, $Writer)
    # Mirrors the shape that Get-HttpStatusCode reads in RetryHelper.psm1 —
    # .Response.StatusCode lands at 400 → classifier returns NonRetryable.
    $resp = [PSCustomObject]@{ StatusCode = 400; Headers = $null }
    $ex   = [System.Exception]::new("400 Bad Request: bad query for $InputId")
    Add-Member -InputObject $ex -MemberType NoteProperty -Name Response -Value $resp
    throw $ex
}

function Get-Skippable {
    param([string]$InputId, [hashtable]$Context, $Writer)
    # 'Request_ResourceNotFound' is the graph-family Skippable pattern in
    # RetryHelper's $skippablePattern. Classifier returns Skippable.
    throw "Request_ResourceNotFound: item $InputId is gone"
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-RootEmit3Ids, Get-Bad400, Get-Skippable
'@
        [System.IO.File]::WriteAllText($classModuleFile, $moduleContent)

        $script:classContext = @{
            RunId          = 'classtest1'
            TenantKey      = 'fabrikam'
            Date           = '2026-05-11'
            SourceType     = 'tenant'
            SourceKey      = 'fabrikam'
            PoolSize       = 2
            AuthConfig     = @{}
            AuthModulePath = Join-Path $PSScriptRoot 'StubConnect.psm1'
            TempRoot       = $classTempDir.FullName
        }
    }

    AfterAll {
        if ($classTempDir -and (Test-Path $classTempDir.FullName)) {
            Remove-Item $classTempDir.FullName -Recurse -Force
        }
        InModuleScope WorkerPool {
            if ($script:OriginalGraphModuleForClassificationTest) {
                $script:ModuleNames['graph'] = $script:OriginalGraphModuleForClassificationTest
            }
        }
    }

    Context 'AllItemsExhausted classification' {
        BeforeEach {
            # Capture every Write-Event line. Mock at EventEmitter scope so
            # Write-Event's internal Write-Log call resolves to the mock — works
            # for stage_failed/stage_completed/stage_skipped emissions from the
            # main process (StageExecutor calls Write-Event, which lives in
            # EventEmitter and calls Write-Log there).
            $script:capturedEventLines = [System.Collections.Generic.List[string]]::new()
            Mock -ModuleName EventEmitter Write-Log {
                $script:capturedEventLines.Add($Message)
            }
        }

        It 'NonRetryable: result row reports Status=failed with FailedCount=N (not SkippedCount)' {
            $results = Invoke-ModuleRun -ModulePath $classModuleFile `
                -RequestedEntities @('entity_child_400') -Context $classContext

            $child = $results | Where-Object { $_.EntityName -eq 'entity_child_400' }
            $child           | Should -Not -BeNullOrEmpty
            $child.Status    | Should -Be 'failed'
            $child.RecordCount  | Should -Be 0
            $child.FailedCount  | Should -Be 3
            $child.SkippedCount | Should -Be 0
        }

        It 'NonRetryable: emits stage_failed with error_class=AllItemsExhausted' {
            $null = Invoke-ModuleRun -ModulePath $classModuleFile `
                -RequestedEntities @('entity_child_400') -Context $classContext

            $stageFailedLines = $script:capturedEventLines | Where-Object {
                $_ -match '"event_type":"stage_failed"' -and $_ -match '"stage":"child_400"'
            }
            @($stageFailedLines).Count | Should -BeGreaterOrEqual 1
            $line = @($stageFailedLines)[0]
            $line | Should -Match '"error_class":"AllItemsExhausted"'
            # error_message should include the counter breakdown that operators see.
            $line | Should -Match 'skipped=0 failed=3 records=0'
        }

        It 'Skippable: result row reports Status=skipped with SkippedCount=N (not FailedCount)' {
            $results = Invoke-ModuleRun -ModulePath $classModuleFile `
                -RequestedEntities @('entity_child_skip') -Context $classContext

            $child = $results | Where-Object { $_.EntityName -eq 'entity_child_skip' }
            $child           | Should -Not -BeNullOrEmpty
            $child.Status    | Should -Be 'skipped'
            $child.RecordCount  | Should -Be 0
            $child.SkippedCount | Should -Be 3
            $child.FailedCount  | Should -Be 0
        }

        It 'Skippable: emits stage_failed with error_class=AllItemsExhausted' {
            # Pre-#356 this case was the worst: 100%-Skippable doesn't populate
            # $errors, so the old totalFailure check (Errors.Count > 0) never
            # fired and the stage emitted stage_completed with records=0 — the
            # exact failure mode #341 surfaced. AllItemsExhausted now catches it.
            $null = Invoke-ModuleRun -ModulePath $classModuleFile `
                -RequestedEntities @('entity_child_skip') -Context $classContext

            $stageFailedLines = $script:capturedEventLines | Where-Object {
                $_ -match '"event_type":"stage_failed"' -and $_ -match '"stage":"child_skip"'
            }
            @($stageFailedLines).Count | Should -BeGreaterOrEqual 1
            $line = @($stageFailedLines)[0]
            $line | Should -Match '"error_class":"AllItemsExhausted"'
            $line | Should -Match 'skipped=3 failed=0 records=0'
        }

        It 'cascade: grandchild of an AllItemsExhausted stage is stage_skipped with ancestor_failed' {
            $results = Invoke-ModuleRun -ModulePath $classModuleFile `
                -RequestedEntities @('entity_grand') -Context $classContext

            # The grandchild result must report Status='skipped' (never ran).
            $grand = $results | Where-Object { $_.EntityName -eq 'entity_grand' }
            $grand          | Should -Not -BeNullOrEmpty
            $grand.Status   | Should -Be 'skipped'
            $grand.RecordCount | Should -Be 0
            $grand.Errors[0] | Should -Match "ancestor stage 'child_400' failed"

            # And a stage_skipped event must reference child_400 as the failed ancestor.
            $skippedLines = $script:capturedEventLines | Where-Object {
                $_ -match '"event_type":"stage_skipped"' -and $_ -match '"stage":"grandchild"'
            }
            @($skippedLines).Count | Should -BeGreaterOrEqual 1
            $skipLine = @($skippedLines)[0]
            $skipLine | Should -Match '"reason":"ancestor_failed"'
            $skipLine | Should -Match '"ancestor_stage":"child_400"'
        }
    }
}

Describe 'Invoke-ModuleRun (inline stage retry)' {
    BeforeAll {
        Import-Module (Join-Path $PSScriptRoot 'StubConnect.psm1') -Force

        $script:authRetryFile = Join-Path $tempDir.FullName 'AuthRetryModule.psm1'
        $content = @'
$script:CallCount = 0

function Get-ModuleStages {
    @{
        'auth_root' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-AuthRoot'; ApiFamily = 'exo'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'entity_auth' = @{ Stage = 'auth_root'; WritesTo = 'root'; SelectFields = @('id','name') }
    }
}
function Get-AuthRoot {
    param([hashtable]$Context, $Writer)
    $script:CallCount++
    if ($script:CallCount -le 1) {
        throw [System.Net.Http.HttpRequestException]::new('Response status code does not indicate success: 401 (Unauthorized).', $null, [System.Net.HttpStatusCode]::Unauthorized)
    }
    $Writer.WriteRecord(@{ id = 'r1'; name = 'recovered' })
}
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-AuthRoot
'@
        [System.IO.File]::WriteAllText($authRetryFile, $content)

        $script:authExhaustFile = Join-Path $tempDir.FullName 'AuthExhaustModule.psm1'
        $exhaustContent = @'
function Get-ModuleStages {
    @{
        'always_401_root' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-Always401'; ApiFamily = 'exo'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'entity_always_401' = @{ Stage = 'always_401_root'; WritesTo = 'root'; SelectFields = @('id') }
    }
}
function Get-Always401 {
    param([hashtable]$Context, $Writer)
    throw [System.Net.Http.HttpRequestException]::new('Response status code does not indicate success: 401 (Unauthorized).', $null, [System.Net.HttpStatusCode]::Unauthorized)
}
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-Always401
'@
        [System.IO.File]::WriteAllText($authExhaustFile, $exhaustContent)

        $script:throttleRetryFile = Join-Path $tempDir.FullName 'ThrottleRetryModule.psm1'
        $throttleContent = @'
$script:CallCount = 0

function Get-ModuleStages {
    @{
        'throttle_root' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-ThrottleRoot'; ApiFamily = 'exo'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'entity_throttle' = @{ Stage = 'throttle_root'; WritesTo = 'root'; SelectFields = @('id') }
    }
}
function Get-ThrottleRoot {
    param([hashtable]$Context, $Writer)
    $script:CallCount++
    if ($script:CallCount -le 1) {
        throw [System.Net.Http.HttpRequestException]::new('Response status code does not indicate success: 429 (TooManyRequests).', $null, [System.Net.HttpStatusCode]::TooManyRequests)
    }
    $Writer.WriteRecord(@{ id = 't1' })
}
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-ThrottleRoot
'@
        [System.IO.File]::WriteAllText($throttleRetryFile, $throttleContent)

        $script:nonRetryableFile = Join-Path $tempDir.FullName 'NonRetryableModule.psm1'
        $nrContent = @'
function Get-ModuleStages {
    @{
        'nr_root' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-NrRoot'; ApiFamily = 'graph'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'entity_nr' = @{ Stage = 'nr_root'; WritesTo = 'root'; SelectFields = @('id') }
    }
}
function Get-NrRoot {
    param([hashtable]$Context, $Writer)
    throw [System.Net.Http.HttpRequestException]::new('Response status code does not indicate success: 400 (BadRequest).', $null, [System.Net.HttpStatusCode]::BadRequest)
}
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-NrRoot
'@
        [System.IO.File]::WriteAllText($nonRetryableFile, $nrContent)
    }

    It 'recovers from a 401 on the first attempt via Restore-ServiceConnection' {
        $results = Invoke-ModuleRun -ModulePath $authRetryFile -RequestedEntities @('entity_auth') -Context $baseContext
        $r = $results | Where-Object { $_.EntityName -eq 'entity_auth' }
        $r.Status      | Should -Be 'success'
        $r.RecordCount | Should -Be 1
    }

    It 'fails after exhausting auth retries when reconnect cannot fix the issue' {
        $results = Invoke-ModuleRun -ModulePath $authExhaustFile -RequestedEntities @('entity_always_401') -Context $baseContext
        $r = $results | Where-Object { $_.EntityName -eq 'entity_always_401' }
        $r.Status | Should -Be 'failed'
        $r.Errors[0] | Should -Match '401'
    }

    It 'recovers from a 429 throttle on the first attempt with backoff' {
        $results = Invoke-ModuleRun -ModulePath $throttleRetryFile -RequestedEntities @('entity_throttle') -Context $baseContext
        $r = $results | Where-Object { $_.EntityName -eq 'entity_throttle' }
        $r.Status      | Should -Be 'success'
        $r.RecordCount | Should -Be 1
    }

    It 'fails immediately on NonRetryable (400) without retrying' {
        $results = Invoke-ModuleRun -ModulePath $nonRetryableFile -RequestedEntities @('entity_nr') -Context $baseContext
        $r = $results | Where-Object { $_.EntityName -eq 'entity_nr' }
        $r.Status | Should -Be 'failed'
        $r.Errors[0] | Should -Match '400'
    }

    It 'does not retry when records were already written (partial output guard)' {
        $partialFile = Join-Path $tempDir.FullName 'PartialModule.psm1'
        $partialContent = @'
function Get-ModuleStages {
    @{
        'partial_root' = @{ InputFrom = $null; RunsOnPool = $false; Function = 'Get-PartialRoot'; ApiFamily = 'exo'; MinimumSelectFields = @('id') }
    }
}
function Get-ModuleEntities {
    @{
        'entity_partial' = @{ Stage = 'partial_root'; WritesTo = 'root'; SelectFields = @('id') }
    }
}
function Get-PartialRoot {
    param([hashtable]$Context, $Writer)
    $Writer.WriteRecord(@{ id = 'p1' })
    throw [System.Net.Http.HttpRequestException]::new('Response status code does not indicate success: 401 (Unauthorized).', $null, [System.Net.HttpStatusCode]::Unauthorized)
}
Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-PartialRoot
'@
        [System.IO.File]::WriteAllText($partialFile, $partialContent)
        $results = Invoke-ModuleRun -ModulePath $partialFile -RequestedEntities @('entity_partial') -Context $baseContext
        $r = $results | Where-Object { $_.EntityName -eq 'entity_partial' }
        $r.Status | Should -Be 'failed'
        $r.Errors[0] | Should -Match '401'
    }
}
