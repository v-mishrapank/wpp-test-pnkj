#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

BeforeAll {
    $modulePath = Join-Path $PSScriptRoot '..' 'modules' 'StageExecutor.psm1'
    Import-Module $modulePath -Force
}

Describe 'Resolve-RequiredStages' {
    BeforeAll {
        # 3-tier chain a -> b -> c plus a separate branch d -> e
        $script:Stages = @{
            'a' = @{ InputFrom = $null }
            'b' = @{ InputFrom = 'a'  }
            'c' = @{ InputFrom = 'b'  }
            'd' = @{ InputFrom = $null }
            'e' = @{ InputFrom = 'd'  }
        }
        $script:Entities = @{
            'entity_a' = @{ Stage = 'a' }
            'entity_b' = @{ Stage = 'b' }
            'entity_c' = @{ Stage = 'c' }
            'entity_d' = @{ Stage = 'd' }
            'entity_e' = @{ Stage = 'e' }
        }
    }

    It 'returns just the root stage when only the root entity is requested' {
        $result = Resolve-RequiredStages -Stages $Stages -Entities $Entities -RequestedEntities @('entity_a')
        $result | Should -Be @('a')
    }

    It 'expands to include all ancestors when a leaf entity is requested' {
        $result = Resolve-RequiredStages -Stages $Stages -Entities $Entities -RequestedEntities @('entity_c')
        ($result | Sort-Object) | Should -Be @('a','b','c')
    }

    It 'deduplicates shared ancestors when multiple entities share a parent' {
        $result = Resolve-RequiredStages -Stages $Stages -Entities $Entities -RequestedEntities @('entity_b','entity_c')
        ($result | Sort-Object) | Should -Be @('a','b','c')
    }

    It 'handles disjoint trees when entities from both are requested' {
        $result = Resolve-RequiredStages -Stages $Stages -Entities $Entities -RequestedEntities @('entity_a','entity_e')
        ($result | Sort-Object) | Should -Be @('a','d','e')
    }

    It 'throws when a requested entity is not declared' {
        { Resolve-RequiredStages -Stages $Stages -Entities $Entities -RequestedEntities @('unknown_entity') } |
            Should -Throw "*not declared by this module*"
    }

    It 'throws when an entity maps to a missing stage' {
        $bad = @{ 'orphan' = @{ Stage = 'nonexistent' } }
        { Resolve-RequiredStages -Stages $Stages -Entities $bad -RequestedEntities @('orphan') } |
            Should -Throw "*not declared in Get-ModuleStages*"
    }

    It 'throws when a stage references a missing parent' {
        $bad = @{
            'orphan_stage' = @{ InputFrom = 'missing_parent' }
        }
        $ents = @{ 'orphan_entity' = @{ Stage = 'orphan_stage' } }
        { Resolve-RequiredStages -Stages $bad -Entities $ents -RequestedEntities @('orphan_entity') } |
            Should -Throw "*references missing parent stage*"
    }

    It 'handles an entity mapped to multiple stages (multi-source producer)' {
        # Models ExoGroups-style case: exo_group_members is fed by both DG and UG root stages
        $stages = @{
            'dg_root'    = @{ InputFrom = $null }
            'dg_members' = @{ InputFrom = 'dg_root' }
            'ug_root'    = @{ InputFrom = $null }
            'ug_members' = @{ InputFrom = 'ug_root' }
        }
        $entities = @{
            'members' = @{ Stage = @('dg_members', 'ug_members') }
        }
        $result = Resolve-RequiredStages -Stages $stages -Entities $entities -RequestedEntities @('members')
        ($result | Sort-Object) | Should -Be @('dg_members','dg_root','ug_members','ug_root')
    }
}

Describe 'Resolve-ExecutionOrder' {
    BeforeAll {
        $script:Stages = @{
            'a' = @{ InputFrom = $null }
            'b' = @{ InputFrom = 'a'  }
            'c' = @{ InputFrom = 'b'  }
            'd' = @{ InputFrom = $null }
        }
    }

    It 'emits a root stage alone' {
        $result = Resolve-ExecutionOrder -Stages $Stages -RequiredStages @('a')
        $result | Should -Be @('a')
    }

    It 'places parents before children in a linear chain' {
        $result = Resolve-ExecutionOrder -Stages $Stages -RequiredStages @('a','b','c')
        $result | Should -Be @('a','b','c')
    }

    It 'treats a non-required ancestor as external and emits the child directly' {
        $result = Resolve-ExecutionOrder -Stages $Stages -RequiredStages @('b')
        $result | Should -Be @('b')
    }

    It 'handles two disjoint roots in either order' {
        $result = Resolve-ExecutionOrder -Stages $Stages -RequiredStages @('a','d')
        $result.Count | Should -Be 2
        $result | Should -Contain 'a'
        $result | Should -Contain 'd'
    }

    It 'throws on a cycle' {
        $cyclic = @{
            'x' = @{ InputFrom = 'y' }
            'y' = @{ InputFrom = 'x' }
        }
        { Resolve-ExecutionOrder -Stages $cyclic -RequiredStages @('x','y') } |
            Should -Throw "*Cycle detected*"
    }

    Context 'depth-blocked emission (#352)' {
        # TeamsTeams-shaped graph: 1 root, 3 depth-1 children, 2 depth-2
        # children of one depth-1. Before #352 the resolver could interleave
        # a depth-2 child between depth-1 siblings because HashSet iteration
        # order made a depth-2 stage eligible in the same pass as its
        # parent's siblings. The pool stage-batch assembly walks the
        # resolved order forward and breaks on a different InputFrom, so
        # any interleaving silently destroys batching.
        BeforeAll {
            $script:TeamsLike = @{
                'teams_root'           = @{ InputFrom = $null }
                'team_details'         = @{ InputFrom = 'teams_root' }
                'team_channels'        = @{ InputFrom = 'teams_root' }
                'team_installed_apps'  = @{ InputFrom = 'teams_root' }
                'channel_members'      = @{ InputFrom = 'team_channels' }
                'channel_tabs'         = @{ InputFrom = 'team_channels' }
            }
            $script:TeamsRequired = @(
                'teams_root','team_details','team_channels',
                'team_installed_apps','channel_members','channel_tabs'
            )
        }

        It 'emits every depth-1 stage before any depth-2 stage' {
            $result = Resolve-ExecutionOrder -Stages $TeamsLike -RequiredStages $TeamsRequired
            $depth1 = @('team_details','team_channels','team_installed_apps')
            $depth2 = @('channel_members','channel_tabs')
            # Presence first — otherwise IndexOf returns -1 for a missing
            # stage and the max/min arithmetic can pass spuriously.
            foreach ($s in $depth1 + $depth2) { $result | Should -Contain $s }
            $maxDepth1Idx = ($depth1 | ForEach-Object { [array]::IndexOf($result, $_) } | Measure-Object -Maximum).Maximum
            $minDepth2Idx = ($depth2 | ForEach-Object { [array]::IndexOf($result, $_) } | Measure-Object -Minimum).Minimum
            $maxDepth1Idx | Should -BeLessThan $minDepth2Idx
        }

        It 'keeps same-parent siblings adjacent in the TeamsTeams shape' {
            $result = Resolve-ExecutionOrder -Stages $TeamsLike -RequiredStages $TeamsRequired
            $depth1 = @('team_details','team_channels','team_installed_apps')
            $depth2 = @('channel_members','channel_tabs')
            foreach ($s in $depth1 + $depth2) { $result | Should -Contain $s }

            $depth1Idxs = $depth1 | ForEach-Object { [array]::IndexOf($result, $_) } | Sort-Object
            ($depth1Idxs[2] - $depth1Idxs[0]) | Should -Be 2

            $depth2Idxs = $depth2 | ForEach-Object { [array]::IndexOf($result, $_) } | Sort-Object
            ($depth2Idxs[1] - $depth2Idxs[0]) | Should -Be 1
        }

        It 'keeps each sibling group contiguous when a pass contains children of multiple parents' {
            # Two independent roots, each with two pool children. A single
            # Kahn's pass sees all four children ready at once; depth-blocking
            # alone wouldn't prevent HashSet enumeration from yielding
            # [r1c1, r2c1, r1c2, r2c2]. The grouping-by-InputFrom step is
            # what keeps each sibling pair contiguous so the executor's
            # batch-assembly walk can form them into proper pool batches.
            $twoRoots = @{
                'r1'    = @{ InputFrom = $null }
                'r2'    = @{ InputFrom = $null }
                'r1_c1' = @{ InputFrom = 'r1' }
                'r1_c2' = @{ InputFrom = 'r1' }
                'r2_c1' = @{ InputFrom = 'r2' }
                'r2_c2' = @{ InputFrom = 'r2' }
            }
            $result = Resolve-ExecutionOrder -Stages $twoRoots -RequiredStages @($twoRoots.Keys)
            foreach ($s in $twoRoots.Keys) { $result | Should -Contain $s }

            $r1Idxs = @('r1_c1','r1_c2') | ForEach-Object { [array]::IndexOf($result, $_) } | Sort-Object
            ($r1Idxs[1] - $r1Idxs[0]) | Should -Be 1

            $r2Idxs = @('r2_c1','r2_c2') | ForEach-Object { [array]::IndexOf($result, $_) } | Sort-Object
            ($r2Idxs[1] - $r2Idxs[0]) | Should -Be 1
        }
    }
}

Describe 'Resolve-SelectFields' {
    BeforeAll {
        $script:Stages = @{
            'root' = @{ InputFrom = $null;  MinimumSelectFields = @('id') }
            'leaf' = @{ InputFrom = 'root'; MinimumSelectFields = @('id','membershipType') }
        }
        $script:Entities = @{
            'root_entity' = @{ Stage = 'root'; SelectFields = @('id','displayName','description') }
            # No SelectFields — leaf_entity is raw-emit (the resolver just
            # unions the stage's MinimumSelectFields when it's requested).
            'leaf_entity' = @{ Stage = 'leaf' }
        }
    }

    It 'returns only MinimumSelectFields when the stages entity is not requested' {
        $result = Resolve-SelectFields -StageName 'root' -Stages $Stages -Entities $Entities -RequestedEntities @('leaf_entity')
        ($result | Sort-Object) | Should -Be @('id')
    }

    It 'unions MinimumSelectFields with entity SelectFields when the entity is requested' {
        $result = Resolve-SelectFields -StageName 'root' -Stages $Stages -Entities $Entities -RequestedEntities @('root_entity')
        ($result | Sort-Object) | Should -Be @('description','displayName','id')
    }

    It 'deduplicates overlap between MinimumSelectFields and SelectFields' {
        $result = Resolve-SelectFields -StageName 'root' -Stages $Stages -Entities $Entities -RequestedEntities @('root_entity')
        ($result | Where-Object { $_ -eq 'id' }).Count | Should -Be 1
    }

    It 'returns MinimumSelectFields alone for a stage whose entity was not requested' {
        $result = Resolve-SelectFields -StageName 'leaf' -Stages $Stages -Entities $Entities -RequestedEntities @('root_entity')
        ($result | Sort-Object) | Should -Be @('id','membershipType')
    }

    It 'handles an absent MinimumSelectFields (null)' {
        $stages = @{ 'bare' = @{ InputFrom = $null } }
        $ents   = @{ 'bare_entity' = @{ Stage = 'bare'; SelectFields = @('foo','bar') } }
        $result = Resolve-SelectFields -StageName 'bare' -Stages $stages -Entities $ents -RequestedEntities @('bare_entity')
        ($result | Sort-Object) | Should -Be @('bar','foo')
    }

    It 'picks the per-stage entry when SelectFields is a hashtable keyed by stage' {
        $stages = @{
            'r' = @{ InputFrom = $null;  MinimumSelectFields = @('id') }
            'c' = @{ InputFrom = 'r' }
        }
        $ents   = @{
            'multi' = @{
                Stage        = @('r','c')
                SelectFields = @{ r = @('id','rootA'); c = @('childB') }
            }
        }
        $rootResult  = Resolve-SelectFields -StageName 'r' -Stages $stages -Entities $ents -RequestedEntities @('multi')
        $childResult = Resolve-SelectFields -StageName 'c' -Stages $stages -Entities $ents -RequestedEntities @('multi')
        ($rootResult  | Sort-Object) | Should -Be @('id','rootA')
        ($childResult | Sort-Object) | Should -Be @('childB')
    }

    It 'treats a stage without a hashtable entry as no entity contribution' {
        $stages = @{
            'r' = @{ InputFrom = $null;  MinimumSelectFields = @('id') }
            'c' = @{ InputFrom = 'r' }
        }
        $ents   = @{
            'multi' = @{
                Stage        = @('r','c')
                SelectFields = @{ r = @('id','rootA') }   # no entry for 'c'
            }
        }
        $childResult = Resolve-SelectFields -StageName 'c' -Stages $stages -Entities $ents -RequestedEntities @('multi')
        @($childResult).Count | Should -Be 0
    }
}

Describe 'Resolve-ProjectionFields' {
    It 'returns $null when no requested entity declares SelectFields for the stage' {
        $ents = @{ 'e' = @{ Stage = 'r' } }   # no SelectFields key
        Resolve-ProjectionFields -StageName 'r' -Entities $ents -RequestedEntities @('e') | Should -Be $null
    }

    It 'returns declared fields (entity-only, no MinimumSelectFields) when present' {
        $ents = @{ 'e' = @{ Stage = 'r'; SelectFields = @('a','b') } }
        $result = Resolve-ProjectionFields -StageName 'r' -Entities $ents -RequestedEntities @('e')
        @($result) | Should -Be @('a','b')
    }

    It 'picks the per-stage hashtable entry' {
        $ents = @{ 'e' = @{ Stage = @('r','c'); SelectFields = @{ r = @('a'); c = @('b','d') } } }
        $r = Resolve-ProjectionFields -StageName 'r' -Entities $ents -RequestedEntities @('e')
        $c = Resolve-ProjectionFields -StageName 'c' -Entities $ents -RequestedEntities @('e')
        @($r) | Should -Be @('a')
        @($c) | Should -Be @('b','d')
    }

    It 'returns $null for a hashtable SelectFields whose stage entry is absent' {
        $ents = @{ 'e' = @{ Stage = @('r','c'); SelectFields = @{ r = @('a') } } }
        Resolve-ProjectionFields -StageName 'c' -Entities $ents -RequestedEntities @('e') | Should -Be $null
    }

    It 'deduplicates across multiple requested entities mapping to the same stage' {
        $ents = @{
            'e1' = @{ Stage = 'r'; SelectFields = @('a','b') }
            'e2' = @{ Stage = 'r'; SelectFields = @('b','c') }
        }
        $result = Resolve-ProjectionFields -StageName 'r' -Entities $ents -RequestedEntities @('e1','e2')
        ($result | Sort-Object) | Should -Be @('a','b','c')
    }
}

Describe 'Select-InlineRecordsSoFar (#373)' {
    # Locks the records_so_far selector used by the inline-stage completion
    # path in Invoke-ModuleRun. Production fetchers (e.g. Get-TeamsRoot) gate
    # $Writer.WriteRecord on $Context.WriteRecords but always call EmitId.
    # When the stage runs as a prereq (no requested entity claims it,
    # $writeRecords=$false), $sw.TotalWritten stays 0 even though the fetcher
    # iterated through every input — so the metric has to come from
    # $sw.EmittedIds.Count. Pre-#373 the inline path used TotalWritten
    # unconditionally, which made `teams_root` complete with
    # records_so_far=0 despite emitting 25078 IDs (madev2 run a7ea2c34569e).
    # Calls the exported StageExecutor function directly so a regression in
    # the production logic would actually flip this test red.

    It 'uses TotalWritten when WriteRecords=$true (writing root stage)' {
        Select-InlineRecordsSoFar -WriteRecords $true -TotalWritten 25078 -EmittedIdsCount 25078 | Should -Be 25078
    }

    It 'uses EmittedIds.Count when WriteRecords=$false (prereq stage)' {
        # The team_channels.input_count=25078 from madev2 run a7ea2c34569e:
        # the inline parent teams_root emitted 25078 IDs but did not write
        # records. Pre-fix the heartbeat reported 0.
        Select-InlineRecordsSoFar -WriteRecords $false -TotalWritten 0 -EmittedIdsCount 25078 | Should -Be 25078
    }

    It 'ignores TotalWritten when WriteRecords=$false even if non-zero' {
        # A fetcher that does call WriteRecord while WriteRecords=$false
        # (e.g., forgets the gate) is misbehaving — the discard FlushCallback
        # drains the records and we still treat EmittedIds.Count as the
        # authoritative work signal. Locks in that TotalWritten is not used
        # in the prereq branch.
        Select-InlineRecordsSoFar -WriteRecords $false -TotalWritten 999 -EmittedIdsCount 42 | Should -Be 42
    }

    It 'reports 0 for a fetch that produced nothing (no records, no IDs)' {
        Select-InlineRecordsSoFar -WriteRecords $true -TotalWritten 0 -EmittedIdsCount 0 | Should -Be 0
        Select-InlineRecordsSoFar -WriteRecords $false -TotalWritten 0 -EmittedIdsCount 0 | Should -Be 0
    }
}
