#Requires -Modules @{ ModuleName='Pester'; ModuleVersion='5.0' }

# Tests for Invoke-EnvOnboarding role management (#564): Path B must revoke the
# System Administrator that /addAppUser grants even when the Service Reader bind
# throws (crash window), and Path A must retroactively revoke a stale
# System Administrator left by a crashed prior Path B run (self-heal).

BeforeAll {
    $script:Scripts = Join-Path $PSScriptRoot '..' 'scripts'
    # Connect.psm1 defines Get-PowerPlatToken so Pester can Mock it.
    Import-Module (Join-Path $script:Scripts 'Connect.psm1')                        -Force -DisableNameChecking
    Import-Module (Join-Path $script:Scripts 'entities' 'PowerPlatEnvironments.psm1') -Force -DisableNameChecking
}

AfterAll {
    Remove-Module PowerPlatEnvironments, Connect -Force -ErrorAction SilentlyContinue
}

Describe 'Invoke-EnvOnboarding role management' {

    Context 'Path B — new SP bootstrap' {
        BeforeEach {
            InModuleScope 'PowerPlatEnvironments' {
                # WhoAmI 403 + 0x80072560 gates Path B.
                Mock Get-PowerPlatToken { 'fake-token' }
                Mock Invoke-RestMethod {
                    $exn = [System.Exception]::new('403 Forbidden')
                    $exn | Add-Member -NotePropertyName Response -NotePropertyValue ([pscustomobject]@{ StatusCode = 403 }) -Force
                    $rec = [System.Management.Automation.ErrorRecord]::new(
                        $exn, 'HttpError',
                        [System.Management.Automation.ErrorCategory]::PermissionDenied, $null)
                    $rec.ErrorDetails = [System.Management.Automation.ErrorDetails]::new(
                        '{"error":{"code":"0x80072560","message":"user is not a member of the organization"}}')
                    throw $rec
                }
                Mock Invoke-EnvBootstrap { 'sysuser-guid' }
                Mock Get-DataverseRoleId {
                    if ($RoleName -eq 'Service Reader') { 'sr-role-id' } else { 'sa-role-id' }
                }
                Mock Set-DataverseUserRole { }
                Mock Remove-DataverseUserRole { }
            }
        }

        It 'revokes System Administrator on the happy path' {
            InModuleScope 'PowerPlatEnvironments' {
                $row = Invoke-EnvOnboarding -EnvName 'env1' `
                    -InstanceUrl 'https://org.crm.dynamics.com' `
                    -InstanceApiUrl 'https://org.api.crm.dynamics.com' `
                    -ClientId 'client-guid'

                $row.created_new | Should -BeTrue
                $row.role_name   | Should -Be 'Service Reader'
                Should -Invoke Remove-DataverseUserRole -Times 1 -Exactly -ParameterFilter {
                    $RoleId -eq 'sa-role-id'
                }
            }
        }

        It 'still revokes System Administrator when the Service Reader bind throws (crash window)' {
            InModuleScope 'PowerPlatEnvironments' {
                Mock Set-DataverseUserRole { throw 'transient Dataverse 500 during Service Reader bind' }

                { Invoke-EnvOnboarding -EnvName 'env1' `
                    -InstanceUrl 'https://org.crm.dynamics.com' `
                    -InstanceApiUrl 'https://org.api.crm.dynamics.com' `
                    -ClientId 'client-guid' } | Should -Throw

                # finally block guarantees the SysAdmin revoke fires anyway.
                Should -Invoke Remove-DataverseUserRole -Times 1 -Exactly -ParameterFilter {
                    $RoleId -eq 'sa-role-id'
                }
            }
        }

        It 'still revokes System Administrator when the Service Reader role lookup throws' {
            InModuleScope 'PowerPlatEnvironments' {
                Mock Get-DataverseRoleId {
                    if ($RoleName -eq 'Service Reader') { throw 'Service Reader role lookup failed' }
                    'sa-role-id'
                }

                { Invoke-EnvOnboarding -EnvName 'env1' `
                    -InstanceUrl 'https://org.crm.dynamics.com' `
                    -InstanceApiUrl 'https://org.api.crm.dynamics.com' `
                    -ClientId 'client-guid' } | Should -Throw

                Should -Invoke Remove-DataverseUserRole -Times 1 -Exactly -ParameterFilter {
                    $RoleId -eq 'sa-role-id'
                }
            }
        }
    }

    Context 'Path A — SP already onboarded' {
        BeforeEach {
            InModuleScope 'PowerPlatEnvironments' {
                Mock Get-PowerPlatToken { 'fake-token' }
                Mock Invoke-RestMethod { [pscustomobject]@{ UserId = 'sysuser-guid' } }  # WhoAmI 200
                Mock Get-DataverseRoleId {
                    if ($RoleName -eq 'Service Reader') { 'sr-role-id' } else { 'sa-role-id' }
                }
                Mock Set-DataverseUserRole { }
                Mock Remove-DataverseUserRole { }
            }
        }

        It 'retroactively revokes a stale System Administrator binding (self-heal)' {
            InModuleScope 'PowerPlatEnvironments' {
                Mock Test-DataverseUserHasRole { $true }

                $row = Invoke-EnvOnboarding -EnvName 'env1' `
                    -InstanceUrl 'https://org.crm.dynamics.com' `
                    -InstanceApiUrl 'https://org.api.crm.dynamics.com' `
                    -ClientId 'client-guid'

                $row.created_new | Should -BeFalse
                Should -Invoke Remove-DataverseUserRole -Times 1 -Exactly -ParameterFilter {
                    $RoleId -eq 'sa-role-id'
                }
            }
        }

        It 'does not revoke when no stale System Administrator is present' {
            InModuleScope 'PowerPlatEnvironments' {
                Mock Test-DataverseUserHasRole { $false }

                Invoke-EnvOnboarding -EnvName 'env1' `
                    -InstanceUrl 'https://org.crm.dynamics.com' `
                    -InstanceApiUrl 'https://org.api.crm.dynamics.com' `
                    -ClientId 'client-guid' | Out-Null

                Should -Invoke Remove-DataverseUserRole -Times 0 -Exactly
            }
        }
    }
}
