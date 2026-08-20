
workspace_config = [
  {
    workspace_name        = "example-workspace-1"
    workspace_description = "Resource vending"
    execution_mode        = "remote"
    vcs_repo_configuration = {
      organization        = "WPPOpen"
      name                = "poe-azure-wppit-ucp-nonprod"
      branch              = "main"
      working_directory   = "terraform/example_workspace_code"
      trigger_patterns    = ["terraform/example_workspace_code/**/*"]
      speculative_enabled = true
    }
    tfc_azure_oidc_configuration = {
      azure_provider_auth   = true
      azure_tenant_id       = "3d8820e2-f4eb-46a2-8253-82539d7cc066" # wpp.cloud tenant ID
      azure_subscription_id = "5171ff1d-22be-41a5-937f-da700888755d" # <output from subscription vending>
      azure_run_client_id   = "80b28b4c-c2fd-4d83-9664-86d86f13d24f" # service principal client id
    }
  },
    {
    workspace_name        = "automation-workspace-1"
    workspace_description = "Resource vending"
    execution_mode        = "remote"
    vcs_repo_configuration = {
      organization        = "WPPOpen"
      name                = "poe-azure-wppit-ucp-nonprod"
      branch              = "main"
      working_directory   = "terraform/automation_workspace_code"
      trigger_patterns    = ["terraform/automation_workspace_code/**/*"]
      speculative_enabled = true
    }
    tfc_azure_oidc_configuration = {
      azure_provider_auth   = true
      azure_tenant_id       = "3d8820e2-f4eb-46a2-8253-82539d7cc066" # wpp.cloud tenant ID
      azure_subscription_id = "5171ff1d-22be-41a5-937f-da700888755d" # <output from subscription vending>
      azure_run_client_id   = "80b28b4c-c2fd-4d83-9664-86d86f13d24f" # service principal client id
    }
  },
]