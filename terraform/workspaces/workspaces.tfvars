
workspace_config = [

  {
    workspace_name        = "example-workspace-1"
    workspace_description = "Resource vending"
    execution_mode        = "remote"
    working_directory     = "terraform/automation_workspace_code"
    vcs_repo_configuration = {
      organization        = "WPPOpen"
      name                = "<repo name here>"
      branch              = "main"
      working_directory   = "terraform/automation_workspace_code"
      trigger_patterns    = ["terraform/automation_workspace_code/**/*"]
      speculative_enabled = true
    }
    tfc_azure_oidc_configuration = {
      azure_provider_auth   = true
      azure_tenant_id       = "3d8820e2-f4eb-46a2-8253-82539d7cc066" # wpp.cloud tenant ID
      azure_subscription_id = "00000000-0000-0000-0000-000000000000" # <output from subscription vending>
      azure_run_client_id   = "00000000-0000-0000-0000-000000000000" # service principal client id
    }
  },

]

//delete me after testing only this line