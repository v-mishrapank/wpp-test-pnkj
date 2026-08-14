module "workspaces" {
  for_each                     = { for ws in var.workspace_config : ws.workspace_name => ws }
  source                       = "app.terraform.io/wpp-cloudhub/wpp-wppit-cloudhub-tfcworkspace/tfe"
  version                      = "~>0.3.5"
  project_name                 = each.value.project_name != "" ? each.value.project_name : var.tfc_project_name
  workspace_description        = each.value.workspace_description
  workspace_name               = each.value.workspace_name
  workspace_source             = local.workspace_source
  workspace_variables          = each.value.workspace_variables
  execution_mode               = each.value.execution_mode
  tfc_azure_oidc_configuration = each.value.tfc_azure_oidc_configuration
  vcs_repo_configuration       = each.value.vcs_repo_configuration
  working_directory            = each.value.working_directory
  agent_pool_id                = each.value.agent_pool_id

}
