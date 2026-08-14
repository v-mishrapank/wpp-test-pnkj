variable "workspace_config" {
  type = list(object(
    {
      project_name          = optional(string, "")
      workspace_name        = string
      path                  = optional(string, "")
      workspace_description = optional(string)
      trigger_patterns      = optional(list(string), [])
      auto_apply            = optional(bool)
      working_directory     = optional(string)
      agent_pool_id         = optional(string)
      execution_mode        = optional(string, "remote")
      workspace_variables = optional(list(object({
        variable_key         = string
        variable_value       = string
        variable_description = optional(string, "")
        variable_sensitivity = optional(bool)
        variable_type = string })
      ), [])
      workspace_tags = optional(list(string), [])
      spn_name       = optional(string)
      spn_client_id  = optional(string)
      vcs_repo_configuration = optional(object(
        {
          organization        = string
          name                = string
          branch              = optional(string, "main")
          speculative_enabled = optional(bool, false)
          trigger_patterns    = optional(list(string), [])
          working_directory   = optional(string, "/")
      }), null)
      tfc_azure_oidc_configuration = optional(object({
        azure_tenant_id       = optional(string)
        azure_provider_auth   = optional(bool, false)
        azure_subscription_id = optional(string)
        azure_run_client_id   = optional(string)
        azure_plan_client_id  = optional(string)
        azure_apply_client_id = optional(string)
      }), {})
  }))
}

variable "tfc_project_name" {
  type        = string
  description = "Project name for this workspace to be deployed to. This is automatically provided from a workspace variable."
}

