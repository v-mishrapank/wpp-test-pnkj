resource "azurerm_function_app_flex_consumption" "this" {
  name                = var.name
  location            = var.location
  resource_group_name = var.resource_group_name
  service_plan_id     = var.service_plan_id

  storage_container_type      = "blobContainer"
  storage_container_endpoint  = var.deployment_container_endpoint
  storage_authentication_type = "SystemAssignedIdentity"

  runtime_name    = var.runtime_name
  runtime_version = var.runtime_version

  instance_memory_in_mb  = var.instance_memory_in_mb
  maximum_instance_count = var.maximum_instance_count

  virtual_network_subnet_id = var.virtual_network_subnet_id

  public_network_access_enabled = var.public_network_access_enabled

  webdeploy_publish_basic_authentication_enabled = false

  https_only = true
  tags       = var.tags

  identity {
    type = "SystemAssigned"
  }
  site_config {
    application_insights_connection_string = var.app_insights_connection_string
  }

  app_settings = merge({
    "AzureWebJobsStorage"                  = ""
    "DEPLOYMENT_STORAGE_CONNECTION_STRING" = ""
    "AzureWebJobsStorage__accountName"     = var.webjobs_storage_account_name
  }, var.additional_app_settings)

  /*dynamic "auth_settings_v2" {
    for_each = var.auth_aad_client_id == null ? [] : [1]
    content {
      auth_enabled                            = true
      require_authentication                  = true
      unauthenticated_action                  = var.auth_unauthenticated_action
      default_provider                        = "azureactivedirectory"
      require_https                           = true
      forward_proxy_convention                = "NoProxy"
      http_route_api_prefix                   = "/.auth"
      excluded_paths                          = []
      runtime_version                         = "~1"
      config_file_path                        = null
      forward_proxy_custom_host_header_name   = null
      forward_proxy_custom_scheme_header_name = null

      active_directory_v2 {
        client_id                   = var.auth_aad_client_id
        tenant_auth_endpoint        = "https://login.microsoftonline.com/${var.auth_aad_tenant_id}/v2.0"
        www_authentication_disabled = false
        allowed_audiences           = concat(["api://${var.auth_aad_client_id}"], var.auth_extra_allowed_audiences)
        allowed_applications        = var.auth_allowed_applications
      }

      login {
        token_store_enabled = true
      }
    }
  }*/
}
