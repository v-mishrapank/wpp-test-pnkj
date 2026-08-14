resource "azurerm_linux_function_app" "main" {
  name                        = var.function_app_name
  location                    = var.location
  resource_group_name         = var.resource_group_name
  service_plan_id             = var.service_plan_id
  storage_account_name        = var.storage_account_name
  storage_account_access_key  = var.storage_account_access_key
  https_only                  = true
  functions_extension_version = "~4"
  tags                        = var.tags

  identity {
    type = "SystemAssigned"
  }

  virtual_network_subnet_id = var.subnet_id

  site_config {
    application_stack {
      python_version = var.runtime_version
    }
    ftps_state             = "FtpsOnly"
    vnet_route_all_enabled = true
  }

  app_settings = merge({
    "FUNCTIONS_WORKER_RUNTIME"                           = var.runtime
    "FUNCTIONS_EXTENSION_VERSION"                        = "~4"
    "AzureWebJobsStorage"                                = "DefaultEndpointsProtocol=https;AccountName=${var.storage_account_name};AccountKey=${var.storage_account_access_key};EndpointSuffix=core.windows.net"
    "WEBSITE_RUN_FROM_PACKAGE"                           = "1"
    "APPLICATIONINSIGHTS_CONNECTION_STRING"              = var.application_insights_connection_string
    "KEY_VAULT_URI"                                      = var.key_vault_uri
    "WEBSITE_CONTENTOVERVNET"                            = "1"
    "WEBSITES_PORT"                                      = "443"
    "WEBSITE_DNS_SERVER"                                 = "168.63.129.16"
    "WEBSITE_VNET_ROUTE_ALL"                             = "1"
    "AzureFunctionsJobHost__logging__console__isEnabled" = "true"
  }, var.app_settings)
}
