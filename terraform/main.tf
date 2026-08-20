resource "random_string" "kv" {
  length  = 4
  special = false
  upper   = false
  numeric = true
}

resource "random_string" "storage" {
  length  = 6
  special = false
  numeric = true
  upper   = false
}

resource "random_string" "cosmos" {
  length  = 6
  special = false
  numeric = true
  upper   = false
}

resource "random_string" "acr" {
  length  = 6
  special = false
  numeric = true
  upper   = false
}

resource "azurerm_resource_group" "wpp_cloud" {
  name     = local.resource_names.rg
  location = var.location
  tags     = local.common_tags
}

/*resource "azurerm_user_assigned_identity" "automation" {
  name                = "${local.resource_prefix}-id-automation-01"
  location            = azurerm_resource_group.wpp_cloud.location
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  tags                = local.common_tags
}

module "app_registration" {
  source                            = "./automation_workspace_code/modules/app-registration"
  resource_prefix                   = local.resource_prefix
  app_registration_sign_in_audience = var.app_registration_sign_in_audience
  dispatcher_function_app_name      = local.resource_names.bot_func
  dispatcher_app_reg_name           = local.resource_names.bot_name
}*/

module "network" {
  source = "./automation_workspace_code/modules/network"

  resource_group_name = azurerm_resource_group.wpp_cloud.name
  location            = azurerm_resource_group.wpp_cloud.location
  tags                = local.common_tags
  resource_prefix     = local.resource_prefix
  vnet_address_space  = var.vnet_address_space
  subnet_prefixes     = var.subnet_prefixes
}

module "windows_vms" {
  source = "./automation_workspace_code/modules/windows-vm"

  resource_group_name                 = "${local.resource_prefix}-rg-vm-01"
  location                            = azurerm_resource_group.wpp_cloud.location
  application_resource_prefix         = local.resource_prefix
  subnet_id                           = module.network.windows_vm_subnet_id
  vm_size                             = var.windows_vm_size
  admin_username                      = var.windows_vm_admin_username
  user_principal_names                = var.windows_vm_user_principal_names
  jit_allowed_source_address_prefixes = var.windows_vm_jit_allowed_source_address_prefixes
  virtual_machines = {
    vm01 = {
      name          = "${local.resource_prefix}-vm-01"
      computer_name = "wppnpvm01"
      zone          = "1"
    }
    vm02 = {
      name          = "${local.resource_prefix}-vm-02"
      computer_name = "wppnpvm02"
      zone          = "2"
    }
  }
  tags = local.common_tags
}
/*
module "keyvault" {
  source = "./automation_workspace_code/modules/keyvault"

  resource_group_name           = azurerm_resource_group.wpp_cloud.name
  location                      = azurerm_resource_group.wpp_cloud.location
  tenant_id                     = var.tenant_id
  resource_prefix               = local.resource_prefix
  kv_name                       = local.resource_names.kv_name
  private_endpoints_subnet_id   = module.network.private_endpoints_subnet_id
  private_dns_zone_keyvault_id  = module.network.private_dns_zone_keyvault_id
  public_network_access_enabled = var.public_network_access_enabled
  tags                          = local.common_tags
}

resource "azurerm_key_vault_secret" "managed" {
  for_each     = var.key_vault_secrets
  name         = each.key
  value        = each.value
  key_vault_id = module.keyvault.key_vault_id
}

module "automation" {
  source = "./automation_workspace_code/modules/automation"

  resource_group_name   = azurerm_resource_group.wpp_cloud.name
  location              = azurerm_resource_group.wpp_cloud.location
  resource_prefix       = local.resource_prefix
  automation_name       = local.resource_names.automation
  identity_id           = azurerm_user_assigned_identity.automation.id
  identity_principal_id = azurerm_user_assigned_identity.automation.principal_id
  enable_hybrid_worker  = var.enable_hybrid_worker
  tags                  = local.common_tags
}

module "monitoring" {
  source = "./automation_workspace_code/modules/monitoring"

  depends_on = [module.log_analytics]

  resource_group_name        = azurerm_resource_group.wpp_cloud.name
  location                   = azurerm_resource_group.wpp_cloud.location
  workspace_name             = local.resource_names.log_analytics
  log_analytics_workspace_id = module.log_analytics.workspace_id
  app_insights_name          = local.resource_names.app_insights
  retention_in_days          = var.diagnostic_log_retention_days
  tags                       = local.common_tags
}*/

/*resource "azurerm_storage_account" "functions" {
  name                     = local.resource_names.storage
  resource_group_name      = azurerm_resource_group.wpp_cloud.name
  location                 = azurerm_resource_group.wpp_cloud.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
  tags                     = local.common_tags
}

resource "azurerm_storage_container" "function_deployments" {
  name                  = "function-deployments"
  storage_account_id    = azurerm_storage_account.functions.id
  container_access_type = "private"
}*/

/*module "dispatcher_storage" {
  source = "./automation_workspace_code/modules/storage_account"

  depends_on = [azurerm_role_assignment.terraform_storage_blob_data]

  name                = var.dispatcher_storage_name
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  location            = azurerm_resource_group.wpp_cloud.location
  is_hns_enabled      = false
  tags                = var.tags
}

resource "azurerm_role_assignment" "terraform_storage_blob_data" {
  scope                = azurerm_resource_group.wpp_cloud.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_storage_container" "dispatcher_deployment" {
  name               = "deployment"
  storage_account_id = module.dispatcher_storage.id
}

resource "azurerm_service_plan" "main" {
  name                = local.resource_names.plan
  location            = azurerm_resource_group.wpp_cloud.location
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  os_type             = "Linux"
  sku_name            = "FC1"
  tags                = local.common_tags
}

resource "azurerm_service_plan" "email" {
  name                = "${local.resource_prefix}-asp-email-01"
  location            = azurerm_resource_group.wpp_cloud.location
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  os_type             = "Linux"
  sku_name            = "FC1"
  tags                = local.common_tags
}

resource "azurerm_service_plan" "bot" {
  name                = "${local.resource_prefix}-asp-bot-01"
  location            = azurerm_resource_group.wpp_cloud.location
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  os_type             = "Linux"
  sku_name            = "FC1"
  tags                = local.common_tags
}

module "function_app_teams" {
  source = "./automation_workspace_code/modules/function-app"

  name                           = local.resource_names.teams_func
  resource_group_name            = azurerm_resource_group.wpp_cloud.name
  location                       = azurerm_resource_group.wpp_cloud.location
  service_plan_id                = azurerm_service_plan.main.id
  deployment_container_endpoint  = "${module.dispatcher_storage.primary_blob_endpoint}${azurerm_storage_container.dispatcher_deployment.name}"
  runtime_name                   = var.function_runtime
  runtime_version                = var.function_runtime_version
  app_insights_connection_string = module.monitoring.application_insights_connection_string
  key_vault_name                 = module.keyvault.key_vault_name
  subscription_id                = local.subscription_id
  webjobs_storage_account_name   = module.dispatcher_storage.name
  virtual_network_subnet_id      = module.network.app_subnet_id
  tags                           = local.common_tags
}

module "function_app_email" {
  source = "./automation_workspace_code/modules/function-app"

  name                           = local.resource_names.email_func
  resource_group_name            = azurerm_resource_group.wpp_cloud.name
  location                       = azurerm_resource_group.wpp_cloud.location
  service_plan_id                = azurerm_service_plan.email.id
  deployment_container_endpoint  = "${module.dispatcher_storage.primary_blob_endpoint}${azurerm_storage_container.dispatcher_deployment.name}"
  runtime_name                   = var.function_runtime
  runtime_version                = var.function_runtime_version
  app_insights_connection_string = module.monitoring.application_insights_connection_string
  key_vault_name                 = module.keyvault.key_vault_name
  subscription_id                = local.subscription_id
  webjobs_storage_account_name   = module.dispatcher_storage.name
  virtual_network_subnet_id      = module.network.app_subnet_id
  tags                           = local.common_tags
}

module "function_app_bot" {
  source = "./automation_workspace_code/modules/function-app"

  name                           = local.resource_names.bot_func
  resource_group_name            = azurerm_resource_group.wpp_cloud.name
  location                       = azurerm_resource_group.wpp_cloud.location
  service_plan_id                = azurerm_service_plan.bot.id
  deployment_container_endpoint  = "${module.dispatcher_storage.primary_blob_endpoint}${azurerm_storage_container.dispatcher_deployment.name}"
  runtime_name                   = var.function_runtime
  runtime_version                = var.function_runtime_version
  app_insights_connection_string = module.monitoring.application_insights_connection_string
  key_vault_name                 = module.keyvault.key_vault_name
  subscription_id                = local.subscription_id
  webjobs_storage_account_name   = module.dispatcher_storage.name
  virtual_network_subnet_id      = module.network.app_subnet_id
  additional_app_settings = {
    "MICROSOFT_APP_ID"  = var.bot_microsoft_app_id != null ? var.bot_microsoft_app_id : module.app_registration.bot_app_client_id
    "BOT_ENDPOINT_PATH" = var.bot_endpoint_path
  }
  auth_aad_client_id = module.app_registration.bot_app_client_id
  auth_aad_tenant_id = var.tenant_id
  auth_extra_allowed_audiences = [
    "api://${var.tenant_id}/${var.dispatcher_app_reg_name}"
  ]
  tags = local.common_tags
}

module "bot_service" {
  source = "./automation_workspace_code/modules/bot-service"

  resource_group_name = azurerm_resource_group.wpp_cloud.name
  location            = azurerm_resource_group.wpp_cloud.location
  bot_name            = local.resource_names.bot_name
  microsoft_app_id    = var.bot_microsoft_app_id != null ? var.bot_microsoft_app_id : module.app_registration.bot_app_client_id
  endpoint_url        = "https://${module.function_app_bot.default_hostname}${var.bot_endpoint_path}"
  description         = "Teams bot for the WPP Cloud automation platform"
  tags                = local.common_tags
}
module "cosmosdb" {
  source = "./automation_workspace_code/modules/cosmosdb"

  resource_group_name           = azurerm_resource_group.wpp_cloud.name
  location                      = azurerm_resource_group.wpp_cloud.location
  resource_prefix               = local.resource_prefix
  account_name                  = local.resource_names.cosmos_name
  offer_type                    = var.cosmos_offer_type
  kind                          = var.cosmos_kind
  consistency_level             = var.cosmos_consistency_level
  capacity_mode                 = var.cosmos_capacity_mode
  throughput                    = var.cosmos_throughput
  public_network_access_enabled = var.public_network_access_enabled
  private_endpoints_subnet_id   = module.network.private_endpoints_subnet_id
  private_dns_zone_cosmos_id    = module.network.private_dns_zone_cosmos_id
  tags                          = local.common_tags
}

module "acr" {
  source = "./automation_workspace_code/modules/container_registry"

  name                          = local.resource_names.acr
  resource_group_name           = azurerm_resource_group.wpp_cloud.name
  location                      = azurerm_resource_group.wpp_cloud.location
  sku                           = "Basic"
  public_network_access_enabled = true
  tags                          = local.common_tags
}

module "log_analytics" {
  source = "./automation_workspace_code/modules/log-analytics"

  name                = local.resource_names.container_app_log_analytics
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  location            = azurerm_resource_group.wpp_cloud.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.common_tags
}

module "container_app" {
  source = "./automation_workspace_code/modules/container-app"

  depends_on = [module.log_analytics]

  resource_group_name        = azurerm_resource_group.wpp_cloud.name
  location                   = azurerm_resource_group.wpp_cloud.location
  environment_name           = local.resource_names.container_app_environment
  log_analytics_workspace_id = module.log_analytics.workspace_id
  app_name                   = local.resource_names.container_app
  image                      = "mcr.microsoft.com/azuredocs/containerapps-helloworld:latest"
  target_port                = 80
  external_enabled           = true
  min_replicas               = 1
  max_replicas               = 2
  env_vars = {
    "ASPNETCORE_ENVIRONMENT" = "Production"
  }
  tags = local.common_tags
}

resource "azurerm_role_assignment" "kv_secrets_user_teams" {
  scope                = module.keyvault.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = module.function_app_teams.principal_id
}

resource "azurerm_role_assignment" "kv_secrets_user_email" {
  scope                = module.keyvault.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = module.function_app_email.principal_id
}

resource "azurerm_role_assignment" "kv_secrets_user_bot" {
  scope                = module.keyvault.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = module.function_app_bot.principal_id
}

resource "azurerm_role_assignment" "automation_kv" {
  scope                = module.keyvault.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = module.automation.automation_principal_id
}

resource "azurerm_role_assignment" "container_app_acr_pull" {
  scope                = module.acr.acr_id
  role_definition_name = "AcrPull"
  principal_id         = module.container_app.container_app_principal_id
}

resource "azurerm_cosmosdb_sql_role_assignment" "cosmos_function_teams" {
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  account_name        = module.cosmosdb.cosmos_account_name
  scope               = module.cosmosdb.cosmos_account_id
  role_definition_id  = "${module.cosmosdb.cosmos_account_id}/sqlRoleDefinitions/00000000-0000-0000-0000-000000000002"
  principal_id        = module.function_app_teams.principal_id
}

resource "azurerm_cosmosdb_sql_role_assignment" "cosmos_function_email" {
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  account_name        = module.cosmosdb.cosmos_account_name
  scope               = module.cosmosdb.cosmos_account_id
  role_definition_id  = "${module.cosmosdb.cosmos_account_id}/sqlRoleDefinitions/00000000-0000-0000-0000-000000000002"
  principal_id        = module.function_app_email.principal_id
}

resource "azurerm_cosmosdb_sql_role_assignment" "cosmos_function_bot" {
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  account_name        = module.cosmosdb.cosmos_account_name
  scope               = module.cosmosdb.cosmos_account_id
  role_definition_id  = "${module.cosmosdb.cosmos_account_id}/sqlRoleDefinitions/00000000-0000-0000-0000-000000000002"
  principal_id        = module.function_app_bot.principal_id
}*/
