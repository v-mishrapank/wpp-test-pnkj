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

resource "azurerm_user_assigned_identity" "automation" {
  name                = "${local.resource_prefix}-id-automation-01"
  location            = azurerm_resource_group.wpp_cloud.location
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  tags                = local.common_tags
}

module "app_registration" {
  source = "./automation_workspace_code/modules/app-registration"

  resource_prefix                   = local.resource_prefix
  app_registration_sign_in_audience = var.app_registration_sign_in_audience
}

module "network" {
  source = "./automation_workspace_code/modules/network"

  resource_group_name = azurerm_resource_group.wpp_cloud.name
  location            = azurerm_resource_group.wpp_cloud.location
  tags                = local.common_tags
  resource_prefix     = local.resource_prefix
  vnet_address_space  = var.vnet_address_space
  subnet_prefixes     = var.subnet_prefixes
}

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

  resource_group_name  = azurerm_resource_group.wpp_cloud.name
  location             = azurerm_resource_group.wpp_cloud.location
  resource_prefix      = local.resource_prefix
  automation_name      = local.resource_names.automation
  identity_id          = azurerm_user_assigned_identity.automation.id
  enable_hybrid_worker = var.enable_hybrid_worker
  tags                 = local.common_tags
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
}

resource "azurerm_storage_account" "functions" {
  name                     = local.resource_names.storage
  resource_group_name      = azurerm_resource_group.wpp_cloud.name
  location                 = azurerm_resource_group.wpp_cloud.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
  tags                     = local.common_tags
}

resource "azurerm_service_plan" "main" {
  name                = local.resource_names.plan
  location            = azurerm_resource_group.wpp_cloud.location
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  os_type             = "Linux"
  sku_name            = "P1v3"
  tags                = local.common_tags
}

module "function_app_teams" {
  source = "./automation_workspace_code/modules/function-app"

  resource_group_name                    = azurerm_resource_group.wpp_cloud.name
  location                               = azurerm_resource_group.wpp_cloud.location
  resource_prefix                        = local.resource_prefix
  service_plan_id                        = azurerm_service_plan.main.id
  storage_account_name                   = azurerm_storage_account.functions.name
  storage_account_access_key             = azurerm_storage_account.functions.primary_access_key
  function_app_name                      = local.resource_names.teams_func
  subnet_id                              = module.network.app_subnet_id
  application_insights_connection_string = module.monitoring.application_insights_connection_string
  key_vault_uri                          = module.keyvault.key_vault_uri
  runtime                                = var.function_runtime
  runtime_version                        = var.function_runtime_version
  tags                                   = local.common_tags
}

module "function_app_email" {
  source = "./automation_workspace_code/modules/function-app"

  resource_group_name                    = azurerm_resource_group.wpp_cloud.name
  location                               = azurerm_resource_group.wpp_cloud.location
  resource_prefix                        = local.resource_prefix
  service_plan_id                        = azurerm_service_plan.main.id
  storage_account_name                   = azurerm_storage_account.functions.name
  storage_account_access_key             = azurerm_storage_account.functions.primary_access_key
  function_app_name                      = local.resource_names.email_func
  subnet_id                              = module.network.app_subnet_id
  application_insights_connection_string = module.monitoring.application_insights_connection_string
  key_vault_uri                          = module.keyvault.key_vault_uri
  runtime                                = var.function_runtime
  runtime_version                        = var.function_runtime_version
  tags                                   = local.common_tags
}

/*module "function_app_bot" {
  source = "./automation_workspace_code/modules/function-app"

  resource_group_name                    = azurerm_resource_group.wpp_cloud.name
  location                               = azurerm_resource_group.wpp_cloud.location
  resource_prefix                        = local.resource_prefix
  service_plan_id                        = azurerm_service_plan.main.id
  storage_account_name                   = azurerm_storage_account.functions.name
  storage_account_access_key             = azurerm_storage_account.functions.primary_access_key
  function_app_name                      = local.resource_names.bot_func
  subnet_id                              = module.network.app_subnet_id
  application_insights_connection_string = module.monitoring.application_insights_connection_string
  key_vault_uri                          = module.keyvault.key_vault_uri
  runtime                                = var.function_runtime
  runtime_version                        = var.function_runtime_version
  app_settings = {
    "MICROSOFT_APP_ID"  = var.bot_microsoft_app_id != null ? var.bot_microsoft_app_id : module.app_registration.bot_app_client_id
    "BOT_ENDPOINT_PATH" = var.bot_endpoint_path
  }
  tags = local.common_tags
}*/

/*module "bot_service" {
  source = "./automation_workspace_code/modules/bot-service"

  resource_group_name = azurerm_resource_group.wpp_cloud.name
  location            = azurerm_resource_group.wpp_cloud.location
  bot_name            = local.resource_names.bot_name
  microsoft_app_id    = var.bot_microsoft_app_id != null ? var.bot_microsoft_app_id : module.app_registration.bot_app_client_id
  endpoint_url        = "https://${module.function_app_bot.default_hostname}${var.bot_endpoint_path}"
  description         = "Teams bot for the WPP Cloud automation platform"
  tags                = local.common_tags
}*/

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
  source = "./automation_workspace_code/modules/acr"

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

/*resource "azurerm_role_assignment" "kv_secrets_user_bot" {
  scope                = module.keyvault.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = module.function_app_bot.principal_id
}*/

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

resource "azurerm_role_assignment" "cosmos_function_teams" {
  scope                = module.cosmosdb.cosmos_account_id
  role_definition_name = "Cosmos DB Built-in Data Contributor"
  principal_id         = module.function_app_teams.principal_id
}

resource "azurerm_role_assignment" "cosmos_function_email" {
  scope                = module.cosmosdb.cosmos_account_id
  role_definition_name = "Cosmos DB Built-in Data Contributor"
  principal_id         = module.function_app_email.principal_id
}

/*resource "azurerm_role_assignment" "cosmos_function_bot" {
  scope                = module.cosmosdb.cosmos_account_id
  role_definition_name = "Cosmos DB Built-in Data Contributor"
  principal_id         = module.function_app_bot.principal_id
}*/
