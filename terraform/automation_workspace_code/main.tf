data "azurerm_client_config" "current" {}

data "azuread_client_config" "current" {}

locals {
  storage_private_dns_zones = {
    blob  = "privatelink.blob.core.windows.net"
    queue = "privatelink.queue.core.windows.net"
    table = "privatelink.table.core.windows.net"
    file  = "privatelink.file.core.windows.net"
  }
}

resource "azuread_application" "dispatcher" {
  display_name = var.app_reg_name
  
  sign_in_audience = "AzureADMyOrg"
  owners = [data.azuread_client_config.current.object_id]

  api {
    oauth2_permission_scope {
      id                         = var.dispatcher_user_impersonation_scope_id
      value                      = "user_impersonation"
      type                       = "User"
      enabled                    = true
      admin_consent_display_name = "Access ${var.app_reg_name}"
      admin_consent_description  = "Allow the application to access the dispatcher API on behalf of the signed-in user."
      user_consent_display_name  = "Access ${var.app_reg_name}"
      user_consent_description   = "Allow the application to access the dispatcher API on your behalf."
    }
  }

  web {
    redirect_uris = ["https://${var.analytics_function_app_name}.azurewebsites.net/.auth/login/aad/callback"]
    implicit_grant {
      id_token_issuance_enabled = true
    }
  }
  lifecycle {
    ignore_changes = [identifier_uris]
  }
}



resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location
  tags = local.common_tags
}

resource "azurerm_private_dns_zone" "storage" {
  for_each = local.storage_private_dns_zones

  name                = each.value
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.common_tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "storage" {
  for_each = local.storage_private_dns_zones

  name                  = "${each.key}-${module.network.vnet_name}-link"
  resource_group_name   = azurerm_resource_group.this.name
  private_dns_zone_name = azurerm_private_dns_zone.storage[each.key].name
  virtual_network_id    = module.network.vnet_id
  registration_enabled = false
  tags                  = local.common_tags
}

resource "azurerm_log_analytics_workspace" "this" {
  name                = "law-wpp-analytics-dev-disp-001"
  location            = var.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.common_tags
}


module "network" {
  source = "./modules/network"

  vnet_name           = var.vnet_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  vnet_address_space  = var.vnet_address_space
  dns_servers         = var.dns_servers

  subnets          = var.subnets
  nsgs             = var.nsgs
  nsg_associations = var.nsg_associations
  tags             = merge(local.common_tags, { workloadComponent = "network" })
}

module "storage" {
  source = "./modules/storage"

  storage_name        = var.storage_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  # container_names     = var.container_names
  tags                = local.common_tags
}

module "private_endpoint_disp_storage_blob" {
  source                         = "./modules/private_endpoint"
  name                           = var.wpp_analytics_storage_blob_name
  location                       = var.location
  resource_group_name            = var.resource_group_name
  subnet_id                      = module.network.subnet_ids["private_endpoint"]
  service_connection_name        = "${var.wpp_analytics_storage_blob_name}-psc"
  private_connection_resource_id = module.storage.id
  subresource_names              = ["blob"]
  private_dns_zone_ids           = [azurerm_private_dns_zone.storage["blob"].id]
  tags                           = local.common_tags
}

module "private_endpoint_disp_storage_queue" {
  source                         = "./modules/private_endpoint"
  name                           = var.wpp_analytics_storage_queue_name
  location                       = var.location
  resource_group_name            = var.resource_group_name
  subnet_id                      = module.network.subnet_ids["private_endpoint"]
  service_connection_name        = "${var.wpp_analytics_storage_queue_name}-psc"
  private_connection_resource_id = module.storage.id
  subresource_names              = ["queue"]
  private_dns_zone_ids           = [azurerm_private_dns_zone.storage["queue"].id]
  tags                           = local.common_tags
}

module "private_endpoint_disp_storage_table" {
  source                         = "./modules/private_endpoint"
  name                           = var.wpp_analytics_storage_table_name
  location                       = var.location
  resource_group_name            = var.resource_group_name
  subnet_id                      = module.network.subnet_ids["private_endpoint"]
  service_connection_name        = "${var.wpp_analytics_storage_table_name}-psc"
  private_connection_resource_id = module.storage.id
  subresource_names              = ["table"]
  private_dns_zone_ids           = [azurerm_private_dns_zone.storage["table"].id]
  tags                           = local.common_tags
}

module "private_endpoint_disp_storage_file" {
  source                         = "./modules/private_endpoint"
  name                           = var.wpp_analytics_storage_file_name
  location                       = var.location
  resource_group_name            = var.resource_group_name
  subnet_id                      = module.network.subnet_ids["private_endpoint"]
  service_connection_name        = "${var.wpp_analytics_storage_file_name}-psc"
  private_connection_resource_id = module.storage.id
  subresource_names              = ["file"]
  private_dns_zone_ids           = [azurerm_private_dns_zone.storage["file"].id]
  tags                           = local.common_tags
}

module "app_plan" {
  source              = "./modules/app_service_plan"
  name                = var.app_service_plan_name
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = local.common_tags
}

module "function_app_insights" {
  source                     = "./modules/application_insights"
  name                       = var.function_app_insights_name
  resource_group_name        = var.resource_group_name
  location                   = var.location
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id
  tags                       = local.common_tags
}
/*
module "analytics_function_app" {
  source                         = "./modules/function_app"
  name                           = var.analytics_function_app_name
  resource_group_name            = azurerm_resource_group.this.name
  location                       = azurerm_resource_group.this.location
  service_plan_id                = module.app_plan.id
  deployment_container_endpoint  = "${module.storage.primary_blob_endpoint}${azurerm_storage_container.dispatcher_deployment.name}"
  webjobs_storage_account_name   = module.storage.this.name
  app_insights_connection_string = module.function_app_insights.connection_string
  tags                           = local.common_tags

  virtual_network_subnet_id     = azurerm_subnet.fn_integration.id
  public_network_access_enabled = true

  # EasyAuth — block unauthenticated requests at the platform layer. Accept
  # tokens minted against either api://<client_id> (always issued by AAD) or
  # the friendly identifier URI (so operators don't need to look up the GUID).
  auth_aad_client_id = azuread_application.dispatcher.client_id
  auth_aad_tenant_id = data.azurerm_client_config.current.tenant_id
  auth_extra_allowed_audiences = [
    "api://${data.azurerm_client_config.current.tenant_id}/${var.app_reg_name}"
  ]

  # Ingest app credentials — single multi-tenant app reg per env used by all
  # containers for cross-tenant auth. tenants.json holds only tenant identity
  # (tenant_id, organization, admin_url); client_id and cert_name come from
  # here so they're managed once per env in IaC.
  additional_app_settings = {
    "Ingest__IngestClientId" = azuread_application.ingest.client_id
    "Ingest__IngestCertName" = azurerm_key_vault_certificate.ingest.name
  }
}*/



