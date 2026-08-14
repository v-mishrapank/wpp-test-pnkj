locals {
  company      = lower(var.company)
  environment  = lower(var.environment)
  workload     = lower(var.workload)
  region_short = lower(var.location_short)
  subscription_id = var.subscription_ids[local.environment]

  resource_prefix = "${local.company}-${local.environment}-${local.region_short}"

  common_tags = merge(
    {
      environment        = var.environment
      owner              = "wpp-platform"
      costCenter         = "platform"
      application        = "wpp-cloud-automation"
      managedBy          = "terraform"
      tenant             = "wpp-cloud"
      workload           = var.workload
      dataClassification = "confidential"
    },
    var.tags
  )

  app_registration_names = {
    automation  = "${local.resource_prefix}-app-automation-01"
    bot         = "${local.resource_prefix}-app-bot-01"
    multitenant = "${local.resource_prefix}-app-multitenant-01"
  }

  resource_names = {
    rg                          = var.resource_group_name != null ? var.resource_group_name : "${local.resource_prefix}-rg-01"
    vnet                        = "${local.resource_prefix}-vnet-01"
    app_subnet                  = "${local.resource_prefix}-subnet-app-01"
    pe_subnet                   = "${local.resource_prefix}-subnet-pe-01"
    data_subnet                 = "${local.resource_prefix}-subnet-data-01"
    auto_subnet                 = "${local.resource_prefix}-subnet-auto-01"
    nsg                         = "${local.resource_prefix}-nsg-01"
    route_table                 = "${local.resource_prefix}-rt-01"
    kv_name                     = "${local.company}${local.environment}${local.region_short}kv${random_string.kv.result}"
    automation                  = "${local.resource_prefix}-automation-01"
    log_analytics               = "${local.resource_prefix}-law-01"
    app_insights                = "${local.resource_prefix}-appi-01"
    storage                     = "${local.company}${local.environment}${local.region_short}st${random_string.storage.result}"
    plan                        = "${local.resource_prefix}-asp-01"
    teams_func                  = "${local.resource_prefix}-func-teamsdis-01"
    email_func                  = "${local.resource_prefix}-func-emaildis-01"
    bot_func                    = "${local.resource_prefix}-func-botapi-01"
    cosmos_name                 = "${local.company}${local.environment}${local.region_short}cosmos${random_string.cosmos.result}"
    bot_name                    = "${local.resource_prefix}-bot-teams-01"
    acr                         = "${local.company}${local.environment}${local.region_short}acr${random_string.acr.result}"
    container_app_environment   = "${local.resource_prefix}-cae-01"
    container_app_log_analytics = "${local.resource_prefix}-law-ca-01"
    container_app               = "${local.resource_prefix}-ca-01"
  }

  private_dns_zones = {
    keyvault = "privatelink.vaultcore.azure.net"
    cosmos   = "privatelink.documents.azure.com"
  }
}
