resource "azurerm_cosmosdb_account" "main" {
  name                          = var.account_name
  location                      = var.location
  resource_group_name           = var.resource_group_name
  offer_type                    = var.offer_type
  kind                          = var.kind
  tags                          = var.tags
  public_network_access_enabled = var.public_network_access_enabled

  consistency_policy {
    consistency_level = var.consistency_level
  }

  geo_location {
    location          = var.location
    failover_priority = 0
  }

  identity {
    type = "SystemAssigned"
  }

  capacity {
    total_throughput_limit = var.capacity_mode == "Serverless" ? null : var.throughput
  }
}

resource "azurerm_cosmosdb_sql_database" "conversation_references" {
  name                = "conversation-references"
  account_name        = azurerm_cosmosdb_account.main.name
  resource_group_name = var.resource_group_name
  throughput          = var.capacity_mode == "Serverless" ? null : var.throughput
}

resource "azurerm_cosmosdb_sql_database" "foundry_thread_mapping" {
  name                = "foundry-thread-mapping"
  account_name        = azurerm_cosmosdb_account.main.name
  resource_group_name = var.resource_group_name
  throughput          = var.capacity_mode == "Serverless" ? null : var.throughput
}

resource "azurerm_cosmosdb_sql_container" "conversation_references" {
  name                  = "conversation-references"
  account_name          = azurerm_cosmosdb_account.main.name
  database_name         = azurerm_cosmosdb_sql_database.conversation_references.name
  resource_group_name   = var.resource_group_name
  partition_key_paths   = ["/conversationId"]
  partition_key_version = 1
  throughput            = var.capacity_mode == "Serverless" ? null : var.throughput
}

resource "azurerm_cosmosdb_sql_container" "foundry_thread_mapping" {
  name                  = "foundry-thread-mapping"
  account_name          = azurerm_cosmosdb_account.main.name
  database_name         = azurerm_cosmosdb_sql_database.foundry_thread_mapping.name
  resource_group_name   = var.resource_group_name
  partition_key_paths   = ["/threadId"]
  partition_key_version = 1
  throughput            = var.capacity_mode == "Serverless" ? null : var.throughput
}

resource "azurerm_private_endpoint" "cosmos" {
  name                = "${var.resource_prefix}-pep-cosmos-01"
  location            = var.location
  resource_group_name = var.resource_group_name
  subnet_id           = var.private_endpoints_subnet_id
  tags                = var.tags

  private_service_connection {
    name                           = "${var.resource_prefix}-cosmos-psc"
    private_connection_resource_id = azurerm_cosmosdb_account.main.id
    is_manual_connection           = false
    subresource_names              = ["Sql"]
  }

  private_dns_zone_group {
    name                 = "default"
    private_dns_zone_ids = [var.private_dns_zone_cosmos_id]
  }
}