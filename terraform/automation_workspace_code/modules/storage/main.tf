resource "azurerm_storage_account" "function" {
  name                     = var.storage_name
  location                 = var.location
  resource_group_name      = var.resource_group_name

  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  public_network_access_enabled   = false
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = true


  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 30
    }
    container_delete_retention_policy {
      days = 30
    }
  }
  tags = var.tags
}

resource "azurerm_storage_container" "deployment" {
  name                  = "function-deployment"
  storage_account_name  = azurerm_storage_account.function.name
  container_access_type = "private"
}