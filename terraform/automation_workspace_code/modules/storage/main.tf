resource "azurerm_storage_account" "this" {
  name                          = var.storage_name
  resource_group_name           = var.resource_group_name
  location                      = var.location
  account_tier                  = var.account_tier
  account_replication_type      = var.account_replication_type
  account_kind                  = var.account_kind
  is_hns_enabled                = var.is_hns_enabled
  public_network_access_enabled = var.public_network_access_enabled

  https_traffic_only_enabled      = true
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false

  tags = var.tags
}
resource "azapi_resource" "container" {
  for_each = toset(var.container_names)

  type      = "Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01"
  name      = each.value
  parent_id = "${azurerm_storage_account.this.id}/blobServices/default"

  body = {
    properties = {
      publicAccess = "None"
    }
  }
}

