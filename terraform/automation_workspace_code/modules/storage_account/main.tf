resource "azurerm_storage_account" "this" {
  name                          = var.name
  resource_group_name           = var.resource_group_name
  location                      = var.location
  account_tier                  = var.account_tier
  account_replication_type      = var.account_replication_type
  account_kind                  = var.account_kind
  is_hns_enabled                = var.is_hns_enabled
  public_network_access_enabled = var.public_network_access_enabled
  https_traffic_only_enabled    = true
  min_tls_version               = "TLS1_2"
  # Tenant policy enforces these to be disabled. Provider defaults are true,
  # so without explicit config we get perpetual drift (plan flips to true,
  # apply succeeds but Azure keeps them false).
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false
  tags                            = var.tags
}
