resource "azurerm_container_registry" "main" {
  name                          = var.name
  resource_group_name           = var.resource_group_name
  location                      = var.location
  sku                           = var.sku
  admin_enabled                 = var.admin_enabled
  public_network_access_enabled = var.public_network_access_enabled
  network_rule_bypass_option    = var.network_rule_bypass_option
  export_policy_enabled         = var.export_policy_enabled
  quarantine_policy_enabled     = var.quarantine_policy_enabled
  retention_policy_in_days      = var.retention_policy_in_days
  zone_redundancy_enabled       = var.zone_redundancy_enabled
  tags                          = var.tags
}
