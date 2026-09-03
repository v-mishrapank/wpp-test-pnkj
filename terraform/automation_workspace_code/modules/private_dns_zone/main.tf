resource "azurerm_private_dns_zone" "storage" {
  for_each = var.storage_private_dns_zones

  name                = each.value
  resource_group_name = var.resource_group_name
  tags                = var.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "storage" {
  for_each = var.storage_private_dns_zones

  name                  = "${each.key}-${var.vnet_name}-link"
  resource_group_name   = var.resource_group_name
  private_dns_zone_name = azurerm_private_dns_zone.storage[each.key].name
  virtual_network_id    = var.vnet_id
  registration_enabled  = false
  tags                  = var.tags
}
