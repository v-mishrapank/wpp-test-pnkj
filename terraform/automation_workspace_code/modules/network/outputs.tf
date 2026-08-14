output "vnet_id" {
  value = azurerm_virtual_network.main.id
}

output "app_subnet_id" {
  value = azurerm_subnet.app.id
}

output "private_endpoints_subnet_id" {
  value = azurerm_subnet.private_endpoints.id
}

output "data_subnet_id" {
  value = azurerm_subnet.data.id
}

output "automation_subnet_id" {
  value = azurerm_subnet.automation.id
}

output "private_dns_zone_keyvault_id" {
  value = azurerm_private_dns_zone.keyvault.id
}

output "private_dns_zone_cosmos_id" {
  value = azurerm_private_dns_zone.cosmos.id
}
