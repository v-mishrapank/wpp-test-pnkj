output "vnet_id" {
  value = azurerm_virtual_network.main.id
}

output "windows_vm_subnet_id" {
  value = azurerm_subnet.windows_vms.id
}
