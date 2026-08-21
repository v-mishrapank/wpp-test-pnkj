output "resource_group_name" {
  value = azurerm_resource_group.wpp_cloud.name
}

output "virtual_network_id" {
  value = module.network.vnet_id
}

output "windows_vm_subnet_id" {
  value = module.network.windows_vm_subnet_id
}

output "windows_virtual_machines" {
  value     = module.windows_vms.virtual_machines
  sensitive = true
}

output "windows_vm_resource_group_name" {
  value = module.windows_vms.resource_group_name
}
