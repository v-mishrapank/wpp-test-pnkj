output "virtual_machines" {
  value = {
    for key, vm in azurerm_windows_virtual_machine.windows_vms : key => {
      id                 = vm.id
      name               = vm.name
      private_ip_address = vm.private_ip_address
      public_ip_address  = azurerm_public_ip.windows_vms[key].ip_address
      zone               = vm.zone
    }
  }
}

output "resource_group_name" {
  value = azurerm_resource_group.windows_vms.name
}