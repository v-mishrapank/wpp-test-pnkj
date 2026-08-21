resource "azurerm_resource_group" "wpp_cloud" {
  name     = local.resource_names.rg
  location = var.location
  tags     = local.common_tags
}

module "network" {
  source = "./automation_workspace_code/modules/network"

  resource_group_name = azurerm_resource_group.wpp_cloud.name
  location            = azurerm_resource_group.wpp_cloud.location
  tags                = local.common_tags
  resource_prefix     = local.resource_prefix
  vnet_address_space  = var.vnet_address_space
  subnet_prefixes     = var.subnet_prefixes
}

module "windows_vms" {
  source = "./automation_workspace_code/modules/windows-vm"

  resource_group_name                 = "rg-${local.application_resource_prefix}-vm-001"
  location                            = var.location
  application_resource_prefix         = local.application_resource_prefix
  subnet_id                           = module.network.windows_vm_subnet_id
  virtual_machines                    = local.windows_vms
  vm_size                             = var.windows_vm_size
  admin_username                      = var.windows_vm_admin_username
  jit_allowed_source_address_prefixes = var.vm_jit_allowed_source_address_prefixes
  tags                                = merge(local.common_tags, { workloadComponent = "windows-vms" })
}
