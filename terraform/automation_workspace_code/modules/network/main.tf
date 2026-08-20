resource "azurerm_virtual_network" "main" {
  name                = "${var.resource_prefix}-vnet-01"
  address_space       = var.vnet_address_space
  location            = var.location
  resource_group_name = var.resource_group_name
  tags                = var.tags
}

resource "azurerm_subnet" "windows_vms" {
  name                 = "${var.resource_prefix}-subnet-windows-vm-01"
  resource_group_name  = var.resource_group_name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [var.subnet_prefixes.windows_vms]
}
