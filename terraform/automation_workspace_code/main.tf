resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location


  tags = var.tags
}


module "network" {
  source = "./modules/network"

  vnet_name           = var.vnet_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  vnet_address_space  = var.vnet_address_space
  dns_servers         = var.dns_servers

  subnets          = var.subnets
  nsgs             = var.nsgs
  nsg_associations = var.nsg_associations
  tags             = merge(local.common_tags, { workloadComponent = "network" })
}

module "storage" {
  source = "./modules/storage"

  storage_name        = var.storage_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  container_names     = var.container_names
  tags                = local.common_tags
}