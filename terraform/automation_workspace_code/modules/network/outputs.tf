output "vnet_id" {
  description = "Resource ID of the VNet."
  value       = azurerm_virtual_network.this.id
}

output "vnet_name" {
  description = "Name of the VNet."
  value       = azurerm_virtual_network.this.name
}

output "vnet_address_space" {
  description = "Address spaces assigned to the VNet."
  value       = azurerm_virtual_network.this.address_space
}

output "subnet_ids" {
  description = "Map of subnet keys to resource IDs."

  value = {
    for key, subnet in azurerm_subnet.this :
    key => subnet.id
  }
}

output "subnet_names" {
  description = "Map of subnet keys to subnet names."

  value = {
    for key, subnet in azurerm_subnet.this :
    key => subnet.name
  }
}

output "subnets" {
  description = "Map containing subnet details."

  value = {
    for key, subnet in azurerm_subnet.this :
    key => {
      id               = subnet.id
      name             = subnet.name
      address_prefixes = subnet.address_prefixes
    }
  }
}

output "nsg_ids" {
  description = "Map of NSG keys to resource IDs."

  value = {
    for key, nsg in azurerm_network_security_group.this :
    key => nsg.id
  }
}

output "nsg_names" {
  description = "Map of NSG keys to NSG names."

  value = {
    for key, nsg in azurerm_network_security_group.this :
    key => nsg.name
  }
}

output "nsg_rule_ids" {
  description = "Map of NSG security-rule resource IDs."

  value = {
    for key, rule in azurerm_network_security_rule.this :
    key => rule.id
  }
}

output "nsg_association_ids" {
  description = "Map of subnet-to-NSG association IDs."

  value = {
    for key, association
    in azurerm_subnet_network_security_group_association.this :
    key => association.id
  }
}