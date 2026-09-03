output "storage_private_dns_zone_ids" {
  description = "IDs of Storage Private DNS Zones"
  value = {
    for k, v in azurerm_private_dns_zone.storage : k => v.id
  }
}

output "storage_private_dns_zone_names" {
  description = "Names of Storage Private DNS Zones"
  value = {
    for k, v in azurerm_private_dns_zone.storage : k => v.name
  }
}

output "storage_private_dns_zone_links" {
  description = "IDs of VNet links to Storage Private DNS Zones"
  value = {
    for k, v in azurerm_private_dns_zone_virtual_network_link.storage : k => v.id
  }
}

output "storage_private_dns_zone_link_names" {
  description = "Names of VNet links to Storage Private DNS Zones"
  value = {
    for k, v in azurerm_private_dns_zone_virtual_network_link.storage : k => v.name
  }
}
