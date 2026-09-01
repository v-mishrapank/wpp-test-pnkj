output "resource_group_id" {
  description = "Resource ID of the resource group."
  value       = azurerm_resource_group.this.id
}

output "resource_group_name" {
  description = "Name of the resource group."
  value       = azurerm_resource_group.this.name
}

output "resource_group_location" {
  description = "Azure region of the resource group."
  value       = azurerm_resource_group.this.location
}

output "vnet_id" {
  description = "Resource ID of the VNet."
  value       = module.network.vnet_id
}

output "vnet_name" {
  description = "Name of the VNet."
  value       = module.network.vnet_name
}

output "subnet_ids" {
  description = "Map of subnet keys to resource IDs."
  value       = module.network.subnet_ids
}

output "subnet_names" {
  description = "Map of subnet keys to subnet names."
  value       = module.network.subnet_names
}

output "nsg_ids" {
  description = "Map of NSG keys to resource IDs."
  value       = module.network.nsg_ids
}

output "nsg_names" {
  description = "Map of NSG keys to NSG names."
  value       = module.network.nsg_names
}

output "nsg_association_ids" {
  description = "Map of subnet-to-NSG association IDs."
  value       = module.network.nsg_association_ids
}
output "storage_account_id" {
  description = "Resource ID of the storage account."
  value       = module.storage.storage_account_id
}

output "storage_account_name" {
  description = "Name of the storage account."
  value       = module.storage.storage_account_name
}

output "primary_blob_endpoint" {
  description = "Primary Blob service endpoint."
  value       = module.storage.primary_blob_endpoint
}

output "deployment_container_id" {
  description = "Resource ID of the function deployment container."
  value       = module.storage.deployment_container_id
}

output "deployment_container_name" {
  description = "Name of the function deployment container."
  value       = module.storage.deployment_container_name
}