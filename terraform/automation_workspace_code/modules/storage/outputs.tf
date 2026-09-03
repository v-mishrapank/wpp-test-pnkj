output "id" {
  description = "The ID of the storage account."
  value       = azurerm_storage_account.this.id
}

output "name" {
  description = "The name of the storage account."
  value       = azurerm_storage_account.this.name
}

output "primary_blob_endpoint" {
  description = "The primary blob endpoint URL of the storage account."
  value       = azurerm_storage_account.this.primary_blob_endpoint
}

output "container_names" {
  description = "Map of configured container keys to names."
  value = {
    for key, container in azapi_resource.container :
    key => container.name
  }
}
