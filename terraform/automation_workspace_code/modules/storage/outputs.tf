output "storage_account_id" {
  description = "Resource ID of the storage account."
  value       = azurerm_storage_account.function.id
}

output "storage_account_name" {
  description = "Name of the storage account."
  value       = azurerm_storage_account.function.name
}

output "primary_blob_endpoint" {
  description = "Primary Blob service endpoint."
  value       = azurerm_storage_account.function.primary_blob_endpoint
}

output "deployment_container_id" {
  description = "Resource ID of the function deployment container."
  value       = azurerm_storage_container.deployment.id
}

output "deployment_container_name" {
  description = "Name of the function deployment container."
  value       = azurerm_storage_container.deployment.name
}
