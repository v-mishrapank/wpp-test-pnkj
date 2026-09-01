output "storage_account_id" {
  value = azurerm_storage_account.function.id
}

output "storage_account_name" {
  value = azurerm_storage_account.function.name
}

output "storage_account_primary_blob_endpoint" {
  value = azurerm_storage_account.function.primary_blob_endpoint
}

output "deployment_container_id" {
  value = azurerm_storage_container.deployment.id
}

output "deployment_container_name" {
  value = azurerm_storage_container.deployment.name
}