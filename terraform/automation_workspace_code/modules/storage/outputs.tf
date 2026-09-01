output "storage_account_id" {
  value = azurerm_storage_account.function.id
}

output "storage_account_name" {
  value = azurerm_storage_account.function.name
}

output "storage_account_primary_blob_endpoint" {
  value = azurerm_storage_account.function.primary_blob_endpoint
}

output "container_ids" {
  value = {
    for name, container in azurerm_storage_container.containers : name => container.id
  }
}

output "container_names" {
  value = {
    for name, container in azurerm_storage_container.containers : name => container.name
  }
}