output "container_app_url" {
  value = azurerm_container_app.main.latest_revision_fqdn
}

output "container_app_id" {
  value = azurerm_container_app.main.id
}

output "container_app_environment_id" {
  value = azurerm_container_app_environment.main.id
}

output "container_app_principal_id" {
  value = azurerm_container_app.main.identity[0].principal_id
}
