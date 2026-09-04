output "id" {
  description = "The ID of the Container App."
  value       = azurerm_container_app.this.id
}

output "name" {
  description = "The name of the Container App."
  value       = azurerm_container_app.this.name
}

output "principal_id" {
  description = "The principal ID of the Container App's system-assigned identity. Use to grant SB / KV / other home-tenant resource access."
  value       = azurerm_container_app.this.identity[0].principal_id
}
