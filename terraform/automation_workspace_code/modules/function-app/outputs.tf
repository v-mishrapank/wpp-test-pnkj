output "id" {
  description = "The ID of the Function App."
  value       = azurerm_function_app_flex_consumption.this.id
}

output "principal_id" {
  description = "The principal ID of the Function App's system-assigned identity."
  value       = azurerm_function_app_flex_consumption.this.identity[0].principal_id
}

output "name" {
  description = "The name of the Function App."
  value       = azurerm_function_app_flex_consumption.this.name
}

output "default_hostname" {
  description = "The default hostname of the Function App."
  value       = azurerm_function_app_flex_consumption.this.default_hostname
}
