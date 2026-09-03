output "id" {
  description = "The ID of the Application Insights instance."
  value       = azurerm_application_insights.this.id
}

output "connection_string" {
  description = "The connection string for Application Insights."
  value       = azurerm_application_insights.this.connection_string
  sensitive   = true
}
