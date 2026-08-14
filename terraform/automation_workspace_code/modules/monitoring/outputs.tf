output "log_analytics_workspace_id" {
  value = var.log_analytics_workspace_id
}

output "application_insights_connection_string" {
  value     = azurerm_application_insights.main.connection_string
  sensitive = true
}
