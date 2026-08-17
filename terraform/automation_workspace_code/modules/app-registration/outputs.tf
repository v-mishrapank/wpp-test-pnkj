output "automation_app_client_id" {
  value = azuread_application.automation.client_id
}

output "bot_app_client_id" {
  value = azuread_application.bot.client_id
}

output "multitenant_app_client_id" {
  value = azuread_application.multitenant.client_id
}
