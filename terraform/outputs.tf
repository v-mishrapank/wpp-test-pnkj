output "resource_group_name" {
  value = azurerm_resource_group.wpp_cloud.name
}

output "virtual_network_id" {
  value = module.network.vnet_id
}

output "windows_virtual_machines" {
  value = module.windows_vms.virtual_machines
}

output "windows_vm_subnet_id" {
  value = module.network.windows_vm_subnet_id
}
/*
output "key_vault_name" {
  value = module.keyvault.key_vault_name
}

output "key_vault_id" {
  value = module.keyvault.key_vault_id
}

output "key_vault_uri" {
  value = module.keyvault.key_vault_uri
}

output "automation_account_name" {
  value = module.automation.automation_account_name
}

output "automation_account_principal_id" {
  value = module.automation.automation_principal_id
}

output "function_app_urls" {
  value = {
    teams_dispatcher = "https://${module.function_app_teams.default_hostname}"
    email_dispatcher = "https://${module.function_app_email.default_hostname}"
    bot_endpoint     = "https://${module.function_app_bot.default_hostname}"
  }
}

output "bot_endpoint_url" {
  value = "https://${module.function_app_bot.default_hostname}${var.bot_endpoint_path}"
}

output "cosmos_db_account_name" {
  value = module.cosmosdb.cosmos_account_name
}

output "cosmos_db_account_id" {
  value = module.cosmosdb.cosmos_account_id
}

output "log_analytics_workspace_id" {
  value = module.monitoring.log_analytics_workspace_id
}

output "application_insights_connection_string" {
  value     = module.monitoring.application_insights_connection_string
  sensitive = true
}

output "bot_app_client_id" {
  value = module.app_registration.bot_app_client_id
}

output "automation_app_client_id" {
  value = module.app_registration.automation_app_client_id
}

output "acr_login_server" {
  value = module.acr.acr_login_server
}

output "container_app_url" {
  value = module.container_app.container_app_url
}*/
