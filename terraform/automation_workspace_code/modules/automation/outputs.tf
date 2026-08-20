output "automation_account_name" {
  value = azurerm_automation_account.main.name
}

output "automation_account_id" {
  value = azurerm_automation_account.main.id
}

output "automation_principal_id" {
  value = var.identity_principal_id
}
