output "cosmos_account_id" {
  description = "The resource ID of the Cosmos DB account."
  value = azurerm_cosmosdb_account.main.id
}

output "cosmos_account_name" {
  description = "The name of the Cosmos DB account."
  value = azurerm_cosmosdb_account.main.name
}

output "cosmos_principal_id" {
  description = "The principal ID of the Cosmos DB account's managed identity."
  value = azurerm_cosmosdb_account.main.identity[0].principal_id
}
