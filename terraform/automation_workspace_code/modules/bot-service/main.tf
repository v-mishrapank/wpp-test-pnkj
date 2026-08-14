resource "azurerm_bot_service_azure_bot" "main" {
  name                = var.bot_name
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "F0"
  microsoft_app_id    = var.microsoft_app_id
  endpoint            = var.endpoint_url
  tags                = var.tags
}

resource "azurerm_bot_channel_ms_teams" "main" {
  bot_name            = azurerm_bot_service_azure_bot.main.name
  location            = var.location
  resource_group_name = var.resource_group_name
}
