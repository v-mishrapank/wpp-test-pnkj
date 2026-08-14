resource "azurerm_automation_account" "main" {
  name                         = var.automation_name
  location                     = var.location
  resource_group_name          = var.resource_group_name
  sku_name                     = "Basic"
  local_authentication_enabled = false
  tags                         = var.tags

  identity {
    type         = "UserAssigned"
    identity_ids = [var.identity_id]
  }
}

resource "azurerm_automation_hybrid_runbook_worker_group" "main" {
  count = var.enable_hybrid_worker ? 1 : 0

  name                    = "${var.resource_prefix}-hybrid-worker-group"
  resource_group_name     = var.resource_group_name
  automation_account_name = azurerm_automation_account.main.name
}
