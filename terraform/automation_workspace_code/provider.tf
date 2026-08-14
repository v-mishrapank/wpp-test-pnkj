provider "azurerm" {
  features {}

  subscription_id = local.subscription_id
  tenant_id       = var.tenant_id
}

provider "azuread" {
  tenant_id = var.tenant_id
}
