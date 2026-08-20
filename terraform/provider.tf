provider "azurerm" {
  features {
    template_deployment {
      delete_nested_items_during_deletion = false
    }
  }
  storage_use_azuread = true
/*
  subscription_id = local.subscription_id
  tenant_id       = var.tenant_id*/
}

data "azurerm_client_config" "current" {}