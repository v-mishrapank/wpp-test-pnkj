terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = "6a0f429d-3dec-45ca-9dba-8f9847b98a7b"
  tenant_id       = "cb9c5d53-9ef5-4bf1-b3b8-9b5237ca7781"
}

resource "azurerm_resource_group" "rg" {
  name     = "wpp-dev123"
  location = "East US"

  tags = {
    Environment = "Dev"
    Project     = "WPP"
    ManagedBy   = "Terraform"
  }
}

output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "resource_group_id" {
  value = azurerm_resource_group.rg.id
}