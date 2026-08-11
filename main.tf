terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

resource "azurerm_resource_group" "rg" {
  name     = "wpp-dev1345"
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