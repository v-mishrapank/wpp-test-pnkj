terraform {
  required_version = ">= 1.6.0, < 2.0.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
    azapi = {
      source  = "Azure/azapi"
      version = "~> 2.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
}

provider "azurerm" {
  features {}
  #subscription_id = var.subscription_id
  resource_provider_registrations = "none"
  # Storage accounts have shared_access_key_enabled = false (tenant policy).
  # Without this flag the provider falls back to key-based auth for data plane
  # polling and gets 403 KeyBasedAuthenticationNotPermitted on apply.
  storage_use_azuread = true

}
