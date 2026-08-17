resource "azuread_application" "automation" {
  display_name     = "${var.resource_prefix}-app-automation-01"
  sign_in_audience = var.app_registration_sign_in_audience

  web {
    redirect_uris = []
  }
}

resource "azuread_service_principal" "automation" {
  client_id = azuread_application.automation.client_id
}

resource "azuread_application" "bot" {
  display_name     = "${var.resource_prefix}-app-bot-01"
  sign_in_audience = var.app_registration_sign_in_audience

  web {
    redirect_uris = []
  }
}

resource "azuread_service_principal" "bot" {
  client_id = azuread_application.bot.client_id
}

resource "azuread_application" "multitenant" {
  display_name     = "${var.resource_prefix}-app-multitenant-01"
  sign_in_audience = "AzureADMultipleOrgs"

  web {
    redirect_uris = []
  }
}

resource "azuread_service_principal" "multitenant" {
  client_id = azuread_application.multitenant.client_id
}