data "azurerm_client_config" "current" {}

data "azuread_client_config" "current" {}

locals {
  # Stable GUID for the dispatcher's user_impersonation scope. Referenced by
  # both the scope definition and the CLI pre-authorization so they can never
  # drift apart.
  dispatcher_user_impersonation_scope_id = "c9f97a41-f846-41b7-8412-e769f0331b15"
  # Microsoft Azure CLI's well-known public client_id.
  azure_cli_client_id = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
}

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

resource "azuread_application" "dispatcher" {
  display_name     = "${var.resource_prefix}-${var.dispatcher_app_reg_name}-app-dispatcher-01"
  sign_in_audience = var.app_registration_sign_in_audience
  owners           = [data.azuread_client_config.current.object_id]
  api {
    oauth2_permission_scope {
      id                         = local.dispatcher_user_impersonation_scope_id
      value                      = "user_impersonation"
      type                       = "User"
      enabled                    = true
      admin_consent_display_name = "Access ${var.dispatcher_app_reg_name}"
      admin_consent_description  = "Allow the application to access the dispatcher API on behalf of the signed-in user."
      user_consent_display_name  = "Access ${var.dispatcher_app_reg_name}"
      user_consent_description   = "Allow the application to access the dispatcher API on your behalf."
    }
  }

  web {
    redirect_uris = ["https://${var.dispatcher_function_app_name}.azurewebsites.net/.auth/login/aad/callback"]
    implicit_grant {
      id_token_issuance_enabled = true
    }
  }

  lifecycle {
    ignore_changes = [identifier_uris]
  }
}
resource "azuread_application_identifier_uri" "dispatcher_tenant" {
  application_id = azuread_application.dispatcher.id
  identifier_uri = "api://${data.azurerm_client_config.current.tenant_id}/${var.dispatcher_app_reg_name}"
}
resource "azuread_application_identifier_uri" "dispatcher_client_id" {
  application_id = azuread_application.dispatcher.id
  identifier_uri = "api://${azuread_application.dispatcher.client_id}"
}
resource "azuread_service_principal" "dispatcher" {
  client_id                    = azuread_application.dispatcher.client_id
  owners                       = [data.azuread_client_config.current.object_id]
  app_role_assignment_required = true
  feature_tags {
    enterprise = true
  }
}

resource "azuread_application" "multitenant" {
  display_name     = "${var.resource_prefix}-app-multitenant-01"
  sign_in_audience = "AzureADMultipleOrgs"

  web {
    redirect_uris = []
  }
}
resource "azuread_application_pre_authorized" "dispatcher_cli" {
  application_id       = azuread_application.dispatcher.id
  authorized_client_id = local.azure_cli_client_id
  permission_ids       = [local.dispatcher_user_impersonation_scope_id]
}

resource "azuread_service_principal" "multitenant" {
  client_id = azuread_application.multitenant.client_id
}