resource "azurerm_function_app_flex_consumption" "this" {
  name                = var.name
  location            = var.location
  resource_group_name = var.resource_group_name
  service_plan_id     = var.service_plan_id

  storage_container_type      = "blobContainer"
  storage_container_endpoint  = var.deployment_container_endpoint
  storage_authentication_type = "SystemAssignedIdentity"

  runtime_name    = var.runtime_name
  runtime_version = var.runtime_version

  instance_memory_in_mb  = var.instance_memory_in_mb
  maximum_instance_count = var.maximum_instance_count

  # VNet integration for outbound traffic. When set, all outbound calls
  # from the function (to storage, KV, ACA job control plane, etc.) route
  # through this subnet — required because those backends are PE-only on
  # branch. The subnet must be delegated to Microsoft.App/environments.
  # Inbound (client) access remains public by default; set
  # public_network_access_enabled = false if you also want to lock that
  # down with a separate PE.
  virtual_network_subnet_id = var.virtual_network_subnet_id

  public_network_access_enabled = var.public_network_access_enabled

  # Disable basic-auth publishing on SCM. CI deploys (Azure/functions-action@v1)
  # authenticate via the prior azure/login@v2 OIDC session against the management
  # plane, so no publish-profile credentials are needed. Leaving the provider
  # default (`true`) would expose a shared publish-profile password against a
  # public SCM endpoint — the pattern Defender for Cloud and CIS 9.x flag. MCAPS
  # policy also auto-remediates this to `false`, so the default shows up as
  # daily drift.
  webdeploy_publish_basic_authentication_enabled = false

  https_only = true
  tags       = var.tags

  identity {
    type = "SystemAssigned"
  }

  # site_config.application_insights_connection_string is the canonical place
  # to set the App Insights connection string — the provider treats it as
  # source of truth and removes any duplicate in app_settings on apply. Setting
  # it in both places caused perpetual drift (state has it only in site_config,
  # config has it in both, plan re-adds the app_settings entry every time).
  site_config {
    application_insights_connection_string = var.app_insights_connection_string
  }

  # Identity-based storage config. WebJobs state uses the __accountName form;
  # package pull uses functionAppConfig.deployment.storage.authentication
  # (configured via storage_container_type above). The connection-string
  # forms (AzureWebJobsStorage, DEPLOYMENT_STORAGE_CONNECTION_STRING) must
  # be absent on the live resource: when either is present alongside the
  # __accountName form the host binds it as a connection-string and never
  # evaluates the prefix family (exact-match beats prefix-match, per
  # https://learn.microsoft.com/azure/azure-functions/functions-bindings-storage-blob-output#connections).
  # Environments that were TF-pinned to "" on these keys before #177 will
  # have a persistent non-empty shadow on the live resource that the
  # azurerm provider does not prune (app_settings is merge-style for keys
  # the module doesn't declare). Clear once per env with:
  # See #171 / #197 for the full trace.
  #
  # azurerm_function_app_flex_consumption (provider issue 29099) auto-injects
  # AzureWebJobsStorage and DEPLOYMENT_STORAGE_CONNECTION_STRING with empty
  # AccountKey on every apply. Setting both to "" here as a TF-managed sentinel
  # forces the provider to overwrite the platform-injected shadow each apply,
  # so the runtime falls through to the __accountName MI form. (Without this,
  # every fresh FA enters webjobs.storage AuthenticationFailed and the timer
  # listener can't start.) MS Learn migration doc documents this exact
  # workaround; remove once hashicorp/terraform-provider-azurerm#29099 ships.
  app_settings = merge({
    "AzureWebJobsStorage"                  = ""
    "DEPLOYMENT_STORAGE_CONNECTION_STRING" = ""
    "AzureWebJobsStorage__accountName"     = var.webjobs_storage_account_name
    "Ingest__KeyVaultName"                 = var.key_vault_name
    "Ingest__SubscriptionId"               = var.subscription_id
    "Ingest__ResourceGroupName"            = var.resource_group_name
    "Ingest__ConfigPath"                   = "Config"
  }, var.additional_app_settings)

  dynamic "auth_settings_v2" {
    for_each = var.auth_aad_client_id == null ? [] : [1]
    content {
      auth_enabled                            = true
      require_authentication                  = true
      unauthenticated_action                  = var.auth_unauthenticated_action
      default_provider                        = "azureactivedirectory"
      require_https                           = true
      forward_proxy_convention                = "NoProxy"
      http_route_api_prefix                   = "/.auth"
      excluded_paths                          = []
      runtime_version                         = "~1"
      config_file_path                        = null
      forward_proxy_custom_host_header_name   = null
      forward_proxy_custom_scheme_header_name = null

      active_directory_v2 {
        client_id                   = var.auth_aad_client_id
        tenant_auth_endpoint        = "https://login.microsoftonline.com/${var.auth_aad_tenant_id}/v2.0"
        www_authentication_disabled = false
        # api://<client_id> is always valid for tokens issued for this app reg.
        # Additional audiences (e.g. friendly identifier_uris) merged in so
        # operators can request tokens by URI rather than by GUID.
        allowed_audiences = concat(["api://${var.auth_aad_client_id}"], var.auth_extra_allowed_audiences)
        # Easy Auth v2 (auth_settings_v2 API) defaultAuthorizationPolicy.
        # allowedApplications is a third gate (on top of audience + issuer/
        # tenant) that filters tokens by `appid`. Per MS Learn: "When this
        # property is configured as a nonempty array, only tokens obtained by
        # an application specified in the list are accepted." Empty (default)
        # skips the filter — any client app in the home tenant is admitted,
        # subject to audience + tenant + SP user/group assignment. Set a
        # non-empty list (e.g., Azure CLI 04b07795-…) to gate by client app.
        # https://learn.microsoft.com/azure/app-service/configure-authentication-provider-aad#use-a-built-in-authorization-policy
        allowed_applications = var.auth_allowed_applications
      }

      login {
        token_store_enabled = true
      }
    }
  }
}
