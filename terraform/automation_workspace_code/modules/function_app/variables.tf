variable "name" {
  description = "Name of the Function App."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name."
  type        = string
}

variable "service_plan_id" {
  description = "ID of the Flex Consumption App Service Plan."
  type        = string
}

variable "deployment_container_endpoint" {
  description = "Full URL of the blob container used for deployment packages (e.g. https://<sa>.blob.core.windows.net/<container>)."
  type        = string
}

variable "runtime_name" {
  description = "Runtime name (dotnet-isolated, node, python, java, powershell, custom)."
  type        = string
  default     = "dotnet-isolated"
}

variable "runtime_version" {
  description = "Runtime version."
  type        = string
  default     = "10.0"
}

variable "instance_memory_in_mb" {
  description = "Instance memory size in MB."
  type        = number
  default     = 2048
}

variable "maximum_instance_count" {
  description = "Maximum number of instances for scaling."
  type        = number
  default     = 40
}

variable "app_insights_connection_string" {
  description = "Application Insights connection string."
  type        = string
  sensitive   = true
}

variable "webjobs_storage_account_name" {
  description = "Name of the storage account backing AzureWebJobsStorage (Functions host state: blobs, queues, tables, leases). On Flex Consumption with SystemAssigned identity, the runtime reads this via AzureWebJobsStorage__accountName and authenticates as the Function App MI — which must hold Storage Blob Data Owner + Storage Queue Data Contributor + Storage Table Data Contributor on the account (all three are required; missing Queue caused webjobs.storage health-check AuthenticationFailed on #167 which blocked the Timer trigger listener). Usually the same account used for deployment_container_endpoint."
  type        = string
}

variable "additional_app_settings" {
  description = "Additional app settings to merge. App Insights settings (APPLICATIONINSIGHTS_CONNECTION_STRING, APPINSIGHTS_INSTRUMENTATIONKEY) are managed via app_insights_connection_string — do not include them here."
  type        = map(string)
  default     = {}

  validation {
    condition = length(setintersection(
      keys(var.additional_app_settings),
      ["APPLICATIONINSIGHTS_CONNECTION_STRING", "APPINSIGHTS_INSTRUMENTATIONKEY"]
    )) == 0
    error_message = "App Insights settings (APPLICATIONINSIGHTS_CONNECTION_STRING, APPINSIGHTS_INSTRUMENTATIONKEY) must not be passed via additional_app_settings — set them via app_insights_connection_string instead. Setting them here causes perpetual drift since the provider treats site_config.application_insights_connection_string as canonical."
  }

  validation {
    condition = length(setintersection(
      keys(var.additional_app_settings),
      ["AzureWebJobsStorage__accountName", "AzureWebJobsStorage", "DEPLOYMENT_STORAGE_CONNECTION_STRING"]
    )) == 0
    error_message = "AzureWebJobsStorage__accountName, AzureWebJobsStorage, and DEPLOYMENT_STORAGE_CONNECTION_STRING must not be passed via additional_app_settings. WebJobs state uses AzureWebJobsStorage__accountName (managed here) and package pull uses functionAppConfig.deployment.storage.authentication; the connection-string forms shadow the identity-based paths and caused #167's silent-timer failure."
  }
}

variable "virtual_network_subnet_id" {
  description = "ID of the subnet to VNet-integrate the Function App into (outbound). Must be delegated to Microsoft.App/environments. If null, no VNet integration and outbound traffic uses platform networking (will fail against PE-only backends)."
  type        = string
  default     = null
}

variable "public_network_access_enabled" {
  description = "Whether the Function App's HTTPS endpoint is reachable from the public internet. Default true — operator access from laptop + services that aren't yet on Private Link. Set false if you add a PE and want to force all inbound through it."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags to apply."
  type        = map(string)
  default     = {}
}

variable "auth_aad_client_id" {
  description = "AAD app registration client ID for EasyAuth. When set, EasyAuth is enabled and only AAD-authenticated requests reach the function. Caller obtains token via `az account get-access-token --resource <client_id>` and passes as Bearer."
  type        = string
  default     = null
}

variable "auth_aad_tenant_id" {
  description = "AAD tenant ID for EasyAuth token validation. Required when auth_aad_client_id is set."
  type        = string
  default     = null
}

variable "auth_extra_allowed_audiences" {
  description = "Additional allowed audience strings to accept on tokens (besides api://<client_id>). Use this to accept tokens minted against the app reg's friendly identifier URI (e.g. api://<tenant>/<app-reg-name>) so callers don't need to look up the client_id GUID."
  type        = list(string)
  default     = []
}

variable "auth_allowed_applications" {
  description = "Client app IDs allowed to call this Function App. Maps to Easy Auth v2 defaultAuthorizationPolicy.allowedApplications. Per MS Learn: empty (default) skips the `appid` filter — any client app in the home tenant is admitted (audience, issuer/tenant, and SP-level user/group assignment still apply). Set to a non-empty list (e.g., `[\"04b07795-8ddb-461a-bbee-02f9e1bf7b46\"]` for Azure CLI only) to gate by client app."
  type        = list(string)
  default     = []
}

variable "auth_unauthenticated_action" {
  description = "What Easy Auth does with anonymous requests. Only consulted when EasyAuth is enabled (`auth_aad_client_id` set); when unset, the `auth_settings_v2` block is skipped entirely and this variable has no effect (e.g. `module \"dapi_function_app\"` in this same env doesn't enable EasyAuth). `Return401` 401s with no redirect (best for service-to-service or pure-API callers). `RedirectToLoginPage` bounces browser GETs through AAD SSO so a user can paste a URL and view JSON after sign-in. Defaults to `RedirectToLoginPage` since EasyAuth-enabled consumers of this module today are admin/operator API surfaces that benefit from browser SSO. `AllowAnonymous` is intentionally not accepted — `require_authentication = true` is hardcoded in the module, so anonymous would be a no-op."
  type        = string
  default     = "RedirectToLoginPage"
  validation {
    condition     = contains(["Return401", "RedirectToLoginPage"], var.auth_unauthenticated_action)
    error_message = "auth_unauthenticated_action must be Return401 or RedirectToLoginPage."
  }
}
