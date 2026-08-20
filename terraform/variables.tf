variable "company" {
  type        = string
  description = "Company name used in resource naming"
  default     = "wpp"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
  default     = "nonprod"
}

variable "location" {
  type        = string
  description = "Azure region"
  default     = "uksouth"
}

variable "location_short" {
  type        = string
  description = "Short region code used in naming"
  default     = "uks"
}

variable "workload" {
  type        = string
  description = "Workload name"
  default     = "cloud"
}

variable "tenant_id" {
  type        = string
  description = "Azure AD tenant ID for the WPP Cloud tenant"
  default     = "cb9c5d53-9ef5-4bf1-b3b8-9b5237ca7781"
}

variable "subscription_ids" {
  type        = map(string)
  description = "Azure subscription IDs keyed by deployment environment"

  validation {
    condition = alltrue([
      for subscription_id in values(var.subscription_ids) :
      can(regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", subscription_id))
    ])
    error_message = "Each subscription ID must be a valid UUID."
  }
  default = {
    nonprod = "6a0f429d-3dec-45ca-9dba-8f9847b98a7b"
    prod    = "6a0f429d-3dec-45ca-9dba-8f9847b98a7b" # Replace with actual prod subscription ID
  }
}

variable "tags" {
  type        = map(string)
  description = "Additional tags applied across all resources"
  default = {
    managedBy = "terraform"
  }
}

variable "resource_group_name" {
  type        = string
  description = "Resource group name override"
  default     = null
}

variable "vnet_address_space" {
  type        = list(string)
  description = "CIDR for the WPP Cloud VNet"
  default     = ["10.30.0.0/16"]
}

variable "subnet_prefixes" {
  type = object({
    app            = string
    private_endpts = string
    data           = string
    automation     = string
    windows_vms    = string
  })
  description = "Subnet CIDR blocks"
  default = {
    app            = "10.30.1.0/24"
    private_endpts = "10.30.2.0/24"
    data           = "10.30.3.0/24"
    automation     = "10.30.4.0/24"
    windows_vms    = "10.30.5.0/24"
  }
}

variable "windows_vm_size" {
  type        = string
  description = "Azure VM size used for both Windows VMs"
  default     = "Standard_D2s_v5"
}

variable "windows_vm_admin_username" {
  type        = string
  description = "Local administrator username for the Windows VMs"
  default     = "azureadmin"
}

variable "windows_vm_user_principal_names" {
  type        = set(string)
  description = "Entra user principal names granted Windows VM login and JIT request access"
  default     = []
}

variable "windows_vm_jit_allowed_source_address_prefixes" {
  type        = list(string)
  description = "CIDR ranges allowed to request JIT RDP access; set this to trusted operator public IP ranges"
  default     = []
}

variable "function_runtime" {
  type        = string
  description = "Function runtime stack"
  default     = "python"
}

variable "function_runtime_version" {
  type        = string
  description = "Python runtime version"
  default     = "3.11"
}

variable "key_vault_secrets" {
  type        = map(string)
  description = "Optional Key Vault secrets to create. Values must be provided through tfvars or secure input, not hard-coded here."
  default     = {}
}

variable "cosmos_offer_type" {
  type        = string
  description = "Cosmos DB offer type"
  default     = "Standard"
}

variable "cosmos_kind" {
  type        = string
  description = "Cosmos DB kind"
  default     = "GlobalDocumentDB"
}

variable "cosmos_consistency_level" {
  type        = string
  description = "Cosmos consistency level"
  default     = "Session"
}

variable "cosmos_capacity_mode" {
  type        = string
  description = "Serverless or provisioned throughput mode"
  default     = "Serverless"
}

variable "cosmos_throughput" {
  type        = number
  description = "Cosmos throughput when provisioned mode is used"
  default     = 400
}

variable "enable_hybrid_worker" {
  type        = bool
  description = "Enable automation hybrid worker configuration"
  default     = false
}

variable "public_network_access_enabled" {
  type        = bool
  description = "Public network access toggle for key vault and cosmos"
  default     = false
}

variable "bot_display_name" {
  type        = string
  description = "Display name for the Azure Bot Service"
  default     = "WPP Cloud Bot"
}

variable "bot_endpoint_path" {
  type        = string
  description = "Bot endpoint route"
  default     = "/api/messages"
}

variable "bot_microsoft_app_id" {
  type        = string
  description = "Microsoft app ID for the bot registration"
  default     = null
}

variable "app_registration_sign_in_audience" {
  type        = string
  description = "This is multi-tenant by default for WPP Cloud"
  default     = "AzureADMultipleOrgs"
}

variable "diagnostic_log_retention_days" {
  type        = number
  description = "Retention for platform logs"
  default     = 30
}
variable "dispatcher_storage_name" {
  type    = string
  default = "stmatoolkitbranchdisp007"
}
variable "dispatcher_app_reg_name" {
  description = "Display name for the dispatcher AAD app registration. Distinct from the Function App name — AAD app regs use identity-focused naming, not Azure-resource conventions."
  type        = string
  default     = "wpp-analytics-ingest-dispatcher"
}
