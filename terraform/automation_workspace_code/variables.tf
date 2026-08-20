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

variable "application_resource_prefix" {
  type        = string
  description = "Resource prefix for the toolkit applications, excluding the Azure resource type and sequence number"
  default     = "ma-toolkit-branch"
}

variable "tenant_id" {
  type        = string
  description = "Azure AD tenant ID for the WPP Cloud tenant"
}

variable "subscription_id" {
  type        = string
  description = "Azure subscription ID for the WPP Cloud workload"
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
    windows_vms = string
  })
  description = "Subnet CIDR blocks"
  default = {
    windows_vms = "10.30.5.0/24"
  }
}

variable "windows_vm_user_principal_names" {
  type        = set(string)
  description = "Entra ID users permitted to request JIT access and sign in to the Windows VMs"
  default = [
    "ratheesh.kumar@hotmail.com",
    "v-mishrapank@microsoft.com"
  ]

  validation {
    condition     = alltrue([for upn in var.windows_vm_user_principal_names : can(regex("^[^@\\s]+@[^@\\s]+$", upn))])
    error_message = "Each Windows VM user principal name must be a valid UPN without spaces."
  }
}

variable "windows_vm_size" {
  type        = string
  description = "Azure VM SKU; Standard_D4s_v5 provides 4 vCPUs and 16 GiB RAM"
  default     = "Standard_D4s_v5"
}

variable "windows_vm_admin_username" {
  type        = string
  description = "Break-glass local administrator username; normal access uses Entra ID"
  default     = "azureadmin"
}

variable "vm_jit_allowed_source_address_prefixes" {
  type        = list(string)
  description = "Source prefixes users may select when requesting JIT RDP access; restrict to corporate egress or private network CIDRs where known"
  default     = ["*"]
}
