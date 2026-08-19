variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "tenant_id" {
  type = string
}

variable "resource_prefix" {
  type = string
}

variable "kv_name" {
  type = string
}

variable "private_endpoints_subnet_id" {
  type = string
}

variable "private_dns_zone_keyvault_id" {
  type = string
}

variable "public_network_access_enabled" {
  type    = bool
  default = false
}

variable "tags" {
  type = map(string)
}
variable "soft_delete_retention_days" {
  description = "Soft delete retention days."
  type        = number
  default     = 90
}
variable "network_acls" {
  description = "Network ACLs for the Key Vault."
  type = object({
    bypass                     = string
    default_action             = string
    ip_rules                   = list(string)
    virtual_network_subnet_ids = list(string)
  })
  default = {
    bypass                     = "AzureServices"
    default_action             = "Deny"
    ip_rules                   = []
    virtual_network_subnet_ids = []
  }
}
variable "purge_protection_enabled" {
  description = "Whether purge protection is enabled."
  type        = bool
  default     = false
}

variable "rbac_authorization_enabled" {
  description = "Whether RBAC authorization is enabled."
  type        = bool
  default     = true
}
variable "sku_name" {
  description = "SKU name for the Key Vault."
  type        = string
  default     = "standard"
}