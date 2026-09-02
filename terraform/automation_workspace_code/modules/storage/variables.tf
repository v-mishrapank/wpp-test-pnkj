variable "name" {
  description = "Name of the storage account."
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "account_tier" {
  description = "Storage account tier."
  type        = string
  default     = "Standard"
}

variable "account_replication_type" {
  description = "Replication type."
  type        = string
  default     = "LRS"
}

variable "account_kind" {
  description = "Storage account kind."
  type        = string
  default     = "StorageV2"
}

variable "is_hns_enabled" {
  description = "Enable hierarchical namespace (ADLS Gen2)."
  type        = bool
  default     = true
}

variable "public_network_access_enabled" {
  description = <<-EOT
    Whether the storage account's public endpoint is reachable. Defaults to
    false — identity access from in-VNet callers goes via the private endpoints
    declared in env_module. Set to true only for accounts that must also be
    reachable from the public internet for operator tooling (e.g. the
    analytics data lake, where the tenant-level MCAPS policy maintains a
    narrow IP allowlist in networkRuleSet). The dispatcher storage account
    has no such allowlist and must remain false, or it becomes wide-open
    with defaultAction=Allow.
  EOT
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags to apply."
  type        = map(string)
  default     = {}
}
