variable "name" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "sku" {
  type    = string
  default = "Basic"
}

variable "admin_enabled" {
  description = "Enable admin for ACR."
  type        = bool
  default     = true
}
variable "public_network_access_enabled" {
  description = "Enable public network access."
  type        = bool
  default     = true
}
variable "network_rule_bypass_option" {
  description = "Network rule bypass option."
  type        = string
  default     = "AzureServices"
}
variable "export_policy_enabled" {
  description = "Enable export policy."
  type        = bool
  default     = true
}
variable "quarantine_policy_enabled" {
  description = "Enable quarantine policy."
  type        = bool
  default     = false
}
variable "retention_policy_in_days" {
  description = "Retention policy in days."
  type        = number
  default     = 0
}
variable "zone_redundancy_enabled" {
  description = "Enable zone redundancy."
  type        = bool
  default     = false
}

variable "tags" {
  type = map(string)
}
