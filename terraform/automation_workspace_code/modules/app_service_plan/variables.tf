variable "name" {
  description = "Name of the App Service Plan."
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

variable "sku_name" {
  description = "SKU name for the plan."
  type        = string
  default     = "FC1"
}

variable "tags" {
  description = "Tags to apply."
  type        = map(string)
  default     = {}
}
