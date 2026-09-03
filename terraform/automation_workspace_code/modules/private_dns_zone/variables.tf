variable "storage_private_dns_zones" {
  description = "Private DNS zones for Storage Account private endpoints"
  type        = map(string)
  default     = {}
}

variable "resource_group_name" {
  description = "Resource group containing the private DNS zones."
  type        = string
}

variable "vnet_id" {
  description = "Resource ID of the VNet linked to the private DNS zones."
  type        = string
}

variable "vnet_name" {
  description = "Name of the VNet used in private DNS link names."
  type        = string
}

variable "tags" {
  description = "Tags to apply."
  type        = map(string)
  default     = {}
}