variable "storage_name" {
  description = "Name of the storage account."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the resource group."
  type        = string
}
variable "location" {
  description = "Azure region."
  type        = string
}
variable "tags" {
  description = "Tags to apply to the resources."
  type        = map(string)
  default     = {}
}