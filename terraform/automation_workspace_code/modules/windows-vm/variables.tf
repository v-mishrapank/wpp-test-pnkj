variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "application_resource_prefix" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "virtual_machines" {
  type = map(object({
    name          = string
    computer_name = string
    zone          = string
  }))
}

variable "vm_size" {
  type = string
}

variable "admin_username" {
  type = string
}

variable "user_principal_names" {
  description = "Entra user principal names granted Windows VM login access."
  type        = set(string)
  default     = []
}

variable "jit_allowed_source_address_prefixes" {
  type = list(string)
}

variable "tags" {
  type = map(string)
}