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
