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

variable "public_network_access_enabled" {
  type    = bool
  default = true
}

variable "tags" {
  type = map(string)
}
