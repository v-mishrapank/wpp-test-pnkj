variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "tags" {
  type = map(string)
}

variable "resource_prefix" {
  type = string
}

variable "vnet_address_space" {
  type = list(string)
}

variable "subnet_prefixes" {
  type = object({
    app            = string
    private_endpts = string
    data           = string
    automation     = string
  })
}
variable "private_endpoint_network_policies" {
  description = "Private endpoint network policies."
  type        = string
  default     = "Disabled"
}
variable "private_link_service_network_policies_enabled" {
  description = "Enable private link service network policies."
  type        = bool
  default     = true
}
