variable "company" {
  type        = string
  description = "Company name used in resource naming"
  default     = "wpp"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
  default     = "nonprod"
}

variable "location_short" {
  type        = string
  description = "Short region code used in naming"
  default     = "uks"
}

variable "workload" {
  type        = string
  description = "Workload name"
  default     = "cloud"
}

variable "application_resource_prefix" {
  type        = string
  description = "Resource prefix for the toolkit applications, excluding the Azure resource type and sequence number"
  default     = "ma-toolkit-branch"
}

variable "resource_group_name" {
  description = "Name of the resource group."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "vnet_name" {
  description = "Name of the VNet."
  type        = string
}

variable "vnet_address_space" {
  description = "Address spaces assigned to the VNet."
  type        = list(string)
}

variable "dns_servers" {
  description = "Optional custom DNS servers."
  type        = list(string)
  default     = []
}

variable "subnets" {
  description = "Map of delegated and non-delegated subnets."

  type = map(object({
    name             = string
    address_prefixes = list(string)

    private_endpoint_network_policies = optional(
      string,
      "Enabled"
    )

    delegation = optional(object({
      name         = string
      service_name = string
      actions      = list(string)
    }))
  }))
}

variable "nsgs" {
  description = "Map of NSGs and security rules."

  type = map(object({
    name = string

    tags = optional(
      map(string),
      {}
    )

    security_rules = optional(map(object({
      name        = string
      description = optional(string)

      priority  = number
      direction = string
      access    = string
      protocol  = string

      source_port_range = optional(
        string
      )

      source_port_ranges = optional(
        list(string)
      )

      destination_port_range = optional(
        string
      )

      destination_port_ranges = optional(
        list(string)
      )

      source_address_prefix = optional(
        string
      )

      source_address_prefixes = optional(
        list(string)
      )

      destination_address_prefix = optional(
        string
      )

      destination_address_prefixes = optional(
        list(string)
      )
    })), {})
  }))
}

variable "nsg_associations" {
  description = "Map of subnet-to-NSG associations."

  type = map(object({
    subnet_key = string
    nsg_key    = string
  }))

  default = {}
}

variable "tags" {
  description = "Common Azure resource tags."
  type        = map(string)
  default     = {}
}
variable "storage_name" {
  description = "Name of the storage account."
  type        = string
}
