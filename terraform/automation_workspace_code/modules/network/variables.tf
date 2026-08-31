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
/*
variable "vnet_address_space" {
  type = list(string)
}

variable "subnets" {
  type = map(object({
    address_prefixes = list(string)
    delegation = optional(object({
      name         = string
      service_name = string
      actions      = list(string)
    }))
  }))
}

variable "nsgs" {
  type = map(any)
}

variable "associations" {
  type = map(object({
    subnet = string
    nsg    = string
  }))
}*/
variable "vnet_name" {
  description = "Name of the virtual network."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the resource group."
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

  validation {
    condition = alltrue([
      for subnet in values(var.subnets) :
      length(subnet.address_prefixes) > 0
    ])

    error_message = "Every subnet must have at least one address prefix."
  }
}

variable "nsgs" {
  description = "Map of NSGs and their security rules."

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

  validation {
    condition = alltrue(flatten([
      for nsg in values(var.nsgs) : [
        for rule in values(nsg.security_rules) :
        rule.priority >= 100 && rule.priority <= 4096
      ]
    ]))

    error_message = "NSG rule priority must be between 100 and 4096."
  }
}

variable "nsg_associations" {
  description = "Map defining subnet-to-NSG associations."

  type = map(object({
    subnet_key = string
    nsg_key    = string
  }))

  default = {}
}

variable "tags" {
  description = "Common tags applied to the VNet and NSGs."
  type        = map(string)
  default     = {}
}