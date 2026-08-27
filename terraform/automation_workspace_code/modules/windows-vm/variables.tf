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

variable "jit_allowed_source_address_prefixes" {
  type = list(string)
}

variable "tags" {
  type = map(string)
}
variable "associations" {
  type = map(object({
    subnet = string
    nsg    = string
  }))

  default = {
    vms = {
      subnet = "vms-subnet"
      nsg    = "vms-nsg"
    }
  }
}

variable "nsgs" {
  description = "Network security groups created for the Windows VM subnets"

  type = map(object({
    name = string
  }))

  default = {
    "vms-nsg" = {
      name = "vms-nsg"
    }
  }
}