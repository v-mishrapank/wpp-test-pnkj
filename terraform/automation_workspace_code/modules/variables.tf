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

variable "location" {
  type        = string
  description = "Azure region"
  default     = "uksouth"
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

variable "tags" {
  type        = map(string)
  description = "Additional tags applied across all resources"
  default = {
    managedBy = "terraform"
  }
}

variable "resource_group_name" {
  type        = string
  description = "Resource group name override"
  default     = null
}

variable "vnet_address_space" {
  type        = list(string)
  description = "CIDR for the WPP Cloud VNet"
  default     = ["10.0.0.0/27"]
}


variable "windows_vm_size" {
  type        = string
  description = "Azure VM SKU; Standard_D4s_v5 provides 4 vCPUs and 16 GiB RAM"
  default     = "Standard_D4s_v5"
}

variable "windows_vm_admin_username" {
  type        = string
  description = "Local administrator username for the Windows VMs"
  default     = "azureadmin"
}

variable "vm_jit_allowed_source_address_prefixes" {
  type        = list(string)
  description = "Source prefixes users may select when requesting JIT RDP access; restrict to corporate egress or private network CIDRs where known"
  default     = []
}

variable "subnets" {
  description = "Subnet configuration"

  type = map(object({
    address_prefixes = list(string)
  }))

  default = {
    "vms-subnet" = {
      address_prefixes = ["10.0.0.0/27"]
    }
  }
}
variable "associations" {
  description = "Subnet to NSG mappings"

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