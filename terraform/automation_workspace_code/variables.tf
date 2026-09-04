variable "company" {
  type        = string
  description = "Company name used in resource naming"
  default     = "wpp"
}

variable "env" {
  type        = string
  description = "Deployment environment"
  default     = "dev"
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
  default     = "wpp-analytics-branch"
}

variable "resource_group_name" {
  description = "Name of the resource group."
  type        = string
  default     = "rg-wpp-network-nonprod-001"
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "uksouth"
}

variable "vnet_name" {
  description = "Name of the VNet."
  type        = string
  default     = "vnet-wpp-nonprod-001"

}

variable "vnet_address_space" {
  description = "Address spaces assigned to the VNet."
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

variable "dns_servers" {
  description = "Optional custom DNS servers."
  type        = list(string)
  default     = []
}
variable "tags" {
  description = "Common Azure resource tags."
  type        = map(string)
  default     = {}
}
variable "storage_name" {
  description = "Name of the storage account."
  type        = string
  default     = "stwppnonprod001"
}
variable "container_names" {
  description = "List of storage container names."
  type        = list(string)
  default     = ["container1"]
}

variable "nsgs" {
  description = "Map of NSGs and security rules."

  type = map(object({
    name = string

    tags = optional(map(string), {})

    security_rules = optional(map(object({
      name        = string
      description = optional(string)

      priority  = number
      direction = string
      access    = string
      protocol  = string

      source_port_range  = optional(string)
      source_port_ranges = optional(list(string))

      destination_port_range  = optional(string)
      destination_port_ranges = optional(list(string))

      source_address_prefix   = optional(string)
      source_address_prefixes = optional(list(string))

      destination_address_prefix   = optional(string)
      destination_address_prefixes = optional(list(string))
    })), {})
  }))

  default = {
    app = {
      name = "nsg-app-001"

      security_rules = {
        allow_rdp = {
          name                       = "Allow-RDP-From-Corporate"
          description                = "Allow RDP from the approved corporate network."
          priority                   = 100
          direction                  = "Inbound"
          access                     = "Allow"
          protocol                   = "Tcp"
          source_port_range          = "*"
          destination_port_range     = "3389"
          source_address_prefix      = "10.20.0.0/16"
          destination_address_prefix = "*"
        }

        allow_https_outbound = {
          name                       = "Allow-HTTPS-Outbound"
          description                = "Allow HTTPS outbound traffic."
          priority                   = 110
          direction                  = "Outbound"
          access                     = "Allow"
          protocol                   = "Tcp"
          source_port_range          = "*"
          destination_port_range     = "443"
          source_address_prefix      = "*"
          destination_address_prefix = "Internet"
        }
      }
    }

    function = {
      name = "nsg-function-001"

      security_rules = {
        allow_https_outbound = {
          name                       = "Allow-HTTPS-Outbound"
          description                = "Allow Azure Function HTTPS outbound traffic."
          priority                   = 100
          direction                  = "Outbound"
          access                     = "Allow"
          protocol                   = "Tcp"
          source_port_range          = "*"
          destination_port_range     = "443"
          source_address_prefix      = "*"
          destination_address_prefix = "Internet"
        }
      }
    }

    private_endpoint = {
      name           = "nsg-private-endpoint-001"
      security_rules = {}
    }
  }
}

variable "nsg_associations" {
  description = "Map of subnet-to-NSG associations."

  type = map(object({
    subnet_key = string
    nsg_key    = string
  }))

  default = {
    app = {
      subnet_key = "app"
      nsg_key    = "app"
    }

    function = {
      subnet_key = "function"
      nsg_key    = "function"
    }

    private_endpoint = {
      subnet_key = "private_endpoint"
      nsg_key    = "private_endpoint"
    }
  }
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

  default = {
    app = {
      name             = "snet-app-001"
      address_prefixes = ["10.0.1.0/24"]

      # A Container Apps managed environment requires an exclusive delegated
      # infrastructure subnet. Do not share the Function App integration
      # subnet because its service association link is owned by Functions.
      delegation = {
        name         = "container-app-environment-delegation"
        service_name = "Microsoft.App/environments"
        actions = [
          "Microsoft.Network/virtualNetworks/subnets/join/action"
        ]
      }
    }

    function = {
      name             = "snet-function-001"
      address_prefixes = ["10.0.2.0/24"]

      delegation = {
        name         = "function-delegation"
        service_name = "Microsoft.App/environments"
        actions = [
          "Microsoft.Network/virtualNetworks/subnets/join/action"
        ]
      }
    }

    private_endpoint = {
      name                              = "snet-private-endpoint-001"
      address_prefixes                  = ["10.0.3.0/24"]
      private_endpoint_network_policies = "Disabled"
    }
  }
}

variable "wpp_analytics_storage_blob_name" {
  type    = string
  default = "wpp-analytics-dev-disp-blob-001"
}

variable "wpp_analytics_storage_queue_name" {
  type    = string
  default = "wpp-analytics-dev-disp-queue-001"
}

variable "wpp_analytics_storage_table_name" {
  type    = string
  default = "wpp-analytics-dev-disp-table-001"
}

variable "wpp_analytics_storage_file_name" {
  type    = string
  default = "wpp-analytics-dev-disp-file-001"
}

variable "app_service_plan_name" {
  type    = string
  default = "asp-wpp-analytics-dev-disp-001"
}

variable "function_app_insights_name" {
  type    = string
  default = "appi-wpp-analytics-dev-disp-001"
}

variable "analytics_function_app_name" {
  type    = string
  default = "func-wpp-analytics-dev-disp-001"
}

variable "app_reg_name" {
  type    = string
  default = "wpp-analytics-dev-analytics-ingest"
}

variable "storage_private_dns_zones" {
  description = "Private DNS zones for Storage Account private endpoints"

  type = map(string)

  default = {
    blob  = "privatelink.blob.core.windows.net"
    queue = "privatelink.queue.core.windows.net"
    table = "privatelink.table.core.windows.net"
    file  = "privatelink.file.core.windows.net"
  }
}

variable "dispatcher_user_impersonation_scope_id" {
  type    = string
  default = "c1e96926-60f3-47d4-a2ae-e9f41298fd34"
}

variable "public_network_access_enabled" {
  description = "Whether public network access is enabled for supported resources."
  type        = bool
  default     = true
}

variable "log_analytics_workspaces" {
  description = "Log Analytics Workspaces"
  type = map(object({
    retention_in_days = number
  }))

  default = {
    function = {
      retention_in_days = 30
    }
    container = {
      retention_in_days = 30
    }
  }
}



