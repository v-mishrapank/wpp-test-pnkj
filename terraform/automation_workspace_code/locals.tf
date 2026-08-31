locals {
  company      = lower(var.company)
  environment  = lower(var.environment)
  workload     = lower(var.workload)
  region_short = lower(var.location_short)

  resource_prefix             = "${local.company}-${local.environment}-${local.region_short}"
  application_resource_prefix = lower(var.application_resource_prefix)

  windows_vms = {
    vm01 = {
      name          = "vm-${local.application_resource_prefix}-001"
      computer_name = "matoolkitvm001"
      zone          = "1"
    }
    vm02 = {
      name          = "vm-${local.application_resource_prefix}-002"
      computer_name = "matoolkitvm002"
      zone          = "2"
    }
  }

  common_tags = merge(
    {
      environment        = var.environment
      owner              = "wpp-platform"
      costCenter         = "platform"
      application        = "wpp-cloud-automation"
      managedBy          = "terraform"
      tenant             = "wpp-cloud"
      workload           = var.workload
      dataClassification = "confidential"
    },
    var.tags
  )

  resource_names = {
    rg = "rg-ma-toolkit-branch-vm-001"
  }

  resource_group_name = "rg-wpp-network-nonprod-001"
  location            = "uksouth"

  vnet_name          = "vnet-wpp-network-nonprod-001"
  vnet_address_space = ["10.0.0.0/16"]

  dns_servers = []

  subnets = {
    vm = {
      name             = "snet-vm-001"
      address_prefixes = ["10.0.1.0/24"]
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

  nsgs = {
    vm = {
      name = "nsg-vm-001"

      security_rules = {
        allow_https = {
          name                       = "Allow-HTTPS-Inbound"
          description                = "Allow HTTPS from the corporate network."
          priority                   = 100
          direction                  = "Inbound"
          access                     = "Allow"
          protocol                   = "Tcp"
          source_port_range          = "*"
          destination_port_range     = "443"
          source_address_prefix      = "10.10.0.0/16"
          destination_address_prefix = "*"
        }

        allow_rdp = {
          name                       = "Allow-RDP-Inbound"
          description                = "Example RDP rule restricted to an approved source range."
          priority                   = 110
          direction                  = "Inbound"
          access                     = "Allow"
          protocol                   = "Tcp"
          source_port_range          = "*"
          destination_port_range     = "3389"
          source_address_prefix      = "10.20.0.0/16"
          destination_address_prefix = "*"
        }
      }
    }

    function = {
      name = "nsg-function-001"

      security_rules = {
        allow_https_outbound = {
          name                       = "Allow-HTTPS-Outbound"
          description                = "Allow HTTPS outbound."
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
      name = "nsg-private-endpoint-001"

      security_rules = {}
    }
  }

  nsg_associations = {
    vm = {
      subnet_key = "vm"
      nsg_key    = "vm"
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

  tags = {
    Environment = "NonProd"
    Application = "WPP"
    ManagedBy   = "Terraform"
  }

  security_rules = merge([
    for nsg_key, nsg in var.nsgs : {
      for rule_key, rule in nsg.security_rules :
      "${nsg_key}-${rule_key}" => merge(
        rule,
        {
          nsg_key  = nsg_key
          rule_key = rule_key
        }
      )
    }
  ])
}

