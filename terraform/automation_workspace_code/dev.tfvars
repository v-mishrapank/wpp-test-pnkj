resource_group_name = "rg-wpp-network-nonprod-001"
location            = "uksouth"

vnet_name          = "vnet-wpp-nonprod-001"
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
      allow_rdp = {
        name        = "Allow-RDP-From-Corporate"
        description = "Allow RDP from the approved corporate network."

        priority  = 100
        direction = "Inbound"
        access    = "Allow"
        protocol  = "Tcp"

        source_port_range          = "*"
        destination_port_range     = "3389"
        source_address_prefix      = "10.20.0.0/16"
        destination_address_prefix = "*"
      }

      allow_https_outbound = {
        name        = "Allow-HTTPS-Outbound"
        description = "Allow HTTPS outbound traffic."

        priority  = 110
        direction = "Outbound"
        access    = "Allow"
        protocol  = "Tcp"

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
        name        = "Allow-HTTPS-Outbound"
        description = "Allow Azure Function HTTPS outbound traffic."

        priority  = 100
        direction = "Outbound"
        access    = "Allow"
        protocol  = "Tcp"

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