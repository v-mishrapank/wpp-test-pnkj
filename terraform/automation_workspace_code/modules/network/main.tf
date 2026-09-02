resource "azurerm_virtual_network" "this" {
  name                = var.vnet_name
  location            = var.location
  resource_group_name = var.resource_group_name
  address_space       = var.vnet_address_space
  dns_servers         = var.dns_servers

  tags = var.tags
}
resource "azurerm_subnet" "this" {
  for_each             = var.subnets
  name                 = each.value.name
  resource_group_name  = var.resource_group_name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = each.value.address_prefixes

  private_endpoint_network_policies = (
    each.value.private_endpoint_network_policies
  )

  dynamic "delegation" {
    for_each = each.value.delegation == null ? [] : [each.value.delegation]

    content {
      name = delegation.value.name

      service_delegation {
        name    = delegation.value.service_name
        actions = delegation.value.actions
      }
    }
  }
}

resource "azurerm_network_security_group" "this" {
  for_each = var.nsgs

  name                = each.value.name
  location            = var.location
  resource_group_name = var.resource_group_name

  tags = merge(
    var.tags,
    each.value.tags
  )
}

locals {
  security_rules = merge(
    [
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
    ]...
  )

  nsg_associations = merge(
    [
      for nsg_key, nsg in azurerm_network_security_group.this : {
        for association_key, association in var.nsg_associations :
        association_key => {
          subnet_key                = association.subnet_key
          network_security_group_id = nsg.id
        }
        if association.nsg_key == nsg_key
      }
    ]...
  )
}

resource "azurerm_network_security_rule" "this" {
  for_each = local.security_rules

  name        = each.value.name
  description = each.value.description

  priority  = each.value.priority
  direction = each.value.direction
  access    = each.value.access
  protocol  = each.value.protocol

  source_port_range  = each.value.source_port_range
  source_port_ranges = each.value.source_port_ranges

  destination_port_range  = each.value.destination_port_range
  destination_port_ranges = each.value.destination_port_ranges

  source_address_prefixes = each.value.source_address_prefixes
  source_address_prefix   = each.value.source_address_prefix

  destination_address_prefixes = (
    each.value.destination_address_prefixes
  )

  destination_address_prefix = (
    each.value.destination_address_prefix
  )

  resource_group_name = var.resource_group_name

  network_security_group_name = (
    azurerm_network_security_group.this[
      each.value.nsg_key
    ].name
  )
}

resource "azurerm_subnet_network_security_group_association" "this" {
  for_each = local.nsg_associations

  subnet_id = azurerm_subnet.this[
    each.value.subnet_key
  ].id

  network_security_group_id = each.value.network_security_group_id
}