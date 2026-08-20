data "azuread_user" "windows_vm_users" {
  for_each = var.user_principal_names

  user_principal_name = each.value
}

resource "azurerm_resource_group" "windows_vms" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_network_security_group" "windows_vms" {
  name                = "nsg-${var.application_resource_prefix}-vm-001"
  location            = azurerm_resource_group.windows_vms.location
  resource_group_name = azurerm_resource_group.windows_vms.name
  tags                = var.tags
}

resource "azurerm_subnet_network_security_group_association" "windows_vms" {
  subnet_id                 = var.subnet_id
  network_security_group_id = azurerm_network_security_group.windows_vms.id
}

resource "azurerm_network_interface" "windows_vms" {
  for_each = var.virtual_machines

  name                = "nic-${var.application_resource_prefix}-${each.key}-001"
  location            = azurerm_resource_group.windows_vms.location
  resource_group_name = azurerm_resource_group.windows_vms.name
  tags                = var.tags

  ip_configuration {
    name                          = "internal"
    subnet_id                     = var.subnet_id
    private_ip_address_allocation = "Dynamic"
  }
}

resource "random_password" "windows_vm_admin" {
  for_each = var.virtual_machines

  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}:?"
}

resource "azurerm_windows_virtual_machine" "windows_vms" {
  for_each = var.virtual_machines

  name                  = each.value.name
  computer_name         = each.value.computer_name
  resource_group_name   = azurerm_resource_group.windows_vms.name
  location              = azurerm_resource_group.windows_vms.location
  size                  = var.vm_size
  zone                  = each.value.zone
  admin_username        = var.admin_username
  admin_password        = random_password.windows_vm_admin[each.key].result
  network_interface_ids = [azurerm_network_interface.windows_vms[each.key].id]

  secure_boot_enabled        = true
  vtpm_enabled               = true
  patch_mode                 = "AutomaticByPlatform"
  patch_assessment_mode      = "AutomaticByPlatform"
  encryption_at_host_enabled = true

  identity {
    type = "SystemAssigned"
  }

  os_disk {
    name                 = "osdisk-${var.application_resource_prefix}-${each.key}-001"
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = 512
  }

  source_image_reference {
    publisher = "MicrosoftWindowsServer"
    offer     = "WindowsServer"
    sku       = "2022-datacenter-azure-edition"
    version   = "latest"
  }

  boot_diagnostics {}

  tags = var.tags
}

resource "azurerm_virtual_machine_extension" "entra_login" {
  for_each = azurerm_windows_virtual_machine.windows_vms

  name                       = "AADLoginForWindows"
  virtual_machine_id         = each.value.id
  publisher                  = "Microsoft.Azure.ActiveDirectory"
  type                       = "AADLoginForWindows"
  type_handler_version       = "1.0"
  automatic_upgrade_enabled  = true
  auto_upgrade_minor_version = true
  tags                       = var.tags
}

resource "azurerm_security_center_subscription_pricing" "defender_for_servers" {
  resource_type = "VirtualMachines"
  tier          = "Standard"
  subplan       = "P2"
}

resource "azurerm_resource_group_template_deployment" "windows_vm_jit" {
  name                = "deploy-${var.application_resource_prefix}-vm-jit-001"
  resource_group_name = azurerm_resource_group.windows_vms.name
  deployment_mode     = "Incremental"

  template_content = jsonencode({
    "$schema"      = "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#"
    contentVersion = "1.0.0.0"
    resources = [
      {
        type       = "Microsoft.Security/locations/jitNetworkAccessPolicies"
        apiVersion = "2020-01-01"
        name       = "${azurerm_resource_group.windows_vms.location}/default"
        kind       = "Basic"
        properties = {
          virtualMachines = [
            for vm in azurerm_windows_virtual_machine.windows_vms : {
              id = vm.id
              ports = [
                {
                  number                       = 3389
                  protocol                     = "TCP"
                  allowedSourceAddressPrefixes = var.jit_allowed_source_address_prefixes
                  maxRequestAccessDuration     = "PT3H"
                }
              ]
            }
          ]
        }
      }
    ]
  })

  depends_on = [
    azurerm_security_center_subscription_pricing.defender_for_servers,
    azurerm_subnet_network_security_group_association.windows_vms
  ]
}

resource "azurerm_role_definition" "vm_jit_requester" {
  name        = "${var.application_resource_prefix}-vm-jit-requester"
  scope       = azurerm_resource_group.windows_vms.id
  description = "Request JIT access and read connection metadata for the isolated Windows VMs"

  permissions {
    actions = [
      "Microsoft.Compute/virtualMachines/read",
      "Microsoft.Network/networkInterfaces/read",
      "Microsoft.Network/networkSecurityGroups/read",
      "Microsoft.Network/publicIPAddresses/read",
      "Microsoft.Security/locations/jitNetworkAccessPolicies/read",
      "Microsoft.Security/locations/jitNetworkAccessPolicies/initiate/action"
    ]
  }

  assignable_scopes = [azurerm_resource_group.windows_vms.id]
}

resource "azurerm_role_assignment" "windows_vm_login" {
  for_each = data.azuread_user.windows_vm_users

  scope                = azurerm_resource_group.windows_vms.id
  role_definition_name = "Virtual Machine User Login"
  principal_id         = each.value.object_id
}

resource "azurerm_role_assignment" "windows_vm_jit_requester" {
  for_each = data.azuread_user.windows_vm_users

  scope              = azurerm_resource_group.windows_vms.id
  role_definition_id = azurerm_role_definition.vm_jit_requester.role_definition_resource_id
  principal_id       = each.value.object_id
}