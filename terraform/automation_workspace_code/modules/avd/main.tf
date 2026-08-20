data "azuread_user" "users" {
  for_each = var.user_principal_names

  user_principal_name = each.value
}

resource "azurerm_virtual_desktop_host_pool" "main" {
  name                     = var.host_pool_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  type                     = "Pooled"
  load_balancer_type       = "BreadthFirst"
  preferred_app_group_type = "Desktop"
  maximum_sessions_allowed = var.maximum_sessions_allowed
  start_vm_on_connect      = true
  custom_rdp_properties    = "targetisaadjoined:i:1;"
  friendly_name            = var.friendly_name
  description              = "Azure Virtual Desktop host pool for ${var.friendly_name}"
  tags                     = var.tags
}

resource "azurerm_virtual_desktop_workspace" "main" {
  name                = var.workspace_name
  resource_group_name = var.resource_group_name
  location            = var.location
  friendly_name       = var.friendly_name
  description         = "Azure Virtual Desktop workspace for ${var.friendly_name}"
  tags                = var.tags
}

resource "azurerm_virtual_desktop_application_group" "desktop" {
  name                         = var.application_group_name
  resource_group_name          = var.resource_group_name
  location                     = var.location
  type                         = "Desktop"
  host_pool_id                 = azurerm_virtual_desktop_host_pool.main.id
  friendly_name                = var.friendly_name
  default_desktop_display_name = var.friendly_name
  description                  = "Full desktop access for ${var.friendly_name}"
  tags                         = var.tags
}

resource "azurerm_virtual_desktop_workspace_application_group_association" "desktop" {
  workspace_id         = azurerm_virtual_desktop_workspace.main.id
  application_group_id = azurerm_virtual_desktop_application_group.desktop.id
}

resource "time_rotating" "registration" {
  rotation_days = 27
}

resource "azurerm_virtual_desktop_host_pool_registration_info" "main" {
  hostpool_id     = azurerm_virtual_desktop_host_pool.main.id
  expiration_date = timeadd(time_rotating.registration.id, "648h")
}

resource "azurerm_role_assignment" "desktop_users" {
  for_each = data.azuread_user.users

  scope                = azurerm_virtual_desktop_application_group.desktop.id
  role_definition_name = "Desktop Virtualization User"
  principal_id         = each.value.object_id
}
