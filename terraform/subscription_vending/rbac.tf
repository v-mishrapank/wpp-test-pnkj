
# # Getting subscription data mapping names to IDs, so we can reference subscriptions by name in role assignment scope
# data "azurerm_subscription" "subscriptions" {
#   for_each        = module.wpp-wppit-subscriptions
#   subscription_id = each.value.subscription.subscription_id
# }

# Assigning built in Azure role
resource "azurerm_role_assignment" "subsriptions" {
  for_each             = { for user in local.ucp_member_entra_ids : user.email_address => user }
  scope                = data.azurerm_subscription.subscriptions["sub-wpp-wppet-ucp-example-d-001"].id
  role_definition_name = "Contributor"
  principal_id         = each.value.object_id
  depends_on = [ azurerm_role_assignment.spn_role_assignment ]
}

resource "azurerm_role_assignment" "appreg_to_subscriptions" {
  for_each             = { for spn in local.ucp_spn_assignments : spn.service_principal_name => spn }
  scope                = data.azurerm_subscription.subscriptions["sub-wpp-wppet-ucp-example-d-001"].id
  role_definition_name = each.value.role
  principal_id         = each.value.service_principal_object_id
  depends_on           = [azurerm_role_assignment.spn_role_assignment]
}


# # Assigning custom role via scoped-ID https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment#role_definition_id
# resource "azurerm_role_assignment" "my_custom_role_example_subscription01" {
#   scope              = data.azurerm_subscription.subscriptions["sub-wppit-cloudhub-poe-eg-x-01"].id
#   role_definition_id = "/subscriptions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/providers/Microsoft.Authorization/roleDefinitions/6977B55A-BD46-4FDA-B40C-3743B61462D9" # This should correspond to the custom "My Custom Role" role (6977B55A-BD46-4FDA-B40C-3743B61462D9) at this subscriptions scope,
#   principal_id       = local.sampleuser_obj_id
# }
