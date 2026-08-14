module "wpp-wppit-subscriptions" {
  for_each                = { for sub_detail in var.sub_detail : sub_detail.subscription_name => sub_detail }
  source                  = "app.terraform.io/wpp-cloudhub/wpp-wppit-subscriptions/azure"
  version                 = "0.0.2"
  billing_account_name    = var.billing_account_name
  enrollment_account_name = var.enrollment_account_name
  subscription            = each.key
  management_group_id     = var.management_group_id
}

data "azurerm_subscription" "subscriptions" {
  for_each        = module.wpp-wppit-subscriptions
  subscription_id = each.value.subscription.subscription_id
}

resource "azurerm_role_assignment" "spn_role_assignment" {
  for_each             = { for sub_detail in var.sub_detail : sub_detail.subscription_name => sub_detail if sub_detail.service_principal_object_id != null }
  scope                = data.azurerm_subscription.subscriptions[each.key].id
  role_definition_name = each.value.role
  principal_id         = each.value.service_principal_object_id
}
