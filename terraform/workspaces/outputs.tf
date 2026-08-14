output "tfc_workspaces" {
  value = flatten([for ws_name, config in module.workspaces : config.azure_federated_credential_config])
}