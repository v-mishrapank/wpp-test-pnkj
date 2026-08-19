// Ingest telemetry Workbook scoped to the deployment's Log Analytics
// workspace. The JSON currently provides a deployable overview shell;
// add telemetry queries and tabs to workbook_ingest.json as they are defined.
//
// JSON body lives in workbook_ingest.json so it can be diffed in PRs.
// Edit the JSON, terraform apply, and the portal reflects the change —
// don't author in the portal and re-export, that path drifts.
//
// `name` requires a GUID (azurerm provider constraint). uuidv5 derives a
// stable GUID from the env-scoped resource group so each env owns its own
// Workbook resource without collision.
resource "azurerm_application_insights_workbook" "ingest" {
  name                = uuidv5("dns", "${azurerm_resource_group.wpp_cloud.name}-ingest-telemetry-workbook")
  resource_group_name = azurerm_resource_group.wpp_cloud.name
  location            = azurerm_resource_group.wpp_cloud.location
  display_name        = "Ingest telemetry"
  # LAW-scoped Workbook — queries run against ContainerAppConsoleLogs_CL.
  # Source_id pinning the workbook to the LAW resource lands it under the
  # workspace's Workbooks tab in the portal.
  source_id = lower(module.log_analytics.workspace_id)
  category  = "workbook"
  data_json = file("${path.module}/workbook_ingest.json")
  tags      = local.common_tags
}
