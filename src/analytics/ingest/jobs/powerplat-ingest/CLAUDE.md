# CLAUDE.md -- Power Platform ingest

## Overview

ACA Job that captures Power Platform assets (environments, environment groups,
canvas apps, connections, custom connectors, cloud flows, Dataverse solutions
+ components, Copilot Studio agents, AI models, Power Pages) for cross-tenant
migration analysis. Lands raw JSONL to ADLS; silver-layer filtering and
dependency extraction live in DLT (#245).

Mirrors the graph/exo/spo pattern: cert-based MSAL auth via
`shared/modules/MsalTokenHelper.psm1`, REST-only data access (no Power
Platform PS modules — confirmed hard-incompatible with pwsh 7 / Linux per
spike #243), and the same StageExecutor / WorkerPool / StorageHelperRest
plumbing from `shared/modules/`.

## Auth model

- **Identity**: shared multi-tenant `azuread_application.ingest`
  (`infra/analytics/env_module/ingest_app.tf`). No new SP. Cert-based client
  credentials.
- **Tenant-wide reach**: SP must be registered as a Power Platform tenant
  management app per target tenant — see [Per-target-tenant onboarding](#per-target-tenant-onboarding-one-time)
  below.
- **Audiences**: `https://service.powerapps.com/.default` (covers BAP,
  PowerApps admin, Flow admin); per-env `https://{org}.crm.dynamics.com/.default`
  for Dataverse Web API. Audience is derived from each env's `instanceUrl`
  (non-`api.` host); REST calls go against `instanceApiUrl` (`api.` host).
  Region-agnostic — handles `crm.dynamics.com`, `crm4.dynamics.com` (EMEA),
  etc. without string surgery. One Dataverse audience cached per env in
  `$script:TokenCache`.

## Per-target-tenant onboarding (one-time)

Two steps per target tenant before the job can read its data. Skip if the
tenant has already been onboarded.

### 1. Admin consent

Same step the existing graph/exo/spo onboarding requires. Open this URL in a
browser, sign in as Global Admin / Application Admin in the target tenant,
confirm:

```
https://login.microsoftonline.com/{TARGET_TENANT_ID}/adminconsent?client_id={INGEST_SP_CLIENT_ID}&redirect_uri=https://entra.microsoft.com/
```

`{INGEST_SP_CLIENT_ID}` comes from `terraform output -raw ingest_app_client_id`
in `infra/analytics/environments/{env}/`.

### 2. Power Platform management-app registration

Registers the SP as a Power Platform tenant management app, granting it admin
equivalent on legacy BAP / PowerApps / Flow admin endpoints. As a Power
Platform Administrator in the target tenant:

```powershell
$TARGET_TENANT_ID = "<target tenant guid>"
$INGEST_SP_CLIENT_ID = "<from terraform output>"

az login --tenant $TARGET_TENANT_ID --allow-no-subscriptions
$token = (az account get-access-token --tenant $TARGET_TENANT_ID `
  --resource https://service.powerapps.com/ | ConvertFrom-Json).accessToken

Invoke-RestMethod -Method PUT `
  -Uri "https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform/adminApplications/${INGEST_SP_CLIENT_ID}?api-version=2020-10-01" `
  -Headers @{ Authorization = "Bearer $token" } `
  -Body "{}" -ContentType "application/json"
```

Idempotent — re-running is a no-op.

### 3. Verify

```powershell
Invoke-RestMethod -Method GET `
  -Uri "https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments?api-version=2020-10-01" `
  -Headers @{ Authorization = "Bearer $token" }
# Expected: list of environments in the target tenant.
```

After both steps complete, the SP can authenticate to the target tenant via
cert-based client credentials and call BAP / PowerApps admin / Flow admin
endpoints. Per-env Dataverse Application User provisioning happens
automatically inside the ingest job — `dataverse_onboardings` is a pool stage
under `environments_root`, fanning out per env. An `InputFilter` excludes
Teams-style envs (null `linkedEnvironmentMetadata.instanceApiUrl`); the
worker reads `instanceUrl` / `instanceApiUrl` from `$Context.InputTags` and
runs the WhoAmI-gated onboarding sequence (Path A re-bind / Path B
bootstrap). The pre-check keeps steady-state safe — `/addAppUser` is not
idempotent on roles. Audit trail lands in `powerplat_dataverse_onboardings`;
the 19 Dataverse data entities (PR-5b: solutions, solution_components,
connection_references, env_variable_definitions, env_variable_values; PR-6:
tables, workflows, plugin_assemblies, plugin_steps, web_resources,
app_modules, bots, bot_components, ai_models, powerpages_websites,
powerpages_components; PR-7: systemusers, publishers, mailboxes) are pool
children of this stage and inherit URL tags from it. PR-7 additionally
fans out per-row dependency-body children (workflow_definitions,
app_module_xml, bot_configurations, bot_component_data,
web_resource_contents, powerpages_component_contents) under those PR-6
parents — same `IdTags=@('instanceUrl','instanceApiUrl')` propagation —
plus an admin-plane per-row child `flow_metadata` under `flows` (non-Dataverse flows only — DV-backed flows' bodies land via `workflow_definitions`).

## Phase rollout

| Phase | Status | Entities |
|-------|--------|----------|
| 1 (PR-1) | done | `powerplat_environments` |
| 1 (PR-2) | done | `powerplat_apps` (canvas), `powerplat_connections`, `powerplat_custom_connectors` |
| 2 (PR-3) | done | `powerplat_flows` (V2 endpoint), `powerplat_app_role_assignments`, `powerplat_flow_role_assignments`, `powerplat_connection_role_assignments` |
| 3 foundation (PR-4) | done | Dataverse audience caching, `Onboard.psm1` Application User provisioning (idempotent, WhoAmI-gated), audit-ledger entity (named `powerplat_application_users` originally; renamed to `powerplat_dataverse_onboardings` in PR-5b to free the inventory name). No data entities. |
| Refactor (PR-5a) | done | Collapse five sibling modules + `Onboard.psm1` into one canonical family module `PowerPlatEnvironments.psm1` matching graph-ingest's `TeamsTeams.psm1` shape. `environments_root` inline; apps / flows / connections / custom_connectors / application_users (renamed in PR-5b) as pool children; `*_role_assignments` as pool-of-pool. Pluralize all entity names. Family folder in landing becomes `powerplat_environments/`. No new entities. Adds `$Context.InputTags` propagation in shared/ for per-input metadata to pool workers (#257). |
| 3 data (PR-5b) | done | First Phase 3 batch: `powerplat_solutions`, `powerplat_solution_components`, `powerplat_connection_references`, `powerplat_env_variable_definitions`, `powerplat_env_variable_values`. Pool children of `dataverse_onboardings` (renamed from `application_users` to avoid colliding with a future env-level Application User inventory entity). Each fetches its OData entity-set verbatim against per-env Dataverse Web API. |
| 4 (PR-6) | done | Phase 4 Dataverse batch (11 entities): `powerplat_tables` (filtered to `IsCustomEntity eq true` — only PR-6 entity with a non-trivial query shape; system-table set inflates ~500-800 rows per env at the wire), `powerplat_workflows`, `powerplat_plugin_assemblies`, `powerplat_plugin_steps`, `powerplat_web_resources` (EntitySetName `webresourceset`, irregular plural), `powerplat_app_modules`, `powerplat_bots`, `powerplat_bot_components`, `powerplat_ai_models`, `powerplat_powerpages_websites`, `powerplat_powerpages_components`. All pool children of `dataverse_onboardings`, same shape as PR-5b. |
| 4 (PR-7) | this PR | Cross-asset dependency parsing — bring back the heavy Memo columns excluded by PR-6 via per-row fan-out, plus three new bulk Dataverse tables. Per-row children (7): `powerplat_flow_metadata` (admin-plane V1 child of `flows`; lands `definitionSummary` + `connectionReferences` + `referencedResources` — NOT the executable WDL body. Gated by `InputFilter` on `workflowEntityId == null` to skip Dataverse-backed flows; their bodies come from `workflow_definitions`. Renamed from `flow_definitions` in #400 once the V2 admin per-flow endpoint was confirmed nonexistent and the V1 path was shown to expose summary metadata only, never the full definition), `powerplat_workflow_definitions` (Dataverse child of `workflows`, fetches clientdata/xaml/inputparameters — this is the actual WDL body for cloud flows; clientdata is the canonical source per `Microsoft.PowerPlatform.Management.Models.CloudFlow.Definition`), `powerplat_app_module_xml` (child of `app_modules`, fetches sitemap), `powerplat_bot_configurations` (child of `bots`, fetches `configuration`), `powerplat_bot_component_data` (child of `bot_components`, fetches `data` — Copilot Studio topic body), `powerplat_web_resource_contents` (child of `web_resources`, fetches `content`), `powerplat_powerpages_component_contents` (child of `powerpages_components`, fetches `content`+`filecontent`). New tier-3 bulk (3): `powerplat_systemusers` (filtered to `applicationid ne null` — SP-backed integration users), `powerplat_publishers` (solution publisher prefixes), `powerplat_mailboxes` (Server-Side Sync bindings). Six parents retrofitted with `EmitIds=$true` to dispatch their per-row children; `flows` additionally emits `workflowEntityId` as a tag so `flow_metadata` can filter. Validates the per-row fan-out shape on madev1 before scope expands further. |
| Modern API | later | `powerplat_environment_groups` (lives on `api.powerplatform.com` with a different audience — folded in when modern-API integration lands) |
| Canvas package spike | separate issue | `powerplat_canvas_app_packages` (per-app `.msapp` export + unpack) — needs MSAPP unpack tooling decision (`pasopa` / `pac canvas unpack` / roll own) and endpoint-quota validation, deferred per #262. |

Full plan + asset catalog in #241. Silver-layer scope in #245.

## Files

```
powerplat-ingest/
  Dockerfile                   pwsh 7.4 / lts-ubuntu-22.04. Az.Accounts + Az.KeyVault only.
  CLAUDE.md                    this file
  scripts/
    Connect.psm1               Connect-Service / Restore-ServiceConnection / Get-PowerPlatToken. Multi-audience MSAL token cache (legacy admin + per-env Dataverse). Sanity-probes BAP at startup. No env-list fetch — environments_root in PowerPlatEnvironments.psm1 IS the env-list call.
    entities/
      PowerPlatEnvironments.psm1   The whole family. environments_root (inline, BAP /scopes/admin/environments?$expand=...) emits envName + IdTags={instanceUrl, instanceApiUrl, environmentType}. Pool children (apps, flows, connections, custom_connectors, dataverse_onboardings) consume envName from InputId; dataverse_onboardings reads instanceUrl/instanceApiUrl via $Context.InputTags and re-emits them for its own pool children. Pool-of-pool grandchildren via dataverse_onboardings (19 Dataverse data entities — PR-5b: solutions, solution_components, connection_references, env_variable_definitions, env_variable_values; PR-6: tables, workflows, plugin_assemblies, plugin_steps, web_resources, app_modules, bots, bot_components, ai_models, powerpages_websites, powerpages_components; PR-7: systemusers, publishers, mailboxes) — each fetches a Dataverse OData entity-set verbatim (bronze raw). powerplat_tables filters to `IsCustomEntity eq true` to drop ~500-800 system-table rows; powerplat_systemusers filters to `applicationid ne null` to drop human users. Pool-of-pool grandchildren via apps/flows/connections (the role-assignments stages) take 'envName:::resourceId' composite from their pool parent. PR-7 adds three-deep pool fan-out for per-row dependency-body fetchers: workflows/app_modules/bots/bot_components/web_resources/powerpages_components each emit per-row IDs to a child stage (workflow_definitions, app_module_xml, bot_configurations, bot_component_data, web_resource_contents, powerpages_component_contents), and flows similarly emits to admin-plane flow_metadata (non-Dataverse flows only — InputFilter gates on workflowEntityId). Each per-row child uses tight `$select` to pull just the heavy Memo column(s) one row at a time. Inlines the PR-4 onboarding logic (WhoAmI pre-check + Path A/B), helpers (Invoke-AdminApiRequest, Invoke-DataverseRequest, Get-DataverseAudience), and per-role binding/revocation. Family-folder layout in landing: powerplat_environments/<tenant>/<date>/{apps,flows,connections,custom_connectors,dataverse_onboardings,solutions,solution_components,connection_references,env_variable_definitions,env_variable_values,tables,workflows,plugin_assemblies,plugin_steps,web_resources,app_modules,bots,bot_components,ai_models,powerpages_websites,powerpages_components,systemusers,publishers,mailboxes,flow_metadata,workflow_definitions,app_module_xml,bot_configurations,bot_component_data,web_resource_contents,powerpages_component_contents,app_role_assignments,flow_role_assignments,connection_role_assignments}/.
```
