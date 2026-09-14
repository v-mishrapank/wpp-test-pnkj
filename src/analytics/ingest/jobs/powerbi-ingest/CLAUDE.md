# CLAUDE.md -- Power BI / Fabric ingest

## Overview

ACA Job that captures Power BI / Fabric tenant assets (workspaces, datasets,
dataflows, reports, dashboards, apps, deployment pipelines, capacities, on-prem
data gateways, Fabric items) for cross-tenant migration analysis. Lands raw
JSONL to ADLS; silver-layer filtering and dependency extraction live in DLT
(#245).

Mirrors the powerplat / graph / exo / spo pattern: cert-based MSAL auth via
`shared/modules/MsalTokenHelper.psm1`, REST-only data access (no Power BI
PowerShell modules — `MicrosoftPowerBIMgmt` is .NET-Framework-only and hard-
incompatible with pwsh 7 / Linux), and the same StageExecutor / WorkerPool /
StorageHelperRest plumbing from `shared/modules/`.

## Auth model

- **Identity**: shared multi-tenant `azuread_application.ingest`
  (`infra/analytics/env_module/ingest_app.tf`). No new SP. Cert-based client
  credentials.
- **Audiences**: `https://analysis.windows.net/powerbi/api/.default` for
  Power BI / V2 gateway endpoints on `api.powerbi.com`, and
  `https://api.fabric.microsoft.com/.default` for Fabric items admin on
  `api.fabric.microsoft.com`. The token grants the *audience*; tenant-side
  authorization differs per endpoint family — see onboarding steps 2 and 3.
- **Sanity probe**: `GET /v1.0/myorg/admin/capacities` at startup. Confirms
  cert-auth + Fabric-admin SP toggle in one shot before any entity fetch.
  Does *not* probe the V2 gateway surface — gateway tenants without the
  step-3 bootstrap simply produce empty / 401 manifests on those entities,
  which is the explicit signal that the bootstrap is needed.

## Per-target-tenant onboarding (one-time)

Three steps per target tenant before the job can read its data. Skip if the
tenant has already been onboarded.

### 1. Admin consent

Same step graph / exo / spo / powerplat onboarding requires. Open this URL in a
browser, sign in as Global Admin / Application Admin in the target tenant,
confirm:

```
https://login.microsoftonline.com/{TARGET_TENANT_ID}/adminconsent?client_id={INGEST_SP_CLIENT_ID}&redirect_uri=https://entra.microsoft.com/
```

`{INGEST_SP_CLIENT_ID}` comes from `terraform output -raw ingest_app_client_id`
in `infra/analytics/environments/{env}/`.

### 2. Fabric admin SP toggle

Power BI / Fabric admin endpoints check tenant-level Fabric admin role for
service principals. The shared SP is **not** allowed by default — admin
consent grants the OAuth scope but the Fabric admin role gate is a separate
tenant setting.

In the target tenant's Fabric admin portal (`app.fabric.microsoft.com` →
Admin portal → Tenant settings → Admin API settings), enable:

- **Service principals can access read-only admin APIs**

Set the toggle to **Enabled**, scope to **Specific security groups**, add a
group containing `azuread_service_principal.ingest`, and click **Apply**.

This toggle gates the read-only **admin** surface we use: `/v1.0/myorg/admin/*`,
the workspace scan endpoints (`getInfo` / `scanStatus` / `scanResult`), and
the Fabric items admin endpoints (`/v1/admin/items?type=...`). The
neighboring "Service principals can access admin APIs used for updates"
toggle is for write operations (e.g., Restore Workspace) and is **not**
needed — our entire scope is read-only.

It does **not** gate `/v2.0/myorg/gatewayClusters/*` — those endpoints check
per-cluster gateway-admin role, which is granted in step 3 below.

This is a tenant-setting toggle, not an Azure permission — there's no
Terraform / Graph API equivalent. Per-tenant manual step.

### 3. Gateway cluster admin bootstrap

V2 gateway endpoints (`/v2.0/myorg/gatewayClusters/...`) gate by per-cluster
gateway-admin role, not the Fabric admin SP toggle. The SP must be added as
Admin on each cluster individually — there is no tenant-level admin override.

Why we need these endpoints: per-data-source `credentialType` and
`singleSignOnType` (Kerberos / KerberosDirectQueryAndRefresh / SAML / etc.)
only surface here. The workspace scan API doesn't expose them, and they're
load-bearing for migration assessment because Kerberos-impersonation gateways
imply hybrid-identity / AD Connect / KCD requirements on the destination
tenant.

The repo-root `scripts/Add-IngestGatewayAdmin.ps1` bootstrap automates the
per-cluster loop. Cross-platform pwsh 7+ (Mac, Linux, Windows) using
Azure CLI (`az login` browser-redirect + `az account get-access-token`)
for interactive auth — not the `MicrosoftPowerBIMgmt` module, because its
`Invoke-PowerBIRestMethod` has a "file scheme is not supported" URL bug
on cross-platform pwsh and `Connect-PowerBIServiceAccount` falls back to
device-code on Mac. Run once per target tenant as a Fabric Administrator. See
[`docs/tenant-onboarding.md`](../../../../docs/tenant-onboarding.md#2c-powerbi-ingest--gateway-cluster-admin-bootstrap)
for the full call sequence.

Idempotent — re-running skips clusters where the SP is already Admin.

### 4. Verify

```powershell
# Acquire a token for the SP, then:
Invoke-RestMethod -Method GET `
  -Uri 'https://api.powerbi.com/v1.0/myorg/admin/capacities' `
  -Headers @{ Authorization = "Bearer $token" }
# Expected: 200 with .value (possibly empty if tenant has no Premium capacities).
```

After both steps complete, the SP can read all Power BI / Fabric admin endpoints
in the target tenant.

## Phase rollout

| Phase | Status | Entities |
|-------|--------|----------|
| C1 | done | `powerbi_capacities` — smoke test of infra + dispatcher + auth chain |
| C2 | done | Tenant singletons: `powerbi_apps`, `powerbi_deployment_pipelines`, `powerbi_fabric_lakehouses/warehouses/kql_databases/notebooks` (Fabric audience added) |
| C3 | done† | Gateways family: `powerbi_gateway_clusters`, `powerbi_gateway_cluster_data_sources`, `powerbi_gateway_cluster_permissions` |
| C4 | done | Workspaces foundation: `workspaces_root` (async scan + /tmp cache) + `powerbi_workspaces`. Validated end-to-end against madev1 (5 workspaces) + madev2 (4 workspaces). |
| C5 | done | Workspaces children: 13 pool entities reading from /tmp + `powerbi_dataset_refresh_schedules` pool-of-pool. Validated end-to-end against madev1+madev2 (refresh-schedule admin endpoint returned rich data — confirms the entity is keepworthy). |
| C6 | done | Docs polish + PR description |

Full plan + asset catalog in #242 ([implementation plan comment](https://github.com/microsoft/ma-toolkit/issues/242#issuecomment-4383508523)).

† **C3 caveat**: code-complete and merged; the gateway entities and onboarding bootstrap (`scripts/Add-IngestGatewayAdmin.ps1`) are not yet end-to-end smoke-tested because both `madev1` and `madev2` have zero gateway clusters. Tracked in #295: provision a real test cluster + harden the bootstrap script for the Windows-on-prem M365 admin persona.

## Files

```
powerbi-ingest/
  Dockerfile                       pwsh 7.4 / lts-ubuntu-22.04. Az.Accounts + Az.KeyVault only.
  CLAUDE.md                        this file
  scripts/
    Connect.psm1                   Connect-Service / Restore-ServiceConnection / Get-PowerBiToken
                                   + shared REST helpers (Invoke-PowerBiRequest,
                                   Invoke-PowerBiPagedFetch, Invoke-FabricPagedFetch,
                                   ConvertTo-PowerBiArray). Per-audience MSAL token cache.
                                   Sanity-probes /admin/capacities at startup.
    entities/
      PowerBiTenant.psm1           Tenant-singleton entities: capacities, apps,
                                   deployment_pipelines, 4× fabric_* items.
      PowerBiGateways.psm1         Gateway clusters family. Requires onboarding step 3.
      PowerBiWorkspaces.psm1       Workspace family — workspaces_root (async scan +
                                   /tmp cache) + 13 pool slicers + dataset_refresh
                                   pool-of-pool. 16 stages, 16 entities.

The gateway-admin onboarding bootstrap script lives at the repo root under
`scripts/Add-IngestGatewayAdmin.ps1` (NOT in this container's scripts/ dir —
it's customer-run, not container code). See onboarding step 3.
```

## Notes for future readers

- **/tmp scan cache**: `workspaces_root` downloads every `scanResult/{scanId}.json`
  payload to `/tmp/powerbi-scans/` before emitting IdEvents. Pool children
  (`powerbi_workspaces` and the 13 C5 slicers) read from disk — zero network I/O.
  Don't "fix" this by writing to ADLS: Microsoft's scanResult TTL is 24 hr and
  ACA Job retry policy reruns the whole execution from scratch on container
  restart, so per-execution `/tmp` is the right boundary.
- **Throttle ceiling**: `getInfo` (scan submit) caps at ~30 / hour per tenant.
  A 3,000-workspace tenant takes 30 submits = exactly at ceiling. Per-resource
  admin endpoints (datasets, reports, ...) burst around 200 / minute.
- **Two endpoint authorization gates, not one**: the Fabric admin SP toggle
  gates `/v1.0/myorg/admin/*` and the Fabric items admin endpoints. It does
  **not** gate `/v2.0/myorg/gatewayClusters/*` — those check per-cluster
  gateway-admin role, granted via the bootstrap script in onboarding step 3.
  See `docs/tenant-onboarding.md` §2c.
- **Refresh-schedule admin endpoint exists despite docs**: `/v1.0/myorg/admin/datasets/{id}/refreshSchedule`
  works for SP auth and returns rich schedule data, even though it's not in
  Microsoft Learn's published list of 44 supported admin SP APIs. Confirmed
  in branch-env smoke against madev1+madev2.
- **Stage `EmitIds`/`IdKey` are metadata-only**: declaring them does NOT cause
  the framework to auto-extract IDs from `WriteRecord` outputs. Fetchers must
  call `$Writer.EmitId($id, $null)` explicitly (same convention as
  `EntraGroups.psm1`, etc.). Forgetting it makes downstream pool stages
  silently land `record_count=0` with no errors. Tracked in #297 as a
  framework hardening.
