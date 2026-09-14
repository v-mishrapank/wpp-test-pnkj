FROM mcr.microsoft.com/powershell:lts-ubuntu-22.04

# REST-only — we don't install MicrosoftPowerBIMgmt because the container's
# auth path is unattended cert-based, and the module's cert-flow has Windows
# .NET Framework dependencies that don't apply under pwsh 7 / Linux. We use
# raw MSAL via Az.Accounts (which bundles MSAL.NET, loaded by
# MsalTokenHelper.psm1). The operator-run scripts/Add-IngestGatewayAdmin.ps1
# bootstrap is also REST-only — it uses Azure CLI for interactive auth, not
# MicrosoftPowerBIMgmt (its Invoke-PowerBIRestMethod has a URL-composition
# bug on cross-platform pwsh).
#
# Modules pinned 2026-08-10 (canonical set + bump cadence:
# docs/analytics/ingest-image-dependencies.md). Unpinned installs would let a
# malicious/breaking PSGallery release into the next build silently.
RUN pwsh -Command "Set-PSRepository PSGallery -InstallationPolicy Trusted; \
    Install-Module Az.Accounts -RequiredVersion 5.5.2 -Force -Scope AllUsers; \
    Install-Module Az.KeyVault -RequiredVersion 6.6.0 -Force -Scope AllUsers"

# Copy shared modules and entry point
COPY jobs/shared/modules/ /app/modules/
COPY jobs/shared/scripts/Invoke-Ingestion.ps1 /app/

# Container-specific files. Three family modules: tenant-singletons,
# gateways, workspaces. The repo-root scripts/Add-IngestGatewayAdmin.ps1
# bootstrap is intentionally NOT copied — it's customer-run, not container code.
COPY jobs/powerbi-ingest/scripts/Connect.psm1 /app/
COPY jobs/powerbi-ingest/scripts/entities/ /app/entities/

WORKDIR /app

# Non-root, like src/automation/worker. Writes hit /tmp only (incl. scan cache), so /app needs no chown.
RUN useradd --create-home --shell /bin/bash ingest
USER ingest

ENTRYPOINT ["pwsh", "-File", "/app/Invoke-Ingestion.ps1"]
