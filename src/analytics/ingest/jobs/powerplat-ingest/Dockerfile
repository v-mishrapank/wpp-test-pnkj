FROM mcr.microsoft.com/powershell:lts-ubuntu-22.04

# No Power Platform PS modules — Microsoft.PowerApps.Administration.PowerShell
# is hard-incompatible with pwsh 7 / Linux (.NET Framework only). Spike #243
# verified the REST-only path covers all entities in our catalog. Az.Accounts
# bundles MSAL.NET, which MsalTokenHelper.psm1 loads at runtime.
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

# Copy container-specific files. PR-5a collapsed the per-area modules into a
# single PowerPlatEnvironments.psm1 family module (canonical multi-stage
# pattern, mirrors graph-ingest's TeamsTeams.psm1) — Onboard.psm1 inlined.
COPY jobs/powerplat-ingest/scripts/Connect.psm1 /app/
COPY jobs/powerplat-ingest/scripts/entities/ /app/entities/

WORKDIR /app

# Non-root, like src/automation/worker. Writes hit /tmp only, so /app needs no chown.
RUN useradd --create-home --shell /bin/bash ingest
USER ingest

ENTRYPOINT ["pwsh", "-File", "/app/Invoke-Ingestion.ps1"]
