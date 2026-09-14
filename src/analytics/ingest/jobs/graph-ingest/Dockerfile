FROM mcr.microsoft.com/powershell:lts-ubuntu-22.04

LABEL org.opencontainers.image.source="https://github.com/microsoft/ma-toolkit"

# Modules pinned 2026-08-10 (canonical set + bump cadence:
# docs/analytics/ingest-image-dependencies.md). Unpinned installs would let a
# malicious/breaking PSGallery release into the next build silently.
RUN pwsh -Command "Set-PSRepository PSGallery -InstallationPolicy Trusted; \
    Install-Module Microsoft.Graph.Authentication -RequiredVersion 2.39.0 -Force -Scope AllUsers; \
    Install-Module Az.Accounts -RequiredVersion 5.5.2 -Force -Scope AllUsers; \
    Install-Module Az.KeyVault -RequiredVersion 6.6.0 -Force -Scope AllUsers; \
    Install-Module Az.Storage -RequiredVersion 9.7.2 -Force -Scope AllUsers"

# Copy shared modules and entry point
COPY jobs/shared/modules/ /app/modules/
COPY jobs/shared/scripts/Invoke-Ingestion.ps1 /app/

# Copy container-specific files
COPY jobs/graph-ingest/scripts/Connect.psm1 /app/
COPY jobs/graph-ingest/scripts/entities/ /app/entities/

WORKDIR /app

# Non-root, like src/automation/worker. Writes hit /tmp only, so /app needs no chown.
RUN useradd --create-home --shell /bin/bash ingest
USER ingest

ENTRYPOINT ["pwsh", "-File", "/app/Invoke-Ingestion.ps1"]
