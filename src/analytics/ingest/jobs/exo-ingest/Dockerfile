FROM mcr.microsoft.com/powershell:lts-ubuntu-22.04

LABEL org.opencontainers.image.source="https://github.com/microsoft/ma-toolkit"

# Modules pinned 2026-08-13 (canonical set + bump cadence:
# docs/analytics/ingest-image-dependencies.md). ExchangeOnlineManagement 3.10.1
# is the latest stable PSGallery release (published 2026-07-24). It is pinned
# to the latest stable version after successful runs on the test tenants.
# Large-tenant scaling is tracked in #158.


RUN pwsh -Command "Set-PSRepository PSGallery -InstallationPolicy Trusted; \
    Install-Module ExchangeOnlineManagement -RequiredVersion 3.10.1 -Force -Scope AllUsers; \
    Install-Module Az.Accounts -RequiredVersion 5.5.2 -Force -Scope AllUsers; \
    Install-Module Az.KeyVault -RequiredVersion 6.6.0 -Force -Scope AllUsers"

# Copy shared modules and entry points (Invoke-WorkerChunk.ps1 is the
# child process entry point for WORKER_POOL_MODE=outofprocess; see
# WorkerPool.psm1's Invoke-StagePoolOutOfProcess for the spawn site).
COPY jobs/shared/modules/ /app/modules/
COPY jobs/shared/scripts/Invoke-Ingestion.ps1 /app/
COPY jobs/shared/scripts/Invoke-WorkerChunk.ps1 /app/

# Copy container-specific files
COPY jobs/exo-ingest/scripts/Connect.psm1 /app/
COPY jobs/exo-ingest/scripts/entities/ /app/entities/

# Out-of-process worker pool (#484). Recycles the pwsh process between
# chunks so per-call EXO module leaks die with the child instead of
# accumulating across the run. Removing WORKER_POOL_MODE (or setting it
# to any other value) restores the original in-process RunspacePool path.
ENV WORKER_POOL_MODE=outofprocess

# Container-specific concurrency cap. Tenants.json MAX_PARALLELISM is the
# per-customer knob; this env var is the per-container "we can't safely
# run more than N workers in 4 GiB regardless of customer config" ceiling.
# Effective PoolSize = min(MAX_PARALLELISM, WORKER_POOL_OOP_MAX).
# Memory math: parent (~1.3 GB after inline stages) + 5 children (~130MB
# each) ≈ 2 GB total, well under the 4 GiB cgroup limit. Raise this when
# moving to a larger ACA workload profile (E4/E8). See WorkerPool.psm1
# for the full rationale.
ENV WORKER_POOL_OOP_MAX=5

WORKDIR /app

# Non-root, like src/automation/worker. Writes hit /tmp only (temp root, oopworker, PFX), so /app needs no chown.
RUN useradd --create-home --shell /bin/bash ingest
USER ingest

ENTRYPOINT ["pwsh", "-File", "/app/Invoke-Ingestion.ps1"]
