# CLAUDE.md -- Ingest Dispatcher

## Project Overview

C# Azure Functions project using the **isolated worker model** (.NET 10, Functions v4). Replaces Azure Data Factory for scheduling and dispatching analytics ingestion ACA container jobs (graph-ingest, exo-ingest, spo-ingest).

Configuration lives in JSON files deployed with the app (`config/`). Run history is written to ADLS as JSONL, queryable via DLT in Databricks. Overlap protection uses blob-based tracking via `RunTracker` (active-run state stored in ADLS so it survives Function App restarts and scales horizontally).

## Build and Run

```bash
# Build
dotnet build src/analytics/ingest/dispatcher/

# Run locally (requires Azure Functions Core Tools v4)
cd src/analytics/ingest/dispatcher/src/IngestDispatcher.Functions

# First time only: copy the template and fill in the required Ingest__* values
# (PowerShell shown; on bash/macOS/Linux use `cp`, on Windows cmd use `copy`)
Copy-Item local.settings.json.example local.settings.json

func start
```

## Tests

```bash
# Run tests
dotnet test src/analytics/ingest/dispatcher/

# Run with verbose output
dotnet test src/analytics/ingest/dispatcher/ --verbosity normal
```

## Directory Structure

```
src/analytics/ingest/dispatcher/
  IngestDispatcher.slnx
  CLAUDE.md
  src/
    IngestDispatcher.Functions/
      IngestDispatcher.Functions.csproj
      Program.cs
      host.json
      local.settings.json.example
      Settings/
        IngestSettings.cs
      Models/
        EntityType.cs
        TenantConfig.cs
        JobDefinition.cs
        StorageConfig.cs
        RunRecord.cs
        TaskRecord.cs
      Services/
        ConfigLoader.cs          -- Loads + validates all 4 JSON config files
        EntityResolver.cs        -- EntitySelector → resolved entity list
        TenantResolver.cs        -- TenantSelector → resolved tenant list
        AcaJobClient.cs          -- ARM REST client for Container App Jobs (start/status/template)
        RunExecutor.cs           -- High-level dispatch + tracking coordination
        RunTracker.cs            -- Blob-based overlap protection
        RunHistoryWriter.cs      -- JSONL run/task records to ADLS
        CronStateStore.cs        -- Persisted last-fired time per job
      Functions/
        IngestTimerFunction.cs   -- Scheduled trigger (cron evaluation per job)
        ManualRunFunction.cs     -- HTTP trigger for ad-hoc runs
        PreviewFunction.cs       -- HTTP trigger for dry-run plan preview
  tests/
    IngestDispatcher.Functions.Tests/
      EntityResolverTests.cs     -- 9 tests: tier selection, include/exclude, ordering
      TenantResolverTests.cs     -- 6 tests: all/specific/all_except modes, disabled filtering
```

## Configuration

Config files live in `config/` and are deployed with the app via `CopyToOutputDirectory`:

- `entity-registry.json` -- Entity type catalog (name, tier 0-5, container)
- `tenants.json` -- Tenant identity + credential references
- `jobs.json` -- Job definitions with cron + entity/tenant selectors
- `storage.json` -- Deployment-wide storage target (account URL, auth method)

Function App app settings are bound under the `Ingest` section (env-var prefix `Ingest__`).

## Key Architecture Decisions

- **No SQL database**: All config is JSON files. Run history and overlap-protection state both go to ADLS as blobs (`RunHistoryWriter` + `RunTracker`), under the `_dispatcher/` prefix on the analytics landing container.
- **Entity selection algebra**: `(entities in included tiers) ∪ include_entities − exclude_entities`
- **Tenant config as env vars**: Dispatcher passes all tenant config to containers as environment variables. No KV tenant registry.
- **Configurable storage auth**: Supports managed identity (default) and service principal (for customer-managed ADLS or Fabric/OneLake).
- **DLT is independent**: DLT pipelines run on their own Databricks Workflows schedule, not managed by this dispatcher.
