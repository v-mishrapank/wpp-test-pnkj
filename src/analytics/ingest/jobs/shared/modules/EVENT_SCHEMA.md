# Ingest event schema (v1)

Frozen contract for structured-event telemetry emitted by the PowerShell ingest jobs. Tracks [#264](https://github.com/microsoft/ma-toolkit/issues/264).

Events live on the same stdout pipe as human prose logs, so a single LAW row carries both. The structured payload sits at the end of the prose line, separated by the literal token `_event:`. A `Write-Log` guard in `LogHelper.psm1` rejects any prose message that contains `_event:`, so this sentinel is unambiguous to parse.

Stdout → `ContainerAppConsoleLogs_CL` is the only transport. Application Insights as a parallel transport was prototyped in PR2 (#271) and dropped — the workload is batch-shaped (no AI SDK auto-correlation to benefit from), workspace-based AI lands in the same LAW workspace anyway, and a hand-rolled REST emitter introduced silent failures without a reliability win. If a future requirement (Live Metrics stream, AI portal UI) demands AI as a transport, the cleanest path is a downstream forwarder that reads `ContainerAppConsoleLogs_CL` and pushes to AI as `customEvents`, leaving the producer untouched.

## Transport

Format on stdout:

```
[2026-05-03T12:34:56.789Z] [INFO] [tenant=madev1, entity=entra_groups] Stage 'entra_groups_root' started input_count=18432 _event:{"schema_version":1,"ts":"2026-05-03T12:34:56.789Z",...}
```

## Base properties (every event)

| Field | Type | Notes |
|---|---|---|
| `schema_version` | int | Always `1`. Bumped only on a breaking change with parallel emit. |
| `ts` | string | ISO8601 with millis (`yyyy-MM-ddTHH:mm:ss.fffZ`). |
| `run_id` | string | 12 hex chars from the dispatcher (`Guid.NewGuid().ToString("N")[..12]`). PS-side falls back to a locally generated 12-char hex if `RUN_ID` env var is unset (manual `docker run`). |
| `tenant` | string | Tenant key (e.g. `madev1`). |
| `entity` | string | Entity name when scoped (e.g. `entra_groups`). May be empty for run-level events. |
| `stage` | string | Stage name when scoped (e.g. `entra_groups_root`). May be empty for run-level events. |
| `event_type` | string | One of the enum values below. |

Container/replica context is intentionally absent from the schema body — `ContainerAppConsoleLogs_CL` carries `ContainerName_s`, `RevisionName_s`, and `ContainerImage_s` columns alongside the parsed event, which is what KQL groups by when displaying API family.

## Event types

| `event_type` | Extra properties | Emitted by |
|---|---|---|
| `run_started` | `input_count` (count of wanted entities) | `Invoke-Ingestion.ps1` after env-var read |
| `stage_started` | `input_count` (pool input ID count; `0` for empty-input pool stages; `null` for inline stages) | `StageExecutor.Invoke-ModuleRun` at top of stage loop |
| `stage_progress` | `records_so_far`, `slice_index` (0-based runspace ID, present only for pool stages), `items_processed` / `items_failed` / `items_skipped` (pool only — per-input-item terminal counts; null for inline stages, see #383) | `WorkerPool` dispatch block — fires on the first per-item iteration where `$processed` *or* `$itemsProcessed` has crossed at least one `FlushInterval` threshold since the last emit. A single fetch iteration that adds many records (e.g. 80→320) collapses into one event with `records_so_far` set to the actual count at emit time. Treat as a snapshot, not a fixed-cadence marker. The items leg of the gate matters for all-failed slices where `$processed` stays at 0 while items climb (#383). |
| `stage_completed` | `records_so_far`, `duration_ms`, `skipped_count` (pool only — true Skippable category, 404s), `failed_count` (pool only — NonRetryable + max-retries-exhausted, per-item terminal failures), `error_count` (pool only — count of `$errors` strings: per-item failures PLUS chunk-fatal entries captured by the aggregator, so `error_count ≥ failed_count`), `items_processed` / `items_failed` / `items_skipped` (pool only — final per-input-item totals from the pool result, see #383; null for inline stages). For pool stages, divide `items_processed / stage_started.input_count` for true % complete. | `StageExecutor.Invoke-ModuleRun` after success branch |
| `stage_failed` | `error_class` ∈ {`TotalFailure`, `AllItemsExhausted`, `<exception type>`}, `error_message`, `duration_ms` | `StageExecutor.Invoke-ModuleRun` catch branches |
| `stage_skipped` | `reason` ∈ {`ancestor_failed`}, `ancestor_stage` (when reason is `ancestor_failed`) | `StageExecutor.Invoke-ModuleRun` when an ancestor stage failed |
| `run_completed` | `total_records`, `duration_ms`, `status` ∈ {`success`, `failed`, `partial`} | `Invoke-Ingestion.ps1` at end of run + fatal catch |
| `throttle_event` | `retry_after_seconds`, `attempt`, `status_code`, `throttle_signal_text` | `RetryHelper.Invoke-WithRetry` and `WorkerPool` pool throttle path |
| `unknown_retry_event` | `attempt`, `delay_seconds`, `status_code`, `exception_type`, `inner_exception_type`, `api_family`, `error_message` | `RetryHelper.Invoke-WithRetry` and `WorkerPool` dispatch-block Unknown branch. The two layers no longer multiply (#526): when an inner `Invoke-WithRetry` exhausts its budget on Unknown/Auth, the rethrow carries a `RETRY_EXHAUSTED:` marker and the dispatch block fails the item terminally (`item_failed` with `category=RetryExhausted`) instead of re-running the inner cycle. |
| `chunk_failed` | `chunk_index`, `exception_type`, `inner_exception_type`, `error_message`, `script_stack_trace` (truncated to 500 chars) | `WorkerPool` aggregators in `Invoke-StagePool` / `Invoke-StagePoolBatch` for every record on a chunk's `$ps.Streams.Error` stream and for `EndInvoke` catch-path failures. Manifest `.errors[]` still carries the back-compat `chunk=N: <msg>` string; this event mirrors that string into LAW with the exception class so chunk failures don't require reading per-tenant ADLS manifests. See #342. |
| `item_failed` | `category` ∈ {`NonRetryable`, `Skippable`, `AuthMaxRetries`, `UnknownMaxRetries`, `RetryExhausted`}, `item_id`, `attempt`, `status_code`, `exception_type`, `error_message` (truncated to 500 chars) | `WorkerPool` dispatch block — one emission per per-item terminal failure inside a pool slice. Mirrors the per-item `$errors`/`$skipped`/`$failed` increments into LAW so the four previously-silent dispatch paths (`NonRetryable`, `Skippable`, Auth-after-MaxRetries, Unknown-after-MaxRetries) leave a record. See #356. `RetryExhausted` (#526, additive): the fetch's inner `Invoke-WithRetry` exhausted its own budget on Unknown/Auth, so the dispatch loop failed the item on its first outer attempt instead of nesting retry budgets; `attempt` is the outer attempt count (usually 1), `status_code` is from the original wrapped error. |

Single `run_completed` with `status` rather than separate `run_failed` — fewer event types, simpler KQL.

Empty-input pool stages (parent emitted no IDs) emit `stage_started` with `input_count=0` followed by `stage_completed` with `records_so_far=0` — distinct from `stage_skipped`, which marks stages that never ran due to upstream failure. The two cases are also distinguishable in manifests (`status='success' record_count=0` vs `status='skipped'`).

`slice_index` lets dashboards aggregate per-runspace progress to a run total: `summarize max(records_so_far) by run_id, stage, slice_index | summarize sum(max_records_so_far) by run_id, stage`.

## KQL examples

Parse the sentinel out of `ContainerAppConsoleLogs_CL`:

```kql
ContainerAppConsoleLogs_CL
| where Log_s has "_event:"
| extend eventJson = extract(@"_event:(.+)$", 1, Log_s)
| extend e = parse_json(eventJson)
| extend run_id = tostring(e.run_id),
         event_type = tostring(e.event_type),
         stage = tostring(e.stage),
         records_so_far = tolong(e.records_so_far)
```

Numeric fields (`records_so_far`, `duration_ms`, `input_count`, `attempt`, etc.) require explicit `tolong()` — `parse_json` returns dynamic values that compare-as-strings under `==` and `>`.

Last-N-runs timing for an entity:

```kql
ContainerAppConsoleLogs_CL
| where Log_s has "_event:"
| extend e = parse_json(extract(@"_event:(.+)$", 1, Log_s))
| where tostring(e.event_type) == "stage_completed"
| where tostring(e.entity) == "entra_groups"
| project ts = tostring(e.ts),
          run_id = tostring(e.run_id),
          tenant = tostring(e.tenant),
          stage = tostring(e.stage),
          records_so_far = tolong(e.records_so_far),
          duration_ms = tolong(e.duration_ms)
| top 20 by ts desc
```

Throttle count per stage per run:

```kql
ContainerAppConsoleLogs_CL
| where Log_s has "_event:"
| extend e = parse_json(extract(@"_event:(.+)$", 1, Log_s))
| where tostring(e.event_type) == "throttle_event"
| summarize throttle_count = count(),
            total_backoff_s = sum(tolong(e.retry_after_seconds))
            by run_id = tostring(e.run_id),
               tenant = tostring(e.tenant),
               stage = tostring(e.stage)
```

Unknown-retries per stage per run (and which exception types are surfacing — feeds classifier broadening):

```kql
ContainerAppConsoleLogs_CL
| where Log_s has "_event:"
| extend e = parse_json(extract(@"_event:(.+)$", 1, Log_s))
| where tostring(e.event_type) == "unknown_retry_event"
| summarize unknown_retries = count(),
            total_backoff_s = sum(tolong(e.delay_seconds)),
            distinct_types = make_set(tostring(e.exception_type))
            by run_id = tostring(e.run_id),
               tenant = tostring(e.tenant),
               stage = tostring(e.stage),
               api_family = tostring(e.api_family)
```

The absence of `unknown_retry_event` rows in a wedge window is positive evidence that silent retry is *not* the cause — the diagnostic ambiguity that initially mis-classified #321 as silent retry. See #327.

Chunk-level uncaught failures by exception type per stage per run — directly answers "what's actually throwing in a `chunk=N` manifest error?":

```kql
ContainerAppConsoleLogs_CL
| where Log_s has "_event:"
| extend e = parse_json(extract(@"_event:(.+)$", 1, Log_s))
| where tostring(e.event_type) == "chunk_failed"
| summarize chunks = count(),
            distinct_messages = make_set(tostring(e.error_message))
            by run_id = tostring(e.run_id),
               tenant = tostring(e.tenant),
               stage = tostring(e.stage),
               exception_type = tostring(e.exception_type)
```

## Versioning

Schema is **frozen at v1**. Any breaking change (rename, type change, removal) ships:

1. A new `schema_version=2` event type alongside the v1 event for at least one release.
2. Updated workbooks/queries reading the v2 shape.
3. Removal of v1 emit only after consumers have migrated.

Additive changes (new optional property, new `event_type` enum value) do not bump the version.
