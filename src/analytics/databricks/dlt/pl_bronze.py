# Databricks notebook source
# MAGIC %md
# MAGIC # MA Toolkit - Bronze Layer Pipeline
# MAGIC
# MAGIC **Pipeline:** `pl_ma_toolkit_bronze`
# MAGIC **Target Schema:** `ma_toolkit_branch.bronze` / `ma_toolkit_dev.bronze`
# MAGIC
# MAGIC ## Purpose
# MAGIC Ingests raw JSONL files from the landing storage account using Databricks Auto Loader.
# MAGIC Reads across all tenants via wildcard path: `{landing}/{entity_name}/*/{date}/*.jsonl`
# MAGIC This is the **raw data landing zone** - no transformations, append-only.
# MAGIC
# MAGIC ## JSONL Envelope Structure
# MAGIC Each line in a landing file follows this structure:
# MAGIC ```json
# MAGIC {
# MAGIC   "source_type": "tenant",
# MAGIC   "source_key":  "madev1",
# MAGIC   "batch_id":    "a1b2c3d4",
# MAGIC   "ingested_at": "2026-04-15T12:00:00.000Z",
# MAGIC   "_record":     { ...entity-specific fields... }
# MAGIC }
# MAGIC ```
# MAGIC The `_record` field holds the raw entity payload from the source API.
# MAGIC
# MAGIC ## Landing Path Pattern
# MAGIC ```
# MAGIC landing/{entity_name}/{tenant_key}/{date}/{entity_name}_{run_id}.jsonl
# MAGIC e.g.  landing/entra_users/madev1/2026-04-15/entra_users_a1b2c3d4.jsonl
# MAGIC ```
# MAGIC
# MAGIC ## Medallion Architecture
# MAGIC ```
# MAGIC Bronze (this layer)    → Raw, append-only, _record stored as unexpanded JSON string
# MAGIC Silver (pl_silver.py)  → Deduplicated, transformed, SCD Type 1
# MAGIC Gold (pl_gold.py)      → Business dimensions, aggregates
# MAGIC ```

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, lit, coalesce, regexp_extract, from_json, get_json_object
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Storage configuration - set via pipeline parameters or use defaults.
# landing_storage_account is optional: when omitted it falls back to storage_account,
# supporting both single-account lab environments and dual-account production deployments.
STORAGE_ACCOUNT = spark.conf.get("storage_account", "stmatoolkitbranchadls001")
LANDING_STORAGE_ACCOUNT = spark.conf.get("landing_storage_account", STORAGE_ACCOUNT)
LANDING_BASE = f"abfss://landing@{LANDING_STORAGE_ACCOUNT}.dfs.core.windows.net"
SCHEMA_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_schemas"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function

# COMMAND ----------


def create_bronze_stream(entity_name: str, source_system: str = "graph_api", landing_path: str = None):
    """
    Create a bronze streaming table from the landing container.

    Contract (see issue #182): JSONL files, one envelope record per line, landed
    under the entity-first path layout `{entity}/{tenant}/{date}/`. Envelope
    fields are `source_type`, `source_key`, `batch_id`, `ingested_at`, `_record`.

    By default (root entity, no `landing_path`), reads across all tenants and all
    date folders via:
        {LANDING_BASE}/{entity_name}/*/*/*
    JSONL files are selected with `pathGlobFilter="*.jsonl"`.

    When `landing_path` is provided (nested entities / family layouts), it replaces
    `{entity_name}/*/*/*` as the load path, e.g.:
        {LANDING_BASE}/powerplat_environments/*/*/apps

    LANDING_STORAGE_ACCOUNT defaults to STORAGE_ACCOUNT but can be overridden
    via the landing_storage_account pipeline parameter for dual-account deployments.

    Landing path pattern: {entity_name}/{tenant_key}/{date}/{entity_name}_{run_id}.jsonl

    Reads JSONL as text (one row per line). Each line is an envelope:
        { source_type, source_key, batch_id, ingested_at, _record: {...} }

    The four scalar envelope fields are parsed with from_json.
    _record is extracted as a raw JSON string via get_json_object — it is NOT
    expanded into columns here. Bronze is append-only raw storage; silver DLT
    pipelines are responsible for parsing _record per entity type.

    source_key falls back to the tenant_key path segment when absent from the envelope.

    When no landing_path is given (root entity), a file_path depth filter is applied:
        only files at exactly {entity_name}/{tenant}/{date}/{file}.jsonl are ingested.
    This prevents child-entity JSONL files (e.g. entra_groups/{t}/{date}/members/*.jsonl)
    from being accidentally ingested into the parent root table.
    Child entities pass an explicit landing_path and are unaffected by this guard.
    """
    from pyspark.sql.types import StructType, StructField, StringType as ST

    # Envelope schema for the four scalar fields only.
    # _record is a JSON object and is extracted separately as a raw string.
    envelope_schema = StructType(
        [
            StructField("source_type", ST(), True),
            StructField("source_key", ST(), True),
            StructField("batch_id", ST(), True),
            StructField("ingested_at", ST(), True),
        ]
    )

    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/{entity_name}")
        .option("pathGlobFilter", "*.jsonl")
        .load(f"{LANDING_BASE}/{landing_path if landing_path else entity_name + '/*/*/*'}")
        .filter(~col("_metadata.file_path").contains("/_run_status/"))
        .filter(col("value").isNotNull() & (col("value") != ""))
    )

    # Root entities only: guard against child-entity JSONL files that live in
    # date sub-folders (e.g. entra_groups/{tenant}/{date}/members/file.jsonl).
    # The path must end with /{date}/{filename} — no intermediate directory.
    if landing_path is None:
        raw = raw.filter(col("_metadata.file_path").rlike(r"/\d{4}-\d{2}-\d{2}/[^/]+$"))

    parsed = raw.select(
        from_json(col("value"), envelope_schema).alias("_env"),
        get_json_object(col("value"), "$._record").alias("_record"),
        col("_metadata"),
    ).select(
        col("_env.source_type").alias("source_type"),
        col("_env.source_key").alias("source_key"),
        col("_env.batch_id").alias("batch_id"),
        col("_env.ingested_at").alias("ingested_at"),
        col("_record"),
        col("_metadata"),
    )

    return (
        parsed.withColumn("_dlt_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source_system", lit(source_system))
        # tenant_key from path: landing/{entity}/{tenant_key}/{date}/file.jsonl
        .withColumn("_path_tenant", regexp_extract(col("_metadata.file_path"), r"/[^/]+/([^/]+)/\d{4}-\d{2}-\d{2}/", 1))
        .withColumn("batch_id", coalesce(col("batch_id"), lit(None).cast(StringType())))
        .withColumn("source_key", coalesce(col("source_key"), col("_path_tenant"), lit(None).cast(StringType())))
        .withColumn("source_type", coalesce(col("source_type"), lit(None).cast(StringType())))
        .withColumn("ingested_at", coalesce(col("ingested_at"), lit(None).cast(StringType())))
        .drop("_path_tenant")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables - Tier 1: Root Entities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID Users

# COMMAND ----------


@dlt.table(
    name="entra_users",
    comment="Raw Entra ID user records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_users():
    return create_bronze_stream("entra_users")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID Groups

# COMMAND ----------


@dlt.table(
    name="entra_groups",
    comment="Raw Entra ID group records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_groups():
    return create_bronze_stream("entra_groups")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID Contacts

# COMMAND ----------


@dlt.table(
    name="entra_contacts",
    comment="Raw Entra ID organizational contact records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_contacts():
    return create_bronze_stream("entra_contacts")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID Devices

# COMMAND ----------


@dlt.table(
    name="entra_devices",
    comment="Raw Entra ID device records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_devices():
    return create_bronze_stream("entra_devices")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID Applications

# COMMAND ----------


@dlt.table(
    name="entra_applications",
    comment="Raw Entra ID application registration records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_applications():
    return create_bronze_stream("entra_applications")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID Service Principals

# COMMAND ----------


@dlt.table(
    name="entra_service_principals",
    comment="Raw Entra ID service principal records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_service_principals():
    return create_bronze_stream("entra_service_principals")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID Delegated Permission Grants

# COMMAND ----------


@dlt.table(
    name="entra_delegated_permission_grants",
    comment="Raw Entra ID OAuth2 delegated permission grant records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_delegated_permission_grants():
    return create_bronze_stream("entra_delegated_permission_grants")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Intune Managed Devices

# COMMAND ----------


@dlt.table(
    name="intune_managed_devices",
    comment="Raw Intune managed device records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def intune_managed_devices():
    return create_bronze_stream("intune_managed_devices")


# COMMAND ----------

# MAGIC %md
# MAGIC ### MDE Devices

# COMMAND ----------


@dlt.table(
    name="mde_devices",
    comment="Raw Microsoft Defender for Endpoint device records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def mde_devices():
    return create_bronze_stream("mde_devices")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Teams Teams

# COMMAND ----------


@dlt.table(
    name="teams_teams",
    comment="Raw Teams team records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def teams_teams():
    return create_bronze_stream("teams_teams")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exchange Online Mailboxes

# COMMAND ----------


@dlt.table(
    name="exo_mailboxes",
    comment="Raw Exchange Online mailbox records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def exo_mailboxes():
    return create_bronze_stream("exo_mailboxes", "exchange_online")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exchange Online Contacts

# COMMAND ----------


@dlt.table(
    name="exo_contacts",
    comment="Raw Exchange Online mail contact records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def exo_contacts():
    return create_bronze_stream("exo_contacts", "exchange_online")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exchange Online Mail Users

# COMMAND ----------


@dlt.table(
    name="exo_mail_users",
    comment="Raw Exchange Online mail user (external member) records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def exo_mail_users():
    return create_bronze_stream("exo_mail_users", "exchange_online")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exchange Online Distribution Groups

# COMMAND ----------


@dlt.table(
    name="exo_distribution_groups",
    comment="Raw Exchange Online distribution group records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def exo_distribution_groups():
    return create_bronze_stream("exo_distribution_groups", "exchange_online")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exchange Online Distribution Group Details

# COMMAND ----------


@dlt.table(
    name="exo_distribution_group_details",
    comment="Raw Exchange Online distribution group detail records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def exo_distribution_group_details():
    return create_bronze_stream(
        "exo_distribution_group_details",
        "exchange_online",
        landing_path="exo_distribution_groups/*/*/details",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exchange Online Unified Groups

# COMMAND ----------


@dlt.table(
    name="exo_unified_groups",
    comment="Raw Exchange Online unified (M365) group records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def exo_unified_groups():
    return create_bronze_stream("exo_unified_groups", "exchange_online")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables - Tier 2: Dependent Entities
# MAGIC ### Exchange Online Unified Group Details

# COMMAND ----------


@dlt.table(
    name="exo_unified_group_details",
    comment="Raw Exchange Online unified group detail records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def exo_unified_group_details():
    return create_bronze_stream(
        "exo_unified_group_details",
        "exchange_online",
        landing_path="exo_unified_groups/*/*/details",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### SharePoint Online Sites

# COMMAND ----------


@dlt.table(
    name="spo_sites",
    comment="Raw SharePoint Online site records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def spo_sites():
    return create_bronze_stream("spo_sites", "sharepoint_online")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables - Tier 2: Dependent Entities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID Group Members

# COMMAND ----------


@dlt.table(
    name="entra_group_members",
    comment="Raw Entra ID group membership records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_group_members():
    return create_bronze_stream("entra_group_members", landing_path="entra_groups/*/*/members")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID Group Owners

# COMMAND ----------


@dlt.table(
    name="entra_group_owners",
    comment="Raw Entra ID group owner records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_group_owners():
    return create_bronze_stream("entra_group_owners", landing_path="entra_groups/*/*/owners")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Teams Team Details

# COMMAND ----------


@dlt.table(
    name="teams_team_details",
    comment="Raw Teams team detail records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def teams_team_details():
    return create_bronze_stream("teams_team_details", landing_path="teams_teams/*/*/details")


# COMMAND ----------

# MAGIC %md
# MAGIC ### SharePoint Online Site Details

# COMMAND ----------


@dlt.table(
    name="spo_site_details",
    comment="Raw SharePoint Online site detail records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def spo_site_details():
    return create_bronze_stream("spo_site_details", "sharepoint_online", landing_path="spo_sites/*/*/spo_site_details")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID SP Role Assignees

# COMMAND ----------


@dlt.table(
    name="entra_sp_role_assignees",
    comment="Raw Entra ID service principal app role assignment records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_sp_role_assignees():
    return create_bronze_stream("entra_sp_role_assignees", landing_path="entra_service_principals/*/*/role_assignees")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID SP Owners

# COMMAND ----------


@dlt.table(
    name="entra_sp_owners",
    comment="Raw Entra ID service principal owner records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_sp_owners():
    return create_bronze_stream("entra_sp_owners", landing_path="entra_service_principals/*/*/owners")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID App Owners

# COMMAND ----------


@dlt.table(
    name="entra_app_owners",
    comment="Raw Entra ID application owner records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_app_owners():
    return create_bronze_stream("entra_app_owners", landing_path="entra_applications/*/*/owners")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Teams Channels

# COMMAND ----------


@dlt.table(
    name="teams_channels",
    comment="Raw Teams channel records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def teams_channels():
    return create_bronze_stream("teams_channels", landing_path="teams_teams/*/*/channels")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exchange Online Group Members

# COMMAND ----------


@dlt.table(
    name="exo_group_members",
    comment="Raw Exchange Online distribution/unified group member records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def exo_group_members():
    return create_bronze_stream("exo_group_members", "exchange_online", landing_path="exo_*_groups/*/*/members")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables - Tier 3: Deep Entities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID SP App Role Assignments

# COMMAND ----------


@dlt.table(
    name="entra_sp_app_role_assignments",
    comment="Raw Entra ID service principal app role assignment records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_sp_app_role_assignments():
    return create_bronze_stream(
        "entra_sp_app_role_assignments", landing_path="entra_service_principals/*/*/app_role_assignments"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID SP Claims Mapping Policies

# COMMAND ----------


@dlt.table(
    name="entra_sp_claims_mapping_policies",
    comment="Raw Entra ID service principal claims mapping policy records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_sp_claims_mapping_policies():
    return create_bronze_stream(
        "entra_sp_claims_mapping_policies", landing_path="entra_service_principals/*/*/claims_mapping_policies"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID SP Permission Classifications

# COMMAND ----------


@dlt.table(
    name="entra_sp_perm_classifications",
    comment="Raw Entra ID service principal permission classification records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_sp_perm_classifications():
    return create_bronze_stream(
        "entra_sp_perm_classifications", landing_path="entra_service_principals/*/*/perm_classifications"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID Sign-in Logs

# COMMAND ----------


@dlt.table(
    name="entra_sign_in_logs",
    comment="Raw Entra ID sign-in log records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_sign_in_logs():
    return create_bronze_stream("entra_sign_in_logs")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID App Proxy Config

# COMMAND ----------


@dlt.table(
    name="entra_app_proxy_config",
    comment="Raw Entra ID application proxy configuration records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_app_proxy_config():
    return create_bronze_stream("entra_app_proxy_config", landing_path="entra_applications/*/*/proxy_config")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entra ID SP Provisioning Jobs

# COMMAND ----------


@dlt.table(
    name="entra_sp_provisioning_jobs",
    comment="Raw Entra ID service principal provisioning job records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def entra_sp_provisioning_jobs():
    return create_bronze_stream(
        "entra_sp_provisioning_jobs", landing_path="entra_service_principals/*/*/provisioning_jobs"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Teams Channel Members

# COMMAND ----------


@dlt.table(
    name="teams_channel_members",
    comment="Raw Teams channel member records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def teams_channel_members():
    return create_bronze_stream("teams_channel_members", landing_path="teams_teams/*/*/channel_members")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Teams Installed Apps

# COMMAND ----------


@dlt.table(
    name="teams_installed_apps",
    comment="Raw Teams installed app records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def teams_installed_apps():
    return create_bronze_stream("teams_installed_apps", landing_path="teams_teams/*/*/installed_apps")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Teams Channel Tabs

# COMMAND ----------


@dlt.table(
    name="teams_channel_tabs",
    comment="Raw Teams channel tab records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def teams_channel_tabs():
    return create_bronze_stream("teams_channel_tabs", landing_path="teams_teams/*/*/channel_tabs")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exchange Online Mailbox Statistics

# COMMAND ----------


@dlt.table(
    name="exo_mailbox_statistics",
    comment="Raw Exchange Online mailbox statistics from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def exo_mailbox_statistics():
    return create_bronze_stream(
        "exo_mailbox_statistics", "exchange_online", landing_path="exo_mailboxes/*/*/statistics"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exchange Online Mailbox Permissions

# COMMAND ----------


@dlt.table(
    name="exo_mailbox_permissions",
    comment="Raw Exchange Online mailbox permission records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def exo_mailbox_permissions():
    return create_bronze_stream(
        "exo_mailbox_permissions", "exchange_online", landing_path="exo_mailboxes/*/*/permissions"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### SharePoint Online Site Groups

# COMMAND ----------


@dlt.table(
    name="spo_site_groups",
    comment="Raw SharePoint Online site group records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def spo_site_groups():
    return create_bronze_stream("spo_site_groups", "sharepoint_online", landing_path="spo_sites/*/*/spo_site_groups")


# COMMAND ----------

# MAGIC %md
# MAGIC ### SharePoint Online Site Users (post-#475 rewrite)

# COMMAND ----------


@dlt.table(
    name="spo_site_users",
    comment="Raw SharePoint Online site user records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def spo_site_users():
    return create_bronze_stream("spo_site_users", "sharepoint_online", landing_path="spo_sites/*/*/spo_site_users")


# COMMAND ----------

# MAGIC %md
# MAGIC ### SharePoint Online Webs (post-#475 rewrite)

# COMMAND ----------


@dlt.table(
    name="spo_webs",
    comment="Raw SharePoint Online web records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def spo_webs():
    return create_bronze_stream("spo_webs", "sharepoint_online", landing_path="spo_sites/*/*/spo_webs")


# COMMAND ----------

# MAGIC %md
# MAGIC ### SharePoint Online Web Role Definitions (post-#475 rewrite)

# COMMAND ----------


@dlt.table(
    name="spo_web_role_definitions",
    comment="Raw SharePoint Online web role definition records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def spo_web_role_definitions():
    return create_bronze_stream(
        "spo_web_role_definitions", "sharepoint_online", landing_path="spo_sites/*/*/spo_web_role_definitions"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### SharePoint Online Web Role Assignments (post-#475 rewrite)

# COMMAND ----------


@dlt.table(
    name="spo_web_role_assignments",
    comment="Raw SharePoint Online web role assignment records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def spo_web_role_assignments():
    return create_bronze_stream(
        "spo_web_role_assignments", "sharepoint_online", landing_path="spo_sites/*/*/spo_web_role_assignments"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### SharePoint Online Web Lists (post-#475 rewrite)

# COMMAND ----------


@dlt.table(
    name="spo_web_lists",
    comment="Raw SharePoint Online web list records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def spo_web_lists():
    return create_bronze_stream("spo_web_lists", "sharepoint_online", landing_path="spo_sites/*/*/spo_web_lists")


# COMMAND ----------

# MAGIC %md
# MAGIC ### SharePoint Online Web Item Permissions (post-#475 rewrite)

# COMMAND ----------


@dlt.table(
    name="spo_web_item_permissions",
    comment="Raw SharePoint Online web item permission records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def spo_web_item_permissions():
    return create_bronze_stream(
        "spo_web_item_permissions", "sharepoint_online", landing_path="spo_sites/*/*/spo_web_item_permissions"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables - Audit Logs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit - Entra ID

# COMMAND ----------


@dlt.table(
    name="audit_entra",
    comment="Raw Entra ID unified audit log records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def audit_entra():
    return create_bronze_stream("audit_entra", "o365_management")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit - Exchange Online

# COMMAND ----------


@dlt.table(
    name="audit_exchange",
    comment="Raw Exchange Online unified audit log records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def audit_exchange():
    return create_bronze_stream("audit_exchange", "o365_management")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit - General (cross-workload)

# COMMAND ----------


@dlt.table(
    name="audit_general",
    comment="Raw cross-workload unified audit log records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def audit_general():
    return create_bronze_stream("audit_general", "o365_management")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit - SharePoint Online

# COMMAND ----------


@dlt.table(
    name="audit_sharepoint",
    comment="Raw SharePoint Online unified audit log records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def audit_sharepoint():
    return create_bronze_stream("audit_sharepoint", "o365_management")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables - Power BI / Fabric

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Apps

# COMMAND ----------


@dlt.table(
    name="powerbi_apps",
    comment="Raw Power BI app records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerbi_apps():
    return create_bronze_stream("powerbi_apps", "powerbi")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Capacities

# COMMAND ----------


@dlt.table(
    name="powerbi_capacities",
    comment="Raw Power BI capacity records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerbi_capacities():
    return create_bronze_stream("powerbi_capacities", "powerbi")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Deployment Pipelines

# COMMAND ----------


@dlt.table(
    name="powerbi_deployment_pipelines",
    comment="Raw Power BI deployment pipeline records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerbi_deployment_pipelines():
    return create_bronze_stream("powerbi_deployment_pipelines", "powerbi")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Gateway Clusters

# COMMAND ----------


@dlt.table(
    name="powerbi_gateway_clusters",
    comment="Raw Power BI gateway cluster records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerbi_gateway_clusters():
    return create_bronze_stream("powerbi_gateway_clusters", "powerbi")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Gateway Cluster Permissions

# COMMAND ----------


@dlt.table(
    name="powerbi_gateway_cluster_permissions",
    comment="Raw Power BI gateway cluster permission records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerbi_gateway_cluster_permissions():
    return create_bronze_stream("powerbi_gateway_cluster_permissions", "powerbi")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Workspaces Root
# MAGIC
# MAGIC Workspace scan results from the Power BI admin getInfo API.
# MAGIC Each record is a workspace with nested reports, datasets, dashboards, dataflows, and users.
# MAGIC Files land under a `root/` subfolder: `powerbi_workspaces_root/{tenant}/{date}/root/*.jsonl`

# COMMAND ----------


@dlt.table(
    name="powerbi_workspaces_root",
    comment="Raw Power BI workspace scan result records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerbi_workspaces_root():
    return create_bronze_stream("powerbi_workspaces_root", "powerbi", landing_path="powerbi_workspaces_root/*/*/root")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Fabric Lakehouses

# COMMAND ----------


@dlt.table(
    name="powerbi_fabric_lakehouses",
    comment="Raw Fabric lakehouse item records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerbi_fabric_lakehouses():
    return create_bronze_stream("powerbi_fabric_lakehouses", "powerbi")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Fabric Warehouses

# COMMAND ----------


@dlt.table(
    name="powerbi_fabric_warehouses",
    comment="Raw Fabric warehouse item records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerbi_fabric_warehouses():
    return create_bronze_stream("powerbi_fabric_warehouses", "powerbi")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Fabric KQL Databases

# COMMAND ----------


@dlt.table(
    name="powerbi_fabric_kql_databases",
    comment="Raw Fabric KQL database item records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerbi_fabric_kql_databases():
    return create_bronze_stream("powerbi_fabric_kql_databases", "powerbi")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Fabric Notebooks

# COMMAND ----------


@dlt.table(
    name="powerbi_fabric_notebooks",
    comment="Raw Fabric notebook item records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerbi_fabric_notebooks():
    return create_bronze_stream("powerbi_fabric_notebooks", "powerbi")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables - Power Platform / BAP
# MAGIC
# MAGIC Per-environment entities fetched from the BAP (Business Application Platform)
# MAGIC admin plane and Power Automate admin API. Landing path pattern:
# MAGIC `powerplat_environments/{tenant}/{date}/{entity}/*.jsonl`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apps

# COMMAND ----------


@dlt.table(
    name="powerplat_apps",
    comment="Raw Power Platform canvas app records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_apps():
    return create_bronze_stream("powerplat_apps", "power_platform", landing_path="powerplat_environments/*/*/apps")


# COMMAND ----------

# MAGIC %md
# MAGIC ### App Role Assignments

# COMMAND ----------


@dlt.table(
    name="powerplat_app_role_assignments",
    comment="Raw Power Platform canvas app role assignment records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_app_role_assignments():
    return create_bronze_stream(
        "powerplat_app_role_assignments",
        "power_platform",
        landing_path="powerplat_environments/*/*/app_role_assignments",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Flows

# COMMAND ----------


@dlt.table(
    name="powerplat_flows",
    comment="Raw Power Automate cloud flow records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_flows():
    return create_bronze_stream("powerplat_flows", "power_platform", landing_path="powerplat_environments/*/*/flows")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Flow Role Assignments

# COMMAND ----------


@dlt.table(
    name="powerplat_flow_role_assignments",
    comment="Raw Power Automate flow role assignment records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_flow_role_assignments():
    return create_bronze_stream(
        "powerplat_flow_role_assignments",
        "power_platform",
        landing_path="powerplat_environments/*/*/flow_role_assignments",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Connections

# COMMAND ----------


@dlt.table(
    name="powerplat_connections",
    comment="Raw Power Platform connector connection records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_connections():
    return create_bronze_stream(
        "powerplat_connections", "power_platform", landing_path="powerplat_environments/*/*/connections"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Connection Role Assignments

# COMMAND ----------


@dlt.table(
    name="powerplat_connection_role_assignments",
    comment="Raw Power Platform connection role assignment records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_connection_role_assignments():
    return create_bronze_stream(
        "powerplat_connection_role_assignments",
        "power_platform",
        landing_path="powerplat_environments/*/*/connection_role_assignments",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Custom Connectors

# COMMAND ----------


@dlt.table(
    name="powerplat_custom_connectors",
    comment="Raw Power Platform custom connector records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_custom_connectors():
    return create_bronze_stream(
        "powerplat_custom_connectors",
        "power_platform",
        landing_path="powerplat_environments/*/*/custom_connectors",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataverse Onboardings

# COMMAND ----------


@dlt.table(
    name="powerplat_dataverse_onboardings",
    comment="Raw Dataverse environment onboarding audit records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_dataverse_onboardings():
    return create_bronze_stream(
        "powerplat_dataverse_onboardings",
        "power_platform",
        landing_path="powerplat_environments/*/*/dataverse_onboardings",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables - Power Platform / Dataverse
# MAGIC
# MAGIC Bulk Dataverse entities fetched per-environment via the Dataverse Web API.
# MAGIC Landing path pattern: `powerplat_environments/{tenant}/{date}/{entity}/*.jsonl`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solutions

# COMMAND ----------


@dlt.table(
    name="powerplat_solutions",
    comment="Raw Dataverse solution records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_solutions():
    return create_bronze_stream(
        "powerplat_solutions", "power_platform", landing_path="powerplat_environments/*/*/solutions"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Components

# COMMAND ----------


@dlt.table(
    name="powerplat_solution_components",
    comment="Raw Dataverse solution component records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_solution_components():
    return create_bronze_stream(
        "powerplat_solution_components", "power_platform", landing_path="powerplat_environments/*/*/solution_components"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Connection References

# COMMAND ----------


@dlt.table(
    name="powerplat_connection_references",
    comment="Raw Dataverse connection reference records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_connection_references():
    return create_bronze_stream(
        "powerplat_connection_references",
        "power_platform",
        landing_path="powerplat_environments/*/*/connection_references",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Variable Definitions

# COMMAND ----------


@dlt.table(
    name="powerplat_env_variable_definitions",
    comment="Raw Dataverse environment variable definition records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_env_variable_definitions():
    return create_bronze_stream(
        "powerplat_env_variable_definitions",
        "power_platform",
        landing_path="powerplat_environments/*/*/env_variable_definitions",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Variable Values

# COMMAND ----------


@dlt.table(
    name="powerplat_env_variable_values",
    comment="Raw Dataverse environment variable value records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_env_variable_values():
    return create_bronze_stream(
        "powerplat_env_variable_values", "power_platform", landing_path="powerplat_environments/*/*/env_variable_values"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables

# COMMAND ----------


@dlt.table(
    name="powerplat_tables",
    comment="Raw Dataverse table (entity) metadata records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_tables():
    return create_bronze_stream("powerplat_tables", "power_platform", landing_path="powerplat_environments/*/*/tables")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Workflows

# COMMAND ----------


@dlt.table(
    name="powerplat_workflows",
    comment="Raw Dataverse workflow (process) records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_workflows():
    return create_bronze_stream(
        "powerplat_workflows", "power_platform", landing_path="powerplat_environments/*/*/workflows"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Plugin Assemblies

# COMMAND ----------


@dlt.table(
    name="powerplat_plugin_assemblies",
    comment="Raw Dataverse plugin assembly records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_plugin_assemblies():
    return create_bronze_stream(
        "powerplat_plugin_assemblies", "power_platform", landing_path="powerplat_environments/*/*/plugin_assemblies"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Plugin Steps

# COMMAND ----------


@dlt.table(
    name="powerplat_plugin_steps",
    comment="Raw Dataverse plugin step (SDK message processing step) records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_plugin_steps():
    return create_bronze_stream(
        "powerplat_plugin_steps", "power_platform", landing_path="powerplat_environments/*/*/plugin_steps"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Web Resources

# COMMAND ----------


@dlt.table(
    name="powerplat_web_resources",
    comment="Raw Dataverse web resource records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_web_resources():
    return create_bronze_stream(
        "powerplat_web_resources", "power_platform", landing_path="powerplat_environments/*/*/web_resources"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### App Modules

# COMMAND ----------


@dlt.table(
    name="powerplat_app_modules",
    comment="Raw Dataverse model-driven app (app module) records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_app_modules():
    return create_bronze_stream(
        "powerplat_app_modules", "power_platform", landing_path="powerplat_environments/*/*/app_modules"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bots

# COMMAND ----------


@dlt.table(
    name="powerplat_bots",
    comment="Raw Dataverse Copilot Studio bot records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_bots():
    return create_bronze_stream("powerplat_bots", "power_platform", landing_path="powerplat_environments/*/*/bots")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bot Components

# COMMAND ----------


@dlt.table(
    name="powerplat_bot_components",
    comment="Raw Dataverse Copilot Studio bot component records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_bot_components():
    return create_bronze_stream(
        "powerplat_bot_components", "power_platform", landing_path="powerplat_environments/*/*/bot_components"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### AI Models

# COMMAND ----------


@dlt.table(
    name="powerplat_ai_models",
    comment="Raw Dataverse AI model records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_ai_models():
    return create_bronze_stream(
        "powerplat_ai_models", "power_platform", landing_path="powerplat_environments/*/*/ai_models"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Pages Websites

# COMMAND ----------


@dlt.table(
    name="powerplat_powerpages_websites",
    comment="Raw Power Pages website records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_powerpages_websites():
    return create_bronze_stream(
        "powerplat_powerpages_websites",
        "power_platform",
        landing_path="powerplat_environments/*/*/powerpages_websites",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Pages Components

# COMMAND ----------


@dlt.table(
    name="powerplat_powerpages_components",
    comment="Raw Power Pages component records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_powerpages_components():
    return create_bronze_stream(
        "powerplat_powerpages_components",
        "power_platform",
        landing_path="powerplat_environments/*/*/powerpages_components",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### System Users

# COMMAND ----------


@dlt.table(
    name="powerplat_systemusers",
    comment="Raw Dataverse system user (service principal) records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_systemusers():
    return create_bronze_stream(
        "powerplat_systemusers", "power_platform", landing_path="powerplat_environments/*/*/systemusers"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Publishers

# COMMAND ----------


@dlt.table(
    name="powerplat_publishers",
    comment="Raw Dataverse solution publisher records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_publishers():
    return create_bronze_stream(
        "powerplat_publishers", "power_platform", landing_path="powerplat_environments/*/*/publishers"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Mailboxes

# COMMAND ----------


@dlt.table(
    name="powerplat_mailboxes",
    comment="Raw Dataverse mailbox configuration records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_mailboxes():
    return create_bronze_stream(
        "powerplat_mailboxes", "power_platform", landing_path="powerplat_environments/*/*/mailboxes"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables - Power Platform / Dependency Bodies
# MAGIC
# MAGIC Per-row fan-out stages that fetch heavy column(s) for each parent row.
# MAGIC These entities store definition bodies (JSON/XML) excluded from bulk-list calls
# MAGIC to avoid blob-induced memory pressure at scale.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flow Metadata

# COMMAND ----------


@dlt.table(
    name="powerplat_flow_metadata",
    comment="Raw Power Automate flow definition body records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_flow_metadata():
    return create_bronze_stream(
        "powerplat_flow_metadata", "power_platform", landing_path="powerplat_environments/*/*/flow_metadata"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Workflow Definitions

# COMMAND ----------


@dlt.table(
    name="powerplat_workflow_definitions",
    comment="Raw Dataverse workflow (Dataverse-backed flow) clientdata definition records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_workflow_definitions():
    return create_bronze_stream(
        "powerplat_workflow_definitions",
        "power_platform",
        landing_path="powerplat_environments/*/*/workflow_definitions",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### App Module XML

# COMMAND ----------


@dlt.table(
    name="powerplat_app_module_xml",
    comment="Raw Dataverse model-driven app sitemap/customization XML records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_app_module_xml():
    return create_bronze_stream(
        "powerplat_app_module_xml", "power_platform", landing_path="powerplat_environments/*/*/app_module_xml"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bot Configurations

# COMMAND ----------


@dlt.table(
    name="powerplat_bot_configurations",
    comment="Raw Copilot Studio bot configuration body records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_bot_configurations():
    return create_bronze_stream(
        "powerplat_bot_configurations", "power_platform", landing_path="powerplat_environments/*/*/bot_configurations"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bot Component Data

# COMMAND ----------


@dlt.table(
    name="powerplat_bot_component_data",
    comment="Raw Copilot Studio bot component body records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_bot_component_data():
    return create_bronze_stream(
        "powerplat_bot_component_data", "power_platform", landing_path="powerplat_environments/*/*/bot_component_data"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Web Resource Contents

# COMMAND ----------


@dlt.table(
    name="powerplat_web_resource_contents",
    comment="Raw Dataverse web resource content body records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_web_resource_contents():
    return create_bronze_stream(
        "powerplat_web_resource_contents",
        "power_platform",
        landing_path="powerplat_environments/*/*/web_resource_contents",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Pages Component Contents

# COMMAND ----------


@dlt.table(
    name="powerplat_powerpages_component_contents",
    comment="Raw Power Pages component content body records from all tenants - append only.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name",
    },
)
@dlt.expect("has_record", "_record IS NOT NULL")
@dlt.expect("has_batch_id", "batch_id IS NOT NULL")
def powerplat_powerpages_component_contents():
    return create_bronze_stream(
        "powerplat_powerpages_component_contents",
        "power_platform",
        landing_path="powerplat_environments/*/*/powerpages_component_contents",
    )
