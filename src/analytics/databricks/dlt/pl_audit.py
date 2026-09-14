# Databricks notebook source
# MAGIC %md
# MAGIC # M365 Migration - Audit Layer Pipeline
# MAGIC
# MAGIC **Pipeline:** `pl_m365_audit`
# MAGIC **Target Schema:** `m365_migration.audit`
# MAGIC
# MAGIC ## Purpose
# MAGIC Pipeline tracking, ingestion lineage, and data quality metrics.
# MAGIC
# MAGIC ## Tables
# MAGIC | Table | Purpose |
# MAGIC |-------|---------|
# MAGIC | `ingestion_batches` | Track all ingestion batches from ACA Jobs (all entity types) |
# MAGIC | `entity_record_counts` | Daily record counts by entity and environment |
# MAGIC | `data_quality_metrics` | Data quality metrics from Silver tables |
# MAGIC | `pipeline_execution_log` | DLT pipeline run tracking |
# MAGIC
# MAGIC ## Dependencies
# MAGIC - Requires `pl_m365_bronze`, `pl_m365_silver`, `pl_m365_gold` to run first

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    when,
    count,
    sum as spark_sum,
    date_trunc,
    max as spark_max,
    min as spark_min,
    avg,
    countDistinct,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "m365_migration"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# All Bronze entity tables to track — merged, one table per entity type
BRONZE_ENTITIES = [
    ("entra_users", "entra_users", "graph_api"),
    ("entra_contacts", "entra_contacts", "graph_api"),
    ("entra_groups", "entra_groups", "graph_api"),
    ("entra_group_members", "entra_group_members", "graph_api"),
    ("entra_group_owners", "entra_group_owners", "graph_api"),
    ("entra_devices", "entra_devices", "graph_api"),
    ("exo_mail_users", "exo_mail_users", "exchange_online"),
    ("exo_contacts", "exo_contacts", "exchange_online"),
    ("onedrive_sites", "onedrive_sites", "graph_api"),
    ("sharepoint_sites", "sharepoint_sites", "graph_api"),
    ("ad_users", "ad_users", "active_directory"),
    ("ad_contacts", "ad_contacts", "active_directory"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------


def safe_table_exists(table_name: str) -> bool:
    """Check if a table exists in the catalog."""
    try:
        spark.table(table_name)
        return True
    except Exception:
        return False


def get_batch_info_from_bronze(table_name: str, entity_type: str, source_system: str):
    """
    Extract batch information from a Bronze table.
    Returns DataFrame with standardized columns or empty DataFrame if table doesn't exist.
    tenant_name is read from the table directly — no hardcoded environment.
    """
    full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    if not safe_table_exists(full_table_name):
        # Return empty DataFrame with expected schema
        return spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("batch_id", StringType(), True),
                    StructField("tenant_name", StringType(), True),
                    StructField("entity_type", StringType(), True),
                    StructField("source_system", StringType(), True),
                    StructField("ingested_at", StringType(), True),
                    StructField("record_count", LongType(), True),
                    StructField("dlt_ingested_at", TimestampType(), True),
                    StructField("source_file", StringType(), True),
                ]
            ),
        )

    return (
        spark.table(full_table_name)
        .select(
            col("batch_id"),
            col("tenant_name"),
            lit(entity_type).alias("entity_type"),
            lit(source_system).alias("source_system"),
            col("ingested_at"),
            col("record_count"),
            col("_dlt_ingested_at").alias("dlt_ingested_at"),
            col("_source_file").alias("source_file"),
        )
        .distinct()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tables - Ingestion Batches
# MAGIC
# MAGIC Tracks every ingestion batch from ACA Jobs across all entity types.

# COMMAND ----------


@dlt.table(
    name="ingestion_batches",
    comment="Comprehensive tracking of all ingestion batches from ACA Jobs - all entity types, both tenants",
    table_properties={"quality": "audit", "pipelines.autoOptimize.managed": "true"},
)
def ingestion_batches():
    """
    Union batch metadata from all Bronze tables.

    Columns:
    - batch_id: Unique identifier for the ingestion batch
    - tenant_id: Azure AD tenant GUID
    - tenant_name: Human-readable tenant name
    - entity_type: Type of entity (entra_users, entra_groups, etc.)
    - environment: source or target
    - source_system: graph_api or exchange_online
    - ingested_at: When ACA Job extracted the data
    - record_count: Number of records in batch
    - dlt_ingested_at: When DLT processed the batch
    - source_file: ADLS file path
    """
    dfs = []

    for table_name, entity_type, source_system in BRONZE_ENTITIES:
        df = get_batch_info_from_bronze(table_name, entity_type, source_system)
        if df.count() > 0:  # Only add if has data
            dfs.append(df)

    if not dfs:
        # Return empty DataFrame with schema if no data yet
        return spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("batch_id", StringType(), True),
                    StructField("tenant_name", StringType(), True),
                    StructField("entity_type", StringType(), True),
                    StructField("source_system", StringType(), True),
                    StructField("ingested_at", StringType(), True),
                    StructField("record_count", LongType(), True),
                    StructField("dlt_ingested_at", TimestampType(), True),
                    StructField("source_file", StringType(), True),
                    StructField("audit_timestamp", TimestampType(), True),
                ]
            ),
        )

    # Union all batch info
    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result.withColumn("audit_timestamp", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tables - Entity Record Counts
# MAGIC
# MAGIC Daily snapshot of record counts across all layers.

# COMMAND ----------


@dlt.table(
    name="entity_record_counts",
    comment="Daily record counts by entity, environment, and layer",
    table_properties={"quality": "audit", "pipelines.autoOptimize.managed": "true"},
)
def entity_record_counts():
    """
    Snapshot of record counts for all key tables.
    Useful for tracking data growth and pipeline health.
    """
    counts = []

    # Silver entity counts
    silver_tables = [
        ("users", "user"),
        ("contacts", "contact"),
        ("groups", "group"),
        ("group_memberships", "group_membership"),
        ("mailboxes", "mailbox"),
    ]

    for table_name, entity_type in silver_tables:
        full_name = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
        if safe_table_exists(full_name):
            df = (
                spark.table(full_name)
                .groupBy("environment")
                .agg(count("*").alias("record_count"))
                .withColumn("layer", lit("silver"))
                .withColumn("entity_type", lit(entity_type))
                .withColumn("table_name", lit(table_name))
            )
            counts.append(df)

    # Gold dimension counts
    gold_tables = [
        ("dim_user", "user", "environment"),
        ("dim_contacts", "contact", "environment"),
        ("dim_group", "group", "environment"),
        ("dim_people", "person", None),  # No environment column - aggregated
    ]

    for table_name, entity_type, env_col in gold_tables:
        full_name = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
        if safe_table_exists(full_name):
            if env_col:
                df = (
                    spark.table(full_name)
                    .groupBy(env_col)
                    .agg(count("*").alias("record_count"))
                    .withColumnRenamed(env_col, "environment")
                    .withColumn("layer", lit("gold"))
                    .withColumn("entity_type", lit(entity_type))
                    .withColumn("table_name", lit(table_name))
                )
            else:
                df = (
                    spark.table(full_name)
                    .agg(count("*").alias("record_count"))
                    .withColumn("environment", lit("all"))
                    .withColumn("layer", lit("gold"))
                    .withColumn("entity_type", lit(entity_type))
                    .withColumn("table_name", lit(table_name))
                )
            counts.append(df)

    if not counts:
        return spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("environment", StringType(), True),
                    StructField("layer", StringType(), True),
                    StructField("entity_type", StringType(), True),
                    StructField("table_name", StringType(), True),
                    StructField("record_count", LongType(), True),
                    StructField("snapshot_date", TimestampType(), True),
                    StructField("snapshot_at", TimestampType(), True),
                ]
            ),
        )

    result = counts[0]
    for df in counts[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return (
        result.withColumn("snapshot_date", date_trunc("day", current_timestamp()))
        .withColumn("snapshot_at", current_timestamp())
        .select("snapshot_date", "layer", "entity_type", "table_name", "environment", "record_count", "snapshot_at")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tables - Data Quality Metrics
# MAGIC
# MAGIC Key data quality indicators from Silver tables.

# COMMAND ----------


@dlt.table(
    name="data_quality_metrics",
    comment="Data quality metrics from Silver tables - completeness and validity checks",
    table_properties={"quality": "audit", "pipelines.autoOptimize.managed": "true"},
)
def data_quality_metrics():
    """
    Data quality metrics for key fields.

    Metrics:
    - Completeness: % of non-null values
    - Validity: % meeting business rules
    """
    metrics = []

    # Users data quality
    users_table = f"{CATALOG}.{SILVER_SCHEMA}.users"
    if safe_table_exists(users_table):
        users = spark.table(users_table)

        user_metrics = (
            users.groupBy("environment")
            .agg(
                count("*").alias("total_records"),
                # Completeness metrics
                spark_sum(when(col("mail").isNotNull(), 1).otherwise(0)).alias("with_email"),
                spark_sum(when(col("employee_id").isNotNull(), 1).otherwise(0)).alias("with_employee_id"),
                spark_sum(when(col("display_name").isNotNull(), 1).otherwise(0)).alias("with_display_name"),
                spark_sum(when(col("department").isNotNull(), 1).otherwise(0)).alias("with_department"),
                spark_sum(when(col("job_title").isNotNull(), 1).otherwise(0)).alias("with_job_title"),
                # Validity metrics
                spark_sum(col("account_enabled").cast("int")).alias("enabled_accounts"),
                spark_sum(when(col("user_type") == "Member", 1).otherwise(0)).alias("member_accounts"),
                spark_sum(when(col("user_type") == "Guest", 1).otherwise(0)).alias("guest_accounts"),
                spark_sum((col("has_e3_license") | col("has_e5_license")).cast("int")).alias("with_e3_or_e5"),
            )
            .withColumn("entity_type", lit("users"))
            # Calculate percentages
            .withColumn("pct_with_email", (col("with_email") / col("total_records")).cast("decimal(5,4)"))
            .withColumn("pct_with_employee_id", (col("with_employee_id") / col("total_records")).cast("decimal(5,4)"))
            .withColumn("pct_with_display_name", (col("with_display_name") / col("total_records")).cast("decimal(5,4)"))
            .withColumn("pct_with_department", (col("with_department") / col("total_records")).cast("decimal(5,4)"))
            .withColumn("pct_enabled", (col("enabled_accounts") / col("total_records")).cast("decimal(5,4)"))
            .withColumn("pct_member", (col("member_accounts") / col("total_records")).cast("decimal(5,4)"))
            .withColumn("pct_with_license", (col("with_e3_or_e5") / col("total_records")).cast("decimal(5,4)"))
        )
        metrics.append(user_metrics)

    # Groups data quality
    groups_table = f"{CATALOG}.{SILVER_SCHEMA}.groups"
    if safe_table_exists(groups_table):
        groups = spark.table(groups_table)

        group_metrics = (
            groups.groupBy("environment")
            .agg(
                count("*").alias("total_records"),
                spark_sum(when(col("mail").isNotNull(), 1).otherwise(0)).alias("with_email"),
                spark_sum(when(col("display_name").isNotNull(), 1).otherwise(0)).alias("with_display_name"),
                spark_sum(col("mail_enabled").cast("int")).alias("mail_enabled_groups"),
                spark_sum(col("security_enabled").cast("int")).alias("security_enabled_groups"),
                # Placeholders for compatibility
                lit(0).alias("with_employee_id"),
                lit(0).alias("with_department"),
                lit(0).alias("with_job_title"),
                lit(0).alias("enabled_accounts"),
                lit(0).alias("member_accounts"),
                lit(0).alias("guest_accounts"),
                lit(0).alias("with_e3_or_e5"),
            )
            .withColumn("entity_type", lit("groups"))
            .withColumn("pct_with_email", (col("with_email") / col("total_records")).cast("decimal(5,4)"))
            .withColumn("pct_with_employee_id", lit(0).cast("decimal(5,4)"))
            .withColumn("pct_with_display_name", (col("with_display_name") / col("total_records")).cast("decimal(5,4)"))
            .withColumn("pct_with_department", lit(0).cast("decimal(5,4)"))
            .withColumn("pct_enabled", lit(0).cast("decimal(5,4)"))
            .withColumn("pct_member", lit(0).cast("decimal(5,4)"))
            .withColumn("pct_with_license", lit(0).cast("decimal(5,4)"))
        )
        metrics.append(group_metrics)

    if not metrics:
        return spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("environment", StringType(), True),
                    StructField("entity_type", StringType(), True),
                    StructField("total_records", LongType(), True),
                    StructField("pct_with_email", StringType(), True),
                    StructField("pct_with_employee_id", StringType(), True),
                    StructField("snapshot_at", TimestampType(), True),
                ]
            ),
        )

    result = metrics[0]
    for df in metrics[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result.withColumn("snapshot_at", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tables - Mapping Statistics
# MAGIC
# MAGIC Statistics on mapping candidates and approvals.

# COMMAND ----------


@dlt.table(
    name="mapping_statistics",
    comment="Statistics on user and group mapping candidates and approvals",
    table_properties={"quality": "audit", "pipelines.autoOptimize.managed": "true"},
)
def mapping_statistics():
    """
    Aggregate statistics on mapping process:
    - Candidate counts by score band
    - Approval rates
    - Match type distribution
    """
    stats = []

    # User mapping candidates
    user_candidates_table = f"{CATALOG}.{SILVER_SCHEMA}.user_mapping_candidates"
    if safe_table_exists(user_candidates_table):
        candidates = spark.table(user_candidates_table)

        user_stats = (
            candidates.agg(
                count("*").alias("total_candidates"),
                countDistinct("source_user_id").alias("unique_source_users"),
                countDistinct("target_user_id").alias("unique_target_users"),
                avg("match_score").alias("avg_match_score"),
                spark_max("match_score").alias("max_match_score"),
                spark_min("match_score").alias("min_match_score"),
                spark_sum(when(col("match_score") >= 90, 1).otherwise(0)).alias("high_confidence_count"),
                spark_sum(when((col("match_score") >= 70) & (col("match_score") < 90), 1).otherwise(0)).alias(
                    "medium_confidence_count"
                ),
                spark_sum(when(col("match_score") < 70, 1).otherwise(0)).alias("low_confidence_count"),
            )
            .withColumn("entity_type", lit("user"))
            .withColumn("avg_match_score", col("avg_match_score").cast("decimal(5,2)"))
        )
        stats.append(user_stats)

    # Group mapping candidates
    group_candidates_table = f"{CATALOG}.{SILVER_SCHEMA}.group_mapping_candidates"
    if safe_table_exists(group_candidates_table):
        candidates = spark.table(group_candidates_table)

        group_stats = (
            candidates.agg(
                count("*").alias("total_candidates"),
                countDistinct("source_group_id").alias("unique_source_users"),  # reusing column name for union
                countDistinct("target_group_id").alias("unique_target_users"),
                avg("match_score").alias("avg_match_score"),
                spark_max("match_score").alias("max_match_score"),
                spark_min("match_score").alias("min_match_score"),
                spark_sum(when(col("match_score") >= 90, 1).otherwise(0)).alias("high_confidence_count"),
                spark_sum(when((col("match_score") >= 70) & (col("match_score") < 90), 1).otherwise(0)).alias(
                    "medium_confidence_count"
                ),
                spark_sum(when(col("match_score") < 70, 1).otherwise(0)).alias("low_confidence_count"),
            )
            .withColumn("entity_type", lit("group"))
            .withColumn("avg_match_score", col("avg_match_score").cast("decimal(5,2)"))
        )
        stats.append(group_stats)

    if not stats:
        return spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("entity_type", StringType(), True),
                    StructField("total_candidates", LongType(), True),
                    StructField("snapshot_at", TimestampType(), True),
                ]
            ),
        )

    result = stats[0]
    for df in stats[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result.withColumn("snapshot_at", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tables - Latest Batch Summary
# MAGIC
# MAGIC Quick view of most recent ingestion per entity type.

# COMMAND ----------


@dlt.table(
    name="latest_batch_summary",
    comment="Most recent ingestion batch for each entity type and environment",
    table_properties={"quality": "audit", "pipelines.autoOptimize.managed": "true"},
)
def latest_batch_summary():
    """
    Summary showing the most recent batch for each entity/environment combination.
    Useful for monitoring pipeline freshness.
    """
    batches = dlt.read("ingestion_batches")

    # Window to get latest batch per entity/environment
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy("entity_type", "environment").orderBy(col("dlt_ingested_at").desc())

    return (
        batches.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .select(
            col("entity_type"),
            col("environment"),
            col("tenant_name"),
            col("batch_id").alias("latest_batch_id"),
            col("record_count").alias("latest_record_count"),
            col("ingested_at").alias("latest_ingested_at"),
            col("dlt_ingested_at").alias("latest_dlt_processed_at"),
            current_timestamp().alias("snapshot_at"),
        )
    )


# COMMAND ----------

from pyspark.sql.functions import row_number

# COMMAND ----------

# MAGIC %md
# MAGIC ## Future Audit Tables
# MAGIC
# MAGIC **When pipeline execution tracking is needed:**
# MAGIC - `pipeline_execution_log` - Track DLT pipeline runs (start, end, status, errors)
# MAGIC - Requires: Databricks Jobs API integration or DLT event log parsing
# MAGIC
# MAGIC **When data drift detection is needed:**
# MAGIC - `schema_changes` - Track when Bronze schemas evolve
# MAGIC - `data_drift_metrics` - Statistical distribution changes over time
