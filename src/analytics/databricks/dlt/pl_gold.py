# Databricks notebook source
# MAGIC %md
# MAGIC # M365 Migration - Gold Layer Pipeline
# MAGIC
# MAGIC **Pipeline:** `pl_m365_gold`
# MAGIC **Target Schema:** `m365_migration.gold`
# MAGIC
# MAGIC ## Tables
# MAGIC | Table | Type | Notes |
# MAGIC |---|---|---|
# MAGIC | dim_organization | Dimension | CDO/org dimension from organizations.json config. One row per org. FK: org_id on dim_user, dim_people, rationalization_input, dual_mailbox_users, groups_no_owner, users_with_active_devices. Customer-specific enrichment (CDO/AE) in extensions/pl_gold_extensions.py. |
# MAGIC | domain_overlap_warnings | Audit | Domains mapped to more than one org in organizations.json. Empty = healthy. Non-empty = config fix required. |
# MAGIC | dim_user | Dimension | Conformed user dimension. PK: user_silver_id. One row per user. |
# MAGIC | dim_mail_users | Dimension | EXO Mail Users + resolved MTO Entra identity |
# MAGIC | dim_contacts | Dimension | Entra contacts source + target |
# MAGIC | dim_group | Dimension | Conformed group dimension. PK: group_silver_id. One row per group. |
# MAGIC | dim_people | Dimension | In-scope persons (source anchor, E/F license, non-resource mailbox). One row per source user. Includes source+target identity, organization, mailbox/OneDrive presence+sizes, migration_path, migration_status. PK: person_id = md5(source_user_id). #485 T5. |
# MAGIC | shared_data_sets | Fact/Dimension | Atomic schedulable units (team, m365_group, site). One row per shared data container. PK: shared_data_set_id = md5(source_key &#124; type &#124; anchor_id). FK: primary_site_id → dim_sites.site_silver_id, hub_shared_data_set_id → self (hub parent), primary_organization_id → dim_organization.org_id. #497 T2. |
# MAGIC | shared_data_set_organizations | Fact | Multi-valued scored org association per shared_data_set. One row per (shared_data_set_id, environment, organization_id) for every in-scope organization that contributes ≥1 owner. PK: shared_data_set_organization_id = md5(shared_data_set_id &#124; environment &#124; organization_id). FK: shared_data_set_id → shared_data_sets, organization_id → dim_organization.org_id. confidence_score is a 0–1 fraction (owners_in_org / owner_count_in_scope, decimal(5,4)); sums to ≤1.0000 per shared_data_set when ≥1 in-scope owner. #497 T2c. |
# MAGIC | dim_sites | Dimension | Site-grain dimension. One row per non-personal SPO site. PK: site_silver_id. FK: shared_data_set_id → shared_data_sets, hub_shared_data_set_id → shared_data_sets (hub parent). site_type ∈ {primary, channel_private, channel_shared, standalone, hub}. #497 T2. |
# MAGIC | user_map | Map | Approved source→target user mappings |
# MAGIC | group_map | Map | Approved source→target group mappings |
# MAGIC | rationalization_input | Output | 12-field output for Owen's automation subsystem + rationalization status |
# MAGIC | dual_mailbox_users | Assessment | Matched targets with existing mailbox (#59) |
# MAGIC | groups_no_owner | Assessment | Source groups with members but no owner, top-3 suggested owners (#52) |
# MAGIC | groups_accept_external_email | Assessment | Source groups (DL, MES, M365) that accept email from the internet (`requires_sender_authentication = false`). Filter by `group_type`, `upn_domain`, or `on_prem_domain_name` for AE-level reporting. #53. |
# MAGIC | mapping_issues | Assessment | Ambiguous/missing/low-confidence mappings |
# MAGIC | migration_summary | Summary | High-level counts by environment |
# MAGIC | people_summary | Summary | migration_path and migration_status counts |
# MAGIC | mapping_candidates_summary | Summary | Candidates by context/score/status |
# MAGIC | non_person_mailboxes | Reporting | Shared/room/equipment mailboxes |
# MAGIC | data_volumes | Reporting | Storage volumes by type/environment |
# MAGIC | dim_spo_site | Dimension | One row per SharePoint/OneDrive site. PK: site_silver_id. Source of descriptive site attributes for the spo_* facts. |
# MAGIC | spo_custom_permission_burden | Reporting | Per-site SPO permission complexity signals (FK: site_silver_id → dim_spo_site) |
# MAGIC | spo_subweb_complexity | Reporting | Per-site SPO subweb topology and depth (FK: site_silver_id → dim_spo_site) |
# MAGIC | spo_sharing_posture | Reporting | Per-site SPO sharing-link posture (FK: site_silver_id → dim_spo_site) |
# MAGIC | storage_summary | Reporting | Total storage rollup |
# MAGIC | users_with_active_devices | Reporting | Users with active Intune devices in source environment |

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col,
    current_timestamp,
    current_date,
    lit,
    coalesce,
    when,
    count,
    countDistinct,
    sum as spark_sum,
    row_number,
    concat,
    concat_ws,
    max as spark_max,
    min as spark_min,
    split,
    explode,
    md5,
    first as spark_first,
    trim,
    lower,
    round as spark_round,
    datediff,
    collect_set,
    collect_list,
    array,
    array_contains,
    array_join,
    array_sort,
    expr,
    size,
    to_timestamp,
    regexp_extract,
    regexp_replace,
)
from pyspark.sql.window import Window

# COMMAND ----------

CATALOG = spark.conf.get("catalog", "")
if not CATALOG:
    raise RuntimeError(
        "Pipeline configuration missing required key: 'catalog'. "
        "Set catalog in the DLT pipeline configuration before running."
    )
SILVER_SCHEMA = spark.conf.get("silver_schema", "silver")
GOLD_SCHEMA = spark.conf.get("gold_schema", "gold")
AUTO_APPROVE_THRESHOLD = 85.0

# COMMAND ----------


def normalized_domain(email_col):
    return lower(trim(split(email_col, "@")[1]))


def _exploded_org_domains(orgs):
    """
    Explodes all three domain arrays in silver.organizations into a flat
    (domain_value, org_id) DataFrame with lower/trim normalisation applied.
    Shared by get_domain_org_map() and domain_overlap_warnings().
    """
    upn = orgs.select(col("org_id"), explode(col("upn_domains")).alias("domain_value"))
    mail = orgs.select(col("org_id"), explode(col("mail_domains")).alias("domain_value"))
    op = orgs.select(col("org_id"), explode(col("on_prem_domains")).alias("domain_value"))
    return (
        upn.unionByName(mail)
        .unionByName(op)
        .select(lower(trim(col("domain_value"))).alias("domain_value"), col("org_id"))
    )


def get_domain_org_map():
    """
    Reads silver.organizations (materialized from organizations.json by pl_ma_toolkit_silver) and
    returns a lookup DataFrame: domain_value → org_id covering all three domain array types.
    Returns an empty DataFrame when silver.organizations is empty — all org_ids will be NULL.
    """
    from pyspark.sql.types import StructType, StructField, StringType as ST

    empty = spark.createDataFrame(
        [], StructType([StructField("domain_value", ST(), True), StructField("org_id", ST(), True)])
    )
    try:
        orgs = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.organizations")
    except Exception:
        return empty
    if orgs.isEmpty():
        return empty
    # Collapse to a single row per domain_value by picking min(org_id) so downstream
    # left-joins remain 1:1 and never duplicate rows.  Overlaps are surfaced by the
    # domain_overlap_warnings DLT table (see below) without requiring collect().
    return _exploded_org_domains(orgs).groupBy("domain_value").agg(spark_min(col("org_id")).alias("org_id"))


def _resolve_user_org_id(df):
    """
    Enriches a user DataFrame (must have user_principal_name, mail, on_prem_domain_name)
    with an org_id column via COALESCE(upn_domain, mail_domain, on_prem_domain) lookup.
    All temporary join columns are dropped before returning.
    """
    dom = get_domain_org_map()
    dom_upn = dom.select(col("domain_value").alias("__d_upn"), col("org_id").alias("__org_upn"))
    dom_mail = dom.select(col("domain_value").alias("__d_mail"), col("org_id").alias("__org_mail"))
    dom_op = dom.select(col("domain_value").alias("__d_op"), col("org_id").alias("__org_op"))
    return (
        df.withColumn("__upn_dom", normalized_domain(col("user_principal_name")))
        .withColumn(
            "__mail_dom",
            when(col("mail").isNotNull() & col("mail").contains("@"), lower(trim(split(col("mail"), "@")[1]))),
        )
        .withColumn("__op_dom", lower(trim(col("on_prem_domain_name"))))
        .join(dom_upn, col("__upn_dom") == col("__d_upn"), "left")
        .join(dom_mail, col("__mail_dom") == col("__d_mail"), "left")
        .join(dom_op, col("__op_dom") == col("__d_op"), "left")
        .withColumn("org_id", coalesce(col("__org_upn"), col("__org_mail"), col("__org_op")))
        .drop(
            "__upn_dom",
            "__mail_dom",
            "__op_dom",
            "__d_upn",
            "__org_upn",
            "__d_mail",
            "__org_mail",
            "__d_op",
            "__org_op",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_tenant

# COMMAND ----------


@dlt.table(
    name="dim_tenant",
    comment=(
        "Tenant dimension. Grain: one row per source_key. PK: source_key. "
        "Maps source_key → tenant_id (Entra GUID) and tenant_role (source/target). "
        "Sourced from DLT pipeline params source_tenant_names/source_tenant_ids and "
        "target_tenant_names/target_tenant_ids (parallel comma lists). Used by "
        "spo_custom_permission_burden to distinguish own-tenant vs foreign-tenant apps. "
        "Update pipeline params (and tenants.json) when onboarding new environments."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
@dlt.expect_or_drop("valid_source_key", "source_key IS NOT NULL")
@dlt.expect_or_drop("valid_tenant_id", "tenant_id IS NOT NULL")
def dim_tenant():
    from pyspark.sql.types import StructType, StructField, StringType

    def _pair(names_param: str, ids_param: str, role: str):
        names = [x.strip() for x in spark.conf.get(names_param, "").split(",") if x.strip()]
        ids = [x.strip() for x in spark.conf.get(ids_param, "").split(",") if x.strip()]
        if names and not ids:
            raise ValueError(
                f"Missing required pipeline param: {ids_param}. Provide one tenant GUID per entry in {names_param}."
            )
        if ids and not names:
            raise ValueError(
                f"Missing required pipeline param: {names_param}. Provide one source_key per entry in {ids_param}."
            )
        if len(names) != len(ids):
            raise ValueError(
                f"{names_param} ({len(names)} entries) and {ids_param} ({len(ids)} entries) "
                f"must be parallel comma-separated lists."
            )
        return [(name, tid, role) for name, tid in zip(names, ids)]

    rows = _pair("source_tenant_names", "source_tenant_ids", "source") + _pair(
        "target_tenant_names", "target_tenant_ids", "target"
    )

    # Enforce PK(source_key): fail fast on duplicate source_key entries across params.
    from collections import Counter

    dup_source_keys = [k for k, c in Counter([r[0] for r in rows]).items() if c > 1]
    if dup_source_keys:
        raise ValueError(f"Duplicate source_key values in tenant params: {', '.join(sorted(dup_source_keys))}")
    schema = StructType(
        [
            StructField("source_key", StringType(), False),
            StructField("tenant_id", StringType(), False),
            StructField("tenant_role", StringType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema).withColumn("gold_loaded_at", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date

# COMMAND ----------


@dlt.table(
    name="dim_date",
    comment="Calendar dimension - 2020-01-01 to 2030-12-31. Grain: one row per day. PK: date_key. Use iso_year + week_of_year (not year + week_of_year) when grouping by ISO week to avoid year-boundary mismatches.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
@dlt.expect_or_drop("valid_date_key", "date_key IS NOT NULL")
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
def dim_date():
    return spark.sql(
        """
        SELECT
            cast(date_format(d, 'yyyyMMdd') as int)             AS date_key,
            d                                                   AS date,
            cast(year(d) as int)                                AS year,
            cast(quarter(d) as int)                             AS quarter,
            cast(month(d) as int)                               AS month,
            date_format(d, 'MMMM')                              AS month_name,
            date_format(d, 'MMM')                               AS month_short,
            cast(day(d) as int)                                 AS day,
            iso_dow                                             AS day_of_week,
            date_format(d, 'EEEE')                              AS day_name,
            cast(weekofyear(d) as int)                          AS week_of_year,
            cast(extract(YEAROFWEEK FROM d) as int)             AS iso_year,
            date_format(d, 'yyyy-MM')                           AS year_month,
            concat(cast(year(d) as string), '-Q', cast(quarter(d) as string))
                                                                AS year_quarter,
            iso_dow IN (6, 7)                                   AS is_weekend,
            d = last_day(d)                                     AS is_month_end,
            month(d) IN (3, 6, 9, 12) AND d = last_day(d)       AS is_quarter_end,
            month(d) = 12 AND day(d) = 31                       AS is_year_end,
            current_timestamp()                                 AS gold_loaded_at
        FROM (
            SELECT
                d,
                cast(((dayofweek(d) + 5) % 7) + 1 as int) AS iso_dow
            FROM (
                SELECT explode(
                    sequence(DATE'2020-01-01', DATE'2030-12-31', interval 1 day)
                ) AS d
            )
        )
        """
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## domain_overlap_warnings

# COMMAND ----------


@dlt.table(
    name="domain_overlap_warnings",
    comment="Audit table: domains assigned to more than one org in organizations.json. Populated only when config has overlapping entries. An empty table is the healthy state. Gold joins resolve to min(org_id) when overlaps exist — fix organizations.json and redeploy to correct affected org_id FKs.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def domain_overlap_warnings():
    orgs = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.organizations")
    org_meta = orgs.select(col("org_id"), col("org_key"), col("org_name"))
    # Join meta before aggregating so we can collect_set on org_key/org_name directly,
    # avoiding the need to reverse-map md5 hashes when reading the output.
    enriched = _exploded_org_domains(orgs).join(org_meta, on="org_id", how="left")
    return (
        enriched.groupBy("domain_value")
        .agg(
            countDistinct("org_id").alias("org_count"),
            spark_min(col("org_id")).alias("resolved_org_id"),  # same value get_domain_org_map() picks
            array_join(array_sort(collect_set(col("org_key"))), ", ").alias("conflicting_org_keys"),
            array_join(
                array_sort(collect_set(coalesce(col("org_name"), col("org_key")))),
                ", ",
            ).alias("conflicting_org_names"),
        )
        .filter(col("org_count") > 1)
        .join(org_meta.alias("r"), col("resolved_org_id") == col("r.org_id"), "left")
        .select(
            col("domain_value"),
            col("org_count"),
            col("conflicting_org_keys"),
            col("conflicting_org_names"),
            col("resolved_org_id"),
            col("r.org_key").alias("resolved_org_key"),
            col("r.org_name").alias("resolved_org_name"),
        )
        .withColumn("gold_loaded_at", current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_organization

# COMMAND ----------


@dlt.table(
    name="dim_organization",
    comment="Organization dimension. One row per CDO/AE organization from organizations.json config. Replaces dim_domain. Stats reflect source Member counts resolved by org via domain lookup. PK: org_id (md5 of org_key). Reads from silver.organizations (set organizations_config_path in the Silver pipeline param). Customer-specific enrichment in extensions/pl_gold_extensions.py.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def dim_organization():
    base_orgs = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.organizations")

    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")
    source_members = users.filter((col("environment") == "source") & (col("user_type") == "Member"))
    members_with_org = _resolve_user_org_id(source_members).filter(col("org_id").isNotNull())

    org_stats = members_with_org.groupBy("org_id").agg(
        count("*").alias("total_users"),
        spark_sum(col("account_enabled").cast("int")).alias("enabled_users"),
        spark_sum(col("on_prem_sync_enabled").cast("int")).alias("synced_users"),
        spark_sum(col("has_e3_license").cast("int")).alias("e3_users"),
        spark_sum(col("has_e5_license").cast("int")).alias("e5_users"),
        spark_sum(col("has_f3_license").cast("int")).alias("f3_users"),
        spark_sum(col("has_e1_license").cast("int")).alias("e1_users"),
    )

    real_orgs = (
        base_orgs.alias("o")
        .join(org_stats.alias("s"), col("o.org_id") == col("s.org_id"), "left")
        .select(
            col("o.org_id"),
            col("o.org_key"),
            col("o.org_name"),
            col("o.upn_domains"),
            col("o.mail_domains"),
            col("o.on_prem_domains"),
            coalesce(col("s.total_users"), lit(0).cast("long")).alias("total_users"),
            coalesce(col("s.enabled_users"), lit(0).cast("long")).alias("enabled_users"),
            coalesce(col("s.synced_users"), lit(0).cast("long")).alias("synced_users"),
            coalesce(col("s.e3_users"), lit(0).cast("long")).alias("e3_users"),
            coalesce(col("s.e5_users"), lit(0).cast("long")).alias("e5_users"),
            coalesce(col("s.f3_users"), lit(0).cast("long")).alias("f3_users"),
            coalesce(col("s.e1_users"), lit(0).cast("long")).alias("e1_users"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )

    # Sentinel row so that fact rows coalesced to md5('__unassigned__') have a
    # matching FK in dim_organization and are sliceable in Power BI reports.
    unassigned_row = spark.range(1).select(
        md5(lit("__unassigned__")).alias("org_id"),
        lit("__unassigned__").alias("org_key"),
        lit("Unassigned").alias("org_name"),
        lit(None).cast("array<string>").alias("upn_domains"),
        lit(None).cast("array<string>").alias("mail_domains"),
        lit(None).cast("array<string>").alias("on_prem_domains"),
        lit(0).cast("long").alias("total_users"),
        lit(0).cast("long").alias("enabled_users"),
        lit(0).cast("long").alias("synced_users"),
        lit(0).cast("long").alias("e3_users"),
        lit(0).cast("long").alias("e5_users"),
        lit(0).cast("long").alias("f3_users"),
        lit(0).cast("long").alias("e1_users"),
        current_timestamp().alias("gold_loaded_at"),
    )

    return real_orgs.union(unassigned_row)


# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_mail_users

# COMMAND ----------


@dlt.table(
    name="dim_mail_users",
    comment="Mail User dimension - EXO Mail Users in target tenant with resolved Entra MTO identity",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def dim_mail_users():
    mail_users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mail_users")
    mto_mapping = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mto_user_entity_mapping")

    return (
        mail_users.alias("mu")
        .join(mto_mapping.alias("mto"), col("mu.mail_user_id") == col("mto.mail_user_id"), "left")
        .select(
            col("mu.mail_user_id"),
            col("mu.environment"),
            col("mu.source_key"),
            col("mu.guid"),
            col("mu.display_name"),
            col("mu.given_name"),
            col("mu.surname"),
            col("mu.alias"),
            col("mu.primary_smtp_address"),
            col("mu.external_email_address"),
            col("mu.external_directory_object_id"),
            col("mu.recipient_type_details"),
            col("mu.is_dir_synced"),
            col("mu.hidden_from_address_lists"),
            col("mu.company"),
            col("mu.department"),
            col("mu.job_title"),
            col("mto.entra_user_id").alias("mto_entra_user_id"),
            col("mto.entra_object_id").alias("mto_entra_object_id"),
            col("mto.entra_upn").alias("mto_entra_upn"),
            col("mto.entra_display_name").alias("mto_entra_display_name"),
            when(col("mto.entra_user_id").isNotNull(), lit(True)).otherwise(lit(False)).alias("has_entra_link"),
            col("mu.source_created_at"),
            col("mu.last_changed_at"),
            col("mu.last_updated_at"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_contacts

# COMMAND ----------


@dlt.table(
    name="dim_contacts",
    comment="Contact dimension - Entra organisational contacts (external mail recipients). Source and target.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def dim_contacts():
    contacts = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.contacts")

    return contacts.select(
        col("contact_id"),
        col("environment"),
        col("source_key"),
        col("entra_object_id"),
        col("display_name"),
        col("given_name"),
        col("surname"),
        col("mail"),
        col("proxy_addresses"),
        col("company_name"),
        col("department"),
        col("job_title"),
        lit(None).cast("string").alias("mapped_to_user_id"),
        lit(None).cast("string").alias("mapped_to_entity_type"),
        col("last_updated_at"),
        current_timestamp().alias("gold_loaded_at"),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## user_map
# MAGIC
# MAGIC **target_user_id semantics by context:**
# MAGIC - `primary_migration` → Entra user_id of target Member
# MAGIC - `external_linkage`  → mail_user_id of target EXO Mail User
# MAGIC - `ad_user_linkage`   → ad_user_id (Sprint 2)

# COMMAND ----------


@dlt.table(
    name="user_map",
    comment="Approved source-to-target user mappings. One row per (source_user_id, match_context). Sourced from Silver user_mapping_candidates.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def user_map():
    candidates = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.user_mapping_candidates")

    window_spec = Window.partitionBy("source_user_id", "match_context").orderBy(
        col("match_score").desc(), col("created_at").asc()
    )

    return (
        candidates.withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .withColumn("mapping_status", col("candidate_status"))
        .withColumn(
            "approval_method",
            when(col("candidate_status") == "approved", lit("auto")).otherwise(lit(None).cast("string")),
        )
        .withColumn(
            "approved_at",
            when(col("candidate_status") == "approved", current_timestamp()).otherwise(lit(None).cast("timestamp")),
        )
        .withColumn(
            "approved_by",
            when(col("candidate_status") == "approved", lit("system")).otherwise(lit(None).cast("string")),
        )
        .withColumn("_original_target", col("target_user_id"))
        .withColumn(
            "target_user_id",
            when(col("match_context") == "primary_migration", col("_original_target")).otherwise(
                lit(None).cast("string")
            ),
        )
        .withColumn(
            "target_mail_user_id",
            when(col("match_context") == "external_linkage", col("_original_target")).otherwise(
                lit(None).cast("string")
            ),
        )
        .drop("_original_target")
        .select(
            md5(concat(col("source_user_id"), lit("_"), col("match_context"))).alias("user_map_id"),
            "source_user_id",
            "target_user_id",
            "target_mail_user_id",
            "match_context",
            "match_score",
            "match_type",
            "match_attributes",
            "mapping_scenario",
            "candidate_status",
            "mapping_status",
            "approval_method",
            "approved_at",
            "approved_by",
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## group_map

# COMMAND ----------


@dlt.table(
    name="group_map",
    comment="Approved source-to-target group mappings.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def group_map():
    candidates = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.group_mapping_candidates")

    window_spec = Window.partitionBy("source_group_id").orderBy(col("match_score").desc(), col("created_at").asc())

    return (
        candidates.withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .withColumn(
            "mapping_status",
            when(col("match_score") >= AUTO_APPROVE_THRESHOLD, lit("approved")).otherwise(lit("pending_review")),
        )
        .withColumn(
            "approval_method",
            when(col("match_score") >= AUTO_APPROVE_THRESHOLD, lit("auto")).otherwise(lit(None).cast("string")),
        )
        .withColumn(
            "approved_at",
            when(col("mapping_status") == "approved", current_timestamp()).otherwise(lit(None).cast("timestamp")),
        )
        .select(
            md5(concat(col("source_group_id"), lit("_"), col("target_group_id"))).alias("group_map_id"),
            "source_group_id",
            "target_group_id",
            "match_score",
            "match_type",
            "match_attributes",
            "mapping_status",
            "approval_method",
            "approved_at",
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_people

# COMMAND ----------


@dlt.table(
    name="dim_people",
    comment=(
        "Canonical persons in scope for migration. Grain: one row per source-tenant Entra user "
        "(silver.users where environment='source') that holds any E or F license and is NOT a "
        "Shared/Room/Equipment mailbox owner. account_enabled is NOT a filter — disabled users "
        "with qualifying licenses still appear. PK: person_id = md5(source_user_id). "
        "Carries source + target identity (12 descriptive fields), organization (FK + name), "
        "mailbox + OneDrive presence and sizes per environment, plus derived migration_path "
        "and migration_status. #485 T5. "
        "source_mailbox_size_mb / target_mailbox_size_mb are NULL today — silver.exo_mailbox_statistics "
        "is a Sprint 2 dependency. "
        "migration_path values: parallel | dual | standard | ambiguous (default). "
        "migration_status values: migrated | ambiguous | pending (default). "
        "Address-based comparisons for migration_status use ALL target mailbox proxy addresses "
        "from email_addresses (prefix-stripped, lowercased), not just primary_smtp_address."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def dim_people():
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")
    mailboxes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mailboxes")
    mail_users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mail_users")
    spo_sites = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_sites")
    candidates = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.user_mapping_candidates")
    organizations = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.organizations")

    # ------------------------------------------------------------------
    # In-scope filter: any E or F license, and NOT a shared/room/equipment
    # mailbox owner (identified by external_directory_object_id match on a
    # source mailbox whose recipient_type is Shared/Room/Equipment).
    # ------------------------------------------------------------------
    non_person_oids = (
        mailboxes.filter(
            (col("environment") == "source")
            & col("recipient_type").isin("SharedMailbox", "RoomMailbox", "EquipmentMailbox")
            & col("external_directory_object_id").isNotNull()
        )
        .select(col("external_directory_object_id").alias("np_oid"))
        .distinct()
    )

    source_users = (
        _resolve_user_org_id(users.filter(col("environment") == "source"))
        .filter(col("has_e_or_f_license"))
        .join(non_person_oids, lower(trim(col("entra_object_id"))) == col("np_oid"), "left_anti")
        .alias("su")
    )

    # ------------------------------------------------------------------
    # Source ↔ target match: Pass 1 (employee_id_and_upn) approved only.
    # candidate_status='approved' implies one_to_one cardinality (silver
    # rule), so we can safely drive a 1:1 left join.
    # ------------------------------------------------------------------
    pass1_approved = (
        candidates.filter(
            (col("match_context") == "primary_migration")
            & (col("match_type") == "employee_id_and_upn")
            & (col("candidate_status") == "approved")
        )
        .select("source_user_id", "target_user_id")
        .alias("p1")
    )

    target_users = users.filter(col("environment") == "target").alias("tu")

    # ------------------------------------------------------------------
    # Mailbox presence per environment. We restrict to non-resource
    # mailboxes so a user with both a regular and a shared mailbox tied to
    # the same OID is not double-counted (the in-scope filter already drops
    # users whose primary mailbox is shared/room/equipment).
    # ------------------------------------------------------------------
    person_mbx_types = ["UserMailbox", "RemoteUserMailbox", "LegacyMailbox"]

    source_mbx = (
        mailboxes.filter(
            (col("environment") == "source")
            & col("external_directory_object_id").isNotNull()
            & col("recipient_type").isin(person_mbx_types)
        )
        .select(
            col("external_directory_object_id").alias("src_mbx_oid"),
            lower(regexp_replace(trim(col("forwarding_smtp_address")), "(?i)^[a-z0-9]+:", "")).alias(
                "src_mbx_fwd_norm"
            ),
            col("deliver_to_mailbox_and_forward").alias("src_mbx_dtmaf"),
        )
        .dropDuplicates(["src_mbx_oid"])
        .alias("smbx")
    )

    target_mbx_base = mailboxes.filter(
        (col("environment") == "target")
        & col("external_directory_object_id").isNotNull()
        & col("recipient_type").isin(person_mbx_types)
    )

    # Exploded, prefix-stripped, lowered proxy set per target mailbox.
    # Strip any leading "<prefix>:" (SMTP:, smtp:, sip:, x500: etc).
    tgt_mbx_proxies = (
        target_mbx_base.select(
            col("external_directory_object_id").alias("tmbx_oid"),
            explode(coalesce(col("email_addresses"), array().cast("array<string>"))).alias("addr"),
        )
        .withColumn("addr_norm", lower(regexp_replace(trim(col("addr")), "(?i)^[a-z0-9]+:", "")))
        .filter(col("addr_norm").isNotNull() & (col("addr_norm") != ""))
        .groupBy("tmbx_oid")
        .agg(collect_set(col("addr_norm")).alias("tgt_proxy_set"))
    )

    target_mbx = (
        target_mbx_base.select(
            col("external_directory_object_id").alias("tgt_mbx_oid"),
            col("forwarding_smtp_address").alias("tgt_mbx_fwd"),
        )
        .dropDuplicates(["tgt_mbx_oid"])
        .join(
            tgt_mbx_proxies.withColumnRenamed("tmbx_oid", "tgt_mbx_oid"),
            "tgt_mbx_oid",
            "left",
        )
        .alias("tmbx")
    )

    # ------------------------------------------------------------------
    # Source-tenant MailUser (for migration_status rule (a) — source user
    # has been converted to a MailUser whose external_email_address points
    # at the target mailbox proxy address set).
    # external_email_address is already prefix-stripped + lowered in
    # mail_users_staged (pl_silver.py).
    # ------------------------------------------------------------------
    source_mail_user = (
        mail_users.filter(
            (col("environment") == "source")
            & col("external_directory_object_id").isNotNull()
            & (col("recipient_type_details") == "MailUser")
        )
        .select(
            lower(trim(col("external_directory_object_id"))).alias("smu_oid"),
            col("external_email_address").alias("smu_ext_email"),
        )
        .dropDuplicates(["smu_oid"])
        .alias("smu")
    )

    # ------------------------------------------------------------------
    # OneDrive presence per environment.
    # ------------------------------------------------------------------
    source_od = (
        spo_sites.filter(
            (col("environment") == "source") & (col("is_personal_site")) & col("owner_entra_object_id").isNotNull()
        )
        .select(
            col("owner_entra_object_id").alias("sod_oid"),
            col("storage_used_mb").alias("sod_size_mb"),
        )
        .dropDuplicates(["sod_oid"])
        .alias("sod")
    )

    target_od = (
        spo_sites.filter(
            (col("environment") == "target") & (col("is_personal_site")) & col("owner_entra_object_id").isNotNull()
        )
        .select(
            col("owner_entra_object_id").alias("tod_oid"),
            col("storage_used_mb").alias("tod_size_mb"),
        )
        .dropDuplicates(["tod_oid"])
        .alias("tod")
    )

    # ------------------------------------------------------------------
    # Organization name lookup (denormalized onto each row).
    # ------------------------------------------------------------------
    org_lookup = organizations.select(
        col("org_id").alias("o_org_id"),
        col("org_name").alias("o_org_name"),
    ).alias("org")

    # ------------------------------------------------------------------
    # Assemble: source user → (pass1 → target user) + presence joins
    # ------------------------------------------------------------------
    joined = (
        source_users.join(pass1_approved, col("su.user_id") == col("p1.source_user_id"), "left")
        .join(target_users, col("p1.target_user_id") == col("tu.user_id"), "left")
        .join(source_mbx, lower(trim(col("su.entra_object_id"))) == col("smbx.src_mbx_oid"), "left")
        .join(target_mbx, lower(trim(col("tu.entra_object_id"))) == col("tmbx.tgt_mbx_oid"), "left")
        .join(source_mail_user, lower(trim(col("su.entra_object_id"))) == col("smu.smu_oid"), "left")
        .join(source_od, col("su.entra_object_id") == col("sod.sod_oid"), "left")
        .join(target_od, col("tu.entra_object_id") == col("tod.tod_oid"), "left")
        .join(org_lookup, col("su.org_id") == col("o_org_id"), "left")
    )

    has_src_mbx = col("smbx.src_mbx_oid").isNotNull()
    has_tgt_mbx = col("tmbx.tgt_mbx_oid").isNotNull()
    has_src_od = col("sod.sod_oid").isNotNull()
    has_tgt_od = col("tod.tod_oid").isNotNull()

    # migration_status derivation. Address checks compare against the full
    # target proxy set (prefix-stripped, lowered). Target mailbox must have
    # NO forwarding configured to be the terminal endpoint.
    tgt_proxies_present = col("tmbx.tgt_proxy_set").isNotNull()
    src_mu_to_tgt_proxy = (
        col("smu.smu_ext_email").isNotNull()
        & tgt_proxies_present
        & array_contains(col("tmbx.tgt_proxy_set"), col("smu.smu_ext_email"))
    )
    src_fwd_to_tgt_proxy = (
        col("smbx.src_mbx_fwd_norm").isNotNull()
        & (col("smbx.src_mbx_fwd_norm") != "")
        & tgt_proxies_present
        & array_contains(col("tmbx.tgt_proxy_set"), col("smbx.src_mbx_fwd_norm"))
    )
    tgt_is_terminal = col("tmbx.tgt_mbx_fwd").isNull() | (trim(col("tmbx.tgt_mbx_fwd")) == lit(""))

    is_migrated = (
        has_tgt_mbx
        & tgt_is_terminal
        & (src_mu_to_tgt_proxy | (src_fwd_to_tgt_proxy & (col("smbx.src_mbx_dtmaf") == lit(False))))
    )
    is_status_ambiguous = has_src_mbx & has_tgt_mbx & src_fwd_to_tgt_proxy & (col("smbx.src_mbx_dtmaf") == lit(True))

    enriched = joined.select(
        md5(lower(trim(col("su.user_id")))).alias("person_id"),
        col("su.user_id").alias("source_user_id"),
        col("p1.target_user_id").alias("target_user_id"),
        col("su.source_key").alias("source_tenant_key"),
        col("tu.source_key").alias("target_tenant_key"),
        when(col("p1.target_user_id").isNotNull(), col("su.employee_id"))
        .otherwise(lit(None).cast("string"))
        .alias("matched_on_employee_id"),
        col("su.org_id").alias("organization_id"),
        col("o_org_name").alias("organization_name"),
        col("su.display_name").alias("display_name_source"),
        col("su.mail").alias("primary_email_source"),
        col("su.user_principal_name").alias("user_principal_name_source"),
        col("su.job_title").alias("job_title_source"),
        col("su.department").alias("department_source"),
        col("su.company_name").alias("company_source"),
        col("su.office_location").alias("office_location_source"),
        col("tu.display_name").alias("display_name_target"),
        col("tu.mail").alias("primary_email_target"),
        col("tu.user_principal_name").alias("user_principal_name_target"),
        col("tu.job_title").alias("job_title_target"),
        col("tu.department").alias("department_target"),
        col("tu.company_name").alias("company_target"),
        col("tu.office_location").alias("office_location_target"),
        has_src_mbx.alias("has_source_mailbox"),
        lit(None).cast("decimal(18,2)").alias("source_mailbox_size_mb"),
        has_tgt_mbx.alias("has_target_mailbox"),
        lit(None).cast("decimal(18,2)").alias("target_mailbox_size_mb"),
        has_src_od.alias("has_source_onedrive"),
        when(has_src_od, col("sod.sod_size_mb")).cast("decimal(18,2)").alias("source_onedrive_size_mb"),
        has_tgt_od.alias("has_target_onedrive"),
        when(has_tgt_od, col("tod.tod_size_mb")).cast("decimal(18,2)").alias("target_onedrive_size_mb"),
        when(is_migrated, lit("migrated"))
        .when(is_status_ambiguous, lit("ambiguous"))
        .otherwise(lit("pending"))
        .alias("migration_status"),
        col("su.last_updated_at").alias("last_updated_at"),
    )

    # ------------------------------------------------------------------
    # migration_path derivation. Parallel = >=2 distinct persons share the
    # same non-null target_user_id. count(target_user_id) over a partition
    # by target_user_id returns 0 for the all-null partition, so unmatched
    # persons are never flagged parallel.
    # ------------------------------------------------------------------
    share_window = Window.partitionBy("target_user_id")
    with_path = enriched.withColumn("_tgt_share_count", count(col("target_user_id")).over(share_window)).withColumn(
        "migration_path",
        when(col("_tgt_share_count") > 1, lit("parallel"))
        .when(col("has_source_mailbox") & col("has_target_mailbox"), lit("dual"))
        .when(col("has_source_mailbox") & (~col("has_target_mailbox")), lit("standard"))
        .otherwise(lit("ambiguous")),
    )

    # Final column order — gold_loaded_at MUST be last.
    return with_path.select(
        "person_id",
        "source_user_id",
        "target_user_id",
        "source_tenant_key",
        "target_tenant_key",
        "matched_on_employee_id",
        "organization_id",
        "organization_name",
        "display_name_source",
        "primary_email_source",
        "user_principal_name_source",
        "job_title_source",
        "department_source",
        "company_source",
        "office_location_source",
        "display_name_target",
        "primary_email_target",
        "user_principal_name_target",
        "job_title_target",
        "department_target",
        "company_target",
        "office_location_target",
        "has_source_mailbox",
        "source_mailbox_size_mb",
        "has_target_mailbox",
        "target_mailbox_size_mb",
        "has_source_onedrive",
        "source_onedrive_size_mb",
        "has_target_onedrive",
        "target_onedrive_size_mb",
        "migration_path",
        "migration_status",
        "last_updated_at",
    ).withColumn("gold_loaded_at", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Data Sets (issue #497)
# MAGIC
# MAGIC Two gold tables with circular FK relationships resolved via deterministic key derivation:
# MAGIC - `shared_data_sets` — Atomic schedulable units (team, m365_group, site). One row per shared data container.
# MAGIC - `dim_sites` — Site-grain dimension. One row per non-personal SPO site.
# MAGIC
# MAGIC Key derivation:
# MAGIC ```
# MAGIC shared_data_set_id = md5(concat_ws('|', lower(source_key), type, anchor_id))
# MAGIC   where type ∈ {team, m365_group, site}
# MAGIC   and anchor_id = entra_group_id (team/m365_group) or sharepoint_id (site)
# MAGIC
# MAGIC primary_site_id = site_silver_id of the primary site
# MAGIC   - team / m365_group → spo_sites WHERE group_id = entra_group_id
# MAGIC   - site              → spo_sites.site_silver_id (itself)
# MAGIC ```
# MAGIC
# MAGIC Standalone site filter: `is_personal_site=false AND (group_id IS NULL OR group_id='00000000-0000-0000-0000-000000000000') AND is_teams_channel_connected=false AND is_teams_connected=false`.
# MAGIC
# MAGIC Channel sites (is_teams_channel_connected=true) do NOT get shared_data_sets rows — they appear in dim_sites with shared_data_set_id pointing to their parent team.
# MAGIC
# MAGIC Owner counts: type=site rows ship with owner_count_total=0 / owner_count_in_scope=0 pending #555 (silver.spo_site_admins not yet available).

# COMMAND ----------


@dlt.table(
    name="shared_data_sets",
    comment=(
        "Atomic schedulable units for migration. Grain: one row per team, M365 group, or standalone site. "
        "PK: shared_data_set_id = md5(source_key | type | anchor_id). "
        "Types: 'team' (Teams-enabled M365 Group + primary site + group mailbox + 0..N channel sites), "
        "'m365_group' (Unified M365 Group without Teams + primary site + group mailbox), "
        "'site' (standalone SharePoint site with no backing group and no Teams channel relationship). "
        "Channel sites (private/shared) ride with the parent team — no separate rows. "
        "OneDrive personal sites do not appear here (belong to dim_people #485). "
        "FK primary_site_id → dim_sites.site_silver_id. FK hub_shared_data_set_id → self (hub parent). "
        "owner_count_total / owner_count_in_scope: for type=site, both are 0 pending #555 (spo_site_admins not available). "
        "primary_organization_id/name: highest-confidence org by owner vote count among in-scope owners; NULL when no in-scope owners. "
        "#497 T2."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def shared_data_sets():
    exo_groups = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.exo_unified_groups")
    teams = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.teams")
    teams_details = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.teams_team_details")
    spo_sites = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_sites")
    group_owners = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.group_owners")
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")
    dim_people = dlt.read("dim_people")
    dim_org = dlt.read("dim_organization")
    # dim_people is used below for in-scope owner checks and org voting.

    # ------------------------------------------------------------------
    # Type: team
    # Anchor on silver.teams (which is already filtered to Teams-enabled groups via Graph /teams endpoint).
    # Join to teams_team_details (rich payload) and exo_unified_groups (member_count, mailbox FK) on entra_object_id.
    # ------------------------------------------------------------------
    teams_base = teams.select(
        col("team_id").alias("team_silver_id"),
        col("entra_object_id").alias("team_oid"),
        col("display_name").alias("team_display_name"),
        col("source_key"),
        col("environment"),
        col("source_created_at"),
        col("last_updated_at"),
    ).alias("t")

    teams_details_join = teams_details.select(
        col("entra_object_id").alias("td_oid"),
        col("source_key").alias("td_sk"),
        col("environment").alias("td_env"),
        col("display_name").alias("td_display_name"),
        col("visibility"),
        col("is_archived"),
        col("members_count"),
        col("source_created_at").alias("td_created_at"),
    ).alias("td")

    exo_groups_join = exo_groups.select(
        col("entra_object_id").alias("eg_oid"),
        col("source_key").alias("eg_sk"),
        col("environment").alias("eg_env"),
        col("unified_group_id"),
        col("member_count").alias("eg_member_count"),
    ).alias("eg")

    teams_rows = (
        teams_base.join(
            teams_details_join,
            (col("t.team_oid") == col("td.td_oid"))
            & (col("t.source_key") == col("td.td_sk"))
            & (col("t.environment") == col("td.td_env")),
            "left",
        )
        .join(
            exo_groups_join,
            (col("t.team_oid") == col("eg.eg_oid"))
            & (col("t.source_key") == col("eg.eg_sk"))
            & (col("t.environment") == col("eg.eg_env")),
            "left",
        )
        .select(
            lit("team").alias("shared_data_set_type"),
            md5(concat_ws("|", lower(col("t.source_key")), lit("team"), lower(col("t.team_oid")))).alias(
                "shared_data_set_id"
            ),
            coalesce(col("td.td_display_name"), col("t.team_display_name")).alias("display_name"),
            col("t.source_key"),
            col("t.environment"),
            col("t.team_oid").alias("entra_group_id"),
            col("t.team_silver_id"),
            col("eg.unified_group_id").alias("group_mailbox_id"),
            col("td.is_archived"),
            col("td.visibility"),
            coalesce(col("td.members_count"), col("eg.eg_member_count")).cast("int").alias("member_count"),
            coalesce(col("td.td_created_at"), col("t.source_created_at")).alias("source_created_at"),
            col("t.last_updated_at"),
        )
    )

    # ------------------------------------------------------------------
    # Type: m365_group
    # From exo_unified_groups where is_team_enabled=false.
    # ------------------------------------------------------------------
    m365_rows = exo_groups.filter(col("is_team_enabled") == lit(False)).select(
        lit("m365_group").alias("shared_data_set_type"),
        md5(concat_ws("|", lower(col("source_key")), lit("m365_group"), lower(col("entra_object_id")))).alias(
            "shared_data_set_id"
        ),
        col("display_name"),
        col("source_key"),
        col("environment"),
        col("entra_object_id").alias("entra_group_id"),
        lit(None).cast("string").alias("team_silver_id"),
        col("unified_group_id").alias("group_mailbox_id"),
        lit(None).cast("boolean").alias("is_archived"),
        col("access_type").alias("visibility"),
        col("member_count"),
        col("source_created_at"),
        col("last_updated_at"),
    )

    # ------------------------------------------------------------------
    # Type: site (standalone sites)
    # Filter: is_personal_site=false AND (group_id IS NULL OR group_id='00000000-0000-0000-0000-000000000000')
    # AND is_teams_channel_connected=false AND is_teams_connected=false
    # ------------------------------------------------------------------
    site_rows = spo_sites.filter(
        (col("is_personal_site") == lit(False))
        & (col("group_id").isNull() | (lower(trim(col("group_id"))) == lit("00000000-0000-0000-0000-000000000000")))
        & (col("is_teams_channel_connected") == lit(False))
        & (col("is_teams_connected") == lit(False))
    ).select(
        lit("site").alias("shared_data_set_type"),
        md5(concat_ws("|", lower(col("source_key")), lit("site"), lower(col("sharepoint_id")))).alias(
            "shared_data_set_id"
        ),
        col("display_name"),
        col("source_key"),
        col("environment"),
        lit(None).cast("string").alias("entra_group_id"),
        lit(None).cast("string").alias("team_silver_id"),
        lit(None).cast("string").alias("group_mailbox_id"),
        lit(None).cast("boolean").alias("is_archived"),
        lit(None).cast("string").alias("visibility"),
        lit(None).cast("int").alias("member_count"),
        col("source_modified_at").alias("source_created_at"),
        col("last_updated_at"),
    )

    # ------------------------------------------------------------------
    # Union all three types
    # ------------------------------------------------------------------
    all_rows = teams_rows.unionByName(m365_rows).unionByName(site_rows).alias("ar")

    # ------------------------------------------------------------------
    # Join to spo_sites to resolve primary_site_id
    # For team/m365_group: join on group_id = entra_group_id + source_key + environment
    # For site: the row itself is the primary site (sharepoint_id → site_silver_id)
    # ------------------------------------------------------------------
    # Build a lookup: entra_group_id + source_key + environment → site_silver_id for primary group sites
    primary_group_sites = (
        spo_sites.filter(
            (col("group_id").isNotNull())
            & (lower(trim(col("group_id"))) != lit("00000000-0000-0000-0000-000000000000"))
        )
        .select(
            lower(trim(col("group_id"))).alias("pgs_group_id"),
            col("source_key").alias("pgs_source_key"),
            col("environment").alias("pgs_environment"),
            col("site_silver_id").alias("pgs_site_id"),
            col("is_hub_site").alias("pgs_is_hub"),
            lower(trim(col("hub_site_id"))).alias("pgs_hub_site_id"),
            col("storage_used_mb").alias("pgs_storage_mb"),
            col("lock_state").alias("pgs_lock_state"),
        )
        .dropDuplicates(["pgs_group_id", "pgs_source_key", "pgs_environment"])
        .alias("pgs")
    )

    # For type=site, the primary site is itself (lookup via shared_data_set_id + environment → site_silver_id)
    standalone_site_lookup = (
        spo_sites.filter(
            (col("is_personal_site") == lit(False))
            & (col("group_id").isNull() | (lower(trim(col("group_id"))) == lit("00000000-0000-0000-0000-000000000000")))
            & (col("is_teams_channel_connected") == lit(False))
            & (col("is_teams_connected") == lit(False))
        )
        .select(
            md5(concat_ws("|", lower(col("source_key")), lit("site"), lower(col("sharepoint_id")))).alias(
                "ss_shared_data_set_id"
            ),
            col("environment").alias("ss_environment"),
            col("site_silver_id").alias("ss_site_id"),
            col("is_hub_site").alias("ss_is_hub"),
            lower(trim(col("hub_site_id"))).alias("ss_hub_site_id"),
            col("storage_used_mb").alias("ss_storage_mb"),
            col("lock_state").alias("ss_lock_state"),
        )
        .dropDuplicates(["ss_shared_data_set_id", "ss_environment"])
        .alias("ss")
    )

    with_primary_site = (
        all_rows.join(
            primary_group_sites,
            (lower(trim(col("ar.entra_group_id"))) == col("pgs.pgs_group_id"))
            & (col("ar.source_key") == col("pgs.pgs_source_key"))
            & (col("ar.environment") == col("pgs.pgs_environment")),
            "left",
        )
        .join(
            standalone_site_lookup,
            (col("ar.shared_data_set_type") == lit("site"))
            & (col("ar.shared_data_set_id") == col("ss.ss_shared_data_set_id"))
            & (col("ar.environment") == col("ss.ss_environment")),
            "left",
        )
        .withColumn(
            "primary_site_id",
            when(col("ar.shared_data_set_type").isin("team", "m365_group"), col("pgs.pgs_site_id")).otherwise(
                col("ss.ss_site_id")
            ),
        )
        .withColumn(
            "_primary_is_hub",
            when(col("ar.shared_data_set_type").isin("team", "m365_group"), col("pgs.pgs_is_hub")).otherwise(
                col("ss.ss_is_hub")
            ),
        )
        .withColumn(
            "_primary_hub_site_id",
            when(col("ar.shared_data_set_type").isin("team", "m365_group"), col("pgs.pgs_hub_site_id")).otherwise(
                col("ss.ss_hub_site_id")
            ),
        )
        .withColumn(
            "_primary_storage_mb",
            when(col("ar.shared_data_set_type").isin("team", "m365_group"), col("pgs.pgs_storage_mb")).otherwise(
                col("ss.ss_storage_mb")
            ),
        )
        .withColumn(
            "_primary_lock_state",
            when(col("ar.shared_data_set_type").isin("team", "m365_group"), col("pgs.pgs_lock_state")).otherwise(
                col("ss.ss_lock_state")
            ),
        )
    )

    # ------------------------------------------------------------------
    # Hub linkage: resolve hub_shared_data_set_id
    # If _primary_hub_site_id is not null, resolve it to the hub site's sharepoint_id,
    # then derive hub_shared_data_set_id = md5(source_key | hub_type | hub_sharepoint_id).
    # Hub type determination: if the hub site has group_id, it's a team or m365_group; else site.
    # ------------------------------------------------------------------
    hub_site_meta = (
        spo_sites.filter((col("is_hub_site") == lit(True)) & (col("is_personal_site") == lit(False)))
        .select(
            lower(trim(col("sharepoint_id"))).alias("h_sp_id"),
            col("source_key").alias("h_source_key"),
            col("environment").alias("h_environment"),
            lower(trim(col("group_id"))).alias("h_group_id"),
            col("display_name").alias("h_display_name"),
        )
        .dropDuplicates(["h_sp_id", "h_source_key", "h_environment"])
        .alias("hsm")
    )

    # Join exo_unified_groups to determine if hub's group is team-enabled
    hub_group_type = (
        exo_groups.select(
            lower(trim(col("entra_object_id"))).alias("hg_oid"),
            col("source_key").alias("hg_source_key"),
            col("environment").alias("hg_environment"),
            col("is_team_enabled").alias("hg_is_team"),
        )
        .dropDuplicates(["hg_oid", "hg_source_key", "hg_environment"])
        .alias("hg")
    )

    with_hub_meta = (
        with_primary_site.join(
            hub_site_meta,
            (col("_primary_hub_site_id") == col("hsm.h_sp_id"))
            & (col("ar.source_key") == col("hsm.h_source_key"))
            & (col("ar.environment") == col("hsm.h_environment")),
            "left",
        )
        .join(
            hub_group_type,
            (col("hsm.h_group_id") == col("hg.hg_oid"))
            & (col("hsm.h_source_key") == col("hg.hg_source_key"))
            & (col("hsm.h_environment") == col("hg.hg_environment")),
            "left",
        )
        .withColumn(
            "_hub_type",
            when(
                col("hsm.h_sp_id").isNull(),
                lit(None).cast("string"),
            )
            .when(
                col("hsm.h_group_id").isNull() | (col("hsm.h_group_id") == lit("00000000-0000-0000-0000-000000000000")),
                lit("site"),
            )
            .when(col("hg.hg_is_team") == lit(True), lit("team"))
            .otherwise(lit("m365_group")),
        )
        .withColumn(
            "_hub_anchor",
            when(
                col("_hub_type").isNull(),
                lit(None).cast("string"),
            )
            .when(col("_hub_type") == lit("site"), col("hsm.h_sp_id"))
            .otherwise(col("hsm.h_group_id")),
        )
        .withColumn(
            "hub_shared_data_set_id",
            when(
                col("_primary_is_hub") == lit(True),
                lit(None).cast("string"),  # The hub itself has NULL (per spec)
            )
            .when(
                col("_hub_type").isNotNull(),
                md5(concat_ws("|", lower(col("ar.source_key")), col("_hub_type"), col("_hub_anchor"))),
            )
            .otherwise(lit(None).cast("string")),
        )
        .withColumn("hub_display_name", col("hsm.h_display_name"))
    )

    # ------------------------------------------------------------------
    # hub_family_size: count of rows sharing the same hub (including the hub itself)
    # When hub_shared_data_set_id IS NULL and is_hub IS NOT TRUE, set to 1.
    # ------------------------------------------------------------------
    # First compute a window over hub_shared_data_set_id (including NULLs grouped together)
    # Then compute is_hub and adjust.
    with_hub_family = with_hub_meta.withColumn("is_hub", coalesce(col("_primary_is_hub"), lit(False))).withColumn(
        "_hub_family_key",
        when(col("is_hub") == lit(True), col("ar.shared_data_set_id")).otherwise(
            coalesce(col("hub_shared_data_set_id"), lit("__no_hub__"))
        ),
    )

    hub_family_counts = (
        with_hub_family.groupBy("_hub_family_key")
        .agg(count(lit(1)).alias("_fam_count"))
        .withColumnRenamed("_hub_family_key", "_hub_family_key_rhs")
        .alias("hfc")
    )

    with_hub_family_size = (
        with_hub_family.join(
            hub_family_counts,
            col("_hub_family_key") == col("hfc._hub_family_key_rhs"),
            "left",
        )
        .withColumn(
            "hub_family_size",
            when(
                (col("hub_shared_data_set_id").isNull()) & (col("is_hub") == lit(False)),
                lit(1),
            ).otherwise(col("hfc._fam_count")),
        )
        .drop("_hub_family_key", "_hub_family_key_rhs", "_fam_count")
    )

    # ------------------------------------------------------------------
    # is_read_only: derived from _primary_lock_state ∈ {ReadOnly, NoAccess} (case-insensitive)
    # ------------------------------------------------------------------
    with_read_only = with_hub_family_size.withColumn(
        "is_read_only",
        when(col("_primary_lock_state").isNull(), lit(None).cast("boolean")).otherwise(
            lower(trim(col("_primary_lock_state"))).isin("readonly", "noaccess")
        ),
    )

    # ------------------------------------------------------------------
    # Channel site count (for type=team only)
    # Count of dim_sites rows with shared_data_set_id == this row's id AND site_type ∈ {channel_private, channel_shared}
    # BUT: dim_sites is a circular FK, so we derive this from silver.spo_sites directly.
    # Channel sites: is_teams_channel_connected=true AND related_group_id IS NOT NULL AND related_group_id != '00000000-0000-0000-0000-000000000000'
    # ------------------------------------------------------------------
    channel_site_counts = (
        spo_sites.filter(
            (col("is_teams_channel_connected") == lit(True))
            & (col("related_group_id").isNotNull())
            & (lower(trim(col("related_group_id"))) != lit("00000000-0000-0000-0000-000000000000"))
        )
        .select(
            lower(trim(col("related_group_id"))).alias("csc_related_gid"),
            col("source_key").alias("csc_source_key"),
            col("environment").alias("csc_environment"),
            col("storage_used_mb"),
        )
        .groupBy("csc_related_gid", "csc_source_key", "csc_environment")
        .agg(
            count(lit(1)).alias("channel_site_count"),
            spark_sum(col("storage_used_mb")).alias("channel_storage_mb"),
        )
        .alias("csc")
    )

    with_channel_sites = (
        with_read_only.join(
            channel_site_counts,
            (lower(trim(col("ar.entra_group_id"))) == col("csc.csc_related_gid"))
            & (col("ar.source_key") == col("csc.csc_source_key"))
            & (col("ar.environment") == col("csc.csc_environment")),
            "left",
        )
        .withColumn(
            "channel_site_count",
            when(col("ar.shared_data_set_type") == lit("team"), coalesce(col("csc.channel_site_count"), lit(0)))
            .otherwise(lit(None).cast("int"))
            .cast("int"),
        )
        .withColumn(
            "total_storage_mb",
            (
                coalesce(col("_primary_storage_mb"), lit(0))
                + when(
                    col("ar.shared_data_set_type") == lit("team"), coalesce(col("csc.channel_storage_mb"), lit(0))
                ).otherwise(lit(0))
            ).cast("decimal(18,2)"),
        )
    )

    # ------------------------------------------------------------------
    # Owner counts and primary organization derivation
    # For type=team and type=m365_group: silver.group_owners joined on group_silver_id
    # For type=site: 0 pending #555
    # In-scope check: source owners via dim_people.source_user_id, target owners via dim_people.target_user_id
    # ------------------------------------------------------------------
    # group_owners.group_silver_id format: concat_ws('_', source_key, entra_group_id)
    owners_base = group_owners.select(
        col("group_silver_id").alias("go_group_sid"),
        col("owner_entra_object_id").alias("go_owner_oid"),
        col("environment").alias("go_environment"),
    ).alias("go")

    # Compose the expected group_silver_id for each row
    with_owners_prep = with_channel_sites.withColumn(
        "_group_silver_id",
        when(
            col("ar.shared_data_set_type").isin("team", "m365_group"),
            concat_ws("_", col("ar.source_key"), col("ar.entra_group_id")),
        ).otherwise(lit(None).cast("string")),
    )

    # Join owners
    with_owners = with_owners_prep.join(
        owners_base,
        col("_group_silver_id") == col("go.go_group_sid"),
        "left",
    )

    # Compose user_id for each owner: concat_ws('_', source_key, owner_entra_object_id)
    with_owner_user_id = with_owners.withColumn(
        "_owner_user_id",
        when(
            col("go.go_owner_oid").isNotNull(), concat_ws("_", col("ar.source_key"), col("go.go_owner_oid"))
        ).otherwise(lit(None).cast("string")),
    )

    # Join to users to check if the owner exists in users table (any environment, matched on source_key + env)
    users_for_owners = (
        users.select(
            col("user_id").alias("u_user_id"),
            col("environment").alias("u_environment"),
        )
        .distinct()
        .alias("ufo")
    )

    with_owner_exists = with_owner_user_id.join(
        users_for_owners,
        (col("_owner_user_id") == col("ufo.u_user_id")) & (col("go.go_environment") == col("ufo.u_environment")),
        "left",
    ).withColumn("_owner_exists", col("ufo.u_user_id").isNotNull())

    # In-scope check: split by environment
    # Source owners: join to dim_people.source_user_id
    # Target owners: join to dim_people.target_user_id (non-null)
    dim_people_source = dim_people.select(
        col("source_user_id").alias("dps_user_id"),
        col("organization_id").alias("dps_org_id"),
    ).alias("dps")

    dim_people_target = (
        dim_people.filter(col("target_user_id").isNotNull())
        .select(
            col("target_user_id").alias("dpt_user_id"),
            col("organization_id").alias("dpt_org_id"),
        )
        .alias("dpt")
    )

    with_owner_in_scope_source = with_owner_exists.join(
        dim_people_source,
        (col("_owner_user_id") == col("dps.dps_user_id")) & (col("go.go_environment") == lit("source")),
        "left",
    )

    with_owner_in_scope_target = with_owner_in_scope_source.join(
        dim_people_target,
        (col("_owner_user_id") == col("dpt.dpt_user_id")) & (col("go.go_environment") == lit("target")),
        "left",
    )

    with_owner_org = with_owner_in_scope_target.withColumn(
        "_owner_in_scope",
        (col("dps.dps_user_id").isNotNull()) | (col("dpt.dpt_user_id").isNotNull()),
    ).withColumn(
        "_owner_org_id",
        coalesce(col("dps.dps_org_id"), col("dpt.dpt_org_id")),
    )

    # Aggregate per shared_data_set_id: owner counts and org vote counts
    owner_agg = (
        with_owner_org.groupBy(col("ar.shared_data_set_id"))
        .agg(
            count(col("_owner_user_id")).alias("owner_count_total"),
            count(when(col("_owner_in_scope"), lit(1))).alias("owner_count_in_scope"),
            # Collect org votes: array of org_id per in-scope owner
            collect_list(when(col("_owner_org_id").isNotNull(), col("_owner_org_id"))).alias("_org_votes"),
        )
        .alias("ownagg")
    )

    # Join back aggregated owner counts
    with_owner_counts = (
        with_owners_prep.join(
            owner_agg,
            col("ar.shared_data_set_id") == col("ownagg.shared_data_set_id"),
            "left",
        )
        .withColumn(
            "owner_count_total",
            coalesce(col("ownagg.owner_count_total"), lit(0)).cast("int"),
        )
        .withColumn("_org_votes", coalesce(col("ownagg._org_votes"), array().cast("array<string>")))
        .withColumn("_org_votes", expr("filter(_org_votes, x -> x is not null)"))
    )

    # Derive primary_organization_id: most common org_id in _org_votes (ties broken by org_id sort)
    # Explode org votes, count per org, rank by count desc / org_id asc, take rank 1
    org_votes_exploded = with_owner_counts.select(
        col("ar.shared_data_set_id").alias("ove_sds_id"),
        explode(col("_org_votes")).alias("ove_org_id"),
    )

    org_vote_counts = (
        org_votes_exploded.groupBy("ove_sds_id", "ove_org_id").agg(count(lit(1)).alias("vote_count")).alias("ovc")
    )

    org_vote_window = Window.partitionBy("ovc.ove_sds_id").orderBy(
        col("ovc.vote_count").desc(), col("ovc.ove_org_id").asc()
    )

    primary_orgs = (
        org_vote_counts.withColumn("_vote_rank", row_number().over(org_vote_window))
        .filter(col("_vote_rank") == 1)
        .select(col("ove_sds_id").alias("po_sds_id"), col("ove_org_id").alias("po_org_id"))
        .alias("po")
    )

    with_primary_org = (
        with_owner_counts.join(
            primary_orgs,
            col("ar.shared_data_set_id") == col("po.po_sds_id"),
            "left",
        )
        .withColumn("primary_organization_id", col("po.po_org_id"))
        .join(
            dim_org.select(col("org_id").alias("dorg_id"), col("org_name").alias("dorg_name")).alias("dorg"),
            col("po.po_org_id") == col("dorg.dorg_id"),
            "left",
        )
        .withColumn("primary_organization_name", col("dorg.dorg_name"))
    )

    # ------------------------------------------------------------------
    # Final select: exact column order per spec
    # ------------------------------------------------------------------
    return with_primary_org.select(
        col("ar.shared_data_set_id"),
        col("ar.shared_data_set_type"),
        col("ar.display_name"),
        col("ar.source_key"),
        col("ar.environment"),
        col("ar.entra_group_id"),
        col("ar.team_silver_id"),
        col("primary_site_id"),
        col("ar.group_mailbox_id"),
        col("is_hub"),
        col("hub_shared_data_set_id"),
        col("hub_display_name"),
        col("hub_family_size").cast("int"),
        col("ar.is_archived"),
        col("is_read_only"),
        col("ar.visibility"),
        col("ar.member_count"),
        col("channel_site_count"),
        col("total_storage_mb"),
        col("owner_count_total"),
        col("owner_count_in_scope"),
        col("primary_organization_id"),
        col("primary_organization_name"),
        col("ar.source_created_at"),
        col("ar.last_updated_at"),
        current_timestamp().alias("gold_loaded_at"),
    )


# COMMAND ----------


@dlt.table(
    name="shared_data_set_organizations",
    comment=(
        "Multi-valued scored organization association per shared_data_set. "
        "Grain: one row per (shared_data_set_id, environment, organization_id) for every in-scope organization "
        "that contributes ≥1 owner. "
        "PK: shared_data_set_organization_id = md5(shared_data_set_id | environment | organization_id). "
        "FK shared_data_set_id → shared_data_sets. FK organization_id → dim_organization.org_id. "
        "Algorithm: (1) collect owners per shared_data_set from silver.group_owners (team / m365_group only; "
        "site has zero owners pending #555 → zero org rows). (2) Resolve each owner's organization_id via dim_people: "
        "source owners match dim_people.source_user_id, target owners match dim_people.target_user_id. "
        "(3) Drop owners not in dim_people (out of scope). (4) Group by (shared_data_set_id, environment, organization_id) "
        "→ owners_in_org. (5) confidence_score = owners_in_org / owner_count_in_scope, decimal(5,4), "
        "sums to ≤1.0000 per shared_data_set when ≥1 in-scope owner (any remainder indicates in-scope owners with null organization_id). (6) is_primary_organization = true for the "
        "highest-confidence org per (shared_data_set_id, environment), ties broken by organization_id ascending — "
        "matches the primary_organization_id logic in shared_data_sets. "
        "Shared_data_sets with no in-scope owners produce zero rows here. "
        "#497 T2c."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def shared_data_set_organizations():
    group_owners = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.group_owners")
    shared_data_sets_t = dlt.read("shared_data_sets")
    dim_people = dlt.read("dim_people")
    dim_org = dlt.read("dim_organization")

    # ------------------------------------------------------------------
    # Owner-bearing shared_data_sets: team and m365_group only.
    # site rows have zero owners pending #555 → naturally produce zero org rows.
    # Reuse owner_count_in_scope from shared_data_sets (single source of truth).
    # ------------------------------------------------------------------
    sds = (
        shared_data_sets_t.filter(col("shared_data_set_type").isin("team", "m365_group"))
        .select(
            col("shared_data_set_id").alias("sds_id"),
            col("source_key").alias("sds_source_key"),
            col("entra_group_id").alias("sds_entra_group_id"),
            col("owner_count_in_scope").alias("sds_owner_count_in_scope"),
        )
        .alias("sds")
    )

    # group_owners.group_silver_id format: concat_ws('_', source_key, entra_group_id)
    # owner_user_id format: concat_ws('_', source_key, owner_entra_object_id)
    owners = group_owners.select(
        col("group_silver_id").alias("go_group_sid"),
        col("source_key").alias("go_source_key"),
        col("owner_entra_object_id").alias("go_owner_oid"),
        col("environment").alias("go_environment"),
    ).alias("go")

    # Join owners to shared_data_sets via composed group_silver_id
    sds_with_owners = sds.join(
        owners,
        col("go.go_group_sid") == concat_ws("_", col("sds.sds_source_key"), col("sds.sds_entra_group_id")),
        "inner",
    ).withColumn(
        "_owner_user_id",
        concat_ws("_", col("sds.sds_source_key"), col("go.go_owner_oid")),
    )

    # ------------------------------------------------------------------
    # Resolve organization_id via dim_people.
    # Source owners (go_environment='source') → match dim_people.source_user_id.
    # Target owners (go_environment='target') → match dim_people.target_user_id (non-null).
    # ------------------------------------------------------------------
    dim_people_source = dim_people.select(
        col("source_user_id").alias("dps_user_id"),
        col("organization_id").alias("dps_org_id"),
    ).alias("dps")

    dim_people_target = (
        dim_people.filter(col("target_user_id").isNotNull())
        .select(
            col("target_user_id").alias("dpt_user_id"),
            col("organization_id").alias("dpt_org_id"),
        )
        .alias("dpt")
    )

    with_source_org = sds_with_owners.join(
        dim_people_source,
        (col("_owner_user_id") == col("dps.dps_user_id")) & (col("go.go_environment") == lit("source")),
        "left",
    )

    with_target_org = with_source_org.join(
        dim_people_target,
        (col("_owner_user_id") == col("dpt.dpt_user_id")) & (col("go.go_environment") == lit("target")),
        "left",
    )

    with_org = with_target_org.withColumn(
        "_organization_id",
        coalesce(col("dps.dps_org_id"), col("dpt.dpt_org_id")),
    )

    # ------------------------------------------------------------------
    # Filter to in-scope owners (those resolved to an organization_id via dim_people).
    # Group by (shared_data_set_id, environment, source_key, organization_id) → owners_in_org.
    # ------------------------------------------------------------------
    in_scope = with_org.filter(col("_organization_id").isNotNull())

    agg = in_scope.groupBy(
        col("sds.sds_id").alias("agg_sds_id"),
        col("go.go_environment").alias("agg_environment"),
        col("sds.sds_source_key").alias("agg_source_key"),
        col("_organization_id").alias("agg_organization_id"),
        col("sds.sds_owner_count_in_scope").alias("agg_owner_count_in_scope"),
    ).agg(count(lit(1)).cast("int").alias("agg_owners_in_org"))

    # ------------------------------------------------------------------
    # confidence_score and is_primary_organization
    # ------------------------------------------------------------------
    with_score = agg.withColumn(
        "confidence_score",
        spark_round(
            col("agg_owners_in_org").cast("decimal(20,4)") / col("agg_owner_count_in_scope").cast("decimal(20,4)"),
            4,
        ).cast("decimal(5,4)"),
    )

    primary_window = Window.partitionBy(col("agg_sds_id"), col("agg_environment")).orderBy(
        col("agg_owners_in_org").desc(), col("agg_organization_id").asc()
    )

    with_primary = with_score.withColumn("_rank", row_number().over(primary_window)).withColumn(
        "is_primary_organization", col("_rank") == lit(1)
    )

    # ------------------------------------------------------------------
    # Join dim_organization for organization_name.
    # ------------------------------------------------------------------
    org_lookup = dim_org.select(
        col("org_id").alias("dorg_id"),
        col("org_name").alias("dorg_name"),
    ).alias("dorg")

    with_org_name = with_primary.join(
        org_lookup,
        col("agg_organization_id") == col("dorg.dorg_id"),
        "left",
    )

    # ------------------------------------------------------------------
    # Final select — exact column order, gold_loaded_at LAST.
    # ------------------------------------------------------------------
    return with_org_name.select(
        md5(
            concat_ws(
                "|",
                col("agg_sds_id"),
                col("agg_environment"),
                col("agg_organization_id"),
            )
        ).alias("shared_data_set_organization_id"),
        col("agg_sds_id").alias("shared_data_set_id"),
        col("agg_environment").alias("environment"),
        col("agg_source_key").alias("source_key"),
        col("agg_organization_id").alias("organization_id"),
        col("dorg.dorg_name").alias("organization_name"),
        col("agg_owners_in_org").alias("owners_in_org"),
        col("agg_owner_count_in_scope").cast("int").alias("owner_count_in_scope"),
        col("confidence_score"),
        col("is_primary_organization"),
        current_timestamp().alias("gold_loaded_at"),
    )


# COMMAND ----------


@dlt.table(
    name="dim_sites",
    comment=(
        "Site-grain dimension. Grain: one row per non-personal SharePoint/OneDrive site. "
        "PK: site_silver_id. "
        "FK shared_data_set_id → parent shared_data_sets row (team/m365_group/site). "
        "site_type: 'primary' (primary group site), 'channel_private' / 'channel_shared' (channel sites), "
        "'standalone' (no group, no channel), 'hub' (hub site — takes precedence). "
        "Channel sites: is_teams_channel_connected=true AND related_group_id IS NOT NULL. "
        "Standalone sites: group_id IS NULL/00000000 AND not channel-connected. "
        "Personal sites (is_personal_site=true) are excluded — those belong to dim_people #485. "
        "FK hub_shared_data_set_id → hub's shared_data_sets row (NULL when not hub-associated; NULL when this site IS the hub primary). "
        "parent_team_silver_id: for channel sites only, the parent team's team_silver_id (concat_ws('_', source_key, related_group_id)). "
        "#497 T2."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def dim_sites():
    spo_sites = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_sites")
    exo_groups = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.exo_unified_groups")

    # ------------------------------------------------------------------
    # Base: all non-personal sites
    # ------------------------------------------------------------------
    sites_base = (
        spo_sites.filter(col("is_personal_site") == lit(False))
        .select(
            col("site_silver_id"),
            col("source_key"),
            col("environment"),
            col("display_name"),
            col("web_url"),
            col("template"),
            col("lock_state"),
            col("sensitivity_label"),
            col("storage_used_mb"),
            col("storage_quota_mb"),
            col("is_personal_site"),
            col("is_hub_site"),
            lower(trim(col("hub_site_id"))).alias("hub_site_id"),
            lower(trim(col("sharepoint_id"))).alias("sharepoint_id"),
            lower(trim(col("group_id"))).alias("group_id"),
            lower(trim(col("related_group_id"))).alias("related_group_id"),
            col("teams_channel_type"),
            col("is_teams_connected"),
            col("is_teams_channel_connected"),
            col("source_modified_at"),
            col("last_updated_at"),
        )
        .alias("sb")
    )

    # ------------------------------------------------------------------
    # Determine site_type
    # Priority: hub > channel_private/channel_shared > primary > standalone
    # ------------------------------------------------------------------
    with_site_type = sites_base.withColumn(
        "site_type",
        when(
            col("sb.is_hub_site") == lit(True),
            lit("hub"),
        )
        .when(
            (col("sb.is_teams_channel_connected") == lit(True))
            & (col("sb.related_group_id").isNotNull())
            & (col("sb.related_group_id") != lit("00000000-0000-0000-0000-000000000000")),
            when(col("sb.teams_channel_type") == 1, lit("channel_private"))
            .when(col("sb.teams_channel_type") == 2, lit("channel_shared"))
            .otherwise(lit(None).cast("string")),
        )
        .when(
            (col("sb.group_id").isNotNull()) & (col("sb.group_id") != lit("00000000-0000-0000-0000-000000000000")),
            lit("primary"),
        )
        .otherwise(lit("standalone")),
    )

    # ------------------------------------------------------------------
    # Derive shared_data_set_id via deterministic key
    # For primary: team or m365_group based on group_id → exo_unified_groups.is_team_enabled
    # For channel: parent team's shared_data_set_id = md5(source_key | 'team' | related_group_id)
    # For standalone: md5(source_key | 'site' | sharepoint_id)
    # For hub: same logic as primary/standalone depending on whether it has a group
    # ------------------------------------------------------------------
    # Join exo_unified_groups to determine if group_id is team-enabled
    group_type_lookup = (
        exo_groups.select(
            lower(trim(col("entra_object_id"))).alias("gtl_oid"),
            col("source_key").alias("gtl_source_key"),
            col("environment").alias("gtl_environment"),
            col("is_team_enabled").alias("gtl_is_team"),
        )
        .dropDuplicates(["gtl_oid", "gtl_source_key", "gtl_environment"])
        .alias("gtl")
    )

    with_group_type = with_site_type.join(
        group_type_lookup,
        (col("sb.group_id") == col("gtl.gtl_oid"))
        & (col("sb.source_key") == col("gtl.gtl_source_key"))
        & (col("sb.environment") == col("gtl.gtl_environment")),
        "left",
    )

    with_shared_data_set_id = with_group_type.withColumn(
        "shared_data_set_id",
        when(
            col("site_type").isin("channel_private", "channel_shared"),
            # Parent team's shared_data_set_id
            md5(concat_ws("|", lower(col("sb.source_key")), lit("team"), col("sb.related_group_id"))),
        )
        .when(
            col("site_type").isin("primary", "hub")
            & (col("sb.group_id").isNotNull())
            & (col("sb.group_id") != lit("00000000-0000-0000-0000-000000000000")),
            # Group-backed: team or m365_group
            when(
                col("gtl.gtl_is_team") == lit(True),
                md5(concat_ws("|", lower(col("sb.source_key")), lit("team"), col("sb.group_id"))),
            ).otherwise(md5(concat_ws("|", lower(col("sb.source_key")), lit("m365_group"), col("sb.group_id")))),
        )
        .otherwise(
            # Standalone or hub without group
            md5(concat_ws("|", lower(col("sb.source_key")), lit("site"), col("sb.sharepoint_id"))),
        ),
    )

    # ------------------------------------------------------------------
    # Derive hub_shared_data_set_id
    # If hub_site_id is not null, resolve the hub site's sharepoint_id → shared_data_set_id.
    # If this site IS the hub itself, hub_shared_data_set_id = NULL (per spec).
    # ------------------------------------------------------------------
    hub_site_meta = (
        spo_sites.filter((col("is_hub_site") == lit(True)) & (col("is_personal_site") == lit(False)))
        .select(
            lower(trim(col("sharepoint_id"))).alias("hsm_sp_id"),
            col("source_key").alias("hsm_source_key"),
            col("environment").alias("hsm_environment"),
            lower(trim(col("group_id"))).alias("hsm_group_id"),
        )
        .dropDuplicates(["hsm_sp_id", "hsm_source_key", "hsm_environment"])
        .alias("hsm")
    )

    hub_group_type = (
        exo_groups.select(
            lower(trim(col("entra_object_id"))).alias("hgt_oid"),
            col("source_key").alias("hgt_source_key"),
            col("environment").alias("hgt_environment"),
            col("is_team_enabled").alias("hgt_is_team"),
        )
        .dropDuplicates(["hgt_oid", "hgt_source_key", "hgt_environment"])
        .alias("hgt")
    )

    with_hub_id = (
        with_shared_data_set_id.join(
            hub_site_meta,
            (col("sb.hub_site_id") == col("hsm.hsm_sp_id"))
            & (col("sb.source_key") == col("hsm.hsm_source_key"))
            & (col("sb.environment") == col("hsm.hsm_environment")),
            "left",
        )
        .join(
            hub_group_type,
            (col("hsm.hsm_group_id") == col("hgt.hgt_oid"))
            & (col("hsm.hsm_source_key") == col("hgt.hgt_source_key"))
            & (col("hsm.hsm_environment") == col("hgt.hgt_environment")),
            "left",
        )
        .withColumn(
            "_hub_type",
            when(
                col("hsm.hsm_sp_id").isNull(),
                lit(None).cast("string"),
            )
            .when(
                col("hsm.hsm_group_id").isNull()
                | (col("hsm.hsm_group_id") == lit("00000000-0000-0000-0000-000000000000")),
                lit("site"),
            )
            .when(col("hgt.hgt_is_team") == lit(True), lit("team"))
            .otherwise(lit("m365_group")),
        )
        .withColumn(
            "_hub_anchor",
            when(
                col("_hub_type").isNull(),
                lit(None).cast("string"),
            )
            .when(col("_hub_type") == lit("site"), col("hsm.hsm_sp_id"))
            .otherwise(col("hsm.hsm_group_id")),
        )
        .withColumn(
            "hub_shared_data_set_id",
            when(
                col("sb.is_hub_site") == lit(True),
                lit(None).cast("string"),  # The hub itself has NULL
            )
            .when(
                col("_hub_type").isNotNull(),
                md5(concat_ws("|", lower(col("sb.source_key")), col("_hub_type"), col("_hub_anchor"))),
            )
            .otherwise(lit(None).cast("string")),
        )
    )

    # ------------------------------------------------------------------
    # Derive parent_team_silver_id for channel sites
    # concat_ws('_', source_key, related_group_id)
    # ------------------------------------------------------------------
    with_parent_team = with_hub_id.withColumn(
        "parent_team_silver_id",
        when(
            col("site_type").isin("channel_private", "channel_shared"),
            concat_ws("_", col("sb.source_key"), col("sb.related_group_id")),
        ).otherwise(lit(None).cast("string")),
    )

    # ------------------------------------------------------------------
    # Final select: exact column order per spec
    # ------------------------------------------------------------------
    return with_parent_team.select(
        col("sb.site_silver_id"),
        col("shared_data_set_id"),
        col("site_type"),
        col("sb.display_name"),
        col("sb.web_url"),
        col("sb.template"),
        col("sb.lock_state"),
        col("sb.sensitivity_label"),
        col("sb.storage_used_mb").cast("decimal(18,2)"),
        col("sb.storage_quota_mb").cast("decimal(18,2)"),
        col("sb.is_personal_site"),
        col("sb.is_hub_site"),
        col("hub_shared_data_set_id"),
        col("parent_team_silver_id"),
        col("sb.source_key"),
        col("sb.environment"),
        col("sb.source_modified_at"),
        col("sb.last_updated_at"),
        current_timestamp().alias("gold_loaded_at"),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## rationalization_input
# MAGIC
# MAGIC | # | Field | Source | Sprint |
# MAGIC |---|-------|--------|--------|
# MAGIC | 1 | HomeTenantObjectId | users.entra_object_id (source) | 1 |
# MAGIC | 2 | HomeTenantUserPrincipalName | users.user_principal_name (source) | 1 |
# MAGIC | 3 | TargetTenantMtoUserObjectId | mto_user_entity_mapping.entra_object_id | 1 |
# MAGIC | 4 | TargetTenantMtoUserPrincipalName | mto_user_entity_mapping.entra_upn | 1 |
# MAGIC | 5 | TargetAdUserObjectId | ad_users.object_guid via DN match through primary_migration | 2 |
# MAGIC | 6 | TargetAdUserPrincipalName | ad_users.user_principal_name via DN match | 2 |
# MAGIC | 7 | TargetTenantHybridUserObjectId | users.entra_object_id (target) | 1 |
# MAGIC | 8 | TargetTenantHybridUserPrincipalName | users.user_principal_name (target) | 1 |
# MAGIC | 9 | TargetAdContactObjectId | ad_contacts.object_guid via mail match | 2 |
# MAGIC | 10 | TargetAdContactTargetAddress | ad_contacts.target_address via mail match | 2 |
# MAGIC | 11 | TargetTenantContactObjectId | contact_entity_mapping two-hop → entra_object_id | 1 |
# MAGIC | 12 | TargetTenantContactTargetAddress | contact_entity_mapping two-hop → mail | 1 |
# MAGIC | - | upn_domain | split(home_tenant_upn, '@')[1] — used for org domain lookup | 1 |
# MAGIC | - | org_id | md5(org_key) — FK to dim_organization.org_id | 1 |

# COMMAND ----------


@dlt.table(
    name="rationalization_input",
    comment="12-field rationalization input for Owen's automation subsystem. One row per source user. Includes derived rationalization_status and is_rationalization_complete.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def rationalization_input():
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")
    mail_users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mail_users")
    # Sprint 2: ad_users / ad_contacts do not exist until AD data arrives in landing.
    # Guard the reads so the pipeline runs Sprint 1 chains fully; fields 5/6/9/10
    # in the output will be null until the tables are created.
    # Note: spark.catalog.tableExists() is blocked in DLT — use SHOW TABLES via SQL.
    from pyspark.sql.types import StringType, StructField, StructType

    def _table_exists(catalog, schema, table):
        return spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").where(col("tableName") == table).limit(1).count() > 0

    if _table_exists(CATALOG, SILVER_SCHEMA, "ad_users"):
        ad_users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.ad_users")
    else:
        ad_users = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("distinguished_name", StringType(), True),
                    StructField("object_guid", StringType(), True),
                    StructField("user_principal_name", StringType(), True),
                ]
            ),
        )

    if _table_exists(CATALOG, SILVER_SCHEMA, "ad_contacts"):
        ad_contacts = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.ad_contacts")
    else:
        ad_contacts = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("target_address", StringType(), True),
                    StructField("object_guid", StringType(), True),
                ]
            ),
        )

    candidates = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.user_mapping_candidates")
    mto_mapping = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mto_user_entity_mapping")
    contact_mapping = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.contact_entity_mapping")
    pm = dlt.read("user_map")

    source_users = _resolve_user_org_id(users.filter(col("environment") == "source"))
    target_users = users.filter(col("environment") == "target")
    target_mail_users = mail_users.filter(col("environment") == "target")

    # Chain 1 (fields 3, 4): external_linkage approved → mail_user_id → MTO Entra user
    el_approved = candidates.filter(
        (col("match_context") == "external_linkage") & (col("candidate_status") == "approved")
    ).select(col("source_user_id"), col("target_user_id").alias("mail_user_id"))
    chain1 = (
        el_approved.alias("el")
        .join(mto_mapping.alias("mto"), col("el.mail_user_id") == col("mto.mail_user_id"), "left")
        .join(target_mail_users.alias("mut"), col("el.mail_user_id") == col("mut.mail_user_id"), "left")
        .select(
            col("el.source_user_id"),
            col("mto.entra_object_id").alias("target_tenant_mto_user_object_id"),  # 3
            col("mto.entra_upn").alias("target_tenant_mto_user_principal_name"),  # 4
            col("mut.display_name").alias("mto_display_name"),
            col("mut.given_name").alias("mto_given_name"),
            col("mut.surname").alias("mto_surname"),
            col("mut.company").alias("mto_company"),
            col("mut.department").alias("mto_department"),
            col("mut.job_title").alias("mto_job_title"),
        )
    )

    # Chain 2 (fields 7, 8): primary_migration approved → target Entra Member
    chain2 = (
        pm.filter((col("match_context") == "primary_migration") & (col("mapping_status") == "approved"))
        .alias("pm")
        .join(target_users.alias("tu"), col("pm.target_user_id") == col("tu.user_id"), "left")
        .select(
            col("pm.source_user_id"),
            lit(True).alias("has_primary_mapping"),  # status gate
            col("tu.entra_object_id").alias("target_tenant_hybrid_user_object_id"),  # 7
            col("tu.user_principal_name").alias("target_tenant_hybrid_user_principal_name"),  # 8
        )
    )

    # Chain 3 (fields 11, 12): two-hop through contact_entity_mapping
    #   Hop 1: source_user_to_exo_contact  → source_user_id → exo_contact_id
    #   Hop 2: exo_contact_to_entra_contact → exo_contact_id → Entra contact fields
    user_to_exo = contact_mapping.filter(col("mapping_type") == "source_user_to_exo_contact").select(
        col("source_id").alias("source_user_id"), col("target_id").alias("exo_contact_id")
    )
    exo_to_entra = contact_mapping.filter(col("mapping_type") == "exo_contact_to_entra_contact").select(
        col("source_id").alias("exo_contact_id"), col("target_entra_object_id"), col("target_entra_mail")
    )
    chain3 = (
        user_to_exo.alias("ue")
        .join(exo_to_entra.alias("ee"), col("ue.exo_contact_id") == col("ee.exo_contact_id"), "left")
        .select(
            col("ue.source_user_id"),
            col("ee.target_entra_object_id").alias("target_tenant_contact_object_id"),  # 11
            col("ee.target_entra_mail").alias("target_tenant_contact_target_address"),  # 12
        )
    )

    # Chain 4 (fields 5, 6): primary_migration target user → AD user via DN
    chain4 = (
        pm.filter((col("match_context") == "primary_migration") & (col("mapping_status") == "approved"))
        .alias("pm4")
        .join(target_users.alias("tu4"), col("pm4.target_user_id") == col("tu4.user_id"), "inner")
        .join(
            ad_users.alias("adu"),
            (lower(trim(col("tu4.on_prem_distinguished_name"))) == lower(trim(col("adu.distinguished_name"))))
            & col("tu4.on_prem_distinguished_name").isNotNull()
            & (trim(col("tu4.on_prem_distinguished_name")) != ""),
            "inner",
        )
        .select(
            col("pm4.source_user_id"),
            col("adu.object_guid").alias("target_ad_user_object_id"),  # 5
            col("adu.user_principal_name").alias("target_ad_user_principal_name"),  # 6
        )
    )

    # Chain 5 (fields 9, 10): source user mail → AD contact mail/target_address
    chain5 = (
        source_users.alias("su5")
        .join(
            ad_contacts.alias("adc"),
            lower(trim(col("su5.mail"))) == col("adc.target_address"),  # target_address already lowered+stripped
            "inner",
        )
        .filter(col("su5.mail").isNotNull() & (trim(col("su5.mail")) != ""))
        .select(
            col("su5.user_id").alias("source_user_id"),
            col("adc.object_guid").alias("target_ad_contact_object_id"),  # 9
            col("adc.target_address").alias("target_ad_contact_target_address"),  # 10
        )
    )

    # -------------------------------------------------------------------------
    # Rationalization status — 7-scenario matrix
    #
    # | # | has_primary | has_mto | attrs_match | has_contact | Status                  |
    # |---|-------------|---------|-------------|-------------|-------------------------|
    # | 1 | No          | —       | —           | —           | not_mapped              |
    # | 2 | Yes         | No      | —           | No          | not_applicable          |
    # | 3 | Yes         | No      | —           | Yes         | pending_contact_removal |
    # | 4 | Yes         | Yes     | No          | Yes         | in_progress             |
    # | 5 | Yes         | Yes     | Yes         | Yes         | pending_contact_removal |
    # | 6 | Yes         | Yes     | No          | No          | attributes_pending      |
    # | 7 | Yes         | Yes     | Yes         | No          | completed               |
    # -------------------------------------------------------------------------
    has_primary = coalesce(col("c2.has_primary_mapping"), lit(False))
    has_mto = col("c1.target_tenant_mto_user_object_id").isNotNull()
    has_contact = col("c3.target_tenant_contact_object_id").isNotNull()
    attrs_match = (
        col("su.display_name").eqNullSafe(col("c1.mto_display_name"))
        & col("su.given_name").eqNullSafe(col("c1.mto_given_name"))
        & col("su.surname").eqNullSafe(col("c1.mto_surname"))
        & col("su.company_name").eqNullSafe(col("c1.mto_company"))
        & col("su.department").eqNullSafe(col("c1.mto_department"))
        & col("su.job_title").eqNullSafe(col("c1.mto_job_title"))
    )

    return (
        source_users.alias("su")
        .join(chain1.alias("c1"), col("su.user_id") == col("c1.source_user_id"), "left")
        .join(chain2.alias("c2"), col("su.user_id") == col("c2.source_user_id"), "left")
        .join(chain3.alias("c3"), col("su.user_id") == col("c3.source_user_id"), "left")
        .join(chain4.alias("c4"), col("su.user_id") == col("c4.source_user_id"), "left")
        .join(chain5.alias("c5"), col("su.user_id") == col("c5.source_user_id"), "left")
        .select(
            md5(col("su.user_id")).alias("rationalization_input_id"),
            col("su.user_id").alias("source_user_id"),
            col("su.entra_object_id").alias("home_tenant_object_id"),  # 1
            col("su.user_principal_name").alias("home_tenant_user_principal_name"),  # 2
            col("c1.target_tenant_mto_user_object_id"),  # 3
            col("c1.target_tenant_mto_user_principal_name"),  # 4
            col("c4.target_ad_user_object_id"),  # 5
            col("c4.target_ad_user_principal_name"),  # 6
            col("c2.target_tenant_hybrid_user_object_id"),  # 7
            col("c2.target_tenant_hybrid_user_principal_name"),  # 8
            col("c5.target_ad_contact_object_id"),  # 9
            col("c5.target_ad_contact_target_address"),  # 10
            col("c3.target_tenant_contact_object_id"),  # 11
            col("c3.target_tenant_contact_target_address"),  # 12
            normalized_domain(col("su.user_principal_name")).alias("upn_domain"),
            col("su.org_id").alias("org_id"),  # FK to dim_organization
            when(~has_primary, lit("not_mapped"))  # 1
            .when(~has_mto & ~has_contact, lit("not_applicable"))  # 2
            .when(~has_mto & has_contact, lit("pending_contact_removal"))  # 3
            .when(has_mto & ~attrs_match & has_contact, lit("in_progress"))  # 4
            .when(has_mto & attrs_match & has_contact, lit("pending_contact_removal"))  # 5
            .when(has_mto & ~attrs_match & ~has_contact, lit("attributes_pending"))  # 6
            .when(has_mto & attrs_match & ~has_contact, lit("completed"))  # 7
            .otherwise(lit("unknown"))
            .alias("rationalization_status"),
            (has_primary & has_mto & attrs_match & ~has_contact).alias(
                "is_rationalization_complete"
            ),  # scenario 7 only
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## dual_mailbox_users (Assessment #59)

# COMMAND ----------


@dlt.table(
    name="dual_mailbox_users",
    comment="Assessment: source users with an approved primary_migration mapping whose matched target account already has a mailbox. These require a separate migration path (#59). Excludes cloud external members — target on_prem_sync_enabled = true only.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def dual_mailbox_users():
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")
    mailboxes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mailboxes")
    pm = dlt.read("user_map")

    source_users = _resolve_user_org_id(users.filter(col("environment") == "source")).alias("su")

    approved_primary = pm.filter(
        (col("match_context") == "primary_migration") & (col("mapping_status") == "approved")
    ).alias("pm")

    # Exclude cloud external members — only on-prem synced target accounts per #59
    target_members = users.filter(
        (col("environment") == "target") & (col("user_type") == "Member") & col("on_prem_sync_enabled")
    ).alias("tu")

    target_mailboxes = mailboxes.filter(col("environment") == "target").alias("tm")

    return (
        approved_primary.join(source_users, col("pm.source_user_id") == col("su.user_id"), "inner")
        .join(target_members, col("pm.target_user_id") == col("tu.user_id"), "inner")
        .join(target_mailboxes, col("tu.mail") == col("tm.primary_smtp_address"), "inner")
        .select(
            col("su.user_id").alias("source_user_id"),
            col("su.user_principal_name").alias("source_upn"),
            col("su.display_name"),
            col("su.mail").alias("source_mail"),
            col("su.employee_id"),
            col("su.on_prem_domain_name").alias("source_on_prem_domain"),
            normalized_domain(col("su.user_principal_name")).alias("upn_domain"),
            col("su.org_id").alias("org_id"),
            col("tu.user_id").alias("target_user_id"),
            col("tu.user_principal_name").alias("target_upn"),
            col("tu.mail").alias("target_mail"),
            col("pm.match_score"),
            col("tm.mailbox_id").alias("target_mailbox_id"),
            col("tm.recipient_type").alias("target_mailbox_type"),
            lit(None).cast("decimal(18,2)").alias("target_mailbox_size_mb"),  # Sprint 2 — exo_mailbox_statistics
            lit(None).cast("long").alias("target_mailbox_item_count"),  # Sprint 2 — exo_mailbox_statistics
            col("tm.litigation_hold").alias("target_litigation_hold"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## groups_no_owner (Assessment #52)
# MAGIC
# MAGIC Source groups with at least one member but no owner assigned.
# MAGIC These require remediation before they can be provisioned in the target.
# MAGIC
# MAGIC **Suggested owner logic:**
# MAGIC Walk up 2 levels in the org chart for all group members.
# MAGIC For each manager candidate found at either level, count how many group
# MAGIC members are in their subtree (direct reports at level 1 + their reports at level 2).
# MAGIC Show top 3 candidates with coverage percentage.
# MAGIC
# MAGIC Org chart built from inline `manager_*` columns on Silver `users` (source tenant).
# MAGIC Level 1: group member → direct manager (users.manager_entra_object_id)
# MAGIC Level 2: group member → manager → manager's manager (self-join on users)

# COMMAND ----------


@dlt.table(
    name="groups_no_owner",
    comment="Assessment: source groups with at least one member but no owner. Includes top-3 suggested owners derived from 2-level org chart walk (#52). Filter by upn_domain or on_prem_domain_name for AE-level reporting.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def groups_no_owner():
    group_owners = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.group_owners")
    groups = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.groups")
    memberships = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.group_memberships")
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")

    # Source groups only
    source_groups = groups.filter(col("environment") == "source")

    # Groups that have at least one member
    groups_with_members = (
        memberships.filter((col("environment") == "source") & (col("member_type") == "user"))
        .groupBy("group_silver_id")
        .agg(count("*").alias("member_count"))
        .filter(col("member_count") > 0)
    )

    # group_owners is now per-owner rows (new silver schema). Derive owner counts
    # by aggregating, then LEFT JOIN: groups absent from the result have zero owners.
    owner_counts = (
        group_owners.filter(col("environment") == "source")
        .groupBy("group_silver_id")
        .agg(count("*").alias("owner_count"))
    )

    no_owner_groups = (
        source_groups.alias("g")
        .join(groups_with_members.alias("mc"), col("g.group_id") == col("mc.group_silver_id"), "inner")
        .join(owner_counts.alias("oc"), col("g.group_id") == col("oc.group_silver_id"), "left")
        .filter(col("oc.owner_count").isNull() | (col("oc.owner_count") == 0))
        .select(
            col("g.group_id"),
            col("g.display_name").alias("group_display_name"),
            col("g.mail").alias("group_mail"),
            col("g.group_type"),
            lit(None).cast("string").alias("on_prem_domain_name"),  # Sprint 2
            col("mc.member_count"),
            coalesce(col("oc.owner_count"), lit(0)).alias("owner_count"),
        )
    )

    # Resolve org_id for each group via mail domain lookup
    _dom = get_domain_org_map()
    _dom_mail = _dom.select(col("domain_value").alias("__gd"), col("org_id").alias("__go"))
    no_owner_groups = (
        no_owner_groups.withColumn(
            "__gmd",
            when(
                col("group_mail").isNotNull() & col("group_mail").contains("@"),
                lower(trim(split(col("group_mail"), "@")[1])),
            ),
        )
        .join(_dom_mail, col("__gmd") == col("__gd"), "left")
        .drop("__gmd", "__gd")
        .withColumnRenamed("__go", "org_id")
    )

    # --- Org chart walk ---
    # Source-tenant users with a manager populated. Replaces the deleted silver.user_managers
    # adjacency table by reading manager_* columns inline off silver.users (#417).
    source_managers = users.filter(
        (col("environment") == "source") & col("manager_entra_object_id").isNotNull()
    ).select(
        col("entra_object_id").alias("user_entra_object_id"),
        col("manager_entra_object_id"),
        col("manager_upn"),
        col("manager_display_name"),
    )

    # Level 1: group member → direct manager
    l1 = source_managers.select(
        col("user_entra_object_id").alias("member_entra_id"),
        col("manager_entra_object_id").alias("l1_manager_id"),
        col("manager_upn").alias("l1_manager_upn"),
        col("manager_display_name").alias("l1_manager_display_name"),
    )

    # Level 2: manager → manager's manager (self-join on source_managers)
    l2 = source_managers.select(
        col("user_entra_object_id").alias("l1_mgr_entra_id"),
        col("manager_entra_object_id").alias("l2_manager_id"),
        col("manager_upn").alias("l2_manager_upn"),
        col("manager_display_name").alias("l2_manager_display_name"),
    )

    # Group member user_ids (source, user members only)
    # member_id is the raw Entra object ID; group_silver_id = FK to groups.group_id
    member_users = memberships.filter((col("environment") == "source") & (col("member_type") == "user")).select(
        col("group_silver_id").alias("group_id"), col("member_id")
    )

    # Attach L1 and L2 managers to each group member
    members_with_managers = (
        member_users.alias("mu")
        .join(l1.alias("l1"), col("mu.member_id") == col("l1.member_entra_id"), "left")
        .join(l2.alias("l2"), col("l1.l1_manager_id") == col("l2.l1_mgr_entra_id"), "left")
        .select(
            col("mu.group_id"),
            col("mu.member_id"),
            col("l1.l1_manager_id"),
            col("l1.l1_manager_upn"),
            col("l1.l1_manager_display_name"),
            col("l2.l2_manager_id"),
            col("l2.l2_manager_upn"),
            col("l2.l2_manager_display_name"),
        )
    )

    # Count coverage per L1 manager per group
    l1_coverage = (
        members_with_managers.filter(col("l1_manager_id").isNotNull())
        .groupBy("group_id", "l1_manager_id", "l1_manager_upn", "l1_manager_display_name")
        .agg(count("member_id").alias("covered_members"))
        .select(
            col("group_id"),
            col("l1_manager_id").alias("candidate_id"),
            col("l1_manager_upn").alias("candidate_upn"),
            col("l1_manager_display_name").alias("candidate_display_name"),
            col("covered_members"),
            lit(1).alias("walk_level"),
        )
    )

    # Count coverage per L2 manager per group
    l2_coverage = (
        members_with_managers.filter(col("l2_manager_id").isNotNull())
        .groupBy("group_id", "l2_manager_id", "l2_manager_upn", "l2_manager_display_name")
        .agg(count("member_id").alias("covered_members"))
        .select(
            col("group_id"),
            col("l2_manager_id").alias("candidate_id"),
            col("l2_manager_upn").alias("candidate_upn"),
            col("l2_manager_display_name").alias("candidate_display_name"),
            col("covered_members"),
            lit(2).alias("walk_level"),
        )
    )

    # Union L1 and L2, keep highest coverage per candidate per group
    all_candidates = (
        l1_coverage.unionByName(l2_coverage)
        .groupBy("group_id", "candidate_id", "candidate_upn", "candidate_display_name")
        .agg(
            spark_sum("covered_members").alias("covered_members"),
            spark_min("walk_level").alias("walk_level"),  # prefer L1 if at both levels
        )
    )

    # Attach member_count to compute coverage_pct, rank within group, take top 3
    window_rank = Window.partitionBy(col("ac.group_id")).orderBy(col("covered_members").desc())

    top3_candidates = (
        all_candidates.alias("ac")
        .join(groups_with_members.alias("mc"), col("ac.group_id") == col("mc.group_silver_id"), "left")
        .withColumn("coverage_pct", spark_round((col("covered_members").cast("double") / col("member_count")) * 100, 1))
        .withColumn("rank", row_number().over(window_rank))
        .filter(col("rank") <= 3)
        .select(
            col("ac.group_id"),
            col("rank"),
            col("candidate_id"),
            col("candidate_upn"),
            col("candidate_display_name"),
            col("covered_members"),
            col("coverage_pct"),
            col("walk_level"),
        )
    )

    # Pivot top 3 candidates into columns on the group row
    c1 = top3_candidates.filter(col("rank") == 1).alias("c1")
    c2 = top3_candidates.filter(col("rank") == 2).alias("c2")
    c3 = top3_candidates.filter(col("rank") == 3).alias("c3")

    return (
        no_owner_groups.alias("g")
        .join(c1, col("g.group_id") == col("c1.group_id"), "left")
        .join(c2, col("g.group_id") == col("c2.group_id"), "left")
        .join(c3, col("g.group_id") == col("c3.group_id"), "left")
        .select(
            col("g.group_id"),
            col("g.group_display_name"),
            col("g.group_mail"),
            col("g.group_type"),
            col("g.on_prem_domain_name"),
            normalized_domain(col("g.group_mail")).alias("upn_domain"),
            col("g.org_id").alias("org_id"),
            col("g.member_count"),
            col("g.owner_count"),
            # Suggested owner 1 (highest coverage)
            col("c1.candidate_id").alias("suggested_owner_1_id"),
            col("c1.candidate_upn").alias("suggested_owner_1_upn"),
            col("c1.candidate_display_name").alias("suggested_owner_1_name"),
            col("c1.coverage_pct").alias("suggested_owner_1_coverage_pct"),
            col("c1.walk_level").alias("suggested_owner_1_walk_level"),
            # Suggested owner 2
            col("c2.candidate_id").alias("suggested_owner_2_id"),
            col("c2.candidate_upn").alias("suggested_owner_2_upn"),
            col("c2.candidate_display_name").alias("suggested_owner_2_name"),
            col("c2.coverage_pct").alias("suggested_owner_2_coverage_pct"),
            col("c2.walk_level").alias("suggested_owner_2_walk_level"),
            # Suggested owner 3
            col("c3.candidate_id").alias("suggested_owner_3_id"),
            col("c3.candidate_upn").alias("suggested_owner_3_upn"),
            col("c3.candidate_display_name").alias("suggested_owner_3_name"),
            col("c3.coverage_pct").alias("suggested_owner_3_coverage_pct"),
            col("c3.walk_level").alias("suggested_owner_3_walk_level"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## groups_accept_external_email (Assessment #53)
# MAGIC
# MAGIC Source groups that accept email from the internet — i.e.,
# MAGIC `RequireSenderAuthenticationEnabled = false` in Exchange Online.
# MAGIC These require remediation (typically conversion to a shared mailbox or
# MAGIC enabling sender-authentication) before they can be safely migrated.
# MAGIC
# MAGIC Covers three EXO group categories:
# MAGIC - **Distribution List** (`recipient_type_details = MailUniversalDistributionGroup`) — from `silver.exo_distribution_groups`
# MAGIC - **Mail-Enabled Security** (`recipient_type_details = MailUniversalSecurityGroup`) — from `silver.exo_distribution_groups`
# MAGIC - **M365 Unified Group** (group mailbox) — from `silver.exo_unified_groups`
# MAGIC
# MAGIC Filter by `group_type`, `upn_domain`, or `on_prem_domain_name` for AE-level reporting.

# COMMAND ----------


@dlt.table(
    name="groups_accept_external_email",
    comment="Assessment: source groups (DL, MES, M365) with RequireSenderAuthenticationEnabled=false — accept email from the internet. Requires remediation before migration. Filter by group_type or upn_domain for AE reporting; on_prem_domain_name is currently NULL (pending #555). #53.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def groups_accept_external_email():
    groups = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.groups")
    exo_dist = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.exo_distribution_groups")
    exo_unified = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.exo_unified_groups")

    source_groups = groups.filter(col("environment") == "source")

    # Distribution Lists + Mail-Enabled Security groups (from Get-DistributionGroup)
    dist_open = (
        exo_dist.filter(
            (col("environment") == "source")
            & (col("requires_sender_authentication") == False)  # noqa: E712
            & col("recipient_type_details").isin(
                "MailUniversalDistributionGroup",
                "MailUniversalSecurityGroup",
            )
        )
        .alias("ed")
        .join(
            source_groups.alias("g"),
            (col("ed.entra_object_id") == col("g.entra_object_id")) & (col("ed.source_key") == col("g.source_key")),
            "inner",
        )
        .select(
            col("g.group_id"),
            col("g.display_name").alias("group_display_name"),
            col("g.mail").alias("group_mail"),
            col("g.group_type"),
            col("ed.recipient_type_details"),
            col("g.source_key"),
        )
    )

    # M365 Unified Groups (from Get-UnifiedGroup)
    unified_open = (
        exo_unified.filter(
            (col("environment") == "source") & (col("requires_sender_authentication") == False)  # noqa: E712
        )
        .alias("eu")
        .join(
            source_groups.alias("g"),
            (col("eu.entra_object_id") == col("g.entra_object_id")) & (col("eu.source_key") == col("g.source_key")),
            "inner",
        )
        .select(
            col("g.group_id"),
            col("g.display_name").alias("group_display_name"),
            col("g.mail").alias("group_mail"),
            col("g.group_type"),
            col("eu.recipient_type_details"),
            col("g.source_key"),
        )
    )

    all_open = dist_open.unionByName(unified_open)

    # Resolve org_id via mail domain
    _dom = get_domain_org_map()
    _dom_mail = _dom.select(col("domain_value").alias("__gd"), col("org_id").alias("__go"))
    all_open = (
        all_open.withColumn(
            "__gmd",
            when(
                col("group_mail").isNotNull() & col("group_mail").contains("@"),
                lower(trim(split(col("group_mail"), "@")[1])),
            ),
        )
        .join(_dom_mail, col("__gmd") == col("__gd"), "left")
        .drop("__gmd", "__gd")
        .withColumnRenamed("__go", "org_id")
    )

    return (
        all_open.withColumn("upn_domain", normalized_domain(col("group_mail")))
        .withColumn("on_prem_domain_name", lit(None).cast("string"))  # DL/MES: requires AD group data (see #555)
        .withColumn("gold_loaded_at", current_timestamp())
        .select(
            "group_id",
            "group_display_name",
            "group_mail",
            "group_type",
            "recipient_type_details",
            "on_prem_domain_name",
            "upn_domain",
            "org_id",
            "source_key",
            "gold_loaded_at",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## mapping_issues (Assessment)

# COMMAND ----------


@dlt.table(
    name="mapping_issues",
    comment="Flagged mapping issues requiring review",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def mapping_issues():
    candidates = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.user_mapping_candidates")
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")
    source_users = users.filter(col("environment") == "source")

    ambiguous = (
        candidates.filter(col("match_score") >= 80)
        .groupBy("source_user_id", "match_context")
        .agg(count("*").alias("high_score_count"))
        .filter(col("high_score_count") > 1)
        .withColumn("issue_type", lit("ambiguous_match"))
        .withColumn(
            "issue_description",
            concat_ws(
                " ",
                lit("Multiple high-scoring matches in"),
                col("match_context"),
                lit("context:"),
                col("high_score_count"),
                lit("candidates"),
            ),
        )
        .withColumn("severity", lit("high"))
        .select(
            concat_ws("_", lit("issue"), col("source_user_id"), col("match_context")).alias("issue_id"),
            col("source_user_id").alias("entity_id"),
            lit("user").alias("entity_type"),
            col("issue_type"),
            col("issue_description"),
            col("severity"),
            col("match_context"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )

    low_confidence = (
        candidates.filter(col("match_context") == "primary_migration")
        .groupBy("source_user_id")
        .agg(spark_max("match_score").alias("best_score"))
        .filter((col("best_score") < 70) & (col("best_score") > 0))
        .withColumn("issue_type", lit("low_confidence_match"))
        .withColumn(
            "issue_description",
            concat_ws(" ", lit("Best primary_migration score is only"), col("best_score").cast("string")),
        )
        .withColumn("severity", lit("medium"))
        .withColumn("match_context", lit("primary_migration"))
        .select(
            concat_ws("_", lit("issue"), col("source_user_id")).alias("issue_id"),
            col("source_user_id").alias("entity_id"),
            lit("user").alias("entity_type"),
            col("issue_type"),
            col("issue_description"),
            col("severity"),
            col("match_context"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )

    source_with_primary = (
        candidates.filter(col("match_context") == "primary_migration").select("source_user_id").distinct()
    )
    no_primary = source_users.join(
        source_with_primary, source_users["user_id"] == source_with_primary["source_user_id"], "left_anti"
    ).select(
        concat_ws("_", lit("issue"), col("user_id")).alias("issue_id"),
        col("user_id").alias("entity_id"),
        lit("user").alias("entity_type"),
        lit("no_primary_migration_match").alias("issue_type"),
        lit("No primary_migration candidates — extension attributes may be missing").alias("issue_description"),
        lit("high").alias("severity"),
        lit("primary_migration").alias("match_context"),
        current_timestamp().alias("gold_loaded_at"),
    )

    return ambiguous.unionByName(low_confidence).unionByName(no_primary)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Summaries

# COMMAND ----------


@dlt.table(
    name="migration_summary",
    comment="High-level migration metrics by environment",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def migration_summary():
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")
    groups = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.groups")
    contacts = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.contacts")
    mailboxes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mailboxes")
    mail_users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mail_users")

    user_stats = users.groupBy("environment").agg(
        count("*").alias("user_count"),
        spark_sum(col("account_enabled").cast("int")).alias("enabled_user_count"),
        spark_sum(col("has_e3_license").cast("int")).alias("e3_license_count"),
        spark_sum(col("has_e5_license").cast("int")).alias("e5_license_count"),
    )
    group_stats = groups.groupBy("environment").agg(count("*").alias("group_count"))
    contact_stats = contacts.groupBy("environment").agg(count("*").alias("contact_count"))
    mailbox_stats = mailboxes.groupBy("environment").agg(
        count("*").alias("mailbox_count"),
        lit(None).cast("decimal(18,2)").alias("total_mailbox_size_mb"),  # Sprint 2 — exo_mailbox_statistics
    )
    mail_user_stats = mail_users.groupBy("environment").agg(count("*").alias("mail_user_count"))

    return (
        user_stats.alias("u")
        .join(group_stats.alias("g"), "environment", "left")
        .join(contact_stats.alias("c"), "environment", "left")
        .join(mailbox_stats.alias("m"), "environment", "left")
        .join(mail_user_stats.alias("mu"), "environment", "left")
        .select(
            col("environment"),
            col("user_count"),
            col("enabled_user_count"),
            col("e3_license_count"),
            col("e5_license_count"),
            coalesce(col("group_count"), lit(0)).alias("group_count"),
            coalesce(col("contact_count"), lit(0)).alias("contact_count"),
            coalesce(col("mailbox_count"), lit(0)).alias("mailbox_count"),
            col("total_mailbox_size_mb"),  # null until Sprint 2 (exo_mailbox_statistics)
            coalesce(col("mail_user_count"), lit(0)).alias("mail_user_count"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="people_summary",
    comment=(
        "Summary of in-scope persons by migration_path and migration_status. "
        "Source: dim_people (#485 T5). Reflects the source-anchored, license-filtered grain."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def people_summary():
    people = dlt.read("dim_people")

    return (
        people.agg(
            count("*").alias("total_people"),
            spark_sum(col("target_user_id").isNotNull().cast("int")).alias("matched_people"),
            spark_sum(col("matched_on_employee_id").isNotNull().cast("int")).alias("pass1_matched_people"),
            spark_sum(col("organization_id").isNotNull().cast("int")).alias("org_resolved_people"),
            spark_sum(col("has_source_mailbox").cast("int")).alias("source_mailbox_count"),
            spark_sum(col("has_target_mailbox").cast("int")).alias("target_mailbox_count"),
            spark_sum(col("has_source_onedrive").cast("int")).alias("source_onedrive_count"),
            spark_sum(col("has_target_onedrive").cast("int")).alias("target_onedrive_count"),
            spark_sum(when(col("migration_path") == "parallel", 1).otherwise(0)).alias("path_parallel_count"),
            spark_sum(when(col("migration_path") == "dual", 1).otherwise(0)).alias("path_dual_count"),
            spark_sum(when(col("migration_path") == "standard", 1).otherwise(0)).alias("path_standard_count"),
            spark_sum(when(col("migration_path") == "ambiguous", 1).otherwise(0)).alias("path_ambiguous_count"),
            spark_sum(when(col("migration_status") == "migrated", 1).otherwise(0)).alias("status_migrated_count"),
            spark_sum(when(col("migration_status") == "ambiguous", 1).otherwise(0)).alias("status_ambiguous_count"),
            spark_sum(when(col("migration_status") == "pending", 1).otherwise(0)).alias("status_pending_count"),
        )
        .withColumn(
            "matched_pct",
            when(col("total_people") > 0, (col("matched_people") / col("total_people")).cast("decimal(5,4)")).otherwise(
                lit(None).cast("decimal(5,4)")
            ),
        )
        .withColumn("gold_loaded_at", current_timestamp())
    )


# COMMAND ----------


@dlt.table(
    name="mapping_candidates_summary",
    comment="Summary of mapping candidates by context, score band, and status",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def mapping_candidates_summary():
    candidates = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.user_mapping_candidates")

    return (
        candidates.withColumn(
            "score_band",
            when(col("match_score") >= 90, lit("90-100 (High)"))
            .when(col("match_score") >= 80, lit("80-89 (Good)"))
            .when(col("match_score") >= 70, lit("70-79 (Moderate)"))
            .otherwise(lit("Below 70 (Low)")),
        )
        .groupBy("match_context", "score_band", "match_type", "candidate_status")
        .agg(count("*").alias("candidate_count"))
        .withColumn("gold_loaded_at", current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Non-Person Mailboxes

# COMMAND ----------


@dlt.table(
    name="non_person_mailboxes",
    comment="Non-person mailboxes: shared, room, equipment, and other resource mailboxes",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def non_person_mailboxes():
    mailboxes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mailboxes")

    non_person_types = [
        "SharedMailbox",
        "RoomMailbox",
        "EquipmentMailbox",
        "SchedulingMailbox",
        "GroupMailbox",
        "DiscoveryMailbox",
        "TeamMailbox",
    ]

    return (
        mailboxes.filter(col("recipient_type").isin(non_person_types))
        .withColumn(
            "mailbox_category",
            when(col("recipient_type") == "SharedMailbox", lit("shared"))
            .when(col("recipient_type") == "RoomMailbox", lit("room"))
            .when(col("recipient_type") == "EquipmentMailbox", lit("equipment"))
            .when(col("recipient_type") == "SchedulingMailbox", lit("scheduling"))
            .when(col("recipient_type") == "GroupMailbox", lit("group"))
            .otherwise(lit("other")),
        )
        .select(
            col("mailbox_id"),
            col("environment"),
            col("source_key"),
            col("exchange_guid"),
            col("display_name"),
            col("primary_smtp_address"),
            col("recipient_type").alias("recipient_type_details"),
            col("mailbox_category"),
            lit(None).cast("decimal(18,2)").alias("total_item_size_mb"),  # Sprint 2 — exo_mailbox_statistics
            lit(None).cast("long").alias("item_count"),  # Sprint 2 — exo_mailbox_statistics
            when(col("has_archive"), lit("Active")).otherwise(lit("None")).alias("archive_status"),
            lit(None).cast("decimal(18,2)").alias("archive_size_mb"),  # Sprint 2 — exo_mailbox_statistics
            col("litigation_hold").alias("litigation_hold_enabled"),
            lit(None).cast("string").alias("mapped_to_mailbox_id"),
            lit(None).cast("string").alias("mapping_status"),
            col("last_updated_at"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Volumes

# COMMAND ----------


@dlt.table(
    name="data_volumes",
    comment=(
        "Long-format data-volume metrics by (environment, data_type, metric_name). "
        "Grain: one row per (environment, data_type, metric_name). "
        "metric_unit is 'count' or 'mb'. Power BI computes GB/TB from MB. "
        "data_type ∈ {mailbox, archive, onedrive, sharepoint}; "
        "metric_name ∈ {item_count, size_mb, litigation_hold_count}."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def data_volumes():
    mailboxes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mailboxes")
    spo_sites = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_sites")

    # Wide per-data_type aggregates first, then unpivot via stack().
    mailbox_wide = mailboxes.groupBy("environment").agg(
        count("*").cast("decimal(18,2)").alias("mailbox__item_count"),
        # Sprint 2: real mailbox sizes live in exo_mailbox_statistics; emit 0 placeholder.
        lit(0).cast("decimal(18,2)").alias("mailbox__size_mb"),
        spark_sum(col("litigation_hold").cast("int")).cast("decimal(18,2)").alias("mailbox__litigation_hold_count"),
        spark_sum(col("has_archive").cast("int")).cast("decimal(18,2)").alias("archive__item_count"),
        lit(0).cast("decimal(18,2)").alias("archive__size_mb"),
    )

    onedrive_wide = (
        spo_sites.filter(col("is_personal_site"))
        .groupBy("environment")
        .agg(
            count("*").cast("decimal(18,2)").alias("onedrive__item_count"),
            spark_sum("storage_used_mb").cast("decimal(18,2)").alias("onedrive__size_mb"),
        )
    )
    sharepoint_wide = (
        spo_sites.filter(~col("is_personal_site"))
        .groupBy("environment")
        .agg(
            count("*").cast("decimal(18,2)").alias("sharepoint__item_count"),
            spark_sum("storage_used_mb").cast("decimal(18,2)").alias("sharepoint__size_mb"),
        )
    )

    # Unpivot helper: emits rows of (environment, data_type, metric_name, metric_value).
    # Each metric column is named "<data_type>__<metric_name>".
    def _unpivot(df, metric_cols):
        # build "stack(N, 'dt1','m1',col1, 'dt2','m2',col2, ...)" expression
        n = len(metric_cols)
        parts = []
        for c in metric_cols:
            data_type, metric_name = c.split("__", 1)
            parts.append(f"'{data_type}','{metric_name}',`{c}`")
        stack_expr = f"stack({n}," + ",".join(parts) + ") as (data_type, metric_name, metric_value)"
        return df.selectExpr("environment", stack_expr)

    mailbox_long = _unpivot(
        mailbox_wide,
        [
            "mailbox__item_count",
            "mailbox__size_mb",
            "mailbox__litigation_hold_count",
            "archive__item_count",
            "archive__size_mb",
        ],
    )
    onedrive_long = _unpivot(onedrive_wide, ["onedrive__item_count", "onedrive__size_mb"])
    sharepoint_long = _unpivot(sharepoint_wide, ["sharepoint__item_count", "sharepoint__size_mb"])

    return (
        mailbox_long.unionByName(onedrive_long)
        .unionByName(sharepoint_long)
        .withColumn(
            "metric_unit",
            when(col("metric_name") == lit("size_mb"), lit("mb")).otherwise(lit("count")),
        )
        .select("environment", "data_type", "metric_name", "metric_value", "metric_unit")
        .withColumn("gold_loaded_at", current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## SPO Site Dimension

# COMMAND ----------


@dlt.table(
    name="dim_spo_site",
    comment=(
        "SharePoint / OneDrive site dimension. "
        "Grain: one row per SharePoint/OneDrive site (environment, source_key, site_silver_id). "
        "PK: site_silver_id. FK candidates: concat_ws('_', source_key, group_id) → dim_group.group_id (M365 group GUID, no relationship enforced). "
        "Holds descriptive site attributes referenced by spo_custom_permission_burden, "
        "spo_subweb_complexity, and spo_sharing_posture facts."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def dim_spo_site():
    sites = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_sites")
    return sites.select(
        col("site_silver_id"),
        col("environment"),
        col("source_key"),
        col("web_url"),
        col("display_name"),
        col("template"),
        col("is_personal_site"),
        col("is_hub_site"),
        col("hub_site_id"),
        col("group_id"),
        col("is_teams_connected"),
        col("is_teams_channel_connected"),
        col("status"),
        col("locale_id"),
        col("storage_quota_mb"),
        col("storage_used_mb"),
        col("sharing_capability"),
        col("lock_state"),
        col("sensitivity_label"),
        col("archive_status"),
        col("archived_file_disk_used_mb"),
        col("source_modified_at"),
        col("last_updated_at"),
        current_timestamp().alias("gold_loaded_at"),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## SPO Custom Permission Burden

# COMMAND ----------


@dlt.table(
    name="spo_custom_permission_burden",
    comment=(
        "Per-site SharePoint permission complexity signals for migration planning. "
        "Grain: (environment, source_key, site_silver_id). "
        "FK: site_silver_id → dim_spo_site (descriptive site attributes live there). "
        "web_with_unique_perms_count is a proxy: counts webs that have rows in "
        "silver.spo_web_role_assignments. SP REST only returns the RoleAssignments "
        "collection on webs with HasUniqueRoleAssignments=true (see ingest config), "
        "so presence == uniqueness in practice."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def spo_custom_permission_burden():
    sites = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_sites")
    webs = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_webs")
    role_assignments = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_web_role_assignments")
    role_definitions = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_web_role_definitions")
    item_principals = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_web_item_principals")

    # Foreign-tenant detection: join role_assignments to dim_tenant on source_key to
    # obtain the site-specific tenant_id per row. This ensures an app from tenant A
    # granted on a site in tenant B is correctly counted as foreign to tenant B's site,
    # even when multiple tenants are configured. The dlt.read() call also tells DLT to
    # wire dim_tenant as an upstream dependency of this table.
    dim_tenant_df = dlt.read("dim_tenant").select(
        col("source_key"),
        lower(col("tenant_id")).alias("site_tenant_id"),
    )
    ids_present = any(
        x.strip()
        for x in (spark.conf.get("source_tenant_ids", "") + "," + spark.conf.get("target_tenant_ids", "")).split(",")
    )
    if not ids_present:
        # Fallback: pipeline params not configured (e.g. dev) — emit the legacy
        # "non-built-in app" proxy rather than silently zero out the metric.
        print(
            "WARNING: source_tenant_ids/target_tenant_ids pipeline params are not configured; "
            "spo_custom_permission_burden.tenant_external_app_grant_count will use the non-built-in-app proxy and "
            "overcount own-tenant apps. Set source_tenant_ids / target_tenant_ids pipeline params to enable proper "
            "foreign-tenant detection."
        )
    ra_with_tenant = role_assignments.join(dim_tenant_df, "source_key", "left")
    # An app grant is foreign-tenant when its home tenant differs from the site's own
    # tenant_id. When site_tenant_id is null (dim_tenant not configured), any
    # non-sharepoint app is counted as foreign to preserve legacy proxy behaviour.
    foreign_app_predicate = (
        (col("principal_type_parsed") == lit("app"))
        & col("aad_app_home_tenant_id").isNotNull()
        & (lower(col("aad_app_home_tenant_id")) != lit("sharepoint"))
        & (col("site_tenant_id").isNull() | (lower(col("aad_app_home_tenant_id")) != col("site_tenant_id")))
    )

    web_counts = webs.groupBy("site_silver_id").agg(count("*").alias("web_count"))

    web_unique = (
        role_assignments.select("site_silver_id", "web_silver_id")
        .distinct()
        .groupBy("site_silver_id")
        .agg(count("*").alias("web_with_unique_perms_count"))
    )

    # SP RoleType enum: 0=None, 1..9=built-ins, 255=Custom. Treat 0 and 255 as custom.
    custom_roles = (
        role_definitions.filter(col("role_type_kind").isin(0, 255))
        .groupBy("site_silver_id")
        .agg(count("*").alias("custom_role_definition_count"))
    )

    unique_perm_items = (
        item_principals.filter(col("has_unique_permissions"))
        .select("site_silver_id", "list_silver_id", "item_unique_id")
        .distinct()
        .groupBy("site_silver_id")
        .agg(count("*").alias("unique_perm_item_count"))
    )

    ra_buckets = ra_with_tenant.groupBy("site_silver_id").agg(
        spark_sum(
            when(
                (col("principal_type_parsed") == lit("user")) & col("aad_user_is_guest").eqNullSafe(lit(True)),
                lit(1),
            ).otherwise(lit(0))
        ).alias("external_principal_grant_count"),
        spark_sum(when(col("principal_type_parsed") == lit("everyoneExceptExternal"), lit(1)).otherwise(lit(0))).alias(
            "eeeu_grant_count"
        ),
        spark_sum(when(col("principal_type_parsed") == lit("everyone"), lit(1)).otherwise(lit(0))).alias(
            "everyone_grant_count"
        ),
        spark_sum(when(col("principal_type_parsed") == lit("app"), lit(1)).otherwise(lit(0))).alias("app_grant_count"),
        spark_sum(when(foreign_app_predicate, lit(1)).otherwise(lit(0))).alias("tenant_external_app_grant_count"),
    )

    return (
        sites.alias("s")
        .join(web_counts.alias("wc"), "site_silver_id", "left")
        .join(web_unique.alias("wu"), "site_silver_id", "left")
        .join(custom_roles.alias("cr"), "site_silver_id", "left")
        .join(unique_perm_items.alias("upi"), "site_silver_id", "left")
        .join(ra_buckets.alias("ra"), "site_silver_id", "left")
        .select(
            col("s.site_silver_id"),
            col("s.environment"),
            col("s.source_key"),
            coalesce(col("wc.web_count"), lit(0)).cast("long").alias("web_count"),
            coalesce(col("wu.web_with_unique_perms_count"), lit(0)).cast("long").alias("web_with_unique_perms_count"),
            coalesce(col("cr.custom_role_definition_count"), lit(0)).cast("long").alias("custom_role_definition_count"),
            coalesce(col("upi.unique_perm_item_count"), lit(0)).cast("long").alias("unique_perm_item_count"),
            coalesce(col("ra.external_principal_grant_count"), lit(0))
            .cast("long")
            .alias("external_principal_grant_count"),
            coalesce(col("ra.eeeu_grant_count"), lit(0)).cast("long").alias("eeeu_grant_count"),
            coalesce(col("ra.everyone_grant_count"), lit(0)).cast("long").alias("everyone_grant_count"),
            coalesce(col("ra.app_grant_count"), lit(0)).cast("long").alias("app_grant_count"),
            coalesce(col("ra.tenant_external_app_grant_count"), lit(0))
            .cast("long")
            .alias("tenant_external_app_grant_count"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## SPO Subweb Complexity

# COMMAND ----------


@dlt.table(
    name="spo_subweb_complexity",
    comment=(
        "Per-site subweb topology for migration scoping. "
        "Grain: (environment, source_key, site_silver_id). "
        "FK: site_silver_id → dim_spo_site (descriptive site attributes live there). "
        "Depth derived from server_relative_url path segments relative to the root web."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def spo_subweb_complexity():
    sites = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_sites")
    webs = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_webs")

    root_segments = webs.filter(col("is_root_web")).select(
        col("site_silver_id"),
        size(split(col("server_relative_url"), "/")).alias("root_seg_count"),
    )

    web_with_depth = (
        webs.alias("w")
        .join(root_segments.alias("r"), "site_silver_id", "left")
        .select(
            col("w.site_silver_id"),
            col("w.is_root_web"),
            (size(split(col("w.server_relative_url"), "/")) - col("r.root_seg_count")).alias("depth"),
        )
    )

    topology = web_with_depth.groupBy("site_silver_id").agg(
        count("*").alias("total_web_count"),
        spark_sum(when(col("is_root_web"), lit(0)).otherwise(lit(1))).alias("subweb_count"),
        spark_max("depth").alias("max_subweb_depth"),
    )

    return (
        sites.alias("s")
        .join(topology.alias("t"), "site_silver_id", "left")
        .select(
            col("s.site_silver_id"),
            col("s.environment"),
            col("s.source_key"),
            coalesce(col("t.subweb_count"), lit(0)).cast("long").alias("subweb_count"),
            coalesce(col("t.total_web_count"), lit(0)).cast("long").alias("total_web_count"),
            coalesce(col("t.max_subweb_depth"), lit(0)).cast("int").alias("max_subweb_depth"),
            (coalesce(col("t.subweb_count"), lit(0)) > 0).alias("has_subwebs"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## SPO Sharing Posture

# COMMAND ----------


@dlt.table(
    name="spo_sharing_posture",
    comment=(
        "Per-site sharing-link posture from silver.spo_web_item_links. "
        "Grain: (environment, source_key, site_silver_id). "
        "FK: site_silver_id → dim_spo_site (descriptive site attributes live there). "
        "Bucketing uses scope (-1=Unknown, 0=Anyone/anonymous, 1=Organization, "
        "2=SpecificPeople) combined with link_kind (1=Direct, 2=OrgView, 3=OrgEdit, "
        "4=AnonView, 5=AnonEdit, 6=Flexible) since dev data uses both."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def spo_sharing_posture():
    sites = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_sites")
    links = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_web_item_links").filter(
        col("share_id") != lit("00000000-0000-0000-0000-000000000000")
    )
    link_exp_ts = to_timestamp(col("link_expiration"))
    is_anonymous = (col("scope") == lit(0)) | col("link_kind").isin(4, 5)
    is_organization = (col("scope") == lit(1)) | col("link_kind").isin(2, 3)
    is_direct = (col("scope") == lit(2)) | (col("link_kind") == lit(1))

    posture = links.groupBy("site_silver_id").agg(
        count("*").alias("total_link_count"),
        spark_sum(when(col("is_active"), lit(1)).otherwise(lit(0))).alias("active_link_count"),
        spark_sum(when(is_anonymous, lit(1)).otherwise(lit(0))).alias("anonymous_link_count"),
        spark_sum(when(is_organization, lit(1)).otherwise(lit(0))).alias("organization_link_count"),
        spark_sum(when(is_direct, lit(1)).otherwise(lit(0))).alias("direct_link_count"),
        spark_sum(when(col("is_edit_link"), lit(1)).otherwise(lit(0))).alias("edit_link_count"),
        spark_sum(
            when(col("has_external_guest_invitees"), coalesce(col("total_link_members_count"), lit(0))).otherwise(
                lit(0)
            )
        ).alias("external_guest_invitee_count"),
        spark_sum(when(link_exp_ts.isNotNull() & (link_exp_ts < current_timestamp()), lit(1)).otherwise(lit(0))).alias(
            "expired_link_count"
        ),
        spark_sum(
            when(
                link_exp_ts.isNotNull()
                & (link_exp_ts >= current_timestamp())
                & (link_exp_ts <= expr("current_timestamp() + INTERVAL 30 DAYS")),
                lit(1),
            ).otherwise(lit(0))
        ).alias("expiring_30d_link_count"),
    )

    return (
        sites.alias("s")
        .join(posture.alias("p"), "site_silver_id", "left")
        .select(
            col("s.site_silver_id"),
            col("s.environment"),
            col("s.source_key"),
            coalesce(col("p.total_link_count"), lit(0)).cast("long").alias("total_link_count"),
            coalesce(col("p.active_link_count"), lit(0)).cast("long").alias("active_link_count"),
            coalesce(col("p.anonymous_link_count"), lit(0)).cast("long").alias("anonymous_link_count"),
            coalesce(col("p.organization_link_count"), lit(0)).cast("long").alias("organization_link_count"),
            coalesce(col("p.direct_link_count"), lit(0)).cast("long").alias("direct_link_count"),
            coalesce(col("p.edit_link_count"), lit(0)).cast("long").alias("edit_link_count"),
            coalesce(col("p.external_guest_invitee_count"), lit(0)).cast("long").alias("external_guest_invitee_count"),
            coalesce(col("p.expired_link_count"), lit(0)).cast("long").alias("expired_link_count"),
            coalesce(col("p.expiring_30d_link_count"), lit(0)).cast("long").alias("expiring_30d_link_count"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="storage_summary",
    comment="Total storage summary across all data types",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def storage_summary():
    volumes = dlt.read("data_volumes")

    # Pivot the long-format data_volumes back to wide on metric_name for aggregation.
    sizes = volumes.filter(col("metric_name") == lit("size_mb")).select(
        "environment", "data_type", col("metric_value").alias("size_mb")
    )
    counts = volumes.filter(col("metric_name") == lit("item_count")).select(
        "environment", "data_type", col("metric_value").cast("long").alias("item_count")
    )
    paired = sizes.join(counts, ["environment", "data_type"], "outer")

    by_environment = (
        paired.groupBy("environment")
        .agg(spark_sum("size_mb").alias("total_size_mb"), spark_sum("item_count").alias("total_items"))
        .withColumn("total_size_gb", (col("total_size_mb") / 1024).cast("decimal(18,2)"))
        .withColumn("total_size_tb", (col("total_size_mb") / 1048576).cast("decimal(18,2)"))
    )
    by_type = (
        paired.groupBy("data_type")
        .agg(spark_sum("size_mb").alias("total_size_mb"), spark_sum("item_count").alias("total_items"))
        .withColumn("environment", lit("all"))
        .withColumn("total_size_gb", (col("total_size_mb") / 1024).cast("decimal(18,2)"))
        .withColumn("total_size_tb", (col("total_size_mb") / 1048576).cast("decimal(18,2)"))
        .select("environment", "data_type", "total_items", "total_size_mb", "total_size_gb", "total_size_tb")
    )

    return (
        by_environment.withColumn("data_type", lit("all"))
        .select("environment", "data_type", "total_items", "total_size_mb", "total_size_gb", "total_size_tb")
        .unionByName(by_type)
        .withColumn("gold_loaded_at", current_timestamp())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Users with Active Devices
# MAGIC
# MAGIC Source users who have at least one Intune-managed device that synced
# MAGIC within the last 30 days. Correlates with corporate device adoption —
# MAGIC these users are likely to encounter device issues post-migration.
# MAGIC
# MAGIC Join: source user UPN = intune_devices.user_principal_name (both lowercased).
# MAGIC Active = last_sync_at >= today - 30 days.
# MAGIC Entra device metadata (trust_type, registration_at, approximate_last_sign_in_at)
# MAGIC is resolved via intune_devices.azure_ad_device_id → devices.azure_ad_device_id.
# MAGIC Filter by upn_domain or on_prem_domain_name for AE-level reporting.

# COMMAND ----------


@dlt.table(
    name="users_with_active_devices",
    comment="Source users with at least one Intune-managed device synced in last 30 days. Used for post-migration device risk assessment (#55). Filter by upn_domain or source_on_prem_domain for AE reporting.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def users_with_active_devices():
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")
    intune_devices = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.intune_devices")
    devices = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.devices")

    source_members = _resolve_user_org_id(
        users.filter(
            (col("environment") == "source") & (col("user_type") == "Member") & col("user_principal_name").isNotNull()
        )
    )

    # Intune devices synced within last 30 days — source environment only
    # (intune_devices spans all tenants; without this filter a target-tenant device
    # with the same UPN would incorrectly match a source user)
    active_intune = intune_devices.filter(
        (col("environment") == "source")
        & col("last_sync_at").isNotNull()
        & (datediff(current_date(), col("last_sync_at")) <= 30)
    )

    # Entra device records — resolved via azure_ad_device_id for trust_type / sign-in date
    entra_device_info = devices.filter(col("azure_ad_device_id").isNotNull()).select(
        col("azure_ad_device_id"),
        col("trust_type"),
        col("registration_at"),
        col("approximate_last_sign_in_at"),
    )

    return (
        source_members.alias("u")
        .join(
            active_intune.alias("d"),
            lower(trim(col("u.user_principal_name")))
            == col("d.user_principal_name"),  # intune UPN is already lower(trim)
            "inner",
        )
        .join(entra_device_info.alias("e"), col("d.azure_ad_device_id") == col("e.azure_ad_device_id"), "left")
        .select(
            col("u.user_id"),
            col("u.user_principal_name"),
            col("u.display_name"),
            col("u.mail"),
            col("u.employee_id"),
            col("u.department"),
            col("u.on_prem_domain_name").alias("source_on_prem_domain"),
            split(lower(trim(col("u.user_principal_name"))), "@")[1].alias("upn_domain"),
            col("u.org_id").alias("org_id"),
            col("u.account_enabled"),
            col("u.on_prem_sync_enabled"),
            # Intune device fields
            col("d.intune_device_id").alias("device_id"),
            col("d.display_name").alias("device_display_name"),
            col("d.azure_ad_device_id"),
            col("d.os_version").alias("operating_system_version"),
            col("d.owner_type").alias("device_ownership"),
            col("d.compliance_state"),
            col("d.is_encrypted"),
            col("d.enrolled_at").alias("device_enrolled_at"),
            col("d.last_sync_at"),
            # Entra device fields (when available via azure_ad_device_id join)
            col("e.trust_type"),
            col("e.registration_at").alias("device_registered_at"),
            col("e.approximate_last_sign_in_at"),
            datediff(current_date(), col("d.last_sync_at")).alias("days_since_sync"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Sprint 8 - RC-01 / RC-02 / RC-05 tables
# MAGIC
# MAGIC | Table | Grain | Reports |
# MAGIC |---|---|---|
# MAGIC | dim_user | one row per user | RC-01, RC-02, RC-03 |
# MAGIC | dim_group | one row per group | RC-01 |
# MAGIC | fact_user_mapping_analysis | one row per mapping candidate | RC-02, RC-01 |
# MAGIC | fact_mailbox_scope | one row per mailbox | RC-05, RC-01 |
# MAGIC | fact_migration_readiness | one row per (object_type, environment, snapshot_date) | RC-01 |
# MAGIC
# MAGIC **ETL notes:**
# MAGIC - `group_memberships.group_silver_id` joins to `groups.group_id` (name mismatch by design)
# MAGIC - `user_mapping_candidates` / `group_mapping_candidates` have NO `environment` column
# MAGIC - `fact_mailbox_scope` best mapping candidate picked via ROW_NUMBER (score DESC, created_at DESC)
# MAGIC - DQ-01: all license flags currently FALSE - confirm ingestion before RC-03

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_user

# COMMAND ----------


@dlt.table(
    name="dim_user",
    comment=(
        "Conformed user dimension. Grain: one row per user. "
        "Source: silver.users joined to silver.mailboxes (left) and silver.spo_sites (left, "
        "personal sites only). OneDrive matching uses an equality join on (environment, source_key) and a personal-tag "
        "derived from the URL's '/personal/<token>' segment vs the lower-cased UPN with "
        "'@' and '.' replaced by '_'. "
        "Single source of truth for user attributes (PK: user_silver_id). "
        "Reports: RC-01 RC-02 RC-03."
    ),
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "identity",
        "pipelines.autoOptimize.managed": "true",
    },
)
def dim_user():
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")
    mailboxes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mailboxes")

    # Personal OneDrive sites from silver.spo_sites. Build a derived join key
    # ("personal tag") from the URL suffix so we can equality-join instead of
    # using a LIKE predicate. The tag is the lowercase token after '/personal/'.
    personal_sites = (
        spark.table(f"{CATALOG}.{SILVER_SCHEMA}.spo_sites")
        .filter(col("is_personal_site") == lit(True))
        .select(
            col("source_key").alias("od_source_key"),
            col("environment").alias("od_environment"),
            col("web_url").alias("od_web_url"),
            col("storage_used_mb").alias("od_storage_used_mb"),
            col("storage_quota_mb").alias("od_storage_quota_mb"),
            col("site_silver_id").alias("od_site_silver_id"),
            lower(regexp_extract(col("web_url"), "/personal/([^/]+)$", 1)).alias("od_personal_tag"),
        )
        .filter(col("od_personal_tag") != lit(""))
    )

    # Resolve org_id only for source-environment users (target users intentionally NULL).
    source_users = users.filter(col("environment") == "source")
    non_source_users = users.filter(col("environment") != "source")
    users_org = _resolve_user_org_id(source_users).unionByName(non_source_users, allowMissingColumns=True)

    license_count_expr = (
        coalesce(col("u.has_e1_license").cast("int"), lit(0))
        + coalesce(col("u.has_e3_license").cast("int"), lit(0))
        + coalesce(col("u.has_e5_license").cast("int"), lit(0))
        + coalesce(col("u.has_f3_license").cast("int"), lit(0))
    )

    has_any = (
        coalesce(col("u.has_e1_license"), lit(False))
        | coalesce(col("u.has_e3_license"), lit(False))
        | coalesce(col("u.has_e5_license"), lit(False))
        | coalesce(col("u.has_f3_license"), lit(False))
    )

    return (
        users_org.alias("u")
        .join(
            mailboxes.alias("m"),
            (col("u.environment") == col("m.environment")) & (col("u.mail") == col("m.primary_smtp_address")),
            "left",
        )
        .join(
            personal_sites,
            (col("u.environment") == col("od_environment"))
            & (col("u.source_key") == col("od_source_key"))
            & (lower(expr("replace(replace(u.user_principal_name, '@', '_'), '.', '_')")) == col("od_personal_tag")),
            "left",
        )
        .select(
            md5(col("u.user_id")).alias("user_silver_id"),
            col("u.user_id"),
            col("u.entra_object_id"),
            col("u.environment"),
            col("u.source_key"),
            col("u.user_principal_name"),
            col("u.display_name"),
            col("u.given_name"),
            col("u.surname"),
            col("u.mail"),
            col("u.proxy_addresses"),
            col("u.user_type"),
            col("u.account_enabled"),
            col("u.employee_id"),
            col("u.job_title"),
            col("u.department"),
            col("u.company_name"),
            col("u.office_location"),
            col("u.on_prem_immutable_id"),
            col("u.on_prem_sam_account_name"),
            col("u.on_prem_sync_enabled"),
            col("u.on_prem_domain_name"),
            col("u.has_e1_license"),
            col("u.has_e3_license"),
            col("u.has_e5_license"),
            col("u.has_f3_license"),
            has_any.alias("has_any_license"),
            license_count_expr.cast("long").alias("license_count"),
            col("m.mailbox_id"),
            col("m.recipient_type").alias("recipient_type_details"),
            # Sprint 2: size/count metrics move to exo_mailbox_statistics — join on exchange_guid
            lit(None).cast("decimal(18,2)").alias("mailbox_size_mb"),
            lit(None).cast("long").alias("mailbox_item_count"),
            when(col("m.has_archive"), lit("Active")).otherwise(lit("None")).alias("archive_status"),
            lit(None).cast("decimal(18,2)").alias("archive_size_mb"),
            col("m.litigation_hold").alias("litigation_hold_enabled"),
            when(col("m.mailbox_id").isNotNull(), lit(True)).otherwise(lit(False)).alias("has_mailbox"),
            coalesce(col("m.has_archive"), lit(False)).alias("has_archive"),
            # OneDrive fields — derived from silver.spo_sites (is_personal_site = true).
            # FK to dim_spo_site.site_silver_id; storage in MB granularity (silver doesn't carry bytes).
            col("od_site_silver_id").alias("onedrive_site_silver_id"),
            col("od_web_url").alias("onedrive_url"),
            col("od_storage_used_mb").cast("decimal(18,2)").alias("onedrive_size_mb"),
            (col("od_storage_used_mb") / lit(1024)).cast("decimal(18,2)").alias("onedrive_size_gb"),
            col("od_storage_quota_mb").cast("decimal(18,2)").alias("onedrive_quota_mb"),
            col("od_site_silver_id").isNotNull().alias("has_onedrive"),
            # mailbox_size_mb is currently NULL (Sprint 2 dependency); total_storage_mb is OneDrive-only for now.
            coalesce(col("od_storage_used_mb"), lit(0)).cast("decimal(18,2)").alias("total_storage_mb"),
            when(col("u.environment") == "source", col("u.org_id")).otherwise(lit(None).cast("string")).alias("org_id"),
            col("u.last_updated_at"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_group

# COMMAND ----------


@dlt.table(
    name="dim_group",
    comment=(
        "Conformed group dimension with pre-joined member/owner counts and mapping status. "
        "Grain: one row per group. "
        "Source: silver.groups + group_memberships + group_owners + group_mapping_candidates. "
        "Single source of truth for group attributes (PK: group_silver_id). "
        "Reports: RC-01."
    ),
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "identity",
        "pipelines.autoOptimize.managed": "true",
    },
)
def dim_group():
    groups = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.groups")
    memberships = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.group_memberships")
    owners = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.group_owners")
    mapping_cands = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.group_mapping_candidates")

    # group_memberships.group_silver_id = groups.group_id (name mismatch by design)
    member_counts = memberships.groupBy(col("group_silver_id").alias("grp_id")).agg(
        count("*").alias("member_count"),
        spark_sum(when(col("member_type") == "user", 1).otherwise(0)).alias("user_member_count"),
        spark_sum(when(col("member_type") == "group", 1).otherwise(0)).alias("group_member_count"),
    )

    owner_counts = owners.groupBy(col("group_silver_id").alias("grp_id")).agg(count("*").alias("owner_count"))

    # Best mapping candidate per source group (score DESC, candidate_id for tie-break)
    best_cand_window = Window.partitionBy("source_group_id").orderBy(col("match_score").desc(), col("candidate_id"))
    best_cand = (
        mapping_cands.withColumn("rn", row_number().over(best_cand_window))
        .filter(col("rn") == 1)
        .select(
            col("source_group_id").alias("mapped_group_id"),
            col("candidate_id").alias("mapping_candidate_id"),
            col("candidate_status").alias("mapping_candidate_status"),
            col("match_score").alias("mapping_match_score"),
        )
    )

    # Resolve org_id for each group via mail domain lookup
    dom = get_domain_org_map()
    dom_mail = dom.select(col("domain_value").alias("__d_mail"), col("org_id").alias("__org_mail"))

    return (
        groups.alias("g")
        .join(member_counts.alias("mc"), col("g.group_id") == col("mc.grp_id"), "left")
        .join(owner_counts.alias("oc"), col("g.group_id") == col("oc.grp_id"), "left")
        .join(best_cand.alias("bc"), col("g.group_id") == col("bc.mapped_group_id"), "left")
        .withColumn("__mail_dom", normalized_domain(col("g.mail")))
        .join(dom_mail, col("__mail_dom") == col("__d_mail"), "left")
        .select(
            md5(col("g.group_id")).alias("group_silver_id"),
            col("g.group_id"),
            col("g.entra_object_id"),
            col("g.environment"),
            col("g.source_key"),
            col("__org_mail").alias("org_id"),
            col("g.display_name"),
            col("g.description"),
            col("g.mail"),
            col("g.proxy_addresses"),
            col("g.group_type"),
            col("g.group_types"),
            col("g.mail_enabled"),
            col("g.security_enabled"),
            col("g.is_dynamic"),
            col("g.membership_rule"),
            coalesce(col("mc.member_count"), lit(0)).alias("member_count"),
            coalesce(col("mc.user_member_count"), lit(0)).alias("user_member_count"),
            coalesce(col("mc.group_member_count"), lit(0)).alias("group_member_count"),
            coalesce(col("oc.owner_count"), lit(0)).alias("owner_count"),
            (coalesce(col("oc.owner_count"), lit(0)) > 0).alias("has_owners"),
            col("bc.mapping_candidate_id"),
            when(col("bc.mapping_candidate_id").isNotNull(), lit("mapped"))
            .otherwise(lit("no_candidate"))
            .alias("mapping_status"),
            col("bc.mapping_candidate_status"),
            col("bc.mapping_match_score"),
            col("g.last_updated_at"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_user_mapping_analysis

# COMMAND ----------


@dlt.table(
    name="fact_user_mapping_analysis",
    comment=(
        "User mapping analysis fact. Grain: one row per candidate pair. "
        "Source: silver.user_mapping_candidates enriched with user attributes. "
        "Reports: RC-02 RC-01."
    ),
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "migration",
        "pipelines.autoOptimize.managed": "true",
    },
)
def fact_user_mapping_analysis():
    cands = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.user_mapping_candidates")
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")

    src_users = _resolve_user_org_id(users.filter(col("environment") == "source")).alias("src")
    tgt_users = users.filter(col("environment") == "target").alias("tgt")

    return (
        cands.alias("c")
        .join(src_users, col("c.source_user_id") == col("src.user_id"), "left")
        .join(tgt_users, col("c.target_user_id") == col("tgt.user_id"), "left")
        .select(
            col("c.candidate_id"),
            col("c.source_user_id"),
            col("src.org_id"),
            col("src.user_principal_name").alias("source_upn"),
            col("src.display_name").alias("source_display_name"),
            col("src.department").alias("source_department"),
            col("src.company_name").alias("source_company_name"),
            col("src.account_enabled").alias("source_account_enabled"),
            col("src.on_prem_sync_enabled").alias("source_on_prem_sync"),
            col("c.target_user_id"),
            col("tgt.user_principal_name").alias("target_upn"),
            col("tgt.display_name").alias("target_display_name"),
            col("c.match_type"),
            col("c.match_score"),
            col("c.match_attributes"),
            col("c.match_context"),
            col("c.candidate_status"),
            col("c.mapping_scenario"),
            col("c.source_count"),
            col("c.target_count"),
            ((coalesce(col("c.source_count"), lit(1)) > 1) | (coalesce(col("c.target_count"), lit(1)) > 1)).alias(
                "is_conflict"
            ),
            lower(trim(coalesce(col("c.candidate_status"), lit(""))))
            .isin("approved", "confirmed")
            .alias("is_confirmed"),
            (lower(trim(coalesce(col("c.candidate_status"), lit("")))) == "pending_review").alias("is_pending"),
            col("c.created_at").alias("candidate_created_at"),
            col("c.updated_at").alias("candidate_updated_at"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_mailbox_scope

# COMMAND ----------


@dlt.table(
    name="fact_mailbox_scope",
    comment=(
        "Mailbox migration scope enriched with user and mapping status. "
        "Grain: one row per mailbox. "
        "Source: silver.mailboxes + silver.users + silver.user_mapping_candidates. "
        "Reports: RC-05 RC-01."
    ),
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "exchange",
        "pipelines.autoOptimize.managed": "true",
    },
)
def fact_mailbox_scope():
    mailboxes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mailboxes")
    users = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.users")
    cands = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.user_mapping_candidates")

    # Resolve org_id for source users only (target users get NULL org_id)
    source_users = _resolve_user_org_id(users.filter(col("environment") == "source"))
    non_source_users = users.filter(col("environment") != "source").withColumn("org_id", lit(None).cast("string"))
    users_with_org = source_users.unionByName(non_source_users, allowMissingColumns=True)

    # Best candidate per source user: score DESC, created_at DESC for tie-breaking
    best_cand_window = Window.partitionBy("source_user_id").orderBy(col("match_score").desc(), col("created_at").desc())
    best_cands = (
        cands.withColumn("rn", row_number().over(best_cand_window))
        .filter(col("rn") == 1)
        .select(
            col("source_user_id").alias("cand_user_id"),
            col("candidate_id").alias("mapping_candidate_id"),
            col("candidate_status").alias("mapping_candidate_status"),
            col("match_score").alias("mapping_match_score"),
            col("mapping_scenario"),
        )
    )

    return (
        mailboxes.alias("m")
        .join(
            users_with_org.alias("u"),
            (col("u.user_principal_name") == col("m.user_principal_name"))
            & (col("u.environment") == col("m.environment")),
            "left",
        )
        .join(best_cands.alias("bc"), col("u.user_id") == col("bc.cand_user_id"), "left")
        .select(
            md5(col("m.mailbox_id")).alias("mailbox_scope_key"),
            col("m.mailbox_id"),
            col("m.environment"),
            col("m.source_key"),
            col("u.org_id"),
            col("m.display_name"),
            col("m.alias"),
            col("m.user_principal_name"),
            col("m.primary_smtp_address"),
            col("m.recipient_type"),
            col("m.has_archive"),
            col("m.litigation_hold"),
            col("m.litigation_hold_date"),
            col("u.user_id"),
            col("bc.mapping_candidate_id"),
            when(col("bc.mapping_candidate_id").isNotNull(), lit("mapped"))
            .otherwise(lit("no_candidate"))
            .alias("mapping_status"),
            col("bc.mapping_candidate_status"),
            col("bc.mapping_match_score"),
            col("bc.mapping_scenario"),
            (coalesce(col("m.has_archive"), lit(False)) | coalesce(col("m.litigation_hold"), lit(False))).alias(
                "needs_special_handling"
            ),
            col("m.source_created_at"),
            col("m.last_changed_at"),
            col("m.last_updated_at"),
            current_timestamp().alias("gold_loaded_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_migration_readiness

# COMMAND ----------


@dlt.table(
    name="fact_migration_readiness",
    comment=(
        "Pre-aggregated migration readiness KPIs. "
        "Grain: one row per (object_type, org_id, snapshot_date). "
        "org_id is coalesced to md5('__unassigned__') to avoid NULL org_id rows (e.g., users/groups without a resolvable org); "
        "a matching sentinel row exists in dim_organization so these rows remain FK-valid in Power BI. "
        "Source: LIVE.dim_user, LIVE.dim_group, LIVE.fact_user_mapping_analysis. "
        "Reports: RC-01."
    ),
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "domain": "migration",
        "pipelines.autoOptimize.managed": "true",
    },
)
def fact_migration_readiness():
    snapshot_dt = current_date().cast("timestamp")
    users = dlt.read("dim_user")
    groups = dlt.read("dim_group")
    mapping = dlt.read("fact_user_mapping_analysis")

    # ---- user rows: one row per org ----
    src_users = users.filter(col("environment") == "source").withColumn(
        "org_id", coalesce(col("org_id"), md5(lit("__unassigned__")))
    )

    user_agg = (
        src_users.groupBy("org_id")
        .agg(
            count("*").alias("total_count"),
            spark_sum(when(col("account_enabled") == True, 1).otherwise(0)).alias("enabled_count"),
        )
        .fillna(0, subset=["total_count", "enabled_count"])
    )

    mapping_with_org = mapping.withColumn("org_id", coalesce(col("org_id"), md5(lit("__unassigned__"))))

    mapping_agg = (
        mapping_with_org.groupBy("org_id")
        .agg(
            countDistinct("source_user_id").alias("distinct_mapped_sources"),
            count("*").alias("candidates_total"),
            spark_sum(when(col("is_confirmed") == True, 1).otherwise(0)).alias("candidates_confirmed"),
            spark_sum(when(col("is_pending") == True, 1).otherwise(0)).alias("candidates_pending"),
            spark_sum(when(col("candidate_status") == "rejected", 1).otherwise(0)).alias("candidates_rejected"),
            spark_sum(when(col("is_conflict") == True, 1).otherwise(0)).alias("conflict_count"),
            spark_sum(when((col("is_conflict") == False) & (col("is_confirmed") == True), 1).otherwise(0)).alias(
                "clean_match_count"
            ),
        )
        .fillna(
            0,
            subset=[
                "distinct_mapped_sources",
                "candidates_total",
                "candidates_confirmed",
                "candidates_pending",
                "candidates_rejected",
                "conflict_count",
                "clean_match_count",
            ],
        )
    )

    user_rows = (
        user_agg.join(mapping_agg, "org_id", "left")
        .fillna(
            0,
            subset=[
                "distinct_mapped_sources",
                "candidates_total",
                "candidates_confirmed",
                "candidates_pending",
                "candidates_rejected",
                "conflict_count",
                "clean_match_count",
            ],
        )
        .withColumn("object_type", lit("user"))
        .withColumn("snapshot_date", snapshot_dt)
        .withColumn("unmatched_count", col("total_count") - col("distinct_mapped_sources"))
        .withColumn(
            "mapping_coverage_pct",
            when(
                col("total_count") > 0, (col("distinct_mapped_sources") / col("total_count")).cast("double")
            ).otherwise(lit(0.0)),
        )
        .withColumn(
            "readiness_key",
            md5(
                concat_ws(
                    "|",
                    col("object_type"),
                    col("org_id"),
                    col("snapshot_date").cast("string"),
                )
            ),
        )
        .select(
            "readiness_key",
            "snapshot_date",
            "object_type",
            "org_id",
            "total_count",
            "enabled_count",
            "distinct_mapped_sources",
            "unmatched_count",
            "mapping_coverage_pct",
            "candidates_total",
            "candidates_confirmed",
            "candidates_pending",
            "candidates_rejected",
            "conflict_count",
            "clean_match_count",
            current_timestamp().alias("gold_loaded_at"),
        )
    )

    # ---- group rows: one row per org ----
    group_src = groups.filter(col("environment") == "source").withColumn(
        "org_id", coalesce(col("org_id"), md5(lit("__unassigned__")))
    )

    group_rows = (
        group_src.groupBy("org_id")
        .agg(
            count("*").alias("total_count"),
            spark_sum(when(col("has_owners") == True, 1).otherwise(0)).alias("enabled_count"),
            spark_sum(when(col("mapping_status") == "mapped", 1).otherwise(0)).alias("distinct_mapped_sources"),
        )
        .fillna(0, subset=["total_count", "enabled_count", "distinct_mapped_sources"])
        .withColumn("object_type", lit("group"))
        .withColumn("snapshot_date", snapshot_dt)
        .withColumn("unmatched_count", col("total_count") - col("distinct_mapped_sources"))
        .withColumn(
            "mapping_coverage_pct",
            when(
                col("total_count") > 0, (col("distinct_mapped_sources") / col("total_count")).cast("double")
            ).otherwise(lit(0.0)),
        )
        .withColumn("candidates_total", lit(0).cast("long"))
        .withColumn("candidates_confirmed", lit(0).cast("long"))
        .withColumn("candidates_pending", lit(0).cast("long"))
        .withColumn("candidates_rejected", lit(0).cast("long"))
        .withColumn("conflict_count", lit(0).cast("long"))
        .withColumn("clean_match_count", lit(0).cast("long"))
        .withColumn(
            "readiness_key",
            md5(
                concat_ws(
                    "|",
                    col("object_type"),
                    col("org_id"),
                    col("snapshot_date").cast("string"),
                )
            ),
        )
        .select(
            "readiness_key",
            "snapshot_date",
            "object_type",
            "org_id",
            "total_count",
            "enabled_count",
            "distinct_mapped_sources",
            "unmatched_count",
            "mapping_coverage_pct",
            "candidates_total",
            "candidates_confirmed",
            "candidates_pending",
            "candidates_rejected",
            "conflict_count",
            "clean_match_count",
            current_timestamp().alias("gold_loaded_at"),
        )
    )

    return user_rows.union(group_rows)
