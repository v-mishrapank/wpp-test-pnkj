# M365 Migration - Delta Live Tables (DLT) Pipeline Documentation

> **Purpose**: This document describes the business rules and logic implemented in the DLT pipelines for M365 migration analytics. Use this as a reference for team discussions and reviews.

---

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Pipeline Execution Order](#pipeline-execution-order)
3. [Bronze Layer (pl_bronze.py)](#bronze-layer)
4. [Silver Layer (pl_silver.py)](#silver-layer)
5. [Gold Layer (pl_gold.py)](#gold-layer)
6. [Audit Layer (pl_audit.py)](#audit-layer)
7. [Configuration Parameters](#configuration-parameters)
8. [Future Enhancements](#future-enhancements)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        M365 Migration DLT Pipeline                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ADLS Landing Zone          Bronze           Silver           Gold       │
│  ┌─────────────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐ │
│  │ raw/entra_users │───►│ entra_    │───►│  users    │───►│ dim_user  │ │
│  │   /source       │    │ users_    │    │           │    │           │ │
│  │   /target       │    │ source    │    │           │    │ dim_people│ │
│  └─────────────────┘    │           │    │           │    │           │ │
│                         │ entra_    │    │           │    │ migration │ │
│                         │ users_    │    │           │    │ _summary  │ │
│                         │ target    │    │           │    │           │ │
│                         └───────────┘    └───────────┘    └───────────┘ │
│                                                                          │
│                                          ┌───────────────────────────┐   │
│                                          │        Audit Schema        │   │
│                                          │  - ingestion_batches      │   │
│                                          │  - daily_record_counts    │   │
│                                          │  - data_quality_summary   │   │
│                                          └───────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Design Principles

| Layer | Purpose | Data Quality |
|-------|---------|--------------|
| **Bronze** | Raw data ingestion via Auto Loader | Append-only, no transformations |
| **Silver** | Cleansed, deduplicated, transformed | SCD Type 1, deterministic rules |
| **Gold** | Business-ready dimensions & aggregates | Derived using opinionated rules |
| **Audit** | Pipeline tracking & data quality | Monitoring & observability |

---

## Pipeline Execution Order

```
1. pl_bronze.py  → Ingests raw JSON from ADLS
2. pl_silver.py  → Transforms and deduplicates
3. pl_gold.py    → Creates business dimensions
4. pl_audit.py   → Generates audit metrics
```

---

## Bronze Layer

**File**: `pl_bronze.py`  
**Target Schema**: `m365_migration.bronze`

### Purpose
Ingest raw JSON files from Azure Data Lake Storage (ADLS) landing zone using Databricks Auto Loader.

### Tables

#### `entra_users_source`
Raw user data from **source tenant** (e.g., MADEV2)

```
INPUT:  abfss://bronze@{storage}/raw/entra_users/source/*.json
OUTPUT: bronze.entra_users_source (streaming/append-only)
```

**Pseudo Code:**
```
FOR EACH new JSON file in landing_zone/entra_users/source:
    READ file with Auto Loader (schema inference enabled)
    ADD metadata columns:
        _dlt_ingested_at = current_timestamp()
        _source_file = file path
        _source_system = "graph_api"
        _environment = "source"
    APPEND to entra_users_source table
```

#### `entra_users_target`
Raw user data from **target tenant** (e.g., MADEV1)

```
INPUT:  abfss://bronze@{storage}/raw/entra_users/target/*.json
OUTPUT: bronze.entra_users_target (streaming/append-only)
```

### Data Quality Rules
- None at Bronze layer (raw data preservation)

### Key Features
- **Schema Evolution**: New columns automatically added (`schemaEvolutionMode: addNewColumns`)
- **Multi-line JSON**: Supports array-based JSON files
- **Append-only**: Historical files preserved for auditing

---

## Silver Layer

**File**: `pl_silver.py`  
**Target Schema**: `m365_migration.silver`

### Purpose
Transform, cleanse, and deduplicate data from Bronze tables. Apply consistent business rules across environments.

### Configuration (Pipeline Parameters)

| Parameter | Description | Example |
|-----------|-------------|---------|
| `source_tenant_name` | Name of source tenant | "MADEV2" |
| `source_tenant_id` | Azure AD tenant ID | "xxxxxxxx-xxxx-..." |
| `target_tenant_name` | Name of target tenant | "MADEV1" |
| `target_tenant_id` | Azure AD tenant ID | "xxxxxxxx-xxxx-..." |

### License SKU Configuration

Default Microsoft 365 license SKU IDs (can be overridden via pipeline parameters):

| License | Default SKU ID |
|---------|----------------|
| E3 | `05e9a617-0261-4cee-bb44-138d3ef5d965` |
| E5 | `06ebc4ee-1bb5-47dd-8120-11324bc54e06` |
| F3 | `66b55226-6b4f-492c-910c-a3b7a3c9d993` |
| E1 | `18181a46-0d4e-45cd-891e-60aabd171b4e` |

### Tables

#### `users` (via `users_staged`)

Deduplicated user accounts from both source and target tenants.

**Pseudo Code:**
```
INPUT: bronze.entra_users_source, bronze.entra_users_target

STEP 1: UNION source and target streams

STEP 2: FOR EACH user record:
    TRANSFORM:
        user_id             = "{environment}_{entra_object_id}"
        user_principal_name = LOWERCASE(TRIM(userPrincipalName))
        mail                = LOWERCASE(TRIM(mail))
        display_name        = TRIM(displayName)
        proxy_addresses     = NORMALIZE(proxyAddresses)  -- remove prefixes, lowercase
        has_e3_license      = EXISTS(assignedLicenses WHERE skuId = E3_SKU)
        has_e5_license      = EXISTS(assignedLicenses WHERE skuId = E5_SKU)
        tenant_name         = LOOKUP(environment → tenant_name)

STEP 3: DATA QUALITY CHECKS:
    EXPECT_OR_DROP: entra_object_id IS NOT NULL
    EXPECT:         user_principal_name IS NOT NULL

STEP 4: APPLY CHANGES (SCD Type 1):
    KEY:         user_id
    SEQUENCE BY: last_updated_at
    BEHAVIOR:    Upsert (latest record wins)

OUTPUT: silver.users
```

**Business Rules:**
1. **User ID Generation**: `{environment}_{entra_object_id}` ensures uniqueness across tenants
2. **Email Normalization**: All emails lowercase, trimmed
3. **Proxy Address Cleaning**: Remove prefixes (smtp:, sip:, x500:, x400:), lowercase
4. **License Detection**: Check `assignedLicenses` array for specific SKU IDs
5. **Deduplication**: Most recent record per `user_id` wins (SCD Type 1)

**Output Schema:**
```
user_id             STRING    -- Composite key: environment_entra_id
entra_object_id     STRING    -- Original Entra ID
environment         STRING    -- "source" or "target"
tenant_name         STRING    -- Human-readable tenant name
user_principal_name STRING    -- Normalized UPN
display_name        STRING    -- Display name
mail                STRING    -- Primary email (lowercase)
proxy_addresses     ARRAY     -- Normalized proxy addresses
user_type           STRING    -- "Member" or "Guest"
account_enabled     BOOLEAN   -- Is account active?
employee_id         STRING    -- HR employee identifier
job_title           STRING    -- Job title
department          STRING    -- Department
company_name        STRING    -- Company
assigned_licenses   ARRAY     -- Array of license SKU IDs
has_e3_license      BOOLEAN   -- Has Microsoft 365 E3?
has_e5_license      BOOLEAN   -- Has Microsoft 365 E5?
source_created_at   TIMESTAMP -- When created in Entra
last_updated_at     TIMESTAMP -- DLT processing timestamp
```

---

## Gold Layer

**File**: `pl_gold.py`  
**Target Schema**: `m365_migration.gold`

### Purpose
Create business-ready dimensions for migration planning. Apply opinionated derivation rules.

### Tables

#### `dim_user`

User dimension with placeholders for enrichment data.

**Pseudo Code:**
```
INPUT: silver.users

FOR EACH user:
    COPY all Silver columns
    ADD placeholder columns:
        person_id        = NULL  -- Will be linked from dim_people
        cdo_name         = NULL  -- Requires CDO data
        mailbox_size_mb  = NULL  -- Requires mailbox data
        archive_size_mb  = NULL  -- Requires archive data
        onedrive_size_mb = NULL  -- Requires OneDrive data

OUTPUT: gold.dim_user
```

#### `dim_people` ⭐ (Key Business Logic)

Canonical person identities derived from users using configurable rules.

**Business Problem Solved:**
> A person may have multiple accounts across source and target tenants. We need to identify "the same person" and designate a primary account.

**Pseudo Code:**
```
INPUT: silver.users

STEP 1: CREATE MATCH KEY
    match_key = COALESCE(
        employee_id,      -- First priority: HR identifier
        mail,             -- Second priority: email
        user_principal_name  -- Fallback: UPN
    )
    FILTER OUT: records with NULL match_key

STEP 2: APPLY DERIVATION RULES (Priority Order)
    ┌──────────┬─────────────────────────────────────────────────┐
    │ Priority │ Rule                                            │
    ├──────────┼─────────────────────────────────────────────────┤
    │    1     │ E5 License Holder AND user_type = "Member"      │
    │    2     │ E3 License Holder AND user_type = "Member"      │
    │    3     │ Has Employee ID AND user_type = "Member"        │
    │    4     │ Active Member (user_type = "Member" AND enabled)│
    │   99     │ Default (fallback)                              │
    └──────────┴─────────────────────────────────────────────────┘

    FOR EACH user:
        derivation_priority = CASE
            WHEN has_e5_license AND user_type = "Member" THEN 1
            WHEN has_e3_license AND user_type = "Member" THEN 2
            WHEN employee_id IS NOT NULL AND user_type = "Member" THEN 3
            WHEN user_type = "Member" AND account_enabled THEN 4
            ELSE 99
        END

STEP 3: RANK USERS WITHIN EACH MATCH KEY GROUP
    PARTITION BY: match_key
    ORDER BY: derivation_priority ASC, last_updated_at DESC
    
    primary_user = FIRST user in each group (rank = 1)

STEP 4: AGGREGATE ACROSS ALL ACCOUNTS IN GROUP
    all_user_ids        = COLLECT_LIST(user_id)
    source_account_count = COUNT WHERE environment = "source"
    target_account_count = COUNT WHERE environment = "target"

STEP 5: GENERATE PERSON RECORD
    person_id         = "person_{match_key}"
    display_name      = primary_user.display_name
    given_name        = FIRST word of display_name
    surname           = REMAINING words of display_name
    primary_email     = primary_user.mail
    employee_id       = primary_user.employee_id
    primary_user_id   = primary_user.user_id
    derivation_rule   = rule that selected this user
    has_source_account = source_account_count > 0
    has_target_account = target_account_count > 0

OUTPUT: gold.dim_people
```

**Output Schema:**
```
person_id              STRING    -- "person_{match_key}"
display_name           STRING    -- From primary user
given_name             STRING    -- First word of display name
surname                STRING    -- Remaining words of display name
primary_email          STRING    -- From primary user
employee_id            STRING    -- From primary user
primary_user_id        STRING    -- Reference to dim_user
primary_environment    STRING    -- "source" or "target"
cdo_name               STRING    -- Placeholder
derivation_rule        STRING    -- Which rule selected this user
derived_at             TIMESTAMP -- When person was derived
has_source_account     BOOLEAN   -- Exists in source tenant?
has_target_account     BOOLEAN   -- Exists in target tenant?
source_account_count   INT       -- Number of source accounts
target_account_count   INT       -- Number of target accounts
last_updated_at        TIMESTAMP -- Processing timestamp
```

#### `migration_summary`

High-level migration metrics by environment.

**Pseudo Code:**
```
INPUT: silver.users

GROUP BY: environment

AGGREGATE:
    user_count         = COUNT(*)
    enabled_user_count = SUM(account_enabled)
    e3_license_count   = SUM(has_e3_license)
    e5_license_count   = SUM(has_e5_license)
    
ADD placeholders:
    group_count    = 0  -- Until groups available
    contact_count  = 0  -- Until contacts available
    mailbox_count  = 0  -- Until mailboxes available

OUTPUT: gold.migration_summary
```

#### `people_summary`

Aggregate people statistics for migration planning.

**Pseudo Code:**
```
INPUT: gold.dim_people

AGGREGATE:
    total_people         = COUNT(*)
    people_with_source   = COUNT WHERE has_source_account
    people_with_target   = COUNT WHERE has_target_account
    people_with_both     = COUNT WHERE has_source AND has_target
    people_source_only   = COUNT WHERE has_source AND NOT has_target
    people_target_only   = COUNT WHERE NOT has_source AND has_target

OUTPUT: gold.people_summary
```

---

## Audit Layer

**File**: `pl_audit.py`  
**Target Schema**: `m365_migration.audit`

### Purpose
Track pipeline execution and data quality metrics.

### Tables

#### `ingestion_batches`

Track each ingestion batch from Azure Container Apps Jobs.

```
INPUT: bronze.entra_users_source

SELECT DISTINCT:
    batch_id
    source_ingested_at
    source_record_count
    dlt_ingested_at
    source_file
    environment
    entity_type = "entra_users_source"
```

#### `daily_record_counts`

Daily snapshot of record counts by entity.

```
INPUT: silver.users

GROUP BY: environment

OUTPUT:
    snapshot_date  = DATE(current_timestamp)
    entity_type    = "users"
    environment
    record_count   = COUNT(*)
```

#### `data_quality_summary`

Data quality metrics from Silver tables.

**Pseudo Code:**
```
INPUT: silver.users

GROUP BY: environment

CALCULATE:
    total_records          = COUNT(*)
    records_with_email     = COUNT WHERE mail IS NOT NULL
    records_with_employee_id = COUNT WHERE employee_id IS NOT NULL
    enabled_accounts       = SUM(account_enabled)
    
DERIVE:
    pct_with_email       = (records_with_email / total_records) * 100
    pct_with_employee_id = (records_with_employee_id / total_records) * 100
```

---

## Configuration Parameters

### Pipeline Configuration (databricks.yml)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `storage_account` | ADLS storage account name | `stm365analyticsdev001` |
| `catalog` | Unity Catalog name | `m365_migration` |
| `source_tenant_name` | Source tenant name | (required) |
| `source_tenant_id` | Source tenant Azure AD ID | (required) |
| `target_tenant_name` | Target tenant name | (required) |
| `target_tenant_id` | Target tenant Azure AD ID | (required) |

### License SKU Overrides (Optional)

| Parameter | Description |
|-----------|-------------|
| `license_sku_e3` | Override E3 SKU ID |
| `license_sku_e5` | Override E5 SKU ID |
| `license_sku_f3` | Override F3 SKU ID |
| `license_sku_e1` | Override E1 SKU ID |
| `license_skus_json` | JSON object with custom SKU mappings |

---

## Future Enhancements

### Planned Tables (Bronze)
- `entra_groups_source` / `entra_groups_target`
- `entra_contacts_source` / `entra_contacts_target`

### Planned Tables (Silver)
- `groups` - Cleansed group records
- `contacts` - Cleansed contact records
- `contact_entity_mapping` - System-level contact-to-user/group mapping
- `user_mapping_candidates` - Fuzzy matching candidates between tenants

### Planned Tables (Gold)
- `dim_group` - Group dimension
- `dim_contacts` - Contact dimension
- `person_map` - Confirmed person mappings
- `group_map` - Confirmed group mappings
- `non_person_map` - Shared mailboxes, rooms, etc.

### Planned Enrichments
- Mailbox sizes from Exchange Online
- OneDrive sizes from SharePoint
- CDO (Chief Data Officer) mappings

---

## Discussion Points for Team

1. **People Derivation Rules**: Are the priority rules correct for your organization?
   - Should E5 always take precedence over E3?
   - Are there other criteria to consider?

2. **Match Key Logic**: Is `employee_id > mail > UPN` the right priority for matching people across tenants?

3. **Name Parsing**: The current logic assumes "FirstName LastName" format. Does this work for your data?

4. **License SKUs**: Are the default SKU IDs correct, or do we need custom SKUs?

5. **Data Quality Thresholds**: What percentage thresholds should trigger alerts?

---

*Document generated from DLT pipeline source code. Last updated: January 2026*
