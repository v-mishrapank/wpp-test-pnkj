# Databricks notebook source
# MAGIC %md
# MAGIC # MA Toolkit - Silver Layer Pipeline
# MAGIC
# MAGIC **Pipeline:** `pl_ma_toolkit_silver`
# MAGIC **Target Schema:** `{catalog}.silver`
# MAGIC
# MAGIC ## Purpose
# MAGIC Reads from Bronze tables (one row per entity record, `_record` = raw JSON string),
# MAGIC parses with explicit StructType schemas, cleanses/normalises, and deduplicates
# MAGIC using APPLY CHANGES (SCD Type 1).
# MAGIC
# MAGIC ## Bronze schema (per row)
# MAGIC | Column | Type | Notes |
# MAGIC |---|---|---|
# MAGIC | `source_type` | STRING | Always "tenant" (hardcoded in envelope) |
# MAGIC | `source_key` | STRING | Tenant key, e.g. "madev1" — replaces old tenant_name |
# MAGIC | `batch_id` | STRING | Run UUID |
# MAGIC | `ingested_at` | STRING | ISO-8601 envelope timestamp |
# MAGIC | `_record` | STRING | Raw JSON of the entity payload |
# MAGIC | `_dlt_ingested_at` | TIMESTAMP | DLT processing time |
# MAGIC | `_source_file` | STRING | ADLS path |
# MAGIC | `_source_system` | STRING | "graph_api" etc. |
# MAGIC
# MAGIC ## Silver Tables
# MAGIC | Table | Bronze Source | Notes |
# MAGIC |---|---|---|
# MAGIC | `users` | `entra_users` | Cleansed user identities with license flags |
# MAGIC | `contacts` | `entra_contacts` | Org contacts (external mail recipients) |
# MAGIC | `groups` | `entra_groups` | Security, M365, distribution groups |
# MAGIC | `group_memberships` | `entra_group_members` | Group membership (one row per member) |
# MAGIC | `group_owners` | `entra_group_owners` | Group owner membership (one row per owner) |
# MAGIC | `mailboxes` | `exo_mailboxes` | Exchange mailboxes |
# MAGIC | `exo_contacts` | `exo_contacts` | EXO mail contacts (target) |
# MAGIC | `mail_users` | `exo_mail_users` | EXO mail users / shadow objects (both tenants) |
# MAGIC | `exo_unified_groups` | `exo_unified_groups` | EXO-side metadata for M365 Unified Groups (group mailbox; sender-auth, IB mode, SP URLs, ManagedBy). Joined via `entra_object_id` to `silver.groups`. |
# MAGIC | `exo_distribution_groups` | `exo_distribution_groups` | EXO-side metadata for traditional distribution lists and mail-enabled security groups (sender-auth, recipient subtype). Joined via `entra_object_id` to `silver.groups`. #53. |
# MAGIC | `spo_sites` | `spo_sites` + `spo_site_details` | SPO + OneDrive sites (isPersonalSite=True for OD). Graph URL list joined to admin-REST SiteProperties for storage/lock/template fields. Owner identity (email/login/display + `owner_entra_object_id` resolved per-environment against `silver.users`). #485 T3. |
# MAGIC | `spo_site_users` | `spo_site_users` | Site-collection user principals (one row per (site, user)). Carries `principal_type_parsed` + AAD identity columns from `LoginName` claim parser. #453. |
# MAGIC | `spo_site_group_users` | `spo_site_groups` | Exploded site-group memberships (one row per (site, sp-group, user)). Member `LoginName` parsed into `principal_type_parsed` + AAD identity columns. #465 T4. #453. |
# MAGIC | `spo_webs` | `spo_webs` | SP webs (subsites). One site can have many webs. |
# MAGIC | `spo_web_role_definitions` | `spo_web_role_definitions` | Role definitions (e.g. Full Control) scoped to a web. |
# MAGIC | `spo_web_role_assignments` | `spo_web_role_assignments` | Exploded principal↔role-binding assignments per web (one row per (web, principal, role)). Member `LoginName` parsed into `principal_type_parsed` + AAD identity columns. #465 T4. #453. |
# MAGIC | `spo_web_lists` | `spo_web_lists` | Lists/libraries on a web. |
# MAGIC | `spo_web_item_principals` | `spo_web_item_permissions` | Exploded direct principal grants on items with unique permissions (one row per (item, principal, role)). `LoginName` parsed into `principal_type_parsed` + AAD identity columns. #465 T4. #453. |
# MAGIC | `spo_web_item_links` | `spo_web_item_permissions` | Exploded sharing-link entries on items with unique permissions (one row per (item, sharing-link)). #465 T4. |
# MAGIC | `devices` | `entra_devices` | Entra-registered Windows devices (source) |
# MAGIC | `contact_entity_mapping` | derived | EXO contact chain resolution |
# MAGIC | `user_mapping_candidates` | derived | Multi-key identity matching with confidence scores |
# MAGIC | `group_mapping_candidates` | derived | Group matching by mail, proxy, display name |
# MAGIC | `mto_user_entity_mapping` | derived | EXO Mail User → Entra MTO user FK join |
# MAGIC | `ad_users` | `ad_users` | AD user accounts (Sprint 2) |
# MAGIC | `ad_contacts` | `ad_contacts` | AD mail contacts (Sprint 2) |
# MAGIC | `organizations` | config JSON | CDO org registry from organizations.json. Batch read from ADLS — no bronze source. Set `organizations_config_path` pipeline param. |
# MAGIC
# MAGIC ## Dependencies
# MAGIC - Requires `pl_ma_toolkit_bronze` to run first

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col,
    lit,
    lower,
    explode,
    current_timestamp,
    array_contains,
    when,
    concat_ws,
    trim,
    expr,
    to_timestamp,
    regexp_replace,
    collect_set,
    max as spark_max,
    coalesce,
    round as spark_round,
    from_json,
    md5,
    row_number,
    regexp_extract,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, IntegerType, LongType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

try:
    CATALOG = spark.conf.get("catalog")
except Exception:
    CATALOG = spark.catalog.currentCatalog()
    print(f"WARNING: catalog not configured in DLT pipeline parameters. " f"Defaulting to current catalog '{CATALOG}'.")

BRONZE_SCHEMA = "bronze"


def get_tenant_roles():
    """
    Returns two frozensets of source_key values: source-environment and target-environment.
    Configure via DLT pipeline parameters:
      source_tenant_names: comma-separated source_key values, e.g. "madev2"
      target_tenant_names: comma-separated source_key values, e.g. "madev1"
    Used to derive the environment column (source/target) from source_key.
    """
    try:
        source = spark.conf.get("source_tenant_names", "")
        target = spark.conf.get("target_tenant_names", "")
    except Exception:
        source, target = "", ""
    source_set = frozenset(t.strip() for t in source.split(",") if t.strip())
    target_set = frozenset(t.strip() for t in target.split(",") if t.strip())
    if not source_set:
        print("WARNING: source_tenant_names not configured. Set in DLT pipeline parameters.")
    return source_set, target_set


SOURCE_TENANT_NAMES, TARGET_TENANT_NAMES = get_tenant_roles()

# Microsoft 365 license SKU IDs
# Reference: https://learn.microsoft.com/en-us/entra/identity/users/licensing-service-plan-reference
# Each tier maps to a list of skuId GUIDs. Multiple GUIDs per tier cover both the
# Microsoft 365 (SPE_*) and Office 365 (ENTERPRISEPACK/DESKLESSPACK/...) SKU variants
# plus legacy editions still seen in tenants.
DEFAULT_LICENSE_SKUS = {
    # Office 365 E1 — STANDARDPACK (no Microsoft 365 E1 SKU exists)
    "E1": [
        "18181a46-0d4e-45cd-891e-60aabd171b4e",  # Office 365 E1 (STANDARDPACK)
    ],
    # Office 365 E2 — STANDARDWOFFPACK (legacy; rarely seen but distinct GUID)
    "E2": [
        "6634e0ce-1a9f-428c-a498-f84ec7b8aa2e",  # Office 365 E2 (STANDARDWOFFPACK)
    ],
    # Microsoft 365 E3 + Office 365 E3 + Office 365 E3 Developer
    "E3": [
        "05e9a617-0261-4cee-bb44-138d3ef5d965",  # Microsoft 365 E3 (SPE_E3)
        "6fd2c87f-b296-42f0-b197-1e91e994b900",  # Office 365 E3 (ENTERPRISEPACK)
        "189a915c-fe4f-4ffa-bde4-85b9628d07a0",  # Office 365 E3 Developer (DEVELOPERPACK)
    ],
    # Office 365 E4 — ENTERPRISEWITHSCAL (legacy retired SKU; included for completeness)
    "E4": [
        "1392051d-0cb9-4b7a-88d5-621fee5e8711",  # Office 365 E4 (ENTERPRISEWITHSCAL)
    ],
    # Microsoft 365 E5 + Office 365 E5 (incl. NoPSTNConf, E5 Developer, and EU-unbundled "no Teams" variants)
    "E5": [
        "06ebc4ee-1bb5-47dd-8120-11324bc54e06",  # Microsoft 365 E5 (SPE_E5)
        "c7df2760-2c81-4ef7-b578-5b5392b571df",  # Office 365 E5 (ENTERPRISEPREMIUM)
        "26d45bd9-adf1-46cd-a9e1-51e9a5524128",  # Office 365 E5 without Audio Conferencing (ENTERPRISEPREMIUM_NOPSTNCONF)
        "c42b9cae-ea4f-4ab7-9717-81576235ccac",  # Microsoft 365 E5 Developer (DEVELOPERPACK_E5)
        "18a4bd3f-0b5b-4887-b04f-61dd0ee15f5e",  # Microsoft 365 E5 (no Teams) — Microsoft_365_E5_(no_Teams)
    ],
    # Microsoft 365 F1 (frontline; includes M365_F1 commercial + M365_F1_COMM legacy)
    "F1": [
        "44575883-256e-4a79-9da4-ebe9acabe2b2",  # Microsoft 365 F1 (M365_F1)
        "50f60901-3181-4b75-8a2c-4c8e4c1d5a72",  # Microsoft 365 F1 (M365_F1_COMM legacy)
    ],
    # Microsoft 365 F3 + Office 365 F3
    "F3": [
        "66b55226-6b4f-492c-910c-a3b7a3c9d993",  # Microsoft 365 F3 (SPE_F1 — note: stringId is SPE_F1 but product name is F3)
        "4b585984-651b-448a-9e53-3b10f069cf7f",  # Office 365 F3 (DESKLESSPACK)
    ],
    # Microsoft 365 F5 — only ships as add-ons (Compliance / Security / Security+Compliance);
    # no "F5 base" SKU exists. Any of these add-ons indicates F5 entitlement.
    "F5": [
        "91de26be-adfa-4a3d-989e-9131cc23dda7",  # Microsoft 365 F5 Compliance Add-on (SPE_F5_COMP)
        "67ffe999-d9ca-49e1-9d2c-03fb28aa7a48",  # Microsoft 365 F5 Security Add-on (SPE_F5_SEC)
        "32b47245-eb31-44fc-b945-a8b1576c439f",  # Microsoft 365 F5 Security + Compliance Add-on (SPE_F5_SECCOMP)
    ],
}


def _split_csv(value: str) -> list:
    """Split a comma-separated SKU list from a pipeline param into a clean list."""
    return [v.strip() for v in value.split(",") if v.strip()]


def get_license_skus():
    """Retrieves license SKU config from pipeline parameters or defaults.

    Pipeline params per tier (`license_sku_e1`, `license_sku_e2`, ..., `license_sku_f5`)
    accept a comma-separated list of skuId GUIDs. `license_skus_json` accepts a JSON
    object mapping tier -> list of GUIDs and overrides everything else when set.
    """
    try:
        skus_json = spark.conf.get("license_skus_json", "")
        if skus_json:
            import json

            parsed = json.loads(skus_json)

            # Normalise keys/values from JSON (case-insensitive tier keys; scalar -> one-item list)
            normalized = {}
            for tier, val in parsed.items():
                tier_norm = str(tier).upper()
                vals = val if isinstance(val, list) else [val]
                normalized[tier_norm] = [str(v).strip() for v in vals if v is not None and str(v).strip()]

            # Merge onto defaults so partial JSON overrides don't break newly added tiers.
            result = {tier: (normalized.get(tier) or defaults) for tier, defaults in DEFAULT_LICENSE_SKUS.items()}

            # Preserve any extra tiers present in JSON for downstream consumers.
            for tier, vals in normalized.items():
                if tier not in result:
                    result[tier] = vals

            return result
        result = {}
        for tier, defaults in DEFAULT_LICENSE_SKUS.items():
            param_name = f"license_sku_{tier.lower()}"
            override = spark.conf.get(param_name, "")
            result[tier] = _split_csv(override) if override else defaults
        return result
    except Exception:
        return DEFAULT_LICENSE_SKUS


LICENSE_SKUS = get_license_skus()


def get_extension_attributes():
    """Retrieves extension attribute column names from DLT pipeline parameters.
    These map to the dynamic Graph API extension attributes (extension_{appId}_{name})
    configured per tenant in ADF / Key Vault.
    Returns empty string if not configured — coalesce will fall through to standard field.
    """
    try:
        ext_employee_id = spark.conf.get("extension_attribute_employee_id", "")
        ext_upn = spark.conf.get("extension_attribute_upn", "")
    except Exception:
        ext_employee_id = ""
        ext_upn = ""
    if ext_employee_id:
        print("Extension attribute for employee_id: [configured]")
    if ext_upn:
        print("Extension attribute for UPN: [configured]")
    return ext_employee_id, ext_upn


EXT_ATTR_EMPLOYEE_ID, EXT_ATTR_UPN = get_extension_attributes()

try:
    ORGANIZATIONS_CONFIG_PATH = spark.conf.get("organizations_config_path", "")
except Exception:
    ORGANIZATIONS_CONFIG_PATH = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity Schemas
# MAGIC
# MAGIC Explicit StructType schemas for `from_json(col("_record"), schema)`.
# MAGIC Fields absent in a given record are returned as null — no `ensure_columns` needed.
# MAGIC Schemas mirror entity SelectFields in the corresponding ingest module.

# COMMAND ----------


def _build_entra_user_schema(ext_employee_id: str = "", ext_upn: str = "") -> StructType:
    """Build entra_users _record schema, optionally including dynamic extension attribute fields."""
    fields = [
        StructField("id", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("mail", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("givenName", StringType(), True),
        StructField("surname", StringType(), True),
        StructField("mailNickname", StringType(), True),
        StructField("jobTitle", StringType(), True),
        StructField("department", StringType(), True),
        StructField("officeLocation", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("companyName", StringType(), True),
        StructField("usageLocation", StringType(), True),
        StructField("preferredLanguage", StringType(), True),
        StructField("mobilePhone", StringType(), True),
        StructField("businessPhones", ArrayType(StringType()), True),
        StructField("employeeId", StringType(), True),
        StructField("employeeType", StringType(), True),
        StructField("accountEnabled", BooleanType(), True),
        StructField("userType", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("lastPasswordChangeDateTime", StringType(), True),
        StructField("securityIdentifier", StringType(), True),
        StructField("externalUserState", StringType(), True),
        StructField("assignedLicenses", ArrayType(StructType([StructField("skuId", StringType(), True)])), True),
        StructField("proxyAddresses", ArrayType(StringType()), True),
        StructField("onPremisesSyncEnabled", BooleanType(), True),
        StructField("onPremisesLastSyncDateTime", StringType(), True),
        StructField("onPremisesDomainName", StringType(), True),
        StructField("onPremisesDistinguishedName", StringType(), True),
        StructField("onPremisesImmutableId", StringType(), True),
        StructField("onPremisesSamAccountName", StringType(), True),
        StructField("onPremisesSecurityIdentifier", StringType(), True),
        StructField("onPremisesUserPrincipalName", StringType(), True),
        StructField(
            "manager",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("displayName", StringType(), True),
                    StructField("userPrincipalName", StringType(), True),
                    StructField("mail", StringType(), True),
                ]
            ),
            True,
        ),
    ]
    if ext_employee_id:
        fields.append(StructField(ext_employee_id, StringType(), True))
    if ext_upn:
        fields.append(StructField(ext_upn, StringType(), True))
    return StructType(fields)


ENTRA_USER_SCHEMA = _build_entra_user_schema(EXT_ATTR_EMPLOYEE_ID, EXT_ATTR_UPN)

ENTRA_CONTACT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("givenName", StringType(), True),
        StructField("surname", StringType(), True),
        StructField("mail", StringType(), True),
        StructField("jobTitle", StringType(), True),
        StructField("department", StringType(), True),
        StructField("companyName", StringType(), True),
        StructField("proxyAddresses", ArrayType(StringType()), True),
        StructField("onPremisesSyncEnabled", BooleanType(), True),
        StructField("onPremisesLastSyncDateTime", StringType(), True),
    ]
)

ENTRA_GROUP_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("description", StringType(), True),
        StructField("mail", StringType(), True),
        StructField("mailNickname", StringType(), True),
        StructField("mailEnabled", BooleanType(), True),
        StructField("securityEnabled", BooleanType(), True),
        StructField("groupTypes", ArrayType(StringType()), True),
        StructField("membershipRule", StringType(), True),
        StructField("membershipRuleProcessingState", StringType(), True),
        StructField("onPremisesSyncEnabled", BooleanType(), True),
        StructField("onPremisesLastSyncDateTime", StringType(), True),
        StructField("onPremisesSamAccountName", StringType(), True),
        StructField("onPremisesSecurityIdentifier", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("proxyAddresses", ArrayType(StringType()), True),
        StructField("visibility", StringType(), True),
    ]
)

# entra_group_members._record: one row per member
# {"groupId":"...", "memberType":"user|group|...", "id":"...", "displayName":"...", "userPrincipalName":"...", "mail":"..."}
ENTRA_GROUP_MEMBER_SCHEMA = StructType(
    [
        StructField("groupId", StringType(), True),
        StructField("memberType", StringType(), True),
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("mail", StringType(), True),
    ]
)

# entra_group_owners._record: one row per owner (same shape as members minus memberType)
ENTRA_GROUP_OWNER_SCHEMA = StructType(
    [
        StructField("groupId", StringType(), True),
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("mail", StringType(), True),
    ]
)

ENTRA_DEVICE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("operatingSystem", StringType(), True),
        StructField("operatingSystemVersion", StringType(), True),
        StructField("trustType", StringType(), True),
        StructField("isManaged", BooleanType(), True),
        StructField("isCompliant", BooleanType(), True),
        StructField("accountEnabled", BooleanType(), True),
        StructField("approximateLastSignInDateTime", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("model", StringType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("profileType", StringType(), True),
        StructField("deviceCategory", StringType(), True),
        StructField("enrollmentProfileName", StringType(), True),
        StructField("onPremisesSyncEnabled", BooleanType(), True),
        StructField("onPremisesLastSyncDateTime", StringType(), True),
        StructField("onPremisesSecurityIdentifier", StringType(), True),
        StructField("mdmAppId", StringType(), True),
        StructField("registrationDateTime", StringType(), True),
    ]
)

# EXO objects use PascalCase field names (PowerShell cmdlet output)
EXO_CONTACTS_SCHEMA = StructType(
    [
        StructField("Guid", StringType(), True),
        StructField("DisplayName", StringType(), True),
        StructField("Alias", StringType(), True),
        StructField("PrimarySmtpAddress", StringType(), True),
        StructField("ExternalEmailAddress", StringType(), True),
        StructField("ExternalDirectoryObjectId", StringType(), True),
        StructField("RecipientTypeDetails", StringType(), True),
        StructField("IsDirSynced", BooleanType(), True),
        StructField("HiddenFromAddressListsEnabled", BooleanType(), True),
        StructField("SimpleDisplayName", StringType(), True),
        StructField("LegacyExchangeDN", StringType(), True),
        StructField("WindowsEmailAddress", StringType(), True),
        StructField("WhenCreated", StringType(), True),
        StructField("WhenChanged", StringType(), True),
    ]
)

EXO_MAIL_USERS_SCHEMA = StructType(
    [
        StructField("Guid", StringType(), True),
        StructField("DisplayName", StringType(), True),
        StructField("FirstName", StringType(), True),
        StructField("LastName", StringType(), True),
        StructField("Alias", StringType(), True),
        StructField("PrimarySmtpAddress", StringType(), True),
        StructField("ExternalEmailAddress", StringType(), True),
        StructField("ExternalDirectoryObjectId", StringType(), True),
        StructField("RecipientTypeDetails", StringType(), True),
        StructField("IsDirSynced", BooleanType(), True),
        StructField("HiddenFromAddressListsEnabled", BooleanType(), True),
        StructField("Company", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("WhenCreated", StringType(), True),
        StructField("WhenChanged", StringType(), True),
    ]
)

EXO_MAILBOXES_SCHEMA = StructType(
    [
        StructField("Guid", StringType(), True),
        StructField("ExchangeGuid", StringType(), True),
        StructField("ExternalDirectoryObjectId", StringType(), True),
        StructField("DisplayName", StringType(), True),
        StructField("Alias", StringType(), True),
        StructField("UserPrincipalName", StringType(), True),
        StructField("PrimarySmtpAddress", StringType(), True),
        StructField("EmailAddresses", ArrayType(StringType()), True),
        StructField("RecipientTypeDetails", StringType(), True),
        StructField("ArchiveStatus", StringType(), True),
        StructField("LitigationHoldEnabled", BooleanType(), True),
        StructField("LitigationHoldDate", StringType(), True),
        StructField("ForwardingAddress", StringType(), True),
        StructField("ForwardingSmtpAddress", StringType(), True),
        StructField("DeliverToMailboxAndForward", BooleanType(), True),
        StructField("WhenCreated", StringType(), True),
        StructField("WhenChanged", StringType(), True),
    ]
)

EXO_UNIFIED_GROUPS_SCHEMA = StructType(
    [
        StructField("AccessType", StringType(), True),
        StructField("ExchangeGuid", StringType(), True),
        StructField("ExternalDirectoryObjectId", StringType(), True),
        StructField("PrimarySmtpAddress", StringType(), True),
        StructField("Alias", StringType(), True),
        StructField("DisplayName", StringType(), True),
        StructField("Notes", StringType(), True),
        StructField("GroupSKU", StringType(), True),
        StructField("GroupType", StringType(), True),
        StructField("GroupMemberCount", IntegerType(), True),
        StructField("GroupExternalMemberCount", IntegerType(), True),
        StructField("AllowAddGuests", BooleanType(), True),
        StructField("HiddenFromAddressListsEnabled", BooleanType(), True),
        StructField("HiddenFromExchangeClientsEnabled", BooleanType(), True),
        StructField("HiddenGroupMembershipEnabled", BooleanType(), True),
        StructField("IsMailboxConfigured", BooleanType(), True),
        StructField("IsMembershipDynamic", BooleanType(), True),
        StructField("WelcomeMessageEnabled", BooleanType(), True),
        StructField("SubscriptionEnabled", BooleanType(), True),
        StructField("AutoSubscribeNewMembers", BooleanType(), True),
        StructField("Classification", StringType(), True),
        StructField("SensitivityLabel", StringType(), True),
        StructField("SharePointSiteUrl", StringType(), True),
        StructField("SharePointDocumentsUrl", StringType(), True),
        StructField("SharePointNotebookUrl", StringType(), True),
        StructField("RecipientType", StringType(), True),
        StructField("RecipientTypeDetails", StringType(), True),
        StructField("AuditLogAgeLimit", StringType(), True),
        StructField("WhenCreated", StringType(), True),
        StructField("WhenChanged", StringType(), True),
        StructField("WhenSoftDeleted", StringType(), True),
        StructField("ExpirationTime", StringType(), True),
        StructField("InformationBarrierMode", StringType(), True),
        StructField("ResourceProvisioningOptions", ArrayType(StringType()), True),
        StructField("ManagedBy", ArrayType(StringType()), True),
        StructField("RequireSenderAuthenticationEnabled", BooleanType(), True),
    ]
)

# bronze.exo_distribution_groups._record fields (Get-DistributionGroup output).
# Covers traditional distribution lists (DLs) AND mail-enabled security groups
# (MES). Distinguished by RecipientTypeDetails:
#   - MailUniversalDistributionGroup    → DL
#   - MailUniversalSecurityGroup        → mail-enabled security
#   - DynamicDistributionGroup          → dynamic DL
# Used by gold.groups_accept_external_email (#53) for the
# RequireSenderAuthenticationEnabled flag.
EXO_DISTRIBUTION_GROUPS_SCHEMA = StructType(
    [
        StructField("ExternalDirectoryObjectId", StringType(), True),
        StructField("ExchangeObjectId", StringType(), True),
        StructField("PrimarySmtpAddress", StringType(), True),
        StructField("Alias", StringType(), True),
        StructField("DisplayName", StringType(), True),
        StructField("GroupType", StringType(), True),
        StructField("RecipientType", StringType(), True),
        StructField("RecipientTypeDetails", StringType(), True),
        StructField("HiddenFromAddressListsEnabled", BooleanType(), True),
        StructField("ModerationEnabled", BooleanType(), True),
        StructField("RequireSenderAuthenticationEnabled", BooleanType(), True),
        StructField("MemberJoinRestriction", StringType(), True),
        StructField("MemberDepartRestriction", StringType(), True),
        StructField("WhenCreated", StringType(), True),
        StructField("WhenChanged", StringType(), True),
        StructField("ManagedBy", ArrayType(StringType()), True),
    ]
)

# spo_sites._record fields (post-#475 rewrite).
# Bronze _record follows the Microsoft Graph site shape (10 fields); this schema
# intentionally parses only the scalar fields used in silver (root/siteCollection omitted).
# Storage/lock/sharing/template/groupId/etc are sourced from bronze.spo_site_details
# (joined in silver.spo_sites — see SPO_SITE_DETAILS_SCHEMA below). #465 T3.
SPO_SITES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("webUrl", StringType(), True),
        StructField("description", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("lastModifiedDateTime", StringType(), True),
        StructField("isPersonalSite", BooleanType(), True),
        # root and siteCollection are nested structs; flattened in T3 if needed
    ]
)

# spo_site_details._record fields (bronze admin-REST GetSitePropertiesByUrl, #465 T3).
# The ingest module (SpoSites.psm1) lands every REST field verbatim — no SelectFields.
# This schema deliberately projects only the columns silver.spo_sites consumes.
# `Url` is the canonical SP key; `SiteUrl` is added by the fetcher as FK context
# and equals `Url` for site-collection rows.
SPO_SITE_DETAILS_SCHEMA = StructType(
    [
        StructField("Url", StringType(), True),
        StructField("SiteUrl", StringType(), True),
        StructField("Template", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("LockState", StringType(), True),
        StructField("IsHubSite", BooleanType(), True),
        StructField("HubSiteId", StringType(), True),
        StructField("GroupId", StringType(), True),
        StructField("RelatedGroupId", StringType(), True),
        StructField("TeamsChannelType", IntegerType(), True),
        StructField("IsTeamsConnected", BooleanType(), True),
        StructField("IsTeamsChannelConnected", BooleanType(), True),
        StructField("Lcid", StringType(), True),
        StructField("StorageMaximumLevel", StringType(), True),
        StructField("StorageUsage", StringType(), True),
        StructField("SharingCapability", IntegerType(), True),
        StructField("SensitivityLabel", StringType(), True),
        StructField("ArchiveStatus", StringType(), True),
        StructField("ArchivedFileDiskUsed", StringType(), True),
        StructField("LastContentModifiedDate", StringType(), True),
        # Owner identity fields — surfaced for OneDrive owner resolution against
        # silver.users (see spo_sites_staged). #485 T3.
        StructField("Owner", StringType(), True),
        StructField("OwnerEmail", StringType(), True),
        StructField("OwnerLoginName", StringType(), True),
        StructField("OwnerName", StringType(), True),
    ]
)

# spo_site_groups._record fields. Mirrors SpoSites.psm1 Get-SpoSiteGroups fetcher
# output (raw /_api/web/SiteGroups with embedded `Users` array). `SiteUrl` and
# `WebUrl` are added by the fetcher as FK context. The `Users` array drives the
# explosion in silver.spo_site_group_users. #465 T4.
SPO_SITE_GROUP_SCHEMA = StructType(
    [
        StructField("Id", IntegerType(), True),
        StructField("LoginName", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("PrincipalType", IntegerType(), True),
        StructField("Description", StringType(), True),
        StructField("OwnerTitle", StringType(), True),
        StructField("AllowMembersEditMembership", BooleanType(), True),
        StructField("AllowRequestToJoinLeave", BooleanType(), True),
        StructField("AutoAcceptRequestToJoinLeave", BooleanType(), True),
        StructField("OnlyAllowMembersViewMembership", BooleanType(), True),
        StructField("RequestToJoinLeaveEmailSetting", StringType(), True),
        StructField("SiteUrl", StringType(), True),
        StructField("WebUrl", StringType(), True),
        StructField(
            "Users",
            ArrayType(
                StructType(
                    [
                        StructField("Id", IntegerType(), True),
                        StructField("LoginName", StringType(), True),
                        StructField("Title", StringType(), True),
                        StructField("PrincipalType", IntegerType(), True),
                        StructField("Email", StringType(), True),
                        StructField("IsSiteAdmin", BooleanType(), True),
                        StructField("UserPrincipalName", StringType(), True),
                        StructField("Expiration", StringType(), True),
                        StructField("IsEmailAuthenticationGuestUser", BooleanType(), True),
                        StructField("IsShareByEmailGuestUser", BooleanType(), True),
                        StructField(
                            "UserId",
                            StructType(
                                [
                                    StructField("NameId", StringType(), True),
                                    StructField("NameIdIssuer", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
    ]
)

# spo_site_users._record fields. Mirrors the SpoSites.psm1 Get-SpoSiteUsers fetcher
# output (raw /_api/web/SiteUsers shape — no SelectFields). `SiteUrl` is added by
# the fetcher as FK context.
SPO_SITE_USER_SCHEMA = StructType(
    [
        StructField("Id", IntegerType(), True),
        StructField("LoginName", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("PrincipalType", IntegerType(), True),
        StructField("Email", StringType(), True),
        StructField("IsSiteAdmin", BooleanType(), True),
        StructField("UserPrincipalName", StringType(), True),
        StructField("IsHiddenInUI", BooleanType(), True),
        StructField("Expiration", StringType(), True),
        StructField("IsEmailAuthenticationGuestUser", BooleanType(), True),
        StructField("IsShareByEmailGuestUser", BooleanType(), True),
        StructField("SiteUrl", StringType(), True),
        StructField(
            "UserId",
            StructType(
                [
                    StructField("NameId", StringType(), True),
                    StructField("NameIdIssuer", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

# spo_webs._record fields. Mirrors the SpoSites.psm1 Get-SpoWebsRoot fetcher output
# (raw /_api/web shape — no SelectFields, 300+ fields land in bronze). This schema
# projects a sensible MVP subset; expand as new gold metrics demand more fields.
# `SiteUrl` is added by the fetcher as FK context to the parent site collection.
SPO_WEB_SCHEMA = StructType(
    [
        StructField("Id", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("Url", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Created", StringType(), True),
        StructField("LastItemModifiedDate", StringType(), True),
        StructField("LastItemUserModifiedDate", StringType(), True),
        StructField("WebTemplate", StringType(), True),
        StructField("Configuration", IntegerType(), True),
        StructField("Language", IntegerType(), True),
        StructField("MasterUrl", StringType(), True),
        StructField("CustomMasterUrl", StringType(), True),
        StructField("ServerRelativeUrl", StringType(), True),
        StructField("EnableMinimalDownload", BooleanType(), True),
        StructField("RecycleBinEnabled", BooleanType(), True),
        StructField("QuickLaunchEnabled", BooleanType(), True),
        StructField("TreeViewEnabled", BooleanType(), True),
        StructField("NoCrawl", BooleanType(), True),
        StructField("IsMultilingual", BooleanType(), True),
        StructField("IsRootWeb", BooleanType(), True),
        StructField("WelcomePage", StringType(), True),
        StructField("SiteUrl", StringType(), True),
        StructField("ParentWebUrl", StringType(), True),
    ]
)

# spo_web_item_permissions._record fields. Mirrors SpoSites.psm1 Get-SpoWebItemPermissions
# fetcher output (raw /_api/web/GetSharingInformation per item with unique perms). Bronze
# carries ~60 top-level fields verbatim; this schema deliberately projects only the
# identity/scope columns plus the `permissionsInformation.principals[]` and
# `permissionsInformation.links[]` arrays that drive the silver explosions
# (silver.spo_web_item_principals and silver.spo_web_item_links). #465 T4.
SPO_WEB_ITEM_PERMISSIONS_SCHEMA = StructType(
    [
        StructField("ItemId", IntegerType(), True),
        StructField("itemUniqueId", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("fileExtension", StringType(), True),
        StructField("FileRef", StringType(), True),
        StructField("directUrl", StringType(), True),
        StructField("itemUrl", StringType(), True),
        StructField("ListId", StringType(), True),
        StructField("WebUrl", StringType(), True),
        StructField("SiteUrl", StringType(), True),
        StructField("siteId", StringType(), True),
        StructField("hasUniquePermissions", BooleanType(), True),
        StructField("currentRole", IntegerType(), True),
        StructField("sharedObjectType", IntegerType(), True),
        StructField("FileSystemObjectType", IntegerType(), True),
        StructField(
            "permissionsInformation",
            StructType(
                [
                    StructField(
                        "principals",
                        ArrayType(
                            StructType(
                                [
                                    StructField("role", IntegerType(), True),
                                    StructField("isInherited", BooleanType(), True),
                                    StructField(
                                        "principal",
                                        StructType(
                                            [
                                                StructField("id", IntegerType(), True),
                                                StructField("loginName", StringType(), True),
                                                StructField("name", StringType(), True),
                                                StructField("email", StringType(), True),
                                                StructField("principalType", IntegerType(), True),
                                                StructField("userPrincipalName", StringType(), True),
                                                StructField("directoryObjectId", StringType(), True),
                                                StructField("isExternal", BooleanType(), True),
                                                StructField("isActive", BooleanType(), True),
                                                StructField("jobTitle", StringType(), True),
                                                StructField("expiration", StringType(), True),
                                            ]
                                        ),
                                        True,
                                    ),
                                ]
                            )
                        ),
                        True,
                    ),
                    StructField(
                        "links",
                        ArrayType(
                            StructType(
                                [
                                    StructField("isInherited", BooleanType(), True),
                                    StructField("totalLinkMembersCount", IntegerType(), True),
                                    StructField(
                                        "linkDetails",
                                        StructType(
                                            [
                                                StructField("AllowsAnonymousAccess", BooleanType(), True),
                                                StructField("ApplicationId", StringType(), True),
                                                StructField("BlocksDownload", BooleanType(), True),
                                                StructField("Created", StringType(), True),
                                                StructField("Description", StringType(), True),
                                                StructField("Embeddable", BooleanType(), True),
                                                StructField("Expiration", StringType(), True),
                                                StructField("HasExternalGuestInvitees", BooleanType(), True),
                                                StructField("IsActive", BooleanType(), True),
                                                StructField("IsAddressBarLink", BooleanType(), True),
                                                StructField("IsCreateOnlyLink", BooleanType(), True),
                                                StructField("IsDefault", BooleanType(), True),
                                                StructField("IsEditLink", BooleanType(), True),
                                                StructField("IsEphemeral", BooleanType(), True),
                                                StructField("IsFormsLink", BooleanType(), True),
                                                StructField("IsMainLink", BooleanType(), True),
                                                StructField("IsManageListLink", BooleanType(), True),
                                                StructField("IsReviewLink", BooleanType(), True),
                                                StructField("IsUnhealthy", BooleanType(), True),
                                                StructField("LastModified", StringType(), True),
                                                StructField("LimitUseToApplication", BooleanType(), True),
                                                StructField("LinkAclState", StringType(), True),
                                                StructField("LinkKind", IntegerType(), True),
                                                StructField("MeetingId", StringType(), True),
                                                StructField("MustAlwaysUseLink", BooleanType(), True),
                                                StructField("RequiresPassword", BooleanType(), True),
                                                StructField("RestrictedShareMembership", BooleanType(), True),
                                                StructField("RestrictToExistingRelationships", BooleanType(), True),
                                                StructField("Scope", IntegerType(), True),
                                                StructField("ShareId", StringType(), True),
                                                StructField("ShareTokenString", StringType(), True),
                                                StructField("SharingLinkStatus", IntegerType(), True),
                                                StructField("TrackLinkUsers", BooleanType(), True),
                                                StructField("Url", StringType(), True),
                                            ]
                                        ),
                                        True,
                                    ),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

# spo_web_role_assignments._record fields. Mirrors SpoSites.psm1 Get-SpoWebRoleAssignments
# fetcher output (raw /_api/web/RoleAssignments?$expand=Member,RoleDefinitionBindings).
# The RoleDefinitionBindings array drives the explosion in silver.spo_web_role_assignments
# (one row per (web, principal, role-binding)). `SiteUrl` and `WebUrl` are added by
# the fetcher as FK context. #465 T4.
SPO_WEB_ROLE_ASSIGNMENT_SCHEMA = StructType(
    [
        StructField("PrincipalId", IntegerType(), True),
        StructField(
            "Member",
            StructType(
                [
                    StructField("Id", IntegerType(), True),
                    StructField("LoginName", StringType(), True),
                    StructField("Title", StringType(), True),
                    StructField("PrincipalType", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "RoleDefinitionBindings",
            ArrayType(
                StructType(
                    [
                        StructField("Id", LongType(), True),
                        StructField("Name", StringType(), True),
                        StructField("Description", StringType(), True),
                        StructField("Hidden", BooleanType(), True),
                        StructField("Order", IntegerType(), True),
                        StructField("RoleTypeKind", IntegerType(), True),
                        StructField(
                            "BasePermissions",
                            StructType(
                                [
                                    StructField("High", StringType(), True),
                                    StructField("Low", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
        StructField("SiteUrl", StringType(), True),
        StructField("WebUrl", StringType(), True),
    ]
)

# spo_web_role_definitions._record fields. Mirrors SpoSites.psm1 Get-SpoWebRoleDefinitions
# fetcher output (raw /_api/web/RoleDefinitions). `SiteUrl` and `WebUrl` added by fetcher.
SPO_WEB_ROLE_DEFINITION_SCHEMA = StructType(
    [
        StructField("Id", LongType(), True),
        StructField("Name", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Hidden", BooleanType(), True),
        StructField("Order", IntegerType(), True),
        StructField("RoleTypeKind", IntegerType(), True),
        StructField(
            "BasePermissions",
            StructType(
                [
                    StructField("High", StringType(), True),
                    StructField("Low", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("SiteUrl", StringType(), True),
        StructField("WebUrl", StringType(), True),
    ]
)

# spo_web_lists._record fields. Mirrors SpoSites.psm1 Get-SpoWebLists fetcher output
# (raw /_api/web/Lists shape — no SelectFields). Heavy payload; projects an MVP subset.
# `SiteUrl` and `WebUrl` are added by the fetcher as FK context.
SPO_WEB_LIST_SCHEMA = StructType(
    [
        StructField("Id", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("BaseTemplate", IntegerType(), True),
        StructField("BaseType", IntegerType(), True),
        StructField("Hidden", BooleanType(), True),
        StructField("IsCatalog", BooleanType(), True),
        StructField("IsApplicationList", BooleanType(), True),
        StructField("IsPrivate", BooleanType(), True),
        StructField("ItemCount", LongType(), True),
        StructField("Created", StringType(), True),
        StructField("LastItemDeletedDate", StringType(), True),
        StructField("LastItemModifiedDate", StringType(), True),
        StructField("LastItemUserModifiedDate", StringType(), True),
        StructField("EnableAttachments", BooleanType(), True),
        StructField("EnableFolderCreation", BooleanType(), True),
        StructField("EnableModeration", BooleanType(), True),
        StructField("EnableVersioning", BooleanType(), True),
        StructField("EnableMinorVersions", BooleanType(), True),
        StructField("ForceCheckout", BooleanType(), True),
        StructField("MajorVersionLimit", IntegerType(), True),
        StructField("MajorWithMinorVersionsLimit", IntegerType(), True),
        StructField("NoCrawl", BooleanType(), True),
        StructField("EntityTypeName", StringType(), True),
        StructField("ParentWebUrl", StringType(), True),
        StructField("SiteUrl", StringType(), True),
        StructField("WebUrl", StringType(), True),
    ]
)

# AD objects use PascalCase (Get-ADUser / Get-ADContact output)
AD_USER_SCHEMA = StructType(
    [
        StructField("ObjectGUID", StringType(), True),
        StructField("DistinguishedName", StringType(), True),
        StructField("SamAccountName", StringType(), True),
        StructField("UserPrincipalName", StringType(), True),
        StructField("DisplayName", StringType(), True),
        StructField("GivenName", StringType(), True),
        StructField("Surname", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("Company", StringType(), True),
        StructField("Office", StringType(), True),
        StructField("EmployeeID", StringType(), True),
        StructField("Manager", StringType(), True),
        StructField("Enabled", BooleanType(), True),
        StructField("Name", StringType(), True),
        StructField("ObjectClass", StringType(), True),
        StructField("WhenCreated", StringType(), True),
        StructField("WhenChanged", StringType(), True),
    ]
)

AD_CONTACT_SCHEMA = StructType(
    [
        StructField("ObjectGUID", StringType(), True),
        StructField("DistinguishedName", StringType(), True),
        StructField("DisplayName", StringType(), True),
        StructField("GivenName", StringType(), True),
        StructField("sn", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("Company", StringType(), True),
        StructField("Mail", StringType(), True),
        StructField("TargetAddress", StringType(), True),
        StructField("ProxyAddresses", ArrayType(StringType()), True),
        StructField("Name", StringType(), True),
        StructField("ObjectClass", StringType(), True),
        StructField("WhenCreated", StringType(), True),
        StructField("WhenChanged", StringType(), True),
    ]
)

# Intune managed devices (beta/deviceManagement/managedDevices)
INTUNE_MANAGED_DEVICE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("deviceName", StringType(), True),
        StructField("managedDeviceOwnerType", StringType(), True),
        StructField("enrolledDateTime", StringType(), True),
        StructField("lastSyncDateTime", StringType(), True),
        StructField("operatingSystem", StringType(), True),
        StructField("complianceState", StringType(), True),
        StructField("jailBroken", StringType(), True),
        StructField("managementAgent", StringType(), True),
        StructField("osVersion", StringType(), True),
        StructField("azureADRegistered", BooleanType(), True),
        StructField("deviceEnrollmentType", StringType(), True),
        StructField("emailAddress", StringType(), True),
        StructField("azureADDeviceId", StringType(), True),
        StructField("deviceRegistrationState", StringType(), True),
        StructField("isEncrypted", BooleanType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("model", StringType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("serialNumber", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("userDisplayName", StringType(), True),
        StructField("totalStorageSpaceInBytes", LongType(), True),
        StructField("freeStorageSpaceInBytes", LongType(), True),
        StructField("managedDeviceName", StringType(), True),
        StructField("partnerReportedThreatState", StringType(), True),
        StructField("autopilotEnrolled", BooleanType(), True),
        StructField("isSupervised", BooleanType(), True),
    ]
)

# MDE devices (api.security.microsoft.com/api/machines) — no SelectFields defined
# in ingest module; full API payload landed. Fields sourced from MDE API docs.
MDE_DEVICE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("computerDnsName", StringType(), True),
        StructField("firstSeen", StringType(), True),
        StructField("lastSeen", StringType(), True),
        StructField("osPlatform", StringType(), True),
        StructField("osVersion", StringType(), True),
        StructField("osBuild", LongType(), True),
        StructField("lastIpAddress", StringType(), True),
        StructField("lastExternalIpAddress", StringType(), True),
        StructField("healthStatus", StringType(), True),
        StructField("riskScore", StringType(), True),
        StructField("exposureLevel", StringType(), True),
        StructField("onboardingStatus", StringType(), True),
        StructField("isAadJoined", BooleanType(), True),
        StructField("aadDeviceId", StringType(), True),
        StructField("machineTags", ArrayType(StringType()), True),
        StructField("defenderAvStatus", StringType(), True),
        StructField("rbacGroupName", StringType(), True),
        StructField("rbacGroupId", LongType(), True),
        StructField("deviceValue", StringType(), True),
        StructField("managedBy", StringType(), True),
        StructField("managedByStatus", StringType(), True),
    ]
)

# Teams (teams_teams root — from /v1.0/teams endpoint, PR #377).
# mail / visibility / createdDateTime are no longer returned by /v1.0/teams
# and are sourced from a LEFT JOIN against bronze.entra_groups in teams_staged (#378).
TEAMS_TEAM_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

# Teams team details (teams_team_details — rich Team payload from /v1.0/teams/{id})
TEAMS_TEAM_DETAILS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("description", StringType(), True),
        StructField("visibility", StringType(), True),
        StructField("isArchived", BooleanType(), True),
        StructField("isMembershipLimitedToOwners", BooleanType(), True),
        StructField("classification", StringType(), True),
        StructField("specialization", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("internalId", StringType(), True),
        StructField("webUrl", StringType(), True),
        StructField(
            "summary",
            StructType(
                [
                    StructField("ownersCount", IntegerType(), True),
                    StructField("membersCount", IntegerType(), True),
                    StructField("guestsCount", IntegerType(), True),
                ]
            ),
            True,
        ),
    ]
)

# Teams channels (teams_teams/*/*/channels)
TEAMS_CHANNEL_SCHEMA = StructType(
    [
        StructField("teamId", StringType(), True),
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("description", StringType(), True),
        StructField("membershipType", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("webUrl", StringType(), True),
        StructField("email", StringType(), True),
        StructField("isArchived", BooleanType(), True),
    ]
)

# Teams channel members (teams_teams/*/*/channel_members — private/shared only)
TEAMS_CHANNEL_MEMBER_SCHEMA = StructType(
    [
        StructField("teamId", StringType(), True),
        StructField("channelId", StringType(), True),
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("email", StringType(), True),
        StructField("roles", ArrayType(StringType()), True),
    ]
)

# Teams installed apps (teams_teams/*/*/installed_apps)
TEAMS_INSTALLED_APP_SCHEMA = StructType(
    [
        StructField("teamId", StringType(), True),
        StructField("appId", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("teamsAppId", StringType(), True),
        StructField("version", StringType(), True),
        StructField("publishingState", StringType(), True),
    ]
)

# EXO group members (exo_*_groups/*/*/members — shared by DG and UG)
EXO_GROUP_MEMBER_SCHEMA = StructType(
    [
        StructField("groupIdentity", StringType(), True),
        StructField("groupObjectId", StringType(), True),
        StructField("groupType", StringType(), True),
        StructField("memberName", StringType(), True),
        StructField("memberObjectId", StringType(), True),
        StructField("memberType", StringType(), True),
        StructField("primarySmtp", StringType(), True),
    ]
)

# Entra applications (v1.0/applications)
ENTRA_APPLICATION_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("appId", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("signInAudience", StringType(), True),
        StructField("identifierUris", ArrayType(StringType()), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("applicationTemplateId", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("publisherDomain", StringType(), True),
        StructField("description", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("groupMembershipClaims", StringType(), True),
        StructField("isFallbackPublicClient", BooleanType(), True),
        StructField("disabledByMicrosoftStatus", StringType(), True),
        StructField("samlMetadataUrl", StringType(), True),
    ]
)

# Entra app owners (entra_applications/*/*/owners)
ENTRA_APP_OWNER_SCHEMA = StructType(
    [
        StructField("applicationId", StringType(), True),
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("mail", StringType(), True),
    ]
)

# Entra service principals (v1.0/servicePrincipals)
ENTRA_SERVICE_PRINCIPAL_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("appId", StringType(), True),
        StructField("appDisplayName", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("servicePrincipalType", StringType(), True),
        StructField("appOwnerOrganizationId", StringType(), True),
        StructField("accountEnabled", BooleanType(), True),
        StructField("appRoleAssignmentRequired", BooleanType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("servicePrincipalNames", ArrayType(StringType()), True),
        StructField("homepage", StringType(), True),
        StructField("loginUrl", StringType(), True),
        StructField("preferredSingleSignOnMode", StringType(), True),
        StructField("signInAudience", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("applicationTemplateId", StringType(), True),
        StructField("description", StringType(), True),
        StructField("disabledByMicrosoftStatus", StringType(), True),
    ]
)

# Entra SP owners (entra_service_principals/*/*/owners)
ENTRA_SP_OWNER_SCHEMA = StructType(
    [
        StructField("servicePrincipalId", StringType(), True),
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("mail", StringType(), True),
    ]
)

# Entra delegated permission grants (v1.0/oauth2PermissionGrants)
# No $select on this endpoint — full payload. Fields from Graph API docs.
ENTRA_DELEGATED_GRANT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("clientId", StringType(), True),
        StructField("consentType", StringType(), True),
        StructField("principalId", StringType(), True),
        StructField("resourceId", StringType(), True),
        StructField("scope", StringType(), True),
    ]
)

# Entra sign-in logs (v1.0/auditLogs/signIns) — append-only, not SCD
ENTRA_SIGN_IN_LOG_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("appDisplayName", StringType(), True),
        StructField("appId", StringType(), True),
        StructField("ipAddress", StringType(), True),
        StructField("clientAppUsed", StringType(), True),
        StructField("conditionalAccessStatus", StringType(), True),
        StructField("isInteractive", BooleanType(), True),
        StructField("resourceDisplayName", StringType(), True),
        StructField("resourceId", StringType(), True),
        StructField("riskDetail", StringType(), True),
        StructField("riskLevelAggregated", StringType(), True),
        StructField("riskLevelDuringSignIn", StringType(), True),
        StructField("riskState", StringType(), True),
        StructField("userDisplayName", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------


def check_license_sku(sku_ids):
    """Returns a column expression checking if the user holds any of the given license SKUs.

    Accepts either a single SKU GUID or a list of GUIDs. Used for tier-level flags
    (e.g. `has_e3_license`) where a tier may correspond to multiple skuId variants
    (Microsoft 365 SPE_E3 vs Office 365 ENTERPRISEPACK, etc.).
    """
    if isinstance(sku_ids, str):
        sku_ids = [sku_ids]

    sku_ids = [str(s).strip() for s in sku_ids if s is not None and str(s).strip()]
    if not sku_ids:
        return lit(False)

    quoted = ", ".join("'" + s.replace("'", "''") + "'" for s in sku_ids)
    return expr(f"exists(assignedLicenses, x -> x.skuId IN ({quoted}))")


def extract_license_skus():
    """Extract license SKU IDs as array."""
    return expr("transform(assignedLicenses, x -> x.skuId)")


def environment_col(tenant_col: str = "source_key"):
    """Derive environment (source/target) from source_key using role configuration.
    Tenants not present in either set return 'unknown'.
    """
    result = lit("unknown")
    for t in TARGET_TENANT_NAMES:
        result = when(col(tenant_col) == lit(t), lit("target")).otherwise(result)
    for t in SOURCE_TENANT_NAMES:
        result = when(col(tenant_col) == lit(t), lit("source")).otherwise(result)
    return result


# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function: SharePoint Principal Parser (#453)
# MAGIC
# MAGIC SharePoint persists principals as a `LoginName` claim string (`i:0#.f|membership|...`,
# MAGIC `c:0o.c|federateddirectoryclaimprovider|...`, etc.) plus a `Title` and an int
# MAGIC `PrincipalType` enum. `parse_sp_principal()` decodes the claim grammar into typed
# MAGIC AAD identity columns shared across all 4 SPO principal-bearing silver tables:
# MAGIC `spo_site_users`, `spo_site_group_users`, `spo_web_role_assignments`,
# MAGIC `spo_web_item_principals`. Pure Spark Column composition — Photon-compatible.
# MAGIC
# MAGIC | `principal_type_parsed` value | Source claim prefix |
# MAGIC |---|---|
# MAGIC | `user` | `i:0#.f\|membership\|<upn>` |
# MAGIC | `aadGroup` | `c:0o.c\|federateddirectoryclaimprovider\|<guid>[_o]` (`_o` → owners pseudo-principal) |
# MAGIC | `app` | `i:0i.t\|<provider>\|<appId>@<homeTenantId>` (e.g. `ms.sp.ext`, or SP built-in `app@sharepoint`) |
# MAGIC | `tenantClaim` | `c:0t.c\|tenant\|<guid>` |
# MAGIC | `sharingLinkClaim` | `c:0u.c\|tenant\|...` (LinkId from `Title` `SLinkClaim.<digest>.<linkId>`) |
# MAGIC | `roleClaim` | `c:0-.f\|rolemanager\|...` |
# MAGIC | `everyone` / `everyoneExceptExternal` | `c:0(.s\|true` (disambiguated via `Title`) |
# MAGIC | `systemAccount` | `SHAREPOINT\\`, `NT Service\\<svc>`, or `i:0#.w\|<windows-account>` |
# MAGIC | `sharePointGroup` | non-claim, fallback by `PrincipalType=8` |
# MAGIC | `securityGroup` | non-claim, fallback by `PrincipalType=4` |
# MAGIC | `unknown` | nothing matched |

# COMMAND ----------


def parse_sp_principal(login_col, title_col, principal_type_int_col=None, upn_native_col=None, user_id_native_col=None):
    """Decode a SharePoint principal `LoginName` claim into typed AAD identity columns.

    Returns a dict[str, Column] of 10 derived columns suitable for ``df.withColumns({...})``
    on the staged DataFrame. Caller passes whatever native attribute columns are available
    on the source table (UPN, directory object id) and falls back to claim parsing otherwise.

    Args:
        login_col: Column for `LoginName` (the claim string).
        title_col: Column for `Title` — used for EEEU disambiguation and `SLinkClaim` parsing.
        principal_type_int_col: Optional Column for the int `PrincipalType` enum (8/4 fallback).
        upn_native_col: Optional Column for a native `UserPrincipalName` field (preferred over claim parse).
        user_id_native_col: Optional Column for a native AAD object id (e.g. `UserId.NameId`).
    """
    user_re = r"^i:0#\.f\|membership\|(.+)$"
    win_re = r"^i:0#\.w\|(.+)$"  # Windows claim — surfaces for SP system accounts (e.g. nt service\spsearch)
    group_re = r"^c:0o\.c\|federateddirectoryclaimprovider\|([0-9a-fA-F-]+)(_o)?$"
    tenant_re = r"^c:0t\.c\|tenant\|(.+)$"
    slink_re = r"^c:0u\.c\|tenant\|"
    role_re = r"^c:0-\.f\|rolemanager\|"
    eeeu_re = r"^c:0\(\.s\|true$"
    # `app` matches any provider in the i:0i.t claim — covers ms.sp.ext (cross/same-tenant AAD apps)
    # AND the SharePoint built-in `app@sharepoint` form (provider == app GUID, homeTenant == "sharepoint")
    app_re = r"^i:0i\.t\|[^|]+\|([^@]+)@(.+)$"
    sys_re = r"^SHAREPOINT\\"  # Regex uses \\ to match a literal backslash after SHAREPOINT
    # NT Service\<name> — surfaces for non-claim system accounts (e.g. NT Service\SPSearch)
    nt_service_re = r"^NT Service\\"
    slink_title_re = r"^SLinkClaim\.[^.]+\.(.+)$"

    pt_int = principal_type_int_col if principal_type_int_col is not None else lit(None).cast("int")

    pt = (
        when(login_col.rlike(user_re), lit("user"))
        .when(login_col.rlike(group_re), lit("aadGroup"))
        .when(login_col.rlike(app_re), lit("app"))
        .when(login_col.rlike(tenant_re), lit("tenantClaim"))
        .when(login_col.rlike(slink_re), lit("sharingLinkClaim"))
        .when(login_col.rlike(role_re), lit("roleClaim"))
        .when(login_col.rlike(sys_re), lit("systemAccount"))
        .when(login_col.rlike(nt_service_re), lit("systemAccount"))
        .when(login_col.rlike(win_re), lit("systemAccount"))
        .when(
            login_col.rlike(eeeu_re),
            when(title_col == lit("Everyone except external users"), lit("everyoneExceptExternal")).otherwise(
                lit("everyone")
            ),
        )
        # Non-claim fallback (no i:/c: prefix matched)
        .when(pt_int == lit(8), lit("sharePointGroup"))
        .when(pt_int == lit(4), lit("securityGroup"))
        .otherwise(lit("unknown"))
    )

    upn_from_claim = regexp_extract(login_col, user_re, 1)
    if upn_native_col is not None:
        # Treat empty native UPN as null so claim parse wins
        aad_upn = coalesce(
            when(upn_native_col == lit(""), lit(None).cast("string")).otherwise(upn_native_col), upn_from_claim
        )
    else:
        aad_upn = upn_from_claim

    aad_user_object_id = user_id_native_col if user_id_native_col is not None else lit(None).cast("string")

    aad_group_id_raw = regexp_extract(login_col, group_re, 1)
    aad_group_subtype = when(
        login_col.rlike(group_re),
        when(regexp_extract(login_col, group_re, 2) == lit("_o"), lit("owners")).otherwise(lit("members")),
    )

    aad_app_id_raw = regexp_extract(login_col, app_re, 1)
    aad_app_home_tenant = regexp_extract(login_col, app_re, 2)

    tenant_claim_id = regexp_extract(login_col, tenant_re, 1)
    sharing_link_id_raw = regexp_extract(title_col, slink_title_re, 1)
    sharing_link_id = when(sharing_link_id_raw == lit(""), lit(None).cast("string")).otherwise(sharing_link_id_raw)

    return {
        "principal_type_parsed": pt,
        "principal_subtype": when(pt == lit("aadGroup"), aad_group_subtype),
        "aad_user_upn": when(pt == lit("user"), aad_upn),
        "aad_user_is_guest": when(
            pt == lit("user"), when(lower(aad_upn).contains("#ext#"), lit(True)).otherwise(lit(False))
        ),
        "aad_user_object_id": when(pt == lit("user"), aad_user_object_id),
        "aad_group_id": when(pt == lit("aadGroup"), aad_group_id_raw),
        "aad_app_id": when(pt == lit("app"), aad_app_id_raw),
        "aad_app_home_tenant_id": when(pt == lit("app"), aad_app_home_tenant),
        "tenant_claim_tenant_id": when(pt == lit("tenantClaim"), tenant_claim_id),
        "sharing_link_id": when(pt == lit("sharingLinkClaim"), sharing_link_id),
    }


# Tuple of derived column names — stable order for .select() projections.
SP_PRINCIPAL_PARSED_COLS = (
    "principal_type_parsed",
    "principal_subtype",
    "aad_user_upn",
    "aad_user_is_guest",
    "aad_user_object_id",
    "aad_group_id",
    "aad_app_id",
    "aad_app_home_tenant_id",
    "tenant_claim_tenant_id",
    "sharing_link_id",
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Users
# MAGIC
# MAGIC **Source:** `entra_users`
# MAGIC **Key:** `user_id` = `{source_key}_{entra_object_id}`
# MAGIC **Dedup:** SCD Type 1 (latest batch wins)
# MAGIC
# MAGIC Transforms:
# MAGIC - Parse `_record` JSON with explicit schema
# MAGIC - Normalize email/UPN to lowercase
# MAGIC - Extract license SKU flags for the full E and F families (E1/E2/E3/E4/E5/F1/F3/F5)
# MAGIC   plus an umbrella `has_e_or_f_license` flag for in-scope-person filtering
# MAGIC - Map on-premises sync attributes for hybrid scenarios

# COMMAND ----------


@dlt.table(name="users_staged", comment="Staged user records before deduplication", temporary=True)
@dlt.expect_or_drop("valid_id", "entra_object_id IS NOT NULL")
@dlt.expect("valid_upn", "user_principal_name IS NOT NULL")
def users_staged():
    df = (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_users")
        .withColumn("r", from_json(col("_record"), ENTRA_USER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
    )
    # Resolve UPN: extension attribute wins over standard field
    upn_col = (
        coalesce(trim(col(EXT_ATTR_UPN)), trim(col("userPrincipalName")))
        if EXT_ATTR_UPN
        else trim(col("userPrincipalName"))
    )
    # Resolve employee_id: extension attribute wins over standard field
    emp_id_col = coalesce(col(EXT_ATTR_EMPLOYEE_ID), col("employeeId")) if EXT_ATTR_EMPLOYEE_ID else col("employeeId")
    return (
        df.withColumn("user_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("entra_object_id", col("id"))
        .withColumn("environment", environment_col())
        .withColumn("user_principal_name", upn_col)
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("given_name", trim(col("givenName")))
        .withColumn("surname", trim(col("surname")))
        .withColumn("mail", lower(trim(col("mail"))))
        .withColumn("proxy_addresses", col("proxyAddresses"))
        .withColumn("user_type", col("userType"))
        .withColumn("account_enabled", col("accountEnabled").cast("boolean"))
        .withColumn("employee_id", emp_id_col)
        .withColumn("employee_type", col("employeeType"))
        .withColumn("job_title", col("jobTitle"))
        .withColumn("department", col("department"))
        .withColumn("company_name", col("companyName"))
        .withColumn("office_location", col("officeLocation"))
        .withColumn("assigned_licenses", extract_license_skus())
        .withColumn("has_e1_license", check_license_sku(LICENSE_SKUS["E1"]))
        .withColumn("has_e2_license", check_license_sku(LICENSE_SKUS["E2"]))
        .withColumn("has_e3_license", check_license_sku(LICENSE_SKUS["E3"]))
        .withColumn("has_e4_license", check_license_sku(LICENSE_SKUS["E4"]))
        .withColumn("has_e5_license", check_license_sku(LICENSE_SKUS["E5"]))
        .withColumn("has_f1_license", check_license_sku(LICENSE_SKUS["F1"]))
        .withColumn("has_f3_license", check_license_sku(LICENSE_SKUS["F3"]))
        .withColumn("has_f5_license", check_license_sku(LICENSE_SKUS["F5"]))
        .withColumn(
            "has_e_or_f_license",
            col("has_e1_license")
            | col("has_e2_license")
            | col("has_e3_license")
            | col("has_e4_license")
            | col("has_e5_license")
            | col("has_f1_license")
            | col("has_f3_license")
            | col("has_f5_license"),
        )
        .withColumn("on_prem_sam_account_name", col("onPremisesSamAccountName"))
        .withColumn("on_prem_immutable_id", col("onPremisesImmutableId"))
        .withColumn("on_prem_distinguished_name", col("onPremisesDistinguishedName"))
        .withColumn("on_prem_security_identifier", col("onPremisesSecurityIdentifier"))
        .withColumn("on_prem_sync_enabled", col("onPremisesSyncEnabled").cast("boolean"))
        .withColumn("on_prem_last_sync_at", to_timestamp(col("onPremisesLastSyncDateTime")))
        .withColumn("on_prem_domain_name", col("onPremisesDomainName"))
        .withColumn("on_prem_user_principal_name", col("onPremisesUserPrincipalName"))
        .withColumn("manager_entra_object_id", col("manager.id"))
        .withColumn("manager_display_name", trim(col("manager.displayName")))
        .withColumn("manager_upn", lower(trim(col("manager.userPrincipalName"))))
        .withColumn("manager_mail", lower(trim(col("manager.mail"))))
        .withColumn("mobile_phone", col("mobilePhone"))
        .withColumn("usage_location", col("usageLocation"))
        .withColumn("preferred_language", col("preferredLanguage"))
        .withColumn("source_created_at", to_timestamp(col("createdDateTime")))
        .withColumn("last_password_change_at", to_timestamp(col("lastPasswordChangeDateTime")))
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "user_id",
            "entra_object_id",
            "environment",
            "source_key",
            "user_principal_name",
            "display_name",
            "given_name",
            "surname",
            "mail",
            "proxy_addresses",
            "user_type",
            "account_enabled",
            "employee_id",
            "employee_type",
            "job_title",
            "department",
            "company_name",
            "office_location",
            "assigned_licenses",
            "has_e1_license",
            "has_e2_license",
            "has_e3_license",
            "has_e4_license",
            "has_e5_license",
            "has_f1_license",
            "has_f3_license",
            "has_f5_license",
            "has_e_or_f_license",
            "on_prem_sam_account_name",
            "on_prem_immutable_id",
            "on_prem_distinguished_name",
            "on_prem_security_identifier",
            "on_prem_sync_enabled",
            "on_prem_last_sync_at",
            "on_prem_domain_name",
            "on_prem_user_principal_name",
            "manager_entra_object_id",
            "manager_display_name",
            "manager_upn",
            "manager_mail",
            "mobile_phone",
            "usage_location",
            "preferred_language",
            "source_created_at",
            "last_password_change_at",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="users",
    comment="Deduplicated user accounts from source and target tenants. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="users", source="users_staged", keys=["user_id"], sequence_by=col("last_updated_at"), stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Contacts
# MAGIC
# MAGIC **Source:** `entra_contacts`
# MAGIC **Key:** `contact_id` = `{source_key}_{entra_object_id}`
# MAGIC
# MAGIC Org Contacts are external mail recipients in the Global Address List.
# MAGIC Their `mail` field is the targetAddress used for mail routing.

# COMMAND ----------


@dlt.table(name="contacts_staged", comment="Staged contact records before deduplication", temporary=True)
@dlt.expect_or_drop("valid_id", "entra_object_id IS NOT NULL")
def contacts_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_contacts")
        .withColumn("r", from_json(col("_record"), ENTRA_CONTACT_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("contact_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("entra_object_id", col("id"))
        .withColumn("environment", environment_col())
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("given_name", trim(col("givenName")))
        .withColumn("surname", trim(col("surname")))
        .withColumn("mail", lower(trim(col("mail"))))
        .withColumn("proxy_addresses", col("proxyAddresses"))
        .withColumn("company_name", col("companyName"))
        .withColumn("department", col("department"))
        .withColumn("job_title", col("jobTitle"))
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "contact_id",
            "entra_object_id",
            "environment",
            "source_key",
            "display_name",
            "given_name",
            "surname",
            "mail",
            "proxy_addresses",
            "company_name",
            "department",
            "job_title",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="contacts",
    comment="Deduplicated organizational contacts from source and target tenants. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="contacts",
    source="contacts_staged",
    keys=["contact_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Groups
# MAGIC
# MAGIC **Source:** `entra_groups`
# MAGIC **Key:** `group_id` = `{source_key}_{entra_object_id}`
# MAGIC
# MAGIC Derives `group_type` from Entra attributes:
# MAGIC - `groupTypes` contains "Unified" → Microsoft 365 Group
# MAGIC - `mailEnabled` + `securityEnabled` → Mail-Enabled Security
# MAGIC - `mailEnabled` only → Distribution List
# MAGIC - `securityEnabled` only → Security Group

# COMMAND ----------


@dlt.table(name="groups_staged", comment="Staged group records before deduplication", temporary=True)
@dlt.expect_or_drop("valid_id", "entra_object_id IS NOT NULL")
def groups_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_groups")
        .withColumn("r", from_json(col("_record"), ENTRA_GROUP_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("group_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("entra_object_id", col("id"))
        .withColumn("environment", environment_col())
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("description", col("description"))
        .withColumn("mail", lower(trim(col("mail"))))
        .withColumn("proxy_addresses", col("proxyAddresses"))
        .withColumn("mail_enabled", col("mailEnabled").cast("boolean"))
        .withColumn("security_enabled", col("securityEnabled").cast("boolean"))
        .withColumn("group_types", col("groupTypes"))
        .withColumn(
            "group_type",
            when(array_contains(col("groupTypes"), "Unified"), lit("Microsoft365"))
            .when(col("mailEnabled") & col("securityEnabled"), lit("MailEnabledSecurity"))
            .when(col("mailEnabled"), lit("Distribution"))
            .otherwise(lit("Security")),
        )
        .withColumn(
            "is_dynamic", when(array_contains(col("groupTypes"), "DynamicMembership"), lit(True)).otherwise(lit(False))
        )
        .withColumn("membership_rule", col("membershipRule"))
        .withColumn("source_created_at", to_timestamp(col("createdDateTime")))
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "group_id",
            "entra_object_id",
            "environment",
            "source_key",
            "display_name",
            "description",
            "mail",
            "proxy_addresses",
            "mail_enabled",
            "security_enabled",
            "group_types",
            "group_type",
            "is_dynamic",
            "membership_rule",
            "source_created_at",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="groups",
    comment="Deduplicated groups from source and target tenants. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="groups", source="groups_staged", keys=["group_id"], sequence_by=col("last_updated_at"), stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Group Memberships
# MAGIC
# MAGIC **Source:** `entra_group_members`
# MAGIC **Key:** `membership_id` = `{source_key}_{groupId}_{member_id}`
# MAGIC
# MAGIC One row per member. Members can be users, groups, or service principals.
# MAGIC `member_type` is derived from `@odata.type` by the ingest module.

# COMMAND ----------


@dlt.table(
    name="group_memberships_staged", comment="Staged group membership records before deduplication", temporary=True
)
@dlt.expect_or_drop(
    "valid_group", "source_key IS NOT NULL AND group_silver_id IS NOT NULL AND group_silver_id != source_key"
)
@dlt.expect_or_drop("valid_member", "member_id IS NOT NULL")
def group_memberships_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_group_members")
        .withColumn("r", from_json(col("_record"), ENTRA_GROUP_MEMBER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("membership_id", concat_ws("_", col("source_key"), col("groupId"), col("id")))
        .withColumn("group_silver_id", concat_ws("_", col("source_key"), col("groupId")))
        .withColumn("member_id", col("id"))
        .withColumn("member_type", col("memberType"))
        .withColumn("member_display_name", trim(col("displayName")))
        .withColumn("member_upn", lower(trim(col("userPrincipalName"))))
        .withColumn("member_mail", lower(trim(col("mail"))))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "membership_id",
            "group_silver_id",
            "member_id",
            "member_type",
            "member_display_name",
            "member_upn",
            "member_mail",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="group_memberships",
    comment="Deduplicated group membership relationships. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="group_memberships",
    source="group_memberships_staged",
    keys=["membership_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - SPO Sites
# MAGIC
# MAGIC **Sources:**
# MAGIC - `spo_sites` (bronze — Microsoft Graph 10-field URL list, post-#475)
# MAGIC - `spo_site_details` (bronze — SP admin-REST GetSitePropertiesByUrl, 134-field shape)
# MAGIC
# MAGIC **Key:** `site_silver_id` = `md5(lower(source_key) || '|' || lower(web_url))`
# MAGIC
# MAGIC Covers both SharePoint site collections and OneDrive personal sites.
# MAGIC Filter `is_personal_site = true` for OneDrive, `false` for SPO.
# MAGIC
# MAGIC The site URL identifies a site collection on both sides of the join:
# MAGIC Graph emits `webUrl`, admin REST emits `Url` (and a `SiteUrl` FK column
# MAGIC added by the fetcher — identical for site-collection rows). Joined here so
# MAGIC gold consumers see one row per (source_key, site) with storage / lock /
# MAGIC template / sharing fields populated from the admin-REST side. #465 T3.
# MAGIC
# MAGIC **Note on `site_silver_id` shape:** url-based md5 (not `{source_key}_{id}`
# MAGIC as in PR #528). The Graph site `id` GUID is not present on any of the
# MAGIC descendant bronze tables (site_users, webs, web_lists, role_definitions) —
# MAGIC they only carry SiteUrl/WebUrl. Switching to url-based md5 lets every
# MAGIC descendant compute the same `site_silver_id` for FK joins. No gold table
# MAGIC currently joins on the column so the shape change is internal.

# COMMAND ----------


@dlt.table(name="spo_sites_staged", comment="Staged SPO/OneDrive site records before deduplication", temporary=True)
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND sharepoint_id IS NOT NULL")
@dlt.expect_or_drop("valid_url", "web_url IS NOT NULL")
def spo_sites_staged():
    # Graph URL list (streaming) — primary driver.
    sites = (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.spo_sites")
        .withColumn("r", from_json(col("_record"), SPO_SITES_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("web_url_lc", lower(trim(col("webUrl"))))
    )

    # Admin-REST SiteProperties (batch read) — dedup to latest per (source_key, Url)
    # to avoid LEFT JOIN fan-out from repeated daily loads. Mirrors the teams_staged
    # window pattern (#378).
    details_window = Window.partitionBy("d_source_key", "d_url_lc").orderBy(col("_dlt_ingested_at").desc())
    details = (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.spo_site_details")
        .withColumn("d", from_json(col("_record"), SPO_SITE_DETAILS_SCHEMA))
        .select(
            col("source_key").alias("d_source_key"),
            lower(trim(coalesce(col("d.Url"), col("d.SiteUrl")))).alias("d_url_lc"),
            col("d.Template").alias("d_template"),
            col("d.Status").alias("d_status"),
            col("d.LockState").alias("d_lock_state"),
            col("d.IsHubSite").alias("d_is_hub_site"),
            col("d.HubSiteId").alias("d_hub_site_id"),
            col("d.GroupId").alias("d_group_id"),
            col("d.RelatedGroupId").alias("d_related_group_id"),
            col("d.TeamsChannelType").alias("d_teams_channel_type"),
            col("d.IsTeamsConnected").alias("d_is_teams_connected"),
            col("d.IsTeamsChannelConnected").alias("d_is_teams_channel_connected"),
            col("d.Lcid").alias("d_lcid"),
            col("d.StorageMaximumLevel").alias("d_storage_max"),
            col("d.StorageUsage").alias("d_storage_used"),
            col("d.SharingCapability").alias("d_sharing_capability"),
            col("d.SensitivityLabel").alias("d_sensitivity_label"),
            col("d.ArchiveStatus").alias("d_archive_status"),
            col("d.ArchivedFileDiskUsed").alias("d_archived_bytes"),
            col("d.LastContentModifiedDate").alias("d_last_modified"),
            # Owner identity fields. OwnerEmail drives the silver.users lookup
            # below; the others are surfaced raw so downstream consumers can
            # claim-parse `OwnerLoginName` or fall back to a display name. #485 T3.
            col("d.Owner").alias("d_owner"),
            lower(trim(col("d.OwnerEmail"))).alias("d_owner_email"),
            col("d.OwnerLoginName").alias("d_owner_login_name"),
            col("d.OwnerName").alias("d_owner_name"),
            col("_dlt_ingested_at"),
        )
        .withColumn("_rn", row_number().over(details_window))
        .filter(col("_rn") == 1)
        .drop("_rn", "_dlt_ingested_at")
    )

    # Owner email → entra_object_id lookup, per environment (source/target).
    # Resolves spo_site_details.OwnerEmail (typically a UPN-like address on
    # OneDrive sites) against silver.users by matching the normalised email
    # to `mail` or any prefix-stripped entry in `proxy_addresses`. Scoped by
    # `environment` so a source-tenant OneDrive owner cannot leak into a
    # target-tenant entra_object_id (and vice versa). #485 T3.
    users_static = dlt.read("users").filter(col("entra_object_id").isNotNull())
    mail_addrs = users_static.filter(col("mail").isNotNull()).select(
        col("environment").alias("u_environment"),
        col("entra_object_id").alias("u_entra_object_id"),
        lower(trim(col("mail"))).alias("u_addr"),
    )
    proxy_addrs = (
        users_static.filter(col("proxy_addresses").isNotNull())
        .select(
            col("environment").alias("u_environment"),
            col("entra_object_id").alias("u_entra_object_id"),
            explode(col("proxy_addresses")).alias("raw_addr"),
        )
        .withColumn("u_addr", lower(regexp_replace(col("raw_addr"), "^(?i)(smtp:|sip:|x500:|x400:)", "")))
        .drop("raw_addr")
    )
    owner_lookup = (
        mail_addrs.unionByName(proxy_addrs)
        .filter(col("u_addr").isNotNull() & (col("u_addr") != ""))
        .dropDuplicates(["u_environment", "u_addr"])
    )

    # SharingCapability is an int enum on the REST side. Map to friendly names so
    # silver consumers don't have to memorise enum values.
    sharing_friendly = (
        when(col("d_sharing_capability") == 0, lit("Disabled"))
        .when(col("d_sharing_capability") == 1, lit("ExternalUserSharingOnly"))
        .when(col("d_sharing_capability") == 2, lit("ExternalUserAndGuestSharing"))
        .when(col("d_sharing_capability") == 3, lit("ExistingExternalUserSharingOnly"))
        .otherwise(lit(None).cast("string"))
    )

    return (
        sites.join(
            details,
            (sites.source_key == details.d_source_key) & (sites.web_url_lc == details.d_url_lc),
            "left",
        )
        # Per-environment owner resolution: join env-derived from source_key against
        # the env-prefixed owner_lookup so source-tenant owners cannot resolve to
        # target-tenant users. #485 T3.
        .join(
            owner_lookup,
            (environment_col() == col("u_environment")) & (col("d_owner_email") == col("u_addr")),
            "left",
        )
        .withColumn("site_silver_id", md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"))))
        .withColumn("sharepoint_id", col("id"))
        .withColumn("web_url", col("webUrl"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("is_personal_site", col("isPersonalSite").cast("boolean"))
        .withColumn("is_hub_site", col("d_is_hub_site").cast("boolean"))
        .withColumn("hub_site_id", col("d_hub_site_id"))
        .withColumn("group_id", col("d_group_id"))
        .withColumn("related_group_id", col("d_related_group_id"))
        .withColumn("teams_channel_type", col("d_teams_channel_type").cast("int"))
        .withColumn("is_teams_connected", col("d_is_teams_connected").cast("boolean"))
        .withColumn("is_teams_channel_connected", col("d_is_teams_channel_connected").cast("boolean"))
        .withColumn("template", col("d_template"))
        .withColumn("status", col("d_status"))
        .withColumn("locale_id", col("d_lcid").cast("int"))
        # StorageMaximumLevel and StorageUsage are reported in MB (SP admin REST contract).
        .withColumn("storage_quota_mb", col("d_storage_max").cast("long"))
        .withColumn("storage_used_mb", col("d_storage_used").cast("long"))
        .withColumn("sharing_capability", sharing_friendly)
        .withColumn("lock_state", col("d_lock_state"))
        .withColumn("sensitivity_label", col("d_sensitivity_label"))
        .withColumn("archive_status", col("d_archive_status"))
        # ArchivedFileDiskUsed is reported in bytes; normalise to MB to match the
        # storage_*_mb columns. Floor-divide so a partial-MB residue rounds down.
        .withColumn(
            "archived_file_disk_used_mb", (col("d_archived_bytes").cast("long") / lit(1024 * 1024)).cast("long")
        )
        .withColumn(
            "source_modified_at",
            to_timestamp(coalesce(col("d_last_modified"), col("lastModifiedDateTime"))),
        )
        # Owner identity. `d_owner_email` is already lower+trim from the details
        # projection; surface it raw alongside the resolved entra_object_id.
        # Prefer `d_owner_name` (admin REST human display) over `d_owner` (which
        # on OneDrive often duplicates the email). #485 T3.
        .withColumn("owner_email", col("d_owner_email"))
        .withColumn("owner_login_name", col("d_owner_login_name"))
        .withColumn("owner_display_name", coalesce(col("d_owner_name"), col("d_owner")))
        .withColumn("owner_entra_object_id", col("u_entra_object_id"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "site_silver_id",
            "sharepoint_id",
            "environment",
            "source_key",
            "web_url",
            "display_name",
            "is_personal_site",
            "is_hub_site",
            "hub_site_id",
            "group_id",
            "related_group_id",
            "teams_channel_type",
            "is_teams_connected",
            "is_teams_channel_connected",
            "template",
            "status",
            "locale_id",
            "storage_quota_mb",
            "storage_used_mb",
            "sharing_capability",
            "lock_state",
            "sensitivity_label",
            "archive_status",
            "archived_file_disk_used_mb",
            "owner_email",
            "owner_login_name",
            "owner_display_name",
            "owner_entra_object_id",
            "source_modified_at",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="spo_sites",
    comment="Deduplicated SPO and OneDrive sites enriched from spo_site_details. isPersonalSite=True for OneDrive. SCD Type 1. Carries owner identity (#485 T3): owner_email/owner_login_name/owner_display_name from spo_site_details + owner_entra_object_id resolved via per-environment silver.users lookup (mail or prefix-stripped proxy_addresses). NOTE: site_silver_id key shape changed to md5(source_key, web_url); a DLT full refresh (or table drop) is required on first rollout to avoid mixed/duplicate keys.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="spo_sites",
    source="spo_sites_staged",
    keys=["site_silver_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - SPO Site Users
# MAGIC
# MAGIC **Source:** `spo_site_users` (bronze — /_api/web/SiteUsers per site collection)
# MAGIC **Key:** `site_user_id` = `md5(source_key || '|' || lower(SiteUrl) || '|' || Id)`
# MAGIC
# MAGIC One row per (site, user principal) — i.e. every user with any permission on
# MAGIC the site collection. `site_silver_id` FKs back to `silver.spo_sites`.
# MAGIC
# MAGIC ### Principal-claim parsing (#453)
# MAGIC `LoginName` decoded into `principal_type_parsed` + AAD identity columns via
# MAGIC `parse_sp_principal()`. Native `user_principal_name` preferred over claim parse for users.

# COMMAND ----------


@dlt.table(
    name="spo_site_users_staged",
    comment="Staged SPO site-user principals before deduplication",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND site_user_id IS NOT NULL")
@dlt.expect_or_drop("valid_site", "site_url IS NOT NULL")
def spo_site_users_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.spo_site_users")
        .withColumn("r", from_json(col("_record"), SPO_SITE_USER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("site_url_lc", lower(trim(col("SiteUrl"))))
        .withColumn(
            "site_user_id",
            md5(concat_ws("|", lower(col("source_key")), col("site_url_lc"), col("Id").cast("string"))),
        )
        .withColumn("site_silver_id", md5(concat_ws("|", lower(col("source_key")), col("site_url_lc"))))
        .withColumn("site_url", col("SiteUrl"))
        .withColumn("spo_user_id", col("Id"))
        .withColumn("login_name", col("LoginName"))
        .withColumn("title", trim(col("Title")))
        .withColumn("principal_type", col("PrincipalType"))
        .withColumn("email", lower(trim(col("Email"))))
        .withColumn("user_principal_name", lower(trim(col("UserPrincipalName"))))
        .withColumn("is_site_admin", col("IsSiteAdmin").cast("boolean"))
        .withColumn("is_hidden_in_ui", col("IsHiddenInUI").cast("boolean"))
        .withColumn("is_email_auth_guest_user", col("IsEmailAuthenticationGuestUser").cast("boolean"))
        .withColumn("is_share_by_email_guest_user", col("IsShareByEmailGuestUser").cast("boolean"))
        .withColumn("expiration", col("Expiration"))
        .withColumn("user_external_id", col("UserId.NameId"))
        .withColumn("user_external_issuer", col("UserId.NameIdIssuer"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .withColumns(
            parse_sp_principal(
                login_col=col("login_name"),
                title_col=col("title"),
                principal_type_int_col=col("principal_type"),
                upn_native_col=col("user_principal_name"),
                user_id_native_col=col("user_external_id"),
            )
        )
        .select(
            "site_user_id",
            "site_silver_id",
            "environment",
            "source_key",
            "site_url",
            "spo_user_id",
            "login_name",
            "title",
            "principal_type",
            "email",
            "user_principal_name",
            "user_external_id",
            "user_external_issuer",
            "is_site_admin",
            "is_hidden_in_ui",
            "is_email_auth_guest_user",
            "is_share_by_email_guest_user",
            "expiration",
            *SP_PRINCIPAL_PARSED_COLS,
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="spo_site_users",
    comment="Deduplicated SPO site-user principals (one row per (site, user)). SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="spo_site_users",
    source="spo_site_users_staged",
    keys=["site_user_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - SPO Webs
# MAGIC
# MAGIC **Source:** `spo_webs` (bronze — recursive /_api/web walk per site collection)
# MAGIC **Key:** `web_silver_id` = `md5(source_key || '|' || lower(Url))`
# MAGIC
# MAGIC One row per SP web (subsite). A site collection can have many webs.
# MAGIC `site_silver_id` FKs back to `silver.spo_sites` via parent SiteUrl.

# COMMAND ----------


@dlt.table(
    name="spo_webs_staged",
    comment="Staged SPO web (subsite) records before deduplication",
    temporary=True,
)
@dlt.expect_or_drop("valid_site", "site_url IS NOT NULL")
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND web_silver_id IS NOT NULL")
@dlt.expect_or_drop("valid_url", "web_url IS NOT NULL")
def spo_webs_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.spo_webs")
        .withColumn("r", from_json(col("_record"), SPO_WEB_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("web_url_lc", lower(trim(col("Url"))))
        .withColumn("site_url_lc", lower(trim(col("SiteUrl"))))
        .withColumn("web_silver_id", md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"))))
        .withColumn("site_silver_id", md5(concat_ws("|", lower(col("source_key")), col("site_url_lc"))))
        .withColumn("spo_web_id", col("Id"))
        .withColumn("web_url", col("Url"))
        .withColumn("site_url", col("SiteUrl"))
        .withColumn("parent_web_url", col("ParentWebUrl"))
        .withColumn("server_relative_url", col("ServerRelativeUrl"))
        .withColumn("title", trim(col("Title")))
        .withColumn("description", col("Description"))
        .withColumn("web_template", col("WebTemplate"))
        .withColumn("configuration", col("Configuration"))
        .withColumn("language", col("Language"))
        .withColumn("master_url", col("MasterUrl"))
        .withColumn("custom_master_url", col("CustomMasterUrl"))
        .withColumn("welcome_page", col("WelcomePage"))
        .withColumn("enable_minimal_download", col("EnableMinimalDownload").cast("boolean"))
        .withColumn("recycle_bin_enabled", col("RecycleBinEnabled").cast("boolean"))
        .withColumn("quick_launch_enabled", col("QuickLaunchEnabled").cast("boolean"))
        .withColumn("tree_view_enabled", col("TreeViewEnabled").cast("boolean"))
        .withColumn("no_crawl", col("NoCrawl").cast("boolean"))
        .withColumn("is_multilingual", col("IsMultilingual").cast("boolean"))
        .withColumn("is_root_web", col("IsRootWeb").cast("boolean"))
        .withColumn("source_created_at", to_timestamp(col("Created")))
        .withColumn("source_last_item_modified_at", to_timestamp(col("LastItemModifiedDate")))
        .withColumn("source_last_item_user_modified_at", to_timestamp(col("LastItemUserModifiedDate")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "web_silver_id",
            "site_silver_id",
            "environment",
            "source_key",
            "spo_web_id",
            "web_url",
            "site_url",
            "parent_web_url",
            "server_relative_url",
            "title",
            "description",
            "web_template",
            "configuration",
            "language",
            "master_url",
            "custom_master_url",
            "welcome_page",
            "enable_minimal_download",
            "recycle_bin_enabled",
            "quick_launch_enabled",
            "tree_view_enabled",
            "no_crawl",
            "is_multilingual",
            "is_root_web",
            "source_created_at",
            "source_last_item_modified_at",
            "source_last_item_user_modified_at",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="spo_webs",
    comment="Deduplicated SPO webs (subsites). SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="spo_webs",
    source="spo_webs_staged",
    keys=["web_silver_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - SPO Web Role Definitions
# MAGIC
# MAGIC **Source:** `spo_web_role_definitions` (bronze — /_api/web/RoleDefinitions per web)
# MAGIC **Key:** `role_definition_id` = `md5(source_key || '|' || lower(WebUrl) || '|' || Id)`
# MAGIC
# MAGIC One row per role definition (e.g. "Full Control") scoped to a web.

# COMMAND ----------


@dlt.table(
    name="spo_web_role_definitions_staged",
    comment="Staged SPO web role-definition records before deduplication",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND role_definition_id IS NOT NULL")
@dlt.expect_or_drop("valid_url", "web_url IS NOT NULL")
@dlt.expect_or_drop("valid_site", "site_url IS NOT NULL")
def spo_web_role_definitions_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.spo_web_role_definitions")
        .withColumn("r", from_json(col("_record"), SPO_WEB_ROLE_DEFINITION_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("web_url_lc", lower(trim(col("WebUrl"))))
        .withColumn("site_url_lc", lower(trim(col("SiteUrl"))))
        .withColumn(
            "role_definition_id",
            md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"), col("Id").cast("string"))),
        )
        .withColumn("web_silver_id", md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"))))
        .withColumn("site_silver_id", md5(concat_ws("|", lower(col("source_key")), col("site_url_lc"))))
        .withColumn("spo_role_id", col("Id"))
        .withColumn("name", trim(col("Name")))
        .withColumn("description", col("Description"))
        .withColumn("hidden", col("Hidden").cast("boolean"))
        .withColumn("role_order", col("Order"))
        .withColumn("role_type_kind", col("RoleTypeKind"))
        .withColumn("base_permissions_high", col("BasePermissions.High"))
        .withColumn("base_permissions_low", col("BasePermissions.Low"))
        .withColumn("web_url", col("WebUrl"))
        .withColumn("site_url", col("SiteUrl"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "role_definition_id",
            "web_silver_id",
            "site_silver_id",
            "environment",
            "source_key",
            "spo_role_id",
            "name",
            "description",
            "hidden",
            "role_order",
            "role_type_kind",
            "base_permissions_high",
            "base_permissions_low",
            "web_url",
            "site_url",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="spo_web_role_definitions",
    comment="Deduplicated SPO web role definitions (one row per (web, role)). SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="spo_web_role_definitions",
    source="spo_web_role_definitions_staged",
    keys=["role_definition_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - SPO Web Lists
# MAGIC
# MAGIC **Source:** `spo_web_lists` (bronze — /_api/web/Lists per web)
# MAGIC **Key:** `list_silver_id` = `md5(source_key || '|' || lower(WebUrl) || '|' || Id)`
# MAGIC
# MAGIC One row per list / document library on a web.

# COMMAND ----------


@dlt.table(
    name="spo_web_lists_staged",
    comment="Staged SPO web list records before deduplication",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND list_silver_id IS NOT NULL")
@dlt.expect_or_drop("valid_url", "web_url IS NOT NULL")
@dlt.expect_or_drop("valid_site", "site_url IS NOT NULL")
def spo_web_lists_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.spo_web_lists")
        .withColumn("r", from_json(col("_record"), SPO_WEB_LIST_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("web_url_lc", lower(trim(col("WebUrl"))))
        .withColumn("site_url_lc", lower(trim(col("SiteUrl"))))
        .withColumn(
            "list_silver_id",
            md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"), col("Id"))),
        )
        .withColumn("web_silver_id", md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"))))
        .withColumn("site_silver_id", md5(concat_ws("|", lower(col("source_key")), col("site_url_lc"))))
        .withColumn("spo_list_id", col("Id"))
        .withColumn("title", trim(col("Title")))
        .withColumn("description", col("Description"))
        .withColumn("base_template", col("BaseTemplate"))
        .withColumn("base_type", col("BaseType"))
        .withColumn("hidden", col("Hidden").cast("boolean"))
        .withColumn("is_catalog", col("IsCatalog").cast("boolean"))
        .withColumn("is_application_list", col("IsApplicationList").cast("boolean"))
        .withColumn("is_private", col("IsPrivate").cast("boolean"))
        .withColumn("item_count", col("ItemCount").cast("long"))
        .withColumn("enable_attachments", col("EnableAttachments").cast("boolean"))
        .withColumn("enable_folder_creation", col("EnableFolderCreation").cast("boolean"))
        .withColumn("enable_moderation", col("EnableModeration").cast("boolean"))
        .withColumn("enable_versioning", col("EnableVersioning").cast("boolean"))
        .withColumn("enable_minor_versions", col("EnableMinorVersions").cast("boolean"))
        .withColumn("force_checkout", col("ForceCheckout").cast("boolean"))
        .withColumn("major_version_limit", col("MajorVersionLimit"))
        .withColumn("major_with_minor_versions_limit", col("MajorWithMinorVersionsLimit"))
        .withColumn("no_crawl", col("NoCrawl").cast("boolean"))
        .withColumn("entity_type_name", col("EntityTypeName"))
        .withColumn("parent_web_url", col("ParentWebUrl"))
        .withColumn("web_url", col("WebUrl"))
        .withColumn("site_url", col("SiteUrl"))
        .withColumn("source_created_at", to_timestamp(col("Created")))
        .withColumn("source_last_item_deleted_at", to_timestamp(col("LastItemDeletedDate")))
        .withColumn("source_last_item_modified_at", to_timestamp(col("LastItemModifiedDate")))
        .withColumn("source_last_item_user_modified_at", to_timestamp(col("LastItemUserModifiedDate")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "list_silver_id",
            "web_silver_id",
            "site_silver_id",
            "environment",
            "source_key",
            "spo_list_id",
            "title",
            "description",
            "base_template",
            "base_type",
            "hidden",
            "is_catalog",
            "is_application_list",
            "is_private",
            "item_count",
            "enable_attachments",
            "enable_folder_creation",
            "enable_moderation",
            "enable_versioning",
            "enable_minor_versions",
            "force_checkout",
            "major_version_limit",
            "major_with_minor_versions_limit",
            "no_crawl",
            "entity_type_name",
            "parent_web_url",
            "web_url",
            "site_url",
            "source_created_at",
            "source_last_item_deleted_at",
            "source_last_item_modified_at",
            "source_last_item_user_modified_at",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="spo_web_lists",
    comment="Deduplicated SPO web lists / document libraries (one row per (web, list)). SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="spo_web_lists",
    source="spo_web_lists_staged",
    keys=["list_silver_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - SPO Site Group Users
# MAGIC
# MAGIC **Source:** `spo_site_groups` (bronze — /_api/web/SiteGroups per site collection,
# MAGIC with embedded `Users` array)
# MAGIC **Grain:** one row per (site, sp-group, user)
# MAGIC **Key:** `membership_id` = `md5(group_id || '|' || user_id_int)` where
# MAGIC `group_id = source_key|site_url|group_id_int` (site groups are scoped to a site
# MAGIC collection — the int Id is unique within (source_key, site_url)).
# MAGIC
# MAGIC FK `site_silver_id` joins back to `silver.spo_sites`. #465 T4.
# MAGIC
# MAGIC ### Principal-claim parsing (#453)
# MAGIC The exploded user `LoginName` is decoded into `principal_type_parsed` + AAD identity columns
# MAGIC via `parse_sp_principal()`. Native `user_principal_name` and `user_external_id` (from `UserId.NameId`)
# MAGIC are preferred over claim parse when populated.

# COMMAND ----------


@dlt.table(
    name="spo_site_group_users_staged",
    comment="Exploded SPO site group memberships before deduplication",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_keys",
    "source_key IS NOT NULL AND site_url IS NOT NULL AND group_id_int IS NOT NULL AND user_id_int IS NOT NULL",
)
def spo_site_group_users_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.spo_site_groups")
        .withColumn("r", from_json(col("_record"), SPO_SITE_GROUP_SCHEMA))
        .withColumn("u", explode(col("r.Users")))
        .select(
            col("source_key"),
            col("batch_id"),
            col("_dlt_ingested_at"),
            col("r.Id").alias("group_id_int"),
            col("r.LoginName").alias("group_login_name"),
            col("r.Title").alias("group_title"),
            col("r.PrincipalType").alias("group_principal_type"),
            col("r.SiteUrl").alias("site_url"),
            col("r.WebUrl").alias("web_url"),
            col("u.Id").alias("user_id_int"),
            col("u.LoginName").alias("user_login_name"),
            col("u.Title").alias("user_title"),
            lower(trim(col("u.Email"))).alias("user_email"),
            col("u.PrincipalType").alias("user_principal_type"),
            col("u.IsSiteAdmin").alias("is_site_admin"),
            lower(trim(col("u.UserPrincipalName"))).alias("user_principal_name"),
            col("u.UserId.NameId").alias("user_external_id"),
            col("u.UserId.NameIdIssuer").alias("user_external_issuer"),
        )
        .withColumn("site_url_lc", lower(trim(col("site_url"))))
        .withColumn(
            "site_silver_id",
            md5(concat_ws("|", lower(col("source_key")), col("site_url_lc"))),
        )
        .withColumn(
            "group_id",
            concat_ws("|", lower(col("source_key")), col("site_url_lc"), col("group_id_int").cast("string")),
        )
        .withColumn(
            "membership_id",
            md5(concat_ws("|", col("group_id"), col("user_id_int").cast("string"))),
        )
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .withColumns(
            parse_sp_principal(
                login_col=col("user_login_name"),
                title_col=col("user_title"),
                principal_type_int_col=col("user_principal_type"),
                upn_native_col=col("user_principal_name"),
                user_id_native_col=col("user_external_id"),
            )
        )
        .select(
            "membership_id",
            "site_silver_id",
            "group_id",
            "group_id_int",
            "group_login_name",
            "group_title",
            "group_principal_type",
            "user_id_int",
            "user_login_name",
            "user_title",
            "user_email",
            "user_principal_type",
            "is_site_admin",
            "user_principal_name",
            "user_external_id",
            "user_external_issuer",
            *SP_PRINCIPAL_PARSED_COLS,
            "environment",
            "source_key",
            "site_url",
            "web_url",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="spo_site_group_users",
    comment="Deduplicated SPO site-group memberships exploded from spo_site_groups.Users[] (one row per (site, sp-group, user)). SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="spo_site_group_users",
    source="spo_site_group_users_staged",
    keys=["membership_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - SPO Web Role Assignments
# MAGIC
# MAGIC **Source:** `spo_web_role_assignments` (bronze — /_api/web/RoleAssignments?$expand=Member,RoleDefinitionBindings per web)
# MAGIC **Grain:** one row per (web, principal, role-binding) — RoleDefinitionBindings[] exploded
# MAGIC **Key:** `assignment_id` = `md5(web_silver_id || '|' || principal_id || '|' || role_definition_int_id)`
# MAGIC
# MAGIC FK `role_definition_id` matches `silver.spo_web_role_definitions.role_definition_id`
# MAGIC (`md5(source_key | lower(web_url) | role_definition_int_id)`). #465 T4.
# MAGIC
# MAGIC ### Principal-claim parsing (#453)
# MAGIC `Member.LoginName` is decoded into `principal_type_parsed` + AAD identity columns
# MAGIC via `parse_sp_principal()`. The `Member` object does not carry UPN/object id natively,
# MAGIC so all identity attributes derive from the claim string. Non-claim Members
# MAGIC (typically SP groups, `PrincipalType=8`) fall back to the int enum.

# COMMAND ----------


@dlt.table(
    name="spo_web_role_assignments_staged",
    comment="Exploded SPO web role assignments before deduplication",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_keys",
    "source_key IS NOT NULL AND web_url IS NOT NULL AND principal_id IS NOT NULL AND role_definition_int_id IS NOT NULL",
)
def spo_web_role_assignments_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.spo_web_role_assignments")
        .withColumn("r", from_json(col("_record"), SPO_WEB_ROLE_ASSIGNMENT_SCHEMA))
        .withColumn("rd", explode(col("r.RoleDefinitionBindings")))
        .select(
            col("source_key"),
            col("batch_id"),
            col("_dlt_ingested_at"),
            col("r.PrincipalId").alias("principal_id"),
            col("r.Member.Id").alias("member_id_int"),
            col("r.Member.LoginName").alias("member_login_name"),
            col("r.Member.Title").alias("member_title"),
            col("r.Member.PrincipalType").alias("member_principal_type"),
            col("r.SiteUrl").alias("site_url"),
            col("r.WebUrl").alias("web_url"),
            col("rd.Id").alias("role_definition_int_id"),
            col("rd.Name").alias("role_name"),
            col("rd.Description").alias("role_description"),
            col("rd.Hidden").alias("role_hidden"),
            col("rd.Order").alias("role_order"),
            col("rd.RoleTypeKind").alias("role_type_kind"),
            col("rd.BasePermissions.High").alias("base_permissions_high"),
            col("rd.BasePermissions.Low").alias("base_permissions_low"),
        )
        .withColumn("web_url_lc", lower(trim(col("web_url"))))
        .withColumn("site_url_lc", lower(trim(col("site_url"))))
        .withColumn(
            "site_silver_id",
            md5(concat_ws("|", lower(col("source_key")), col("site_url_lc"))),
        )
        .withColumn(
            "web_silver_id",
            md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"))),
        )
        .withColumn(
            "role_definition_id",
            md5(
                concat_ws(
                    "|", lower(col("source_key")), col("web_url_lc"), col("role_definition_int_id").cast("string")
                )
            ),
        )
        .withColumn(
            "assignment_id",
            md5(
                concat_ws(
                    "|",
                    col("web_silver_id"),
                    col("principal_id").cast("string"),
                    col("role_definition_int_id").cast("string"),
                )
            ),
        )
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .withColumns(
            parse_sp_principal(
                login_col=col("member_login_name"),
                title_col=col("member_title"),
                principal_type_int_col=col("member_principal_type"),
                upn_native_col=None,
                user_id_native_col=None,
            )
        )
        .select(
            "assignment_id",
            "site_silver_id",
            "web_silver_id",
            "role_definition_id",
            "principal_id",
            "member_id_int",
            "member_login_name",
            "member_title",
            "member_principal_type",
            "role_definition_int_id",
            "role_name",
            "role_description",
            "role_hidden",
            "role_order",
            "role_type_kind",
            "base_permissions_high",
            "base_permissions_low",
            *SP_PRINCIPAL_PARSED_COLS,
            "environment",
            "source_key",
            "site_url",
            "web_url",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="spo_web_role_assignments",
    comment="Deduplicated SPO web role assignments exploded from RoleDefinitionBindings[] (one row per (web, principal, role-binding)). SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="spo_web_role_assignments",
    source="spo_web_role_assignments_staged",
    keys=["assignment_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - SPO Web Item Principals
# MAGIC
# MAGIC **Source:** `spo_web_item_permissions` (bronze — GetSharingInformation per item with
# MAGIC unique permissions). The `permissionsInformation.principals[]` array is exploded.
# MAGIC **Grain:** one row per (item, principal, role)
# MAGIC **Key:** `item_principal_id` = `md5(source_key | lower(item_unique_id) | principal_id_int | role)`
# MAGIC
# MAGIC FK `list_silver_id` joins to `silver.spo_web_lists` using the same shape
# MAGIC (`md5(source_key | lower(web_url) | list_id)`). `web_silver_id` and `site_silver_id`
# MAGIC FK back to `silver.spo_webs` and `silver.spo_sites`. #465 T4.
# MAGIC
# MAGIC ### Principal-claim parsing (#453)
# MAGIC `principal_login_name` decoded into `principal_type_parsed` + AAD identity columns via
# MAGIC `parse_sp_principal()`. Native `principal_upn` and `principal_directory_object_id`
# MAGIC are preferred over claim parse when populated.

# COMMAND ----------


@dlt.table(
    name="spo_web_item_principals_staged",
    comment="Exploded SPO item principal grants before deduplication",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_keys",
    "source_key IS NOT NULL AND item_unique_id IS NOT NULL AND principal_id_int IS NOT NULL AND role IS NOT NULL AND site_url IS NOT NULL AND web_url IS NOT NULL AND list_id IS NOT NULL",
)
def spo_web_item_principals_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.spo_web_item_permissions")
        .withColumn("r", from_json(col("_record"), SPO_WEB_ITEM_PERMISSIONS_SCHEMA))
        .withColumn("p", explode(col("r.permissionsInformation.principals")))
        .select(
            col("source_key"),
            col("batch_id"),
            col("_dlt_ingested_at"),
            col("r.ItemId").alias("item_id_int"),
            col("r.itemUniqueId").alias("item_unique_id"),
            col("r.displayName").alias("item_display_name"),
            col("r.fileExtension").alias("item_file_extension"),
            col("r.FileRef").alias("item_file_ref"),
            col("r.directUrl").alias("item_direct_url"),
            col("r.itemUrl").alias("item_url"),
            col("r.ListId").alias("list_id"),
            col("r.SiteUrl").alias("site_url"),
            col("r.WebUrl").alias("web_url"),
            col("r.hasUniquePermissions").alias("has_unique_permissions"),
            col("r.sharedObjectType").alias("shared_object_type"),
            col("r.FileSystemObjectType").alias("file_system_object_type"),
            col("p.role").alias("role"),
            col("p.isInherited").alias("is_inherited"),
            col("p.principal.id").alias("principal_id_int"),
            col("p.principal.loginName").alias("principal_login_name"),
            col("p.principal.name").alias("principal_name"),
            lower(trim(col("p.principal.email"))).alias("principal_email"),
            col("p.principal.principalType").alias("principal_type"),
            lower(trim(col("p.principal.userPrincipalName"))).alias("principal_upn"),
            col("p.principal.directoryObjectId").alias("principal_directory_object_id"),
            col("p.principal.isExternal").alias("principal_is_external"),
            col("p.principal.isActive").alias("principal_is_active"),
            col("p.principal.jobTitle").alias("principal_job_title"),
            col("p.principal.expiration").alias("principal_expiration"),
        )
        .withColumn("web_url_lc", lower(trim(col("web_url"))))
        .withColumn("site_url_lc", lower(trim(col("site_url"))))
        .withColumn(
            "site_silver_id",
            md5(concat_ws("|", lower(col("source_key")), col("site_url_lc"))),
        )
        .withColumn(
            "web_silver_id",
            md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"))),
        )
        .withColumn(
            "list_silver_id",
            md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"), col("list_id"))),
        )
        .withColumn(
            "item_principal_id",
            md5(
                concat_ws(
                    "|",
                    lower(col("source_key")),
                    lower(col("item_unique_id")),
                    col("principal_id_int").cast("string"),
                    col("role").cast("string"),
                )
            ),
        )
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .withColumns(
            parse_sp_principal(
                login_col=col("principal_login_name"),
                title_col=col("principal_name"),
                principal_type_int_col=col("principal_type"),
                upn_native_col=col("principal_upn"),
                user_id_native_col=col("principal_directory_object_id"),
            )
        )
        .select(
            "item_principal_id",
            "site_silver_id",
            "web_silver_id",
            "list_silver_id",
            "item_id_int",
            "item_unique_id",
            "item_display_name",
            "item_file_extension",
            "item_file_ref",
            "item_direct_url",
            "item_url",
            "list_id",
            "principal_id_int",
            "principal_login_name",
            "principal_name",
            "principal_email",
            "principal_type",
            "principal_upn",
            "principal_directory_object_id",
            "principal_is_external",
            "principal_is_active",
            "principal_job_title",
            "principal_expiration",
            *SP_PRINCIPAL_PARSED_COLS,
            "role",
            "is_inherited",
            "has_unique_permissions",
            "shared_object_type",
            "file_system_object_type",
            "environment",
            "source_key",
            "site_url",
            "web_url",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="spo_web_item_principals",
    comment="Deduplicated SPO item principal grants exploded from permissionsInformation.principals[] (one row per (item, principal, role)). SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="spo_web_item_principals",
    source="spo_web_item_principals_staged",
    keys=["item_principal_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - SPO Web Item Links
# MAGIC
# MAGIC **Source:** `spo_web_item_permissions` (bronze — GetSharingInformation per item with
# MAGIC unique permissions). The `permissionsInformation.links[]` array is exploded.
# MAGIC **Grain:** one row per (item, sharing-link)
# MAGIC **Key:** `item_link_id` = `md5(source_key | lower(item_unique_id) | share_id | link_kind)`
# MAGIC
# MAGIC Note: many bronze rows carry placeholder `ShareId="00000000-0000-0000-0000-000000000000"`
# MAGIC representing the well-known default-link templates per item (encoding link KIND
# MAGIC availability even when no actual link exists). These are kept; filter / aggregate
# MAGIC downstream as needed. `link_kind` is included in the surrogate key to disambiguate
# MAGIC multiple template rows that share the placeholder ShareId. #465 T4.

# COMMAND ----------


@dlt.table(
    name="spo_web_item_links_staged",
    comment="Exploded SPO item sharing-link entries before deduplication",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_keys",
    "source_key IS NOT NULL AND item_unique_id IS NOT NULL AND share_id IS NOT NULL AND link_kind IS NOT NULL AND site_url IS NOT NULL AND web_url IS NOT NULL AND list_id IS NOT NULL",
)
def spo_web_item_links_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.spo_web_item_permissions")
        .withColumn("r", from_json(col("_record"), SPO_WEB_ITEM_PERMISSIONS_SCHEMA))
        .withColumn("ln", explode(col("r.permissionsInformation.links")))
        .select(
            col("source_key"),
            col("batch_id"),
            col("_dlt_ingested_at"),
            col("r.ItemId").alias("item_id_int"),
            col("r.itemUniqueId").alias("item_unique_id"),
            col("r.displayName").alias("item_display_name"),
            col("r.FileRef").alias("item_file_ref"),
            col("r.ListId").alias("list_id"),
            col("r.SiteUrl").alias("site_url"),
            col("r.WebUrl").alias("web_url"),
            col("ln.isInherited").alias("is_inherited"),
            col("ln.totalLinkMembersCount").alias("total_link_members_count"),
            col("ln.linkDetails.LinkKind").alias("link_kind"),
            col("ln.linkDetails.IsDefault").alias("is_default"),
            col("ln.linkDetails.IsEditLink").alias("is_edit_link"),
            col("ln.linkDetails.IsActive").alias("is_active"),
            col("ln.linkDetails.AllowsAnonymousAccess").alias("allows_anonymous_access"),
            col("ln.linkDetails.RequiresPassword").alias("requires_password"),
            col("ln.linkDetails.BlocksDownload").alias("blocks_download"),
            col("ln.linkDetails.HasExternalGuestInvitees").alias("has_external_guest_invitees"),
            col("ln.linkDetails.RestrictedShareMembership").alias("restricted_share_membership"),
            col("ln.linkDetails.Scope").alias("scope"),
            col("ln.linkDetails.ShareId").alias("share_id"),
            col("ln.linkDetails.ShareTokenString").alias("share_token_string"),
            col("ln.linkDetails.SharingLinkStatus").alias("sharing_link_status"),
            col("ln.linkDetails.LinkAclState").alias("link_acl_state"),
            col("ln.linkDetails.Expiration").alias("link_expiration"),
            col("ln.linkDetails.Url").alias("link_url"),
            col("ln.linkDetails.Description").alias("link_description"),
            col("ln.linkDetails.ApplicationId").alias("application_id"),
            col("ln.linkDetails.LimitUseToApplication").alias("limit_use_to_application"),
            col("ln.linkDetails.TrackLinkUsers").alias("track_link_users"),
            col("ln.linkDetails.IsUnhealthy").alias("is_unhealthy"),
            col("ln.linkDetails.Created").alias("link_created_at"),
            col("ln.linkDetails.LastModified").alias("link_last_modified_at"),
        )
        .withColumn("web_url_lc", lower(trim(col("web_url"))))
        .withColumn("site_url_lc", lower(trim(col("site_url"))))
        .withColumn(
            "site_silver_id",
            md5(concat_ws("|", lower(col("source_key")), col("site_url_lc"))),
        )
        .withColumn(
            "web_silver_id",
            md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"))),
        )
        .withColumn(
            "list_silver_id",
            md5(concat_ws("|", lower(col("source_key")), col("web_url_lc"), col("list_id"))),
        )
        .withColumn(
            "item_link_id",
            md5(
                concat_ws(
                    "|",
                    lower(col("source_key")),
                    lower(col("item_unique_id")),
                    col("share_id"),
                    coalesce(col("link_kind").cast("string"), lit("")),
                )
            ),
        )
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "item_link_id",
            "site_silver_id",
            "web_silver_id",
            "list_silver_id",
            "item_id_int",
            "item_unique_id",
            "item_display_name",
            "item_file_ref",
            "list_id",
            "share_id",
            "share_token_string",
            "link_kind",
            "is_default",
            "is_edit_link",
            "is_active",
            "is_inherited",
            "allows_anonymous_access",
            "requires_password",
            "blocks_download",
            "has_external_guest_invitees",
            "restricted_share_membership",
            "scope",
            "sharing_link_status",
            "link_acl_state",
            "link_expiration",
            "link_url",
            "link_description",
            "application_id",
            "limit_use_to_application",
            "track_link_users",
            "is_unhealthy",
            "total_link_members_count",
            "link_created_at",
            "link_last_modified_at",
            "environment",
            "source_key",
            "site_url",
            "web_url",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="spo_web_item_links",
    comment="Deduplicated SPO item sharing-link entries exploded from permissionsInformation.links[] (one row per (item, sharing-link)). SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="spo_web_item_links",
    source="spo_web_item_links_staged",
    keys=["item_link_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Mailboxes
# MAGIC
# MAGIC **Source:** `exo_mailboxes`
# MAGIC **Key:** `mailbox_id` = `{source_key}_{Guid}`
# MAGIC
# MAGIC EXO mailboxes from source and target tenants (Get-EXOMailbox -PropertySets All).
# MAGIC Size metrics (total_size_mb, item_count) are in `exo_mailbox_statistics` bronze —
# MAGIC join on `exchange_guid` in gold for the enriched view.

# COMMAND ----------


@dlt.table(name="mailboxes_staged", comment="Staged EXO mailbox records before deduplication", temporary=True)
@dlt.expect_or_drop("valid_guid", "source_key IS NOT NULL AND mailbox_id IS NOT NULL AND mailbox_id != source_key")
def mailboxes_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.exo_mailboxes")
        .withColumn("r", from_json(col("_record"), EXO_MAILBOXES_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("mailbox_id", concat_ws("_", col("source_key"), col("Guid")))
        .withColumn("exchange_guid", col("ExchangeGuid"))
        .withColumn("external_directory_object_id", lower(trim(col("ExternalDirectoryObjectId"))))
        .withColumn("display_name", trim(col("DisplayName")))
        .withColumn("alias", col("Alias"))
        .withColumn("user_principal_name", lower(trim(col("UserPrincipalName"))))
        .withColumn("primary_smtp_address", lower(trim(col("PrimarySmtpAddress"))))
        .withColumn("email_addresses", col("EmailAddresses"))
        .withColumn("recipient_type", col("RecipientTypeDetails"))
        .withColumn(
            "has_archive",
            when(col("ArchiveStatus").isNotNull() & (col("ArchiveStatus") != "None"), lit(True)).otherwise(lit(False)),
        )
        .withColumn("litigation_hold", col("LitigationHoldEnabled").cast("boolean"))
        .withColumn("litigation_hold_date", to_timestamp(col("LitigationHoldDate")))
        .withColumn("forwarding_address", col("ForwardingAddress"))
        .withColumn(
            "forwarding_smtp_address",
            lower(regexp_replace(trim(col("ForwardingSmtpAddress")), "^(?i)smtp:", "")),
        )
        .withColumn("deliver_to_mailbox_and_forward", col("DeliverToMailboxAndForward").cast("boolean"))
        .withColumn("environment", environment_col())
        .withColumn("source_created_at", to_timestamp(col("WhenCreated")))
        .withColumn("last_changed_at", to_timestamp(col("WhenChanged")))
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "mailbox_id",
            "exchange_guid",
            "external_directory_object_id",
            "environment",
            "source_key",
            "display_name",
            "alias",
            "user_principal_name",
            "primary_smtp_address",
            "email_addresses",
            "recipient_type",
            "has_archive",
            "litigation_hold",
            "litigation_hold_date",
            "forwarding_address",
            "forwarding_smtp_address",
            "deliver_to_mailbox_and_forward",
            "source_created_at",
            "last_changed_at",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="mailboxes",
    comment="Deduplicated EXO mailboxes from source and target tenants. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="mailboxes",
    source="mailboxes_staged",
    keys=["mailbox_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - EXO Mail Contacts (Target Only)
# MAGIC
# MAGIC **Source:** `exo_contacts`
# MAGIC **Key:** `exo_contact_id` = `{source_key}_{Guid}`
# MAGIC
# MAGIC EXO Mail Contacts are external address entries in the target tenant GAL,
# MAGIC created by Cross-Tenant Sync or hybrid writeback. Distinct from Entra Contacts.
# MAGIC
# MAGIC **Key fields:**
# MAGIC - `external_email_address` — stripped of `SMTP:` prefix; the external routing address.
# MAGIC   Used in Match 1: source user `proxy_addresses` contains this value.
# MAGIC - `external_directory_object_id` — Entra Contact `id` for this contact object.
# MAGIC   Used in Match 2: bridges EXO Contact → Entra Contact.

# COMMAND ----------


@dlt.table(
    name="exo_contacts_staged",
    comment="Staged EXO Mail Contact records (target tenant only) before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop("valid_guid", "guid IS NOT NULL")
@dlt.expect("valid_external_email", "external_email_address IS NOT NULL")
def exo_contacts_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.exo_contacts")
        .withColumn("r", from_json(col("_record"), EXO_CONTACTS_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("exo_contact_id", concat_ws("_", col("source_key"), col("Guid")))
        .withColumn("guid", col("Guid"))
        .withColumn("environment", environment_col())
        .withColumn("display_name", trim(col("DisplayName")))
        .withColumn("alias", col("Alias"))
        .withColumn("primary_smtp_address", lower(trim(col("PrimarySmtpAddress"))))
        .withColumn(
            "external_email_address", lower(regexp_replace(trim(col("ExternalEmailAddress")), "^(?i)smtp:", ""))
        )
        .withColumn("external_directory_object_id", col("ExternalDirectoryObjectId"))
        .withColumn("recipient_type_details", col("RecipientTypeDetails"))
        .withColumn("is_dir_synced", col("IsDirSynced").cast("boolean"))
        .withColumn("hidden_from_address_lists", col("HiddenFromAddressListsEnabled").cast("boolean"))
        .withColumn("source_created_at", to_timestamp(col("WhenCreated")))
        .withColumn("last_changed_at", to_timestamp(col("WhenChanged")))
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "exo_contact_id",
            "guid",
            "environment",
            "source_key",
            "display_name",
            "alias",
            "primary_smtp_address",
            "external_email_address",
            "external_directory_object_id",
            "recipient_type_details",
            "is_dir_synced",
            "hidden_from_address_lists",
            "source_created_at",
            "last_changed_at",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="exo_contacts",
    comment="Deduplicated EXO Mail Contacts from target tenant. SCD Type 1. Used for Entity 3 contact chain resolution.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="exo_contacts",
    source="exo_contacts_staged",
    keys=["exo_contact_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Contact Entity Mapping
# MAGIC
# MAGIC **Source:** `users` (source), `exo_contacts` (target), `contacts` (target Entra)
# MAGIC **Key:** `mapping_id`
# MAGIC
# MAGIC Three deterministic mapping types forming the Entity 3 contact chain:
# MAGIC
# MAGIC | mapping_type | From | Match Key | To | Sprint |
# MAGIC |---|---|---|---|---|
# MAGIC | `source_user_to_exo_contact` | Source user `proxy_addresses` | contains EXO contact `external_email_address` | Target EXO contact | 1 |
# MAGIC | `exo_contact_to_entra_contact` | EXO contact `external_directory_object_id` | = Entra contact `entra_object_id` | Target Entra contact | 1 |
# MAGIC | `source_user_to_ad_contact` | Source user `proxy_addresses` | contains AD contact `targetAddress` | Target AD contact | 2 — stubbed null |
# MAGIC
# MAGIC All three types are system-level deterministic joins — no human review needed.
# MAGIC Sprint 2 adds `source_user_to_ad_contact` once AD ingestion is available.

# COMMAND ----------


@dlt.table(
    name="contact_entity_mapping",
    comment="Entity 3 contact chain: source users to EXO contacts, EXO contacts to Entra contacts. Three mapping types.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
def contact_entity_mapping():
    # --- Sources ---
    source_users = dlt.read("users").filter(
        (col("environment") == "source") & (col("user_type") == "Member") & col("proxy_addresses").isNotNull()
    )

    exo_contacts = dlt.read("exo_contacts").filter(col("external_email_address").isNotNull())

    entra_contacts_target = (
        dlt.read("contacts").filter(col("environment") == "target").filter(col("entra_object_id").isNotNull())
    )

    # --- Match 1: source_user_to_exo_contact ---
    # Source user proxy_addresses (strip prefix at match time) contains EXO contact external_email_address
    # external_email_address is already stripped in exo_contacts Silver.
    source_users_exploded = (
        source_users.select(
            col("user_id").alias("source_user_id"),
            col("entra_object_id").alias("source_entra_object_id"),
            explode(col("proxy_addresses")).alias("raw_addr"),
        )
        .withColumn("clean_addr", lower(regexp_replace(trim(col("raw_addr")), "^(?i)(smtp:|sip:|x500:|x400:)", "")))
        .filter(col("clean_addr") != "")
    )

    match1 = (
        source_users_exploded.alias("su")
        .join(exo_contacts.alias("ec"), col("su.clean_addr") == col("ec.external_email_address"), "inner")
        .select(
            concat_ws("_", lit("m1"), col("su.source_user_id"), col("ec.exo_contact_id")).alias("mapping_id"),
            lit("source_user_to_exo_contact").alias("mapping_type"),
            # Source side
            col("su.source_user_id").alias("source_id"),
            col("su.source_entra_object_id").alias("source_entra_object_id"),
            col("su.clean_addr").alias("matched_on_address"),
            # Target side — EXO contact
            col("ec.exo_contact_id").alias("target_id"),
            col("ec.external_email_address").alias("target_external_email_address"),
            col("ec.external_directory_object_id").alias("target_external_directory_object_id"),
            col("ec.is_dir_synced").alias("target_is_dir_synced"),
            # Entra contact fields null for Match 1
            lit(None).cast("string").alias("target_entra_contact_id"),
            lit(None).cast("string").alias("target_entra_object_id"),
            lit(None).cast("string").alias("target_entra_mail"),
            current_timestamp().alias("mapped_at"),
            lit("proxy_address_contains_external_email").alias("match_rule"),
        )
    )

    # --- Match 2: exo_contact_to_entra_contact ---
    # EXO contact ExternalDirectoryObjectId = Entra contact id (confirmed by Jeff)
    match2 = (
        exo_contacts.alias("ec")
        .join(
            entra_contacts_target.alias("tc"),
            col("ec.external_directory_object_id") == col("tc.entra_object_id"),
            "inner",
        )
        .select(
            concat_ws("_", lit("m2"), col("ec.exo_contact_id"), col("tc.contact_id")).alias("mapping_id"),
            lit("exo_contact_to_entra_contact").alias("mapping_type"),
            # Source side — EXO contact
            col("ec.exo_contact_id").alias("source_id"),
            col("ec.external_directory_object_id").alias("source_entra_object_id"),
            col("ec.external_email_address").alias("matched_on_address"),
            # Target side — Entra contact
            col("ec.exo_contact_id").alias("target_id"),
            col("ec.external_email_address").alias("target_external_email_address"),
            col("ec.external_directory_object_id").alias("target_external_directory_object_id"),
            col("ec.is_dir_synced").alias("target_is_dir_synced"),
            col("tc.contact_id").alias("target_entra_contact_id"),
            col("tc.entra_object_id").alias("target_entra_object_id"),
            col("tc.mail").alias("target_entra_mail"),
            current_timestamp().alias("mapped_at"),
            lit("external_directory_object_id_match").alias("match_rule"),
        )
    )

    # --- Match 3: source_user_to_ad_contact ---
    # Sprint 2 — AD ingestion not yet available. Return empty DataFrame with correct schema.
    match3 = match1.filter(lit(False)).withColumn(  # Empty — preserves schema for union
        "mapping_type", lit("source_user_to_ad_contact")
    )

    return match1.unionByName(match2).unionByName(match3).dropDuplicates(["mapping_id"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - User Mapping Candidates
# MAGIC
# MAGIC **Three Matching Contexts:**
# MAGIC
# MAGIC | match_context | Source | Target | Pass | Score | Auto-approve |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `primary_migration` | Source Member | Target Member | Pass 1: `employee_id` + `user_principal_name` | 95 | Yes - score >= 85 AND one_to_one |
# MAGIC | `primary_migration` | Source Member | Target Member | Pass 2: `employee_id` only | 80 | Never (score < 85) |
# MAGIC | `external_linkage` | Source Member | Target Mail User | Source `proxy_addresses` contains mail user `external_email_address` | 90 | Yes - one_to_one |
# MAGIC | `ad_user_linkage` | Target Member | Target AD User | Target Entra `on_prem_distinguished_name` = AD `DistinguishedName` | 95 | Yes - one_to_one |
# MAGIC
# MAGIC **Notes:**
# MAGIC - `external_linkage` `target_user_id` holds `mail_user_id` (not an Entra user_id)
# MAGIC - `ad_user_linkage` `target_user_id` will hold an AD user key (Sprint 2)
# MAGIC - Cardinality is calculated per `match_context` to avoid cross-context interference

# COMMAND ----------


@dlt.table(
    name="user_mapping_candidates",
    comment="""
    User mapping candidates across three matching contexts.

    match_context values:
      primary_migration  - Source Member -> Target Member via employee_id / on-prem UPN
      external_linkage   - Source Member -> Target Mail User via proxy address match
      ad_user_linkage    - Target Member -> Target AD User via DN match

    Auto-approval: score >= 85 AND mapping_scenario = one_to_one
      Exception: primary_migration Pass 2 (score 80) never auto-approved.

    target_user_id holds different key types per context:
      primary_migration : Entra user_id  (target_{entra_object_id})
      external_linkage  : mail_user_id   (target_{guid})
      ad_user_linkage   : ad_user_id
    """,
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
def user_mapping_candidates():
    from pyspark.sql.functions import countDistinct

    users = dlt.read("users")
    mail_users = dlt.read("mail_users")

    source_users = users.filter((col("environment") == "source") & (col("user_type") == "Member"))
    target_members = users.filter((col("environment") == "target") & (col("user_type") == "Member"))

    # =========================================================================
    # CONTEXT: primary_migration
    # Source Member -> Target Member via extension attributes
    # =========================================================================

    # --- Pass 1: employee_id AND user_principal_name (score 95) ---
    pm_pass1 = (
        source_users.alias("s")
        .join(
            target_members.alias("t"),
            (lower(trim(col("s.employee_id"))) == lower(trim(col("t.employee_id"))))
            & (lower(trim(col("s.user_principal_name"))) == lower(trim(col("t.user_principal_name"))))
            & col("s.employee_id").isNotNull()
            & col("s.user_principal_name").isNotNull()
            & (trim(col("s.employee_id")) != "")
            & (trim(col("s.user_principal_name")) != "")
            & col("t.employee_id").isNotNull()
            & (trim(col("t.employee_id")) != "")
            & col("t.user_principal_name").isNotNull()
            & (trim(col("t.user_principal_name")) != ""),
            "inner",
        )
        .select(
            col("s.user_id").alias("source_user_id"),
            col("t.user_id").alias("target_user_id"),
            lit(95.0).cast("decimal(5,2)").alias("match_score"),
            lit("employee_id_and_upn").alias("match_type"),
            lit("primary_migration").alias("match_context"),
        )
    )

    # --- Pass 2: employee_id only (score 80) ---
    # Score < 85 so never auto-approved regardless of cardinality
    pm_pass2 = (
        source_users.alias("s")
        .join(
            target_members.alias("t"),
            (lower(trim(col("s.employee_id"))) == lower(trim(col("t.employee_id"))))
            & col("s.employee_id").isNotNull()
            & (trim(col("s.employee_id")) != "")
            & col("t.employee_id").isNotNull()
            & (trim(col("t.employee_id")) != ""),
            "inner",
        )
        .select(
            col("s.user_id").alias("source_user_id"),
            col("t.user_id").alias("target_user_id"),
            lit(80.0).cast("decimal(5,2)").alias("match_score"),
            lit("employee_id_only").alias("match_type"),
            lit("primary_migration").alias("match_context"),
        )
    )

    # =========================================================================
    # CONTEXT: external_linkage
    # Source Member -> Target Mail User via proxy address match
    # target_user_id holds mail_user_id (not an Entra user_id)
    # proxy_addresses on source users are stored as raw arrays (prefixes NOT stripped in Silver)
    # external_email_address on mail users IS prefix-stripped in mail_users_staged
    # =========================================================================

    # Explode and strip source proxy addresses for matching (done here, not at storage)
    source_proxies_stripped = (
        source_users.filter(col("proxy_addresses").isNotNull())
        .select(col("user_id").alias("source_user_id"), explode(col("proxy_addresses")).alias("raw_addr"))
        .withColumn("clean_addr", lower(regexp_replace(col("raw_addr"), "^(?i)(smtp:|sip:|x500:|x400:)", "")))
        .filter(col("clean_addr") != "")
        .select("source_user_id", "clean_addr")
        .distinct()
    )

    el_pass1 = (
        source_proxies_stripped.alias("sp")
        .join(
            mail_users.filter(
                col("external_email_address").isNotNull() & (trim(col("external_email_address")) != "")
            ).alias("m"),
            col("sp.clean_addr") == col("m.external_email_address"),
            "inner",
        )
        .select(
            col("sp.source_user_id"),
            col("m.mail_user_id").alias("target_user_id"),
            lit(90.0).cast("decimal(5,2)").alias("match_score"),
            lit("proxy_address_match").alias("match_type"),
            lit("external_linkage").alias("match_context"),
        )
    )

    # =========================================================================
    # CONTEXT: ad_user_linkage (Sprint 2 - pending ad_users bronze data)
    # Target Member -> Target AD User via DistinguishedName match (score 95)
    # Will be added when ad_users/ad_contacts arrive in landing.
    # =========================================================================

    # =========================================================================
    # Combine all contexts
    # =========================================================================
    all_matches = pm_pass1.unionByName(pm_pass2).unionByName(el_pass1)

    # Best score per (source, target, context) pair
    best_matches = (
        all_matches.groupBy("source_user_id", "target_user_id", "match_context")
        .agg(spark_max("match_score").alias("match_score"), collect_set("match_type").alias("match_types_arr"))
        .withColumn(
            "match_type",
            when(array_contains(col("match_types_arr"), "employee_id_and_upn"), lit("employee_id_and_upn"))
            .when(array_contains(col("match_types_arr"), "proxy_address_match"), lit("proxy_address_match"))
            .otherwise(lit("employee_id_only")),
        )
        .withColumn("match_attributes", concat_ws(",", col("match_types_arr")))
    )

    # --- Cardinality detection (scoped per match_context) ---
    source_cardinality = best_matches.groupBy("source_user_id", "match_context").agg(
        countDistinct("target_user_id").alias("target_count")
    )
    target_cardinality = best_matches.groupBy("target_user_id", "match_context").agg(
        countDistinct("source_user_id").alias("source_count")
    )

    with_cardinality = best_matches.join(source_cardinality, ["source_user_id", "match_context"], "left").join(
        target_cardinality, ["target_user_id", "match_context"], "left"
    )

    return (
        with_cardinality.withColumn(
            "mapping_scenario",
            when((col("source_count") == 1) & (col("target_count") == 1), lit("one_to_one"))
            .when((col("source_count") == 1) & (col("target_count") > 1), lit("one_to_many"))
            .when((col("source_count") > 1) & (col("target_count") == 1), lit("many_to_one"))
            .otherwise(lit("many_to_many")),
        )
        # Auto-approve: score >= 85 AND one_to_one
        # primary_migration pass2 (score 80) naturally excluded by score threshold
        .withColumn(
            "candidate_status",
            when((col("match_score") >= 85) & (col("mapping_scenario") == "one_to_one"), lit("approved")).otherwise(
                lit("pending_review")
            ),
        )
        .withColumn("candidate_id", concat_ws("_", col("source_user_id"), col("target_user_id"), col("match_context")))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .select(
            "candidate_id",
            "source_user_id",
            "target_user_id",
            "match_context",
            "match_score",
            "match_type",
            "match_attributes",
            "mapping_scenario",
            "source_count",
            "target_count",
            "candidate_status",
            "created_at",
            "updated_at",
        )
    )


# COMMAND ----------


@dlt.table(
    name="group_mapping_candidates",
    comment="Group mapping candidates between source and target tenants with multi-key matching.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
def group_mapping_candidates():
    groups = dlt.read("groups")
    source_groups = groups.filter(col("environment") == "source")
    target_groups = groups.filter(col("environment") == "target")

    # --- Pass 1: Exact mail match (score: 90) ---
    mail_matches = (
        source_groups.alias("s")
        .join(target_groups.alias("t"), col("s.mail") == col("t.mail"), "inner")  # Already lowercased
        .filter(col("s.mail").isNotNull())
        .select(
            col("s.group_id").alias("source_group_id"),
            col("t.group_id").alias("target_group_id"),
            lit(90.0).cast("decimal(5,2)").alias("match_score"),
            lit("exact_mail").alias("match_type"),
        )
    )

    # --- Pass 2: Shared SMTP proxy address (score: 85) ---
    source_proxies = (
        source_groups.filter(col("proxy_addresses").isNotNull())
        .select(col("group_id").alias("source_group_id"), explode(col("proxy_addresses")).alias("raw_addr"))
        .filter(col("raw_addr").rlike("^(?i)smtp:"))
        .withColumn("clean_addr", lower(regexp_replace(col("raw_addr"), "^(?i)smtp:", "")))
        .select("source_group_id", "clean_addr")
        .distinct()
    )
    target_proxies = (
        target_groups.filter(col("proxy_addresses").isNotNull())
        .select(col("group_id").alias("target_group_id"), explode(col("proxy_addresses")).alias("raw_addr"))
        .filter(col("raw_addr").rlike("^(?i)smtp:"))
        .withColumn("clean_addr", lower(regexp_replace(col("raw_addr"), "^(?i)smtp:", "")))
        .select("target_group_id", "clean_addr")
        .distinct()
    )
    proxy_matches = (
        source_proxies.alias("s")
        .join(target_proxies.alias("t"), col("s.clean_addr") == col("t.clean_addr"), "inner")
        .select(
            col("s.source_group_id"),
            col("t.target_group_id"),
            lit(85.0).cast("decimal(5,2)").alias("match_score"),
            lit("shared_proxy_address").alias("match_type"),
        )
        .distinct()
    )

    # --- Pass 3: Exact display_name match (score: 75) ---
    name_matches = (
        source_groups.alias("s")
        .join(
            target_groups.alias("t"), lower(trim(col("s.display_name"))) == lower(trim(col("t.display_name"))), "inner"
        )
        .filter(col("s.display_name").isNotNull())
        .select(
            col("s.group_id").alias("source_group_id"),
            col("t.group_id").alias("target_group_id"),
            lit(75.0).cast("decimal(5,2)").alias("match_score"),
            lit("exact_display_name").alias("match_type"),
        )
    )

    # --- Combine all passes ---
    all_matches = mail_matches.unionByName(proxy_matches).unionByName(name_matches)

    # Aggregate: one row per (source, target) pair
    return (
        all_matches.groupBy("source_group_id", "target_group_id")
        .agg(spark_max("match_score").alias("match_score"), collect_set("match_type").alias("match_types_arr"))
        .withColumn("candidate_id", concat_ws("_", col("source_group_id"), col("target_group_id")))
        .withColumn(
            "match_type",
            when(array_contains(col("match_types_arr"), "exact_mail"), lit("exact_mail"))
            .when(array_contains(col("match_types_arr"), "shared_proxy_address"), lit("shared_proxy_address"))
            .otherwise(lit("exact_display_name")),
        )
        .withColumn("match_attributes", concat_ws(",", col("match_types_arr")))
        .withColumn("candidate_status", lit("pending"))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .select(
            "candidate_id",
            "source_group_id",
            "target_group_id",
            "match_score",
            "match_type",
            "match_attributes",
            "candidate_status",
            "created_at",
            "updated_at",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - EXO Mail Users
# MAGIC
# MAGIC **Source:** `exo_mail_users`
# MAGIC **Key:** `mail_user_id` = `{source_key}_{Guid}`
# MAGIC
# MAGIC EXO Mail Users from both source and target tenants. Target-tenant rows are the
# MAGIC shadow objects created by Cross-Tenant Sync (one per source-tenant user as
# MAGIC seen from the target tenant). Source-tenant rows surface mail-enabled users
# MAGIC whose mail is routed externally (forwarded out of the source tenant).
# MAGIC
# MAGIC Key fields for identity chain resolution:
# MAGIC - `external_email_address` — stripped of `SMTP:` prefix; on target rows matches
# MAGIC   source user's proxy_addresses for `external_linkage` in user_mapping_candidates;
# MAGIC   on source rows is the external routing target used by #485 migration_status
# MAGIC   rule (a) (source mail user pointing at a target mailbox proxy address).
# MAGIC - `external_directory_object_id` — Entra objectId of the corresponding MTO user
# MAGIC   in the target tenant; used by `mto_user_entity_mapping` to resolve Chain 1
# MAGIC
# MAGIC RecipientTypeDetails values expected: `MailUser`, `GuestMailUser`

# COMMAND ----------


@dlt.table(
    name="mail_users_staged",
    comment="Staged EXO Mail User records (source and target tenants) before deduplication",
    temporary=True,
)
@dlt.expect_or_drop("valid_guid", "guid IS NOT NULL")
@dlt.expect("valid_external_email", "external_email_address IS NOT NULL")
def mail_users_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.exo_mail_users")
        .withColumn("r", from_json(col("_record"), EXO_MAIL_USERS_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("mail_user_id", concat_ws("_", col("source_key"), col("Guid")))
        .withColumn("guid", col("Guid"))
        .withColumn("environment", environment_col())
        .withColumn("display_name", trim(col("DisplayName")))
        .withColumn("given_name", trim(col("FirstName")))
        .withColumn("surname", trim(col("LastName")))
        .withColumn("alias", col("Alias"))
        .withColumn("primary_smtp_address", lower(trim(col("PrimarySmtpAddress"))))
        .withColumn(
            "external_email_address", lower(regexp_replace(trim(col("ExternalEmailAddress")), "^(?i)smtp:", ""))
        )
        .withColumn("external_directory_object_id", col("ExternalDirectoryObjectId"))
        .withColumn("recipient_type_details", col("RecipientTypeDetails"))
        .withColumn("is_dir_synced", col("IsDirSynced").cast("boolean"))
        .withColumn("hidden_from_address_lists", col("HiddenFromAddressListsEnabled").cast("boolean"))
        .withColumn("company", col("Company"))
        .withColumn("department", col("Department"))
        .withColumn("job_title", col("Title"))
        .withColumn("source_created_at", to_timestamp(col("WhenCreated")))
        .withColumn("last_changed_at", to_timestamp(col("WhenChanged")))
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "mail_user_id",
            "guid",
            "environment",
            "source_key",
            "display_name",
            "given_name",
            "surname",
            "alias",
            "primary_smtp_address",
            "external_email_address",
            "external_directory_object_id",
            "recipient_type_details",
            "is_dir_synced",
            "hidden_from_address_lists",
            "company",
            "department",
            "job_title",
            "source_created_at",
            "last_changed_at",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="mail_users",
    comment="Deduplicated EXO Mail Users from source and target tenants. SCD Type 1. Used for external_linkage and Chain 1 MTO resolution (target rows) and #485 migration_status rule (a) (source rows pointing at target mailbox proxy addresses).",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="mail_users",
    source="mail_users_staged",
    keys=["mail_user_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - EXO Unified Groups
# MAGIC
# MAGIC **Source:** `exo_unified_groups`
# MAGIC **Key:** `unified_group_id` = `{source_key}_{ExternalDirectoryObjectId}`
# MAGIC
# MAGIC M365 Unified Groups (group mailbox side) from both source and target tenants.
# MAGIC Provides the group_mailbox FK source for gold.shared_data_sets.
# MAGIC
# MAGIC Key join fields:
# MAGIC - `entra_object_id` — joins to `silver.groups.entra_object_id` and `silver.teams.entra_object_id`
# MAGIC - `exchange_guid` — Exchange GUID for the group mailbox
# MAGIC - `primary_smtp_address` — mail address of the group
# MAGIC - `is_team_enabled` — derived from ResourceProvisioningOptions containing 'Team'
# MAGIC - `managed_by` — array of owner object IDs used for downstream owner resolution
# MAGIC
# MAGIC RecipientTypeDetails expected: `GroupMailbox`

# COMMAND ----------


@dlt.table(
    name="exo_unified_groups_staged",
    comment="Staged EXO Unified Group records (source and target tenants) before deduplication",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_external_id", "source_key IS NOT NULL AND unified_group_id IS NOT NULL AND unified_group_id != source_key"
)
def exo_unified_groups_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.exo_unified_groups")
        .withColumn("r", from_json(col("_record"), EXO_UNIFIED_GROUPS_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("unified_group_id", concat_ws("_", col("source_key"), col("ExternalDirectoryObjectId")))
        .withColumn("entra_object_id", col("ExternalDirectoryObjectId"))
        .withColumn("exchange_guid", col("ExchangeGuid"))
        .withColumn("display_name", trim(col("DisplayName")))
        .withColumn("description", col("Notes"))
        .withColumn("alias", col("Alias"))
        .withColumn("primary_smtp_address", lower(trim(col("PrimarySmtpAddress"))))
        .withColumn("group_sku", col("GroupSKU"))
        .withColumn("group_type", col("GroupType"))
        .withColumn("recipient_type", col("RecipientType"))
        .withColumn("recipient_type_details", col("RecipientTypeDetails"))
        .withColumn("access_type", col("AccessType"))
        .withColumn(
            "is_team_enabled",
            coalesce(array_contains(col("ResourceProvisioningOptions"), lit("Team")), lit(False)),
        )
        .withColumn("member_count", col("GroupMemberCount").cast("int"))
        .withColumn("external_member_count", col("GroupExternalMemberCount").cast("int"))
        .withColumn("allow_add_guests", col("AllowAddGuests").cast("boolean"))
        .withColumn("hidden_from_address_lists", col("HiddenFromAddressListsEnabled").cast("boolean"))
        .withColumn("hidden_from_exchange_clients", col("HiddenFromExchangeClientsEnabled").cast("boolean"))
        .withColumn("hidden_group_membership", col("HiddenGroupMembershipEnabled").cast("boolean"))
        .withColumn("is_mailbox_configured", col("IsMailboxConfigured").cast("boolean"))
        .withColumn("is_membership_dynamic", col("IsMembershipDynamic").cast("boolean"))
        .withColumn("welcome_message_enabled", col("WelcomeMessageEnabled").cast("boolean"))
        .withColumn("subscription_enabled", col("SubscriptionEnabled").cast("boolean"))
        .withColumn("auto_subscribe_new_members", col("AutoSubscribeNewMembers").cast("boolean"))
        .withColumn("classification", col("Classification"))
        .withColumn("sensitivity_label", col("SensitivityLabel"))
        .withColumn("sharepoint_site_url", col("SharePointSiteUrl"))
        .withColumn("sharepoint_documents_url", col("SharePointDocumentsUrl"))
        .withColumn("sharepoint_notebook_url", col("SharePointNotebookUrl"))
        .withColumn("audit_log_age_limit", col("AuditLogAgeLimit"))
        .withColumn("information_barrier_mode", col("InformationBarrierMode"))
        .withColumn("managed_by", col("ManagedBy"))
        .withColumn("requires_sender_authentication", col("RequireSenderAuthenticationEnabled").cast("boolean"))
        .withColumn("source_created_at", to_timestamp(col("WhenCreated")))
        .withColumn("last_changed_at", to_timestamp(col("WhenChanged")))
        .withColumn("source_soft_deleted_at", to_timestamp(col("WhenSoftDeleted")))
        .withColumn("expiration_time", to_timestamp(col("ExpirationTime")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "unified_group_id",
            "entra_object_id",
            "exchange_guid",
            "display_name",
            "description",
            "alias",
            "primary_smtp_address",
            "group_sku",
            "group_type",
            "recipient_type",
            "recipient_type_details",
            "access_type",
            "is_team_enabled",
            "member_count",
            "external_member_count",
            "allow_add_guests",
            "hidden_from_address_lists",
            "hidden_from_exchange_clients",
            "hidden_group_membership",
            "is_mailbox_configured",
            "is_membership_dynamic",
            "welcome_message_enabled",
            "subscription_enabled",
            "auto_subscribe_new_members",
            "classification",
            "sensitivity_label",
            "sharepoint_site_url",
            "sharepoint_documents_url",
            "sharepoint_notebook_url",
            "audit_log_age_limit",
            "information_barrier_mode",
            "managed_by",
            "requires_sender_authentication",
            "source_created_at",
            "last_changed_at",
            "source_soft_deleted_at",
            "expiration_time",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="exo_unified_groups",
    comment="Deduplicated EXO Unified Groups (M365 Groups with mailboxes) from source and target tenants. SCD Type 1. Feeds group_mailbox FK for gold.shared_data_sets and provides mailbox-side metadata for Teams and M365 Groups. Carries `requires_sender_authentication` for #53 (gold.groups_accept_external_email).",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="exo_unified_groups",
    source="exo_unified_groups_staged",
    keys=["unified_group_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - EXO Distribution Groups
# MAGIC
# MAGIC **Source:** `exo_distribution_groups`
# MAGIC **Key:** `distribution_group_id` = `{source_key}_{ExternalDirectoryObjectId}`
# MAGIC
# MAGIC EXO-side metadata for traditional distribution lists AND mail-enabled
# MAGIC security groups, both sourced from `Get-DistributionGroup`. Distinguish
# MAGIC the two via `recipient_type_details`:
# MAGIC
# MAGIC | `recipient_type_details` | Meaning |
# MAGIC |---|---|
# MAGIC | `MailUniversalDistributionGroup` | Distribution List |
# MAGIC | `MailUniversalSecurityGroup`     | Mail-Enabled Security |
# MAGIC | `DynamicDistributionGroup`       | Dynamic DL |
# MAGIC
# MAGIC Joined back to `silver.groups` via `entra_object_id` for Entra-side
# MAGIC identity (mail, proxy_addresses, source_key, environment).
# MAGIC
# MAGIC The flag `requires_sender_authentication` is the inverse of "accepts
# MAGIC email from the internet" used by gold.groups_accept_external_email (#53).

# COMMAND ----------


@dlt.table(
    name="exo_distribution_groups_staged",
    comment="Staged EXO Distribution Group records (source and target tenants) before deduplication",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_external_id",
    "source_key IS NOT NULL AND distribution_group_id IS NOT NULL AND distribution_group_id != source_key",
)
def exo_distribution_groups_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.exo_distribution_groups")
        .withColumn("r", from_json(col("_record"), EXO_DISTRIBUTION_GROUPS_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("distribution_group_id", concat_ws("_", col("source_key"), col("ExternalDirectoryObjectId")))
        .withColumn("entra_object_id", col("ExternalDirectoryObjectId"))
        .withColumn("exchange_object_id", col("ExchangeObjectId"))
        .withColumn("display_name", trim(col("DisplayName")))
        .withColumn("alias", col("Alias"))
        .withColumn("primary_smtp_address", lower(trim(col("PrimarySmtpAddress"))))
        .withColumn("group_type", col("GroupType"))
        .withColumn("recipient_type", col("RecipientType"))
        .withColumn("recipient_type_details", col("RecipientTypeDetails"))
        .withColumn("hidden_from_address_lists", col("HiddenFromAddressListsEnabled").cast("boolean"))
        .withColumn("moderation_enabled", col("ModerationEnabled").cast("boolean"))
        .withColumn("requires_sender_authentication", col("RequireSenderAuthenticationEnabled").cast("boolean"))
        .withColumn("member_join_restriction", col("MemberJoinRestriction"))
        .withColumn("member_depart_restriction", col("MemberDepartRestriction"))
        .withColumn("managed_by", col("ManagedBy"))
        .withColumn("source_created_at", to_timestamp(col("WhenCreated")))
        .withColumn("last_changed_at", to_timestamp(col("WhenChanged")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "distribution_group_id",
            "entra_object_id",
            "exchange_object_id",
            "display_name",
            "alias",
            "primary_smtp_address",
            "group_type",
            "recipient_type",
            "recipient_type_details",
            "hidden_from_address_lists",
            "moderation_enabled",
            "requires_sender_authentication",
            "member_join_restriction",
            "member_depart_restriction",
            "managed_by",
            "source_created_at",
            "last_changed_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="exo_distribution_groups",
    comment="Deduplicated EXO Distribution Groups (traditional DLs + mail-enabled security; from Get-DistributionGroup) for source and target tenants. SCD Type 1. Carries `requires_sender_authentication` for #53 (gold.groups_accept_external_email). Joined back to silver.groups via `entra_object_id`.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="exo_distribution_groups",
    source="exo_distribution_groups_staged",
    keys=["distribution_group_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - MTO User Entity Mapping
# MAGIC
# MAGIC **Source:** `mail_users`, `users` (silver tables)
# MAGIC **Key:** `mapping_id` = `mu_{mail_user_id}_{entra_user_id}`
# MAGIC
# MAGIC Resolves Chain 1 of Jeff's rationalization architecture:
# MAGIC   EXO Mail User → Entra MTO User
# MAGIC
# MAGIC Match condition (deterministic FK join, no candidate scoring needed):
# MAGIC   `mail_users.external_directory_object_id` = `users.entra_object_id`
# MAGIC   WHERE `users.environment = 'target'`
# MAGIC
# MAGIC This produces rationalization fields 3 and 4:
# MAGIC   `TargetTenantMtoUserObjectId`        → `entra_object_id`
# MAGIC   `TargetTenantMtoUserPrincipalName`   → `entra_upn`
# MAGIC
# MAGIC Assumption (confirmed with Jeff): CTS always creates both the EXO Mail User
# MAGIC and the Entra MTO Member. Edge cases where EXO exists without an Entra record
# MAGIC are treated as data quality issues and surfaced via the `valid_entra_link` expectation.

# COMMAND ----------


@dlt.table(
    name="mto_user_entity_mapping",
    comment="Deterministic FK join: EXO Mail User -> Entra MTO User via external_directory_object_id. Resolves Chain 1 rationalization fields (3, 4).",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
@dlt.expect("valid_entra_link", "entra_user_id IS NOT NULL")
def mto_user_entity_mapping():
    mail_users = dlt.read("mail_users")
    users = dlt.read("users")

    target_entra_users = users.filter(col("environment") == "target").select(
        col("user_id").alias("entra_user_id"),
        col("entra_object_id"),
        col("user_principal_name").alias("entra_upn"),
        col("display_name").alias("entra_display_name"),
        col("user_type").alias("entra_user_type"),
    )

    return (
        mail_users.filter(col("external_directory_object_id").isNotNull())
        .alias("mu")
        .join(
            target_entra_users.alias("eu"),
            col("mu.external_directory_object_id") == col("eu.entra_object_id"),
            "left",  # Left join: keep mail user even if Entra link missing (surfaced by expectation)
        )
        .select(
            concat_ws(
                "_", lit("mu"), col("mu.mail_user_id"), coalesce(col("eu.entra_user_id"), lit("unresolved"))
            ).alias("mapping_id"),
            col("mu.mail_user_id"),
            col("eu.entra_user_id"),
            col("mu.external_directory_object_id"),
            col("eu.entra_object_id"),
            col("eu.entra_upn"),
            col("eu.entra_display_name"),
            col("eu.entra_user_type"),
            lit("external_directory_object_id_match").alias("match_rule"),
            current_timestamp().alias("mapped_at"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Group Owners
# MAGIC
# MAGIC **Source:** `entra_group_owners`
# MAGIC **Key:** `group_owner_id` = `{source_key}_{groupId}_{owner_id}`
# MAGIC
# MAGIC One row per owner. Groups with no owners have no rows in this table.
# MAGIC `group_silver_id` is a FK to `silver.groups.group_id`.
# MAGIC Gold can detect ownerless groups via LEFT JOIN existence check.


# COMMAND ----------
@dlt.table(
    name="group_owners_staged",
    comment="Staged group owner records before deduplication. One row per owner.",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_group", "source_key IS NOT NULL AND group_silver_id IS NOT NULL AND group_silver_id != source_key"
)
@dlt.expect_or_drop("valid_owner", "owner_entra_object_id IS NOT NULL")
def group_owners_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_group_owners")
        .withColumn("r", from_json(col("_record"), ENTRA_GROUP_OWNER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("group_owner_id", concat_ws("_", col("source_key"), col("groupId"), col("id")))
        .withColumn("group_silver_id", concat_ws("_", col("source_key"), col("groupId")))
        .withColumn("owner_entra_object_id", col("id"))
        .withColumn("owner_display_name", trim(col("displayName")))
        .withColumn("owner_upn", lower(trim(col("userPrincipalName"))))
        .withColumn("owner_mail", lower(trim(col("mail"))))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "group_owner_id",
            "group_silver_id",
            "owner_entra_object_id",
            "owner_display_name",
            "owner_upn",
            "owner_mail",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="group_owners",
    comment="Deduplicated group owner membership. One row per owner per group. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="group_owners",
    source="group_owners_staged",
    keys=["group_owner_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Devices
# MAGIC
# MAGIC **Source:** `entra_devices`
# MAGIC **Key:** `device_id` = `{source_key}_{entra_object_id}`
# MAGIC
# MAGIC One row per Entra-registered device.
# MAGIC Note: `registeredOwners` is not available via the /beta/devices collection endpoint —
# MAGIC owner correlation is done in gold via `device.deviceId` ↔ user relationships.
# MAGIC
# MAGIC Gold consumers filter on `approximate_last_sign_in_at >= now - 30 days`.

# COMMAND ----------


@dlt.table(name="devices_staged", comment="Staged device records before deduplication.", temporary=True)
@dlt.expect_or_drop("valid_id", "entra_object_id IS NOT NULL")
def devices_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_devices")
        .withColumn("r", from_json(col("_record"), ENTRA_DEVICE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("device_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("entra_object_id", col("id"))
        .withColumn("azure_ad_device_id", col("deviceId"))
        .withColumn("environment", environment_col())
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("operating_system", col("operatingSystem"))
        .withColumn("operating_system_version", col("operatingSystemVersion"))
        .withColumn("trust_type", col("trustType"))
        .withColumn("profile_type", col("profileType"))
        .withColumn("device_category", col("deviceCategory"))
        .withColumn("model", col("model"))
        .withColumn("manufacturer", col("manufacturer"))
        .withColumn("is_compliant", col("isCompliant").cast("boolean"))
        .withColumn("is_managed", col("isManaged").cast("boolean"))
        .withColumn("account_enabled", col("accountEnabled").cast("boolean"))
        .withColumn("enrollment_profile_name", col("enrollmentProfileName"))
        .withColumn("on_prem_sync_enabled", col("onPremisesSyncEnabled").cast("boolean"))
        .withColumn("on_prem_last_sync_at", to_timestamp(col("onPremisesLastSyncDateTime")))
        .withColumn("on_prem_security_identifier", col("onPremisesSecurityIdentifier"))
        .withColumn("approximate_last_sign_in_at", to_timestamp(col("approximateLastSignInDateTime")))
        .withColumn("registration_at", to_timestamp(col("registrationDateTime")))
        .withColumn("source_created_at", to_timestamp(col("createdDateTime")))
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "device_id",
            "entra_object_id",
            "azure_ad_device_id",
            "environment",
            "source_key",
            "display_name",
            "operating_system",
            "operating_system_version",
            "trust_type",
            "profile_type",
            "device_category",
            "model",
            "manufacturer",
            "is_compliant",
            "is_managed",
            "account_enabled",
            "enrollment_profile_name",
            "on_prem_sync_enabled",
            "on_prem_last_sync_at",
            "on_prem_security_identifier",
            "approximate_last_sign_in_at",
            "registration_at",
            "source_created_at",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="devices",
    comment="Deduplicated Entra-registered devices. One row per device. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="devices",
    source="devices_staged",
    keys=["device_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Intune Managed Devices
# MAGIC
# MAGIC **Source:** `intune_managed_devices`
# MAGIC **Key:** `intune_device_id` = `{source_key}_{id}`
# MAGIC
# MAGIC `azure_ad_device_id` links to `silver.devices.azure_ad_device_id`.
# MAGIC `user_entra_object_id` links to `silver.users.entra_object_id`.

# COMMAND ----------


@dlt.table(
    name="intune_devices_staged",
    comment="Staged Intune managed device records before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND intune_device_id IS NOT NULL")
def intune_devices_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.intune_managed_devices")
        .withColumn("r", from_json(col("_record"), INTUNE_MANAGED_DEVICE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("intune_device_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("entra_device_record_id", col("id"))
        .withColumn("azure_ad_device_id", col("azureADDeviceId"))
        .withColumn("user_entra_object_id", col("userId"))
        .withColumn("display_name", trim(col("deviceName")))
        .withColumn("managed_device_name", trim(col("managedDeviceName")))
        .withColumn("owner_type", col("managedDeviceOwnerType"))
        .withColumn("operating_system", col("operatingSystem"))
        .withColumn("os_version", col("osVersion"))
        .withColumn("compliance_state", col("complianceState"))
        .withColumn("management_agent", col("managementAgent"))
        .withColumn("enrollment_type", col("deviceEnrollmentType"))
        .withColumn("registration_state", col("deviceRegistrationState"))
        .withColumn("model", col("model"))
        .withColumn("manufacturer", col("manufacturer"))
        .withColumn("serial_number", col("serialNumber"))
        .withColumn("is_encrypted", col("isEncrypted").cast("boolean"))
        .withColumn("is_supervised", col("isSupervised").cast("boolean"))
        .withColumn("is_aad_registered", col("azureADRegistered").cast("boolean"))
        .withColumn("autopilot_enrolled", col("autopilotEnrolled").cast("boolean"))
        .withColumn("email_address", lower(trim(col("emailAddress"))))
        .withColumn("user_principal_name", lower(trim(col("userPrincipalName"))))
        .withColumn("user_display_name", trim(col("userDisplayName")))
        .withColumn("total_storage_bytes", col("totalStorageSpaceInBytes"))
        .withColumn("free_storage_bytes", col("freeStorageSpaceInBytes"))
        .withColumn("threat_state", col("partnerReportedThreatState"))
        .withColumn("enrolled_at", to_timestamp(col("enrolledDateTime")))
        .withColumn("last_sync_at", to_timestamp(col("lastSyncDateTime")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "intune_device_id",
            "entra_device_record_id",
            "azure_ad_device_id",
            "user_entra_object_id",
            "display_name",
            "managed_device_name",
            "owner_type",
            "operating_system",
            "os_version",
            "compliance_state",
            "management_agent",
            "enrollment_type",
            "registration_state",
            "model",
            "manufacturer",
            "serial_number",
            "is_encrypted",
            "is_supervised",
            "is_aad_registered",
            "autopilot_enrolled",
            "email_address",
            "user_principal_name",
            "user_display_name",
            "total_storage_bytes",
            "free_storage_bytes",
            "threat_state",
            "enrolled_at",
            "last_sync_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="intune_devices",
    comment="Deduplicated Intune managed devices. One row per device. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="intune_devices",
    source="intune_devices_staged",
    keys=["intune_device_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - MDE Devices
# MAGIC
# MAGIC **Source:** `mde_devices`
# MAGIC **Key:** `mde_device_id` = `{source_key}_{id}`
# MAGIC
# MAGIC `aad_device_id` links to `silver.devices.azure_ad_device_id`.

# COMMAND ----------


@dlt.table(
    name="mde_devices_staged",
    comment="Staged MDE device records before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND mde_device_id IS NOT NULL")
def mde_devices_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.mde_devices")
        .withColumn("r", from_json(col("_record"), MDE_DEVICE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("mde_device_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("mde_id", col("id"))
        .withColumn("aad_device_id", col("aadDeviceId"))
        .withColumn("computer_dns_name", col("computerDnsName"))
        .withColumn("os_platform", col("osPlatform"))
        .withColumn("os_version", col("osVersion"))
        .withColumn("os_build", col("osBuild"))
        .withColumn("last_ip_address", col("lastIpAddress"))
        .withColumn("last_external_ip_address", col("lastExternalIpAddress"))
        .withColumn("health_status", col("healthStatus"))
        .withColumn("risk_score", col("riskScore"))
        .withColumn("exposure_level", col("exposureLevel"))
        .withColumn("onboarding_status", col("onboardingStatus"))
        .withColumn("is_aad_joined", col("isAadJoined").cast("boolean"))
        .withColumn("defender_av_status", col("defenderAvStatus"))
        .withColumn("rbac_group_name", col("rbacGroupName"))
        .withColumn("rbac_group_id", col("rbacGroupId"))
        .withColumn("device_value", col("deviceValue"))
        .withColumn("managed_by", col("managedBy"))
        .withColumn("managed_by_status", col("managedByStatus"))
        .withColumn("machine_tags", col("machineTags"))
        .withColumn("first_seen_at", to_timestamp(col("firstSeen")))
        .withColumn("last_seen_at", to_timestamp(col("lastSeen")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "mde_device_id",
            "mde_id",
            "aad_device_id",
            "computer_dns_name",
            "os_platform",
            "os_version",
            "os_build",
            "last_ip_address",
            "last_external_ip_address",
            "health_status",
            "risk_score",
            "exposure_level",
            "onboarding_status",
            "is_aad_joined",
            "defender_av_status",
            "rbac_group_name",
            "rbac_group_id",
            "device_value",
            "managed_by",
            "managed_by_status",
            "machine_tags",
            "first_seen_at",
            "last_seen_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="mde_devices",
    comment="Deduplicated MDE-enrolled devices. One row per device. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="mde_devices",
    source="mde_devices_staged",
    keys=["mde_device_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Entra Applications
# MAGIC
# MAGIC **Source:** `entra_applications`
# MAGIC **Key:** `application_id` = `{source_key}_{id}`
# MAGIC
# MAGIC `app_id` is the client ID (UUID); `id` is the object ID.
# MAGIC `app_owners` is a separate table keyed on `app_owner_id`.

# COMMAND ----------


@dlt.table(
    name="applications_staged",
    comment="Staged Entra application records before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND application_id IS NOT NULL")
def applications_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_applications")
        .withColumn("r", from_json(col("_record"), ENTRA_APPLICATION_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("application_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("entra_object_id", col("id"))
        .withColumn("app_id", col("appId"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("sign_in_audience", col("signInAudience"))
        .withColumn("identifier_uris", col("identifierUris"))
        .withColumn("tags", col("tags"))
        .withColumn("application_template_id", col("applicationTemplateId"))
        .withColumn("publisher_domain", col("publisherDomain"))
        .withColumn("description", col("description"))
        .withColumn("notes", col("notes"))
        .withColumn("group_membership_claims", col("groupMembershipClaims"))
        .withColumn("is_fallback_public_client", col("isFallbackPublicClient").cast("boolean"))
        .withColumn("disabled_by_microsoft_status", col("disabledByMicrosoftStatus"))
        .withColumn("saml_metadata_url", col("samlMetadataUrl"))
        .withColumn("source_created_at", to_timestamp(col("createdDateTime")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "application_id",
            "entra_object_id",
            "app_id",
            "display_name",
            "sign_in_audience",
            "identifier_uris",
            "tags",
            "application_template_id",
            "publisher_domain",
            "description",
            "notes",
            "group_membership_claims",
            "is_fallback_public_client",
            "disabled_by_microsoft_status",
            "saml_metadata_url",
            "source_created_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="applications",
    comment="Deduplicated Entra application registrations. One row per application. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="applications",
    source="applications_staged",
    keys=["application_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Entra Application Owners
# MAGIC
# MAGIC **Source:** `entra_app_owners`
# MAGIC **Key:** `app_owner_id` = `{source_key}_{applicationId}_{id}`
# MAGIC
# MAGIC `application_silver_id` = `{source_key}_{applicationId}` links to `applications.application_id`.
# MAGIC `owner_entra_object_id` links to `users.entra_object_id`.

# COMMAND ----------


@dlt.table(
    name="app_owners_staged",
    comment="Staged application owner records before deduplication. One row per owner.",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_app",
    "source_key IS NOT NULL AND application_silver_id IS NOT NULL AND application_silver_id != source_key",
)
@dlt.expect_or_drop("valid_owner", "owner_entra_object_id IS NOT NULL")
def app_owners_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_app_owners")
        .withColumn("r", from_json(col("_record"), ENTRA_APP_OWNER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("app_owner_id", concat_ws("_", col("source_key"), col("applicationId"), col("id")))
        .withColumn("application_silver_id", concat_ws("_", col("source_key"), col("applicationId")))
        .withColumn("owner_entra_object_id", col("id"))
        .withColumn("owner_display_name", trim(col("displayName")))
        .withColumn("owner_upn", lower(trim(col("userPrincipalName"))))
        .withColumn("owner_mail", lower(trim(col("mail"))))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "app_owner_id",
            "application_silver_id",
            "owner_entra_object_id",
            "owner_display_name",
            "owner_upn",
            "owner_mail",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="app_owners",
    comment="Deduplicated application owner membership. One row per owner per application. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="app_owners",
    source="app_owners_staged",
    keys=["app_owner_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Entra Service Principals
# MAGIC
# MAGIC **Source:** `entra_service_principals`
# MAGIC **Key:** `service_principal_id` = `{source_key}_{id}`
# MAGIC
# MAGIC `app_id` is the client ID (UUID) and links to `applications.app_id`.
# MAGIC `app_owner_org_id` identifies the tenant that owns the app.

# COMMAND ----------


@dlt.table(
    name="service_principals_staged",
    comment="Staged Entra service principal records before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND service_principal_id IS NOT NULL")
def service_principals_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_service_principals")
        .withColumn("r", from_json(col("_record"), ENTRA_SERVICE_PRINCIPAL_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("service_principal_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("entra_object_id", col("id"))
        .withColumn("app_id", col("appId"))
        .withColumn("app_display_name", trim(col("appDisplayName")))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("sp_type", col("servicePrincipalType"))
        .withColumn("app_owner_org_id", col("appOwnerOrganizationId"))
        .withColumn("account_enabled", col("accountEnabled").cast("boolean"))
        .withColumn("app_role_assignment_required", col("appRoleAssignmentRequired").cast("boolean"))
        .withColumn("tags", col("tags"))
        .withColumn("sp_names", col("servicePrincipalNames"))
        .withColumn("homepage", col("homepage"))
        .withColumn("login_url", col("loginUrl"))
        .withColumn("preferred_sso_mode", col("preferredSingleSignOnMode"))
        .withColumn("sign_in_audience", col("signInAudience"))
        .withColumn("notes", col("notes"))
        .withColumn("application_template_id", col("applicationTemplateId"))
        .withColumn("description", col("description"))
        .withColumn("disabled_by_microsoft_status", col("disabledByMicrosoftStatus"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "service_principal_id",
            "entra_object_id",
            "app_id",
            "app_display_name",
            "display_name",
            "sp_type",
            "app_owner_org_id",
            "account_enabled",
            "app_role_assignment_required",
            "tags",
            "sp_names",
            "homepage",
            "login_url",
            "preferred_sso_mode",
            "sign_in_audience",
            "notes",
            "application_template_id",
            "description",
            "disabled_by_microsoft_status",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="service_principals",
    comment="Deduplicated Entra service principals. One row per SP. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="service_principals",
    source="service_principals_staged",
    keys=["service_principal_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Entra SP Owners
# MAGIC
# MAGIC **Source:** `entra_sp_owners`
# MAGIC **Key:** `sp_owner_id` = `{source_key}_{servicePrincipalId}_{id}`
# MAGIC
# MAGIC `sp_silver_id` links to `service_principals.service_principal_id`.
# MAGIC `owner_entra_object_id` links to `users.entra_object_id`.

# COMMAND ----------


@dlt.table(
    name="sp_owners_staged",
    comment="Staged service principal owner records before deduplication. One row per owner.",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_sp",
    "source_key IS NOT NULL AND sp_silver_id IS NOT NULL AND sp_silver_id != source_key",
)
@dlt.expect_or_drop("valid_owner", "owner_entra_object_id IS NOT NULL")
def sp_owners_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_sp_owners")
        .withColumn("r", from_json(col("_record"), ENTRA_SP_OWNER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("sp_owner_id", concat_ws("_", col("source_key"), col("servicePrincipalId"), col("id")))
        .withColumn("sp_silver_id", concat_ws("_", col("source_key"), col("servicePrincipalId")))
        .withColumn("owner_entra_object_id", col("id"))
        .withColumn("owner_display_name", trim(col("displayName")))
        .withColumn("owner_upn", lower(trim(col("userPrincipalName"))))
        .withColumn("owner_mail", lower(trim(col("mail"))))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "sp_owner_id",
            "sp_silver_id",
            "owner_entra_object_id",
            "owner_display_name",
            "owner_upn",
            "owner_mail",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="sp_owners",
    comment="Deduplicated SP owner membership. One row per owner per service principal. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="sp_owners",
    source="sp_owners_staged",
    keys=["sp_owner_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Delegated Permission Grants
# MAGIC
# MAGIC **Source:** `entra_delegated_permission_grants`
# MAGIC **Key:** `grant_id` = `{source_key}_{id}`
# MAGIC
# MAGIC `client_sp_id` = `{source_key}_{clientId}` links to `service_principals.service_principal_id`.
# MAGIC `resource_sp_id` = `{source_key}_{resourceId}` links to `service_principals.service_principal_id`.
# MAGIC `principal_user_id` = `{source_key}_{principalId}` links to `users.entra_object_id` (null for AllPrincipals).

# COMMAND ----------


@dlt.table(
    name="delegated_permission_grants_staged",
    comment="Staged OAuth2 delegated permission grant records before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND grant_id IS NOT NULL")
@dlt.expect_or_drop("valid_client", "client_sp_id IS NOT NULL")
def delegated_permission_grants_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_delegated_permission_grants")
        .withColumn("r", from_json(col("_record"), ENTRA_DELEGATED_GRANT_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("grant_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("client_sp_id", concat_ws("_", col("source_key"), col("clientId")))
        .withColumn("resource_sp_id", concat_ws("_", col("source_key"), col("resourceId")))
        .withColumn(
            "principal_user_id",
            when(col("principalId").isNotNull(), concat_ws("_", col("source_key"), col("principalId"))).otherwise(
                lit(None)
            ),
        )
        .withColumn("consent_type", col("consentType"))
        .withColumn("scope", col("scope"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "grant_id",
            "client_sp_id",
            "resource_sp_id",
            "principal_user_id",
            "consent_type",
            "scope",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="delegated_permission_grants",
    comment="Deduplicated OAuth2 delegated permission grants. One row per grant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="delegated_permission_grants",
    source="delegated_permission_grants_staged",
    keys=["grant_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Entra Sign-in Logs
# MAGIC
# MAGIC **Source:** `entra_sign_in_logs`
# MAGIC **Key:** none (append-only streaming table)
# MAGIC
# MAGIC Sign-in logs are high-volume time-series data. They are NOT deduplicated via APPLY CHANGES.
# MAGIC Instead this is a simple streaming append table. Gold consumers filter by `created_at` window.
# MAGIC
# MAGIC `user_silver_id` = `{source_key}_{userId}` links to `users.entra_object_id`.

# COMMAND ----------


@dlt.table(
    name="sign_in_logs",
    comment="Entra sign-in log events. Append-only streaming. Filter by created_at for time windows.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
@dlt.expect_or_drop("valid_log", "source_key IS NOT NULL AND sign_in_id IS NOT NULL")
def sign_in_logs():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_sign_in_logs")
        .withColumn("r", from_json(col("_record"), ENTRA_SIGN_IN_LOG_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("sign_in_id", col("id"))
        .withColumn("user_silver_id", concat_ws("_", col("source_key"), col("userId")))
        .withColumn("user_id", col("userId"))
        .withColumn("user_display_name", trim(col("userDisplayName")))
        .withColumn("user_principal_name", lower(trim(col("userPrincipalName"))))
        .withColumn("app_id", col("appId"))
        .withColumn("app_display_name", trim(col("appDisplayName")))
        .withColumn("ip_address", col("ipAddress"))
        .withColumn("client_app_used", col("clientAppUsed"))
        .withColumn("conditional_access_status", col("conditionalAccessStatus"))
        .withColumn("is_interactive", col("isInteractive").cast("boolean"))
        .withColumn("resource_display_name", trim(col("resourceDisplayName")))
        .withColumn("resource_id", col("resourceId"))
        .withColumn("risk_detail", col("riskDetail"))
        .withColumn("risk_level_aggregated", col("riskLevelAggregated"))
        .withColumn("risk_level_during_sign_in", col("riskLevelDuringSignIn"))
        .withColumn("risk_state", col("riskState"))
        .withColumn("created_at", to_timestamp(col("createdDateTime")))
        .withColumn("environment", environment_col())
        .select(
            "sign_in_id",
            "user_silver_id",
            "user_id",
            "user_display_name",
            "user_principal_name",
            "app_id",
            "app_display_name",
            "ip_address",
            "client_app_used",
            "conditional_access_status",
            "is_interactive",
            "resource_display_name",
            "resource_id",
            "risk_detail",
            "risk_level_aggregated",
            "risk_level_during_sign_in",
            "risk_state",
            "created_at",
            "environment",
            "source_key",
            "batch_id",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Teams
# MAGIC
# MAGIC **Source:** `teams_teams`
# MAGIC **Key:** `team_id` = `{source_key}_{id}`
# MAGIC
# MAGIC `entra_object_id` = Entra group object ID — links to `silver.groups.entra_object_id`.
# MAGIC All Teams-enabled groups have an Entra group object with team properties.

# COMMAND ----------


@dlt.table(
    name="teams_staged",
    comment="Staged Teams records before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "source_key IS NOT NULL AND team_id IS NOT NULL")
def teams_staged():
    # /v1.0/teams (PR #377) no longer returns mail / visibility / createdDateTime.
    # Source those three columns from bronze.entra_groups via a streaming-static LEFT JOIN
    # keyed on (source_key, id). Standardize on the entra_groups values — no coalesce
    # against legacy inline values (clean break, single source of truth). See #378.
    teams = (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.teams_teams")
        .withColumn("r", from_json(col("_record"), TEAMS_TEAM_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
    )

    # bronze.entra_groups is append-only and has many rows per (source_key, id) from
    # repeated daily loads. Dedup to the latest row per key to avoid LEFT JOIN fan-out.
    # Window partitions reference the post-select aliases (g_source_key, g_id).
    groups_window = Window.partitionBy("g_source_key", "g_id").orderBy(col("_dlt_ingested_at").desc())
    groups = (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.entra_groups")
        .withColumn("g", from_json(col("_record"), ENTRA_GROUP_SCHEMA))
        .select(
            col("source_key").alias("g_source_key"),
            col("g.id").alias("g_id"),
            col("g.mail").alias("g_mail"),
            col("g.visibility").alias("g_visibility"),
            col("g.createdDateTime").alias("g_createdDateTime"),
            col("_dlt_ingested_at"),
        )
        .withColumn("_rn", row_number().over(groups_window))
        .filter(col("_rn") == 1)
        .drop("_rn", "_dlt_ingested_at")
    )

    return (
        teams.join(
            groups,
            (teams.id == groups.g_id) & (teams.source_key == groups.g_source_key),
            "left",
        )
        .withColumn("team_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("entra_object_id", col("id"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("description", col("description"))
        .withColumn("mail", lower(trim(col("g_mail"))))
        .withColumn("visibility", col("g_visibility"))
        .withColumn("source_created_at", to_timestamp(col("g_createdDateTime")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "team_id",
            "entra_object_id",
            "display_name",
            "description",
            "mail",
            "visibility",
            "source_created_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="teams",
    comment="Deduplicated Teams (Teams-enabled Entra groups). One row per team. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="teams",
    source="teams_staged",
    keys=["team_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Teams Team Details
# MAGIC
# MAGIC **Source:** `teams_team_details`
# MAGIC **Key:** `team_silver_id` = `{source_key}_{id}`
# MAGIC
# MAGIC Rich Team payload including visibility, archive status, member counts, and settings.
# MAGIC Feeds `is_archived`, `visibility`, `members_count` fields on `gold.shared_data_sets` team rows.

# COMMAND ----------


@dlt.table(
    name="teams_team_details_staged",
    comment="Staged Teams team details records before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_id",
    "source_key IS NOT NULL AND team_silver_id IS NOT NULL AND team_silver_id != source_key",
)
def teams_team_details_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.teams_team_details")
        .withColumn("r", from_json(col("_record"), TEAMS_TEAM_DETAILS_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("team_silver_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("entra_object_id", col("id"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("description", col("description"))
        .withColumn("visibility", col("visibility"))
        .withColumn("is_archived", col("isArchived").cast("boolean"))
        .withColumn("is_membership_limited_to_owners", col("isMembershipLimitedToOwners").cast("boolean"))
        .withColumn("classification", col("classification"))
        .withColumn("specialization", col("specialization"))
        .withColumn("internal_id", col("internalId"))
        .withColumn("web_url", col("webUrl"))
        .withColumn("owners_count", col("summary.ownersCount").cast("int"))
        .withColumn("members_count", col("summary.membersCount").cast("int"))
        .withColumn("guests_count", col("summary.guestsCount").cast("int"))
        .withColumn("source_created_at", to_timestamp(col("createdDateTime")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "team_silver_id",
            "entra_object_id",
            "display_name",
            "description",
            "visibility",
            "is_archived",
            "is_membership_limited_to_owners",
            "classification",
            "specialization",
            "internal_id",
            "web_url",
            "owners_count",
            "members_count",
            "guests_count",
            "source_created_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="teams_team_details",
    comment="Deduplicated Teams team details (rich Team payload). One row per team. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="teams_team_details",
    source="teams_team_details_staged",
    keys=["team_silver_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Team Channels
# MAGIC
# MAGIC **Source:** `teams_channels`
# MAGIC **Key:** `channel_id` = `{source_key}_{teamId}_{id}`
# MAGIC
# MAGIC `team_silver_id` = `{source_key}_{teamId}` links to `teams.team_id`.

# COMMAND ----------


@dlt.table(
    name="team_channels_staged",
    comment="Staged Teams channel records before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_id",
    "source_key IS NOT NULL AND channel_id IS NOT NULL AND team_silver_id IS NOT NULL AND team_silver_id != source_key",
)
def team_channels_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.teams_channels")
        .withColumn("r", from_json(col("_record"), TEAMS_CHANNEL_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("channel_id", concat_ws("_", col("source_key"), col("teamId"), col("id")))
        .withColumn("teams_channel_id", col("id"))
        .withColumn("team_silver_id", concat_ws("_", col("source_key"), col("teamId")))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("description", col("description"))
        .withColumn("membership_type", col("membershipType"))
        .withColumn("web_url", col("webUrl"))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("is_archived", col("isArchived").cast("boolean"))
        .withColumn("source_created_at", to_timestamp(col("createdDateTime")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "channel_id",
            "teams_channel_id",
            "team_silver_id",
            "display_name",
            "description",
            "membership_type",
            "web_url",
            "email",
            "is_archived",
            "source_created_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="team_channels",
    comment="Deduplicated Teams channels. One row per channel. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="team_channels",
    source="team_channels_staged",
    keys=["channel_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Team Channel Members
# MAGIC
# MAGIC **Source:** `teams_channel_members`
# MAGIC **Key:** `channel_member_id` = `{source_key}_{channelId}_{id}`
# MAGIC
# MAGIC Only populated for private/shared channels (standard channels inherit team membership).
# MAGIC `channel_silver_id` links to `team_channels.channel_id`.
# MAGIC `email` links to `users.mail`.

# COMMAND ----------


@dlt.table(
    name="team_channel_members_staged",
    comment="Staged Teams channel member records before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_id",
    "source_key IS NOT NULL AND channel_member_id IS NOT NULL AND channel_silver_id IS NOT NULL",
)
def team_channel_members_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.teams_channel_members")
        .withColumn("r", from_json(col("_record"), TEAMS_CHANNEL_MEMBER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("channel_member_id", concat_ws("_", col("source_key"), col("channelId"), col("id")))
        .withColumn("channel_silver_id", concat_ws("_", col("source_key"), col("teamId"), col("channelId")))
        .withColumn("membership_id", col("id"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("roles", col("roles"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "channel_member_id",
            "channel_silver_id",
            "membership_id",
            "display_name",
            "email",
            "roles",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="team_channel_members",
    comment="Deduplicated Teams channel members (private/shared channels only). SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="team_channel_members",
    source="team_channel_members_staged",
    keys=["channel_member_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - EXO Group Members
# MAGIC
# MAGIC **Source:** `exo_group_members`
# MAGIC **Key:** `exo_group_member_id` = `{source_key}_{groupObjectId}_{memberObjectId}`
# MAGIC
# MAGIC `groupType` is `"DistributionGroup"` or `"UnifiedGroup"`.
# MAGIC `groupObjectId` links to `entra_groups.entra_object_id` (for unified groups) or
# MAGIC distribution group identity.
# MAGIC `memberObjectId` links to `users.entra_object_id` when `memberType` is `"UserMailbox"`.

# COMMAND ----------


@dlt.table(
    name="exo_group_members_staged",
    comment="Staged EXO group member records before deduplication.",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_id",
    "source_key IS NOT NULL AND exo_group_member_id IS NOT NULL",
)
@dlt.expect_or_drop("valid_group", "group_object_id IS NOT NULL")
@dlt.expect_or_drop("valid_member", "member_object_id IS NOT NULL")
def exo_group_members_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.exo_group_members")
        .withColumn("r", from_json(col("_record"), EXO_GROUP_MEMBER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn(
            "exo_group_member_id",
            concat_ws("_", col("source_key"), col("groupObjectId"), col("memberObjectId")),
        )
        .withColumn("group_object_id", col("groupObjectId"))
        .withColumn("group_identity", col("groupIdentity"))
        .withColumn("group_type", col("groupType"))
        .withColumn("member_object_id", col("memberObjectId"))
        .withColumn("member_name", col("memberName"))
        .withColumn("member_type", col("memberType"))
        .withColumn("primary_smtp", lower(trim(col("primarySmtp"))))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "exo_group_member_id",
            "group_object_id",
            "group_identity",
            "group_type",
            "member_object_id",
            "member_name",
            "member_type",
            "primary_smtp",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="exo_group_members",
    comment="Deduplicated EXO group members (DG + Unified). One row per group-member pair. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="exo_group_members",
    source="exo_group_members_staged",
    keys=["exo_group_member_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Teams Installed Apps (Sprint 2 - Pending)
# MAGIC
# MAGIC **Status:** Bronze table `teams_installed_apps` exists but no StructType defined until
# MAGIC live ingest confirms the exact JSON payload shape (app catalog metadata varies by tenant).
# MAGIC The schema `TEAMS_INSTALLED_APP_SCHEMA` is defined — uncomment once confirmed.
# MAGIC
# MAGIC <!-- SPRINT 2 PLACEHOLDER
# MAGIC
# MAGIC @dlt.table(name="team_installed_apps_staged", ...)
# MAGIC @dlt.table(name="team_installed_apps", ...)
# MAGIC
# MAGIC -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - AD Users (Sprint 2 - Pending)
# MAGIC
# MAGIC **Status:** Awaiting `ad_users` / `ad_contacts` data to arrive in landing zone.
# MAGIC Bronze tables (`bronze.ad_users`, `bronze.ad_contacts`) do not yet exist.
# MAGIC These tables will be uncommented once ingest is confirmed.
# MAGIC
# MAGIC <!-- SPRINT 2 PLACEHOLDER - DO NOT UNCOMMENT until bronze tables are confirmed
# MAGIC
# MAGIC @dlt.table(name="ad_users_staged", ...)
# MAGIC @dlt.table(name="ad_contacts_staged", ...)
# MAGIC
# MAGIC -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Audit Logs
# MAGIC
# MAGIC Audit logs are high-volume time-series events. They are **not** deduplicated via APPLY CHANGES.
# MAGIC All four audit tables use simple streaming append (same pattern as `sign_in_logs`).
# MAGIC Each event is uniquely identified by `Id` from the Microsoft 365 Unified Audit Log schema.
# MAGIC
# MAGIC Common ULA envelope fields parsed into typed columns:
# MAGIC - `audit_id` — `Id` (event GUID)
# MAGIC - `user_id` — `UserId`
# MAGIC - `operation` — `Operation`
# MAGIC - `workload` — `Workload`
# MAGIC - `record_type` — `RecordType` (int)
# MAGIC - `result_status` — `ResultStatus`
# MAGIC - `created_at` — `CreationTime`

# COMMAND ----------

AUDIT_COMMON_SCHEMA = StructType(
    [
        StructField("Id", StringType(), True),
        StructField("UserId", StringType(), True),
        StructField("Operation", StringType(), True),
        StructField("Workload", StringType(), True),
        StructField("RecordType", IntegerType(), True),
        StructField("ResultStatus", StringType(), True),
        StructField("CreationTime", StringType(), True),
        StructField("OrganizationId", StringType(), True),
        StructField("ObjectId", StringType(), True),
        StructField("ClientIP", StringType(), True),
        StructField("UserType", IntegerType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit Entra
# MAGIC
# MAGIC **Source:** `audit_entra`
# MAGIC **Key:** none (append-only streaming table)
# MAGIC
# MAGIC Entra ID / Azure AD audit events (sign-ins, user/group changes, app consent, etc.).

# COMMAND ----------


@dlt.table(
    name="audit_entra",
    comment="Entra ID unified audit log events. Append-only streaming. Filter by created_at for time windows.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
@dlt.expect_or_drop("valid_log", "source_key IS NOT NULL AND audit_id IS NOT NULL")
def audit_entra():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.audit_entra")
        .withColumn("r", from_json(col("_record"), AUDIT_COMMON_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("audit_id", col("Id"))
        .withColumn("user_id", col("UserId"))
        .withColumn("operation", col("Operation"))
        .withColumn("workload", col("Workload"))
        .withColumn("record_type", col("RecordType"))
        .withColumn("result_status", col("ResultStatus"))
        .withColumn("organization_id", col("OrganizationId"))
        .withColumn("object_id", col("ObjectId"))
        .withColumn("client_ip", col("ClientIP"))
        .withColumn("user_type", col("UserType"))
        .withColumn("created_at", to_timestamp(col("CreationTime")))
        .withColumn("environment", environment_col())
        .select(
            "audit_id",
            "user_id",
            "operation",
            "workload",
            "record_type",
            "result_status",
            "organization_id",
            "object_id",
            "client_ip",
            "user_type",
            "created_at",
            "environment",
            "source_key",
            "batch_id",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit Exchange
# MAGIC
# MAGIC **Source:** `audit_exchange`
# MAGIC **Key:** none (append-only streaming table)
# MAGIC
# MAGIC Exchange Online audit events (mailbox access, message send/receive, delegate actions, etc.).

# COMMAND ----------


@dlt.table(
    name="audit_exchange",
    comment="Exchange Online unified audit log events. Append-only streaming. Filter by created_at for time windows.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
@dlt.expect_or_drop("valid_log", "source_key IS NOT NULL AND audit_id IS NOT NULL")
def audit_exchange():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.audit_exchange")
        .withColumn("r", from_json(col("_record"), AUDIT_COMMON_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("audit_id", col("Id"))
        .withColumn("user_id", col("UserId"))
        .withColumn("operation", col("Operation"))
        .withColumn("workload", col("Workload"))
        .withColumn("record_type", col("RecordType"))
        .withColumn("result_status", col("ResultStatus"))
        .withColumn("organization_id", col("OrganizationId"))
        .withColumn("object_id", col("ObjectId"))
        .withColumn("client_ip", col("ClientIP"))
        .withColumn("user_type", col("UserType"))
        .withColumn("created_at", to_timestamp(col("CreationTime")))
        .withColumn("environment", environment_col())
        .select(
            "audit_id",
            "user_id",
            "operation",
            "workload",
            "record_type",
            "result_status",
            "organization_id",
            "object_id",
            "client_ip",
            "user_type",
            "created_at",
            "environment",
            "source_key",
            "batch_id",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit General
# MAGIC
# MAGIC **Source:** `audit_general`
# MAGIC **Key:** none (append-only streaming table)
# MAGIC
# MAGIC Cross-workload audit events not covered by the workload-specific streams
# MAGIC (Teams, Yammer, Planner, Viva, Power Platform, etc.).

# COMMAND ----------


@dlt.table(
    name="audit_general",
    comment="Cross-workload unified audit log events. Append-only streaming. Filter by created_at for time windows.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
@dlt.expect_or_drop("valid_log", "source_key IS NOT NULL AND audit_id IS NOT NULL")
def audit_general():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.audit_general")
        .withColumn("r", from_json(col("_record"), AUDIT_COMMON_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("audit_id", col("Id"))
        .withColumn("user_id", col("UserId"))
        .withColumn("operation", col("Operation"))
        .withColumn("workload", col("Workload"))
        .withColumn("record_type", col("RecordType"))
        .withColumn("result_status", col("ResultStatus"))
        .withColumn("organization_id", col("OrganizationId"))
        .withColumn("object_id", col("ObjectId"))
        .withColumn("client_ip", col("ClientIP"))
        .withColumn("user_type", col("UserType"))
        .withColumn("created_at", to_timestamp(col("CreationTime")))
        .withColumn("environment", environment_col())
        .select(
            "audit_id",
            "user_id",
            "operation",
            "workload",
            "record_type",
            "result_status",
            "organization_id",
            "object_id",
            "client_ip",
            "user_type",
            "created_at",
            "environment",
            "source_key",
            "batch_id",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit SharePoint
# MAGIC
# MAGIC **Source:** `audit_sharepoint`
# MAGIC **Key:** none (append-only streaming table)
# MAGIC
# MAGIC SharePoint Online audit events (file access, sharing, permission changes, site activity, etc.).

# COMMAND ----------


@dlt.table(
    name="audit_sharepoint",
    comment="SharePoint Online unified audit log events. Append-only streaming. Filter by created_at for time windows.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
@dlt.expect_or_drop("valid_log", "source_key IS NOT NULL AND audit_id IS NOT NULL")
def audit_sharepoint():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.audit_sharepoint")
        .withColumn("r", from_json(col("_record"), AUDIT_COMMON_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("audit_id", col("Id"))
        .withColumn("user_id", col("UserId"))
        .withColumn("operation", col("Operation"))
        .withColumn("workload", col("Workload"))
        .withColumn("record_type", col("RecordType"))
        .withColumn("result_status", col("ResultStatus"))
        .withColumn("organization_id", col("OrganizationId"))
        .withColumn("object_id", col("ObjectId"))
        .withColumn("client_ip", col("ClientIP"))
        .withColumn("user_type", col("UserType"))
        .withColumn("created_at", to_timestamp(col("CreationTime")))
        .withColumn("environment", environment_col())
        .select(
            "audit_id",
            "user_id",
            "operation",
            "workload",
            "record_type",
            "result_status",
            "organization_id",
            "object_id",
            "client_ip",
            "user_type",
            "created_at",
            "environment",
            "source_key",
            "batch_id",
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Power BI / Fabric
# MAGIC
# MAGIC All Power BI / Fabric silver tables use SCD Type 1 via APPLY CHANGES.
# MAGIC The `_record` column is carried through to silver to preserve nested JSON arrays
# MAGIC (reports, datasets, permissions, etc.) that would otherwise require wide schemas.
# MAGIC Gold-layer views flatten specific arrays when needed.

# COMMAND ----------

POWERBI_CAPACITY_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("admins", StringType(), True),  # JSON array, kept as string
        StructField("sku", StringType(), True),
        StructField("state", StringType(), True),
        StructField("region", StringType(), True),
        StructField("capacityUserAccessRight", StringType(), True),
    ]
)

POWERBI_APP_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("publishedBy", StringType(), True),
        StructField("lastUpdate", StringType(), True),
    ]
)

POWERBI_DEPLOYMENT_PIPELINE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

POWERBI_GATEWAY_CLUSTER_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("region", StringType(), True),
        StructField("clusterFeatures", StringType(), True),
    ]
)

POWERBI_GATEWAY_CLUSTER_PERMISSION_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("clusterId", StringType(), True),
        StructField("principalType", StringType(), True),
        StructField("role", StringType(), True),
        StructField("principalDisplayName", StringType(), True),
        StructField("principalEmail", StringType(), True),
        StructField("tenantId", StringType(), True),
        StructField("allowedDataSourceTypes", ArrayType(StringType()), True),
    ]
)

POWERBI_WORKSPACE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("scanId", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("state", StringType(), True),
        StructField("isOnDedicatedCapacity", BooleanType(), True),
        StructField("capacityId", StringType(), True),
        StructField("defaultDatasetStorageFormat", StringType(), True),
        StructField("dataRetrievalState", StringType(), True),
    ]
)

FABRIC_ITEM_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("description", StringType(), True),
        StructField("type", StringType(), True),
        StructField("workspaceId", StringType(), True),
        StructField("capacityId", StringType(), True),
        StructField("createdBy", StringType(), True),
        StructField("modifiedBy", StringType(), True),
        StructField("createdDate", StringType(), True),
        StructField("modifiedDate", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Capacities
# MAGIC
# MAGIC **Source:** `powerbi_capacities`
# MAGIC **Key:** `capacity_id` = `{source_key}_{id}`
# MAGIC
# MAGIC One row per Power BI Premium / Embedded capacity per tenant. SCD Type 1.

# COMMAND ----------


@dlt.table(name="powerbi_capacities_staged", comment="Staged Power BI capacity records.", temporary=True)
@dlt.expect_or_drop("valid_id", "capacity_id IS NOT NULL")
def powerbi_capacities_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerbi_capacities")
        .withColumn("r", from_json(col("_record"), POWERBI_CAPACITY_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("capacity_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("capacity_object_id", col("id"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("sku", col("sku"))
        .withColumn("state", col("state"))
        .withColumn("region", col("region"))
        .withColumn("capacity_user_access_right", col("capacityUserAccessRight"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "capacity_id",
            "capacity_object_id",
            "display_name",
            "sku",
            "state",
            "region",
            "capacity_user_access_right",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerbi_capacities",
    comment="Deduplicated Power BI capacities. One row per capacity per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerbi_capacities",
    source="powerbi_capacities_staged",
    keys=["capacity_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Apps
# MAGIC
# MAGIC **Source:** `powerbi_apps`
# MAGIC **Key:** `app_id` = `{source_key}_{id}`
# MAGIC
# MAGIC One row per published Power BI app per tenant. SCD Type 1.

# COMMAND ----------


@dlt.table(name="powerbi_apps_staged", comment="Staged Power BI app records.", temporary=True)
@dlt.expect_or_drop("valid_id", "app_id IS NOT NULL")
def powerbi_apps_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerbi_apps")
        .withColumn("r", from_json(col("_record"), POWERBI_APP_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("app_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("app_object_id", col("id"))
        .withColumn("name", trim(col("name")))
        .withColumn("published_by", col("publishedBy"))
        .withColumn("last_update", to_timestamp(col("lastUpdate")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "app_id",
            "app_object_id",
            "name",
            "published_by",
            "last_update",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerbi_apps",
    comment="Deduplicated Power BI apps. One row per app per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerbi_apps",
    source="powerbi_apps_staged",
    keys=["app_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Deployment Pipelines
# MAGIC
# MAGIC **Source:** `powerbi_deployment_pipelines`
# MAGIC **Key:** `pipeline_id` = `{source_key}_{id}`
# MAGIC
# MAGIC One row per deployment pipeline per tenant. SCD Type 1.

# COMMAND ----------


@dlt.table(
    name="powerbi_deployment_pipelines_staged", comment="Staged Power BI deployment pipeline records.", temporary=True
)
@dlt.expect_or_drop("valid_id", "pipeline_id IS NOT NULL")
def powerbi_deployment_pipelines_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerbi_deployment_pipelines")
        .withColumn("r", from_json(col("_record"), POWERBI_DEPLOYMENT_PIPELINE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("pipeline_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("pipeline_object_id", col("id"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("description", col("description"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "pipeline_id",
            "pipeline_object_id",
            "display_name",
            "description",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerbi_deployment_pipelines",
    comment="Deduplicated Power BI deployment pipelines. One row per pipeline per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerbi_deployment_pipelines",
    source="powerbi_deployment_pipelines_staged",
    keys=["pipeline_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Gateway Clusters
# MAGIC
# MAGIC **Source:** `powerbi_gateway_clusters`
# MAGIC **Key:** `cluster_id` = `{source_key}_{id}`
# MAGIC
# MAGIC One row per gateway cluster per tenant. Nested `permissions` and `memberGateways`
# MAGIC arrays are preserved in `_record`; `powerbi_gateway_cluster_permissions` exposes
# MAGIC flat permission rows. SCD Type 1.

# COMMAND ----------


@dlt.table(name="powerbi_gateway_clusters_staged", comment="Staged Power BI gateway cluster records.", temporary=True)
@dlt.expect_or_drop("valid_id", "cluster_id IS NOT NULL")
def powerbi_gateway_clusters_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerbi_gateway_clusters")
        .withColumn("r", from_json(col("_record"), POWERBI_GATEWAY_CLUSTER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("cluster_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("cluster_object_id", col("id"))
        .withColumn("name", trim(col("name")))
        .withColumn("type", col("type"))
        .withColumn("status", col("status"))
        .withColumn("region", col("region"))
        .withColumn("cluster_features", col("clusterFeatures"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "cluster_id",
            "cluster_object_id",
            "name",
            "type",
            "status",
            "region",
            "cluster_features",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerbi_gateway_clusters",
    comment="Deduplicated Power BI gateway clusters. One row per cluster per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerbi_gateway_clusters",
    source="powerbi_gateway_clusters_staged",
    keys=["cluster_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Gateway Cluster Permissions
# MAGIC
# MAGIC **Source:** `powerbi_gateway_cluster_permissions`
# MAGIC **Key:** `permission_id` = `{source_key}_{clusterId}_{id}`
# MAGIC
# MAGIC One row per (cluster, principal, role) permission assignment per tenant. SCD Type 1.

# COMMAND ----------


@dlt.table(
    name="powerbi_gateway_cluster_permissions_staged",
    comment="Staged Power BI gateway cluster permission records.",
    temporary=True,
)
@dlt.expect_or_drop(
    "valid_id",
    "source_key IS NOT NULL AND permission_id IS NOT NULL",
)
def powerbi_gateway_cluster_permissions_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerbi_gateway_cluster_permissions")
        .withColumn("r", from_json(col("_record"), POWERBI_GATEWAY_CLUSTER_PERMISSION_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .filter(col("clusterId").isNotNull() & (col("clusterId") != ""))
        .withColumn("cluster_silver_id", concat_ws("_", col("source_key"), col("clusterId")))
        .withColumn("permission_id", concat_ws("_", col("source_key"), col("clusterId"), col("id")))
        .withColumn("principal_type", col("principalType"))
        .withColumn("role", col("role"))
        .withColumn("principal_display_name", trim(col("principalDisplayName")))
        .withColumn("principal_email", lower(trim(col("principalEmail"))))
        .withColumn("tenant_id", col("tenantId"))
        .withColumn("allowed_data_source_types", col("allowedDataSourceTypes"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "permission_id",
            "cluster_silver_id",
            "principal_type",
            "role",
            "principal_display_name",
            "principal_email",
            "tenant_id",
            "allowed_data_source_types",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerbi_gateway_cluster_permissions",
    comment="Deduplicated Power BI gateway cluster permissions. One row per (cluster, principal, role). SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerbi_gateway_cluster_permissions",
    source="powerbi_gateway_cluster_permissions_staged",
    keys=["permission_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI Workspaces (Scan Results)
# MAGIC
# MAGIC **Source:** `powerbi_workspaces_root`
# MAGIC **Key:** `workspace_id` = `{source_key}_{id}`
# MAGIC
# MAGIC One row per workspace from the Power BI admin getInfo scan result per tenant.
# MAGIC Nested arrays (reports, datasets, dashboards, dataflows, users) are preserved
# MAGIC in `_record`; gold-layer views explode them when needed. SCD Type 1.

# COMMAND ----------


@dlt.table(name="powerbi_workspaces_staged", comment="Staged Power BI workspace scan result records.", temporary=True)
@dlt.expect_or_drop("valid_id", "workspace_id IS NOT NULL")
def powerbi_workspaces_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerbi_workspaces_root")
        .withColumn("r", from_json(col("_record"), POWERBI_WORKSPACE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("workspace_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("workspace_object_id", col("id"))
        .withColumn("scan_id", col("scanId"))
        .withColumn("name", trim(col("name")))
        .withColumn("type", col("type"))
        .withColumn("state", col("state"))
        .withColumn("is_on_dedicated_capacity", col("isOnDedicatedCapacity").cast("boolean"))
        .withColumn("capacity_silver_id", concat_ws("_", col("source_key"), col("capacityId")))
        .withColumn("default_dataset_storage_format", col("defaultDatasetStorageFormat"))
        .withColumn("data_retrieval_state", col("dataRetrievalState"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "workspace_id",
            "workspace_object_id",
            "scan_id",
            "name",
            "type",
            "state",
            "is_on_dedicated_capacity",
            "capacity_silver_id",
            "default_dataset_storage_format",
            "data_retrieval_state",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerbi_workspaces",
    comment="Deduplicated Power BI workspaces (scan results). One row per workspace per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerbi_workspaces",
    source="powerbi_workspaces_staged",
    keys=["workspace_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fabric Lakehouses
# MAGIC
# MAGIC **Source:** `powerbi_fabric_lakehouses`
# MAGIC **Key:** `lakehouse_id` = `{source_key}_{id}`
# MAGIC
# MAGIC One row per Fabric lakehouse item per tenant. SCD Type 1.

# COMMAND ----------


@dlt.table(name="fabric_lakehouses_staged", comment="Staged Fabric lakehouse records.", temporary=True)
@dlt.expect_or_drop("valid_id", "lakehouse_id IS NOT NULL")
def fabric_lakehouses_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerbi_fabric_lakehouses")
        .withColumn("r", from_json(col("_record"), FABRIC_ITEM_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("lakehouse_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("item_object_id", col("id"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("description", col("description"))
        .withColumn("workspace_silver_id", concat_ws("_", col("source_key"), col("workspaceId")))
        .withColumn("capacity_silver_id", concat_ws("_", col("source_key"), col("capacityId")))
        .withColumn("created_by", col("createdBy"))
        .withColumn("modified_by", col("modifiedBy"))
        .withColumn("created_at", to_timestamp(col("createdDate")))
        .withColumn("modified_at", to_timestamp(col("modifiedDate")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "lakehouse_id",
            "item_object_id",
            "display_name",
            "description",
            "workspace_silver_id",
            "capacity_silver_id",
            "created_by",
            "modified_by",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="fabric_lakehouses",
    comment="Deduplicated Fabric lakehouses. One row per lakehouse per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="fabric_lakehouses",
    source="fabric_lakehouses_staged",
    keys=["lakehouse_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fabric Warehouses
# MAGIC
# MAGIC **Source:** `powerbi_fabric_warehouses`
# MAGIC **Key:** `warehouse_id` = `{source_key}_{id}`
# MAGIC
# MAGIC One row per Fabric warehouse item per tenant. SCD Type 1.

# COMMAND ----------


@dlt.table(name="fabric_warehouses_staged", comment="Staged Fabric warehouse records.", temporary=True)
@dlt.expect_or_drop("valid_id", "warehouse_id IS NOT NULL")
def fabric_warehouses_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerbi_fabric_warehouses")
        .withColumn("r", from_json(col("_record"), FABRIC_ITEM_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("warehouse_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("item_object_id", col("id"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("description", col("description"))
        .withColumn("workspace_silver_id", concat_ws("_", col("source_key"), col("workspaceId")))
        .withColumn("capacity_silver_id", concat_ws("_", col("source_key"), col("capacityId")))
        .withColumn("created_by", col("createdBy"))
        .withColumn("modified_by", col("modifiedBy"))
        .withColumn("created_at", to_timestamp(col("createdDate")))
        .withColumn("modified_at", to_timestamp(col("modifiedDate")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "warehouse_id",
            "item_object_id",
            "display_name",
            "description",
            "workspace_silver_id",
            "capacity_silver_id",
            "created_by",
            "modified_by",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="fabric_warehouses",
    comment="Deduplicated Fabric warehouses. One row per warehouse per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="fabric_warehouses",
    source="fabric_warehouses_staged",
    keys=["warehouse_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fabric KQL Databases
# MAGIC
# MAGIC **Source:** `powerbi_fabric_kql_databases`
# MAGIC **Key:** `kql_database_id` = `{source_key}_{id}`
# MAGIC
# MAGIC One row per Fabric KQL database item per tenant. SCD Type 1.

# COMMAND ----------


@dlt.table(name="fabric_kql_databases_staged", comment="Staged Fabric KQL database records.", temporary=True)
@dlt.expect_or_drop("valid_id", "kql_database_id IS NOT NULL")
def fabric_kql_databases_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerbi_fabric_kql_databases")
        .withColumn("r", from_json(col("_record"), FABRIC_ITEM_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("kql_database_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("item_object_id", col("id"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("description", col("description"))
        .withColumn("workspace_silver_id", concat_ws("_", col("source_key"), col("workspaceId")))
        .withColumn("capacity_silver_id", concat_ws("_", col("source_key"), col("capacityId")))
        .withColumn("created_by", col("createdBy"))
        .withColumn("modified_by", col("modifiedBy"))
        .withColumn("created_at", to_timestamp(col("createdDate")))
        .withColumn("modified_at", to_timestamp(col("modifiedDate")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "kql_database_id",
            "item_object_id",
            "display_name",
            "description",
            "workspace_silver_id",
            "capacity_silver_id",
            "created_by",
            "modified_by",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="fabric_kql_databases",
    comment="Deduplicated Fabric KQL databases. One row per KQL database per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="fabric_kql_databases",
    source="fabric_kql_databases_staged",
    keys=["kql_database_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fabric Notebooks
# MAGIC
# MAGIC **Source:** `powerbi_fabric_notebooks`
# MAGIC **Key:** `notebook_id` = `{source_key}_{id}`
# MAGIC
# MAGIC One row per Fabric notebook item per tenant. SCD Type 1.

# COMMAND ----------


@dlt.table(name="fabric_notebooks_staged", comment="Staged Fabric notebook records.", temporary=True)
@dlt.expect_or_drop("valid_id", "notebook_id IS NOT NULL")
def fabric_notebooks_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerbi_fabric_notebooks")
        .withColumn("r", from_json(col("_record"), FABRIC_ITEM_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("notebook_id", concat_ws("_", col("source_key"), col("id")))
        .withColumn("item_object_id", col("id"))
        .withColumn("display_name", trim(col("displayName")))
        .withColumn("description", col("description"))
        .withColumn("workspace_silver_id", concat_ws("_", col("source_key"), col("workspaceId")))
        .withColumn("capacity_silver_id", concat_ws("_", col("source_key"), col("capacityId")))
        .withColumn("created_by", col("createdBy"))
        .withColumn("modified_by", col("modifiedBy"))
        .withColumn("created_at", to_timestamp(col("createdDate")))
        .withColumn("modified_at", to_timestamp(col("modifiedDate")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "notebook_id",
            "item_object_id",
            "display_name",
            "description",
            "workspace_silver_id",
            "capacity_silver_id",
            "created_by",
            "modified_by",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="fabric_notebooks",
    comment="Deduplicated Fabric notebooks. One row per notebook per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="fabric_notebooks",
    source="fabric_notebooks_staged",
    keys=["notebook_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Power Platform / BAP
# MAGIC
# MAGIC Per-environment entities from the BAP admin plane and Power Automate admin API.
# MAGIC
# MAGIC All tables use SCD Type 1. Permission tables (app/flow/connection role assignments)
# MAGIC use `_dlt_ingested_at` for sequence since the BAP permission API carries no
# MAGIC modified-time; the latest ingest batch wins.

# COMMAND ----------

POWERPLAT_APP_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField(
            "properties",
            StructType(
                [
                    StructField("displayName", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("appType", StringType(), True),
                    StructField("appPlanClassification", StringType(), True),
                    StructField("usesPremiumApi", BooleanType(), True),
                    StructField("usesCustomApi", BooleanType(), True),
                    StructField("createdTime", StringType(), True),
                    StructField("lastModifiedTime", StringType(), True),
                    StructField("lastPublishedTime", StringType(), True),
                    StructField(
                        "environment",
                        StructType([StructField("name", StringType(), True)]),
                        True,
                    ),
                    StructField(
                        "owner",
                        StructType(
                            [
                                StructField("id", StringType(), True),
                                StructField("type", StringType(), True),
                                StructField("displayName", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

# Unified permission schema for apps, flows, and connections.
# envName / appId / flowId / connectionId / apiName are added as top-level
# fields by the ingest module (Add-Member). Only the relevant subset is
# populated per entity type; the rest are null.
POWERPLAT_PERMISSION_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("envName", StringType(), True),
        StructField("appId", StringType(), True),
        StructField("flowId", StringType(), True),
        StructField("connectionId", StringType(), True),
        StructField("apiName", StringType(), True),
        StructField(
            "properties",
            StructType(
                [
                    StructField("roleName", StringType(), True),
                    StructField(
                        "principal",
                        StructType(
                            [
                                StructField("id", StringType(), True),
                                StructField("type", StringType(), True),
                                StructField("tenantId", StringType(), True),
                                StructField("displayName", StringType(), True),
                                StructField("email", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

POWERPLAT_FLOW_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField(
            "properties",
            StructType(
                [
                    StructField("displayName", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("createdTime", StringType(), True),
                    StructField("lastModifiedTime", StringType(), True),
                    StructField("workflowEntityId", StringType(), True),
                    StructField(
                        "environment",
                        StructType([StructField("name", StringType(), True)]),
                        True,
                    ),
                    StructField(
                        "creator",
                        StructType([StructField("objectId", StringType(), True)]),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

POWERPLAT_CONNECTION_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField(
            "properties",
            StructType(
                [
                    StructField("displayName", StringType(), True),
                    StructField("apiId", StringType(), True),
                    StructField("iconUri", StringType(), True),
                    StructField("apiType", StringType(), True),
                    StructField("createdTime", StringType(), True),
                    StructField("lastModifiedTime", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

POWERPLAT_CUSTOM_CONNECTOR_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField(
            "properties",
            StructType(
                [
                    StructField("displayName", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("iconUri", StringType(), True),
                    StructField("apiType", StringType(), True),
                    StructField("createdTime", StringType(), True),
                    StructField("lastModifiedTime", StringType(), True),
                    StructField(
                        "backendService",
                        StructType([StructField("serviceUrl", StringType(), True)]),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

POWERPLAT_DV_ONBOARDING_SCHEMA = StructType(
    [
        StructField("env_id", StringType(), True),
        StructField("application_id", StringType(), True),
        StructField("systemuser_id", StringType(), True),
        StructField("role_id", StringType(), True),
        StructField("role_name", StringType(), True),
        StructField("onboarded_at", StringType(), True),
        StructField("created_new", BooleanType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Apps
# MAGIC
# MAGIC **Source:** `powerplat_apps`
# MAGIC **Key:** `app_id` = `{source_key}_{name}`
# MAGIC
# MAGIC One row per canvas app per tenant. SCD Type 1. `_record` kept for `connectionReferences`.

# COMMAND ----------


@dlt.table(name="powerplat_apps_staged", comment="Staged Power Platform canvas app records.", temporary=True)
@dlt.expect_or_drop("valid_id", "app_id IS NOT NULL")
def powerplat_apps_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_apps")
        .withColumn("r", from_json(col("_record"), POWERPLAT_APP_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("app_id", concat_ws("_", col("source_key"), col("name")))
        .withColumn("app_object_id", col("name"))
        .withColumn("env_name", col("properties.environment.name"))
        .withColumn("display_name", trim(col("properties.displayName")))
        .withColumn("description", col("properties.description"))
        .withColumn("app_type", col("properties.appType"))
        .withColumn("app_plan_classification", col("properties.appPlanClassification"))
        .withColumn("uses_premium_api", col("properties.usesPremiumApi"))
        .withColumn("uses_custom_api", col("properties.usesCustomApi"))
        .withColumn("owner_id", col("properties.owner.id"))
        .withColumn("owner_type", col("properties.owner.type"))
        .withColumn("owner_display_name", trim(col("properties.owner.displayName")))
        .withColumn("created_at", to_timestamp(col("properties.createdTime")))
        .withColumn("last_modified_at", to_timestamp(col("properties.lastModifiedTime")))
        .withColumn("last_published_at", to_timestamp(col("properties.lastPublishedTime")))
        .withColumn("environment", environment_col())
        .withColumn(
            "last_updated_at", coalesce(to_timestamp(col("properties.lastModifiedTime")), col("_dlt_ingested_at"))
        )
        .select(
            "app_id",
            "app_object_id",
            "env_name",
            "display_name",
            "description",
            "app_type",
            "app_plan_classification",
            "uses_premium_api",
            "uses_custom_api",
            "owner_id",
            "owner_type",
            "owner_display_name",
            "created_at",
            "last_modified_at",
            "last_published_at",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerplat_apps",
    comment="Deduplicated Power Platform canvas apps. One row per app per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_apps",
    source="powerplat_apps_staged",
    keys=["app_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform App Role Assignments
# MAGIC
# MAGIC **Source:** `powerplat_app_role_assignments`
# MAGIC **Key:** `assignment_id` = `{source_key}_{envName}_{appId}_{name}`
# MAGIC
# MAGIC One row per app permission entry per tenant. SCD Type 1.

# COMMAND ----------


@dlt.table(
    name="powerplat_app_role_assignments_staged",
    comment="Staged Power Platform canvas app permission records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "assignment_id IS NOT NULL")
def powerplat_app_role_assignments_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_app_role_assignments")
        .withColumn("r", from_json(col("_record"), POWERPLAT_PERMISSION_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn(
            "assignment_id",
            when(
                col("source_key").isNotNull()
                & col("envName").isNotNull()
                & col("appId").isNotNull()
                & col("name").isNotNull(),
                concat_ws("_", col("source_key"), col("envName"), col("appId"), col("name")),
            ).otherwise(lit(None)),
        )
        .withColumn("app_silver_id", concat_ws("_", col("source_key"), col("appId")))
        .withColumn("role_name", col("properties.roleName"))
        .withColumn("principal_id", col("properties.principal.id"))
        .withColumn("principal_type", col("properties.principal.type"))
        .withColumn("principal_tenant_id", col("properties.principal.tenantId"))
        .withColumn("principal_display_name", trim(col("properties.principal.displayName")))
        .withColumn("principal_email", lower(trim(col("properties.principal.email"))))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "assignment_id",
            "app_silver_id",
            "role_name",
            "principal_id",
            "principal_type",
            "principal_tenant_id",
            "principal_display_name",
            "principal_email",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_app_role_assignments",
    comment="Deduplicated Power Platform canvas app permissions. One row per principal per app. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_app_role_assignments",
    source="powerplat_app_role_assignments_staged",
    keys=["assignment_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Flows
# MAGIC
# MAGIC **Source:** `powerplat_flows`
# MAGIC **Key:** `flow_id` = `{source_key}_{name}`
# MAGIC
# MAGIC One row per Power Automate cloud flow per tenant. SCD Type 1.
# MAGIC V2 metadata only — flow definition body is in `powerplat_flow_metadata` (non-DV)
# MAGIC or `powerplat_workflow_definitions` (DV-backed flows via `workflowEntityId`).

# COMMAND ----------


@dlt.table(name="powerplat_flows_staged", comment="Staged Power Platform flow records.", temporary=True)
@dlt.expect_or_drop("valid_id", "flow_id IS NOT NULL")
def powerplat_flows_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_flows")
        .withColumn("r", from_json(col("_record"), POWERPLAT_FLOW_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("flow_id", concat_ws("_", col("source_key"), col("name")))
        .withColumn("flow_object_id", col("name"))
        .withColumn("env_name", col("properties.environment.name"))
        .withColumn("display_name", trim(col("properties.displayName")))
        .withColumn("description", col("properties.description"))
        .withColumn("state", col("properties.state"))
        .withColumn("workflow_entity_id", col("properties.workflowEntityId"))
        .withColumn("creator_object_id", col("properties.creator.objectId"))
        .withColumn("created_at", to_timestamp(col("properties.createdTime")))
        .withColumn("last_modified_at", to_timestamp(col("properties.lastModifiedTime")))
        .withColumn("environment", environment_col())
        .withColumn(
            "last_updated_at", coalesce(to_timestamp(col("properties.lastModifiedTime")), col("_dlt_ingested_at"))
        )
        .select(
            "flow_id",
            "flow_object_id",
            "env_name",
            "display_name",
            "description",
            "state",
            "workflow_entity_id",
            "creator_object_id",
            "created_at",
            "last_modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_flows",
    comment="Deduplicated Power Automate cloud flows. One row per flow per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_flows",
    source="powerplat_flows_staged",
    keys=["flow_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Flow Role Assignments
# MAGIC
# MAGIC **Source:** `powerplat_flow_role_assignments`
# MAGIC **Key:** `assignment_id` = `{source_key}_{envName}_{flowId}_{name}`
# MAGIC
# MAGIC Explicit flow shares only (BAP /permissions does not return the implicit Owner row).

# COMMAND ----------


@dlt.table(
    name="powerplat_flow_role_assignments_staged",
    comment="Staged Power Platform flow permission records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "assignment_id IS NOT NULL")
def powerplat_flow_role_assignments_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_flow_role_assignments")
        .withColumn("r", from_json(col("_record"), POWERPLAT_PERMISSION_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn(
            "assignment_id",
            when(
                col("source_key").isNotNull()
                & col("envName").isNotNull()
                & col("flowId").isNotNull()
                & col("name").isNotNull(),
                concat_ws("_", col("source_key"), col("envName"), col("flowId"), col("name")),
            ).otherwise(lit(None)),
        )
        .withColumn("flow_silver_id", concat_ws("_", col("source_key"), col("flowId")))
        .withColumn("role_name", col("properties.roleName"))
        .withColumn("principal_id", col("properties.principal.id"))
        .withColumn("principal_type", col("properties.principal.type"))
        .withColumn("principal_tenant_id", col("properties.principal.tenantId"))
        .withColumn("principal_display_name", trim(col("properties.principal.displayName")))
        .withColumn("principal_email", lower(trim(col("properties.principal.email"))))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "assignment_id",
            "flow_silver_id",
            "role_name",
            "principal_id",
            "principal_type",
            "principal_tenant_id",
            "principal_display_name",
            "principal_email",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_flow_role_assignments",
    comment="Deduplicated Power Automate flow permissions. One row per explicit share per flow. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_flow_role_assignments",
    source="powerplat_flow_role_assignments_staged",
    keys=["assignment_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Connections
# MAGIC
# MAGIC **Source:** `powerplat_connections`
# MAGIC **Key:** `connection_id` = `{source_key}_{name}`
# MAGIC
# MAGIC One row per connector connection per tenant. SCD Type 1. `_record` kept for `connectionParameters`.

# COMMAND ----------


@dlt.table(name="powerplat_connections_staged", comment="Staged Power Platform connection records.", temporary=True)
@dlt.expect_or_drop("valid_id", "connection_id IS NOT NULL")
def powerplat_connections_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_connections")
        .withColumn("r", from_json(col("_record"), POWERPLAT_CONNECTION_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("connection_id", concat_ws("_", col("source_key"), col("name")))
        .withColumn("connection_object_id", col("name"))
        .withColumn("display_name", trim(col("properties.displayName")))
        .withColumn("api_id", col("properties.apiId"))
        .withColumn("api_type", col("properties.apiType"))
        .withColumn("icon_uri", col("properties.iconUri"))
        .withColumn("created_at", to_timestamp(col("properties.createdTime")))
        .withColumn("last_modified_at", to_timestamp(col("properties.lastModifiedTime")))
        .withColumn("environment", environment_col())
        .withColumn(
            "last_updated_at", coalesce(to_timestamp(col("properties.lastModifiedTime")), col("_dlt_ingested_at"))
        )
        .select(
            "connection_id",
            "connection_object_id",
            "display_name",
            "api_id",
            "api_type",
            "icon_uri",
            "created_at",
            "last_modified_at",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerplat_connections",
    comment="Deduplicated Power Platform connector connections. One row per connection per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_connections",
    source="powerplat_connections_staged",
    keys=["connection_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Connection Role Assignments
# MAGIC
# MAGIC **Source:** `powerplat_connection_role_assignments`
# MAGIC **Key:** `assignment_id` = `{source_key}_{envName}_{connectionId}_{name}`
# MAGIC
# MAGIC Includes the implicit Owner row (unlike flow permissions).

# COMMAND ----------


@dlt.table(
    name="powerplat_connection_role_assignments_staged",
    comment="Staged Power Platform connection permission records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "assignment_id IS NOT NULL")
def powerplat_connection_role_assignments_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_connection_role_assignments")
        .withColumn("r", from_json(col("_record"), POWERPLAT_PERMISSION_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn(
            "assignment_id",
            when(
                col("source_key").isNotNull()
                & col("envName").isNotNull()
                & col("connectionId").isNotNull()
                & col("name").isNotNull(),
                concat_ws("_", col("source_key"), col("envName"), col("connectionId"), col("name")),
            ).otherwise(lit(None)),
        )
        .withColumn("connection_silver_id", concat_ws("_", col("source_key"), col("connectionId")))
        .withColumn("role_name", col("properties.roleName"))
        .withColumn("principal_id", col("properties.principal.id"))
        .withColumn("principal_type", col("properties.principal.type"))
        .withColumn("principal_tenant_id", col("properties.principal.tenantId"))
        .withColumn("principal_display_name", trim(col("properties.principal.displayName")))
        .withColumn("principal_email", lower(trim(col("properties.principal.email"))))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "assignment_id",
            "connection_silver_id",
            "role_name",
            "principal_id",
            "principal_type",
            "principal_tenant_id",
            "principal_display_name",
            "principal_email",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_connection_role_assignments",
    comment="Deduplicated Power Platform connection permissions. One row per principal per connection. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_connection_role_assignments",
    source="powerplat_connection_role_assignments_staged",
    keys=["assignment_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Custom Connectors
# MAGIC
# MAGIC **Source:** `powerplat_custom_connectors`
# MAGIC **Key:** `custom_connector_id` = `{source_key}_{name}`
# MAGIC
# MAGIC One row per custom connector per tenant. SCD Type 1.

# COMMAND ----------


@dlt.table(
    name="powerplat_custom_connectors_staged",
    comment="Staged Power Platform custom connector records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "custom_connector_id IS NOT NULL")
def powerplat_custom_connectors_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_custom_connectors")
        .withColumn("r", from_json(col("_record"), POWERPLAT_CUSTOM_CONNECTOR_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("custom_connector_id", concat_ws("_", col("source_key"), col("name")))
        .withColumn("connector_object_id", col("name"))
        .withColumn("display_name", trim(col("properties.displayName")))
        .withColumn("description", col("properties.description"))
        .withColumn("icon_uri", col("properties.iconUri"))
        .withColumn("api_type", col("properties.apiType"))
        .withColumn("backend_service_url", col("properties.backendService.serviceUrl"))
        .withColumn("created_at", to_timestamp(col("properties.createdTime")))
        .withColumn("last_modified_at", to_timestamp(col("properties.lastModifiedTime")))
        .withColumn("environment", environment_col())
        .withColumn(
            "last_updated_at", coalesce(to_timestamp(col("properties.lastModifiedTime")), col("_dlt_ingested_at"))
        )
        .select(
            "custom_connector_id",
            "connector_object_id",
            "display_name",
            "description",
            "icon_uri",
            "api_type",
            "backend_service_url",
            "created_at",
            "last_modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_custom_connectors",
    comment="Deduplicated Power Platform custom connectors. One row per connector per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_custom_connectors",
    source="powerplat_custom_connectors_staged",
    keys=["custom_connector_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Dataverse Onboardings
# MAGIC
# MAGIC **Source:** `powerplat_dataverse_onboardings`
# MAGIC **Key:** `onboarding_id` = `{source_key}_{env_id}`
# MAGIC
# MAGIC Audit row per successfully onboarded Dataverse environment. SCD Type 1.

# COMMAND ----------


@dlt.table(
    name="powerplat_dataverse_onboardings_staged",
    comment="Staged Dataverse environment onboarding audit records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "onboarding_id IS NOT NULL")
def powerplat_dataverse_onboardings_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_dataverse_onboardings")
        .withColumn("r", from_json(col("_record"), POWERPLAT_DV_ONBOARDING_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("onboarding_id", concat_ws("_", col("source_key"), col("env_id")))
        .withColumn("onboarded_at", to_timestamp(col("onboarded_at")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("onboarded_at")), col("_dlt_ingested_at")))
        .select(
            "onboarding_id",
            "env_id",
            "application_id",
            "systemuser_id",
            "role_id",
            "role_name",
            "created_new",
            "onboarded_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_dataverse_onboardings",
    comment="Deduplicated Dataverse environment onboarding audit. One row per environment per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_dataverse_onboardings",
    source="powerplat_dataverse_onboardings_staged",
    keys=["onboarding_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Power Platform / Dataverse
# MAGIC
# MAGIC Bulk Dataverse entities. All use SCD Type 1.
# MAGIC Most entity tables set `last_updated_at` from `to_timestamp(modifiedon)` when
# MAGIC that column is available. Some Dataverse sources, especially metadata endpoints
# MAGIC such as `EntityDefinitions`, may not include `modifiedon`; those tables
# MAGIC sequence on ingest time instead (`_dlt_ingested_at`).

# COMMAND ----------

POWERPLAT_SOLUTION_SCHEMA = StructType(
    [
        StructField("solutionid", StringType(), True),
        StructField("uniquename", StringType(), True),
        StructField("friendlyname", StringType(), True),
        StructField("version", StringType(), True),
        StructField("ismanaged", BooleanType(), True),
        StructField("isvisible", BooleanType(), True),
        StructField("installedon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_SOLUTION_COMPONENT_SCHEMA = StructType(
    [
        StructField("solutioncomponentid", StringType(), True),
        StructField("objectid", StringType(), True),
        StructField("componenttype", IntegerType(), True),
        StructField("rootcomponentbehavior", IntegerType(), True),
        StructField("ismetadata", BooleanType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_CONNECTION_REFERENCE_SCHEMA = StructType(
    [
        StructField("connectionreferenceid", StringType(), True),
        StructField("connectionreferencelogicalname", StringType(), True),
        StructField("description", StringType(), True),
        StructField("connectorid", StringType(), True),
        StructField("statecode", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_ENV_VAR_DEFINITION_SCHEMA = StructType(
    [
        StructField("environmentvariabledefinitionid", StringType(), True),
        StructField("schemaname", StringType(), True),
        StructField("displayname", StringType(), True),
        StructField("description", StringType(), True),
        StructField("defaultvalue", StringType(), True),
        StructField("type", IntegerType(), True),
        StructField("statecode", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_ENV_VAR_VALUE_SCHEMA = StructType(
    [
        StructField("environmentvariablevalueid", StringType(), True),
        StructField("value", StringType(), True),
        StructField("statecode", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_TABLE_SCHEMA = StructType(
    [
        StructField("MetadataId", StringType(), True),
        StructField("LogicalName", StringType(), True),
        StructField("SchemaName", StringType(), True),
        StructField("EntitySetName", StringType(), True),
        StructField("ObjectTypeCode", IntegerType(), True),
        StructField("IsCustomEntity", BooleanType(), True),
        StructField("IsManaged", BooleanType(), True),
        StructField("ExternalName", StringType(), True),
        StructField("TableType", StringType(), True),
        StructField(
            "DisplayName",
            StructType(
                [
                    StructField(
                        "LocalizedLabels",
                        ArrayType(
                            StructType(
                                [
                                    StructField("Label", StringType(), True),
                                    StructField("LanguageCode", IntegerType(), True),
                                ]
                            )
                        ),
                        True,
                    )
                ]
            ),
            True,
        ),
    ]
)

POWERPLAT_WORKFLOW_SCHEMA = StructType(
    [
        StructField("workflowid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("type", IntegerType(), True),
        StructField("category", IntegerType(), True),
        StructField("mode", IntegerType(), True),
        StructField("subprocess", BooleanType(), True),
        StructField("ismanaged", BooleanType(), True),
        StructField("componentstate", IntegerType(), True),
        StructField("primaryentity", StringType(), True),
        StructField("scope", IntegerType(), True),
        StructField("businessprocesstype", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_PLUGIN_ASSEMBLY_SCHEMA = StructType(
    [
        StructField("pluginassemblyid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("version", StringType(), True),
        StructField("culture", StringType(), True),
        StructField("publickeytoken", StringType(), True),
        StructField("description", StringType(), True),
        StructField("isolationmode", IntegerType(), True),
        StructField("sourcetype", IntegerType(), True),
        StructField("sourcehash", StringType(), True),
        StructField("ismanaged", BooleanType(), True),
        StructField("componentstate", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_PLUGIN_STEP_SCHEMA = StructType(
    [
        StructField("sdkmessageprocessingstepid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("mode", IntegerType(), True),
        StructField("stage", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("statecode", IntegerType(), True),
        StructField("ismanaged", BooleanType(), True),
        StructField("componentstate", IntegerType(), True),
        StructField("asyncautodelete", BooleanType(), True),
        StructField("filteringattributes", StringType(), True),
        StructField("supporteddeployment", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_WEB_RESOURCE_SCHEMA = StructType(
    [
        StructField("webresourceid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("displayname", StringType(), True),
        StructField("description", StringType(), True),
        StructField("webresourcetype", IntegerType(), True),
        StructField("languagecode", IntegerType(), True),
        StructField("ismanaged", BooleanType(), True),
        StructField("componentstate", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_APP_MODULE_SCHEMA = StructType(
    [
        StructField("appmoduleid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("uniquename", StringType(), True),
        StructField("description", StringType(), True),
        StructField("formfactor", IntegerType(), True),
        StructField("isfeatured", BooleanType(), True),
        StructField("ismanaged", BooleanType(), True),
        StructField("componentstate", IntegerType(), True),
        StructField("navigationtype", IntegerType(), True),
        StructField("clienttype", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_BOT_SCHEMA = StructType(
    [
        StructField("botid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("schemaname", StringType(), True),
        StructField("language", StringType(), True),
        StructField("ismanaged", BooleanType(), True),
        StructField("componentstate", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_BOT_COMPONENT_SCHEMA = StructType(
    [
        StructField("botcomponentid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("schemaname", StringType(), True),
        StructField("componenttype", IntegerType(), True),
        StructField("ismanaged", BooleanType(), True),
        StructField("componentstate", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_AI_MODEL_SCHEMA = StructType(
    [
        StructField("msdyn_aimodelid", StringType(), True),
        StructField("msdyn_name", StringType(), True),
        StructField("ismanaged", BooleanType(), True),
        StructField("componentstate", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_POWERPAGES_WEBSITE_SCHEMA = StructType(
    [
        StructField("mspp_websiteid", StringType(), True),
        StructField("mspp_name", StringType(), True),
        StructField("mspp_primarydomainname", StringType(), True),
        StructField("statecode", IntegerType(), True),
        StructField("statuscode", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_POWERPAGES_COMPONENT_SCHEMA = StructType(
    [
        StructField("powerpagecomponentid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ismanaged", BooleanType(), True),
        StructField("componentstate", IntegerType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_SYSTEMUSER_SCHEMA = StructType(
    [
        StructField("systemuserid", StringType(), True),
        StructField("applicationid", StringType(), True),
        StructField("fullname", StringType(), True),
        StructField("domainname", StringType(), True),
        StructField("internalemailaddress", StringType(), True),
        StructField("isdisabled", BooleanType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_PUBLISHER_SCHEMA = StructType(
    [
        StructField("publisherid", StringType(), True),
        StructField("uniquename", StringType(), True),
        StructField("friendlyname", StringType(), True),
        StructField("customizationprefix", StringType(), True),
        StructField("customizationoptionvalueprefix", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("emailaddress", StringType(), True),
        StructField("supportingwebsiteurl", StringType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

POWERPLAT_MAILBOX_SCHEMA = StructType(
    [
        StructField("mailboxid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("emailaddress", StringType(), True),
        StructField("statecode", IntegerType(), True),
        StructField("statuscode", IntegerType(), True),
        StructField("enabledforincomingemail", BooleanType(), True),
        StructField("enabledforoutgoingemail", BooleanType(), True),
        StructField("enabledforact", BooleanType(), True),
        StructField("allowemailconnectortousecredentials", BooleanType(), True),
        StructField("createdon", StringType(), True),
        StructField("modifiedon", StringType(), True),
    ]
)

# Dependency body fan-out schemas — extract only IDs needed for the silver key;
# all body content is preserved via _record passthrough.
POWERPLAT_FLOW_METADATA_ID_SCHEMA = StructType(
    [StructField("name", StringType(), True), StructField("envName", StringType(), True)]
)
POWERPLAT_WORKFLOW_DEFINITION_ID_SCHEMA = StructType(
    [StructField("workflowid", StringType(), True), StructField("envName", StringType(), True)]
)
POWERPLAT_APP_MODULE_XML_ID_SCHEMA = StructType(
    [StructField("appmoduleid", StringType(), True), StructField("envName", StringType(), True)]
)
POWERPLAT_BOT_CONFIGURATION_ID_SCHEMA = StructType(
    [StructField("botid", StringType(), True), StructField("envName", StringType(), True)]
)
POWERPLAT_BOT_COMPONENT_DATA_ID_SCHEMA = StructType(
    [StructField("botcomponentid", StringType(), True), StructField("envName", StringType(), True)]
)
POWERPLAT_WEB_RESOURCE_CONTENT_ID_SCHEMA = StructType(
    [StructField("webresourceid", StringType(), True), StructField("envName", StringType(), True)]
)
POWERPLAT_POWERPAGES_COMPONENT_CONTENT_ID_SCHEMA = StructType(
    [StructField("powerpagecomponentid", StringType(), True), StructField("envName", StringType(), True)]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Solutions
# MAGIC
# MAGIC **Source:** `powerplat_solutions`
# MAGIC **Key:** `solution_id` = `{source_key}_{solutionid}`

# COMMAND ----------


@dlt.table(name="powerplat_solutions_staged", comment="Staged Dataverse solution records.", temporary=True)
@dlt.expect_or_drop("valid_id", "solution_id IS NOT NULL")
def powerplat_solutions_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_solutions")
        .withColumn("r", from_json(col("_record"), POWERPLAT_SOLUTION_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("solution_id", concat_ws("_", col("source_key"), col("solutionid")))
        .withColumn("unique_name", col("uniquename"))
        .withColumn("friendly_name", col("friendlyname"))
        .withColumn("version", col("version"))
        .withColumn("is_managed", col("ismanaged"))
        .withColumn("is_visible", col("isvisible"))
        .withColumn("installed_at", to_timestamp(col("installedon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "solution_id",
            "solutionid",
            "unique_name",
            "friendly_name",
            "version",
            "is_managed",
            "is_visible",
            "installed_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_solutions",
    comment="Deduplicated Dataverse solutions. One row per solution per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_solutions",
    source="powerplat_solutions_staged",
    keys=["solution_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Solution Components
# MAGIC
# MAGIC **Source:** `powerplat_solution_components`
# MAGIC **Key:** `component_id` = `{source_key}_{solutioncomponentid}`

# COMMAND ----------


@dlt.table(
    name="powerplat_solution_components_staged",
    comment="Staged Dataverse solution component records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "component_id IS NOT NULL")
def powerplat_solution_components_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_solution_components")
        .withColumn("r", from_json(col("_record"), POWERPLAT_SOLUTION_COMPONENT_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("component_id", concat_ws("_", col("source_key"), col("solutioncomponentid")))
        .withColumn("object_id", col("objectid"))
        .withColumn("component_type", col("componenttype"))
        .withColumn("root_component_behavior", col("rootcomponentbehavior"))
        .withColumn("is_metadata", col("ismetadata"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "component_id",
            "solutioncomponentid",
            "object_id",
            "component_type",
            "root_component_behavior",
            "is_metadata",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_solution_components",
    comment="Deduplicated Dataverse solution components. One row per component per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_solution_components",
    source="powerplat_solution_components_staged",
    keys=["component_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Connection References
# MAGIC
# MAGIC **Source:** `powerplat_connection_references`
# MAGIC **Key:** `reference_id` = `{source_key}_{connectionreferenceid}`

# COMMAND ----------


@dlt.table(
    name="powerplat_connection_references_staged",
    comment="Staged Dataverse connection reference records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "reference_id IS NOT NULL")
def powerplat_connection_references_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_connection_references")
        .withColumn("r", from_json(col("_record"), POWERPLAT_CONNECTION_REFERENCE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("reference_id", concat_ws("_", col("source_key"), col("connectionreferenceid")))
        .withColumn("logical_name", col("connectionreferencelogicalname"))
        .withColumn("description", col("description"))
        .withColumn("connector_id", col("connectorid"))
        .withColumn("state_code", col("statecode"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "reference_id",
            "connectionreferenceid",
            "logical_name",
            "description",
            "connector_id",
            "state_code",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_connection_references",
    comment="Deduplicated Dataverse connection references. One row per reference per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_connection_references",
    source="powerplat_connection_references_staged",
    keys=["reference_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Environment Variable Definitions
# MAGIC
# MAGIC **Source:** `powerplat_env_variable_definitions`
# MAGIC **Key:** `definition_id` = `{source_key}_{environmentvariabledefinitionid}`

# COMMAND ----------


@dlt.table(
    name="powerplat_env_variable_definitions_staged",
    comment="Staged Dataverse environment variable definition records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "definition_id IS NOT NULL")
def powerplat_env_variable_definitions_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_env_variable_definitions")
        .withColumn("r", from_json(col("_record"), POWERPLAT_ENV_VAR_DEFINITION_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("definition_id", concat_ws("_", col("source_key"), col("environmentvariabledefinitionid")))
        .withColumn("schema_name", col("schemaname"))
        .withColumn("display_name", col("displayname"))
        .withColumn("description", col("description"))
        .withColumn("default_value", col("defaultvalue"))
        .withColumn("var_type", col("type"))
        .withColumn("state_code", col("statecode"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "definition_id",
            "environmentvariabledefinitionid",
            "schema_name",
            "display_name",
            "description",
            "default_value",
            "var_type",
            "state_code",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_env_variable_definitions",
    comment="Deduplicated Dataverse environment variable definitions. One row per definition per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_env_variable_definitions",
    source="powerplat_env_variable_definitions_staged",
    keys=["definition_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Environment Variable Values
# MAGIC
# MAGIC **Source:** `powerplat_env_variable_values`
# MAGIC **Key:** `value_id` = `{source_key}_{environmentvariablevalueid}`

# COMMAND ----------


@dlt.table(
    name="powerplat_env_variable_values_staged",
    comment="Staged Dataverse environment variable value records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "value_id IS NOT NULL")
def powerplat_env_variable_values_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_env_variable_values")
        .withColumn("r", from_json(col("_record"), POWERPLAT_ENV_VAR_VALUE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("value_id", concat_ws("_", col("source_key"), col("environmentvariablevalueid")))
        .withColumn("value", col("value"))
        .withColumn("state_code", col("statecode"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "value_id",
            "environmentvariablevalueid",
            "value",
            "state_code",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_env_variable_values",
    comment="Deduplicated Dataverse environment variable values. One row per value per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_env_variable_values",
    source="powerplat_env_variable_values_staged",
    keys=["value_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Tables
# MAGIC
# MAGIC **Source:** `powerplat_tables`
# MAGIC **Key:** `table_id` = `{source_key}_{MetadataId}`
# MAGIC
# MAGIC Custom Dataverse tables only (IsCustomEntity eq true, filtered at ingest).
# MAGIC `display_name` uses the English label (LanguageCode 1033) when present.

# COMMAND ----------


@dlt.table(name="powerplat_tables_staged", comment="Staged Dataverse custom table metadata records.", temporary=True)
@dlt.expect_or_drop("valid_id", "table_id IS NOT NULL")
def powerplat_tables_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_tables")
        .withColumn("r", from_json(col("_record"), POWERPLAT_TABLE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("table_id", concat_ws("_", col("source_key"), col("MetadataId")))
        .withColumn("logical_name", col("LogicalName"))
        .withColumn("schema_name", col("SchemaName"))
        .withColumn("entity_set_name", col("EntitySetName"))
        .withColumn("object_type_code", col("ObjectTypeCode"))
        .withColumn("is_custom_entity", col("IsCustomEntity"))
        .withColumn("is_managed", col("IsManaged"))
        .withColumn("external_name", col("ExternalName"))
        .withColumn("table_type", col("TableType"))
        .withColumn(
            "display_name",
            expr("element_at(filter(DisplayName.LocalizedLabels, x -> x.LanguageCode = 1033), 1).Label"),
        )
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "table_id",
            "MetadataId",
            "logical_name",
            "schema_name",
            "entity_set_name",
            "object_type_code",
            "is_custom_entity",
            "is_managed",
            "external_name",
            "table_type",
            "display_name",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_tables",
    comment="Deduplicated Dataverse custom tables. One row per table per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_tables",
    source="powerplat_tables_staged",
    keys=["table_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Workflows
# MAGIC
# MAGIC **Source:** `powerplat_workflows`
# MAGIC **Key:** `workflow_id` = `{source_key}_{workflowid}`
# MAGIC
# MAGIC Dataverse workflow metadata. Category: 0=classic, 4=BPF, 5=cloud, 6=desktop.
# MAGIC Definition body is in `powerplat_workflow_definitions`.

# COMMAND ----------


@dlt.table(name="powerplat_workflows_staged", comment="Staged Dataverse workflow records.", temporary=True)
@dlt.expect_or_drop("valid_id", "workflow_id IS NOT NULL")
def powerplat_workflows_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_workflows")
        .withColumn("r", from_json(col("_record"), POWERPLAT_WORKFLOW_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("workflow_id", concat_ws("_", col("source_key"), col("workflowid")))
        .withColumn("display_name", col("name"))
        .withColumn("description", col("description"))
        .withColumn("workflow_type", col("type"))
        .withColumn("category", col("category"))
        .withColumn("mode", col("mode"))
        .withColumn("is_subprocess", col("subprocess"))
        .withColumn("is_managed", col("ismanaged"))
        .withColumn("component_state", col("componentstate"))
        .withColumn("primary_entity", col("primaryentity"))
        .withColumn("scope", col("scope"))
        .withColumn("business_process_type", col("businessprocesstype"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "workflow_id",
            "workflowid",
            "display_name",
            "description",
            "workflow_type",
            "category",
            "mode",
            "is_subprocess",
            "is_managed",
            "component_state",
            "primary_entity",
            "scope",
            "business_process_type",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_workflows",
    comment="Deduplicated Dataverse workflows. One row per workflow per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_workflows",
    source="powerplat_workflows_staged",
    keys=["workflow_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Plugin Assemblies
# MAGIC
# MAGIC **Source:** `powerplat_plugin_assemblies`
# MAGIC **Key:** `assembly_id` = `{source_key}_{pluginassemblyid}`
# MAGIC
# MAGIC DLL bytes excluded at ingest (`content`/`content2` columns).

# COMMAND ----------


@dlt.table(
    name="powerplat_plugin_assemblies_staged", comment="Staged Dataverse plugin assembly records.", temporary=True
)
@dlt.expect_or_drop("valid_id", "assembly_id IS NOT NULL")
def powerplat_plugin_assemblies_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_plugin_assemblies")
        .withColumn("r", from_json(col("_record"), POWERPLAT_PLUGIN_ASSEMBLY_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("assembly_id", concat_ws("_", col("source_key"), col("pluginassemblyid")))
        .withColumn("assembly_name", col("name"))
        .withColumn("version", col("version"))
        .withColumn("culture", col("culture"))
        .withColumn("public_key_token", col("publickeytoken"))
        .withColumn("description", col("description"))
        .withColumn("isolation_mode", col("isolationmode"))
        .withColumn("source_type", col("sourcetype"))
        .withColumn("source_hash", col("sourcehash"))
        .withColumn("is_managed", col("ismanaged"))
        .withColumn("component_state", col("componentstate"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "assembly_id",
            "pluginassemblyid",
            "assembly_name",
            "version",
            "culture",
            "public_key_token",
            "description",
            "isolation_mode",
            "source_type",
            "source_hash",
            "is_managed",
            "component_state",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_plugin_assemblies",
    comment="Deduplicated Dataverse plugin assemblies. One row per assembly per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_plugin_assemblies",
    source="powerplat_plugin_assemblies_staged",
    keys=["assembly_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Plugin Steps
# MAGIC
# MAGIC **Source:** `powerplat_plugin_steps`
# MAGIC **Key:** `step_id` = `{source_key}_{sdkmessageprocessingstepid}`

# COMMAND ----------


@dlt.table(name="powerplat_plugin_steps_staged", comment="Staged Dataverse plugin step records.", temporary=True)
@dlt.expect_or_drop("valid_id", "step_id IS NOT NULL")
def powerplat_plugin_steps_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_plugin_steps")
        .withColumn("r", from_json(col("_record"), POWERPLAT_PLUGIN_STEP_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("step_id", concat_ws("_", col("source_key"), col("sdkmessageprocessingstepid")))
        .withColumn("step_name", col("name"))
        .withColumn("description", col("description"))
        .withColumn("mode", col("mode"))
        .withColumn("stage", col("stage"))
        .withColumn("rank", col("rank"))
        .withColumn("state_code", col("statecode"))
        .withColumn("is_managed", col("ismanaged"))
        .withColumn("component_state", col("componentstate"))
        .withColumn("async_auto_delete", col("asyncautodelete"))
        .withColumn("filtering_attributes", col("filteringattributes"))
        .withColumn("supported_deployment", col("supporteddeployment"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "step_id",
            "sdkmessageprocessingstepid",
            "step_name",
            "description",
            "mode",
            "stage",
            "rank",
            "state_code",
            "is_managed",
            "component_state",
            "async_auto_delete",
            "filtering_attributes",
            "supported_deployment",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_plugin_steps",
    comment="Deduplicated Dataverse plugin steps. One row per SDK message processing step per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_plugin_steps",
    source="powerplat_plugin_steps_staged",
    keys=["step_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Web Resources
# MAGIC
# MAGIC **Source:** `powerplat_web_resources`
# MAGIC **Key:** `resource_id` = `{source_key}_{webresourceid}`
# MAGIC
# MAGIC File content excluded at ingest; use `powerplat_web_resource_contents` for decoded body.

# COMMAND ----------


@dlt.table(name="powerplat_web_resources_staged", comment="Staged Dataverse web resource records.", temporary=True)
@dlt.expect_or_drop("valid_id", "resource_id IS NOT NULL")
def powerplat_web_resources_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_web_resources")
        .withColumn("r", from_json(col("_record"), POWERPLAT_WEB_RESOURCE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("resource_id", concat_ws("_", col("source_key"), col("webresourceid")))
        .withColumn("resource_name", col("name"))
        .withColumn("display_name", col("displayname"))
        .withColumn("description", col("description"))
        .withColumn("resource_type", col("webresourcetype"))
        .withColumn("language_code", col("languagecode"))
        .withColumn("is_managed", col("ismanaged"))
        .withColumn("component_state", col("componentstate"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "resource_id",
            "webresourceid",
            "resource_name",
            "display_name",
            "description",
            "resource_type",
            "language_code",
            "is_managed",
            "component_state",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_web_resources",
    comment="Deduplicated Dataverse web resources. One row per web resource per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_web_resources",
    source="powerplat_web_resources_staged",
    keys=["resource_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform App Modules
# MAGIC
# MAGIC **Source:** `powerplat_app_modules`
# MAGIC **Key:** `app_module_id` = `{source_key}_{appmoduleid}`
# MAGIC
# MAGIC Model-driven apps. Sitemap/descriptor XML in `powerplat_app_module_xml`.

# COMMAND ----------


@dlt.table(name="powerplat_app_modules_staged", comment="Staged Dataverse model-driven app records.", temporary=True)
@dlt.expect_or_drop("valid_id", "app_module_id IS NOT NULL")
def powerplat_app_modules_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_app_modules")
        .withColumn("r", from_json(col("_record"), POWERPLAT_APP_MODULE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("app_module_id", concat_ws("_", col("source_key"), col("appmoduleid")))
        .withColumn("app_name", col("name"))
        .withColumn("unique_name", col("uniquename"))
        .withColumn("description", col("description"))
        .withColumn("form_factor", col("formfactor"))
        .withColumn("is_featured", col("isfeatured"))
        .withColumn("is_managed", col("ismanaged"))
        .withColumn("component_state", col("componentstate"))
        .withColumn("navigation_type", col("navigationtype"))
        .withColumn("client_type", col("clienttype"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "app_module_id",
            "appmoduleid",
            "app_name",
            "unique_name",
            "description",
            "form_factor",
            "is_featured",
            "is_managed",
            "component_state",
            "navigation_type",
            "client_type",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_app_modules",
    comment="Deduplicated Dataverse model-driven apps. One row per app module per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_app_modules",
    source="powerplat_app_modules_staged",
    keys=["app_module_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Bots
# MAGIC
# MAGIC **Source:** `powerplat_bots`
# MAGIC **Key:** `bot_id` = `{source_key}_{botid}`
# MAGIC
# MAGIC Copilot Studio agents and classic PVA bots. Configuration JSON in `powerplat_bot_configurations`.

# COMMAND ----------


@dlt.table(name="powerplat_bots_staged", comment="Staged Dataverse Copilot Studio bot records.", temporary=True)
@dlt.expect_or_drop("valid_id", "bot_id IS NOT NULL")
def powerplat_bots_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_bots")
        .withColumn("r", from_json(col("_record"), POWERPLAT_BOT_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("bot_id", concat_ws("_", col("source_key"), col("botid")))
        .withColumn("bot_name", col("name"))
        .withColumn("schema_name", col("schemaname"))
        .withColumn("language", col("language"))
        .withColumn("is_managed", col("ismanaged"))
        .withColumn("component_state", col("componentstate"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "bot_id",
            "botid",
            "bot_name",
            "schema_name",
            "language",
            "is_managed",
            "component_state",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_bots",
    comment="Deduplicated Dataverse Copilot Studio bots. One row per bot per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_bots",
    source="powerplat_bots_staged",
    keys=["bot_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Bot Components
# MAGIC
# MAGIC **Source:** `powerplat_bot_components`
# MAGIC **Key:** `bot_component_id` = `{source_key}_{botcomponentid}`
# MAGIC
# MAGIC Topics, skills, knowledge sources. OBI body data in `powerplat_bot_component_data`.

# COMMAND ----------


@dlt.table(name="powerplat_bot_components_staged", comment="Staged Dataverse bot component records.", temporary=True)
@dlt.expect_or_drop("valid_id", "bot_component_id IS NOT NULL")
def powerplat_bot_components_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_bot_components")
        .withColumn("r", from_json(col("_record"), POWERPLAT_BOT_COMPONENT_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("bot_component_id", concat_ws("_", col("source_key"), col("botcomponentid")))
        .withColumn("component_name", col("name"))
        .withColumn("schema_name", col("schemaname"))
        .withColumn("component_type", col("componenttype"))
        .withColumn("is_managed", col("ismanaged"))
        .withColumn("component_state", col("componentstate"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "bot_component_id",
            "botcomponentid",
            "component_name",
            "schema_name",
            "component_type",
            "is_managed",
            "component_state",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_bot_components",
    comment="Deduplicated Dataverse bot components. One row per component per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_bot_components",
    source="powerplat_bot_components_staged",
    keys=["bot_component_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform AI Models
# MAGIC
# MAGIC **Source:** `powerplat_ai_models`
# MAGIC **Key:** `ai_model_id` = `{source_key}_{msdyn_aimodelid}`

# COMMAND ----------


@dlt.table(name="powerplat_ai_models_staged", comment="Staged Dataverse AI model records.", temporary=True)
@dlt.expect_or_drop("valid_id", "ai_model_id IS NOT NULL")
def powerplat_ai_models_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_ai_models")
        .withColumn("r", from_json(col("_record"), POWERPLAT_AI_MODEL_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("ai_model_id", concat_ws("_", col("source_key"), col("msdyn_aimodelid")))
        .withColumn("model_name", col("msdyn_name"))
        .withColumn("is_managed", col("ismanaged"))
        .withColumn("component_state", col("componentstate"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "ai_model_id",
            "msdyn_aimodelid",
            "model_name",
            "is_managed",
            "component_state",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_ai_models",
    comment="Deduplicated Dataverse AI models. One row per model per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_ai_models",
    source="powerplat_ai_models_staged",
    keys=["ai_model_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Pages Websites
# MAGIC
# MAGIC **Source:** `powerplat_powerpages_websites`
# MAGIC **Key:** `website_id` = `{source_key}_{mspp_websiteid}`

# COMMAND ----------


@dlt.table(name="powerplat_powerpages_websites_staged", comment="Staged Power Pages website records.", temporary=True)
@dlt.expect_or_drop("valid_id", "website_id IS NOT NULL")
def powerplat_powerpages_websites_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_powerpages_websites")
        .withColumn("r", from_json(col("_record"), POWERPLAT_POWERPAGES_WEBSITE_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("website_id", concat_ws("_", col("source_key"), col("mspp_websiteid")))
        .withColumn("site_name", col("mspp_name"))
        .withColumn("primary_domain", col("mspp_primarydomainname"))
        .withColumn("state_code", col("statecode"))
        .withColumn("status_code", col("statuscode"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "website_id",
            "mspp_websiteid",
            "site_name",
            "primary_domain",
            "state_code",
            "status_code",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_powerpages_websites",
    comment="Deduplicated Power Pages websites. One row per website per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_powerpages_websites",
    source="powerplat_powerpages_websites_staged",
    keys=["website_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Pages Components
# MAGIC
# MAGIC **Source:** `powerplat_powerpages_components`
# MAGIC **Key:** `pp_component_id` = `{source_key}_{powerpagecomponentid}`
# MAGIC
# MAGIC Unified replacement for legacy mspp_* component tables. Content in `powerplat_powerpages_component_contents`.

# COMMAND ----------


@dlt.table(
    name="powerplat_powerpages_components_staged", comment="Staged Power Pages component records.", temporary=True
)
@dlt.expect_or_drop("valid_id", "pp_component_id IS NOT NULL")
def powerplat_powerpages_components_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_powerpages_components")
        .withColumn("r", from_json(col("_record"), POWERPLAT_POWERPAGES_COMPONENT_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("pp_component_id", concat_ws("_", col("source_key"), col("powerpagecomponentid")))
        .withColumn("component_name", col("name"))
        .withColumn("is_managed", col("ismanaged"))
        .withColumn("component_state", col("componentstate"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "pp_component_id",
            "powerpagecomponentid",
            "component_name",
            "is_managed",
            "component_state",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_powerpages_components",
    comment="Deduplicated Power Pages components. One row per component per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_powerpages_components",
    source="powerplat_powerpages_components_staged",
    keys=["pp_component_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform System Users
# MAGIC
# MAGIC **Source:** `powerplat_systemusers`
# MAGIC **Key:** `systemuser_id` = `{source_key}_{systemuserid}`
# MAGIC
# MAGIC SP-backed application users only (applicationid IS NOT NULL, filtered at ingest).

# COMMAND ----------


@dlt.table(name="powerplat_systemusers_staged", comment="Staged Dataverse system user (SP) records.", temporary=True)
@dlt.expect_or_drop("valid_id", "systemuser_id IS NOT NULL")
def powerplat_systemusers_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_systemusers")
        .withColumn("r", from_json(col("_record"), POWERPLAT_SYSTEMUSER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("systemuser_id", concat_ws("_", col("source_key"), col("systemuserid")))
        .withColumn("application_id", col("applicationid"))
        .withColumn("full_name", trim(col("fullname")))
        .withColumn("domain_name", col("domainname"))
        .withColumn("email_address", lower(trim(col("internalemailaddress"))))
        .withColumn("is_disabled", col("isdisabled"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "systemuser_id",
            "systemuserid",
            "application_id",
            "full_name",
            "domain_name",
            "email_address",
            "is_disabled",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_systemusers",
    comment="Deduplicated Dataverse SP-backed system users. One row per application user per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_systemusers",
    source="powerplat_systemusers_staged",
    keys=["systemuser_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Publishers
# MAGIC
# MAGIC **Source:** `powerplat_publishers`
# MAGIC **Key:** `publisher_id` = `{source_key}_{publisherid}`

# COMMAND ----------


@dlt.table(name="powerplat_publishers_staged", comment="Staged Dataverse solution publisher records.", temporary=True)
@dlt.expect_or_drop("valid_id", "publisher_id IS NOT NULL")
def powerplat_publishers_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_publishers")
        .withColumn("r", from_json(col("_record"), POWERPLAT_PUBLISHER_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("publisher_id", concat_ws("_", col("source_key"), col("publisherid")))
        .withColumn("unique_name", col("uniquename"))
        .withColumn("friendly_name", col("friendlyname"))
        .withColumn("customization_prefix", col("customizationprefix"))
        .withColumn("customization_option_value_prefix", col("customizationoptionvalueprefix"))
        .withColumn("description", col("description"))
        .withColumn("email_address", lower(trim(col("emailaddress"))))
        .withColumn("website_url", col("supportingwebsiteurl"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "publisher_id",
            "publisherid",
            "unique_name",
            "friendly_name",
            "customization_prefix",
            "customization_option_value_prefix",
            "description",
            "email_address",
            "website_url",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_publishers",
    comment="Deduplicated Dataverse solution publishers. One row per publisher per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_publishers",
    source="powerplat_publishers_staged",
    keys=["publisher_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Mailboxes
# MAGIC
# MAGIC **Source:** `powerplat_mailboxes`
# MAGIC **Key:** `mailbox_id` = `{source_key}_{mailboxid}`

# COMMAND ----------


@dlt.table(name="powerplat_mailboxes_staged", comment="Staged Dataverse mailbox records.", temporary=True)
@dlt.expect_or_drop("valid_id", "mailbox_id IS NOT NULL")
def powerplat_mailboxes_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_mailboxes")
        .withColumn("r", from_json(col("_record"), POWERPLAT_MAILBOX_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"))
        .withColumn("mailbox_id", concat_ws("_", col("source_key"), col("mailboxid")))
        .withColumn("mailbox_name", col("name"))
        .withColumn("email_address", lower(trim(col("emailaddress"))))
        .withColumn("state_code", col("statecode"))
        .withColumn("status_code", col("statuscode"))
        .withColumn("enabled_for_incoming", col("enabledforincomingemail"))
        .withColumn("enabled_for_outgoing", col("enabledforoutgoingemail"))
        .withColumn("enabled_for_act", col("enabledforact"))
        .withColumn("allow_email_connector_credentials", col("allowemailconnectortousecredentials"))
        .withColumn("created_at", to_timestamp(col("createdon")))
        .withColumn("modified_at", to_timestamp(col("modifiedon")))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", coalesce(to_timestamp(col("modifiedon")), col("_dlt_ingested_at")))
        .select(
            "mailbox_id",
            "mailboxid",
            "mailbox_name",
            "email_address",
            "state_code",
            "status_code",
            "enabled_for_incoming",
            "enabled_for_outgoing",
            "enabled_for_act",
            "allow_email_connector_credentials",
            "created_at",
            "modified_at",
            "environment",
            "source_key",
            "last_updated_at",
        )
    )


dlt.create_streaming_table(
    name="powerplat_mailboxes",
    comment="Deduplicated Dataverse mailbox configurations. One row per mailbox per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_mailboxes",
    source="powerplat_mailboxes_staged",
    keys=["mailbox_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables - Power Platform / Dependency Bodies
# MAGIC
# MAGIC Per-row fan-out stages carrying heavy definition bodies excluded from bulk-list
# MAGIC calls. Each table stores the raw body in `_record` and exposes only the ID and
# MAGIC FK columns as typed fields. `last_updated_at` uses `_dlt_ingested_at` (no
# MAGIC modified-time available on point-in-time fetches).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Flow Metadata
# MAGIC
# MAGIC **Source:** `powerplat_flow_metadata`
# MAGIC **Key:** `flow_id` = `{source_key}_{name}` — aligns with `powerplat_flows.flow_id`
# MAGIC
# MAGIC Admin V1 per-flow payload (non-Dataverse flows only). Includes `connectionReferences`,
# MAGIC `definitionSummary`, and `referencedResources` in `_record`.

# COMMAND ----------


@dlt.table(
    name="powerplat_flow_metadata_staged", comment="Staged Power Automate flow metadata body records.", temporary=True
)
@dlt.expect_or_drop("valid_id", "flow_id IS NOT NULL")
def powerplat_flow_metadata_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_flow_metadata")
        .withColumn("r", from_json(col("_record"), POWERPLAT_FLOW_METADATA_ID_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("flow_id", concat_ws("_", col("source_key"), col("name")))
        .withColumn("flow_object_id", col("name"))
        .withColumn("env_name", col("envName"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "flow_id",
            "flow_object_id",
            "env_name",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerplat_flow_metadata",
    comment="Deduplicated Power Automate flow metadata bodies. One row per non-DV flow per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_flow_metadata",
    source="powerplat_flow_metadata_staged",
    keys=["flow_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Workflow Definitions
# MAGIC
# MAGIC **Source:** `powerplat_workflow_definitions`
# MAGIC **Key:** `workflow_id` = `{source_key}_{workflowid}` — aligns with `powerplat_workflows.workflow_id`
# MAGIC
# MAGIC Dataverse workflow Memo columns: `clientdata` (cloud flow JSON), `xaml` (classic/BPF),
# MAGIC `inputparameters`. All stored in `_record`.

# COMMAND ----------


@dlt.table(
    name="powerplat_workflow_definitions_staged",
    comment="Staged Dataverse workflow definition body records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "workflow_id IS NOT NULL")
def powerplat_workflow_definitions_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_workflow_definitions")
        .withColumn("r", from_json(col("_record"), POWERPLAT_WORKFLOW_DEFINITION_ID_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("workflow_id", concat_ws("_", col("source_key"), col("workflowid")))
        .withColumn("env_name", col("envName"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "workflow_id",
            "workflowid",
            "env_name",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerplat_workflow_definitions",
    comment="Deduplicated Dataverse workflow definition bodies. One row per workflow per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_workflow_definitions",
    source="powerplat_workflow_definitions_staged",
    keys=["workflow_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform App Module XML
# MAGIC
# MAGIC **Source:** `powerplat_app_module_xml`
# MAGIC **Key:** `app_module_id` = `{source_key}_{appmoduleid}` — aligns with `powerplat_app_modules.app_module_id`
# MAGIC
# MAGIC Model-driven app sitemap XML (`appmodulexmlmanaged`) and descriptor JSON in `_record`.

# COMMAND ----------


@dlt.table(
    name="powerplat_app_module_xml_staged",
    comment="Staged Dataverse model-driven app XML body records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "app_module_id IS NOT NULL")
def powerplat_app_module_xml_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_app_module_xml")
        .withColumn("r", from_json(col("_record"), POWERPLAT_APP_MODULE_XML_ID_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("app_module_id", concat_ws("_", col("source_key"), col("appmoduleid")))
        .withColumn("env_name", col("envName"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "app_module_id",
            "appmoduleid",
            "env_name",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerplat_app_module_xml",
    comment="Deduplicated Dataverse model-driven app XML bodies. One row per app module per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_app_module_xml",
    source="powerplat_app_module_xml_staged",
    keys=["app_module_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Bot Configurations
# MAGIC
# MAGIC **Source:** `powerplat_bot_configurations`
# MAGIC **Key:** `bot_id` = `{source_key}_{botid}` — aligns with `powerplat_bots.bot_id`
# MAGIC
# MAGIC Copilot Studio bot `configuration` JSON (channel bindings, OAuth, AAD refs) in `_record`.

# COMMAND ----------


@dlt.table(
    name="powerplat_bot_configurations_staged",
    comment="Staged Copilot Studio bot configuration body records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "bot_id IS NOT NULL")
def powerplat_bot_configurations_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_bot_configurations")
        .withColumn("r", from_json(col("_record"), POWERPLAT_BOT_CONFIGURATION_ID_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("bot_id", concat_ws("_", col("source_key"), col("botid")))
        .withColumn("env_name", col("envName"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "bot_id",
            "botid",
            "env_name",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerplat_bot_configurations",
    comment="Deduplicated Copilot Studio bot configuration bodies. One row per bot per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_bot_configurations",
    source="powerplat_bot_configurations_staged",
    keys=["bot_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Bot Component Data
# MAGIC
# MAGIC **Source:** `powerplat_bot_component_data`
# MAGIC **Key:** `bot_component_id` = `{source_key}_{botcomponentid}` — aligns with `powerplat_bot_components.bot_component_id`
# MAGIC
# MAGIC OBI-format topic/skill/dialog body (`data`) in `_record`.

# COMMAND ----------


@dlt.table(
    name="powerplat_bot_component_data_staged",
    comment="Staged Copilot Studio bot component data body records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "bot_component_id IS NOT NULL")
def powerplat_bot_component_data_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_bot_component_data")
        .withColumn("r", from_json(col("_record"), POWERPLAT_BOT_COMPONENT_DATA_ID_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("bot_component_id", concat_ws("_", col("source_key"), col("botcomponentid")))
        .withColumn("env_name", col("envName"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "bot_component_id",
            "botcomponentid",
            "env_name",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerplat_bot_component_data",
    comment="Deduplicated Copilot Studio bot component data bodies. One row per component per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_bot_component_data",
    source="powerplat_bot_component_data_staged",
    keys=["bot_component_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Platform Web Resource Contents
# MAGIC
# MAGIC **Source:** `powerplat_web_resource_contents`
# MAGIC **Key:** `resource_id` = `{source_key}_{webresourceid}` — aligns with `powerplat_web_resources.resource_id`
# MAGIC
# MAGIC Base64-encoded file body (`content`) in `_record`.

# COMMAND ----------


@dlt.table(
    name="powerplat_web_resource_contents_staged",
    comment="Staged Dataverse web resource content body records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "resource_id IS NOT NULL")
def powerplat_web_resource_contents_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_web_resource_contents")
        .withColumn("r", from_json(col("_record"), POWERPLAT_WEB_RESOURCE_CONTENT_ID_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("resource_id", concat_ws("_", col("source_key"), col("webresourceid")))
        .withColumn("env_name", col("envName"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "resource_id",
            "webresourceid",
            "env_name",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerplat_web_resource_contents",
    comment="Deduplicated Dataverse web resource content bodies. One row per resource per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_web_resource_contents",
    source="powerplat_web_resource_contents_staged",
    keys=["resource_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power Pages Component Contents
# MAGIC
# MAGIC **Source:** `powerplat_powerpages_component_contents`
# MAGIC **Key:** `pp_component_id` = `{source_key}_{powerpagecomponentid}` — aligns with `powerplat_powerpages_components.pp_component_id`
# MAGIC
# MAGIC Liquid template body (`content`) and file binary (`filecontent`) in `_record`.

# COMMAND ----------


@dlt.table(
    name="powerplat_powerpages_component_contents_staged",
    comment="Staged Power Pages component content body records.",
    temporary=True,
)
@dlt.expect_or_drop("valid_id", "pp_component_id IS NOT NULL")
def powerplat_powerpages_component_contents_staged():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.powerplat_powerpages_component_contents")
        .withColumn("r", from_json(col("_record"), POWERPLAT_POWERPAGES_COMPONENT_CONTENT_ID_SCHEMA))
        .select(col("r.*"), col("source_key"), col("batch_id"), col("_dlt_ingested_at"), col("_record"))
        .withColumn("pp_component_id", concat_ws("_", col("source_key"), col("powerpagecomponentid")))
        .withColumn("env_name", col("envName"))
        .withColumn("environment", environment_col())
        .withColumn("last_updated_at", col("_dlt_ingested_at"))
        .select(
            "pp_component_id",
            "powerpagecomponentid",
            "env_name",
            "environment",
            "source_key",
            "last_updated_at",
            "_record",
        )
    )


dlt.create_streaming_table(
    name="powerplat_powerpages_component_contents",
    comment="Deduplicated Power Pages component content bodies. One row per component per tenant. SCD Type 1.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)

dlt.apply_changes(
    target="powerplat_powerpages_component_contents",
    source="powerplat_powerpages_component_contents_staged",
    keys=["pp_component_id"],
    sequence_by=col("last_updated_at"),
    stored_as_scd_type=1,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Organizations (config)
# MAGIC
# MAGIC **Source:** ADLS organizations.json (no bronze table - static config file)
# MAGIC **Key:** org_id = md5(lower(trim(org_key)))
# MAGIC **Pipeline param:** organizations_config_path (e.g. abfss://landing@<storage>.dfs.core.windows.net/config/organizations.json)

# COMMAND ----------


@dlt.table(
    name="organizations",
    comment="CDO/AE org registry loaded from organizations.json config. One row per org. Batch read from ADLS - no bronze streaming table. Set organizations_config_path DLT pipeline param to enable.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
def organizations():
    from pyspark.sql.types import StructType, StructField, StringType, ArrayType

    json_schema = StructType(
        [
            StructField("org_key", StringType(), True),
            StructField("org_name", StringType(), True),
            StructField("upn_domains", ArrayType(StringType()), True),
            StructField("mail_domains", ArrayType(StringType()), True),
            StructField("on_prem_domains", ArrayType(StringType()), True),
        ]
    )
    empty_schema = StructType(
        [
            StructField("org_id", StringType(), True),
            StructField("org_key", StringType(), True),
            StructField("org_name", StringType(), True),
            StructField("upn_domains", ArrayType(StringType()), True),
            StructField("mail_domains", ArrayType(StringType()), True),
            StructField("on_prem_domains", ArrayType(StringType()), True),
        ]
    )

    if not ORGANIZATIONS_CONFIG_PATH:
        return spark.createDataFrame([], empty_schema)
    try:
        raw = spark.read.schema(json_schema).option("multiline", "true").json(ORGANIZATIONS_CONFIG_PATH)
    except Exception as exc:
        # Only swallow "path does not exist" — this is expected when the config file
        # has not yet been uploaded to the landing zone (e.g. first pipeline run
        # before CI/CD has copied organizations.json).  Any other error (bad storage
        # credentials, malformed JSON, schema mismatch) should propagate so it is
        # visible in the DLT event log rather than silently producing empty org_ids.
        exc_str = str(exc).lower()
        if "path does not exist" in exc_str or "filenotfoundexception" in exc_str:
            return spark.createDataFrame([], empty_schema)
        raise

    normalized = raw.withColumn("org_key", lower(trim(col("org_key")))).filter(
        col("org_key").isNotNull() & (col("org_key") != "")
    )

    return normalized.select(
        md5(col("org_key")).alias("org_id"),
        col("org_key"),
        col("org_name"),
        col("upn_domains"),
        col("mail_domains"),
        col("on_prem_domains"),
    ).dropDuplicates(["org_id", "org_key"])
