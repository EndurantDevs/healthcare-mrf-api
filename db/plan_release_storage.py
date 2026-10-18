# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native schemas for immutable plan-release binding and pin cohorts."""

import os
import re

BINDING_TABLE = "plan_release_snapshot_binding"
LEGACY_BINDING_TABLE = "plan_release_snapshot_binding_legacy"
PIN_TABLE = "plan_release_snapshot_pin"
REVISION_TABLE = "plan_release_serving_revision"
CONTRACT = "plan-release-list-cohort-v1"

HEADER_COLUMNS = (
    ("serving_revision_id", "character varying(64)", True, None),
    ("plan_release_id", "character varying(64)", True, None),
    ("healthporta_plan_id", "character varying(64)", True, None),
    ("plan_version_id", "character varying(64)", False, None),
    ("release_month", "character varying(7)", True, None),
    ("release_status", "character varying(16)", True, None),
    ("serving_status", "character varying(16)", True, None),
    ("is_current", "boolean", True, "false"),
    ("expected_binding_count", "integer", True, None),
    ("binding_set_digest", "character varying(64)", True, None),
    ("source_manifest", "jsonb", True, "'{}'::jsonb"),
    ("created_at", "timestamp with time zone", True, "transaction_timestamp()"),
    ("published_at", "timestamp with time zone", False, None),
    ("retired_at", "timestamp with time zone", False, None),
)
BINDING_COLUMNS = (
    ("serving_revision_id", "character varying(64)", True, None),
    ("binding_ordinal", "integer", True, None),
    ("snapshot_id", "character varying(128)", True, None),
    ("source_key", "character varying(128)", True, None),
    ("plan_id", "character varying(128)", True, None),
    ("plan_market_type", "character varying(64)", False, None),
    ("role", "character varying(32)", True, None),
    ("required", "boolean", True, "true"),
    ("metadata_json", "jsonb", True, "'{}'::jsonb"),
    ("created_at", "timestamp with time zone", True, "transaction_timestamp()"),
)
PIN_COLUMNS = (
    ("serving_revision_id", "character varying(64)", True, None),
    ("snapshot_id", "character varying(128)", True, None),
    ("reason", "character varying(256)", False, None),
    ("created_at", "timestamp with time zone", True, "transaction_timestamp()"),
)
BINDING_CHECKS = {
    "plan_release_snapshot_binding_ordinal_check": "binding_ordinal >= 0",
    "plan_release_snapshot_binding_role_check": "role IN ('in_network', 'allowed_amounts')",
    "plan_release_snapshot_binding_source_check": "btrim(source_key) <> ''",
    "plan_release_snapshot_binding_plan_check": "btrim(plan_id) <> ''",
}
PIN_CHECKS = {
    "plan_release_snapshot_pin_revision_check": "serving_revision_id ~ '^hpserve_[0-9A-HJKMNP-TV-Z]{26}$'",
    "plan_release_snapshot_pin_snapshot_check": "btrim(snapshot_id) <> ''",
}


def plan_release_schema():
    """Keep the existing explicit projection schema environment contract."""
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    return runtime or legacy or "mrf"


def storage_identifier(name):
    """Keep the existing bounded SQL identifier contract without runtime imports."""
    if type(name) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", name) is None:
        raise ValueError("plan_release_storage_identifier_invalid")
    return '"' + name + '"'


def storage_relation(schema_name, table_name):
    """Quote both server-selected identifiers with the existing strict helper."""
    return f"{storage_identifier(schema_name)}.{storage_identifier(table_name)}"


def storage_table_sql(schema_name, table_name, *, pin=False, partitioned=False, temporary=False, primary_key=True):
    """Create a native table; detached heaps have the same essential contract."""
    column_specs = PIN_COLUMNS if pin else BINDING_COLUMNS
    check_expressions = PIN_CHECKS if pin else BINDING_CHECKS
    columns = []
    for name, sql_type, required, default in column_specs:
        clause = f"{storage_identifier(name)} {sql_type}" + (" NOT NULL" if required else "")
        columns.append(clause + (f" DEFAULT {default}" if default else ""))
    columns.extend(
        f"CONSTRAINT {storage_identifier(name)} CHECK ({expression})" for name, expression in check_expressions.items()
    )
    key = "serving_revision_id, snapshot_id" if pin else "serving_revision_id, role, binding_ordinal"
    if primary_key:
        columns.append(f"PRIMARY KEY ({key})")
    prefix = "CREATE TEMPORARY TABLE" if temporary else "CREATE TABLE"
    suffix = " PARTITION BY LIST (serving_revision_id)" if partitioned else ""
    return f"{prefix} {storage_relation(schema_name, table_name)} ({', '.join(columns)}){suffix}"


def storage_snapshot_index_sql(schema_name, table_name, index_name):
    """Prepare the retention anti-join index before publication."""
    return f"CREATE INDEX {storage_identifier(index_name)} ON {storage_relation(schema_name, table_name)} (snapshot_id)"
