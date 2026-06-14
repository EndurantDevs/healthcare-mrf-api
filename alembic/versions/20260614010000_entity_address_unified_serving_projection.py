"""Add entity address serving projection tables.

Revision ID: 20260614010000
Revises: 20260613190000_reapply_address_canonical_identity_hotfixes
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260614010000_entity_address_unified_serving_projection"
down_revision = "20260613190000_reapply_address_canonical_identity_hotfixes"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _add_column_if_missing(bind, schema: str, table: str, column: str, ddl: str) -> None:
    exists = bind.execute(
        text(
            """
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = :schema
           AND table_name = :table
           AND column_name = :column
        """
        ),
        {"schema": schema, "table": table, "column": column},
    ).scalar()
    if not exists:
        bind.exec_driver_sql(f"ALTER TABLE {_qt(schema, table)} ADD COLUMN {ddl};")


def _table_exists(bind, schema: str, table: str) -> bool:
    return bool(
        bind.execute(
            text("SELECT to_regclass(:name)"),
            {"name": f"{schema}.{table}"},
        ).scalar()
    )


def _entity_address_unified_columns(bind, schema: str) -> None:
    table = "entity_address_unified"
    if not _table_exists(bind, schema, table):
        return
    columns = (
        ("location_key", "location_key varchar(64)"),
        ("row_origin", "row_origin varchar(32) NOT NULL DEFAULT 'base'"),
        ("archive_identity_version", "archive_identity_version varchar(16) NOT NULL DEFAULT 'v1'"),
        ("address_precision", "address_precision varchar(32) NOT NULL DEFAULT 'unknown'"),
        ("premise_key", "premise_key uuid"),
        ("zip5", "zip5 varchar(5)"),
        ("state_code", "state_code varchar(2)"),
        ("city_norm", "city_norm varchar"),
        ("county_fips", "county_fips varchar(5)"),
        ("source_mask", "source_mask bigint NOT NULL DEFAULT 0"),
        ("address_source_mask", "address_source_mask bigint NOT NULL DEFAULT 0"),
        ("independent_source_count", "independent_source_count integer NOT NULL DEFAULT 0"),
        ("location_confidence_id", "location_confidence_id smallint NOT NULL DEFAULT 0"),
        ("confidence_score", "confidence_score smallint"),
        ("freshness_score", "freshness_score smallint"),
        ("aca_plan_array", "aca_plan_array varchar[] NOT NULL DEFAULT '{}'::varchar[]"),
        ("aca_network_array", "aca_network_array varchar[] NOT NULL DEFAULT '{}'::varchar[]"),
        ("ptg_plan_array", "ptg_plan_array varchar[] NOT NULL DEFAULT '{}'::varchar[]"),
        ("ptg_source_array", "ptg_source_array varchar[] NOT NULL DEFAULT '{}'::varchar[]"),
        ("group_plan_array", "group_plan_array varchar[] NOT NULL DEFAULT '{}'::varchar[]"),
        ("base_address_version", "base_address_version varchar(64)"),
        ("ptg_address_version", "ptg_address_version varchar(64)"),
        ("last_seen_at", "last_seen_at timestamp"),
    )
    for column, ddl in columns:
        _add_column_if_missing(bind, schema, table, column, ddl)


def _create_tables(bind, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg_address")} (
            node_id varchar,
            source_key varchar NOT NULL,
            snapshot_id varchar NOT NULL,
            plan_id varchar,
            ptg_plan_id varchar,
            market_type varchar,
            provider_group_id varchar,
            provider_set_id varchar,
            npi bigint,
            tin varchar,
            location_key varchar(64) NOT NULL,
            address_key uuid,
            premise_key uuid,
            archive_identity_version varchar(16) NOT NULL DEFAULT 'v1',
            address_precision varchar(32) NOT NULL DEFAULT 'unknown',
            address_source_id smallint NOT NULL DEFAULT 0,
            address_source_record_key varchar,
            address_role_id smallint NOT NULL DEFAULT 0,
            location_confidence_id smallint NOT NULL DEFAULT 0,
            zip5 varchar(5),
            state_code varchar(2),
            city_norm varchar,
            county_fips varchar(5),
            lat numeric(11,8),
            long numeric(11,8),
            ptg_plan_array varchar[] NOT NULL DEFAULT '{{}}'::varchar[],
            ptg_source_array varchar[] NOT NULL DEFAULT '{{}}'::varchar[],
            group_plan_array varchar[] NOT NULL DEFAULT '{{}}'::varchar[],
            base_address_version varchar(64),
            ptg_snapshot_published_at timestamptz,
            observed_at timestamptz,
            updated_at timestamptz,
            PRIMARY KEY (source_key, snapshot_id, location_key)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "entity_address_evidence")} (
            evidence_id bigserial PRIMARY KEY,
            location_key varchar(64) NOT NULL,
            address_key uuid,
            premise_key uuid,
            archive_identity_version varchar(16),
            entity_type varchar(64) NOT NULL,
            entity_id varchar(128) NOT NULL,
            npi bigint,
            tin varchar,
            source_id smallint NOT NULL,
            source_record_key varchar,
            source_run_id varchar NOT NULL,
            source_snapshot_id varchar,
            node_id varchar,
            plan_id varchar,
            network_id varchar,
            ptg_plan_id varchar,
            ptg_source_key varchar,
            ptg_snapshot_id varchar,
            market_type varchar,
            address_role_id smallint,
            location_confidence_id smallint NOT NULL DEFAULT 0,
            address_precision varchar(32),
            observed_at timestamptz,
            last_seen_at timestamptz,
            retired_at timestamptz
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "entity_address_plan_bridge")} (
            location_key varchar(64) NOT NULL,
            entity_type varchar(64) NOT NULL,
            entity_id varchar(128) NOT NULL,
            plan_id varchar NOT NULL,
            market_type varchar,
            PRIMARY KEY (location_key, entity_type, entity_id, plan_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "entity_address_network_bridge")} (
            location_key varchar(64) NOT NULL,
            entity_type varchar(64) NOT NULL,
            entity_id varchar(128) NOT NULL,
            network_id varchar NOT NULL,
            PRIMARY KEY (location_key, entity_type, entity_id, network_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "entity_address_ptg_bridge")} (
            location_key varchar(64) NOT NULL,
            entity_type varchar(64) NOT NULL,
            entity_id varchar(128) NOT NULL,
            source_key varchar NOT NULL,
            snapshot_id varchar NOT NULL,
            ptg_plan_id varchar NOT NULL,
            PRIMARY KEY (location_key, entity_type, entity_id, source_key, snapshot_id, ptg_plan_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "entity_address_procedure_bridge")} (
            location_key varchar(64) NOT NULL,
            npi bigint NOT NULL,
            code_system varchar NOT NULL,
            code varchar NOT NULL,
            PRIMARY KEY (location_key, npi, code_system, code)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "entity_address_medication_bridge")} (
            location_key varchar(64) NOT NULL,
            npi bigint NOT NULL,
            code_system varchar NOT NULL,
            code varchar NOT NULL,
            PRIMARY KEY (location_key, npi, code_system, code)
        )
        """,
    )
    for statement in statements:
        bind.exec_driver_sql(statement)


def _create_indexes(bind, schema: str) -> None:
    statements = (
        f"CREATE INDEX IF NOT EXISTS entity_address_unified_location_key_idx ON {_qt(schema, 'entity_address_unified')} (location_key)",
        f"CREATE INDEX IF NOT EXISTS entity_address_unified_archive_version_idx ON {_qt(schema, 'entity_address_unified')} (archive_identity_version)",
        f"CREATE INDEX IF NOT EXISTS entity_address_unified_precision_idx ON {_qt(schema, 'entity_address_unified')} (address_precision)",
        f"CREATE INDEX IF NOT EXISTS entity_address_unified_zip5_idx ON {_qt(schema, 'entity_address_unified')} (zip5)",
        f"CREATE INDEX IF NOT EXISTS entity_address_unified_state_city_norm_idx ON {_qt(schema, 'entity_address_unified')} (state_code, city_norm)",
        f"CREATE INDEX IF NOT EXISTS entity_address_unified_premise_key_idx ON {_qt(schema, 'entity_address_unified')} (premise_key) WHERE premise_key IS NOT NULL",
        f"CREATE INDEX IF NOT EXISTS entity_address_unified_ptg_plan_gin_idx ON {_qt(schema, 'entity_address_unified')} USING gin (ptg_plan_array)",
        f"CREATE INDEX IF NOT EXISTS entity_address_unified_ptg_source_gin_idx ON {_qt(schema, 'entity_address_unified')} USING gin (ptg_source_array)",
        f"CREATE INDEX IF NOT EXISTS ptg_address_source_snapshot_idx ON {_qt(schema, 'ptg_address')} (source_key, snapshot_id)",
        f"CREATE INDEX IF NOT EXISTS ptg_address_npi_idx ON {_qt(schema, 'ptg_address')} (npi)",
        f"CREATE INDEX IF NOT EXISTS ptg_address_zip5_idx ON {_qt(schema, 'ptg_address')} (zip5)",
        f"CREATE INDEX IF NOT EXISTS ptg_address_state_city_norm_idx ON {_qt(schema, 'ptg_address')} (state_code, city_norm)",
        f"CREATE INDEX IF NOT EXISTS ptg_address_address_key_idx ON {_qt(schema, 'ptg_address')} (address_key) WHERE address_key IS NOT NULL",
        f"CREATE INDEX IF NOT EXISTS entity_address_evidence_npi_idx ON {_qt(schema, 'entity_address_evidence')} (npi)",
        f"CREATE INDEX IF NOT EXISTS entity_address_evidence_location_idx ON {_qt(schema, 'entity_address_evidence')} (location_key)",
        f"CREATE INDEX IF NOT EXISTS entity_address_plan_bridge_plan_idx ON {_qt(schema, 'entity_address_plan_bridge')} (plan_id, location_key)",
        f"CREATE INDEX IF NOT EXISTS entity_address_network_bridge_network_idx ON {_qt(schema, 'entity_address_network_bridge')} (network_id, location_key)",
        f"CREATE INDEX IF NOT EXISTS entity_address_ptg_bridge_plan_idx ON {_qt(schema, 'entity_address_ptg_bridge')} (ptg_plan_id, source_key, location_key)",
        f"CREATE INDEX IF NOT EXISTS entity_address_procedure_bridge_code_idx ON {_qt(schema, 'entity_address_procedure_bridge')} (code_system, code, location_key)",
        f"CREATE INDEX IF NOT EXISTS entity_address_medication_bridge_code_idx ON {_qt(schema, 'entity_address_medication_bridge')} (code_system, code, location_key)",
    )
    for statement in statements:
        if "entity_address_unified" in statement and not _table_exists(bind, schema, "entity_address_unified"):
            continue
        bind.exec_driver_sql(statement + ";")


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    bind.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)};")
    _entity_address_unified_columns(bind, schema)
    _create_tables(bind, schema)
    _create_indexes(bind, schema)


def downgrade():
    return None
