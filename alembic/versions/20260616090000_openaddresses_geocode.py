"""Add OpenAddresses geocode cache.

Revision ID: 20260616090000
Revises: 20260615190000_facility_anchor_source_npi
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260616090000_openaddresses_geocode"
down_revision = "20260615190000_facility_anchor_source_npi"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _table_exists(bind, schema: str, table: str) -> bool:
    return bool(
        bind.execute(
            text("SELECT to_regclass(:name)"),
            {"name": f"{schema}.{table}"},
        ).scalar()
    )


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    table = _qt(schema, "openaddresses_geocode")
    archive = _qt(schema, "address_archive_v2")
    enum_type = f"{_q(schema)}.{_q('address_archive_geo_source')}"

    bind.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    bind.exec_driver_sql(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM pg_type t
                  JOIN pg_namespace n ON n.oid = t.typnamespace
                 WHERE n.nspname = {schema!r}
                   AND t.typname = 'address_archive_geo_source'
            ) THEN
                ALTER TYPE {enum_type} ADD VALUE IF NOT EXISTS 'openaddresses';
            END IF;
        END $$;
        """
    )

    bind.exec_driver_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            row_hash varchar(64) PRIMARY KEY,
            address_key uuid,
            identity_key text,
            house_number text NOT NULL,
            street_match_key text NOT NULL,
            street_name text,
            unit text,
            city_name text,
            state_code varchar(2) NOT NULL,
            zip5 varchar(5) NOT NULL,
            formatted_address text,
            lat numeric(11,8) NOT NULL,
            long numeric(11,8) NOT NULL,
            source text,
            data_id integer,
            job_id integer,
            feature_id text,
            accuracy text,
            source_updated timestamptz,
            imported_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )
    bind.exec_driver_sql(
        f"""
        CREATE INDEX IF NOT EXISTS openaddresses_geocode_exact_idx
            ON {table} (state_code, zip5, house_number, street_match_key);
        """
    )
    bind.exec_driver_sql(
        f"""
        CREATE INDEX IF NOT EXISTS openaddresses_geocode_address_key_idx
            ON {table} (address_key)
            WHERE address_key IS NOT NULL;
        """
    )
    bind.exec_driver_sql(
        f"""
        CREATE INDEX IF NOT EXISTS openaddresses_geocode_source_idx
            ON {table} (source);
        """
    )
    if _table_exists(bind, schema, "address_archive_v2"):
        bind.exec_driver_sql(
            f"""
            CREATE INDEX IF NOT EXISTS address_archive_v2_missing_geo_shard_idx
                ON {archive} (state_code, zip5)
                WHERE lat IS NULL
                  AND long IS NULL
                  AND COALESCE(country_code, 'US') = 'US'
                  AND precision = 'street'
                  AND state_code IS NOT NULL
                  AND zip5 IS NOT NULL;
            """
        )


def downgrade():
    bind = op.get_bind()
    schema = _schema()
    bind.exec_driver_sql(f"DROP INDEX IF EXISTS {_q(schema)}.{_q('address_archive_v2_missing_geo_shard_idx')};")
    if _table_exists(bind, schema, "openaddresses_geocode"):
        bind.exec_driver_sql(f"DROP TABLE {_qt(schema, 'openaddresses_geocode')};")
    # PostgreSQL enum values are intentionally not removed on downgrade.
