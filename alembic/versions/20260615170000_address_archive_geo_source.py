"""Add typed geocode source to address archive v2.

Revision ID: 20260615170000
Revises: 20260614020000_ptg_address_source_snapshot_key
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260615170000_address_archive_geo_source"
down_revision = "20260614020000_ptg_address_source_snapshot_key"
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


def _column_exists(bind, schema: str, table: str, column: str) -> bool:
    return bool(
        bind.execute(
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
    )


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    enum_type = f"{_q(schema)}.{_q('address_archive_geo_source')}"
    archive = _qt(schema, "address_archive_v2")

    bind.exec_driver_sql(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                  FROM pg_type t
                  JOIN pg_namespace n ON n.oid = t.typnamespace
                 WHERE n.nspname = {schema!r}
                   AND t.typname = 'address_archive_geo_source'
            ) THEN
                CREATE TYPE {enum_type} AS ENUM ('mapbox', 'google', 'tiger', 'manual');
            END IF;
        END $$;
        """
    )

    if not _table_exists(bind, schema, "address_archive_v2"):
        return

    if not _column_exists(bind, schema, "address_archive_v2", "geo_source"):
        bind.exec_driver_sql(
            f"ALTER TABLE {archive} ADD COLUMN geo_source {enum_type};"
        )

    bind.exec_driver_sql(
        f"""
        UPDATE {archive}
           SET geo_source = CASE
               WHEN NULLIF(place_id, '') IS NOT NULL THEN 'google'::{enum_type}
               WHEN lower(NULLIF(geocode_source, '')) = 'google' THEN 'google'::{enum_type}
               WHEN lower(NULLIF(geocode_source, '')) = 'mapbox' THEN 'mapbox'::{enum_type}
               WHEN lower(NULLIF(geocode_source, '')) = 'tiger' THEN 'tiger'::{enum_type}
               WHEN lower(NULLIF(geocode_source, '')) IN ('manual', 'facility_anchor') THEN 'manual'::{enum_type}
               ELSE geo_source
           END
         WHERE geo_source IS NULL
           AND lat IS NOT NULL
           AND long IS NOT NULL
           AND (
                NULLIF(place_id, '') IS NOT NULL
                OR lower(NULLIF(geocode_source, '')) IN (
                    'google', 'mapbox', 'tiger', 'manual', 'facility_anchor'
                )
           );
        """
    )


def downgrade():
    bind = op.get_bind()
    schema = _schema()

    if _table_exists(bind, schema, "address_archive_v2") and _column_exists(
        bind, schema, "address_archive_v2", "geo_source"
    ):
        bind.exec_driver_sql(
            f"ALTER TABLE {_qt(schema, 'address_archive_v2')} DROP COLUMN geo_source;"
        )

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
                DROP TYPE {_q(schema)}.{_q('address_archive_geo_source')};
            END IF;
        END $$;
        """
    )
