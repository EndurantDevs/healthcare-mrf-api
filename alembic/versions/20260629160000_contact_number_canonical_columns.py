"""Add canonical contact-number columns.

Revision ID: 20260629160000_contact_number_canonical_columns
Revises: 20260629123000_provider_directory_location_run_index
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260629160000_contact_number_canonical_columns"
down_revision = "20260629123000_provider_directory_location_run_index"
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


def _add_contact_columns(bind, schema: str, table: str) -> None:
    if not _table_exists(bind, schema, table):
        return
    bind.exec_driver_sql(
        f"""
        ALTER TABLE {_qt(schema, table)}
            ADD COLUMN IF NOT EXISTS phone_number varchar(15),
            ADD COLUMN IF NOT EXISTS phone_extension varchar(16),
            ADD COLUMN IF NOT EXISTS fax_number_digits varchar(15),
            ADD COLUMN IF NOT EXISTS fax_extension varchar(16);
        """
    )


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    for table in (
        "npi_address",
        "mrf_address",
        "mrf_address_evidence",
        "entity_address_unified",
        "ptg_address",
        "provider_directory_location",
    ):
        _add_contact_columns(bind, schema, table)

    with op.get_context().autocommit_block():
        if _table_exists(bind, schema, "npi_address"):
            bind.exec_driver_sql(
                f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS npi_address_idx_primary_phone_number_npi
                ON {_qt(schema, "npi_address")} (phone_number, npi)
                WHERE type='primary' AND phone_number IS NOT NULL AND phone_number <> '';
                """
            )
        if _table_exists(bind, schema, "mrf_address"):
            bind.exec_driver_sql(
                f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS mrf_address_idx_practice_phone_number_npi
                ON {_qt(schema, "mrf_address")} (phone_number, npi)
                WHERE type='practice' AND phone_number IS NOT NULL AND phone_number <> '';
                """
            )
        if _table_exists(bind, schema, "entity_address_unified"):
            bind.exec_driver_sql(
                f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS entity_address_unified_idx_primary_phone_number_npi
                ON {_qt(schema, "entity_address_unified")} (phone_number, npi)
                WHERE type='primary' AND phone_number IS NOT NULL AND phone_number <> '';
                """
            )
        if _table_exists(bind, schema, "provider_directory_location"):
            bind.exec_driver_sql(
                f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS provider_directory_location_phone_number_idx
                ON {_qt(schema, "provider_directory_location")} (phone_number)
                WHERE phone_number IS NOT NULL AND phone_number <> '';
                """
            )


def downgrade():
    bind = op.get_bind()
    schema = _schema()
    with op.get_context().autocommit_block():
        bind.exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.provider_directory_location_phone_number_idx;"
        )
        bind.exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.entity_address_unified_idx_primary_phone_number_npi;"
        )
        bind.exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.mrf_address_idx_practice_phone_number_npi;"
        )
        bind.exec_driver_sql(
            f"DROP INDEX CONCURRENTLY IF EXISTS {_q(schema)}.npi_address_idx_primary_phone_number_npi;"
        )
    for table in (
        "provider_directory_location",
        "ptg_address",
        "entity_address_unified",
        "mrf_address_evidence",
        "mrf_address",
        "npi_address",
    ):
        if not _table_exists(bind, schema, table):
            continue
        bind.exec_driver_sql(
            f"""
            ALTER TABLE {_qt(schema, table)}
                DROP COLUMN IF EXISTS fax_extension,
                DROP COLUMN IF EXISTS fax_number_digits,
                DROP COLUMN IF EXISTS phone_extension,
                DROP COLUMN IF EXISTS phone_number;
            """
        )
