"""Add Provider Directory address overlay table.

Revision ID: 20260701090000_provider_directory_address_overlay
Revises: 20260630134000_fix_service_phone_digits_index
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260701090000_provider_directory_address_overlay"
down_revision = "20260630134000_fix_service_phone_digits_index"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade():
    schema = _schema()
    table = _qt(schema, "provider_directory_address_overlay")
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            source_record_id varchar PRIMARY KEY,
            source_id varchar(64) NOT NULL,
            last_seen_run_id varchar(64),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            npi bigint NOT NULL,
            address_key uuid NOT NULL,
            first_line varchar,
            second_line varchar,
            city_name varchar,
            state_name varchar,
            state_code varchar(2),
            postal_code varchar,
            country_code varchar,
            telephone_number varchar,
            fax_number varchar,
            phone_number varchar(15),
            fax_number_digits varchar(15),
            address_precision varchar(32) NOT NULL DEFAULT 'street',
            source_updated_at timestamp,
            published_at timestamp NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS provider_directory_address_overlay_source_record_idx "
        f"ON {table} (source_record_id);"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS provider_directory_address_overlay_npi_idx "
        f"ON {table} (npi);"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS provider_directory_address_overlay_address_key_idx "
        f"ON {table} (address_key);"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS provider_directory_address_overlay_source_idx "
        f"ON {table} (source_id);"
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS provider_directory_address_overlay_phone_idx
            ON {table} (phone_number, npi)
         WHERE phone_number IS NOT NULL AND phone_number <> '';
        """
    )


def downgrade():
    schema = _schema()
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'provider_directory_address_overlay')};")
