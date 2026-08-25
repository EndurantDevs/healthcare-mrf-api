# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add transitional geo-assurance projection columns.

Revision ID: 20260825090000_geo_assurance_projection
Revises: 20260825120000_hospital_price_storage
"""

from __future__ import annotations

import os
import re

from alembic import op


revision = "20260825090000_geo_assurance_projection"
down_revision = "20260825120000_hospital_price_storage"
branch_labels = None
depends_on = None

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_TABLES = ("entity_address_unified", "entity_address_unified_old")
_STATE_TABLE = "entity_address_geo_assurance_state"
_COLUMNS = (
    ("geo_evidence_source_id", "smallint"),
    ("geo_identity_coherent", "boolean"),
    ("geo_point_coherent", "boolean"),
    ("geo_assurance_version", "smallint"),
)


def _schema() -> str:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    if not _IDENTIFIER.fullmatch(schema):
        raise RuntimeError("database schema must be a simple PostgreSQL identifier")
    return schema


def upgrade() -> None:
    schema = _schema()
    column_sql = ", ".join(
        f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        for column_name, column_type in _COLUMNS
    )
    for table_name in _TABLES:
        op.execute(
            f'ALTER TABLE IF EXISTS "{schema}"."{table_name}" {column_sql}'
        )
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{_STATE_TABLE}" (
            singleton boolean PRIMARY KEY DEFAULT true,
            active_geo_assurance_version smallint,
            active_table_oid oid,
            active_relation_signature jsonb,
            candidate_geo_assurance_version smallint,
            candidate_table_oid oid,
            candidate_relation_signature jsonb,
            candidate_projected_rows bigint,
            CONSTRAINT entity_address_geo_assurance_state_singleton_ck
                CHECK (singleton)
        )
        """
    )
    op.execute(
        f"""
        INSERT INTO "{schema}"."{_STATE_TABLE}" (singleton)
        VALUES (true)
        ON CONFLICT (singleton) DO NOTHING
        """
    )


def downgrade() -> None:
    schema = _schema()
    op.execute(f'DROP TABLE IF EXISTS "{schema}"."{_STATE_TABLE}"')
    column_sql = ", ".join(
        f"DROP COLUMN IF EXISTS {column_name}"
        for column_name, _column_type in reversed(_COLUMNS)
    )
    for table_name in reversed(_TABLES):
        op.execute(
            f'ALTER TABLE IF EXISTS "{schema}"."{table_name}" {column_sql}'
        )
