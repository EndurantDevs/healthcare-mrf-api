"""Widen Provider Directory source data quality cadence.

Revision ID: 20260701120000_provider_directory_source_checked_text
Revises: 20260701110000_provider_directory_network_catalog
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260701120000_provider_directory_source_checked_text"
down_revision = "20260701110000_provider_directory_network_catalog"
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
    op.execute(
        f"""
        ALTER TABLE IF EXISTS {_qt(schema, "provider_directory_source")}
            ALTER COLUMN data_quality_checked TYPE text;
        """
    )


def downgrade():
    schema = _schema()
    op.execute(
        f"""
        ALTER TABLE IF EXISTS {_qt(schema, "provider_directory_source")}
            ALTER COLUMN data_quality_checked TYPE varchar(64);
        """
    )
