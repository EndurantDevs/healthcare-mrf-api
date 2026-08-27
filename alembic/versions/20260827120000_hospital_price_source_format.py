# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonicalize hospital-price CSV source formats.

Revision ID: 20260827120000_hospital_price_source_format
Revises: 20260826200000_hospital_price_selector_range_index
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260827120000_hospital_price_source_format"
down_revision = "20260826200000_hospital_price_selector_range_index"
branch_labels = None
depends_on = None


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _drop_shape_check(table: str) -> None:
    op.execute(
        f"ALTER TABLE {table} "
        "DROP CONSTRAINT hospital_price_version_shape_check;"
    )


def _add_shape_check(table: str, *, source_formats: str) -> None:
    op.execute(
        f"ALTER TABLE {table} "
        "ADD CONSTRAINT hospital_price_version_shape_check CHECK ("
        "version_id ~ '^[0-9a-f]{64}$' "
        "AND parser_contract_sha256 ~ '^[0-9a-f]{64}$' "
        "AND semantic_sha256 ~ '^[0-9a-f]{64}$' "
        f"AND source_format IN ({source_formats}) "
        "AND location_count > 0 AND npi_count > 0 AND license_count > 0 "
        "AND service_count > 0 AND charge_count > 0 "
        "AND payer_charge_count >= 0);"
    )


def upgrade() -> None:
    """Rewrite legacy metadata and enforce parser-canonical values."""

    table = f'{_q(_schema())}."hospital_price_version"'
    _drop_shape_check(table)
    op.execute(
        f"UPDATE {table} SET source_format = CASE source_format "
        "WHEN 'csv_tall' THEN 'csv-tall' "
        "WHEN 'csv_wide' THEN 'csv-wide' ELSE source_format END "
        "WHERE source_format IN ('csv_tall', 'csv_wide');"
    )
    _add_shape_check(
        table, source_formats="'json', 'csv-tall', 'csv-wide'"
    )


def downgrade() -> None:
    """Keep the canonical repair required by the predecessor application."""

    # The predecessor runtime already emits and serves the canonical spellings.
    # Reintroducing the deployed constraint defect would break its CSV imports.
    return None
