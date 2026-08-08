"""Add the current-alias medication serving index.

Revision ID: 20260808160000_fhir_formulary_serving_index
Revises: 20260808150000_ptg_import_wave_admission_rollback
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260808160000_fhir_formulary_serving_index"
down_revision = "20260808150000_ptg_import_wave_admission_rollback"
branch_labels = None
depends_on = None

_INDEX_NAME = "fhir_formulary_membership_medication_version_idx"
_TABLE_NAME = "fhir_formulary_alias_membership"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def upgrade() -> None:
    """Install the keyset index before full formulary publication."""

    schema = _quote(_schema())
    op.execute(
        f"CREATE INDEX {_quote(_INDEX_NAME)} "
        f"ON {schema}.{_quote(_TABLE_NAME)} "
        "(alias_version_id, medication_version_id);"
    )


def downgrade() -> None:
    """Remove only the serving index."""

    schema = _quote(_schema())
    op.execute(f"DROP INDEX IF EXISTS {schema}.{_quote(_INDEX_NAME)};")
