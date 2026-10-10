# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Distinguish exhaustive materialization from bounded sampled verification."""

from __future__ import annotations

import os

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "20261010010000_custom_import_materialization_contract"
down_revision = "20261010000000_custom_import_admission_indexes"
branch_labels = None
depends_on = None

_TABLE = "custom_import_generation_seal"
_CHECK = "custom_import_generation_seal_materialization_check"
_LEGACY = "custom-import/materialization/v1"
_COMPACT = "custom-import/materialization/v2"


def _schema() -> str:
    """Use the same schema selection as the migration environment."""
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def upgrade() -> None:
    """Add metadata without rewriting immutable retained seal rows."""
    schema = _schema()
    op.add_column(
        _TABLE,
        sa.Column("materialization_contract", sa.String(63), nullable=False, server_default=sa.text(f"'{_LEGACY}'")),
        schema=schema,
    )
    op.add_column(_TABLE, sa.Column("verification_evidence", JSONB(none_as_null=True)), schema=schema)
    op.create_check_constraint(
        _CHECK,
        _TABLE,
        f"(materialization_contract = '{_LEGACY}' AND verification_evidence IS NULL) OR "
        f"(materialization_contract = '{_COMPACT}' AND verification_evidence IS NOT NULL "
        "AND jsonb_typeof(verification_evidence) = 'object')",
        schema=schema,
    )


def downgrade() -> None:
    """Refuse to erase the interpretation of retained compact proofs."""
    schema = _schema()
    quoted_schema = schema.replace('"', '""')
    op.execute(sa.text(f'LOCK TABLE "{quoted_schema}"."{_TABLE}" IN SHARE ROW EXCLUSIVE MODE'))
    table = sa.table(_TABLE, sa.column("materialization_contract"), schema=schema)
    if op.get_bind().scalar(sa.select(sa.exists().where(table.c.materialization_contract != _LEGACY))):
        raise RuntimeError("custom_import_compact_materialization_downgrade_blocked")
    op.drop_constraint(_CHECK, _TABLE, schema=schema, type_="check")
    op.drop_column(_TABLE, "verification_evidence", schema=schema)
    op.drop_column(_TABLE, "materialization_contract", schema=schema)
