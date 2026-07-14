"""Align strict PTG V3 schema metadata used by lifecycle cleanup.

Revision ID: 20260714120000_ptg2_v3_schema_gc_consistency
Revises: 20260712120000_ptg2_v3_shared_schema
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260714120000_ptg2_v3_schema_gc_consistency"
down_revision = "20260712120000_ptg2_v3_shared_schema"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade() -> None:
    schema = _schema()
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS
            {_q("ptg2_v3_candidate_audit_attestation_snapshot_key_idx")}
        ON {_qt(schema, "ptg2_v3_candidate_audit_attestation")} (snapshot_key);
        """
    )


def downgrade() -> None:
    schema = _schema()
    op.execute(
        f"DROP INDEX IF EXISTS "
        f"{_qt(schema, 'ptg2_v3_candidate_audit_attestation_snapshot_key_idx')};"
    )
