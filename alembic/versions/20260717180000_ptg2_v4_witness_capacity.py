"""Raise strict PTG V3 source-witness capacity for the V4 payload.

Revision ID: 20260717180000_ptg2_v4_witness_capacity
Revises: 20260716150000_npi_provider_sex_lookup_index
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260717180000_ptg2_v4_witness_capacity"
down_revision = "20260716150000_npi_provider_sex_lookup_index"
branch_labels = None
depends_on = None


_TABLE = "ptg2_v3_source_audit_witness"
_OCCURRENCE_CONSTRAINT = "ptg2_v3_source_audit_witness_occurrence_count_check"
_PROVIDER_CONSTRAINT = "ptg2_v3_source_audit_witness_provider_count_check"
_TOTAL_CONSTRAINT = "ptg2_v3_source_audit_witness_total_count_check"


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


def _replace_constraints(*, occurrence: int, provider: int, total: int) -> None:
    table = _qt(_schema(), _TABLE)
    op.execute(
        f"""
        ALTER TABLE {table}
            DROP CONSTRAINT IF EXISTS {_q(_OCCURRENCE_CONSTRAINT)},
            DROP CONSTRAINT IF EXISTS {_q(_PROVIDER_CONSTRAINT)},
            DROP CONSTRAINT IF EXISTS {_q(_TOTAL_CONSTRAINT)},
            ADD CONSTRAINT {_q(_OCCURRENCE_CONSTRAINT)}
                CHECK (occurrence_witness_count BETWEEN 1 AND {occurrence}),
            ADD CONSTRAINT {_q(_PROVIDER_CONSTRAINT)}
                CHECK (provider_witness_count BETWEEN 0 AND {provider}),
            ADD CONSTRAINT {_q(_TOTAL_CONSTRAINT)}
                CHECK (
                    occurrence_witness_count + provider_witness_count <= {total}
                );
        """
    )


def upgrade() -> None:
    _replace_constraints(occurrence=10_000, provider=1_000, total=11_000)


def downgrade() -> None:
    _replace_constraints(occurrence=2_048, provider=2_048, total=2_048)
