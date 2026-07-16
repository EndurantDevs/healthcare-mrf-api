"""Remove the redundant wide strict V3 code identity index.

Revision ID: 20260716140000_ptg2_v3_code_identity_index
Revises: 20260716130000_ptg2_v3_multi_plan_scope
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260716140000_ptg2_v3_code_identity_index"
down_revision = "20260716130000_ptg2_v3_multi_plan_scope"
branch_labels = None
depends_on = None


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table_name: str) -> str:
    return f"{_q(schema)}.{_q(table_name)}"


def upgrade() -> None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    op.execute(
        f"""
        ALTER TABLE {_qt(schema, "ptg2_v3_code")}
        DROP CONSTRAINT IF EXISTS {_q("ptg2_v3_code_identity_key")};
        """
    )


def downgrade() -> None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    op.execute(
        f"""
        ALTER TABLE {_qt(schema, "ptg2_v3_code")}
        ADD CONSTRAINT {_q("ptg2_v3_code_identity_key")}
        UNIQUE NULLS NOT DISTINCT (
            snapshot_key,
            coverage_scope_id,
            reported_code_system,
            reported_code,
            negotiation_arrangement,
            billing_code_type_version,
            source_name,
            source_description
        );
        """
    )
