"""Allow one strict V3 physical snapshot to serve multiple logical plans.

Revision ID: 20260716130000_ptg2_v3_multi_plan_scope
Revises: 20260716120000_ptg2_allowed_amounts_v3
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260716130000_ptg2_v3_multi_plan_scope"
down_revision = "20260716120000_ptg2_allowed_amounts_v3"
branch_labels = None
depends_on = None


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table_name: str) -> str:
    return f"{_q(schema)}.{_q(table_name)}"


def upgrade() -> None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    table = _qt(schema, "ptg2_v3_snapshot_plan_scope")
    op.execute(
        f"""
        CREATE TABLE {table} (
            snapshot_id varchar(96) NOT NULL,
            plan_id varchar(64) NOT NULL,
            plan_market_type varchar(32) NOT NULL DEFAULT '',
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q("ptg2_v3_snapshot_plan_scope_pkey")}
                PRIMARY KEY (snapshot_id, plan_id, plan_market_type),
            CONSTRAINT {_q("ptg2_v3_snapshot_plan_scope_snapshot_id_fkey")}
                FOREIGN KEY (snapshot_id)
                REFERENCES {_qt(schema, "ptg2_v3_snapshot_scope")} (snapshot_id)
                ON DELETE CASCADE
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_snapshot_plan_scope_lookup_idx")}
            ON {table} (plan_id, plan_market_type, snapshot_id);
        """
    )
    op.execute(
        f"""
        INSERT INTO {table}
            (snapshot_id, plan_id, plan_market_type)
        SELECT snapshot_id, plan_id, plan_market_type
          FROM {_qt(schema, "ptg2_v3_snapshot_scope")}
        ON CONFLICT DO NOTHING;
        """
    )


def downgrade() -> None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    op.execute(
        f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_snapshot_plan_scope')};"
    )
