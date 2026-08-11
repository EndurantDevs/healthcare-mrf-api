"""Protect shared PTG CAS blocks without long-lived row locks.

Revision ID: 20260810130000_ptg2_block_build_pins
Revises: 20260810120000_ptg2_layout_build_candidates
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260810130000_ptg2_block_build_pins"
down_revision = "20260810120000_ptg2_layout_build_candidates"
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
    table = "ptg2_block_build_pin"
    op.execute(
        f"""
        CREATE TABLE {_qt(schema, table)} (
            snapshot_key bigint NOT NULL,
            build_token varchar(96) NOT NULL,
            pin_token varchar(96) NOT NULL,
            block_hash bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            heartbeat_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            lease_until timestamptz NOT NULL,
            CONSTRAINT {_q(f"{table}_pkey")}
                PRIMARY KEY (snapshot_key, pin_token, block_hash),
            CONSTRAINT {_q(f"{table}_snapshot_key_fkey")}
                FOREIGN KEY (snapshot_key)
                REFERENCES {_qt(schema, "ptg2_v3_snapshot_layout")} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q(f"{table}_hash_check")}
                CHECK (octet_length(block_hash) = 32)
        )
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q(f"{table}_active_hash_idx")}
            ON {_qt(schema, table)} (block_hash, lease_until)
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q(f"{table}_lease_idx")}
            ON {_qt(schema, table)} (lease_until, snapshot_key)
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q(f"{table}_token_lease_idx")}
            ON {_qt(schema, table)}
               (snapshot_key, build_token, pin_token, lease_until)
        """
    )


def downgrade() -> None:
    schema = _schema()
    table = "ptg2_block_build_pin"
    op.execute(
        f"""
        DO $$
        BEGIN
            IF to_regclass('{_qt(schema, table)}') IS NOT NULL
               AND EXISTS (SELECT 1 FROM {_qt(schema, table)} LIMIT 1)
            THEN
                RAISE EXCEPTION
                    'refusing to downgrade active PTG block build pins';
            END IF;
        END
        $$
        """
    )
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, table)}")
