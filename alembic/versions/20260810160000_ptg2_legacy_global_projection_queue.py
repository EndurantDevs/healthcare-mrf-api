"""Persist source-local requests for the legacy global pointer projection.

Revision ID: 20260810160000_ptg2_legacy_global_projection_queue
Revises: 20260810150000_ptg2_artifact_blob_chunks
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260810160000_ptg2_legacy_global_projection_queue"
down_revision = "20260810150000_ptg2_artifact_blob_chunks"
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
    table = "ptg2_legacy_global_pointer_projection_queue"
    op.execute(
        f"""
        CREATE TABLE {_qt(schema, table)} (
            source_key varchar(96) NOT NULL,
            requested_generation bigint NOT NULL DEFAULT 1,
            applied_generation bigint NOT NULL DEFAULT 0,
            available_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            lease_token varchar(64),
            lease_until timestamptz,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q(f"{table}_pkey")} PRIMARY KEY (source_key),
            CONSTRAINT {_q(f"{table}_generation_check")}
                CHECK (requested_generation > 0
                       AND applied_generation >= 0
                       AND applied_generation <= requested_generation),
            CONSTRAINT {_q(f"{table}_lease_shape_check")}
                CHECK ((lease_token IS NULL) = (lease_until IS NULL))
        )
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q(f"{table}_ready_idx")}
            ON {_qt(schema, table)} (available_at, updated_at, source_key)
         WHERE applied_generation < requested_generation
           AND lease_token IS NULL
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q(f"{table}_lease_idx")}
            ON {_qt(schema, table)} (lease_until, source_key)
         WHERE lease_token IS NOT NULL
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('ptg2_current_plan_source_source_snapshot_idx')}
            ON {_qt(schema, 'ptg2_current_plan_source')}
               (source_key, snapshot_id)
        """
    )


def downgrade() -> None:
    schema = _schema()
    table = "ptg2_legacy_global_pointer_projection_queue"
    op.execute(
        f"""
        DO $$
        BEGIN
            IF to_regclass('{_qt(schema, table)}') IS NOT NULL
               AND EXISTS (SELECT 1 FROM {_qt(schema, table)} LIMIT 1)
            THEN
                RAISE EXCEPTION
                    'refusing to downgrade pending legacy global projection work';
            END IF;
        END
        $$
        """
    )
    op.execute(
        f"DROP INDEX IF EXISTS "
        f"{_qt(schema, 'ptg2_current_plan_source_source_snapshot_idx')}"
    )
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, table)}")
