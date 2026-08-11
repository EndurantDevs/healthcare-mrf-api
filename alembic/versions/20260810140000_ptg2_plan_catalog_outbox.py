"""Persist non-gating PTG plan catalog compatibility work.

Revision ID: 20260810140000_ptg2_plan_catalog_outbox
Revises: 20260810130000_ptg2_block_build_pins
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260810140000_ptg2_plan_catalog_outbox"
down_revision = "20260810130000_ptg2_block_build_pins"
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
    table = "ptg2_plan_catalog_outbox"
    op.execute(
        f"""
        CREATE TABLE {_qt(schema, table)} (
            request_id varchar(64) NOT NULL,
            snapshot_id varchar(96) NOT NULL,
            chunk_index integer NOT NULL,
            chunk_count integer NOT NULL,
            payload_sha256 varchar(64) NOT NULL,
            plan_rows jsonb NOT NULL,
            alias_rows jsonb NOT NULL,
            plan_count integer NOT NULL,
            alias_count integer NOT NULL,
            payload_bytes integer NOT NULL,
            attempt_count integer NOT NULL DEFAULT 0,
            available_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            lease_token varchar(64),
            lease_until timestamptz,
            terminal_error_code varchar(64),
            terminal_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q(f"{table}_pkey")} PRIMARY KEY (request_id),
            CONSTRAINT {_q(f"{table}_snapshot_chunk_key")}
                UNIQUE (snapshot_id, chunk_index),
            CONSTRAINT {_q(f"{table}_payload_sha256_check")}
                CHECK (payload_sha256 ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT {_q(f"{table}_plan_rows_check")}
                CHECK (jsonb_typeof(plan_rows) = 'array'),
            CONSTRAINT {_q(f"{table}_alias_rows_check")}
                CHECK (jsonb_typeof(alias_rows) = 'array'),
            CONSTRAINT {_q(f"{table}_attempt_count_check")}
                CHECK (attempt_count >= 0),
            CONSTRAINT {_q(f"{table}_chunk_shape_check")}
                CHECK (chunk_index >= 0 AND chunk_index < chunk_count),
            CONSTRAINT {_q(f"{table}_plan_count_check")}
                CHECK (plan_count BETWEEN 0 AND 16),
            CONSTRAINT {_q(f"{table}_alias_count_check")}
                CHECK (alias_count BETWEEN 0 AND 128),
            CONSTRAINT {_q(f"{table}_payload_bytes_check")}
                CHECK (payload_bytes BETWEEN 1 AND 524288),
            CONSTRAINT {_q(f"{table}_nonempty_check")}
                CHECK (plan_count > 0 OR alias_count > 0),
            CONSTRAINT {_q(f"{table}_lease_shape_check")}
                CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
            CONSTRAINT {_q(f"{table}_terminal_shape_check")}
                CHECK ((terminal_error_code IS NULL) = (terminal_at IS NULL))
        )
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q(f"{table}_ready_idx")}
            ON {_qt(schema, table)} (available_at, created_at, request_id)
            WHERE lease_token IS NULL AND terminal_at IS NULL
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q(f"{table}_lease_idx")}
            ON {_qt(schema, table)} (lease_until, request_id)
            WHERE lease_token IS NOT NULL
        """
    )


def downgrade() -> None:
    schema = _schema()
    table = "ptg2_plan_catalog_outbox"
    op.execute(
        f"""
        DO $$
        BEGIN
            IF to_regclass('{_qt(schema, table)}') IS NOT NULL
               AND EXISTS (SELECT 1 FROM {_qt(schema, table)} LIMIT 1)
            THEN
                RAISE EXCEPTION
                    'refusing to downgrade pending, leased, or poisoned '
                    'PTG plan catalog outbox work';
            END IF;
        END
        $$
        """
    )
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, table)}")
