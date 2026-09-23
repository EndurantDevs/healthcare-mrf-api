# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind idempotent custom-import executions to an optional request identity.

Revision ID: 20260922010000_custom_import_execution_request_identity
Revises: 20260923021000_ms_drg_result_generation
"""

from __future__ import annotations

import os

from alembic import op

revision = "20260922010000_custom_import_execution_request_identity"
down_revision = "20260923021000_ms_drg_result_generation"
branch_labels = None
depends_on = None


_EXECUTION_TABLE = "custom_import_execution"
_REQUEST_IDENTITY_COLUMN = "request_identity_sha256"
_REQUEST_IDENTITY_CHECK = "custom_import_execution_request_identity_shape_check"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qualified(schema: str, object_name: str) -> str:
    return f"{_quote(schema)}.{_quote(object_name)}"


def _add_request_identity_sql(schema: str) -> str:
    execution = _qualified(schema, _EXECUTION_TABLE)
    return f"""
    ALTER TABLE {execution}
        ADD COLUMN {_quote(_REQUEST_IDENTITY_COLUMN)} BYTEA,
        ADD CONSTRAINT {_quote(_REQUEST_IDENTITY_CHECK)} CHECK (
            {_quote(_REQUEST_IDENTITY_COLUMN)} IS NULL
            OR octet_length({_quote(_REQUEST_IDENTITY_COLUMN)}) = 32
        )
    """


def _downgrade_guard_sql(schema: str) -> str:
    execution = _qualified(schema, _EXECUTION_TABLE)
    return f"""
    DO $block$
    BEGIN
        LOCK TABLE {execution} IN ACCESS EXCLUSIVE MODE;
        IF EXISTS (
            SELECT 1
              FROM {execution}
             WHERE {_quote(_REQUEST_IDENTITY_COLUMN)} IS NOT NULL
        ) THEN
            RAISE EXCEPTION 'custom_import_execution_request_identity_downgrade_blocked'
                USING ERRCODE = 'P0001';
        END IF;
    END;
    $block$
    """


def _drop_request_identity_sql(schema: str) -> str:
    execution = _qualified(schema, _EXECUTION_TABLE)
    return f"""
    ALTER TABLE {execution}
        DROP CONSTRAINT IF EXISTS {_quote(_REQUEST_IDENTITY_CHECK)},
        DROP COLUMN IF EXISTS {_quote(_REQUEST_IDENTITY_COLUMN)}
    """


def upgrade() -> None:
    """Add nullable fixed-width identity evidence without changing existing rows."""

    op.execute(_add_request_identity_sql(_schema()))


def downgrade() -> None:
    """Refuse to discard any retained identity evidence."""

    schema = _schema()
    op.execute(_downgrade_guard_sql(schema))
    op.execute(_drop_request_identity_sql(schema))
