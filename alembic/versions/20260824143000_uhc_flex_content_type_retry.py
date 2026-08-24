# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retry transient Flex response media-type failures.

Revision ID: 20260824143000_uhc_flex_content_type_retry
Revises: 20260821143000_ptg_legacy_plan_identifier_width
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260824143000_uhc_flex_content_type_retry"
down_revision = "20260821143000_ptg_legacy_plan_identifier_width"
branch_labels = None
depends_on = None

_FUNCTION = "guard_pd_uhc_flex_practitioner_work"
_WORK_TABLE = "provider_directory_uhc_flex_practitioner_work"
_OLD_PREDICATE = """(OLD.status = 'pending' OR (
                    OLD.status = 'leased'
                    AND OLD.lease_expires_at <= clock_timestamp()
                ))"""
_NEW_PREDICATE = """(OLD.status = 'pending' OR (
                    OLD.status = 'leased'
                    AND OLD.lease_expires_at <= clock_timestamp()
                ) OR (
                    OLD.status = 'error'
                    AND OLD.error_code = 'content_type_invalid'
                    AND OLD.attempt_count = 1
                ))"""


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


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _replacement_sql(old_predicate: str, new_predicate: str) -> str:
    signature = f"{_q(_schema())}.{_q(_FUNCTION)}()"
    return f"""
    DO $migration$
    DECLARE
        definition text;
        old_fragment constant text := {_literal(old_predicate)};
        new_fragment constant text := {_literal(new_predicate)};
    BEGIN
        SELECT pg_catalog.pg_get_functiondef(
            pg_catalog.to_regprocedure({_literal(signature)})
        ) INTO definition;
        IF definition IS NULL
           OR pg_catalog.length(definition)
                - pg_catalog.length(pg_catalog.replace(
                    definition, old_fragment, ''
                ))
                <> pg_catalog.length(old_fragment)
           OR pg_catalog.strpos(definition, new_fragment) <> 0 THEN
            RAISE EXCEPTION
                'UHC_FLEX_CONTENT_TYPE_RETRY_PATCH_PRECONDITION_FAILED'
                USING ERRCODE = 'P0001';
        END IF;
        EXECUTE pg_catalog.replace(definition, old_fragment, new_fragment);
    END;
    $migration$
    """


def _lock_work_table() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        f"LOCK TABLE {_q(_schema())}.{_q(_WORK_TABLE)} "
        "IN SHARE ROW EXCLUSIVE MODE;"
    )


def upgrade() -> None:
    """Allow only legacy content-type errors to re-enter exact claiming."""

    _lock_work_table()
    op.execute(_replacement_sql(_OLD_PREDICATE, _NEW_PREDICATE))


def downgrade() -> None:
    """Restore terminal treatment for stored content-type errors."""

    _lock_work_table()
    op.execute(_replacement_sql(_NEW_PREDICATE, _OLD_PREDICATE))
