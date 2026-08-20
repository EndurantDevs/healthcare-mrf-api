"""Accept ORM-equivalent JSON null in ordinary terminal runs.

Revision ID: 20260820020000_ptg_ordinary_terminal_json_null_guard
Revises: 20260820010000_prescription_autocomplete_trigram_index

``ImportRun.error`` is a SQLAlchemy JSON column. A Python ``None`` may be
stored as JSON ``null`` rather than SQL NULL. Patch the installed terminal
receipt guard while preserving rejection of non-null errors.
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260820020000_ptg_ordinary_terminal_json_null_guard"
down_revision = "20260820010000_prescription_autocomplete_trigram_index"
branch_labels = None
depends_on = None

_FUNCTION = "ptg_wave_ordinary_terminal_receipt_guard"
_OLD_PREDICATE = "OR ordinary_run.error IS NOT NULL"
_NEW_PREDICATE = (
    "OR (ordinary_run.error IS NOT NULL "
    "AND ordinary_run.error::jsonb IS DISTINCT FROM 'null'::jsonb)"
)


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
                'PTG_ORDINARY_TERMINAL_JSON_NULL_PATCH_PRECONDITION_FAILED'
                USING ERRCODE = 'P0001';
        END IF;
        EXECUTE pg_catalog.replace(definition, old_fragment, new_fragment);
    END;
    $migration$
    """


def upgrade() -> None:
    """Treat SQL NULL and JSON null as the same pristine error value."""

    op.execute(_replacement_sql(_OLD_PREDICATE, _NEW_PREDICATE))


def downgrade() -> None:
    """Restore the prior predicate only when the patched body is exact."""

    op.execute(_replacement_sql(_NEW_PREDICATE, _OLD_PREDICATE))
