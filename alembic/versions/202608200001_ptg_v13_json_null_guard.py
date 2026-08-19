"""Accept ORM-equivalent JSON null in the V13 pristine-run guard.

Revision ID: 202608200001_ptg_v13_json_null_guard
Revises: 202608170001_ptg_v13_post_ready_failure_guard

``ImportRun.error`` is a SQLAlchemy JSON column. A Python ``None`` may be
stored as JSON ``null`` rather than SQL NULL, and both forms deserialize to
the same pristine in-memory value. Patch the installed V13 guard without
weakening rejection of non-null errors.
"""

from __future__ import annotations

import os

from alembic import op


revision = "202608200001_ptg_v13_json_null_guard"
down_revision = "202608170001_ptg_v13_post_ready_failure_guard"
branch_labels = None
depends_on = None

_FUNCTION = "ptg_import_wave_v13_abandonment_guard"
_OLD_PREDICATE = "AND admitted.error IS NULL"
_NEW_PREDICATE = (
    "AND (\n"
    "                        admitted.error IS NULL\n"
    "                        OR admitted.error::jsonb = 'null'::jsonb\n"
    "                  )"
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
                'PTG_IMPORT_WAVE_V13_JSON_NULL_PATCH_PRECONDITION_FAILED'
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
