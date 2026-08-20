"""Preserve JSON number spelling in ordinary terminal document digests.

Revision ID: 20260820030000_ptg_ordinary_terminal_json_canonical_digest
Revises: 20260820020000_ptg_ordinary_terminal_json_null_guard

The five durable engine documents use PostgreSQL ``json`` columns.  Hashing
them through ``jsonb`` rewrites exponent-valued numbers and can diverge from
Python's canonical receipt bytes.  Add a ``json`` canonicalizer that retains
scalar token spelling while still sorting objects and patch only those five
full-document digest inputs.
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260820030000_ptg_ordinary_terminal_json_canonical_digest"
down_revision = "20260820020000_ptg_ordinary_terminal_json_null_guard"
branch_labels = None
depends_on = None

_CANONICAL_FUNCTION = "ptg_wave_canonical_json_ascii_v1"
_ASCII_FUNCTION = "ptg_wave_json_ascii_text_v1"
_GUARD_FUNCTION = "ptg_wave_ordinary_terminal_receipt_guard"
_DOCUMENTS = (
    ("run_params", "ordinary_run.params"),
    ("run_metrics", "ordinary_run.metrics"),
    ("engine_options", "durable_run.options"),
    ("engine_report", "durable_run.report"),
    ("snapshot_manifest", "durable_snapshot.manifest"),
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


def _qt(identifier: str) -> str:
    return f"{_q(_schema())}.{_q(identifier)}"


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _canonical_call(value: str) -> str:
    return f"{_qt(_CANONICAL_FUNCTION)}({value})"


def _canonical_json_function_sql() -> str:
    canonical = _qt(_CANONICAL_FUNCTION)
    ascii_text = _qt(_ASCII_FUNCTION)
    return f"""
    CREATE FUNCTION {canonical}(payload json)
    RETURNS text LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE STRICT
    SET search_path = pg_catalog, {_q(_schema())} AS $$
    DECLARE
        canonical_value text;
        has_duplicate_key boolean;
    BEGIN
        CASE json_typeof(payload)
            WHEN 'object' THEN
                SELECT '{{' || COALESCE(
                    string_agg(
                        {ascii_text}(to_jsonb(entry.key)::text)
                        || ':' || {canonical}(entry.value),
                        ',' ORDER BY entry.key COLLATE "C"
                    ),
                    ''
                ) || '}}',
                count(*) <> count(DISTINCT entry.key COLLATE "C")
                  INTO canonical_value, has_duplicate_key
                  FROM json_each(payload) AS entry;
                IF has_duplicate_key THEN
                    RAISE EXCEPTION 'PTG_WAVE_CANONICAL_JSON_DUPLICATE_KEY'
                        USING ERRCODE = 'P0001';
                END IF;
                RETURN canonical_value;
            WHEN 'array' THEN
                SELECT '[' || COALESCE(
                    string_agg(
                        {canonical}(entry.value),
                        ',' ORDER BY entry.ordinality
                    ),
                    ''
                ) || ']'
                  INTO canonical_value
                  FROM json_array_elements(payload) WITH ORDINALITY
                       AS entry(value, ordinality);
                RETURN canonical_value;
            WHEN 'string' THEN
                RETURN {canonical}(payload::jsonb);
            ELSE
                RETURN btrim(payload::text, E' \\t\\n\\r');
        END CASE;
    END;
    $$
    """


def _replacement_sql(*, upgrade: bool) -> str:
    signature = f"{_qt(_GUARD_FUNCTION)}()"
    replacements = _DOCUMENTS if upgrade else tuple(
        (new, old) for old, new in _DOCUMENTS
    )
    checks: list[str] = []
    for old_value, new_value in replacements:
        old_fragment = _canonical_call(old_value)
        new_fragment = _canonical_call(new_value)
        checks.append(
            f"""
            old_fragment := {_literal(old_fragment)};
            new_fragment := {_literal(new_fragment)};
            IF pg_catalog.length(definition)
                    - pg_catalog.length(pg_catalog.replace(
                        definition, old_fragment, ''
                    ))
                    <> pg_catalog.length(old_fragment)
               OR pg_catalog.strpos(definition, new_fragment) <> 0 THEN
                RAISE EXCEPTION
                    'PTG_ORDINARY_TERMINAL_JSON_DIGEST_PATCH_PRECONDITION_FAILED'
                    USING ERRCODE = 'P0001';
            END IF;
            definition := pg_catalog.replace(
                definition, old_fragment, new_fragment
            );
            """
        )
    return f"""
    DO $migration$
    DECLARE
        definition text;
        old_fragment text;
        new_fragment text;
    BEGIN
        SELECT pg_catalog.pg_get_functiondef(
            pg_catalog.to_regprocedure({_literal(signature)})
        ) INTO definition;
        IF definition IS NULL THEN
            RAISE EXCEPTION
                'PTG_ORDINARY_TERMINAL_JSON_DIGEST_PATCH_PRECONDITION_FAILED'
                USING ERRCODE = 'P0001';
        END IF;
        {''.join(checks)}
        EXECUTE definition;
    END;
    $migration$
    """


def upgrade() -> None:
    """Hash the exact durable JSON documents without normalizing numbers."""

    op.execute(_canonical_json_function_sql())
    op.execute(_replacement_sql(upgrade=True))


def downgrade() -> None:
    """Restore jsonb digest inputs and remove only the json overload."""

    op.execute(_replacement_sql(upgrade=False))
    op.execute(f"DROP FUNCTION {_qt(_CANONICAL_FUNCTION)}(json)")
