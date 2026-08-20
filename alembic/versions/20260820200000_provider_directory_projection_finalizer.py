"""Enable guarded native projection finalization.

Revision ID: 20260820200000_provider_directory_projection_finalizer
Revises: 20260820140000_prescription_autocomplete_rollup
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260820200000_provider_directory_projection_finalizer"
down_revision = "20260820140000_prescription_autocomplete_rollup"
branch_labels = None
depends_on = None


_INSERT_BLOCKER = """            IF TG_OP = 'INSERT' THEN
                RAISE EXCEPTION
                    'provider_directory_projection_native_attestation_required'
                    USING ERRCODE = '55000';
            END IF;
"""
_RECIPE_BLOCKER = """            IF NEW.status IN ('proof_ready', 'sealed')
               AND NEW.status IS DISTINCT FROM OLD.status THEN
                RAISE EXCEPTION
                    'provider_directory_projection_native_attestation_required'
                    USING ERRCODE = '55000';
            END IF;
"""
_RAW_PARTITION_COUNT_CTE = """                actual_partition_counts AS (
                    SELECT proof_partition_id,
                           count(*)::bigint AS resource_count
                      FROM %I.%I
                     GROUP BY proof_partition_id
                ),
"""
_RAW_PARTITION_COUNT_CHECK = """                    AND NOT EXISTS (
                        SELECT 1
                          FROM actual_partition_counts AS actual
                          FULL JOIN expected_partitions AS expected
                            ON expected.partition_id =
                               actual.proof_partition_id
                         WHERE actual.resource_count IS DISTINCT FROM
                               expected.resource_count
                    )
"""


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _quoted_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return str(sa.literal(value).compile(compile_kwargs={"literal_binds": True}))


def _rewrite_function_sql(
    schema: str,
    function_name: str,
    blocker: str,
    anchor: str,
    *,
    upgrade: bool,
    argument_types: str = "",
) -> str:
    qualified_function = (
        f"{_quoted_identifier(schema)}.{function_name}({argument_types})"
    )
    blocked_fragment = blocker + anchor
    source = blocked_fragment if upgrade else anchor
    target = anchor if upgrade else blocked_fragment
    target_absence = "true" if upgrade else f"strpos(current_definition, {_literal(target)}) = 0"
    return f"""
    DO $migration$
    DECLARE
        current_definition text;
        rewritten_definition text;
    BEGIN
        SELECT pg_get_functiondef(CAST({_literal(qualified_function)} AS regprocedure))
          INTO current_definition;
        IF length(current_definition) - length(replace(
               current_definition, {_literal(source)}, ''
           )) <> length({_literal(source)})
           OR NOT ({target_absence}) THEN
            RAISE EXCEPTION
                'provider_directory_projection_finalizer_guard_contract_unexpected'
                USING ERRCODE = '55000';
        END IF;
        rewritten_definition := replace(
            current_definition, {_literal(source)}, {_literal(target)}
        );
        EXECUTE rewritten_definition;
    END;
    $migration$;
    """


def _guard_rewrites(schema: str, *, upgrade: bool) -> tuple[str, ...]:
    insert_anchor = """            IF TG_OP = 'INSERT'
               AND action_setting = 'seal'"""
    recipe_anchor = "            IF TG_OP = 'DELETE' THEN"
    owner_guard_old = "WHERE admission_id = OLD.admission_id"
    owner_guard_new = "WHERE admission_id = to_jsonb(OLD) ->> 'admission_id'"
    if not upgrade:
        owner_guard_old, owner_guard_new = owner_guard_new, owner_guard_old
    return (
        *(
            _rewrite_function_sql(
                schema,
                function_name,
                _INSERT_BLOCKER,
                insert_anchor,
                upgrade=upgrade,
            )
            for function_name in (
                "guard_provider_directory_physical_projection",
                "guard_provider_directory_physical_source_summary",
                "guard_provider_directory_physical_partition",
            )
        ),
        _rewrite_function_sql(
            schema,
            "guard_provider_directory_projection_recipe",
            _RECIPE_BLOCKER,
            recipe_anchor,
            upgrade=upgrade,
        ),
        _rewrite_function_sql(
            schema,
            "provider_directory_projection_stage_matches_proof",
            _RAW_PARTITION_COUNT_CTE,
            "                expected_partitions AS (",
            upgrade=upgrade,
            argument_types="text,text,bigint,bigint,text,jsonb",
        ),
        _rewrite_function_sql(
            schema,
            "provider_directory_projection_stage_matches_proof",
            _RAW_PARTITION_COUNT_CHECK,
            """                    AND NOT EXISTS (
                        SELECT 1
                          FROM %I.%I AS resource_record
            """,
            upgrade=upgrade,
            argument_types="text,text,bigint,bigint,text,jsonb",
        ),
        _rewrite_exact_sql(
            schema,
            "guard_projection_child_owner_terminal",
            owner_guard_old,
            owner_guard_new,
        ),
    )


def _rewrite_exact_sql(
    schema: str,
    function_name: str,
    source: str,
    target: str,
) -> str:
    qualified_function = f"{_quoted_identifier(schema)}.{function_name}()"
    return f"""
    DO $migration$
    DECLARE
        current_definition text;
    BEGIN
        SELECT pg_get_functiondef(CAST({_literal(qualified_function)} AS regprocedure))
          INTO current_definition;
        IF length(current_definition) - length(replace(
               current_definition, {_literal(source)}, ''
           )) <> length({_literal(source)})
           OR strpos(current_definition, {_literal(target)}) > 0 THEN
            RAISE EXCEPTION
                'provider_directory_projection_finalizer_guard_contract_unexpected'
                USING ERRCODE = '55000';
        END IF;
        EXECUTE replace(
            current_definition, {_literal(source)}, {_literal(target)}
        );
    END;
    $migration$;
    """


def _rewrite_guards(*, upgrade: bool) -> None:
    schema = _schema()
    quoted_schema = _quoted_identifier(schema)
    op.execute(
        sa.text(
            "LOCK TABLE "
            f"{quoted_schema}.provider_directory_physical_projection, "
            f"{quoted_schema}.provider_directory_physical_projection_source_summary, "
            f"{quoted_schema}.provider_directory_physical_projection_partition, "
            f"{quoted_schema}.provider_directory_projection_recipe "
            "IN SHARE ROW EXCLUSIVE MODE;"
        )
    )
    for statement in _guard_rewrites(schema, upgrade=upgrade):
        op.execute(sa.text(statement))


def upgrade() -> None:
    """Enable only the already-guarded proof-ready and seal transitions."""

    _rewrite_guards(upgrade=True)


def downgrade() -> None:
    """Restore the deliberate native-attestation blockers."""

    _rewrite_guards(upgrade=False)
