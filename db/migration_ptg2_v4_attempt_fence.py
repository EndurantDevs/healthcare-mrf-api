# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lifecycle and downgrade guards for the PTG V4 attempt authority."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import sqlalchemy as sa

from db.migration_ptg2_v4_attempt_catalog import (
    validate_current_attempt_authority,
)
from db.migration_expression_adoption import _normalized_expression
from db.ptg2_v4_attempt_schema import (
    ATTEMPT_FENCE_TABLE,
    ATTEMPT_STAGE_TABLE,
)


_LIFECYCLE_LOCK_KEY = "ptg2_source_pointer_gc_v1"
_LIFECYCLE_FUNCTION = "lock_ptg2_v4_attempt_lifecycle"
_LIFECYCLE_FUNCTION_BODY = f"""
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('{_LIFECYCLE_LOCK_KEY}'));
    RETURN NULL;
END;
""".strip()


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def execute_when_table_exists(
    operations: Any,
    schema: str,
    table_name: str,
    statement: str,
) -> None:
    """Execute one DDL statement only when its runtime-owned table exists."""

    qualified_table = _qt(schema, table_name)
    operations.execute(
        f"""
        DO $guard$
        BEGIN
            IF to_regclass({_sql_literal(qualified_table)}) IS NOT NULL THEN
                EXECUTE {_sql_literal(statement.strip())};
            END IF;
        END;
        $guard$
        """
    )


def install_lifecycle_lock_layer(
    operations: Any,
    schema: str,
    guarded_tables: Iterable[str],
) -> None:
    """Install the shared predecessor lifecycle-lock layer."""

    lifecycle_function = f"{_q(schema)}.{_q(_LIFECYCLE_FUNCTION)}"
    operations.execute(
        f"""
        CREATE OR REPLACE FUNCTION {lifecycle_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        {_LIFECYCLE_FUNCTION_BODY}
        $$;
        """
    )
    for table_name in guarded_tables:
        trigger_name = f"{table_name}_attempt_lifecycle_lock"
        qualified_table = _qt(schema, table_name)
        execute_when_table_exists(
            operations,
            schema,
            table_name,
            f"DROP TRIGGER IF EXISTS {_q(trigger_name)} ON {qualified_table}",
        )
        execute_when_table_exists(
            operations,
            schema,
            table_name,
            f"""
            CREATE TRIGGER {_q(trigger_name)}
            BEFORE INSERT OR UPDATE OR DELETE ON {qualified_table}
            FOR EACH STATEMENT EXECUTE FUNCTION {lifecycle_function}()
            """,
        )


def _is_exact_lifecycle_function(
    connection: sa.engine.Connection,
    schema: str,
) -> bool:
    function_result = connection.execute(
        sa.text(
            """
            SELECT language_record.lanname,
                   function_record.pronargs,
                   function_record.prorettype::regtype::text AS return_type,
                   function_record.prosrc
              FROM pg_proc AS function_record
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = function_record.pronamespace
              JOIN pg_language AS language_record
                ON language_record.oid = function_record.prolang
             WHERE namespace_record.nspname = :schema
               AND function_record.proname = :function_name
            """
        ),
        {"schema": schema, "function_name": _LIFECYCLE_FUNCTION},
    ).mappings()
    function_catalog_entries = list(function_result)
    if len(function_catalog_entries) != 1:
        return False
    function_mapping = function_catalog_entries[0]
    return (
        function_mapping["lanname"] == "plpgsql"
        and int(function_mapping["pronargs"]) == 0
        and function_mapping["return_type"] == "trigger"
        and _normalized_expression(function_mapping["prosrc"])
        == _normalized_expression(_LIFECYCLE_FUNCTION_BODY)
    )


def _lifecycle_trigger_catalog(
    connection: sa.engine.Connection,
    schema: str,
) -> dict[str, tuple[str, int, Any, str, str]]:
    trigger_result = connection.execute(
        sa.text(
            """
            SELECT trigger_record.tgname,
                   relation_record.relname,
                   trigger_record.tgtype,
                   trigger_record.tgenabled,
                   function_record.proname,
                   function_schema.nspname AS function_schema
              FROM pg_trigger AS trigger_record
              JOIN pg_class AS relation_record
                ON relation_record.oid = trigger_record.tgrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
              JOIN pg_proc AS function_record
                ON function_record.oid = trigger_record.tgfoid
              JOIN pg_namespace AS function_schema
                ON function_schema.oid = function_record.pronamespace
             WHERE namespace_record.nspname = :schema
               AND NOT trigger_record.tgisinternal
               AND right(
                    trigger_record.tgname,
                    length('_attempt_lifecycle_lock')
               ) = '_attempt_lifecycle_lock'
            """
        ),
        {"schema": schema},
    ).mappings()
    return {
        str(trigger_mapping["tgname"]): (
            str(trigger_mapping["relname"]),
            int(trigger_mapping["tgtype"]),
            trigger_mapping["tgenabled"],
            str(trigger_mapping["proname"]),
            str(trigger_mapping["function_schema"]),
        )
        for trigger_mapping in trigger_result
    }


def validate_lifecycle_lock_layer(
    operations: Any,
    schema: str,
    guarded_tables: Iterable[str],
) -> None:
    """Require the exact lifecycle function and BEFORE-statement triggers."""

    get_bind = getattr(operations, "get_bind", None)
    connection = get_bind() if callable(get_bind) else None
    if not isinstance(connection, sa.engine.Connection):
        raise RuntimeError("ptg2_v4_lifecycle_validation_requires_online")
    existing_tables = {
        table_name
        for table_name in guarded_tables
        if connection.execute(
            sa.text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
            {"qualified_name": f"{schema}.{table_name}"},
        ).scalar_one()
    }
    expected_by_trigger = {
        f"{table_name}_attempt_lifecycle_lock": (
            table_name,
            30,
            {"O", b"O"},
            _LIFECYCLE_FUNCTION,
            schema,
        )
        for table_name in existing_tables
    }
    actual_by_trigger = _lifecycle_trigger_catalog(connection, schema)
    if not _is_exact_lifecycle_function(connection, schema):
        raise RuntimeError(f"ptg2_v4_lifecycle_function_unknown:{schema}")
    if set(actual_by_trigger) != set(expected_by_trigger):
        raise RuntimeError(f"ptg2_v4_lifecycle_trigger_set_unknown:{schema}")
    for trigger_name, expected in expected_by_trigger.items():
        actual = actual_by_trigger[trigger_name]
        if actual[:2] != expected[:2] or actual[3:] != expected[3:]:
            raise RuntimeError(f"ptg2_v4_lifecycle_trigger_unknown:{trigger_name}")
        if actual[2] not in expected[2]:
            raise RuntimeError(f"ptg2_v4_lifecycle_trigger_disabled:{trigger_name}")


def _lock_downgrade_catalog(operations: Any, schema: str) -> None:
    """Hold every decision relation before checking downgrade eligibility."""

    get_bind = getattr(operations, "get_bind", None)
    connection = get_bind() if callable(get_bind) else None

    def execute_lock(statement: str) -> None:
        """Execute one lock through the online or offline operation path."""

        if isinstance(connection, sa.engine.Connection):
            connection.exec_driver_sql(statement)
        else:
            operations.execute(statement)

    snapshot = _qt(schema, "ptg2_snapshot")
    internal_run = _qt(schema, "ptg2_import_run")
    layout = _qt(schema, "ptg2_v3_snapshot_layout")
    root = _qt(schema, "ptg2_v4_snapshot_map_root")
    fence = _qt(schema, ATTEMPT_FENCE_TABLE)
    stage = _qt(schema, ATTEMPT_STAGE_TABLE)
    execute_lock(
        f"SELECT pg_advisory_xact_lock(hashtext('{_LIFECYCLE_LOCK_KEY}'));"
    )
    execute_lock(
        f"""
        LOCK TABLE {snapshot}, {internal_run}, {layout}, {root}
            IN SHARE ROW EXCLUSIVE MODE
        """
    )
    execute_lock(
        f"""
        LOCK TABLE {fence}, {stage} IN ACCESS EXCLUSIVE MODE
        """
    )


def _downgrade_refusal_sql(schema: str) -> str:
    """Return the complete live-authority downgrade refusal statement."""

    fence = _qt(schema, ATTEMPT_FENCE_TABLE)
    stage = _qt(schema, ATTEMPT_STAGE_TABLE)
    snapshot = _qt(schema, "ptg2_snapshot")
    internal_run = _qt(schema, "ptg2_import_run")
    root = _qt(schema, "ptg2_v4_snapshot_map_root")
    layout = _qt(schema, "ptg2_v3_snapshot_layout")
    return f"""
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM {fence})
               OR EXISTS (SELECT 1 FROM {stage})
               OR EXISTS (
                    SELECT 1 FROM {internal_run}
                     WHERE COALESCE(
                        options::jsonb->>'storage_generation', ''
                     ) = 'shared_blocks_v4'
               )
               OR EXISTS (
                    SELECT 1
                      FROM {snapshot} AS snapshot
                      JOIN {internal_run} AS internal_run
                        ON internal_run.import_run_id = snapshot.import_run_id
                     WHERE COALESCE(
                        internal_run.options::jsonb->>'storage_generation', ''
                     ) = 'shared_blocks_v4'
               )
               OR EXISTS (SELECT 1 FROM {root})
               OR EXISTS (
                    SELECT 1 FROM {layout}
                     WHERE generation = 'shared_blocks_v4'
               )
               OR EXISTS (
                    SELECT 1 FROM {snapshot}
                     WHERE COALESCE(manifest::jsonb, '{{}}'::jsonb)
                           ? 'ptg2_v4_stale_metadata_reconciliation'
               )
               OR EXISTS (
                    SELECT 1 FROM {internal_run}
                     WHERE COALESCE(report::jsonb, '{{}}'::jsonb)
                           ? 'ptg2_v4_stale_metadata_reconciliation'
               )
            THEN
                RAISE EXCEPTION
                    'ptg2_v4_attempt_downgrade_requires_empty_authority'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $block$;
    """


def assert_attempt_downgrade_safe(operations: Any, schema: str) -> None:
    """Refuse to discard authority while any V4 or audit state remains."""

    _lock_downgrade_catalog(operations, schema)
    validate_current_attempt_authority(operations, schema)
    operations.execute(_downgrade_refusal_sql(schema))


__all__ = [
    "assert_attempt_downgrade_safe",
    "execute_when_table_exists",
    "install_lifecycle_lock_layer",
    "validate_lifecycle_lock_layer",
]
