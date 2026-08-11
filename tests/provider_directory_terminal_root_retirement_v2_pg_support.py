# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL migration-fence helpers for terminal-root retirement v2."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest

from db import (
    migration_provider_directory_terminal_root_retirement_guards as legacy_guards,
)
from tests.provider_directory_terminal_root_retirement_pg_support import (
    RetirementPostgres,
    SqlCapture,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic/versions"
    / "20260810120000_provider_directory_terminal_root_retirement_v2.py"
)


def load_v2_migration() -> Any:
    """Load the additive v2 migration as an isolated module."""

    module_spec = importlib.util.spec_from_file_location(
        "terminal_root_retirement_v2_postgres_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def run_v2_migration(
    scenario: RetirementPostgres,
    migration: Any,
    action: str,
) -> None:
    """Execute captured migration statements atomically."""

    capture = SqlCapture()
    migration.op = capture
    getattr(migration, action)()
    async with scenario.connection.transaction():
        for statement in capture.statements:
            await scenario.connection.execute(statement)


async def retirement_trigger_state(
    scenario: RetirementPostgres,
) -> tuple[tuple[Any, ...], ...]:
    """Return stable identity and enablement for every retirement trigger."""

    trigger_records = await scenario.connection.fetch(
        """
        SELECT trigger_row.oid, trigger_row.tgrelid,
               relation_row.relname, trigger_row.tgname,
               trigger_row.tgfoid, trigger_row.tgtype,
               trigger_row.tgenabled
          FROM pg_catalog.pg_trigger AS trigger_row
          JOIN pg_catalog.pg_class AS relation_row
            ON relation_row.oid = trigger_row.tgrelid
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = relation_row.relnamespace
         WHERE namespace_row.nspname = $1
           AND left(trigger_row.tgname, 7) = 'pd_trr_'
         ORDER BY relation_row.relname, trigger_row.tgname
        """,
        scenario.schema_name,
    )
    return tuple(tuple(trigger_record) for trigger_record in trigger_records)


async def expect_fence_rejection(
    scenario: RetirementPostgres,
    mutation_sql: str,
    fence_sql: str,
    expected_error: str,
) -> None:
    """Prove a catalog mutation is rejected and roll it back."""

    transaction = scenario.connection.transaction()
    await transaction.start()
    try:
        await scenario.connection.execute(mutation_sql)
        with pytest.raises(Exception, match=expected_error) as error:
            await scenario.connection.execute(fence_sql)
        assert getattr(error.value, "sqlstate", None) == "55000"
    finally:
        await transaction.rollback()


async def expect_migration_rejection(
    scenario: RetirementPostgres,
    migration: Any,
    action: str,
    mutation_sql: str,
    expected_error: str,
) -> None:
    """Prove a full migration rejects drift and leaves no writes."""

    capture = SqlCapture()
    migration.op = capture
    getattr(migration, action)()
    transaction = scenario.connection.transaction()
    await transaction.start()
    try:
        await scenario.connection.execute(mutation_sql)
        with pytest.raises(Exception, match=expected_error) as error:
            for statement in capture.statements:
                await scenario.connection.execute(statement)
        assert getattr(error.value, "sqlstate", None) == "55000"
    finally:
        await transaction.rollback()


def drop_trigger_sql(
    scenario: RetirementPostgres,
    table_name: str,
    trigger_name: str,
) -> str:
    """Return one exact trigger-drop mutation."""

    return f'DROP TRIGGER "{trigger_name}" ON ' f'{scenario.schema}."{table_name}"'


def disable_trigger_sql(
    scenario: RetirementPostgres,
    table_name: str,
    trigger_name: str,
) -> str:
    """Return one exact trigger-disable mutation."""

    return (
        f'ALTER TABLE {scenario.schema}."{table_name}" '
        f'DISABLE TRIGGER "{trigger_name}"'
    )


def function_signature_sql(
    scenario: RetirementPostgres,
    migration: Any,
    function_spec: dict[str, object],
) -> str:
    """Return the migration's schema-qualified function signature."""

    return migration._signature(
        scenario.schema_name,
        str(function_spec["name"]),
        str(function_spec["arguments"]),
    )


def drifted_function_body_sql(
    migration: Any,
    function_spec: dict[str, object],
) -> str:
    """Return a valid same-signature body with an authenticated text drift."""

    replacement_sql = migration._replacement_sql(str(function_spec["rendered_sql"]))
    body_marker = "AS $function$\n"
    assert replacement_sql.count(body_marker) == 1
    return replacement_sql.replace(
        body_marker,
        body_marker + "    -- authenticated drift probe\n",
        1,
    )


def _trigger_replacement_sql(
    scenario: RetirementPostgres,
    *,
    table_name: str,
    trigger_name: str,
    event_clause: str,
    execution_clause: str,
    function_name: str,
    arguments: str = "",
) -> str:
    return (
        drop_trigger_sql(scenario, table_name, trigger_name)
        + "; CREATE TRIGGER "
        + f'"{trigger_name}" {event_clause} ON '
        + f'{scenario.schema}."{table_name}" {execution_clause} '
        + f'EXECUTE FUNCTION {scenario.schema}."{function_name}"({arguments})'
    )


def representative_trigger_drift_sql(
    scenario: RetirementPostgres,
) -> tuple[str, ...]:
    """Return drift across timing, events, function, args, and WHEN shape."""

    parent = legacy_guards.PARENT_GUARD
    child = legacy_guards.CHILD_GUARD
    run_guard = legacy_guards.IMPORT_RUN_GUARD
    replacement = _trigger_replacement_sql
    return (
        replacement(
            scenario,
            table_name="provider_directory_endpoint_dataset",
            trigger_name="pd_trr_dataset_row",
            event_clause="BEFORE INSERT",
            execution_clause="FOR EACH ROW",
            function_name=child,
        ),
        replacement(
            scenario,
            table_name="provider_directory_endpoint_dataset",
            trigger_name="pd_trr_dataset_truncate",
            event_clause="AFTER TRUNCATE",
            execution_clause="FOR EACH STATEMENT",
            function_name=parent,
        ),
        replacement(
            scenario,
            table_name="provider_directory_dataset_resource",
            trigger_name="pd_trr_resource_row",
            event_clause="AFTER UPDATE",
            execution_clause="FOR EACH ROW",
            function_name=child,
        ),
        replacement(
            scenario,
            table_name="provider_directory_dataset_resource",
            trigger_name="pd_trr_resource_truncate",
            event_clause="BEFORE TRUNCATE",
            execution_clause="FOR EACH STATEMENT",
            function_name=child,
            arguments="'drift'",
        ),
        replacement(
            scenario,
            table_name="import_run",
            trigger_name="pd_trr_import_run_row",
            event_clause="BEFORE INSERT OR UPDATE OR DELETE",
            execution_clause="FOR EACH ROW WHEN (true)",
            function_name=run_guard,
        ),
        replacement(
            scenario,
            table_name="import_run",
            trigger_name="pd_trr_import_run_truncate",
            event_clause="AFTER TRUNCATE",
            execution_clause="FOR EACH STATEMENT",
            function_name=run_guard,
        ),
    )
