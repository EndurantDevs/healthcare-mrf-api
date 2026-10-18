# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact native checkpoint layout/guard component fixture, without private profile claims."""

import importlib.util
from pathlib import Path
from uuid import uuid4

from db.migration_provider_directory_terminal_root_retirement_guards import child_guard_function_sql
from process.provider_directory_generic_source_policy import bound_publisher
from process.provider_directory_source_checkpoint_catalog import _GUARDS, CHECKPOINT_LAYOUTS, RETIRED_BULK_HOOKS


def _checkpoint_function(schema):
    path = (
        Path(__file__).parents[1]
        / "alembic/versions/20260810010000_provider_directory_reviewed_subset_terminal_disposition.py"
    )
    spec = importlib.util.spec_from_file_location("journal_checkpoint_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._checkpoint_guard_sql(schema)


async def _layout(connection, schema, root, layout):
    columns = [
        f'"{name}" {kind}' + (" NOT NULL" if required else "") + (" DEFAULT " + default if default is not None else "")
        for name, kind, required, default in layout["columns"]
    ]
    constraints = [definition.replace("mrf.", f'"{schema}".') for _, definition in layout["constraints"]]
    await connection.execute(f'CREATE TABLE "{schema}"."{root}" (' + ",".join(columns + constraints) + ")")
    for primary, unique, method, predicate, expression, keys in layout["indexes"]:
        if not primary and not unique:
            assert predicate is expression is None and method == "btree"
            name = "checkpoint_fixture_" + uuid4().hex
            await connection.execute(
                f'CREATE INDEX "{name}" ON "{schema}"."{root}" (' + ",".join('"' + key + '"' for key in keys) + ")"
            )


async def _guard(connection, schema, root, name, guard):
    event, deferred, initial, old, new, function = guard
    assert old is new is None
    constraint = "CONSTRAINT " if deferred else ""
    timing = "BEFORE" if event & 2 else "AFTER"
    actions = " OR ".join(
        action for bit, action in ((4, "INSERT"), (8, "DELETE"), (16, "UPDATE"), (32, "TRUNCATE")) if event & bit
    )
    defer = " DEFERRABLE INITIALLY DEFERRED" if initial else ""
    level = "ROW" if event & 1 else "STATEMENT"
    await connection.execute(
        f'CREATE {constraint}TRIGGER "{name}" {timing} {actions} ON "{schema}"."{root}"'
        + defer
        + f' FOR EACH {level} EXECUTE FUNCTION "{schema}"."{function}"()'
    )
    await connection.execute(f'ALTER TABLE "{schema}"."{root}" ENABLE ALWAYS TRIGGER "{name}"')


async def _controls(case):
    """Install fixed native layouts and authentic existing guard bodies, not a runtime manifest."""
    connection = bound_publisher().connection
    for root, layout in CHECKPOINT_LAYOUTS.items():
        await _layout(connection, case.schema, root, layout)
    await connection.execute(child_guard_function_sql(case.schema))
    await connection.execute(_checkpoint_function(case.schema))
    for root, guards in _GUARDS.items():
        for name, guard in guards.items():
            if name not in RETIRED_BULK_HOOKS:
                await _guard(connection, case.schema, root, name, guard)
        await connection.execute(f'GRANT SELECT ON "{case.schema}"."{root}" TO "{case.reader}"')
