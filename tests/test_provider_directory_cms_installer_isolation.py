# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Installer isolation compatibility leaves every runtime Admin predicate intact."""

import sqlite3
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from db import migration_cms_noncurrent_seal as seal
from db import migration_cms_replay_resolution as replay
from db import migration_cms_retirement_resolution as retirement
from db import migration_cms_snapshot_allocator as allocator
from db import migration_cms_terminal_resolution as terminal
from process import provider_directory_cms_generic_seal as generic_runtime
from process import provider_directory_cms_snapshot_allocation as allocation_runtime
from process import provider_directory_cms_snapshot_parents as parent_runtime

_INSTALLERS = (
    (allocator, "upgrade_cms_snapshot_allocator", "_canonical_copy_guard_snapshot"),
    (allocator, "upgrade_cms_generic_finalization_guard", "_canonical_copy_guard_snapshot"),
    (replay, "upgrade_cms_replay_guard_resolution", "_canonical_copy_guard_snapshot"),
    (terminal, "upgrade_cms_terminal_guard_resolution", "_canonical_copy_guard_snapshot"),
    (retirement, "upgrade_cms_retirement_resolution", "_snapshot"),
    (seal, "upgrade_cms_noncurrent_seal", "_snapshot"),
)


def test_installer_query_changes_only_the_exact_isolation_clause():
    original = "pg_catalog.current_setting('transaction_isolation')='read committed'"
    successor = "pg_catalog.current_setting('transaction_isolation') IN ('read committed','serializable')"
    assert str(allocator._ADMIN_SQL).count(original) == 1
    assert str(allocator._INSTALLER_ADMIN_SQL) == str(allocator._ADMIN_SQL).replace(original, successor, 1)


def test_runtime_consumers_keep_the_original_query():
    assert generic_runtime._ADMIN_SQL is allocator._ADMIN_SQL
    assert allocation_runtime._ADMIN_SQL is allocator._ADMIN_SQL
    assert parent_runtime._ADMIN_SQL is allocator._ADMIN_SQL
    assert "serializable" not in str(allocator._ADMIN_SQL)


def _is_query_accepted(query, context_by_field):
    """Evaluate the portable SELECT logic; real PostgreSQL actor custody is tested natively."""
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute("CREATE TABLE pg_roles(rolname text,rolsuper boolean,session_user text,current_user text)")
        connection.execute(
            "INSERT INTO pg_roles VALUES(?,?,?,?)",
            (
                context_by_field["role"],
                context_by_field["superuser"],
                context_by_field["session"],
                context_by_field["current"],
            ),
        )
        connection.create_function("current_setting", 1, lambda name: context_by_field[name])
        observed = connection.execute(str(query).replace("pg_catalog.", "")).fetchone()
        return observed is not None and observed[0] == 1


@pytest.mark.parametrize(
    "changes,runtime_accepts,installer_accepts",
    [
        ({}, True, True),
        ({"transaction_isolation": "serializable"}, False, True),
        ({"transaction_isolation": "repeatable read"}, False, False),
        ({"transaction_isolation": "read uncommitted"}, False, False),
        ({"transaction_read_only": "on"}, False, False),
        ({"transaction_isolation": "serializable", "transaction_read_only": "on"}, False, False),
        ({"current": "assumed_role"}, False, False),
        ({"superuser": False}, False, False),
        ({"session_replication_role": "replica"}, False, False),
        ({"role": "different_login"}, False, False),
    ],
)
def test_query_predicates_preserve_every_other_original_admin_check(changes, runtime_accepts, installer_accepts):
    context_by_field = dict(
        role="administrator",
        superuser=True,
        session="administrator",
        current="administrator",
        transaction_isolation="read committed",
        transaction_read_only="off",
        session_replication_role="origin",
    )
    context_by_field.update(changes)
    assert _is_query_accepted(allocator._ADMIN_SQL, context_by_field) is runtime_accepts
    assert _is_query_accepted(allocator._INSTALLER_ADMIN_SQL, context_by_field) is installer_accepts


@pytest.mark.parametrize("module,installer,snapshot", _INSTALLERS)
@pytest.mark.parametrize("admin", [False, True])
def test_each_installer_uses_only_the_installer_query_before_snapshot(monkeypatch, module, installer, snapshot, admin):
    reached_snapshot = RuntimeError("verified context reached snapshot")
    snapshot_read = Mock(side_effect=reached_snapshot)
    monkeypatch.setattr(module, snapshot, snapshot_read)
    connection = SimpleNamespace(
        in_transaction=Mock(return_value=True),
        exec_driver_sql=Mock(return_value=SimpleNamespace(scalar_one=lambda: "pg_catalog")),
        execute=Mock(return_value=SimpleNamespace(scalar_one_or_none=lambda: admin)),
        begin_nested=Mock(),
    )
    pins_by_key = dict(oid=101, owner_oid=201)
    if module is retirement:
        pins_by_key = {name: dict(pins_by_key) for name in retirement.retirement_resolution_bodies("sample")}
    with pytest.raises(RuntimeError) as caught:
        getattr(module, installer)(connection, "sample", expected=pins_by_key)
    if admin:
        assert caught.value is reached_snapshot
        snapshot_read.assert_called_once()
    else:
        assert str(caught.value).endswith("original_admin_required")
        snapshot_read.assert_not_called()
    connection.execute.assert_called_once_with(allocator._INSTALLER_ADMIN_SQL)
    connection.begin_nested.assert_not_called()
    connection.exec_driver_sql.assert_called_once_with("SHOW search_path")
