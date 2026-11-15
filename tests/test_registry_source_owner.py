# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Operator capability cannot be inferred from ordinary API database access."""

import importlib.util
import json
import re
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db import connection as database_connection
from process import registry_management_permissions as permissions
from process import registry_source_import as source_import
from tests.registry_source_fixtures import _arguments, _edition, _input


@pytest.mark.parametrize(
    "filename,table",
    [
        ("20261009010000_registry_ptg_producer_scope.py", "registry_ptg_producer_scope"),
        ("20261009040000_registry_ptg_published_plan_scope.py", "registry_ptg_published_plan_scope"),
        ("20261010010000_registry_ptg_office_approval.py", "registry_ptg_office_approval"),
        ("20261010020000_registry_ptg_office_retention.py", "registry_ptg_office_hold"),
    ],
)
def test_ptg_migration_tables_are_complete_and_read_only_for_api(filename, table):
    path = Path(__file__).parents[1] / "alembic/versions" / filename
    specification = importlib.util.spec_from_file_location(path.stem, path)
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    statement = next(sql for sql in migration._ddl("example_registry") if sql.startswith("CREATE TABLE"))
    columns = tuple(
        re.findall(r"(?:\(|,)\s*([a-z_][a-z0-9_]*)\s+(?:uuid|varchar|timestamptz|jsonb)\b", statement, re.I)
    )
    assert permissions._TABLES[table] == columns
    assert table not in permissions._INSERT_COLUMNS and table not in permissions._UPDATE_COLUMNS


class OwnerConnection:
    def __init__(self):
        self.statements = []

    def is_in_transaction(self):
        return True

    async def execute(self, query, *_arguments):
        self.statements.append(query)


def test_optional_owner_mode_keeps_original_edition_and_default(tmp_path):
    edition = _edition(_input())
    arguments = _arguments(tmp_path / "input.csv", edition)
    ordinary = source_import._parser().parse_args(arguments)
    closed = source_import._parser().parse_args(arguments + ["--protected-owner-role", "example_owner"])
    assert ordinary.protected_owner_role is None
    assert closed.protected_owner_role == "example_owner"
    assert source_import._edition(ordinary) == source_import._edition(closed) == edition


class OperatorDatabase:
    def __init__(self, failure_stage=None):
        self.failure_stage = failure_stage
        self.events = []
        self.in_transaction = False
        self.engine = SimpleNamespace(echo=True)

    def noisy_stage(self, stage):
        self.events.append(stage)
        print("synthetic-connection-detail")
        print("synthetic-connection-detail", file=sys.stderr)
        if stage == self.failure_stage:
            raise RuntimeError("synthetic-connection-detail")

    async def connect(self):
        self.noisy_stage("connect")

    async def disconnect(self):
        self.noisy_stage("disconnect")

    @asynccontextmanager
    async def acquire_driver(self):
        self.events.append("acquire")
        try:
            yield self
        finally:
            self.events.append("release")

    @asynccontextmanager
    async def transaction(self):
        self.events.append("begin")
        self.in_transaction = True
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")
        finally:
            self.in_transaction = False


def _operator_arguments():
    return [
        "--source-schema",
        "example_registry",
        "--target-schema",
        "example_protected_registry",
        "--api-role",
        "example_api",
        "--owner-role",
        "example_owner",
    ]


@pytest.mark.parametrize("retire", [False, True])
def test_operator_cli_selects_transaction_and_closes_pool(monkeypatch, capsys, retire):
    database = OperatorDatabase()
    operation_name = "retire_registry_read_views" if retire else "provision_empty_registry_namespace"

    async def operation(connection, **keywords):
        assert connection is database and database.in_transaction and database.engine.echo is False
        assert keywords == {
            "source_schema": "example_registry",
            "target_schema": "example_protected_registry",
            "api_role": "example_api",
            "owner_role": "example_owner",
        }
        database.noisy_stage(operation_name)
        return {"component": operation_name}

    async def unexpected(*_args, **_keywords):
        raise AssertionError("Unselected operation must not run")

    monkeypatch.setattr(database_connection, "Database", lambda: database)
    monkeypatch.setattr(permissions, operation_name, operation)
    other = "provision_empty_registry_namespace" if retire else "retire_registry_read_views"
    monkeypatch.setattr(permissions, other, unexpected)
    arguments = _operator_arguments() + (["--retire-read-views"] if retire else [])
    assert permissions.run_command(arguments) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"status": "ok", "component": operation_name}
    assert captured.out.count("\n") == 1 and captured.err == ""
    assert database.events == ["connect", "acquire", "begin", operation_name, "commit", "release", "disconnect"]
    assert not database.in_transaction and database.engine.echo is True


@pytest.mark.parametrize("failure_stage", ["connect", "operation", "disconnect"])
def test_operator_cli_sanitizes_failure_and_finishes_teardown(monkeypatch, capsys, failure_stage):
    database = OperatorDatabase(failure_stage)

    async def operation(connection, **_keywords):
        assert connection is database and database.in_transaction and database.engine.echo is False
        database.noisy_stage("operation")
        return {"component": "example"}

    monkeypatch.setattr(database_connection, "Database", lambda: database)
    monkeypatch.setattr(permissions, "provision_empty_registry_namespace", operation)
    assert permissions.run_command(_operator_arguments()) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {"status": "error", "code": "registry_provisioning_failed"}
    assert captured.err.count("\n") == 1
    assert database.events[-1] == "disconnect" and database.events.count("disconnect") == 1
    assert ("rollback" in database.events) == (failure_stage == "operation")
    assert ("release" in database.events) == (failure_stage != "connect")
    assert not database.in_transaction and database.engine.echo is True


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_mode", ["ordinary", "closed", "refused"])
async def test_source_cli_owner_gate_precedes_admission_and_teardown(monkeypatch, capsys, owner_mode):
    database, input_bytes = OperatorDatabase(), _input()
    edition = _edition(input_bytes)

    async def assume(connection, owner_role):
        assert connection is database and database.in_transaction and owner_role == "example_owner"
        database.events.append("owner")
        if owner_mode == "refused":
            raise permissions.RegistryManagementPermissionError("synthetic-connection-detail")

    async def admit(connection, actual_input, actual_edition):
        assert connection is database and database.in_transaction and database.engine.echo is False
        assert actual_input == input_bytes and actual_edition == edition
        database.events.append("admit")
        return {
            "copy_sha256": "a" * 64,
            "copy_bytes": 0,
            "replayed": False,
            "identity_materialization": {},
            "native_counts": {},
        }

    monkeypatch.setattr(source_import, "Database", lambda: database)
    monkeypatch.setattr(permissions, "assume_registry_source_owner", assume)
    monkeypatch.setattr(source_import, "admit_cms_mlr_edition", admit)
    owner_role = None if owner_mode == "ordinary" else "example_owner"
    if owner_mode == "refused":
        with pytest.raises(permissions.RegistryManagementPermissionError):
            await source_import._run_import(input_bytes, edition, owner_role)
        assert "admit" not in database.events and "rollback" in database.events
    else:
        receipt = json.loads(await source_import._run_import(input_bytes, edition, owner_role))
        assert receipt["status"] == "ok" and receipt["snapshot_id"] == str(edition.snapshot_id)
        assert ("owner" in database.events) == (owner_mode == "closed")
        assert database.events.index("admit") > database.events.index("begin")
        if owner_mode == "closed":
            assert database.events.index("owner") < database.events.index("admit")
    assert database.events[-2:] == ["release", "disconnect"]
    assert not database.in_transaction and database.engine.echo is True
    captured = capsys.readouterr()
    assert captured.out == captured.err == ""


@pytest.mark.asyncio
async def test_source_owner_requires_transaction_before_catalog_access():
    connection = OwnerConnection()
    connection.is_in_transaction = lambda: False
    connection.fetchrow = AsyncMock()
    connection.fetchval = AsyncMock()
    with pytest.raises(permissions.RegistryManagementPermissionError, match="caller transaction"):
        await permissions.assume_registry_source_owner(connection, "example_owner", control_schema="example_registry")
    connection.fetchrow.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    assert connection.statements == []


def _read_bridge_catalog():
    return [
        {
            "relname": name,
            "relkind": "v",
            "relpersistence": "p",
            "columns": list(columns),
            "targets": [{"schema": "example_protected_registry", "name": name}],
        }
        for name, columns in permissions._TABLES.items()
    ]


@pytest.mark.asyncio
async def test_source_owner_uses_actual_set_role_without_catalog_identity_queries():
    connection = OwnerConnection()
    await permissions.assume_registry_source_owner(connection, "example_owner", control_schema="example_registry")
    assert connection.statements == ['SET LOCAL ROLE "example_owner"']


@pytest.mark.asyncio
async def test_source_owner_propagates_postgresql_role_denial():
    failure = PermissionError("not permitted")
    connection = SimpleNamespace(is_in_transaction=lambda: True, execute=AsyncMock(side_effect=failure))
    with pytest.raises(PermissionError) as caught:
        await permissions.assume_registry_source_owner(connection, "example_owner", control_schema="example_registry")
    assert caught.value is failure
    connection.execute.assert_awaited_once_with('SET LOCAL ROLE "example_owner"')


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "missing", "duplicate", "columns", "target", "kind"])
async def test_read_bridge_requires_complete_named_data_projection(monkeypatch, fault):
    views = _read_bridge_catalog()
    match fault:
        case "missing":
            views.pop()
        case "duplicate":
            views[-1] = views[0]
        case "columns":
            views[0]["columns"] = []
        case "target":
            views[0]["targets"] = [{"schema": "other", "name": views[0]["relname"]}]
        case "kind":
            views[0]["relkind"] = "r"
    reader = AsyncMock(return_value=views)
    monkeypatch.setattr(permissions, "_read_view_catalog", reader)
    if fault:
        with pytest.raises(permissions.RegistryManagementPermissionError):
            await permissions._registry_read_views(object(), "example_registry", "example_protected_registry")
    else:
        assert await permissions._registry_read_views(object(), "example_registry", "example_protected_registry") == [
            view["relname"] for view in views
        ]


@pytest.mark.asyncio
async def test_empty_registry_locks_names_and_rejects_retained_business_rows(monkeypatch):
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=True))
    tables = [{"relname": "company_registry", "columns": []}]
    catalog = AsyncMock(return_value=(tables, []))
    monkeypatch.setattr(permissions, "_catalog", catalog)
    with pytest.raises(permissions.RegistryManagementPermissionError, match="retained data"):
        await permissions._lock_empty_registry(connection, "example_registry", tables, [])
    connection.execute.assert_awaited_once_with(
        'LOCK TABLE "example_registry"."company_registry" IN ACCESS EXCLUSIVE MODE NOWAIT'
    )
    assert "WHERE NOT (false)" in connection.fetchval.await_args.args[0]
    catalog.assert_awaited_once_with(connection, "example_registry")


@pytest.mark.asyncio
async def test_registry_move_preserves_named_standalone_sequences(monkeypatch):
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=True))
    tables = [{"relname": "company_registry"}]
    sequences = [{"relname": "network_registry_identity_network_id_seq"}]
    catalog = AsyncMock(return_value=(tables, sequences))
    monkeypatch.setattr(permissions, "_catalog", catalog)
    await permissions._move_registry_objects(
        connection, "example_registry", "example_protected_registry", "example_owner", tables, sequences
    )
    assert connection.execute.await_args.args == (
        'ALTER SEQUENCE "example_registry"."network_registry_identity_network_id_seq" SET SCHEMA "example_protected_registry"',
    )
    catalog.assert_awaited_once_with(connection, "example_protected_registry")
