# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Operator capability cannot be inferred from ordinary API database access."""

import importlib.util
import json
import re
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

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
    def __init__(self, *, login=False, schema_owner=17, can_set=True, schema_create=True, schema_usage=True):
        self.owner = {
            "oid": 17,
            "rolcanlogin": login,
            "rolsuper": False,
            "rolcreatedb": False,
            "rolcreaterole": False,
            "rolreplication": False,
            "rolbypassrls": False,
        }
        self.schema_owner, self.can_set, self.statements = schema_owner, can_set, []
        self.schema_create, self.schema_usage = schema_create, schema_usage

    def is_in_transaction(self):
        return True

    async def fetchrow(self, _query, *_arguments):
        return self.owner

    async def fetchval(self, query, *_arguments):
        if "has_schema_privilege" in query:
            return self.schema_create if "'CREATE'" in query else self.schema_usage
        return self.schema_owner if "nspowner" in query else self.can_set

    async def execute(self, query, *_arguments):
        self.statements.append(query)


@pytest.mark.asyncio
@pytest.mark.parametrize("configuration", [{"login": True}, {"schema_owner": 18}, {"can_set": False}])
async def test_unsafe_source_owner_never_changes_role(configuration):
    connection = OwnerConnection(**configuration)
    with pytest.raises(permissions.RegistryManagementPermissionError):
        await permissions.assume_registry_source_owner(connection, "example_owner", control_schema="example_registry")
    assert connection.statements == []


@pytest.mark.asyncio
async def test_trusted_source_owner_checks_closed_custody_after_scoped_role(monkeypatch):
    connection, checked = OwnerConnection(), []

    async def closed(actual, namespace):
        assert actual is connection and actual.statements == ['SET LOCAL ROLE "example_owner"']
        checked.append(namespace)

    monkeypatch.setattr(permissions, "_require_source_custody", closed)
    await permissions.assume_registry_source_owner(connection, "example_owner", control_schema="example_registry")
    assert checked == ["example_registry"]


def test_optional_owner_mode_keeps_original_edition_and_default(tmp_path):
    edition = _edition(_input())
    arguments = _arguments(tmp_path / "input.csv", edition)
    ordinary = source_import._parser().parse_args(arguments)
    closed = source_import._parser().parse_args(arguments + ["--protected-owner-role", "example_owner"])
    assert ordinary.protected_owner_role is None
    assert closed.protected_owner_role == "example_owner"
    assert source_import._edition(ordinary) == source_import._edition(closed) == edition


@pytest.mark.asyncio
@pytest.mark.parametrize("configuration", [{"can_set": False}, {"schema_create": False}, {"schema_usage": False}])
async def test_read_bridge_requires_existing_native_privileges(configuration):
    connection = OwnerConnection(**configuration)
    with pytest.raises(permissions.RegistryManagementPermissionError):
        await permissions._require_provisioning_operator(
            connection, {"oid": 31, "nspowner": 17}, connection.owner, [{"relowner": 19}], [], "example_api"
        )
    assert connection.statements == []


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


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["closed", "open", "error"])
async def test_source_custody_restores_search_path_on_success_and_refusal(monkeypatch, outcome):
    from process import registry_source_admission_receipt as admission

    previous = '"example_path",public'
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=previous), execute=AsyncMock())
    custody = AsyncMock(return_value={} if outcome == "closed" else None)
    if outcome == "error":
        custody.side_effect = RuntimeError("synthetic catalog failure")
    monkeypatch.setattr(admission, "_custody", custody)
    if outcome == "closed":
        await permissions._require_source_custody(connection, "example_registry")
    else:
        expected = RuntimeError if outcome == "error" else permissions.RegistryManagementPermissionError
        with pytest.raises(expected):
            await permissions._require_source_custody(connection, "example_registry")
    custody.assert_awaited_once_with(connection, "example_registry")
    assert connection.execute.await_args_list == [
        call("SELECT set_config('search_path','pg_catalog,pg_temp',true)"),
        call("SELECT set_config('search_path',$1,true)", previous),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary,message",
    [
        ("transaction", "caller transaction"),
        ("same namespace", "separate namespace"),
        ("missing owner", "existing protected ownership"),
        ("login owner", "existing protected ownership"),
        ("missing namespace", "existing protected ownership"),
        ("native identity", "native source custody identity"),
    ],
)
async def test_provisioning_scope_refuses_unsafe_boundary_before_ddl(boundary, message):
    owner, namespace = OwnerConnection(login=boundary == "login owner").owner, {"oid": 31, "nspowner": 17}
    connection = SimpleNamespace(
        is_in_transaction=lambda: boundary != "transaction",
        fetchrow=AsyncMock(
            side_effect=[
                None if boundary == "missing owner" else owner,
                None if boundary == "missing namespace" else namespace,
            ]
        ),
        fetchval=AsyncMock(return_value=boundary != "native identity"),
        execute=AsyncMock(),
    )
    target = "example_registry" if boundary == "same namespace" else "example_protected_registry"
    with pytest.raises(permissions.RegistryManagementPermissionError, match=message):
        await permissions._provisioning_scope(connection, "example_registry", target, "example_api", "example_owner")
    connection.execute.assert_not_awaited()
    if boundary in {"transaction", "same namespace"}:
        connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("denied", ["database", "API namespace"])
async def test_provisioning_operator_requires_database_and_old_api_read_privileges(denied):
    async def has_privilege(query, *arguments):
        if "has_database_privilege" in query:
            return denied != "database"
        if "has_schema_privilege($1,$2::oid" in query:
            assert arguments == ("example_api", 31)
            return denied != "API namespace"
        return True

    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=has_privilege), execute=AsyncMock())
    message = "create the registry namespace" if denied == "database" else "read the old registry namespace"
    with pytest.raises(permissions.RegistryManagementPermissionError, match=message):
        await permissions._require_provisioning_operator(
            connection, {"oid": 31, "nspowner": 17}, OwnerConnection().owner, [{"relowner": 19}], [], "example_api"
        )
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("readable,writable", [(False, False), (True, True), (True, False)])
async def test_read_bridge_requires_effective_select_without_effective_writes(readable, writable):
    grants = [{"column_grant": False, "grantee": 18, "privilege_type": "SELECT", "is_grantable": False}]
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[18, readable, writable]), fetch=AsyncMock(return_value=grants)
    )
    if readable and not writable:
        await permissions._verify_read_view_grants(connection, 41, "example_api", 17)
    else:
        message = "must be read only" if writable else "reads are unavailable"
        with pytest.raises(permissions.RegistryManagementPermissionError, match=message):
            await permissions._verify_read_view_grants(connection, 41, "example_api", 17)


def _read_bridge_catalog():
    return [
        {
            "oid": 100 + index,
            "relname": name,
            "relkind": "v",
            "relpersistence": "p",
            "relowner": 17,
            "reloptions": ["security_invoker=true"],
            "columns": list(columns),
            "targets": [200 + index],
            "rules": 1,
            "triggers": False,
            "definition": "SELECT " + ", ".join(columns) + f" FROM example_protected_registry.{name};",
        }
        for index, (name, columns) in enumerate(permissions._TABLES.items())
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value,message",
    [
        ("relkind", "r", "relation kind"),
        ("relpersistence", "t", "relation kind"),
        ("relowner", 18, "protected owner"),
        ("reloptions", [], "view options"),
        ("columns", [], "column order"),
        ("targets", [999], "target identity"),
        ("rules", 2, "rules or triggers"),
        ("triggers", True, "rules or triggers"),
        ("definition", "SELECT 1;", "canonical definition"),
    ],
)
async def test_read_bridge_refuses_wrong_native_shape_before_grant_verification(monkeypatch, field, value, message):
    catalog = _read_bridge_catalog()
    target_by_name = {f'"example_protected_registry"."{row["relname"]}"': row["targets"][0] for row in catalog}
    catalog[0][field] = value

    async def native_identity(query, *arguments):
        if "has_schema_privilege" in query:
            return True
        if "FROM pg_roles" in query:
            return 17
        return target_by_name[arguments[0]]

    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=native_identity))
    grants = AsyncMock()
    monkeypatch.setattr(permissions, "_read_view_catalog", AsyncMock(return_value=catalog))
    monkeypatch.setattr(permissions, "_verify_read_view_grants", grants)
    with pytest.raises(permissions.RegistryManagementPermissionError, match=message):
        await permissions._verify_registry_read_views(
            connection, "example_registry", "example_protected_registry", "example_api", "example_owner"
        )
    grants.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_registry_lock_refuses_catalog_identity_change_before_row_scan(monkeypatch):
    tables = [{"relname": "company_registry", "oid": 41}]
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=31), execute=AsyncMock())
    monkeypatch.setattr(permissions, "_catalog", AsyncMock(return_value=([{**tables[0], "oid": 42}], [])))
    with pytest.raises(permissions.RegistryManagementPermissionError, match="objects changed while locking"):
        await permissions._lock_empty_registry(connection, "example_registry", tables, [])
    connection.execute.assert_awaited_once_with(
        'LOCK TABLE "example_registry"."company_registry" IN ACCESS EXCLUSIVE MODE NOWAIT'
    )
    assert connection.fetchval.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_relation", [None, "table", "sequence"])
async def test_registry_move_preserves_standalone_sequence_identity(monkeypatch, changed_relation):
    tables = [{"relname": "company_registry", "oid": 41}]
    sequences = [{"relname": "network_registry_identity_network_id_seq", "oid": 42}]
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=[42, 32]), execute=AsyncMock())
    moved = (
        [{**tables[0], "oid": 99}] if changed_relation == "table" else tables,
        [{**sequences[0], "oid": 99}] if changed_relation == "sequence" else sequences,
    )
    monkeypatch.setattr(permissions, "_catalog", AsyncMock(return_value=moved))
    arguments = (connection, "example_registry", "example_protected_registry", "example_owner", tables, sequences)
    if changed_relation is not None:
        with pytest.raises(permissions.RegistryManagementPermissionError, match="native identities changed"):
            await permissions._move_registry_objects(*arguments)
    else:
        await permissions._move_registry_objects(*arguments)
    assert connection.execute.await_args_list[-1] == call(
        'ALTER SEQUENCE "example_registry"."network_registry_identity_network_id_seq" SET SCHEMA "example_protected_registry"'
    )


@pytest.mark.asyncio
async def test_read_bridge_retirement_rolls_back_when_locked_view_identity_changes(monkeypatch):
    connection = OperatorDatabase()
    connection.fetchval = AsyncMock(return_value=True)
    connection.fetchrow = AsyncMock(return_value=None)
    connection.execute = AsyncMock()
    for name in ("_provisioning_scope", "verify_registry_management_permissions", "_require_source_custody"):
        monkeypatch.setattr(permissions, name, AsyncMock())
    monkeypatch.setattr(
        permissions,
        "_verify_registry_read_views",
        AsyncMock(side_effect=[{"company_registry": 41}, {"company_registry": 42}]),
    )
    with pytest.raises(permissions.RegistryManagementPermissionError, match="views changed while locking"):
        await permissions.retire_registry_read_views(
            connection,
            source_schema="example_registry",
            target_schema="example_protected_registry",
            api_role="example_api",
            owner_role="example_owner",
        )
    connection.execute.assert_awaited_once_with(
        'LOCK TABLE "example_registry"."company_registry" IN ACCESS SHARE MODE NOWAIT'
    )
    from db.registry_aca_source_notification import TABLE

    connection.fetchrow.assert_awaited_once()
    assert connection.fetchrow.await_args.args[1:] == ("example_registry", TABLE)
    assert connection.events == ["begin", "rollback"]
