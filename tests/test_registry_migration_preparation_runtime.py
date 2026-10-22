# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Operator ordering, refusal and receipt contracts without database access."""

import json
import logging
import runpy
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process import registry_migration_preparation as preparation

SCHEMA = "registry_control"
OWNER = "registry_owner"
CALLER = '"maintenance role"'
RECEIPT = {
    "component": "registry_migration_preparation",
    "prepared_revisions": [
        "20261016070000_registry_allowed_plan_scope",
        "20261017010000_registry_cms_source_notifications",
        "20261017030000_registry_ptg_office_recipe_selection",
        "20261018011000_network_candidate_custody",
        "20261018040000_registry_aca_source_notifications",
    ],
}
ACL = [(101, "registry_network_binding", 201, "owner=arwdDxt/owner", ["binding_id", "revision"])]


def _migration_fixture(monkeypatch, *, after=None, custody_failure=False):
    events = []
    manifests = iter((ACL, ACL if after is None else after))

    def execute(statement, arguments):
        assert arguments == {"schema": SCHEMA}
        assert "a.attname<>'office_recipe_json'" in str(statement)
        assert "c.oid,c.relname,c.relowner,c.relacl::text" in str(statement)
        events.append(("acl",))
        return next(manifests)

    connection = SimpleNamespace(execute=execute, exec_driver_sql=lambda sql: events.append(("ddl", sql)))

    def migration(revision):
        def ddl(schema, **arguments):
            events.append(("definition", revision, schema, arguments))
            return [revision + ":table", revision + ":index", revision + ":extra"]

        return SimpleNamespace(
            _ddl=ddl,
            _create=lambda connection, schema: events.append(("create", revision, connection, schema)),
            _postimage=lambda schema: [revision + ":postimage:" + schema],
        )

    def validate(actual_connection, schema, table, expected):
        assert actual_connection is connection and schema == SCHEMA
        events.append(("validate", table, expected("pg_temp")))

    def custody(actual_connection, schema):
        assert actual_connection is connection and schema == SCHEMA
        events.append(("custody",))
        if custody_failure:
            raise RuntimeError("synthetic custody preparation refusal")

    monkeypatch.setattr(preparation, "registry_migration", migration)
    monkeypatch.setattr(preparation, "validate_prepared_registry_table", validate)
    monkeypatch.setattr(preparation, "prepare_candidate_custody_table", custody)
    return connection, events


def test_preparation_validates_incumbents_before_ddl_and_preserves_acl_receipt(monkeypatch):
    connection, events = _migration_fixture(monkeypatch)
    assert preparation._prepare(connection, SCHEMA) == RECEIPT
    validations = [event for event in events if event[0] == "validate"]
    assert [event[1] for event in validations] == [
        "registry_ptg_published_plan_scope",
        "registry_network_binding",
        *[table for _revision, table in preparation._CHANGES if table != "network_candidate_custody"],
    ]
    assert [len(event[2]) for event in validations[:2]] == [1, 2]
    first_ddl = next(index for index, event in enumerate(events) if event[0] == "ddl")
    assert events.index(validations[1]) < first_ddl
    assert [event[1] for event in events if event[0] == "ddl"] == [
        revision + suffix
        for revision, table in preparation._CHANGES
        if table not in {"registry_aca_source_notification", "network_candidate_custody"}
        for suffix in (":table", ":index", ":extra")
    ]
    assert [event[3] for event in events if event[0] == "definition" and event[2] == SCHEMA] == [
        {"include_allowed": True},
        {},
        {},
    ]
    assert [event for event in events if event[0] == "create"] == [
        ("create", preparation._CHANGES[-1][0], connection, SCHEMA)
    ]
    custody_index = events.index(("custody",))
    assert events.count(("custody",)) == 1
    assert (
        events.index(validations[-2])
        < custody_index
        < events.index(("create", preparation._CHANGES[-1][0], connection, SCHEMA))
    )
    assert events[0] == events[-1] == ("acl",)


@pytest.mark.asyncio
async def test_custody_preparation_refusal_rolls_back_before_later_changes_or_success_receipt(monkeypatch):
    original_prepare = preparation._prepare
    connection, events = _migration_fixture(monkeypatch, custody_failure=True)
    runtime, native_events = _runtime_connection(monkeypatch)

    def prepare(actual_connection, schema):
        assert actual_connection is runtime and schema == SCHEMA
        return original_prepare(connection, schema)

    monkeypatch.setattr(preparation, "_prepare", prepare)
    with pytest.raises(RuntimeError, match="synthetic custody preparation refusal"):
        await preparation.prepare_registry_migrations(runtime, owner_role=OWNER, control_schema=SCHEMA)
    assert events[-1] == ("custody",)
    assert [event for event in events if event[0] == "create"] == []
    assert all("registry_aca_source_notification" not in str(event) for event in events)
    assert events.count(("acl",)) == 1
    assert native_events[-1] == "rollback"
    assert "release" not in native_events and f"SET LOCAL ROLE {CALLER}" not in native_events


@pytest.mark.parametrize(
    "column,replacement", [(0, 999), (2, 999), (3, "changed grant"), (4, ["changed column grant"])]
)
def test_preparation_refuses_changed_identity_owner_or_existing_grants(monkeypatch, column, replacement):
    changed_acl_fields = list(ACL[0])
    changed_acl_fields[column] = replacement
    connection, _events = _migration_fixture(monkeypatch, after=[tuple(changed_acl_fields)])
    with pytest.raises(RuntimeError, match="changed existing identities or privileges"):
        preparation._prepare(connection, SCHEMA)


def _runtime_observation(step, failure, sql, arguments):
    if sql == "SELECT quote_ident(current_user)":
        step("caller")
        return CALLER
    if "pg_try_advisory_xact_lock" in sql:
        assert arguments == (preparation._fence_key(SCHEMA),)
        step("company")
        return failure != "company_busy"
    if "registry_revision_control" in sql:
        assert sql.endswith("FOR UPDATE NOWAIT")
        step("revision")
        return 1
    assert "state IN ('queued','running')" in sql
    step("requests")
    return failure == "requests_active"


def _runtime_connection(monkeypatch, *, failure=None, in_transaction=True):
    """Model native lock order and savepoint outcomes without a live connection."""
    events = []

    def step(name):
        events.append(name)
        if failure == name:
            raise RuntimeError("synthetic " + name)

    async def fetchval(sql, *arguments):
        return _runtime_observation(step, failure, sql, arguments)

    async def execute(sql):
        step(sql)

    driver = SimpleNamespace(fetchval=fetchval, execute=execute)

    @asynccontextmanager
    async def nested():
        step("savepoint")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("release")

    async def raw():
        step("raw")
        return SimpleNamespace(driver_connection=driver)

    async def owner(actual_driver, owner_role, *, control_schema):
        assert actual_driver is driver and (owner_role, control_schema) == (OWNER, SCHEMA)
        step("owner")

    def prepare(actual_connection, schema):
        assert actual_connection is connection and schema == SCHEMA
        step("prepare")
        return RECEIPT

    async def run_sync(function, *arguments):
        return function(connection, *arguments)

    connection = SimpleNamespace(
        in_transaction=lambda: in_transaction,
        begin_nested=nested,
        get_raw_connection=raw,
        run_sync=run_sync,
    )
    monkeypatch.setattr(preparation, "assume_registry_source_owner", owner)
    monkeypatch.setattr(preparation, "_prepare", prepare)
    return connection, events


@pytest.mark.asyncio
async def test_runtime_owns_company_fence_before_revision_and_drains_before_ddl(monkeypatch):
    connection, events = _runtime_connection(monkeypatch)
    assert await preparation.prepare_registry_migrations(connection, owner_role=OWNER, control_schema=SCHEMA) is RECEIPT
    assert events == [
        "savepoint",
        "raw",
        "caller",
        "owner",
        "company",
        "revision",
        *[
            f'LOCK TABLE "{SCHEMA}"."{table}" IN SHARE MODE NOWAIT'
            for table in ("registry_publication_request", *preparation._SOURCE_TABLES)
        ],
        "requests",
        *[f'LOCK TABLE "{SCHEMA}"."{table}" IN ACCESS EXCLUSIVE MODE NOWAIT' for table in preparation._EXISTING],
        "prepare",
        f"SET LOCAL ROLE {CALLER}",
        "release",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,match",
    [
        ("owner", "synthetic owner"),
        ("company_busy", "company approval is active"),
        ("revision", "synthetic revision"),
        (f'LOCK TABLE "{SCHEMA}"."registry_source_snapshot" IN SHARE MODE NOWAIT', "synthetic LOCK"),
        ("requests_active", "requires drained publication and office requests"),
        (f'LOCK TABLE "{SCHEMA}"."registry_network_binding" IN ACCESS EXCLUSIVE MODE NOWAIT', "synthetic LOCK"),
        ("prepare", "synthetic prepare"),
    ],
)
async def test_runtime_refusal_rolls_back_savepoint_without_running_later_ddl(monkeypatch, failure, match):
    connection, events = _runtime_connection(monkeypatch, failure=failure)
    with pytest.raises(RuntimeError, match=match):
        await preparation.prepare_registry_migrations(connection, owner_role=OWNER, control_schema=SCHEMA)
    assert events[-1] == "rollback"
    assert "release" not in events and f"SET LOCAL ROLE {CALLER}" not in events
    if failure != "prepare":
        assert "prepare" not in events


@pytest.mark.asyncio
@pytest.mark.parametrize("in_transaction,schema", [(False, SCHEMA), (True, "invalid-schema")])
async def test_runtime_refuses_missing_transaction_or_unsafe_namespace_before_native_work(
    monkeypatch, in_transaction, schema
):
    connection, events = _runtime_connection(monkeypatch, in_transaction=in_transaction)
    with pytest.raises(ValueError):
        await preparation.prepare_registry_migrations(connection, owner_role=OWNER, control_schema=schema)
    assert events == []


def _cli_database(monkeypatch, failure):
    import db.connection

    events = []
    private_detail = "synthetic private connection detail"

    def step(name):
        events.append(name)
        print(private_detail)
        print(private_detail, file=sys.stderr)
        if failure == name:
            raise RuntimeError(private_detail)

    async def execute(statement):
        events.append(str(statement))

    connection = SimpleNamespace(execute=execute)

    @asynccontextmanager
    async def begin():
        step("begin")
        try:
            yield connection
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    async def connect():
        step("connect")

    async def disconnect():
        step("disconnect")

    database = SimpleNamespace(engine=SimpleNamespace(begin=begin, echo=True), connect=connect, disconnect=disconnect)

    async def prepare(actual_connection, **arguments):
        assert actual_connection is connection
        assert arguments == {"owner_role": OWNER, "control_schema": SCHEMA}
        assert database.engine.echo is False
        step("prepare")
        return RECEIPT

    monkeypatch.setattr(db.connection, "Database", lambda: database)
    monkeypatch.setattr(preparation, "prepare_registry_migrations", prepare)
    return database, events, private_detail


@pytest.mark.parametrize("failure", [None, "connect", "begin", "prepare", "disconnect"])
def test_cli_uses_bounded_transaction_cleans_up_and_redacts_database_output(monkeypatch, capsys, failure):
    database, events, private_detail = _cli_database(monkeypatch, failure)
    previous_logging_disable = logging.root.manager.disable
    status = preparation.run_command(["--control-schema", SCHEMA, "--owner-role", OWNER])
    output = capsys.readouterr()
    assert status == (0 if failure is None else 1)
    assert events[0] == "connect" and events[-1] == "disconnect"
    assert database.engine.echo is True
    assert logging.root.manager.disable == previous_logging_disable
    assert private_detail not in output.out + output.err
    if failure is None:
        assert output.err == ""
        assert json.loads(output.out) == {"status": "ok", **RECEIPT}
        assert events[1:-1] == [
            "begin",
            "SET LOCAL statement_timeout='3s'",
            "SET LOCAL lock_timeout='500ms'",
            "prepare",
            "commit",
        ]
    else:
        assert output.out == ""
        assert json.loads(output.err) == {"status": "error", "code": "registry_preparation_failed"}
        if failure == "prepare":
            assert "rollback" in events and "commit" not in events


def test_cli_invalid_arguments_do_not_open_database_or_echo_rejected_values(monkeypatch, capsys):
    import db.connection

    database = Mock(side_effect=AssertionError("must not open credentials"))
    monkeypatch.setattr(db.connection, "Database", database)
    with pytest.raises(SystemExit) as stopped:
        preparation.run_command(["--control-schema", SCHEMA, "--owner", "synthetic-rejected-value"])
    assert stopped.value.code == 2
    database.assert_not_called()
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err) == {"status": "error", "code": "invalid_arguments"}


def test_module_entrypoint_returns_fixed_error_and_disconnects_after_connect_failure(monkeypatch, capsys):
    _database, events, private_detail = _cli_database(monkeypatch, "connect")
    monkeypatch.setattr(sys, "argv", [preparation.__file__, "--control-schema", SCHEMA, "--owner-role", OWNER])
    with pytest.raises(SystemExit) as stopped:
        runpy.run_path(preparation.__file__, run_name="__main__")
    assert stopped.value.code == 1 and events == ["connect", "disconnect"]
    output = capsys.readouterr()
    assert output.out == "" and private_detail not in output.err
    assert json.loads(output.err) == {"status": "error", "code": "registry_preparation_failed"}
