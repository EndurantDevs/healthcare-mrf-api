# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Administrative transition scope and rollback checks; native SQL is tested separately."""

from contextlib import asynccontextmanager
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from process import cms_native_input_metadata_transition as transition


def options():
    return dict(
        ledger_schema="source_schema",
        retained_qname=("archive_schema", "revision_history"),
        owner_roles={"ledger": "ledger_owner", "archive": "archive_owner"},
        counter_role="counter_writer",
        writer_roles=("source_writer",),
        reader_roles=("source_reader",),
        assert_idle=AsyncMock(),
    )


class Connection:
    def __init__(self):
        self.active = True
        self.actor = dict(session="test_admin", current="test_admin", native_admin=True)
        self.state = dict(canonical="legacy", retained=False)
        self.events = []

    def is_in_transaction(self):
        return self.active

    async def fetchrow(self, sql, *args):
        assert "session_user" in sql
        return self.actor

    async def fetchval(self, sql, *args):
        self.events.append((sql, args))
        if sql.startswith("SHOW "):
            return "17ms"
        if "SELECT to_regclass" in sql:
            return self.state["retained"]
        if "pg_trigger" in sql:
            return False
        raise AssertionError(sql)

    async def execute(self, sql, *args):
        self.events.append((sql, args))

    @asynccontextmanager
    async def transaction(self):
        before = deepcopy(self.state)
        self.events.append(("savepoint", ()))
        try:
            yield
            self.events.append(("release", ()))
        except BaseException:
            self.state = before
            self.events.append(("rollback", ()))
            raise


@pytest.fixture
def prepared_case(monkeypatch):
    connection = Connection()
    legacy = (
        ("relation_oid", "bigint"),
        ("schema_name", "name"),
        ("table_name", "name"),
        ("revision", "bigint"),
        ("mutation_xid", "xid8"),
    )
    named = legacy[1:]

    async def columns(_connection, relation):
        return legacy if relation.endswith('"revision_history"') or connection.state["canonical"] == "legacy" else named

    async def relocate(*args):
        connection.state.update(canonical="named", retained=True)

    monkeypatch.setattr(transition, "_columns", columns)
    monkeypatch.setattr(transition, "_legacy_projection", AsyncMock(return_value=dict(raw_rows=3, named_rows=2)))
    monkeypatch.setattr(transition, "_guard_definitions", AsyncMock(return_value=("row_guard", "truncate_guard")))
    monkeypatch.setattr(transition, "_rewrite_existing_functions", AsyncMock())
    monkeypatch.setattr(transition, "_relocate_ledger", relocate)
    for name in ("_close_configured_access", "_relocate_guards", "_verify_named_projection"):
        monkeypatch.setattr(transition, name, AsyncMock())
    return connection


async def test_preparation_and_replay_preserve_explicit_names_and_timeout(prepared_case):
    connection = prepared_case
    arguments = options()
    first = await transition.prepare_cms_native_input_metadata_transition(connection, **arguments)
    assert first == dict(
        status="prepared",
        ledger_qname=("source_schema", "cms_native_input_revision"),
        retained_qname=arguments["retained_qname"],
        raw_rows=3,
        named_rows=2,
    )
    assert arguments["assert_idle"].await_count == 3
    assert connection.events[-2] == ("SELECT set_config('lock_timeout',$1,true)", ("17ms",))
    replay = await transition.prepare_cms_native_input_metadata_transition(connection, **arguments)
    assert replay == {**first, "status": "already_prepared"}
    transition._rewrite_existing_functions.assert_awaited_once()
    assert transition._close_configured_access.await_count == 2
    transition._relocate_guards.assert_awaited_once()
    assert transition._verify_named_projection.await_count == 2


@pytest.mark.parametrize("fault", ["transaction", "actor", "native_admin", "role", "idle", "writers", "retained"])
async def test_explicit_admin_transaction_and_configuration_are_required(prepared_case, fault):
    connection = prepared_case
    arguments = options()
    match fault:
        case "transaction":
            connection.active = False
        case "actor":
            connection.actor["current"] = "another_admin"
        case "native_admin":
            connection.actor["native_admin"] = False
        case "role":
            connection.actor.update(session="source_writer", current="source_writer")
        case "idle":
            arguments["assert_idle"] = None
        case "writers":
            arguments["writer_roles"] = ()
        case "retained":
            arguments["retained_qname"] = ("source_schema", "cms_native_input_revision")
    with pytest.raises(RuntimeError):
        await transition.prepare_cms_native_input_metadata_transition(connection, **arguments)
    assert connection.state == dict(canonical="legacy", retained=False)
    transition._rewrite_existing_functions.assert_not_awaited()


@pytest.mark.parametrize("failure", ["idle", "assertion", "sql"])
async def test_caller_caught_failure_rolls_back_the_nested_preparation(prepared_case, failure):
    connection = prepared_case
    arguments = options()
    if failure == "idle":
        arguments["assert_idle"].side_effect = [None, None, RuntimeError("busy")]
    elif failure == "assertion":
        transition._verify_named_projection.side_effect = AssertionError("postcondition")
    else:
        transition._relocate_guards.side_effect = RuntimeError("native SQL refusal")
    with pytest.raises((RuntimeError, AssertionError)):
        await transition.prepare_cms_native_input_metadata_transition(connection, **arguments)
    assert connection.state == dict(canonical="legacy", retained=False)
    assert connection.events[-1][0] == "rollback"
    assert connection.is_in_transaction()


@pytest.mark.parametrize("fault", [None, "invalid", "divergent"])
async def test_only_complete_equivalent_logical_rows_can_project(fault):
    counts_by_field = dict(raw_rows=3, named_rows=2, invalid=fault == "invalid", divergent=fault == "divergent")
    connection = type("Driver", (), {"fetchrow": AsyncMock(return_value=counts_by_field)})()
    if fault:
        with pytest.raises(RuntimeError, match="invalid|diverge"):
            await transition._legacy_projection(connection, '"source_schema"."cms_native_input_revision"')
    else:
        assert (
            await transition._legacy_projection(connection, '"source_schema"."cms_native_input_revision"')
            == counts_by_field
        )
    sql = connection.fetchrow.await_args.args[0]
    assert "SELECT DISTINCT schema_name,table_name,revision,mutation_xid" in sql
    assert "relation_oid" not in sql and "max(" not in sql.lower()


@pytest.mark.parametrize("fault", [None, "body", "definition"])
async def test_existing_function_rewrite_preserves_definition_except_exact_body(fault):
    schema = "source_schema"
    bodies = [transition.inputs._advance_body(schema), transition.inputs._ledger_guard_body()]
    previous = [
        bodies[0].replace(
            "WHERE schema_name=TG_TABLE_SCHEMA AND table_name=TG_TABLE_NAME;", "WHERE relation_oid=TG_RELID::bigint;"
        ),
        bodies[1].replace(
            "AND NEW.schema_name=OLD.schema_name",
            "AND NEW.relation_oid=OLD.relation_oid AND NEW.schema_name=OLD.schema_name",
        ),
    ]
    definitions = [
        f"CREATE OR REPLACE FUNCTION fixed() RETURNS trigger LANGUAGE plpgsql AS $body${body}$body$"
        for body in previous
    ]
    function_rows = [
        dict(prosrc=previous[0], definition=definitions[0]),
        dict(prosrc=previous[1], definition=definitions[1]),
    ]
    if fault == "body":
        function_rows[0]["prosrc"] += " changed"
    elif fault == "definition":
        function_rows[0]["definition"] += previous[0]
    connection = type("Driver", (), {"fetchrow": AsyncMock(side_effect=function_rows), "execute": AsyncMock()})()
    if fault:
        with pytest.raises(RuntimeError, match="function_.*changed"):
            await transition._rewrite_existing_functions(connection, schema)
        connection.execute.assert_not_awaited()
    else:
        await transition._rewrite_existing_functions(connection, schema)
        assert [call.args[0] for call in connection.execute.await_args_list] == [
            definition.replace(old, new) for definition, old, new in zip(definitions, previous, bodies, strict=True)
        ]


async def test_equivalent_duplicate_preparation_is_required_before_any_relocation(prepared_case):
    transition._legacy_projection.return_value = dict(raw_rows=2, named_rows=2)
    with pytest.raises(RuntimeError, match="duplicates_required"):
        await transition.prepare_cms_native_input_metadata_transition(prepared_case, **options())
    assert prepared_case.state == dict(canonical="legacy", retained=False)
    transition._rewrite_existing_functions.assert_not_awaited()
    transition._close_configured_access.assert_not_awaited()
