# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Remaining exact settings consumers preserve ordinary guard contracts."""

import re
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import dialect

from process import provider_directory_cms_receipt_guard as receipt
from process import provider_directory_output_snapshot_journal as journal
from process import provider_directory_profile_initial_guards as initial
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.test_cms_archive_transaction_migration import _load as _load_historical_migration
from tests.test_cms_native_input_named_revision_migration import _migration as _named_revision_migration
from tests.test_provider_directory_output_snapshot_journal import _capability, _plan


def _parse_statements(statements, schema):
    function_by_name = {}
    pattern = re.compile(
        rf'CREATE(?: OR REPLACE)? FUNCTION "{schema}"\."?(\w+)"?\((.*?)\)\s+'
        r"RETURNS\s+(\w+(?:\[\])?)\s+LANGUAGE\s+(\w+)(.*?)AS (\$\w*\$)(.*?)\6",
        re.S,
    )
    for statement in statements:
        compiled = text(statement).compile(dialect=dialect())
        assert not compiled.params
        matches = pattern.findall(str(compiled))
        assert matches
        for name, arguments, returns, language, options, _tag, body in matches:
            args = tuple(tuple(arg.strip().split()) for arg in arguments.split(",")) if arguments.strip() else ()
            settings = re.findall(r"\bSET\s+(\w+)\s*=\s*('[^']*'|[a-z_]+)(?=\s|$)", options)
            assert len(settings) == len(re.findall(r"\bSET\b", options))
            function_by_name[name] = (
                args,
                returns,
                (),
                language,
                "i" if "IMMUTABLE" in options else "s" if "STABLE" in options else "v",
                "STRICT" in options,
                "s" if "PARALLEL SAFE" in options else "u",
                "SECURITY DEFINER" in options,
                [setting + "=" + value.strip("'") for setting, value in settings] or None,
                body,
            )
    return function_by_name


_OLD = ["search_path=pg_catalog"]
_NEW = ["search_path=pg_catalog, pg_temp"]
_BAD = [
    None,
    [],
    ["search_path=pg_temp, pg_catalog"],
    [*_NEW, "TimeZone=UTC"],
    ["search_path=pg_catalog, public, pg_temp"],
]
_EVENTS = {
    "cms_serving_profile_transition": (29, True),
    "cms_serving_no_truncate": (34, False),
    "cms_serving_receipt_immutable": (58, False),
    "cms_serving_receipt_insert": (5, True),
}


def _guard_mapping(name, event, deferred, body, settings):
    return dict(
        name=name,
        event_mask=event,
        enabled="A",
        deferrable=deferred,
        initially_deferred=deferred,
        argument_count=0,
        no_condition=True,
        update_columns="",
        function_schema="example",
        function_name=name,
        function_oid=51,
        function_arguments=0,
        security_definer=False,
        function_settings=deepcopy(settings),
        function_body=body,
        language="plpgsql",
        returns_trigger=True,
    )


def _installed_body(name, body):
    """Apply the named-revision migration to a guard body the original migration created."""
    if name == "cms_serving_profile_transition":
        return receipt.named_profile_transition_body('"example"')
    for old, new, *_count in _named_revision_migration()._replacements("example").get(name, ()):
        body = body.replace(old, new)
    return body


def _receipt_case(monkeypatch, group, settings):
    migration, recorder = _load_historical_migration("20260930100000"), Mock()
    monkeypatch.setattr(migration, "op", recorder)
    for name in (
        "_create_native_transition_guards",
        "_create_truncate_guards",
        "_create_table",
        "_create_insert_guard",
    ):
        getattr(migration, name)('"example"')
    statements = [call.args[0] for call in recorder.execute.call_args_list]
    functions = _parse_statements([sql for sql in statements if sql.startswith("CREATE FUNCTION")], "example")
    names = (
        ("cms_serving_profile_transition", "cms_serving_no_truncate")
        if group == "profile"
        else ("cms_serving_receipt_immutable", "cms_serving_receipt_insert")
    )
    guard_rows = [
        _guard_mapping(name, *_EVENTS[name], _installed_body(name, functions[name][-1]), settings) for name in names
    ]
    database = SimpleNamespace(
        all=AsyncMock(return_value=[SimpleNamespace(_mapping=guard) for guard in guard_rows]),
        scalar=AsyncMock(return_value=False),
    )
    check = receipt.assert_profile_receipt_guards if group == "profile" else receipt.assert_common_receipt_guards
    return database, guard_rows, check


@pytest.mark.parametrize("group", ["profile", "common"])
@pytest.mark.parametrize("settings", [_OLD, _NEW])
async def test_receipt_flows_keep_exact_source_bodies_and_settings(monkeypatch, group, settings):
    database, guards, check = _receipt_case(monkeypatch, group, settings)
    before = deepcopy(guards)
    await check(database, dict(schema_name="example", relation_oid=41))
    assert guards == before
    guards[0]["function_body"] += " RETURN NULL;"
    with pytest.raises(RuntimeError, match="receipt_guard_shape_changed"):
        await check(database, dict(schema_name="example", relation_oid=41))


@pytest.mark.parametrize("group", ["profile", "common"])
@pytest.mark.parametrize("settings", _BAD)
async def test_receipt_settings_near_misses_refuse(monkeypatch, group, settings):
    database, _guards, check = _receipt_case(monkeypatch, group, settings)
    with pytest.raises(RuntimeError, match="receipt_guard_shape_changed"):
        await check(database, dict(schema_name="example", relation_oid=41))


@pytest.mark.parametrize("field,value", [("function_schema", "other"), ("security_definer", True), ("language", "sql")])
async def test_receipt_attributes_remain_required_with_successor(monkeypatch, field, value):
    database, guards, check = _receipt_case(monkeypatch, "common", _NEW)
    guards[0][field] = value
    with pytest.raises(RuntimeError, match="receipt_guard_shape_changed"):
        await check(database, dict(schema_name="example", relation_oid=41))


async def test_initial_guards_outside_transition_keep_original_settings_only():
    expected = initial.initial_serving_guard_expected("example")
    name = next(iter(expected))
    event, deferred, body = expected[name]
    guard = _guard_mapping(name, event, deferred, body, _OLD)
    database = SimpleNamespace(all=AsyncMock(return_value=[SimpleNamespace(_mapping=guard)]))
    relation_by_field = dict(schema_name="example", relation_oid=41)
    await receipt._assert_guards(database, relation_by_field, expected)
    guard["function_settings"] = _NEW
    with pytest.raises(RuntimeError, match="receipt_guard_shape_changed"):
        await receipt._assert_guards(database, relation_by_field, expected)


async def test_initial_match_outside_transition_remains_old_only():
    entry_by_field = dict(
        prosrc=initial._receipt_matches_sql("example"),
        prosecdef=False,
        provolatile="v",
        proconfig=_OLD,
        pronargs=1,
        proargnames=["receipt"],
        returns_boolean=True,
        receipt_argument=True,
        lanname="sql",
    )
    database = SimpleNamespace(all=AsyncMock(return_value=[SimpleNamespace(_mapping=entry_by_field)]))
    assert await initial._assert_match_function(database, "example")
    entry_by_field["proconfig"] = _NEW
    with pytest.raises(RuntimeError, match="receipt_guard_shape_changed"):
        await initial._assert_match_function(database, "example")


def _plan_catalog():
    return dict(
        oid=41,
        relname=journal.PLAN_ROOT,
        relowner=17,
        acl_closed=True,
        readers_closed=True,
        readers_select=True,
        acl=dict(columns=[]),
        topology=dict(
            kind="r",
            persistence="p",
            partition=False,
            rls=False,
            force_rls=False,
            parents=[],
            children=[],
            hooks=1,
            rules=0,
        ),
        columns=[
            dict(
                name=name,
                type=kind,
                type_oid=oid,
                required=True,
                default=None,
                identity="",
                generated="",
                native_collation=True,
            )
            for name, kind, oid in (("singleton", "boolean", 16), ("plan", "jsonb", 3802))
        ],
        constraints=[
            dict(type="p", validated=True, deferrable=False, deferred=False, keys=[1]),
            dict(type="c", validated=True, deferrable=False, deferred=False, definition="CHECK (singleton)"),
        ],
        indexes=[
            dict(
                owner=17,
                method="btree",
                primary=True,
                unique=True,
                valid=True,
                ready=True,
                live=True,
                immediate=True,
                native=True,
                exclusion=False,
                keys_count=1,
                total_count=1,
                keys=["singleton"],
                predicate=None,
                expression=None,
            )
        ],
    )


def _plan_case(monkeypatch):
    capability = _capability()
    capability.policy = GenericSourcePolicy("source_schema", "plan_owner", "plan_publisher", ("reader",))
    native = _plan_catalog()
    native["topology"]["hooks"] = 0
    capability.connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[native]), fetchval=AsyncMock(return_value=_plan())
    )
    monkeypatch.setattr(journal.custody, "_require_capability", AsyncMock())
    return capability, native


async def test_output_journal_reads_native_plan_without_retired_owner_hook(monkeypatch):
    capability, _native = _plan_case(monkeypatch)
    old_plan = _plan()
    old_plan["legacy_extension"] = {"owner_token": {"copied": "opaque"}, "settings": None}
    capability.connection.fetchval.return_value = old_plan
    assert await journal._plan(capability) == old_plan
    capability.connection.fetch.assert_awaited_once_with(
        journal.OUTPUT_CATALOG_SQL, "source_schema", [journal.PLAN_ROOT]
    )


@pytest.mark.parametrize("damage", ["missing", "substituted"])
async def test_output_journal_still_requires_complete_logical_plan(monkeypatch, damage):
    capability, _native = _plan_case(monkeypatch)
    plan = capability.connection.fetchval.return_value
    key = next(iter(plan["tables"]))
    plan["tables"].pop(key)
    if damage == "substituted":
        plan["tables"]["unrelated_table"] = {}
    with pytest.raises(RuntimeError, match="plan_changed"):
        await journal._plan(capability)


@pytest.mark.parametrize("damage", ["column", "primary_key", "check"])
async def test_output_journal_keeps_native_plan_layout_validation(monkeypatch, damage):
    capability, native = _plan_case(monkeypatch)
    if damage == "column":
        native["columns"][0]["type"] = "text"
    elif damage == "primary_key":
        native["indexes"][0]["primary"] = False
    else:
        native["constraints"][1]["definition"] = "CHECK (true)"
    with pytest.raises(RuntimeError, match="layout_changed"):
        await journal._plan(capability)
