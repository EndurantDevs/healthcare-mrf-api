# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite source settings and settings-only installer checks without a database."""

import asyncio
import re
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_dependency_policy as dependencies
from process import provider_directory_cms_owner_execution as execution
from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_cms_routine_settings as settings
from process import provider_directory_cms_transition_routines as transition
from tests.test_acquisition_validator_argument_types_migration import _current_functions, _migration
from tests.test_provider_directory_cms_allocation_guard_contracts import original_guards as original_guards
from tests.test_provider_directory_cms_callee_custody import _catalog_definition, _parse_statements
from tests.test_provider_directory_cms_transition_routines import _extra_rows, _native_foreign, _raw
from tests.test_provider_directory_source_callable_custody import _entry
from tests.test_source_validator_argument_types_migration import _load

_SCHEMA = "snapshot_schema"
_PREPARATION = "preparation_schema"


@pytest.mark.parametrize(
    "legacy,target",
    [
        (None, None),
        (["search_path=pg_catalog"], ["search_path=pg_catalog, pg_temp"]),
        (["search_path=pg_catalog", "TimeZone=UTC"], ["search_path=pg_catalog, pg_temp", "TimeZone=UTC"]),
        (["search_path=pg_catalog, snapshot_schema"], ["search_path=pg_catalog, snapshot_schema, pg_temp"]),
        (['search_path=pg_catalog, "Mixed_Schema"'], ['search_path=pg_catalog, "Mixed_Schema", pg_temp']),
        (['search_path=pg_catalog, "select"'], ['search_path=pg_catalog, "select", pg_temp']),
    ],
)
def test_exact_source_settings_are_detached(legacy, target):
    before = deepcopy(legacy)
    old, new = settings.approved_cms_routine_settings(legacy)
    assert old == legacy and new == target and legacy == before
    if old is not None:
        old.append("changed")
        new.append("changed")
        assert legacy == before


@pytest.mark.parametrize(
    "legacy",
    [
        [],
        (),
        "search_path=pg_catalog",
        [None],
        ["search_path=pg_temp"],
        ["search_path=pg_catalog, pg_temp"],
        ["search_path=pg_catalog", "timezone=UTC"],
        ["TimeZone=UTC", "search_path=pg_catalog"],
        ["search_path=pg_catalog", "TimeZone=UTC", "jit=off"],
        ["search_path=pg_catalog, public, pg_temp"],
        ["search_path=pg_catalog, public;RESET ALL"],
        ["search_path=pg_catalog, Mixed_Schema"],
        ['search_path=pg_catalog, "bad-name"'],
    ],
)
def test_unknown_or_observed_settings_cannot_become_expectations(legacy):
    with pytest.raises(ValueError, match="source_settings_required"):
        settings.approved_cms_routine_settings(legacy)


def _ptg_rows():
    rows = []
    for oid, (name, kind) in enumerate(
        (("ptg_wave_json_ascii_text_v1", "text"), ("ptg_wave_canonical_json_ascii_v1", "jsonb")), 8000
    ):
        body = _current_functions()[name, kind][-1]
        for caller, old, new, _count in _migration()._calls(_SCHEMA):
            if caller == name:
                body = body.replace(old, new)
        entry = _entry(name + "(" + kind + ")")
        entry["definition"].update(oid=str(oid), prosrc=body, proconfig=["search_path=pg_catalog, " + _SCHEMA])
        row = _raw(entry)
        row["native"]["namespace_identifier"] = _SCHEMA
        rows.append(row)
    return rows


def _null_profile_row(monkeypatch):
    migration = _load("20260730110000_provider_directory_profile_delta.py")
    statements = []
    monkeypatch.setattr(
        migration,
        "op",
        SimpleNamespace(
            create_table=lambda *args, **kwargs: None,
            create_index=lambda *args, **kwargs: None,
            execute=statements.append,
        ),
    )
    migration._create_delta_receipt_table(_SCHEMA)
    declaration = next(statement for statement in statements if "CREATE FUNCTION" in statement)
    name = "provider_directory_profile_delta_receipt_immutable_v1"
    spec = _parse_statements([declaration], _SCHEMA)[name]
    return _raw(dict(definition=_catalog_definition(name, spec, 105, 8002), language=spec[3]))


def _declaration(row):
    native = row["native"]
    definition = native["definition"]
    lines = ["CREATE OR REPLACE FUNCTION fixed_function()\n", " RETURNS boolean\n", " LANGUAGE sql\n"]
    for item in definition["proconfig"] or []:
        if item.startswith("search_path="):
            path = item.removeprefix("search_path=").split(", ")
            lines.append(" SET search_path TO " + ", ".join("'" + part.strip('"') + "'" for part in path) + "\n")
        else:
            assert item == "TimeZone=UTC"
            lines.append(" SET \"TimeZone\" TO 'UTC'\n")
    return "".join(lines) + "AS $function$" + definition["prosrc"] + "$function$\n"


@pytest.fixture
def routines(original_guards, monkeypatch):
    entries = [_raw(row) for row in original_guards]
    entries.extend(
        [*_extra_rows(monkeypatch), *_ptg_rows(), _null_profile_row(monkeypatch), _native_foreign("RI_FKey_check_ins")]
    )
    for entry in entries:
        entry["native"].update(body=_declaration(entry), dependencies=[], acl=[[105, 105, "EXECUTE", False]])
    return sorted(entries, key=lambda entry: entry["oid"])


def _target_rows(routines):
    expected = deepcopy(routines)
    for row in expected:
        native = row["native"]
        old = native["definition"]["proconfig"]
        if native["namespace"][1] == "pg_catalog" or old is None or old[0].endswith(", pg_temp"):
            continue
        native["definition"]["proconfig"] = [old[0] + ", pg_temp", *old[1:]]
        header, delimiter, body = native["body"].partition("\nAS ")
        lines = header.splitlines()
        for index, line in enumerate(lines):
            if line.startswith(" SET search_path TO "):
                lines[index] = line + ", 'pg_temp'"
        native["body"] = "\n".join(lines) + delimiter + body
    return expected


def _terminal(routines):
    return settings.require_cms_terminal_routine_settings(
        routines, catalog_schema=_SCHEMA, catalog_schema_oid=42, preparation_schema=_PREPARATION
    )


def test_complete_source_subset_keeps_native_and_null_exceptions(routines):
    before = deepcopy(routines)
    with pytest.raises(ValueError, match="terminal_settings_required"):
        _terminal(routines)
    target = _target_rows(routines)
    assert _terminal(target) is None and routines == before
    assert [row for row in routines if row["native"]["definition"]["proconfig"] is None]
    assert [row for row in routines if row["native"]["namespace"][1] == "pg_catalog"]


@pytest.mark.parametrize("fault", ["prosrc", "proconfig", "proargtypes", "namespace", "missing_native"])
def test_target_settings_do_not_replace_source_definition_checks(routines, fault):
    rows = _target_rows(routines)
    selected = rows[0]["native"]
    if fault == "namespace":
        selected["namespace"][0] = 99
    elif fault == "missing_native":
        rows[-1]["native"]["definition"]["proconfig"] = ["search_path=pg_catalog, pg_temp"]
    else:
        selected["definition"][fault] = "changed"
    with pytest.raises(ValueError):
        _terminal(rows)


@pytest.mark.parametrize(
    "legacy,target",
    [
        (["search_path=pg_catalog"], " SET search_path TO 'pg_catalog', 'pg_temp'\n"),
        (['search_path=pg_catalog, "Mixed_Schema"'], " SET search_path TO 'pg_catalog', 'Mixed_Schema', 'pg_temp'\n"),
    ],
)
def test_pg18_declaration_changes_only_the_header(legacy, target):
    old, new = settings.approved_cms_routine_settings(legacy)
    original_line = target.replace(", 'pg_temp'", "")
    declaration = (
        "CREATE OR REPLACE FUNCTION fixed()\n" + original_line + "AS $function$\n" + original_line + "$function$\n"
    )
    assert (
        settings._declaration_after(declaration, old, new)
        == "CREATE OR REPLACE FUNCTION fixed()\n" + target + "AS $function$\n" + original_line + "$function$\n"
    )


@pytest.mark.parametrize("damage", ["missing", "duplicate", "body_only", "unknown"])
def test_pg18_declaration_unknown_header_refuses(damage):
    old, new = settings.approved_cms_routine_settings(["search_path=pg_catalog"])
    line = " SET search_path TO 'pg_catalog'\n"
    header = "CREATE OR REPLACE FUNCTION fixed()\n"
    if damage == "duplicate":
        header += line * 2
    elif damage == "unknown":
        header += " SET search_path TO 'public'\n"
    body = "AS $function$" + (line if damage == "body_only" else "") + "$function$\n"
    with pytest.raises(ValueError, match="declaration_settings_changed"):
        settings._declaration_after(header + body, old, new)


def _admin_actor():
    return dict(
        session="administrator",
        current="administrator",
        administrator=True,
        backend=91,
        transaction="1/1",
        readonly="off",
        replication="origin",
        search_path="pg_catalog, pg_temp",
        database_oid=1,
        isolation="serializable",
    )


@pytest.fixture
def installation(routines):
    state = SimpleNamespace(
        rows=deepcopy(routines),
        actor=_admin_actor(),
        ddl=True,
        callback=None,
        fail=False,
        comment="preserved comment",
        hooks=[],
    )

    async def fetch(query, *args):
        if query == dependencies._ROUTINES_SQL:
            return deepcopy(state.rows)
        assert query == settings._STABILITY_SQL
        return [dict(oid=oid, description=state.comment, hooks=deepcopy(state.hooks)) for oid in args[0]]

    async def fetchval(query, *args):
        if query == "SHOW search_path":
            return state.actor["search_path"]
        if query == execution._DDL_HOOKS_SQL:
            return state.ddl
        assert "array_agg(oid ORDER BY oid)" in query
        return [1, 2, 3, 4, 5, 6]

    async def execute(command):
        if command.startswith("LOCK TABLE"):
            return
        names = re.findall(r'ALTER FUNCTION "snapshot_schema"\."(\w+)"\(', command)
        assert names and all("OWNER TO" not in command for _ in names)
        target_by_name = {row["native"]["definition"]["proname"]: row for row in _target_rows(state.rows)}
        for name in names:
            position = next(
                index for index, row in enumerate(state.rows) if row["native"]["definition"]["proname"] == name
            )
            state.rows[position] = target_by_name[name]
            if state.fail:
                raise RuntimeError("after one real-settings stand-in")
        if state.callback is not None:
            state.callback()

    connection = SimpleNamespace(
        fetch=AsyncMock(side_effect=fetch),
        fetchrow=AsyncMock(side_effect=lambda *args: deepcopy(state.actor)),
        fetchval=AsyncMock(side_effect=fetchval),
        execute=AsyncMock(side_effect=execute),
        is_in_transaction=Mock(return_value=True),
        is_closed=Mock(return_value=False),
        get_server_pid=Mock(return_value=91),
    )
    return SimpleNamespace(connection=connection, state=state, pins=routines)


async def _install(case, *, deadline=None):
    return await settings.install_cms_routine_settings(
        case.connection,
        expected_routines=case.pins,
        catalog_schema=_SCHEMA,
        catalog_schema_oid=42,
        preparation_schema=_PREPARATION,
        deadline=asyncio.get_running_loop().time() + 3 if deadline is None else deadline,
    )


@pytest.mark.asyncio
async def test_installer_compares_full_pins_and_returns_detached_exact_postimage(installation):
    case = installation
    before = deepcopy(case.pins)
    result = await _install(case)
    assert result == case.state.rows == _target_rows(before) and case.pins == before
    assert _terminal(result) is None
    result[0]["native"]["definition"]["proowner"] = "changed"
    assert result != case.state.rows
    assert not hasattr(case.connection, "commit") and not hasattr(case.connection, "transaction")


@pytest.mark.asyncio
async def test_already_hardened_pins_do_not_repeat_any_alter(installation):
    case = installation
    case.pins = case.state.rows = _target_rows(case.pins)
    assert await _install(case) == case.pins
    assert all(not call.args[0].startswith("ALTER FUNCTION") for call in case.connection.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline", [0, True, float("inf"), float("nan")])
async def test_bad_deadline_refuses_before_native_calls(installation, deadline):
    case = installation
    with pytest.raises(ValueError, match="deadline_required"):
        await _install(case, deadline=deadline)
    case.connection.fetch.assert_not_awaited()
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("administrator", False),
        ("current", "other"),
        ("backend", 92),
        ("readonly", "on"),
        ("replication", "replica"),
        ("search_path", "public"),
    ],
)
async def test_wrong_original_admin_context_refuses(installation, field, value):
    case = installation
    case.state.actor[field] = value
    with pytest.raises(ValueError):
        await _install(case)
    assert not any(call.args[0].startswith("ALTER FUNCTION") for call in case.connection.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["stale", "ddl", "duplicate_pin", "missing_pin", "unknown_settings"])
async def test_all_preimages_and_source_guards_precede_mutation(installation, fault):
    case = installation
    if fault == "stale":
        case.state.rows[0]["native"]["definition"]["prosrc"] += "changed"
    elif fault == "ddl":
        case.state.ddl = False
    elif fault == "duplicate_pin":
        case.pins.append(deepcopy(case.pins[0]))
    elif fault == "missing_pin":
        case.pins.pop()
    else:
        case.pins[0]["native"]["definition"]["proconfig"] = ["search_path=public"]
    with pytest.raises(ValueError):
        await _install(case)
    assert not any(call.args[0].startswith("ALTER FUNCTION") for call in case.connection.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["prosrc", "proowner", "proacl", "proconfig"])
async def test_complete_native_postimage_rejects_unplanned_changes(installation, field):
    case = installation
    case.state.callback = lambda: case.state.rows[0]["native"]["definition"].update({field: "changed"})
    with pytest.raises(ValueError, match="settings_postimage_changed"):
        await _install(case)


@pytest.mark.asyncio
async def test_partial_failure_propagates_without_commit_or_retry(installation):
    case = installation
    case.state.fail = True
    with pytest.raises(RuntimeError, match="after one real-settings stand-in"):
        await _install(case)
    assert case.state.rows != case.pins
    assert case.connection.is_in_transaction() is True
    assert sum(call.args[0].startswith("ALTER FUNCTION") for call in case.connection.execute.await_args_list) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("comment", "changed"), ("hooks", [{"oid": "999"}])])
async def test_comment_and_reverse_hook_stability_are_rechecked(installation, field, value):
    case = installation
    case.state.callback = lambda: setattr(case.state, field, value)
    with pytest.raises(ValueError, match="stability_changed"):
        await _install(case)


@pytest.mark.asyncio
async def test_actor_change_after_mutation_refuses(installation):
    case = installation
    case.state.callback = lambda: case.state.actor.update(transaction="1/2")
    with pytest.raises(ValueError, match="original_admin_changed"):
        await _install(case)


@pytest.mark.asyncio
async def test_mixed_old_and_target_pins_only_change_remaining_functions(installation):
    case = installation
    hardened = _target_rows(case.pins)
    case.pins[0] = case.state.rows[0] = deepcopy(hardened[0])
    assert await _install(case) == hardened
    commands = [call.args[0] for call in case.connection.execute.await_args_list if call.args[0].startswith("ALTER")]
    untouched = case.pins[0]["native"]["definition"]["proname"]
    assert all('"' + untouched + '"(' not in command for command in commands)


@pytest.mark.asyncio
async def test_unknown_derived_declaration_refuses_before_any_native_call(installation):
    case = installation
    case.pins[0]["native"]["body"] = "unbound declaration"
    with pytest.raises(ValueError, match="declaration_required"):
        await _install(case)
    case.connection.fetch.assert_not_awaited()
    case.connection.execute.assert_not_awaited()
