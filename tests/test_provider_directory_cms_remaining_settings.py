# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Remaining exact settings consumers preserve ordinary guard contracts."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import entity_address_snapshot_preparation as address
from process import network_fhir_retained_capture as capture
from process import provider_directory_cms_receipt_guard as receipt
from process import provider_directory_output_snapshot_journal as journal
from process import provider_directory_profile_initial_guards as initial
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.test_entity_address_preparation_cms import _migration
from tests.test_network_fhir_retained_capture import retained
from tests.test_provider_directory_cms_allocation_guard_contracts import _guard_specs
from tests.test_provider_directory_cms_callee_custody import _parse_statements
from tests.test_provider_directory_output_snapshot_journal import _capability, _plan

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


def _receipt_case(monkeypatch, group, settings):
    migration, recorder = _migration(), Mock()
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
    guard_rows = [_guard_mapping(name, *_EVENTS[name], functions[name][-1], settings) for name in names]
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


def _retained_guard(monkeypatch, settings):
    origin = retained().origin
    body = _guard_specs(monkeypatch, origin.schema_name)["guard_tin_npi_connector_dataset_resource"][-1]
    guard_by_field = dict(
        guards=4, prosecdef=True, proconfig=deepcopy(settings), lanname="plpgsql", provolatile="v", prosrc=body
    )
    reader = SimpleNamespace(fetchrow=AsyncMock(return_value=guard_by_field))
    candidate_check = AsyncMock()
    monkeypatch.setattr(capture, "_origin_candidate_guards", candidate_check)
    return reader, origin, guard_by_field, candidate_check


@pytest.mark.parametrize("settings", [_OLD, _NEW])
async def test_retained_source_guard_preserves_actual_source_body(monkeypatch, settings):
    reader, origin, guard, candidate_check = _retained_guard(monkeypatch, settings)
    await capture._origin_guard(reader, origin)
    candidate_check.assert_awaited_once_with(reader, origin)
    candidate_check.reset_mock()
    guard["prosrc"] += " RETURN NULL;"
    with pytest.raises(capture.RetainedFHIRCaptureError):
        await capture._origin_guard(reader, origin)
    candidate_check.assert_not_awaited()


@pytest.mark.parametrize("settings", _BAD)
async def test_retained_source_settings_near_misses_refuse(monkeypatch, settings):
    reader, origin, _guard, candidate_check = _retained_guard(monkeypatch, settings)
    with pytest.raises(capture.RetainedFHIRCaptureError):
        await capture._origin_guard(reader, origin)
    candidate_check.assert_not_awaited()


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


def _plan_case(monkeypatch, settings):
    capability = _capability()
    capability.policy = GenericSourcePolicy("source_schema", "plan_owner", "plan_publisher", ("reader",))
    hook_by_field = dict(
        tgname="pd_dataset_plan_owner",
        tgtype=62,
        tgenabled="A",
        tgisinternal=False,
        tgconstraint=0,
        tgargs=b"",
        tgqual=None,
        tgoldtable=None,
        tgnewtable=None,
        proname="guard_pd_dataset_migration_owner",
        nspname="source_schema",
        prosrc=journal.candidate_functions("source_schema").split("$fn$", 2)[1],
        prosecdef=False,
        proconfig=deepcopy(settings),
        pronargs=0,
        trigger_result=True,
        lanname="plpgsql",
        table_method="heap",
        function_owner="plan_owner",
    )
    capability.connection = SimpleNamespace(
        fetch=AsyncMock(
            side_effect=lambda *args: [_plan_catalog()] if args[0] == journal.OUTPUT_CATALOG_SQL else [hook_by_field]
        ),
        fetchval=AsyncMock(return_value=_plan()),
    )
    monkeypatch.setattr(journal.custody, "_require_capability", AsyncMock())
    monkeypatch.setattr(journal, "storage_roles", AsyncMock(return_value=(17, [18])))
    return capability, hook_by_field


@pytest.mark.parametrize("settings", [_OLD, _NEW])
async def test_output_journal_keeps_real_plan_hook_body(monkeypatch, settings):
    capability, hook = _plan_case(monkeypatch, settings)
    assert await journal._plan(capability) == _plan()
    hook["prosrc"] += " RETURN NULL;"
    with pytest.raises(RuntimeError, match="owner_hook_changed"):
        await journal._plan(capability)


@pytest.mark.parametrize("field,value", [("function_owner", "unknown"), ("prosecdef", True), ("tgenabled", "D")])
async def test_output_journal_other_hook_properties_stay_exact(monkeypatch, field, value):
    capability, hook = _plan_case(monkeypatch, _NEW)
    hook[field] = value
    with pytest.raises(RuntimeError, match="owner_hook_changed"):
        await journal._plan(capability)


@pytest.mark.parametrize("settings", _BAD)
async def test_output_journal_settings_near_misses_refuse(monkeypatch, settings):
    capability, _hook = _plan_case(monkeypatch, settings)
    with pytest.raises(RuntimeError, match="owner_hook_changed"):
        await journal._plan(capability)


def test_address_sql_keeps_exact_settings_pair_and_original_authority():
    assert "AND (p.proconfig=ARRAY['search_path=pg_catalog']::text[]" in address._CMS_GUARDS_SQL
    assert "OR p.proconfig=ARRAY['search_path=pg_catalog, pg_temp']::text[])" in address._CMS_GUARDS_SQL
    assert "p.pronamespace=c.relnamespace AND p.proname=t.tgname" in address._CMS_GUARDS_SQL
    assert "AND NOT p.prosecdef" in address._CMS_GUARDS_SQL
