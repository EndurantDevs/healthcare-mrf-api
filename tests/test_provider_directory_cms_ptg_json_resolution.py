# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One native qualification preserves the existing JSONB helper and transaction."""

import asyncio
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from db import migration_cms_ptg_json_resolution as migration
from db.migration_cms_noncurrent_seal import _migration
from db.migration_cms_snapshot_allocator import _INSTALLER_ADMIN_SQL
from process import provider_directory_cms_transition_routines as transition
from process import provider_directory_source_callable_custody as source
from tests.test_acquisition_validator_argument_types_migration import _parse_functions
from tests.test_provider_directory_cms_transition_routines import _raw
from tests.test_provider_directory_source_callable_custody import _entry

_KEY = migration.FUNCTION + "(jsonb)"


def _constructor_bodies(monkeypatch, schema):
    """Record trusted historical constructors without executing SQL."""
    statements = []
    original = _migration("20260810110000_ptg_wave_receipt_authority.py")
    monkeypatch.setattr(original, "op", SimpleNamespace(execute=statements.append))
    original._install_receipt_verification_functions(schema)
    functions = _parse_functions("\n".join(statements).replace('"' + schema + '"', '"snapshot_schema"'))
    value = functions[migration.FUNCTION, "jsonb"][-1]
    return value.replace('"snapshot_schema"', '"' + schema + '"')


@pytest.mark.parametrize("schema", ["sample", "alternate_catalog", "Mixed_Catalog23", "select"])
def test_exact_constructor_changes_only_native_call_qualification(monkeypatch, schema):
    old, new = migration.ptg_json_bodies(schema)
    assert old == _constructor_bodies(monkeypatch, schema)
    assert new.count("pg_catalog.to_jsonb(entry.key)") == 1
    assert new.replace("pg_catalog.to_jsonb(entry.key)", "to_jsonb(entry.key)") == old
    assert source._body_digest(old, schema) == migration.ORIGINAL_SHA256
    repairs = _migration("20261016050000_acquisition_validator_argument_types.py")
    selected_rows = [row for row in repairs._CONTRACTS if row[0] == migration.FUNCTION and row[2] == "jsonb"]
    assert len(selected_rows) == 1 and selected_rows[0][-2:] == (migration.ORIGINAL_SHA256, migration.ORIGINAL_SHA256)
    assert not any(caller == migration.FUNCTION for caller, *_rest in repairs._calls(schema))


def test_unknown_predecessor_refuses_without_renewing_expected_digest(monkeypatch):
    migration.ptg_json_bodies.cache_clear()
    monkeypatch.setattr(migration, "ORIGINAL_SHA256", "f" * 64)
    try:
        with pytest.raises(RuntimeError, match="predecessor_changed"):
            migration.ptg_json_bodies("unrecognized_catalog")
    finally:
        migration.ptg_json_bodies.cache_clear()


def _source_entry(version, *, target_settings=False):
    entry = _entry(_KEY)
    entry["definition"]["prosrc"] = migration.ptg_json_bodies("sample")[version]
    if target_settings:
        entry["definition"]["proconfig"] = ["search_path=pg_catalog, sample, pg_temp"]
    return entry


@pytest.mark.parametrize("version", [0, 1])
@pytest.mark.parametrize("target_settings", [False, True])
def test_shared_source_and_complete_inventory_recognizers_accept_only_fixed_pair(version, target_settings):
    entry = _source_entry(version, target_settings=target_settings)
    before = deepcopy(entry)
    source._require_definition(entry, source._CONTRACTS[_KEY], "sample", 201, source._TYPE_OIDS, "sample")
    native = _raw(entry, schema="sample", namespace=201)
    native["native"]["namespace_identifier"] = "sample"
    transition.require_cms_transition_routine_definitions(
        [native], catalog_schema="sample", catalog_schema_oid=201, preparation_schema="preparation_schema"
    )
    assert entry == before
    assert not source._is_approved_body(entry["definition"], "sample", "f" * 64)
    assert not source._is_approved_body(
        entry["definition"], "sample", source._CONTRACTS[migration.FUNCTION + "(json)"][-1]
    )
    entry["definition"]["prosrc"] += "\n"
    with pytest.raises(ValueError, match="definition_changed"):
        source._require_definition(entry, source._CONTRACTS[_KEY], "sample", 201, source._TYPE_OIDS, "sample")


@pytest.mark.parametrize(
    "field,value",
    [
        ("pronamespace", "999"),
        ("proname", "other_helper"),
        ("proargtypes", ["114"]),
        ("proargnames", ["other_input"]),
        ("proargdefaults", "unapproved"),
        ("provariadic", "3802"),
        ("prorettype", "3802"),
        ("prosecdef", True),
        ("proisstrict", False),
        ("provolatile", "v"),
        ("proparallel", "u"),
        ("proconfig", ["search_path=sample, pg_catalog"]),
    ],
)
def test_exact_successor_preserves_source_abi_and_settings(field, value):
    entry = _source_entry(1)
    entry["definition"][field] = value
    with pytest.raises(ValueError, match="definition_changed"):
        source._require_definition(entry, source._CONTRACTS[_KEY], "sample", 201, source._TYPE_OIDS, "sample")


@pytest.fixture
def upgrade_case(monkeypatch):
    """Labelled IO stub: real updater/property coverage belongs to the native suite."""
    old, new = migration.ptg_json_bodies("sample")
    entry = _source_entry(0)
    identity_by_field = dict(entry["definition"])
    identity_by_field.pop("prosrc")
    state = SimpleNamespace(
        body=old,
        definition="CREATE OR REPLACE FUNCTION trusted() AS $function$" + old + "$function$;",
        identity=identity_by_field,
        description="retained comment",
        hooks=[],
        language=entry["language"],
        namespace_identifier="sample",
        path="pg_catalog, pg_temp",
        admin=True,
        writes=0,
    )

    @contextmanager
    def savepoint():
        before = deepcopy((state.body, state.definition, state.identity, state.description, state.hooks))
        try:
            yield
        except BaseException:
            state.body, state.definition, state.identity, state.description, state.hooks = before
            raise

    def execute(query):
        if query == "SHOW search_path":
            return SimpleNamespace(scalar_one=lambda: state.path)
        assert query == state.definition.replace(old, new, 1)
        state.body, state.definition = new, query
        state.writes += 1

    state.connection = SimpleNamespace(
        in_transaction=Mock(return_value=True),
        exec_driver_sql=Mock(side_effect=execute),
        execute=Mock(side_effect=lambda query: SimpleNamespace(scalar_one_or_none=lambda: state.admin)),
        begin_nested=Mock(side_effect=savepoint),
    )
    monkeypatch.setattr(
        migration,
        "_snapshot",
        lambda *_arguments: {
            key: deepcopy(getattr(state, key))
            for key in ("body", "definition", "identity", "description", "hooks", "language", "namespace_identifier")
        },
    )
    return state


def _upgrade(case, expected=None):
    migration.upgrade_cms_ptg_json_resolution(
        case.connection, "sample", expected=expected if expected is not None else dict(oid=501, owner_oid=103)
    )


def test_installer_preserves_properties_and_repeats_without_write(upgrade_case):
    state = upgrade_case
    before = deepcopy((state.identity, state.description, state.hooks))
    _upgrade(state)
    assert state.body == migration.ptg_json_bodies("sample")[1]
    assert (state.identity, state.description, state.hooks) == before
    state.connection.execute.assert_called_once_with(_INSTALLER_ADMIN_SQL)
    _upgrade(state)
    assert state.writes == 1 and not hasattr(state.connection, "commit")


@pytest.mark.parametrize("fault", ["transaction", "path", "admin", "oid", "owner", "body", "definition", "abi"])
def test_installer_refuses_before_body_write(upgrade_case, fault):
    state, pins = upgrade_case, dict(oid=501, owner_oid=103)
    match fault:
        case "transaction":
            state.connection.in_transaction.return_value = False
        case "path":
            state.path = "public"
        case "admin":
            state.admin = False
        case "oid" | "owner":
            pins["oid" if fault == "oid" else "owner_oid"] += 1
        case "body":
            state.body += "unapproved"
        case "definition":
            state.definition += state.body
        case "abi":
            state.identity["prosecdef"] = True
    with pytest.raises((RuntimeError, ValueError)):
        _upgrade(state, pins)
    assert state.writes == 0


@pytest.mark.parametrize("field,value", [("description", "changed"), ("hooks", [dict(oid=999)]), ("identity", "acl")])
def test_postwrite_property_changes_roll_back_the_body(upgrade_case, field, value):
    state = upgrade_case
    before = deepcopy((state.body, state.definition, state.identity, state.description, state.hooks))
    original = state.connection.exec_driver_sql.side_effect

    def changed(query):
        result = original(query)
        if query != "SHOW search_path":
            if field == "identity":
                state.identity["proacl"] = ["unapproved"]
            else:
                setattr(state, field, value)
        return result

    state.connection.exec_driver_sql.side_effect = changed
    with pytest.raises(RuntimeError, match="identity_changed"):
        _upgrade(state)
    assert (state.body, state.definition, state.identity, state.description, state.hooks) == before
    assert state.connection.in_transaction()


@pytest.mark.parametrize("failure", [RuntimeError("interrupted body update"), asyncio.CancelledError()])
def test_interruption_rolls_back_body_and_retains_caller_transaction(upgrade_case, failure):
    state = upgrade_case
    before = state.body, state.definition
    original = state.connection.exec_driver_sql.side_effect

    def interrupted(query):
        result = original(query)
        if query != "SHOW search_path":
            raise failure
        return result

    state.connection.exec_driver_sql.side_effect = interrupted
    with pytest.raises(type(failure)) as observed:
        _upgrade(state)
    assert observed.value is failure
    assert (state.body, state.definition) == before
    assert state.connection.in_transaction() and not hasattr(state.connection, "commit")
