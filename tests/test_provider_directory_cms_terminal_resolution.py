# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The terminal guard's two casts preserve every other accepted contract."""

import hashlib
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from db import migration_cms_terminal_resolution as migration
from process import network_fhir_source_custody as reader
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_transition_routines as transition
from process import provider_directory_source_callable_custody as source
from tests.test_provider_directory_cms_allocation_guard_contracts import original_guards as original_guards
from tests.test_provider_directory_cms_callee_custody import _original_allocation_pins
from tests.test_provider_directory_cms_transition_routines import _raw


@pytest.mark.parametrize("schema", ["snapshot_schema", "alternate_catalog", "Mixed_Catalog23"])
def test_only_the_two_frozen_varchar_arguments_change(schema):
    original, hardened = migration.terminal_guard_bodies(schema)
    assert callables._body_digest(original, schema) == migration.ORIGINAL_SHA256
    assert hardened.count("NEW.dataset_id::pg_catalog.text") == 2
    assert hardened.replace("NEW.dataset_id::pg_catalog.text", "NEW.dataset_id") == original
    assert len(callables._cms_callee_contracts()) == 32


def test_unknown_constructor_body_refuses_without_recapturing_authority(monkeypatch):
    module = SimpleNamespace(
        _dataset_guard_sql=lambda schema: "CREATE FUNCTION changed() AS $function$changed$function$;"
    )
    monkeypatch.setattr(migration, "_migration", lambda name: module)
    migration.terminal_guard_bodies.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="predecessor_changed"):
            migration.terminal_guard_bodies("unknown_predecessor")
    finally:
        migration.terminal_guard_bodies.cache_clear()


@pytest.mark.parametrize("version", [0, 1])
def test_allocation_and_complete_supplied_inventory_share_exact_body_pair(original_guards, version):
    guard = next(row for row in original_guards if row["definition"]["proname"] == migration.GUARD)
    guard["definition"]["prosrc"] = migration.terminal_guard_bodies("snapshot_schema")[version]
    pins = _original_allocation_pins(original_guards)
    assert callables.require_original_cms_allocation_callees(original_guards, **pins)[migration.GUARD] == guard
    assert (
        transition.require_cms_transition_routine_definitions(
            [_raw(guard)],
            catalog_schema="snapshot_schema",
            catalog_schema_oid=42,
            preparation_schema="preparation_schema",
        )
        is None
    )
    assert not source._is_approved_body(guard["definition"], "snapshot_schema", "f" * 64)
    guard["definition"]["prosrc"] += "\n-- unapproved"
    with pytest.raises(ValueError, match="original_allocation_pin_changed"):
        callables.require_original_cms_allocation_callees(original_guards, **pins)
    with pytest.raises(ValueError, match="definition_changed"):
        callables.require_original_cms_allocation_callees(original_guards, **_original_allocation_pins(original_guards))


def _reader_function(version):
    expected = reader._REACHABLE_FUNCTIONS[migration.GUARD]
    return dict(
        proname=migration.GUARD,
        prosrc=migration.terminal_guard_bodies("snapshot_schema")[version],
        proowner=100,
        lanname=expected[1],
        provolatile=expected[2],
        prosecdef=expected[3],
        proconfig=list(expected[4]),
    )


@pytest.mark.parametrize("version", [0, 1])
@pytest.mark.parametrize("precomputed", [False, True])
def test_retained_reader_recognizes_exact_successor_without_input_changes(version, precomputed):
    function = _reader_function(version)
    if precomputed:
        body = function.pop("prosrc").replace('"snapshot_schema".', '"__schema__".')
        function["body_sha256"] = hashlib.sha256(reader._normalized(body).encode()).hexdigest()
    before = deepcopy(function)
    reader._require_function(function, "snapshot_schema", 100)
    assert function == before


@pytest.mark.parametrize("fault", ["body", "owner", "security", "settings", "language", "volatility", "predecessor"])
def test_reader_keeps_native_identity_and_frozen_predecessor_checks(monkeypatch, fault):
    function = _reader_function(1)
    if fault == "body":
        function["prosrc"] += "\n-- unapproved"
    elif fault == "predecessor":
        spec = reader._REACHABLE_FUNCTIONS[migration.GUARD]
        monkeypatch.setitem(reader._REACHABLE_FUNCTIONS, migration.GUARD, ("f" * 64, *spec[1:]))
    else:
        key, value = {
            "owner": ("proowner", 999),
            "security": ("prosecdef", False),
            "settings": ("proconfig", ["search_path=public"]),
            "language": ("lanname", "sql"),
            "volatility": ("provolatile", "s"),
        }[fault]
        function[key] = value
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_function(function, "snapshot_schema", 100)


@pytest.fixture
def upgrade_case(monkeypatch):
    old, new = migration.terminal_guard_bodies("sample")
    state = SimpleNamespace(
        body=old,
        definition="CREATE OR REPLACE FUNCTION trusted() AS $function$" + old + "$function$;",
        identity={"oid": "100", "proowner": "200", "proacl": ["unchanged"]},
        description="retained comment",
        path="pg_catalog",
        admin=True,
        writes=0,
    )

    @contextmanager
    def savepoint():
        before = state.body, state.definition
        try:
            yield
        except BaseException:
            state.body, state.definition = before
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
        execute=Mock(side_effect=lambda *args: SimpleNamespace(scalar_one_or_none=lambda: state.admin)),
        begin_nested=Mock(side_effect=savepoint),
    )
    monkeypatch.setattr(
        migration,
        "_canonical_copy_guard_snapshot",
        lambda *args: {key: deepcopy(getattr(state, key)) for key in ("body", "definition", "identity", "description")},
    )
    return state


def _upgrade(case, expected=None):
    migration.upgrade_cms_terminal_guard_resolution(
        case.connection, "sample", expected=expected if expected is not None else dict(oid=100, owner_oid=200)
    )


def test_installer_preserves_identity_comment_and_repeats_without_writes(upgrade_case):
    before = deepcopy(upgrade_case.identity), upgrade_case.description
    _upgrade(upgrade_case)
    assert upgrade_case.body == migration.terminal_guard_bodies("sample")[1]
    assert (upgrade_case.identity, upgrade_case.description) == before
    _upgrade(upgrade_case)
    assert upgrade_case.writes == 1 and not hasattr(upgrade_case.connection, "commit")


@pytest.mark.parametrize("fault", ["transaction", "path", "admin", "oid", "owner", "body"])
def test_installer_refuses_before_body_write(upgrade_case, fault):
    expected_by_field = dict(oid=100, owner_oid=200)
    match fault:
        case "transaction":
            upgrade_case.connection.in_transaction.return_value = False
        case "path":
            upgrade_case.path = "public"
        case "admin":
            upgrade_case.admin = False
        case "oid" | "owner":
            expected_by_field["oid" if fault == "oid" else "owner_oid"] += 1
        case "body":
            upgrade_case.body += "unapproved"
    with pytest.raises(RuntimeError):
        _upgrade(upgrade_case, expected_by_field)
    assert upgrade_case.writes == 0


def test_interruption_rolls_back_body_and_keeps_caller_transaction(upgrade_case):
    before = upgrade_case.body, upgrade_case.definition
    execute = upgrade_case.connection.exec_driver_sql.side_effect

    def interrupted(query):
        result = execute(query)
        if query != "SHOW search_path":
            raise RuntimeError("after actual body replacement")
        return result

    upgrade_case.connection.exec_driver_sql.side_effect = interrupted
    with pytest.raises(RuntimeError, match="after actual body replacement"):
        _upgrade(upgrade_case)
    assert (upgrade_case.body, upgrade_case.definition) == before
    assert upgrade_case.writes == 1 and upgrade_case.connection.in_transaction()
