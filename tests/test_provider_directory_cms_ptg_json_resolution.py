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
from tests.test_acquisition_validator_argument_types_migration import _parse_functions
from tests.test_cms_archive_transaction_migration import body_digest


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
    assert body_digest(old, schema) == migration.ORIGINAL_SHA256
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


@pytest.fixture
def upgrade_case(monkeypatch):
    """Labelled IO stub: real updater/property coverage belongs to the native suite."""
    old, new = migration.ptg_json_bodies("sample")
    state = SimpleNamespace(
        body=old,
        definition="CREATE OR REPLACE FUNCTION trusted() AS $function$" + old + "$function$;",
        identity=dict(oid="501", proowner="103", proname=migration.FUNCTION, prosecdef=False),
        description="retained comment",
        hooks=[],
        language="sql",
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


@pytest.mark.parametrize("fault", ["transaction", "path", "admin", "oid", "owner", "body", "definition"])
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
