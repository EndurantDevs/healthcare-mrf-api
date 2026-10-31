# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact invocation binding for the existing allocator, without expanding custody."""

from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db import migration_cms_snapshot_allocator as migration
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_transition_routines as transition
from process import provider_directory_generic_source_release_checks as checks
from process import provider_directory_source_callable_custody as source
from tests.test_provider_directory_cms_callee_custody import _catalog_rows, _original_allocation_pins
from tests.test_provider_directory_cms_transition_routines import _raw
from tests.test_provider_directory_generic_source_release_checks import legacy_function_rows

_NAME = "prepare_pd_generic_dataset_storage"


def _bodies(schema):
    return (*migration.allocator_bodies(schema), migration.bound_cms_allocator_body(schema))


@pytest.mark.parametrize("schema", ["source_schema", "alternate_catalog", "Mixed_Catalog23"])
def test_bound_variant_adds_only_source_fixed_invocation_before_all_existing_branches(schema):
    original, previous, bound = _bodies(schema)
    assert callables._body_digest(original, schema) == migration.ORIGINAL_ALLOCATOR_SHA256
    start, tail = bound.split("BEGIN\n", 1)
    invocation, separator, unchanged = tail.partition("    END IF;\n")
    assert separator and start + "BEGIN\n" + unchanged == previous
    for clause in (
        "TG_WHEN IS DISTINCT FROM 'AFTER'",
        "TG_OP IS DISTINCT FROM 'INSERT'",
        "TG_LEVEL IS DISTINCT FROM 'ROW'",
        "TG_TABLE_SCHEMA IS DISTINCT FROM '" + schema + "'",
        "TG_TABLE_NAME IS DISTINCT FROM 'provider_directory_endpoint_dataset'",
        "TG_RELID IS DISTINCT FROM pg_catalog.to_regclass('"
        + storage_relation(schema, "provider_directory_endpoint_dataset")
        + "')::pg_catalog.oid",
    ):
        assert clause in invocation
    assert "NEW." not in invocation and "EXECUTE" not in invocation
    assert bound.index("allocator_invocation_invalid") < bound.index(migration.SELECTOR) < bound.index("FOREACH")
    assert (
        previous.replace(previous.split("BEGIN\n", 1)[1].split("    END IF;\n", 1)[0] + "    END IF;\n", "", 1)
        == original
    )


@pytest.mark.parametrize("damage", ["body", "begin", "declaration"])
def test_unknown_constructor_never_becomes_approved_authority(monkeypatch, damage):
    rendered = migration.candidate_functions("source_schema")
    old, new = {
        "body": (
            "parent_name text; candidate_name text; grant_row record;",
            "parent_name varchar; candidate_name text; grant_row record;",
        ),
        "begin": ("BEGIN\n    IF NEW.publication_metadata_json", "BEGIN\nBEGIN\n    IF NEW.publication_metadata_json"),
        "declaration": (_NAME + "() RETURNS trigger", _NAME + "(unexpected text) RETURNS trigger"),
    }[damage]
    assert rendered.count(old) == 1
    monkeypatch.setattr(migration, "candidate_functions", lambda schema: rendered.replace(old, new, 1))
    with pytest.raises(RuntimeError, match="predecessor_changed|source_template_changed"):
        migration.bound_cms_allocator_body("source_schema")


def _row(monkeypatch, schema="source_schema"):
    row = next(row for row in legacy_function_rows(monkeypatch) if row["definition"]["proname"] == _NAME)
    row["definition"]["prosrc"] = migration.allocator_bodies(schema)[0]
    return row


@pytest.mark.parametrize("version", [0, 1, 2])
def test_release_and_transition_consumers_share_exact_approved_variants(monkeypatch, version):
    row = _row(monkeypatch)
    row["definition"]["prosrc"] = _bodies("source_schema")[version]
    before = deepcopy(row)
    checks._require_native_function(
        row["definition"], checks._LEGACY_FUNCTIONS[_NAME + "()"], row["language"], "source_schema", 201, {105}
    )
    transition.require_cms_transition_routine_definitions(
        [_raw(row, "source_schema", 201)],
        catalog_schema="source_schema",
        catalog_schema_oid=201,
        preparation_schema="preparation_schema",
    )
    assert row == before
    assert not source._is_approved_body(row["definition"], "source_schema", "f" * 64)


@pytest.mark.parametrize("fault", ["body", "owner", "settings", "security", "type", "name", "default", "language"])
def test_bound_variant_preserves_all_other_native_refusals(monkeypatch, fault):
    row = _row(monkeypatch)
    definition = row["definition"]
    definition["prosrc"] = migration.bound_cms_allocator_body("source_schema")
    key, value = {
        "body": ("prosrc", definition["prosrc"] + "\n-- changed"),
        "owner": ("proowner", "999"),
        "settings": ("proconfig", ["search_path=public"]),
        "security": ("prosecdef", False),
        "type": ("prorettype", "2278"),
        "name": ("proname", "different_allocator"),
        "default": ("pronargdefaults", 1),
        "language": ("language", "sql"),
    }[fault]
    (row if fault == "language" else definition)[key] = value
    with pytest.raises(ValueError):
        checks._require_native_function(
            definition, checks._LEGACY_FUNCTIONS[_NAME + "()"], row["language"], "source_schema", 201, {105}
        )


@pytest.mark.parametrize("version", [0, 1, 2])
def test_optional_allocation_keeps_whole_authenticated_pin_and_fixed_frontier(monkeypatch, version):
    rows = _catalog_rows("snapshot_schema")
    row = _row(monkeypatch, "snapshot_schema")
    row["definition"].update(oid="999", pronamespace="42")
    row["definition"]["prosrc"] = _bodies("snapshot_schema")[version]
    row["signature"] = storage_relation("snapshot_schema", _NAME) + "()"
    rows.append(row)
    pins = _original_allocation_pins(rows)
    assert len(callables._cms_callee_contracts()) == 32
    assert len(callables.require_original_cms_allocation_callees(rows, **pins)) == len(rows)
    row["definition"]["prosrc"] += "\n-- post-authentication drift"
    with pytest.raises(ValueError, match="original_allocation_pin_changed"):
        callables.require_original_cms_allocation_callees(rows, **pins)


@pytest.fixture
def upgrade_case(monkeypatch):
    state = SimpleNamespace(
        body=_bodies("sample")[0],
        identity={"oid": "100", "proowner": "200", "proacl": ["unchanged"]},
        description="retained comment",
        path="pg_catalog",
        admin=True,
        writes=0,
    )

    @contextmanager
    def savepoint():
        before = state.body
        try:
            yield
        except BaseException:
            state.body = before
            raise

    def execute(query):
        if query == "SHOW search_path":
            return SimpleNamespace(scalar_one=lambda: state.path)
        assert query == "CREATE OR REPLACE FUNCTION trusted() AS $fn$" + _bodies("sample")[2] + "$fn$;"
        state.body = _bodies("sample")[2]
        state.writes += 1

    def observe(*args):
        return dict(
            body=state.body,
            definition="CREATE OR REPLACE FUNCTION trusted() AS $fn$" + state.body + "$fn$;",
            identity=deepcopy(state.identity),
            description=state.description,
        )

    state.connection = SimpleNamespace(
        in_transaction=Mock(return_value=True),
        exec_driver_sql=Mock(side_effect=execute),
        execute=Mock(side_effect=lambda *args: SimpleNamespace(scalar_one_or_none=lambda: state.admin)),
        begin_nested=Mock(side_effect=savepoint),
    )
    monkeypatch.setattr(migration, "_canonical_copy_guard_snapshot", observe)
    return state


def _upgrade(case, expected=None):
    migration.upgrade_cms_snapshot_allocator(
        case.connection, "sample", expected=expected if expected is not None else dict(oid=100, owner_oid=200)
    )


@pytest.mark.parametrize("version", [0, 1, 2])
def test_installer_preserves_identity_and_comment_from_both_predecessors_and_repeats(upgrade_case, version):
    upgrade_case.body = _bodies("sample")[version]
    before = deepcopy(upgrade_case.identity), upgrade_case.description
    _upgrade(upgrade_case)
    assert upgrade_case.body == _bodies("sample")[2]
    assert (upgrade_case.identity, upgrade_case.description) == before
    _upgrade(upgrade_case)
    assert upgrade_case.writes == (0 if version == 2 else 1) and not hasattr(upgrade_case.connection, "commit")


@pytest.mark.parametrize("fault", ["transaction", "path", "admin", "oid", "owner", "body"])
def test_installer_refuses_before_writes(upgrade_case, fault):
    expected_by_field = dict(oid=100, owner_oid=200)
    if fault == "transaction":
        upgrade_case.connection.in_transaction.return_value = False
    elif fault == "path":
        upgrade_case.path = "public"
    elif fault == "admin":
        upgrade_case.admin = False
    elif fault in {"oid", "owner"}:
        expected_by_field["oid" if fault == "oid" else "owner_oid"] += 1
    else:
        upgrade_case.body += "unapproved"
    with pytest.raises(RuntimeError):
        _upgrade(upgrade_case, expected_by_field)
    assert upgrade_case.writes == 0


def test_interrupted_installer_rolls_back_and_preserves_outer_transaction(upgrade_case):
    before = upgrade_case.body
    execute = upgrade_case.connection.exec_driver_sql.side_effect

    def interrupted(query):
        result = execute(query)
        if query != "SHOW search_path":
            raise RuntimeError("after body replacement")
        return result

    upgrade_case.connection.exec_driver_sql.side_effect = interrupted
    with pytest.raises(RuntimeError, match="after body replacement"):
        _upgrade(upgrade_case)
    assert upgrade_case.body == before and upgrade_case.connection.in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [0, 1, 2])
async def test_protected_cms_execution_requires_the_bound_target(monkeypatch, version):
    from process import network_cms_registry_address_capture as native

    observed_by_field = dict(body=_bodies("sample")[version], identity={"oid": 100, "proowner": 200})
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: observed_by_field)))
    monkeypatch.setattr(native, "NativeReceiptSession", lambda connection: session)
    driver = SimpleNamespace(
        is_in_transaction=lambda: True, is_closed=lambda: False, fetchval=AsyncMock(return_value="pg_catalog")
    )
    if version == 2:
        assert await migration.require_cms_snapshot_allocator(
            driver, "sample", expected=dict(oid=100, owner_oid=200)
        ) == dict(oid=100, owner_oid=200)
    else:
        with pytest.raises(RuntimeError, match="function_changed"):
            await migration.require_cms_snapshot_allocator(driver, "sample", expected=dict(oid=100, owner_oid=200))
