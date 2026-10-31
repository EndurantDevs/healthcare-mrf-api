# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Secondary consumers accept exact source settings successors and reject near misses."""

import hashlib
import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db import migration_generic_source_candidates as candidates
from process import provider_directory_cms_allocator_callables as allocator
from process import provider_directory_cms_control_custody as controls
from process import provider_directory_source_checkpoint_catalog as checkpoint
from process import provider_directory_terminal_root_retirement_controls as retirement
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_cms_control_custody import _authentic_entry
from tests.test_provider_directory_source_callable_custody import _entry
from tests.test_provider_directory_source_checkpoint_catalog import _guards
from tests.test_source_validator_argument_types_migration import _load, _migration, _parse_function

_OLD = ["search_path=pg_catalog"]
_NEW = ["search_path=pg_catalog, pg_temp"]
_UNAPPROVED = (
    None,
    [],
    ["search_path=pg_temp, pg_catalog"],
    ["search_path=pg_catalog", "search_path=pg_temp"],
    ["search_path=pg_catalog, pg_temp", "TimeZone=UTC"],
    ["search_path=pg_catalog, public, pg_temp"],
)


def _allocator_case(name):
    bodies = allocator._source_bodies("synthetic", "preparation")
    definition, language = allocator._expected_properties(name, 31, 41, bodies[name])
    definition.update(oid="51", prolang="14" if language == "sql" else "15", proacl=None)
    entry_by_field = dict(
        definition=definition, language=language, trusted_language=True, acl=[[41, 41, "EXECUTE", False]]
    )
    return entry_by_field, {name: 41}, bodies


@pytest.mark.parametrize("name", allocator._SPECS)
@pytest.mark.parametrize("settings", [_OLD, _NEW])
def test_allocator_fixed_definitions_accept_only_exact_settings_pair(name, settings):
    row, owners, bodies = _allocator_case(name)
    row["definition"]["proconfig"] = deepcopy(settings)
    original = deepcopy(row)
    assert allocator._require_definition(row, 31, owners, bodies) == original["definition"]
    assert row == original
    row["definition"]["prosrc"] += "\nRETURN NULL;"
    with pytest.raises(ValueError, match="definition_changed"):
        allocator._require_definition(row, 31, owners, bodies)


@pytest.mark.parametrize("settings", _UNAPPROVED)
def test_allocator_settings_near_misses_refuse(settings):
    row, owners, bodies = _allocator_case("pd_dataset_candidate_tables")
    row["definition"]["proconfig"] = deepcopy(settings)
    with pytest.raises(ValueError, match="definition_changed"):
        allocator._require_definition(row, 31, owners, bodies)


@pytest.mark.parametrize("name", [entry[0] for entry in _migration()._CONTRACTS])
@pytest.mark.parametrize("settings", [_OLD, _NEW])
def test_control_source_contracts_accept_only_settings_pair(name, settings):
    row = _authentic_entry(name)
    row["definition"]["proconfig"] = deepcopy(settings)
    original = deepcopy(row)
    controls._require_callable_definition(row, "snapshot_schema", 44)
    assert row == original
    row["definition"]["proargdefaults"] = "0"
    with pytest.raises(ValueError, match="callable_definition_changed"):
        controls._require_callable_definition(row, "snapshot_schema", 44)


@pytest.mark.parametrize("settings", _UNAPPROVED)
def test_control_settings_near_misses_refuse(settings):
    row = _authentic_entry("provider_directory_reviewed_subset_activation_valid")
    row["definition"]["proconfig"] = deepcopy(settings)
    with pytest.raises(ValueError, match="callable_definition_changed"):
        controls._require_callable_definition(row, "snapshot_schema", 44)


def test_null_settings_control_exception_remains_exact(monkeypatch):
    row = _entry("provider_directory_profile_delta_receipt_immutable_v1()")
    migration, statements = _load("20260730110000_provider_directory_profile_delta.py"), []
    monkeypatch.setattr(
        migration,
        "op",
        SimpleNamespace(
            create_table=lambda *args, **kwargs: None,
            create_index=lambda *args, **kwargs: None,
            execute=statements.append,
        ),
    )
    migration._create_delta_receipt_table("snapshot_schema")
    definitions = [sql for sql in statements if sql.strip().upper().startswith("CREATE FUNCTION")]
    assert len(definitions) == 1
    name, attributes = _parse_function(definitions[0].replace("$$", "$guard$"))
    assert name == row["definition"]["proname"]
    row["definition"]["prosrc"] = attributes[-1]
    namespace = int(row["definition"]["pronamespace"])
    controls._require_callable_definition(row, "snapshot_schema", namespace)
    for settings in (_OLD, _NEW):
        row["definition"]["proconfig"] = settings
        with pytest.raises(ValueError, match="callable_definition_changed"):
            controls._require_callable_definition(row, "snapshot_schema", namespace)


def _checkpoint_case(monkeypatch, root, settings):
    guards = _guards(root)
    monkeypatch.setattr(
        checkpoint,
        "_BODIES",
        {name: hashlib.sha256(b"BEGIN RETURN NULL; END;").hexdigest() for name in checkpoint._BODIES},
    )
    for guard in guards:
        guard["proconfig"] = deepcopy(settings)
    policy = SimpleNamespace(schema="sample", owner_role="owner", control_runtime_roles=("builder",))
    return guards, policy


@pytest.mark.parametrize("root", checkpoint.CHECKPOINT_LAYOUTS)
@pytest.mark.parametrize("settings", [_OLD, _NEW])
def test_checkpoint_settings_pair_preserves_complete_invocation(monkeypatch, root, settings):
    guards, policy = _checkpoint_case(monkeypatch, root, settings)
    original = deepcopy(guards)
    checkpoint._require_guards(guards, root, policy, legacy=True)
    assert guards == original
    guards[0]["tgargs"] = b"changed\x00"
    with pytest.raises(GenericCandidateError, match="checkpoint_guard_changed"):
        checkpoint._require_guards(guards, root, policy, legacy=True)


@pytest.mark.parametrize("settings", _UNAPPROVED)
def test_checkpoint_settings_near_misses_refuse(monkeypatch, settings):
    guards, policy = _checkpoint_case(monkeypatch, checkpoint.BULK_ROOT, settings)
    with pytest.raises(GenericCandidateError, match="checkpoint_guard_changed"):
        checkpoint._require_guards(guards, checkpoint.BULK_ROOT, policy, legacy=True)


@pytest.mark.asyncio
async def test_maintenance_query_keeps_exact_source_derived_settings_body_and_hooks(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "snapshot_schema")
    monkeypatch.setattr(retirement, "require_native_retirement_controls", AsyncMock(return_value=set()))
    database = SimpleNamespace(scalar=AsyncMock(return_value=True))
    await retirement.require_retirement_controls(database)
    call = database.scalar.await_args
    functions = json.loads(call.kwargs["functions"])
    for function in functions:
        legacy = function["settings"]
        assert legacy in (_OLD, [*_OLD, "TimeZone=UTC"])
        assert function["settings_successor"] == [*_NEW, *legacy[1:]]
        assert function["body"] and function["language"] in {"sql", "plpgsql"}
    assert json.loads(call.kwargs["triggers"])
    assert "IS DISTINCT FROM expected.settings\n                   AND" in call.args[0]
    assert "IS DISTINCT FROM expected.settings_successor)" in call.args[0]
    assert "overload.oid<>p.oid" in call.args[0]
    assert "t.tgfoid<>pg_catalog.to_regprocedure(expected.function)" in call.args[0]


def test_existing_copy_guard_snapshot_keeps_finite_stored_settings():
    statement = str(candidates._COPY_GUARD_SNAPSHOT)
    assert "p.proconfig IN (ARRAY['search_path=pg_catalog']::text[]," in statement
    assert "ARRAY['search_path=pg_catalog, pg_temp']::text[])" in statement
    assert "p.prorettype='trigger'::regtype AND language.lanname='plpgsql' AND p.prosecdef" in statement
