# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Affected closure and existing caller transaction/lifecycle contracts."""

import asyncio
import datetime
import importlib
import sqlite3
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import entity_address_affected_patch as patch
from process import entity_address_affected_support as support
from process import entity_address_ordinary_overlay as overlay
from process import entity_address_ordinary_preparation as ordinary
from process import entity_address_source_preparation as source

native = importlib.import_module("process.entity_address_unified")


@pytest.fixture
def build(monkeypatch):
    capture = SimpleNamespace(
        affected_scope={"source_ids": ["source-a"]},
        is_unchanged=False,
        address_task={"refresh_mode": "provider-directory-partial"},
    )
    scope = SimpleNamespace(
        admission=SimpleNamespace(), native_input_hash="a" * 64, owned_oids={"stage": 11, "locations": 12, "groups": 13}
    )
    state = patch._AffectedBuild(scope, capture, None, incumbent=(99, (101, 102), {"generation": 1}))
    monkeypatch.setattr(patch, "_capture", lambda supplied: capture if supplied is scope else None)
    token = patch._ACTIVE.set(state)
    yield state
    patch._ACTIVE.reset(token)


def test_logical_overlay_keeps_unchanged_sources_without_copy(build):
    inputs = SimpleNamespace(overlay_table="overlay_delta")
    sql = patch.logical_overlay_sql(native, "mrf", inputs)
    assert "FROM mrf.provider_directory_address_overlay WHERE NOT(source_id=ANY(" in sql
    assert "UNION ALL" in sql and "FROM mrf.overlay_delta WHERE source_id=ANY(" in sql
    assert "INSERT" not in sql and "CREATE" not in sql


def test_affected_source_filter_uses_persisted_source_ids(build):
    sql = native._provider_directory_live_source_filter_sql("mrf", "live", source_ids=["ignored"])
    assert "split_part(rid,':',3)" in sql and "source-a" in sql
    assert "provider_directory_source" not in sql


def test_inferred_npi_enters_location_closure(build):
    assert "live.inferred_npi" in native._entity_address_row_npi_expr("live")
    sql = native._prepare_affected_live_locations_sql(
        "mrf",
        live_table="entity_address_unified",
        affected_group_table="groups",
        replacement_lookup_table="stage",
        affected_location_table="locations",
    )
    assert "live.inferred_npi" in sql
    assert "SELECT replacement.location_key" in sql


def test_default_npi_and_source_queries_keep_their_original_branch():
    assert "inferred_npi" not in native._entity_address_row_npi_expr("live")
    assert "provider_directory_source" in native._provider_directory_live_source_filter_sql(
        "mrf", "live", source_ids=["source-a"]
    )
    assert patch.logical_overlay_sql(native, "mrf", SimpleNamespace()) is None


def _descriptor(state):
    descriptor = patch.AffectedAddressPatch(
        "mrf",
        "stage",
        11,
        "locations",
        12,
        "groups",
        13,
        99,
        (101, 102),
        ("source-a",),
        state.scope.admission,
        state.capture,
        state,
        tuple(
            (model.__main_table__, "stage_" + model.__main_table__, index + 20)
            for index, model in enumerate(native.SUPPORT_TABLE_MODELS)
        ),
    )
    state.descriptor = descriptor
    patch._ISSUED.add(descriptor)
    return descriptor


def test_descriptor_has_seven_stages_and_two_scopes(build):
    descriptor = _descriptor(build)
    assert len(descriptor.owned_relations) == 9
    assert descriptor.owned_relations[0] == ("entity_address_unified", "stage", 11)
    assert descriptor.owned_relations[-2:] == (("locations", "locations", 12), ("groups", "groups", 13))
    assert {target_relation for target_relation, _stage, _oid in descriptor.support_stages} == {
        model.__main_table__ for model in native.SUPPORT_TABLE_MODELS
    }
    forged = replace(descriptor)
    with pytest.raises(RuntimeError, match="affected_patch_invalid"):
        patch.require_descriptor(SimpleNamespace(affected_patch=forged, committed=False))


@pytest.mark.asyncio
async def test_issued_descriptor_remains_bound_to_original_task(build):
    build.task = asyncio.current_task()
    descriptor = _descriptor(build)
    prepared = SimpleNamespace(affected_patch=descriptor, committed=False)
    assert patch.require_descriptor(prepared) is descriptor

    async def other_task():
        with pytest.raises(RuntimeError, match="affected_patch_invalid"):
            patch.require_descriptor(prepared)

    await asyncio.create_task(other_task())


def test_actual_patch_sql_replaces_only_closed_keys_and_rolls_back(build):
    descriptor = _descriptor(build)
    database = sqlite3.connect(":memory:")
    database.execute("ATTACH DATABASE ':memory:' AS mrf")
    database.executescript(
        "CREATE TABLE mrf.entity_address_unified(location_key TEXT PRIMARY KEY,value TEXT);CREATE TABLE mrf.stage(location_key TEXT PRIMARY KEY,value TEXT);CREATE TABLE mrf.locations(location_key TEXT PRIMARY KEY);INSERT INTO mrf.entity_address_unified VALUES ('old','R1'),('unrelated','unchanged');INSERT INTO mrf.stage VALUES ('new','R2');INSERT INTO mrf.locations VALUES ('old'),('new');"
    )
    statements = patch.patch_statements(
        SimpleNamespace(_entity_address_column_list=lambda: "location_key,value"), descriptor
    )
    database.execute("BEGIN")
    for statement in statements:
        if statement.startswith("DELETE"):
            statement = statement.replace(
                "DELETE FROM mrf.entity_address_unified live USING mrf.locations scope WHERE live.location_key=scope.location_key",
                "DELETE FROM mrf.entity_address_unified WHERE location_key IN (SELECT location_key FROM mrf.locations)",
            )
        database.execute(statement)
    assert database.execute("SELECT * FROM mrf.entity_address_unified ORDER BY location_key").fetchall() == [
        ("new", "R2"),
        ("unrelated", "unchanged"),
    ]
    database.rollback()
    assert database.execute("SELECT * FROM mrf.entity_address_unified ORDER BY location_key").fetchall() == [
        ("old", "R1"),
        ("unrelated", "unchanged"),
    ]
    assert not any(keyword in " ".join(statements) for keyword in ("DROP", "ALTER", "CREATE", "TRUNCATE"))
    database.close()


@pytest.mark.asyncio
async def test_location_scope_is_created_via_existing_owned_lifecycle(build, monkeypatch):
    create = AsyncMock()
    status = AsyncMock()
    monkeypatch.setattr(native.preparation_admission, "create_stage_sql", create)
    monkeypatch.setattr(native.preparation_admission, "stage_status", status)
    context_by_field = {}
    await patch.prepare_location_scope(native, "mrf", "stage", "groups", context_by_field)
    create.assert_awaited_once()
    assert "replacement.location_key" in create.await_args.args[2]
    assert context_by_field["partial_provider_directory_unaffected_live_rows_copied"] == 0


@pytest.mark.asyncio
async def test_row_count_validation_accounts_for_merged_result(build):
    database = SimpleNamespace(scalar=AsyncMock(return_value=3))
    context_by_field = {"staged_rows": 2, "partial_provider_directory_affected_live_location_table": "locations"}
    assert await patch.merged_row_count(SimpleNamespace(db=database), "mrf", "stage", context_by_field, 100) == 99
    assert "JOIN mrf.locations" in database.scalar.await_args.args[0]


@pytest.mark.asyncio
async def test_support_candidates_require_replacement_locations(build):
    dependency = SimpleNamespace(
        db=SimpleNamespace(scalar=AsyncMock(side_effect=[False, False, True])),
        SUPPORT_TABLE_MODELS=native.SUPPORT_TABLE_MODELS,
    )
    with pytest.raises(RuntimeError, match="affected_support_invalid"):
        await patch.validate_merged_support(dependency, _descriptor(build))
    assert "WHERE candidate.location_key=support.location_key" in dependency.db.scalar.await_args.args[0]


@pytest.mark.asyncio
async def test_support_validation_stays_within_candidates(build):
    dependency = SimpleNamespace(
        db=SimpleNamespace(scalar=AsyncMock(return_value=False)), SUPPORT_TABLE_MODELS=native.SUPPORT_TABLE_MODELS
    )
    await patch.validate_merged_support(dependency, _descriptor(build))
    assert dependency.db.scalar.await_count == 8


@pytest.mark.asyncio
async def test_publish_requires_caller_transaction_before_any_statement(build):
    dependency = SimpleNamespace(
        db=SimpleNamespace(),
        require_caller_owned_cutover_transaction=Mock(side_effect=RuntimeError("caller transaction required")),
    )
    with pytest.raises(RuntimeError, match="caller transaction required"):
        await patch.publish(dependency, SimpleNamespace(affected_patch=_descriptor(build), committed=False), None)


def test_source_lifecycle_never_converts_cms_admission_to_ordinary(monkeypatch):
    admission = SimpleNamespace(plan=object())
    assert ordinary.issue(admission, "a" * 64) is None
    monkeypatch.setattr(source.source, "is_source_storage_required", lambda: False)
    assert source.issue_source_preparation(admission, "a" * 64) is None


@pytest.mark.asyncio
async def test_scope_retirement_preserves_reused_physical_name():
    descriptor = SimpleNamespace(owned_relations=(("entity_address_unified", "stage", 11),), schema="mrf")
    database = SimpleNamespace(scalar=AsyncMock(return_value=999))
    with pytest.raises(RuntimeError, match="ordinary_preparation_invalid"):
        await ordinary.assert_relations(database, descriptor)


def test_cms_finalized_family_contract_remains_separate():
    from process.network_registry_cms_prepared_address import prepared_address_stages

    with pytest.raises(ValueError, match="prepared_address_invalid"):
        prepared_address_stages(SimpleNamespace(stage_oids=(("entity_address_unified", "stage", 11),)))


def test_captured_partial_recipe_requires_no_full_replacement_copy():
    import inspect

    text = inspect.getsource(native.process_entity_address_unified_data) + inspect.getsource(
        native.publish_entity_address_unified_generation
    )
    assert "and not affected_patch.is_active()" in text
    assert "await affected_patch.prepare_location_scope(" in text
    assert 'record_candidate": False} if protected_publication or affected_patch.is_active()' in text
    assert "coordinate_scope_table and not affected_patch.is_active()" in text
    assert "preparation_admission.inference_enabled(test_mode)" in text


@pytest.mark.asyncio
async def test_finalize_uses_actual_owned_scope_oids(build, monkeypatch):
    build.task = asyncio.current_task()
    prototype = _descriptor(build)
    build.scope.stage_oids = (("entity_address_unified", "stage", 11), *prototype.support_stages)
    build.scope.owned_oids.update({name: oid for _target, name, oid in prototype.support_stages})
    monkeypatch.setattr(overlay, "bind", lambda *args: object())
    monkeypatch.setattr(support, "finalize_identities", AsyncMock())
    monkeypatch.setattr(support, "validate", AsyncMock())
    database = SimpleNamespace(
        scalar=AsyncMock(side_effect=[11, 12, 13, datetime.datetime(2026, 1, 3, tzinfo=datetime.timezone.utc)])
    )
    dependency = SimpleNamespace(db=database, SUPPORT_TABLE_MODELS=())
    context_by_field = {
        "partial_provider_directory_affected_group_table": "groups",
        "partial_provider_directory_affected_live_location_table": "locations",
    }
    descriptor = await patch.finalize(dependency, "mrf", "stage", context_by_field)
    assert len(descriptor.owned_relations) == 9
    assert descriptor.support_stages == prototype.support_stages
    assert build.descriptor is descriptor


@pytest.mark.asyncio
async def test_publish_keeps_heap_indexes_and_caller_owned_authority(build, monkeypatch):
    monkeypatch.setattr(overlay, "assert_applied", AsyncMock())
    build.task = asyncio.current_task()
    descriptor = _descriptor(build)
    admission = descriptor.admission
    admission.assert_ready = AsyncMock()
    admission.assert_cutover_complete = AsyncMock()
    statements = []

    async def scalar(statement, **_params):
        return 99 if statement in ("record-geo", "activate-geo") else True

    async def status(statement, **_params):
        statements.append(statement)
        return 1

    authority = SimpleNamespace(as_dict=lambda: {"generation": 2, "relation_oids": [99, 100]})
    dependency = SimpleNamespace(
        db=SimpleNamespace(scalar=scalar, status=status),
        require_caller_owned_cutover_transaction=Mock(),
        lock_selected_publication_dependencies=AsyncMock(),
        address_alias_sql=SimpleNamespace(alias_advisory_xact_lock_sql=lambda: "alias-lock"),
        _address_alias_generation=AsyncMock(return_value=4),
        _assert_provider_directory_overlay_alias_fence=AsyncMock(),
        candidate_preparation=SimpleNamespace(
            assert_dataset_pins=AsyncMock(),
            _assert_prepared_stages=AsyncMock(),
            prepared_doctors=SimpleNamespace(assert_applied_dependencies=AsyncMock()),
        ),
        _entity_address_column_list=lambda: "location_key,value",
        _record_geo_assurance_candidate_sql=lambda *args, **kwargs: "record-geo",
        _activate_geo_assurance_candidate_sql=lambda *args: "activate-geo",
        result_generation=SimpleNamespace(publish_local_entity_address_generation=AsyncMock(return_value=authority)),
        SUPPORT_TABLE_MODELS=(),
    )
    monkeypatch.setattr(patch, "_live_identity", AsyncMock(return_value=build.incumbent))
    monkeypatch.setattr(ordinary, "require_prepared", lambda prepared, admission: object())
    monkeypatch.setattr(support, "validate", AsyncMock())
    monkeypatch.setattr(support, "apply_statements", lambda *args: ("apply six support candidates",))
    prepared = SimpleNamespace(
        affected_patch=descriptor,
        committed=False,
        preparation_input=SimpleNamespace(address_alias_generation=4),
        context={"merged_result_rows": 101},
        native_dependencies=None,
    )
    callbacks = SimpleNamespace(before_cutover=AsyncMock(), after_publish=AsyncMock())
    assert await patch.publish(dependency, prepared, callbacks) == authority.as_dict()
    assert statements[0].endswith(" IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    assert all("mrf." + target_relation in statements[0] for target_relation, _stage, _oid in descriptor.support_stages)
    assert statements[1].startswith("DELETE FROM mrf.entity_address_unified")
    assert statements[2].startswith("INSERT INTO mrf.entity_address_unified")
    admission.assert_cutover_complete.assert_awaited_once()
    callbacks.before_cutover.assert_awaited_once()
    callbacks.after_publish.assert_awaited_once()
    assert not any(word in " ".join(statements) for word in ("DROP", "ALTER", "TRUNCATE"))


def test_inferred_facility_raw_rows_are_recomputed_before_replacement(build):
    sql = native._affected_npi_source_select_sql(
        "mrf", "SELECT entity_type,entity_id,npi,inferred_npi FROM facility", "groups"
    )
    assert "src.inferred_npi" in sql and "previous.inferred_npi" in sql
    assert "previous.entity_type=src.entity_type AND previous.entity_id=src.entity_id" in sql
    assert "src.npi IS NULL AND EXISTS" in sql
