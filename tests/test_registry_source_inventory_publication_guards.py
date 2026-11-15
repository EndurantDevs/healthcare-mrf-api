# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inventory publication keeps complete named payloads and completed logical validation."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_compact_readiness as indexes
from process import network_membership_publication as publication
from process import network_provider_only_projection as projection
from process import network_serving_read as serving
from process import registry_candidate_composition as composition
from process.registry_source_inventory_contract import source_inventory_root
from process.registry_source_inventory_reference import SourceInventoryReference, source_inventory_heaps
from tests.registry_source_recipe_support import _example
from tests.test_registry_source_inventory_contract import _entries
from tests.test_registry_source_inventory_reference import _candidate


def _registered(count):
    inventory = SourceInventoryReference(source_inventory_root(_entries(_example(index) for index in range(count))))
    identity = UUID(int=1)
    return {
        **_candidate(inventory),
        **{field: str(identity) for field in ("dataset_id", "schema_id", "producer_id", "candidate_id")},
        "schema_name": "network_candidate_" + identity.hex,
        "validation_json": {},
    }


def _validated(candidate):
    fields = (
        "raw_rows",
        "batch_rows",
        "batch_count",
        "membership_rows",
        "distinct_memberships",
        "projected_locations",
        "orphan_bindings",
        "unknown_network_rows",
    )
    report_by_field = dict(candidate["validation_json"])
    report_by_field.update(dict.fromkeys(fields, 0))
    report_by_field["final_validation"] = {
        "candidate_id": candidate["candidate_id"],
        "schema_name": candidate["schema_name"],
        "source_generations": candidate["source_generations"],
        "complete": True,
        "counts": {field: report_by_field[field] for field in fields},
    }
    return {**candidate, "validation_json": report_by_field}


@pytest.mark.parametrize("count", [0, 1])
def test_inventory_snapshot_names_include_both_payload_tables(count):
    candidate = _registered(count)
    tables = publication._publication_snapshot_tables(candidate, candidate["validation_json"])
    assert tables["schema_name"] == candidate["schema_name"]
    assert set(tables["tables"]) == {
        "network_membership",
        "provider_location_binding",
        "entity_address_unified",
        "network_source_inventory",
        "network_provider_only_evidence",
    }
    assert source_inventory_heaps(candidate) == {"network_source_inventory", "network_provider_only_evidence"}


@pytest.mark.parametrize("failure", [None, "validation", "readiness"])
async def test_ready_inventory_replay_keeps_current_validation_and_readiness(monkeypatch, failure):
    from process import network_membership_pipeline as pipeline

    candidate_by_field = {**_registered(1), "state": "ready"}
    events = []
    monkeypatch.setattr(pipeline, "_locked_candidate", AsyncMock(return_value=candidate_by_field))
    monkeypatch.setattr(pipeline, "_scope_report", lambda *args: {})

    def check(stage):
        def validate(*arguments):
            events.append(stage)
            if failure == stage:
                raise ValueError("changed_" + stage)

        return validate

    monkeypatch.setattr(pipeline, "_validated_replay", check("validation"))
    monkeypatch.setattr(pipeline, "_publication_readiness", check("readiness"))
    if failure:
        with pytest.raises(ValueError, match="changed_" + failure):
            await pipeline._prepare_phase_snapshot(object(), object(), object(), {}, "fixture")
    else:
        await pipeline._prepare_phase_snapshot(object(), object(), object(), {}, "fixture")
    assert events == (["validation"] if failure == "validation" else ["validation", "readiness"])


@pytest.mark.parametrize("damage", ["complete", "candidate", "source", "counts"])
def test_inventory_final_validation_requires_exact_logical_data(damage):
    candidate = _validated(_registered(1))
    assert publication._require_final_validation(candidate) is candidate["validation_json"]["final_validation"]
    record = candidate["validation_json"]["final_validation"]
    if damage == "complete":
        record["complete"] = False
    elif damage == "candidate":
        record["candidate_id"] = str(UUID(int=2))
    elif damage == "source":
        record["source_generations"] = {}
    else:
        record["counts"]["membership_rows"] = 1
    with pytest.raises(publication.NetworkPublicationError):
        publication._require_final_validation(candidate)


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("count", [0, 1])
async def test_rollback_requires_final_data_and_matching_structural_indexes(monkeypatch, compact, count):
    from process import registry_publication_execution as execution

    candidate_by_field = {**_registered(count), "state": "ready", "expected_head": 1, "approved_custom_revision": 2}
    if compact:
        candidate_by_field["validation_json"] = {"serving_readiness": {"component": indexes.COMPONENT}}
    candidate_by_field = _validated(candidate_by_field)
    copy_target = SimpleNamespace(schema_name=candidate_by_field["schema_name"])
    command = SimpleNamespace(expected_serving_generation=1, expected_approved_revision=2)
    monkeypatch.setattr(execution, "_locked_candidate", AsyncMock(return_value=candidate_by_field))
    monkeypatch.setattr(
        execution, "_publication_readiness", lambda _: ({}, None, {"index_definition_sha256": "b" * 64})
    )
    legacy, compact_indexes = AsyncMock(return_value="b" * 64), AsyncMock(return_value="b" * 64)
    monkeypatch.setattr(execution, "_catalog_readiness", AsyncMock())
    monkeypatch.setattr(execution, "_index_digest", legacy)
    monkeypatch.setattr(indexes, "compact_index_digest", compact_indexes)
    actual, digest = await execution._verified_source_rollback_candidate(object(), copy_target, command, {}, "fixture")
    assert actual == candidate_by_field and len(digest) == 64
    assert compact_indexes.await_count == int(compact) and legacy.await_count == int(not compact)
    if compact:
        assert compact_indexes.await_args.kwargs == {"candidate": candidate_by_field}
    selected = compact_indexes if compact else legacy
    selected.return_value = "c" * 64
    with pytest.raises(ValueError, match="registry_source_rollback_candidate_changed"):
        await execution._verified_source_rollback_candidate(object(), copy_target, command, {}, "fixture")


@pytest.mark.parametrize("count", [0, 1])
async def test_rollback_selection_requirement_uses_verified_count_not_reference_truthiness(monkeypatch, count):
    from process import registry_publication_execution as execution

    report = dict.fromkeys(
        ("membership_rows", "distinct_memberships", "projected_locations", "orphan_bindings", "unknown_network_rows"), 0
    )
    report.update(valid=True, serving_readiness={"ready": True})
    candidate_by_name = {**_registered(count), "state": "ready", "validation_json": report}
    monkeypatch.setattr(
        execution, "_verified_source_rollback_candidate", AsyncMock(return_value=(candidate_by_name, "a" * 64))
    )
    difference_by_name = {"has_imported": False, "additions": 0, "removals": 0, "changes": 0, "samples": {}}
    monkeypatch.setattr(execution, "_source_rollback_difference", AsyncMock(return_value=difference_by_name))
    monkeypatch.setattr(execution, "verify_registry_source_selection_receipt", lambda _: None)
    monkeypatch.setattr(execution, "_source_rollback_result", lambda *_args: None)
    command = SimpleNamespace(expected_approved_revision=2)
    operation = execution._source_rollback_preview_result(
        object(),
        SimpleNamespace(candidate_id="candidate_by_name"),
        SimpleNamespace(generation_id=1),
        SimpleNamespace(generation_id=2),
        command,
        {},
        "fixture",
    )
    if count:
        with pytest.raises(ValueError, match="registry_source_rollback_selection_unproven"):
            await operation
    else:
        preview_by_name = await operation
        assert preview_by_name["source_selection"] == {"mapped_rows": 0, "omitted_rows": 0}


def _index_rows():
    paths = [
        ("network_slice_ref", ["network_id", "office_bucket"], True, True),
        ("network_office_edge", ["slice_id", "location_key"], True, True),
        ("network_office_edge", ["location_key", "slice_id"], False, False),
        *indexes._inventory_indexes(),
    ]
    return [
        dict(
            table_name=table,
            columns=columns,
            indisprimary=primary,
            indisunique=unique,
            indisvalid=True,
            indisready=True,
            indislive=True,
            indnkeyatts=len(columns),
            indnatts=len(columns),
            amname="btree",
            no_predicate=True,
            no_expressions=True,
        )
        for table, columns, primary, unique in paths
    ]


@pytest.mark.parametrize("missing", [3, 4, 5, 6, 7])
async def test_inventory_lookup_and_identity_indexes_are_mandatory(missing):
    rows = _index_rows()
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=rows))
    candidate = _registered(0)
    assert len(await indexes.compact_index_digest(connection, "fixture", candidate=candidate)) == 64
    rows.pop(missing)
    with pytest.raises(indexes.NetworkCompactReadinessError):
        await indexes.compact_index_digest(connection, "fixture", candidate=candidate)


async def test_preparing_provider_projection_uses_stage_facts_and_open_scope(monkeypatch):
    events = []
    copy_target = SimpleNamespace(schema_name="fixture")
    stage, approved = object(), object()
    recipes_by_identity, descriptors_by_identity = {}, {}
    monkeypatch.setattr(
        projection,
        "_staged_sources",
        lambda _recipes, actual: (
            (recipes_by_identity, descriptors_by_identity) if actual is stage else pytest.fail("Wrong producer stage")
        ),
    )

    async def scope(_connection, actual, authority, schema, *, preparing):
        assert actual is copy_target and authority is approved and schema == "control" and preparing
        events.append("scope")

    async def create(*_args, **_options):
        events.append("create")

    async def append(_connection, relation, recipes, descriptors, actual, authority, schema):
        assert relation == '"fixture".network_provider_only_evidence'
        assert recipes is recipes_by_identity and descriptors is descriptors_by_identity
        assert actual is stage and authority is approved and schema == "control"
        events.append("append")

    monkeypatch.setattr(projection, "_candidate", scope)
    monkeypatch.setattr(projection, "create_provider_only_projection", create)
    monkeypatch.setattr(projection, "_append_staged_sources", append)
    monkeypatch.setattr(projection, "_rebuild_source", AsyncMock(side_effect=AssertionError("Raw SOURCE reopened")))
    await projection.prepare_provider_only_projection(
        object(), copy_target, (), stage, approved, control_schema="control"
    )
    assert events == ["scope", "create", "append"]


async def test_retained_flat_fallback_discards_inherited_empty_selection_marker(monkeypatch):
    generations_by_name = {"registry_source_membership": "a" * 64}
    manifest = SimpleNamespace(source_generations={})
    inputs = composition._CompositionInputs(object(), None, object(), None, manifest)
    stage = composition.RegistrySourceRecipeStage("fixture", 3, 0, "b" * 64)
    monkeypatch.setattr(composition, "_retained_source", AsyncMock(return_value=(generations_by_name, 3)))
    monkeypatch.setattr(composition, "_selected_composition_sources", AsyncMock(return_value=((), (), False)))
    monkeypatch.setattr(composition, "_stage_source", AsyncMock(return_value=stage))
    generations, rows, recipes, selected = await composition._composition_sources_for_format(
        inputs, UUID(int=4), "fixture", inventory=True
    )
    assert generations == {} and rows == 3 and recipes == () and selected.inventory is True
