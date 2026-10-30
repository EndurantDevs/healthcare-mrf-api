# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inventory metadata never permits the legacy missing-custody fallback."""

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
from tests.test_registry_source_inventory_contract import _entries
from tests.test_registry_source_inventory_reference import _candidate
from tests.test_registry_source_recipe_store_postgres import _example


def _registered(count):
    inventory = SourceInventoryReference(source_inventory_root(_entries(_example(index) for index in range(count))))
    identity = UUID(int=1)
    return {
        **_candidate(inventory),
        **{field: str(identity) for field in ("dataset_id", "schema_id", "producer_id", "candidate_id")},
        "schema_name": "network_candidate_" + identity.hex,
        "validation_json": {},
    }


@pytest.mark.parametrize("count", [0, 1])
async def test_inventory_custody_is_required_even_without_contributions(monkeypatch, count):
    candidate = _registered(count)
    verify = AsyncMock(return_value={"seal_sha256": "a" * 64})
    monkeypatch.setattr(publication, "verify_network_candidate_custody", verify)
    result = await publication._candidate_custody(object(), candidate, "fixture")
    assert result == {"seal_sha256": "a" * 64}
    assert verify.await_args.kwargs == {"control_schema": "fixture", "required": True}
    assert source_inventory_heaps(candidate) == {"network_source_inventory", "network_provider_only_evidence"}


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("count", [0, 1])
async def test_rollback_requires_inventory_custody_and_matching_physical_indexes(monkeypatch, compact, count):
    from process import network_candidate_custody as custody
    from process import registry_publication_execution as execution

    candidate_by_name = {**_registered(count), "state": "ready", "expected_head": 1, "approved_custom_revision": 2}
    if compact:
        candidate_by_name["validation_json"] = {"serving_readiness": {"component": indexes.COMPONENT}}
    copy_target = SimpleNamespace(schema_name=candidate_by_name["schema_name"])
    command = SimpleNamespace(expected_serving_generation=1, expected_approved_revision=2)
    monkeypatch.setattr(execution, "_locked_candidate", AsyncMock(return_value=candidate_by_name))
    native = AsyncMock(return_value={"seal_sha256": "a" * 64})
    monkeypatch.setattr(custody, "verify_network_candidate_custody", native)
    monkeypatch.setattr(execution, "verify_network_candidate_writer_closure", AsyncMock())
    monkeypatch.setattr(
        execution, "_publication_readiness", lambda _: ({}, None, {"index_definition_sha256": "b" * 64})
    )
    legacy = AsyncMock(return_value="b" * 64)
    compact_indexes = AsyncMock(return_value="b" * 64)
    monkeypatch.setattr(execution, "_catalog_readiness", AsyncMock())
    monkeypatch.setattr(execution, "_index_digest", legacy)
    monkeypatch.setattr(indexes, "compact_index_digest", compact_indexes)
    actual, digest = await execution._verified_source_rollback_candidate(object(), copy_target, command, {}, "fixture")
    assert actual == candidate_by_name and len(digest) == 64
    assert native.await_args.kwargs == {"control_schema": "fixture", "required": True}
    assert compact_indexes.await_count == int(compact) and legacy.await_count == int(not compact)
    if compact:
        assert compact_indexes.await_args.kwargs == {"candidate": candidate_by_name}
    selected = compact_indexes if compact else legacy
    selected.return_value = "c" * 64
    with pytest.raises(ValueError, match="registry_source_rollback_candidate_changed"):
        await execution._verified_source_rollback_candidate(object(), copy_target, command, {}, "fixture")
    execution.verify_network_candidate_writer_closure.assert_not_awaited()


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


@pytest.mark.parametrize("missing", ["network_source_inventory", "network_provider_only_evidence"])
def test_publication_closure_requires_both_inventory_heaps(missing):
    candidate = _registered(0)
    relation_oid_by_name = {
        name: index + 1
        for index, name in enumerate(
            (
                "network_membership",
                "provider_location_binding",
                "entity_address_unified",
                "network_source_inventory",
                "network_provider_only_evidence",
            )
        )
    }
    closure_by_field = {
        "component": "network_candidate_writer_closure",
        "revision": 1,
        "scope": {
            field: candidate[field]
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        },
        "owner_role": "owner_fixture",
        "loader_roles": [],
        "reader_roles": [],
        "relation_oids": relation_oid_by_name,
    }
    assert publication._publication_writer_closure(candidate, {"writer_closure": closure_by_field}) == closure_by_field
    relation_oid_by_name.pop(missing)
    with pytest.raises(publication.NetworkPublicationError, match="serving storage"):
        publication._publication_writer_closure(candidate, {"writer_closure": closure_by_field})


def test_unsealed_empty_inventory_cannot_use_legacy_manifest_closure(monkeypatch):
    candidate = _registered(0)
    monkeypatch.setattr(serving, "_publication_readiness", lambda _candidate: (None, None, None, None))
    monkeypatch.setattr(serving, "_retained_manifest", lambda *_args, **_kwargs: candidate)
    monkeypatch.setattr(
        serving, "_verified_writer_closure", lambda *_args: pytest.fail("Unsealed inventory reached legacy closure")
    )
    with pytest.raises(serving.NetworkServingReadUnavailable, match="protected custody"):
        serving._pinned_manifest({"candidate_json": json.dumps(candidate), "manifest_json": "{}"})


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
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
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
