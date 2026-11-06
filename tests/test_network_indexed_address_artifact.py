# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Indexed dependencies require exact native pins and independent final proof."""

import copy
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_address_artifact as artifact
from process import network_compact_address_origin as origin
from process import network_compact_retention as retention
from process import network_indexed_address_artifact as indexed
from process.network_address_projection import PinnedAddressSource
from tests.test_network_bootstrap_address_artifact import _descriptor as bootstrap_descriptor


def _descriptor():
    original = bootstrap_descriptor()
    descriptor_by_field = {
        "contract": indexed.CONTRACT,
        "database": original["database"],
        "namespace": {"name": "network_candidate_" + "2" * 32, "oid": 50, "owner_oid": 51},
        "protected_owner_oid": 51,
        "table": {"oid": 52, "relfilenode": 53},
        "generation_id": "d" * 64,
        "origin_artifact": original,
        "catalog": {
            "shapes": [],
            "columns": [],
            "constraints": [],
            "indexes": [
                {"relation_oid": 52, "index_oid": 54, "indisvalid": True, "indisready": True, "indislive": True}
            ],
        },
        "index_definition_sha256": "e" * 64,
        "runtime_roles": original["runtime_roles"],
        "dependency_oids": sorted([*original["dependency_oids"], 52, 54]),
    }
    descriptor_by_field["descriptor_sha256"] = artifact._digest(descriptor_by_field)
    return descriptor_by_field


def test_indexed_descriptor_preserves_original_origin_and_native_dependencies():
    descriptor = _descriptor()
    assert artifact._descriptor(descriptor) == descriptor
    assert indexed.original_address_source(descriptor).generation_id == descriptor["origin_artifact"]["generation_id"]
    assert descriptor["generation_id"] != descriptor["origin_artifact"]["generation_id"]


@pytest.mark.parametrize("damage", ["oid", "owner", "generation", "roles", "dependencies", "index", "origin"])
def test_rehashed_indexed_descriptor_rejects_invalid_structure(damage):
    descriptor = _descriptor()
    match damage:
        case "oid":
            descriptor["table"]["oid"] = 52.0
        case "owner":
            descriptor["protected_owner_oid"] = 90
        case "generation":
            descriptor["generation_id"] = "unproved"
        case "roles":
            descriptor["runtime_roles"] = ["reader"]
        case "dependencies":
            descriptor["dependency_oids"].remove(54)
        case "index":
            descriptor["catalog"]["indexes"][0]["indisready"] = False
        case "origin":
            descriptor["origin_artifact"]["generation_id"] = "f" * 64
    descriptor["descriptor_sha256"] = artifact._digest(
        {key: value for key, value in descriptor.items() if key != "descriptor_sha256"}
    )
    with pytest.raises(artifact.NetworkAddressArtifactError):
        artifact._descriptor(descriptor)


async def test_only_independently_resolved_compact_predecessor_reuses_content_proof(monkeypatch):
    from process import registry_retained_generation

    descriptor = _descriptor()
    proof = AsyncMock()
    monkeypatch.setattr(indexed, "prove_indexed_address_equivalence", proof)
    resolver = AsyncMock(
        return_value=(
            SimpleNamespace(compact_layout=object()),
            {"validation_json": {"serving_readiness": {"address_artifact": descriptor}}},
        )
    )
    monkeypatch.setattr(registry_retained_generation, "resolve_registry_retained_generation", resolver)
    await indexed.prove_new_indexed_address_dependency(object(), {}, descriptor, control_schema="registry")
    proof.assert_not_awaited()
    resolver.return_value[0].compact_layout = None
    await indexed.prove_new_indexed_address_dependency(object(), {}, descriptor, control_schema="registry")
    proof.assert_awaited_once()


async def test_indexed_transition_preserves_current_membership_source_and_requires_same_original(monkeypatch):
    descriptor = _descriptor()
    source = PinnedAddressSource(descriptor["namespace"]["name"], "entity_address_unified", descriptor["generation_id"])
    previous_by_field = {"validation_json": {"serving_readiness": {"address_artifact": descriptor["origin_artifact"]}}}
    candidate_by_field = {
        "source_generations": {"unified_address": source.generation_id, "retained_network_source": "c" * 64}
    }
    before = copy.deepcopy(candidate_by_field)
    monkeypatch.setattr(origin, "_retained_origin", AsyncMock(return_value=None))
    monkeypatch.setattr(indexed, "capture_indexed_address_artifact", AsyncMock(return_value=descriptor))
    monkeypatch.setattr(
        origin,
        "resolve_registry_retained_generation",
        AsyncMock(return_value=(SimpleNamespace(compact_layout=object()), previous_by_field)),
    )
    result, relation = await origin.resolve_compact_address_origin(
        object(), candidate_by_field, source, runtime_roles=("reader", "worker"), control_schema="registry"
    )
    assert result == descriptor and relation.table_oid == 52 and candidate_by_field == before
    previous_by_field["validation_json"]["serving_readiness"]["address_artifact"] = bootstrap_descriptor(
        sequence_oid=26
    )
    with pytest.raises(ValueError, match="readiness differs"):
        await origin.resolve_compact_address_origin(
            object(), candidate_by_field, source, runtime_roles=("reader", "worker"), control_schema="registry"
        )


def test_retention_keeps_indexed_heap_indexes_and_authenticated_bootstrap(monkeypatch):
    descriptor = _descriptor()
    source = PinnedAddressSource(descriptor["namespace"]["name"], "entity_address_unified", descriptor["generation_id"])
    candidate_by_field = {"state": "published", "validation_json": {"address_source": asdict(source)}}
    monkeypatch.setattr(retention, "_scope_report", lambda *_: {})
    monkeypatch.setattr(
        retention,
        "compact_readiness",
        lambda *_: ({"edge_heap_oid": 60, "ref_map_oid": 61}, {"address_artifact": descriptor, "edge_artifacts": []}),
    )
    receipt_by_field = {"closure": {"relation_oids": {"network_office_edge": 60, "network_slice_ref": 61}}}
    assert retention._dependencies(candidate_by_field, receipt_by_field) == {*descriptor["dependency_oids"], 60, 61}
