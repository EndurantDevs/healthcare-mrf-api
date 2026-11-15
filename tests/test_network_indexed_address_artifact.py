# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named indexed address editions retain index and full-content validation."""

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_address_artifact as artifact
from process import network_compact_address_origin as origin
from process import network_indexed_address_artifact as indexed
from process.network_address_projection import PinnedAddressSource
from tests.test_network_bootstrap_address_artifact import _descriptor as bootstrap_descriptor


def _descriptor():
    original = bootstrap_descriptor()
    document_by_field = {
        "contract": indexed.CONTRACT,
        "database": original["database"],
        "namespace": {"name": "network_candidate_" + "2" * 32, "oid": 50, "owner_oid": 51},
        "protected_owner_oid": 51,
        "table": {"oid": 52, "relfilenode": 53},
        "generation_id": "d" * 64,
        "origin_artifact": original,
        "catalog": {"indexes": [{"relation_oid": 52, "index_oid": 54, "indisready": True}]},
        "index_definition_sha256": "e" * 64,
        "runtime_roles": original["runtime_roles"],
        "dependency_oids": sorted([*original["dependency_oids"], 52, 54]),
    }
    document_by_field["descriptor_sha256"] = artifact._digest(document_by_field)
    return document_by_field


def _named_descriptor():
    descriptor = indexed._descriptor(_descriptor())
    descriptor["revision"] = 2
    descriptor["descriptor_sha256"] = artifact._digest(
        {key: value for key, value in descriptor.items() if key != "descriptor_sha256"}
    )
    return descriptor


def test_indexed_legacy_projects_names_with_original_content_keys():
    saved = _descriptor()
    descriptor = artifact._descriptor(saved)
    assert descriptor["generation_id"] == saved["generation_id"]
    assert descriptor["descriptor_sha256"] == saved["descriptor_sha256"]
    assert descriptor["table"] == {"schema_name": saved["namespace"]["name"], "table_name": "entity_address_unified"}
    assert indexed.original_address_source(descriptor).generation_id == saved["origin_artifact"]["generation_id"]


@pytest.mark.parametrize(
    "field,value",
    [("generation_id", "invalid"), ("index_definition_sha256", "invalid"), ("unexpected", True), ("revision", True)],
)
def test_named_indexed_descriptor_rejects_invalid_inputs(field, value):
    descriptor = _named_descriptor()
    descriptor[field] = value
    descriptor["descriptor_sha256"] = artifact._digest(
        {key: value for key, value in descriptor.items() if key != "descriptor_sha256"}
    )
    with pytest.raises(ValueError):
        indexed._descriptor(descriptor)


@pytest.mark.parametrize("indexed_predecessor", [False, True])
async def test_indexed_transition_keeps_membership_source_and_original_edition(monkeypatch, indexed_predecessor):
    descriptor = _named_descriptor()
    source = PinnedAddressSource(descriptor["namespace"]["name"], "entity_address_unified", descriptor["generation_id"])
    prior = copy.deepcopy(descriptor if indexed_predecessor else descriptor["origin_artifact"])
    previous_by_field = {"validation_json": {"serving_readiness": {"address_artifact": prior}}}
    candidate_by_field = {"source_generations": {"unified_address": source.generation_id}, "validation_json": {}}
    before = copy.deepcopy(candidate_by_field)
    monkeypatch.setattr(origin, "_retained_origin", AsyncMock(return_value=None))
    monkeypatch.setattr(indexed, "capture_indexed_address_artifact", AsyncMock(return_value=descriptor))
    monkeypatch.setattr(
        origin,
        "resolve_registry_retained_generation",
        AsyncMock(return_value=(SimpleNamespace(compact_layout=object()), previous_by_field)),
    )
    resolved, relation = await origin.resolve_compact_address_origin(
        object(), candidate_by_field, source, control_schema="registry"
    )
    assert resolved == descriptor and relation.schema_name == source.schema_name
    assert relation.table_name == "entity_address_unified" and candidate_by_field == before
    original = prior["origin_artifact"] if indexed_predecessor else prior
    original["bootstrap_receipt"]["parent_receipts"][0]["rows_sha256"] = "f" * 64
    with pytest.raises(ValueError):
        await origin.resolve_compact_address_origin(object(), candidate_by_field, source, control_schema="registry")
    assert candidate_by_field == before


@pytest.mark.parametrize("digest_kind", ["canonical", "legacy", "changed"])
async def test_indexed_verification_keeps_canonical_and_legacy_index_definition_checks(monkeypatch, digest_kind):
    from process import network_membership_serving_indexes as serving

    descriptor = _named_descriptor()
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value="repeatable read"))
    monkeypatch.setattr(indexed.bootstrap, "verify_bootstrap_address_artifact", AsyncMock())
    monkeypatch.setattr(
        indexed,
        "_index_readiness",
        AsyncMock(return_value=descriptor["index_definition_sha256"] if digest_kind == "canonical" else "f" * 64),
    )
    legacy = AsyncMock(return_value=descriptor["index_definition_sha256"] if digest_kind == "legacy" else "b" * 64)
    monkeypatch.setattr(serving, "_index_digest", legacy)
    if digest_kind == "changed":
        with pytest.raises(ValueError, match="indexed_artifact_changed"):
            await indexed.verify_indexed_address_artifact(connection, descriptor)
    else:
        relation = await indexed.verify_indexed_address_artifact(connection, descriptor)
        assert relation.schema_name == descriptor["namespace"]["name"]
    assert legacy.await_count == int(digest_kind != "canonical")


@pytest.mark.parametrize("changed", [False, True])
async def test_indexed_equivalence_compares_complete_bounded_rows(monkeypatch, changed):
    from process import network_membership_candidate_indexes as candidate_indexes
    from process import network_membership_serving_indexes as serving

    descriptor = _named_descriptor()
    monkeypatch.setattr(
        indexed,
        "verify_indexed_address_artifact",
        AsyncMock(
            return_value=SimpleNamespace(
                schema_name=descriptor["namespace"]["name"], table_name="entity_address_unified"
            )
        ),
    )
    monkeypatch.setattr(serving, "_source_semantics", AsyncMock())
    monkeypatch.setattr(
        candidate_indexes,
        "_projection_address_difference",
        AsyncMock(return_value="source.city IS DISTINCT FROM projected.city"),
    )
    ranges = AsyncMock(side_effect=["z", None])
    monkeypatch.setattr(candidate_indexes, "_next_projection_range", ranges)
    counts_by_field = dict(
        source_rows=1, address_rows=1, missing_locations=0, extra_locations=0, changed_address_rows=int(changed)
    )
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=counts_by_field))
    if changed:
        with pytest.raises(ValueError, match="indexed_content_differs"):
            await indexed.prove_indexed_address_equivalence(connection, descriptor)
    else:
        assert await indexed.prove_indexed_address_equivalence(connection, descriptor) == counts_by_field
    assert connection.fetchrow.await_args.args[1:] == ("z",)
    assert ranges.await_count == 2


async def test_unchanged_retained_indexed_dependency_reuses_completed_equivalence(monkeypatch):
    from process import registry_retained_generation

    descriptor = _named_descriptor()
    proof = AsyncMock()
    monkeypatch.setattr(indexed, "prove_indexed_address_equivalence", proof)
    resolved = (
        SimpleNamespace(compact_layout=object()),
        {"validation_json": {"serving_readiness": {"address_artifact": descriptor}}},
    )
    monkeypatch.setattr(
        registry_retained_generation, "resolve_registry_retained_generation", AsyncMock(return_value=resolved)
    )
    await indexed.prove_new_indexed_address_dependency(object(), {}, descriptor, control_schema="registry")
    proof.assert_not_awaited()
    resolved[0].compact_layout = None
    await indexed.prove_new_indexed_address_dependency(object(), {}, descriptor, control_schema="registry")
    proof.assert_awaited_once()
