# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained typed reuse requires published logical data evidence and exact inventory content."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import registry_typed_retained_source as retained
from process.registry_source_recipe_store import canonical_registry_source_recipes
from tests.test_registry_typed_source_recipe import _metadata, _recipe


def _predecessor_reads(monkeypatch, previous_recipes, *, validation=None):
    manifest = SimpleNamespace(generation_id=17, manifest_sha256="f" * 64, candidate_id=str(UUID(int=101)))
    current = AsyncMock(return_value=manifest)
    recipes = AsyncMock(return_value=previous_recipes)
    candidates = AsyncMock(return_value={manifest.candidate_id: {"candidate_id": manifest.candidate_id}})
    evidence = AsyncMock(
        return_value={
            "candidate_id": manifest.candidate_id,
            "generation_id": manifest.manifest_sha256,
            "validation": validation,
            "semantic_validation": None,
        }
    )
    inventory = AsyncMock(return_value=(None, ()))
    for name, method in (
        ("resolve_network_serving_manifest", current),
        ("resolve_retained_registry_source_recipes", recipes),
        ("_published_artifact_candidates", candidates),
        ("load_published_artifact_validation", evidence),
        ("load_source_inventory", inventory),
    ):
        monkeypatch.setattr(retained, name, method)
    monkeypatch.setattr(retained, "_artifact_copy_target", lambda candidate: "named-target")
    return SimpleNamespace(
        manifest=manifest,
        current=current,
        recipes=recipes,
        candidates=candidates,
        evidence=evidence,
        inventory=inventory,
    )


@pytest.mark.asyncio
async def test_predecessor_without_typed_recipes_has_no_reuse_records(monkeypatch):
    reads = _predecessor_reads(monkeypatch, ())
    connection = object()
    assert await retained.retained_typed_source_proofs(connection, reads.manifest, (_recipe(),), "registry") == {}
    reads.current.assert_awaited_once_with(
        connection, generation_id=reads.manifest.generation_id, control_schema="registry"
    )
    reads.recipes.assert_awaited_once_with(connection, reads.manifest, control_schema="registry")
    reads.candidates.assert_not_awaited()
    reads.evidence.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["changed_manifest", "changed_recipes"])
async def test_retained_reuse_refuses_changed_logical_predecessor(monkeypatch, damage):
    recipe = _recipe()
    reads = _predecessor_reads(monkeypatch, (recipe,))
    if damage == "changed_manifest":
        reads.current.return_value = object()
    else:
        reads.recipes.side_effect = ValueError("source_recipe_changed")
    with pytest.raises(ValueError):
        await retained.retained_typed_source_proofs(object(), reads.manifest, (recipe,), "registry")
    reads.candidates.assert_not_awaited()
    if damage == "changed_manifest":
        reads.recipes.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_published_data_evidence_requires_fresh_source_check(monkeypatch):
    recipe = _recipe()
    reads = _predecessor_reads(monkeypatch, (recipe,))
    assert await retained.retained_typed_source_proofs(object(), reads.manifest, (recipe,), "registry") == {}
    reads.evidence.assert_awaited_once()
    reads.inventory.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "capture", "catalog", "recipe"])
async def test_complete_logical_inventory_binds_retained_capture(monkeypatch, damage):
    recipe = _recipe()
    reads = _predecessor_reads(monkeypatch, (recipe,), validation={"complete": True})
    contribution, derivation = _metadata(recipe)
    if damage:
        field = {"capture": "capture_sha256", "catalog": "catalog_generation", "recipe": "recipe_sha256"}[damage]
        contribution[field] = "f" * 64
    entry = SimpleNamespace(
        recipe_json=canonical_registry_source_recipes((recipe,))[1:-1],
        contribution_json=json.dumps(contribution),
        office_derivation_json=json.dumps(derivation),
    )
    reads.inventory.return_value = (None, (entry,))
    if damage:
        with pytest.raises(ValueError, match="retained_source_inventory"):
            await retained.retained_typed_source_proofs(object(), reads.manifest, (recipe,), "registry")
    else:
        proofs_by_recipe = await retained.retained_typed_source_proofs(object(), reads.manifest, (recipe,), "registry")
        proof = proofs_by_recipe[contribution["recipe_sha256"]]
        assert proof == retained.RetainedTypedSourceProof(
            contribution["recipe_sha256"],
            reads.manifest.candidate_id,
            reads.manifest.manifest_sha256,
            recipe.source_pin.generation_id,
        )
        assert set(proof.__dataclass_fields__) == {
            "recipe_sha256",
            "candidate_id",
            "generation_id",
            "capture_sha256",
            "semantic_receipt",
        }
    reads.inventory.assert_awaited_once()


def test_retained_read_checks_capture_content_without_connection_identity():
    recipe = _recipe()
    contribution, _ = _metadata(recipe)
    proof = retained.RetainedTypedSourceProof(
        contribution["recipe_sha256"], str(UUID(int=101)), "f" * 64, recipe.source_pin.generation_id
    )
    loaded = SimpleNamespace(capture=recipe.source_pin.capture, require_ready=Mock())
    read = retained.RetainedTypedGraphRead(loaded, proof)
    read.require_ready()
    loaded.require_ready.assert_called_once_with(complete=False)
    loaded.capture = {**loaded.capture, "capture_sha256": "f" * 64}
    with pytest.raises(ValueError, match="retained_source_changed"):
        read.require_ready()
    assert loaded.require_ready.call_count == 1


@pytest.mark.asyncio
async def test_retained_legacy_recipe_lookup_keeps_raw_inventory_commitment(monkeypatch):
    import hashlib

    from process.network_fhir_source_witness_contract import digest
    from process.registry_source_recipe_store import decode_registry_source_recipes
    from tests.test_named_fhir_typed_graph import _legacy_typed_recipe

    document, catalog_generation = _legacy_typed_recipe()
    (recipe,) = decode_registry_source_recipes([document])
    reads = _predecessor_reads(monkeypatch, (recipe,), validation={"complete": True})
    raw_json = json.dumps(document, sort_keys=True, separators=(",", ":"))
    raw_identity = hashlib.sha256(("[" + raw_json + "]").encode()).hexdigest()
    contribution, derivation = _metadata(recipe)
    contribution["recipe_sha256"] = raw_identity
    derivation.update(recipe_sha256=raw_identity, contribution_sha256=digest(contribution))
    entry = SimpleNamespace(
        recipe_json=raw_json, contribution_json=json.dumps(contribution), office_derivation_json=json.dumps(derivation)
    )
    reads.inventory.return_value = (None, (entry,))
    proofs = await retained.retained_typed_source_proofs(object(), reads.manifest, (recipe,), "registry")
    named_identity = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
    assert named_identity != raw_identity
    assert list(proofs) == [named_identity]
    assert proofs[named_identity].recipe_sha256 == raw_identity
    assert recipe.source_pin.catalog_generation == catalog_generation
    contribution["recipe_sha256"] = named_identity
    entry.contribution_json = json.dumps(contribution)
    with pytest.raises(ValueError, match="retained_source_inventory"):
        await retained.retained_typed_source_proofs(object(), reads.manifest, (recipe,), "registry")
