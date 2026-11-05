# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sampled receipts remain distinct from full proof across sealing and reuse."""

import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_candidate_custody as custody
from process import network_sampled_custody as sampled
from process import registry_typed_retained_source as retained
from process.network_fhir_sampled_validation import _LIMITATIONS, CONTRACT, SemanticSamplingPolicy
from process.network_fhir_source_witness_contract import digest
from process.registry_source_recipe_store import canonical_registry_source_recipes
from tests.test_registry_typed_source_recipe import _recipe

CANDIDATE = str(UUID(int=101))


def _semantic_receipt(capture_sha256="a" * 64):
    receipt_by_field = {
        "contract": CONTRACT,
        "mode": "sampled",
        "seed": "b" * 64,
        "algorithm": "sha256-bottom-k-v1",
        "series_count": 10,
        "items_per_series": 100,
        "source_origin_sha256": "c" * 64,
        "capture_id": str(UUID(int=102)),
        "observed_at": "2026-01-01T00:00:00.000000+00:00",
        "capture_sha256": capture_sha256,
        "database_identity": {
            "database_name": "synthetic",
            "database_oid": 100,
            "system_identifier": "123",
            "postmaster_started": "2026-01-01T00:00:00.000000+00:00",
            "recovering": False,
        },
        "membership_as_of": "2026-01-01",
        "networks": [
            {
                "network_source_key": "network-a",
                "source_to_candidate": {
                    "unit": "source-role-witness",
                    "population": 0,
                    "selected": 0,
                    "series_counts": [0] * 10,
                    "derived_pair_rows": 0,
                    "failures": 0,
                },
                "candidate_to_source": {
                    "unit": "distinct-provider-office-pair",
                    "population": 0,
                    "selected": 0,
                    "series_counts": [0] * 10,
                    "population_lower_bound": 0,
                    "provenance_rows": 0,
                    "witness_rows": 0,
                    "failures": 0,
                },
            }
        ],
        "limitations": list(_LIMITATIONS),
    }
    receipt_by_field["receipt_sha256"] = digest(receipt_by_field)
    return receipt_by_field


def _sampled_document(capture_sha256="a" * 64):
    run = sampled.SampledValidationRun(SemanticSamplingPolicy("b" * 64), CANDIDATE)
    run._record(capture_sha256, _semantic_receipt(capture_sha256), "sampled", None)
    document_by_field = {
        "contract": sampled.CONTRACT,
        "candidate_id": CANDIDATE,
        "seed": run.policy.seed,
        "sources": list(run._receipts.values()),
        "preserved_lineage": None,
        "exhaustive_checks": list(sampled._CHECKS),
    }
    document_by_field["sha256"] = digest(document_by_field)
    return document_by_field


def test_sampled_custody_requires_new_revision():
    semantic = _sampled_document()
    receipt_by_field = {
        "component": "network_candidate_custody",
        "revision": 5,
        "control": {},
        "candidate_sha256": "d" * 64,
        "closure": {},
        "index_definition_sha256": "e" * 64,
        "runtime_roles": ["synthetic_reader"],
        "semantic_validation": semantic,
    }
    seal_by_field = {
        "receipt_json": custody._encoded(receipt_by_field).decode(),
        "seal_sha256": custody._digest(receipt_by_field),
    }
    assert custody._sealed_receipt(seal_by_field) == receipt_by_field
    receipt_by_field["revision"] = 4
    seal_by_field = {
        "receipt_json": custody._encoded(receipt_by_field).decode(),
        "seal_sha256": custody._digest(receipt_by_field),
    }
    with pytest.raises(custody.NetworkCandidateCustodyError):
        custody._sealed_receipt(seal_by_field)


@pytest.mark.parametrize("damage", ["empty", "duplicate", "wrong_candidate", "seed", "checks", "hash", "cycle"])
def test_sampled_custody_refuses_incomplete_or_changed_evidence(damage):
    document = _sampled_document()
    retained_by_field = document["sources"][0] | {
        "method": "sampled-retained",
        "predecessor": {"candidate_id": CANDIDATE, "seal_sha256": "d" * 64},
    }
    changes_by_damage = {
        "empty": {"sources": []},
        "duplicate": {"sources": document["sources"] * 2},
        "wrong_candidate": {"candidate_id": str(UUID(int=103))},
        "seed": {"seed": "c" * 64},
        "checks": {"exhaustive_checks": document["exhaustive_checks"][:-1]},
        "cycle": {"sources": [retained_by_field]},
        "hash": {},
    }
    document.update(changes_by_damage[damage])
    if damage != "hash":
        document["sha256"] = digest({key: value for key, value in document.items() if key != "sha256"})
    else:
        document["sha256"] = "0" * 64
    with pytest.raises(ValueError):
        sampled.validate_sampled_custody(document, candidate_id=CANDIDATE)


@pytest.mark.asyncio
async def test_exhaustive_reuse_cannot_promote_sampled_predecessor(monkeypatch):
    recipe = _recipe()
    capture = recipe.source_pin.generation_id
    manifest = SimpleNamespace(generation_id=2, candidate_id=CANDIDATE)
    semantic = _sampled_document(capture)
    custody_by_field = {
        "receipt": {
            "revision": 5,
            "semantic_validation": semantic,
            "closure": {"relation_oids": {retained.INVENTORY_TABLE: 123}},
        },
        "seal_sha256": "d" * 64,
    }
    entry = SimpleNamespace(recipe_json=canonical_registry_source_recipes((recipe,))[1:-1])
    for name, value in (
        ("resolve_network_serving_manifest", manifest),
        ("resolve_retained_registry_source_recipes", (recipe,)),
        ("_published_artifact_candidates", {CANDIDATE: {"candidate_id": CANDIDATE}}),
        ("_verify_artifact_owner", custody_by_field),
        ("_load_locked_source_inventory", (None, [entry])),
    ):
        monkeypatch.setattr(retained, name, AsyncMock(return_value=value))
    monkeypatch.setattr(retained, "_artifact_copy_target", lambda candidate: object())
    assert await retained.retained_typed_source_proofs(object(), manifest, (recipe,), "synthetic") == {}


@pytest.mark.asyncio
async def test_sampling_run_refuses_changed_transaction(monkeypatch):
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    monkeypatch.setattr(sampled, "_virtual_xid", AsyncMock(side_effect=["1/1", "1/2"]))
    run = sampled.SampledValidationRun(SemanticSamplingPolicy("b" * 64), CANDIDATE)
    document = _sampled_document()
    source = copy.deepcopy(document["sources"][0])
    run._record(source["capture_sha256"], source["receipt"], source["method"], None)
    await run._bind(connection)
    with pytest.raises(ValueError, match="sampling_scope_changed"):
        await run.finish(connection, CANDIDATE)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sampled_keys,current_keys,expected",
    [
        (["network-a"], ["network-a"], True),
        (["network-a"], ["network-a", "network-b"], False),
        (["network-a", "network-b"], ["network-b"], True),
    ],
)
async def test_retained_sample_requires_current_approved_network_coverage(
    monkeypatch, sampled_keys, current_keys, expected
):
    from process import network_approved_source_bindings as bindings
    from process import network_fhir_typed_mapping as mapping
    from process.registry_typed_source_stage import _has_retained_sample_coverage

    required = AsyncMock()
    keys = AsyncMock(return_value=current_keys)
    monkeypatch.setattr(bindings, "require_approved_network_source_bindings", required)
    monkeypatch.setattr(mapping, "_approved_graph_keys", keys)
    recipe = _recipe()
    proof = SimpleNamespace(semantic_receipt={"networks": [{"network_source_key": key} for key in sampled_keys]})
    approved = SimpleNamespace(approved_revision=7)
    connection = object()
    assert await _has_retained_sample_coverage(connection, recipe, approved, "synthetic", proof) is expected
    required.assert_awaited_once_with(connection, approved, recipe.binding_coordinates, control_schema="synthetic")
    keys.assert_awaited_once()


@pytest.mark.asyncio
async def test_exhaustive_predecessor_covers_newly_approved_keys():
    from process.registry_typed_source_stage import _has_retained_sample_coverage

    assert await _has_retained_sample_coverage(
        object(), _recipe(), object(), "synthetic", SimpleNamespace(semantic_receipt=None)
    )


@pytest.mark.asyncio
async def test_retained_capability_cannot_cross_transactions(monkeypatch):
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    proof = retained.RetainedTypedSourceProof(
        connection,
        asyncio.current_task(),
        "r",
        CANDIDATE,
        "d" * 64,
        "a" * 64,
        retained._BRAND,
        _semantic_receipt(),
        "1/1",
    )
    loaded = SimpleNamespace(capture={"capture_sha256": "a" * 64}, verify=AsyncMock())
    monkeypatch.setattr(retained, "_virtual_xid", AsyncMock(return_value="1/2"))
    with pytest.raises(ValueError, match="retained_source_transaction"):
        await retained.RetainedTypedGraphRead(loaded, proof).verify(connection)
    loaded.verify.assert_not_awaited()
    monkeypatch.setattr(sampled, "_virtual_xid", AsyncMock(return_value="1/2"))
    run = sampled.SampledValidationRun(SemanticSamplingPolicy("b" * 64), str(UUID(int=103)))
    with pytest.raises(ValueError, match="sampled_retained_transaction"):
        await run.record_retained(proof, connection)


@pytest.mark.asyncio
async def test_repeated_carry_retains_sampled_lineage_and_refuses_full_mode(monkeypatch):
    from process import registry_source_contribution_artifacts as artifacts

    connection = SimpleNamespace(is_in_transaction=lambda: True)
    parent = _sampled_document()
    monkeypatch.setattr(sampled, "_virtual_xid", AsyncMock(return_value="1/1"))
    for current_id in (103, 104):
        preservation = SimpleNamespace(
            receipt={"carried_rows": 1}, predecessor=SimpleNamespace(candidate_id=parent["candidate_id"])
        )
        custody_by_field = {"seal_sha256": "d" * 64, "receipt": {"revision": 5, "semantic_validation": parent}}
        monkeypatch.setattr(
            artifacts, "_published_artifact_candidates", AsyncMock(return_value={parent["candidate_id"]: {}})
        )
        monkeypatch.setattr(artifacts, "_verify_artifact_owner", AsyncMock(return_value=custody_by_field))
        with pytest.raises(ValueError, match="sampled_preservation_requires_policy"):
            await sampled.inherit_preserved_sampling(connection, preservation, None, "synthetic")
        run = sampled.SampledValidationRun(SemanticSamplingPolicy("b" * 64), str(UUID(int=current_id)))
        await sampled.inherit_preserved_sampling(
            connection, preservation, SimpleNamespace(semantic_validation=run), "synthetic"
        )
        current = await run.finish(connection, run.candidate_id)
        assert current["sources"] == []
        assert current["preserved_lineage"] == {
            "candidate_id": parent["candidate_id"],
            "seal_sha256": "d" * 64,
            "semantic_sha256": parent["sha256"],
        }
        assert sampled.validate_sampled_custody(current) is current
        parent = current
