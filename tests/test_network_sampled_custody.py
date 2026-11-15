# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sampled receipts remain distinct from full proof across sealing and reuse."""

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

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


def test_sampled_document_records_logical_data_checks_only():
    document = _sampled_document()
    assert sampled.validate_sampled_custody(document) == document
    assert document["exhaustive_checks"] == [
        "authorization",
        "source_identity",
        "capture_content_hashes",
        "candidate_accounting",
        "office_mapping",
        "compact_parity",
    ]
    assert "database_identity" not in document["sources"][0]["receipt"]


@pytest.mark.parametrize("damage", ["empty", "duplicate", "wrong_candidate", "seed", "checks", "hash", "cycle"])
def test_sampled_custody_refuses_incomplete_or_changed_evidence(damage):
    document = _sampled_document()
    retained_by_field = document["sources"][0] | {
        "method": "sampled-retained",
        "predecessor": {"candidate_id": CANDIDATE, "generation_id": "f" * 64},
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
    manifest = SimpleNamespace(generation_id=17, manifest_sha256="f" * 64, candidate_id=CANDIDATE)
    semantic = _sampled_document(capture)
    evidence_by_field = {
        "candidate_id": CANDIDATE,
        "generation_id": "f" * 64,
        "validation": {"complete": True},
        "semantic_validation": semantic,
    }
    entry = SimpleNamespace(recipe_json=canonical_registry_source_recipes((recipe,))[1:-1])
    for name, value in (
        ("resolve_network_serving_manifest", manifest),
        ("resolve_retained_registry_source_recipes", (recipe,)),
        ("_published_artifact_candidates", {CANDIDATE: {"candidate_id": CANDIDATE}}),
        ("load_published_artifact_validation", evidence_by_field),
        ("load_source_inventory", (None, [entry])),
    ):
        monkeypatch.setattr(retained, name, AsyncMock(return_value=value))
    monkeypatch.setattr(retained, "_artifact_copy_target", lambda candidate: object())
    assert await retained.retained_typed_source_proofs(object(), manifest, (recipe,), "synthetic") == {}


@pytest.mark.asyncio
async def test_sampling_run_keeps_candidate_data_across_read_handles():
    run = sampled.SampledValidationRun(SemanticSamplingPolicy("b" * 64), CANDIDATE)
    source = copy.deepcopy(_sampled_document()["sources"][0])
    run._record(source["capture_sha256"], source["receipt"], source["method"], None)
    assert (await run.finish(object(), CANDIDATE))["sources"] == [source]
    assert (await run.finish(object(), CANDIDATE))["sources"] == [source]
    with pytest.raises(ValueError, match="sampling_candidate_changed"):
        await run.finish(object(), str(UUID(int=103)))


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
async def test_retained_reuse_keeps_sampled_classification():
    from unittest.mock import Mock

    proof = retained.RetainedTypedSourceProof("e" * 64, CANDIDATE, "f" * 64, "a" * 64, _semantic_receipt())
    loaded = SimpleNamespace(capture={"capture_sha256": "a" * 64}, require_ready=Mock())
    retained.RetainedTypedGraphRead(loaded, proof).require_ready()
    loaded.require_ready.assert_called_once_with(complete=False)
    run = sampled.SampledValidationRun(SemanticSamplingPolicy("b" * 64), str(UUID(int=103)))
    await run.record_retained(proof, object())
    document = await run.finish(object(), run.candidate_id)
    assert document["sources"][0]["method"] == "sampled-retained"
    assert document["sources"][0]["receipt"] == proof.semantic_receipt
    assert document["sources"][0]["predecessor"] == {"candidate_id": CANDIDATE, "generation_id": "f" * 64}


@pytest.mark.asyncio
async def test_repeated_carry_retains_sampled_lineage_and_refuses_full_mode(monkeypatch):
    from process import registry_source_contribution_artifacts as artifacts

    connection = SimpleNamespace(is_in_transaction=lambda: True)
    parent = _sampled_document()
    for current_id in (103, 104):
        preservation = SimpleNamespace(
            receipt={"carried_rows": 1}, predecessor=SimpleNamespace(candidate_id=parent["candidate_id"])
        )
        evidence_by_field = {
            "candidate_id": parent["candidate_id"],
            "generation_id": digest({"generation": current_id - 1}),
            "validation": {"complete": True},
            "semantic_validation": parent,
        }
        monkeypatch.setattr(
            artifacts, "_published_artifact_candidates", AsyncMock(return_value={parent["candidate_id"]: {}})
        )
        monkeypatch.setattr(artifacts, "load_published_artifact_validation", AsyncMock(return_value=evidence_by_field))
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
            "generation_id": digest({"generation": current_id - 1}),
            "semantic_sha256": parent["sha256"],
        }
        assert sampled.validate_sampled_custody(current) is current
        parent = current


def test_retained_sampling_projects_obsolete_physical_fields():
    document = _sampled_document()
    document["exhaustive_checks"] = list(sampled._LEGACY_CHECKS)
    receipt = document["sources"][0]["receipt"]
    receipt["database_identity"] = {"database_oid": 123, "database_name": "old_example"}
    receipt["receipt_sha256"] = digest({key: value for key, value in receipt.items() if key != "receipt_sha256"})
    document["sha256"] = digest({key: value for key, value in document.items() if key != "sha256"})
    projected = sampled.validate_sampled_custody(document)
    assert projected["sources"][0]["method"] == "sampled"
    assert "database_identity" not in projected["sources"][0]["receipt"]
    assert projected["exhaustive_checks"] == list(sampled._CHECKS)
    assert receipt["database_identity"]["database_oid"] == 123


@pytest.mark.asyncio
async def test_preserved_unknown_validation_requires_fresh_source_checks(monkeypatch):
    from process import registry_source_contribution_artifacts as artifacts

    preservation = SimpleNamespace(receipt={"carried_rows": 1}, predecessor=SimpleNamespace(candidate_id=CANDIDATE))
    monkeypatch.setattr(artifacts, "_published_artifact_candidates", AsyncMock(return_value={CANDIDATE: {}}))
    monkeypatch.setattr(
        artifacts,
        "load_published_artifact_validation",
        AsyncMock(
            return_value={
                "candidate_id": CANDIDATE,
                "generation_id": "f" * 64,
                "validation": None,
                "semantic_validation": None,
            }
        ),
    )
    with pytest.raises(ValueError, match="preserved_source_validation_required"):
        await sampled.inherit_preserved_sampling(object(), preservation, None, "synthetic")


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_origin", [False, True])
async def test_sampling_run_records_ready_named_graph_and_refuses_changed_origin(changed_origin):
    import json

    from process.network_fhir_sampled_validation import SampledTypedGraphRead
    from process.network_fhir_typed_capture import TABLES, TypedGraphRead
    from process.network_fhir_typed_contract import TypedFHIRGraphStage

    recipe = _recipe()
    capture = recipe.source_pin.capture
    stage = TypedFHIRGraphStage(TABLES, schema_name=capture["schema_name"], document=capture["graph"])
    read = TypedGraphRead(stage, capture, recipe.source_pin.anchor)
    receipt = _semantic_receipt(capture["capture_sha256"])
    receipt.update(
        capture_id=read.anchor.capture_id,
        observed_at=read.anchor.observed_at,
        source_origin_sha256=digest(read.document["origin"]),
        membership_as_of=read.document["membership_as_of"],
    )
    if changed_origin:
        receipt["source_origin_sha256"] = "f" * 64
    receipt["receipt_sha256"] = digest(
        {key: field_value for key, field_value in receipt.items() if key != "receipt_sha256"}
    )
    sampled_read = SampledTypedGraphRead(read, json.dumps(receipt))
    run = sampled.SampledValidationRun(SemanticSamplingPolicy("b" * 64), CANDIDATE)
    if changed_origin:
        with pytest.raises(ValueError, match="sample_read"):
            await run.record(sampled_read, object())
        assert run._receipts == {}
    else:
        await run.record(sampled_read, object())
        document = await run.finish(object(), CANDIDATE)
        assert document["sources"] == [
            {"capture_sha256": capture["capture_sha256"], "method": "sampled", "receipt": receipt, "predecessor": None}
        ]


def _network_receipt(keys):
    receipt = _semantic_receipt()
    template = receipt["networks"][0]
    receipt["networks"] = [dict(copy.deepcopy(template), network_source_key=key) for key in keys]
    receipt["receipt_sha256"] = digest({key: value for key, value in receipt.items() if key != "receipt_sha256"})
    return receipt


@pytest.mark.asyncio
async def test_fresh_sampled_network_scopes_merge_without_upgrading_classification():
    run = sampled.SampledValidationRun(SemanticSamplingPolicy("b" * 64), CANDIDATE)
    first, second = _network_receipt(["network-a", "network-b"]), _network_receipt(["network-b", "network-c"])
    run._record(first["capture_sha256"], first, "sampled", None)
    run._record(second["capture_sha256"], second, "sampled", None)
    run._record(first["capture_sha256"], first, "sampled", None)
    document = await run.finish(object(), CANDIDATE)
    assert len(document["sources"]) == 1
    evidence = document["sources"][0]
    assert evidence["method"] == "sampled" and evidence["predecessor"] is None
    assert [network["network_source_key"] for network in evidence["receipt"]["networks"]] == [
        "network-a",
        "network-b",
        "network-c",
    ]
    assert first["networks"] == _network_receipt(["network-a", "network-b"])["networks"]


@pytest.mark.parametrize("damage", ["seed", "origin", "algorithm", "as_of", "capture", "overlap", "retained"])
def test_sampled_network_union_refuses_changed_context_or_overlap(damage):
    run = sampled.SampledValidationRun(SemanticSamplingPolicy("b" * 64), CANDIDATE)
    first, changed = _network_receipt(["network-a"]), _network_receipt(["network-a", "network-b"])
    run._record(first["capture_sha256"], first, "sampled", None)
    changes_by_field = {
        "seed": ("seed", "d" * 64),
        "origin": ("source_origin_sha256", "d" * 64),
        "algorithm": ("algorithm", "other"),
        "as_of": ("membership_as_of", "2026-01-02"),
        "capture": ("capture_id", str(UUID(int=999))),
    }
    if damage in changes_by_field:
        field, value = changes_by_field[damage]
        changed[field] = value
    if damage == "overlap":
        changed["networks"][0]["source_to_candidate"].update(population=1, selected=1, series_counts=[1] + [0] * 9)
    changed["receipt_sha256"] = digest({key: value for key, value in changed.items() if key != "receipt_sha256"})
    if damage == "overlap":
        from process.network_fhir_sampled_validation import validate_sampled_semantic_receipt

        validate_sampled_semantic_receipt(changed)
    before = copy.deepcopy(run._receipts)
    with pytest.raises(ValueError):
        run._record(first["capture_sha256"], changed, "sampled-retained" if damage == "retained" else "sampled", None)
    assert run._receipts == before
