# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit witness dispatch keeps legacy identities and original anchors separate."""

import copy
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import registry_source_contribution_artifacts as artifacts
from process import registry_source_contribution_store as store
from process import registry_source_inventory_summaries as summaries
from tests.test_verified_inventory_v2 import prepared, rows


def test_witness_summary_dispatch_requires_inventory_and_keeps_all_counters():
    recipe, descriptor, derivation = rows()
    root, _ = prepared([(recipe, descriptor, derivation)])
    expected = summaries.contribution_summary_v4(root, [descriptor])
    stage = SimpleNamespace(contribution_receipts=(descriptor,), contribution_revision=2)
    assert store.registry_source_contribution_receipt(stage, inventory_root=root) == expected
    assert store.validate_registry_source_contribution_receipt(expected, inventory_root=root) == expected
    with pytest.raises(store.RegistrySourceContributionError):
        store.validate_registry_source_contribution_receipt(expected)
    assert expected["unresolved_rows"] == descriptor["unresolved_rows"]
    assert expected["unresolved_raw_source_rows"] == descriptor["unresolved_raw_source_rows"]


@pytest.mark.parametrize("anchored", [False, True])
def test_explicit_witness_artifact_preserves_original_anchor_and_identity(anchored):
    _, descriptor, _ = rows(anchored=anchored)
    native = artifacts._validated_descriptor(descriptor)
    assert not set(native) & store.FACT_ANCHOR_FIELDS
    assert native["descriptor_version"] == 2
    reference = artifacts.source_fact_artifact_reference(descriptor)
    assert (reference is not None) is anchored
    if anchored:
        assert reference.fact_candidate_id == str(UUID(int=9))
        assert reference.fact_table_oid == 71
    assert descriptor["fact_candidate_id"] == (str(UUID(int=9)) if anchored else None)


@pytest.mark.parametrize(
    "change",
    [
        {"descriptor_version": True},
        {"descriptor_version": 3},
        {"transform_version": "generic-fhir-source-facts-v1"},
        {"fact_candidate_id": str(UUID(int=1))},
    ],
)
def test_artifact_cannot_relabel_or_partially_anchor_witnesses(change):
    _, descriptor, _ = rows()
    with pytest.raises(store.RegistrySourceContributionError):
        artifacts._validated_descriptor({**descriptor, **change})


def test_reused_witness_mapping_can_change_but_original_fact_identity_cannot():
    _, descriptor, _ = rows()
    reference = artifacts.SourceFactArtifactReference(str(UUID(int=3)), 17, "a" * 64)
    artifact = artifacts.ResolvedSourceFactArtifact(reference, "synthetic_original_facts", descriptor)
    mapped = copy.deepcopy(descriptor)
    mapped.update(mapping_sha256="b" * 64, source_generation=store._digest([mapped["capture_sha256"], "b" * 64]))
    anchored = artifacts.anchored_source_fact_descriptor(mapped, artifact)
    assert anchored["fact_candidate_id"] == str(UUID(int=3)) and anchored["mapping_sha256"] == "b" * 64
    with pytest.raises(store.RegistrySourceContributionError, match="identity_changed"):
        artifacts.anchored_source_fact_descriptor({**mapped, "fact_sha256": "c" * 64}, artifact)
