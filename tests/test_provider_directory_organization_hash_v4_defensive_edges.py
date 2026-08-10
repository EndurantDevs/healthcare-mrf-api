# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Defensive unit edges for semantic-v4 Organization hashing."""

from __future__ import annotations

import copy
import dataclasses
import importlib

import pytest

from process.provider_directory_organization_hash import (
    canonical_organization_payload,
    composed_organization_semantic_sha256,
    merge_organization_semantic_payloads,
)
from process.provider_directory_resource_hash import (
    RESOURCE_TRANSPORT_PAYLOAD_FIELDS,
)
from tests.test_provider_directory_organization_hash_v4 import (
    _organization_payload,
)


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.parametrize("invalid_name", (7, " "))
def test_v4_organization_rejects_invalid_primary_labels(invalid_name) -> None:
    """Require every optional Organization primary label to be nonempty text."""

    with pytest.raises(ValueError, match="organization_names_invalid"):
        canonical_organization_payload(
            {**_organization_payload("Community Health Center"), "name": invalid_name}
        )


def test_v4_organization_adds_primary_missing_from_retained_variants() -> None:
    """Preserve a newly observed primary beside already retained variants."""

    canonical = canonical_organization_payload(
        {
            **_organization_payload("Zeta Community Health"),
            "name_variants": ["Alpha Community Health"],
        }
    )

    assert canonical["name"] == "Alpha Community Health"
    assert canonical["name_variants"] == [
        "Alpha Community Health",
        "Zeta Community Health",
    ]
    assert canonical["aliases"] == ["Zeta Community Health"]


@pytest.mark.parametrize(
    ("base_hash", "name_hashes"),
    (
        (7, []),
        ("a" * 63, []),
        ("g" * 64, []),
        ("a" * 64, [None]),
        ("a" * 64, ["A" * 64]),
    ),
)
def test_v4_composed_hash_rejects_invalid_digests(
    base_hash,
    name_hashes,
) -> None:
    """Accept only exact lowercase SHA-256 components."""

    with pytest.raises(ValueError, match="organization_hash_invalid"):
        composed_organization_semantic_sha256(base_hash, name_hashes)


def test_v4_merge_drops_volatile_fields_absent_from_preferred_observation() -> None:
    """Keep one observed provenance tuple without filling omitted fields."""

    first = _organization_payload("Community Health Center")
    preferred = copy.deepcopy(first)
    for field_name in RESOURCE_TRANSPORT_PAYLOAD_FIELDS:
        preferred.pop(field_name, None)
    preferred["fhir_meta"]["lastUpdated"] = "9999-01-01T00:00:00Z"

    merged = merge_organization_semantic_payloads(first, preferred)

    assert all(
        field_name not in merged
        for field_name in RESOURCE_TRANSPORT_PAYLOAD_FIELDS
    )
    assert merged["fhir_meta"]["lastUpdated"] == "9999-01-01T00:00:00Z"


def test_v4_merge_accepts_observations_without_fhir_metadata() -> None:
    """Keep absent volatile metadata absent through deterministic merging."""

    first_by_field = {
        **_organization_payload("Community Health Center"),
        "fhir_meta": None,
    }
    second_by_field = copy.deepcopy(first_by_field)

    merged = merge_organization_semantic_payloads(
        first_by_field,
        second_by_field,
    )

    assert merged["fhir_meta"] is None


def _twin_hash_state(proof_by_field) -> importer._TwinRootDatasetProofState:
    """Return one exact v4 twin proof state."""

    metadata = {
        importer.RESOURCE_HASH_CONTRACT_METADATA_KEY: (
            importer.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
        ),
        importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY: [
            "Organization"
        ],
        importer.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY: "2026-08-10",
    }
    return importer._TwinRootDatasetProofState(
        metadata=metadata,
        verification={},
        proof=proof_by_field,
        semantic_projection_as_of="2026-08-10",
        stored_proof_resource_scope=["Organization"],
        proof_resource_count=1,
        dataset_resource_count=1,
    )


def test_v4_twin_root_hash_requires_exact_embedded_contract() -> None:
    """Accept only a self-describing v4 twin proof at finalized replay."""

    proof_by_field = {
        importer.RESOURCE_HASH_CONTRACT_METADATA_KEY: (
            importer.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
        ),
        importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY: [
            "Organization"
        ],
        importer.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY: "2026-08-10",
        "dataset_hash": "a" * 64,
        "resource_hashes": {"Organization": "b" * 64},
        "resource_counts": {"Organization": 1},
    }
    state = _twin_hash_state(proof_by_field)
    dataset_by_field = {
        "publication_metadata_json": state.metadata,
        "dataset_hash": "a" * 64,
    }

    assert importer._is_twin_root_proof_hash_exact(dataset_by_field, state)
    invalid_proof_by_field = dict(proof_by_field)
    invalid_proof_by_field.pop(importer.RESOURCE_HASH_CONTRACT_METADATA_KEY)
    assert not importer._is_twin_root_proof_hash_exact(
        dataset_by_field,
        dataclasses.replace(state, proof=invalid_proof_by_field),
    )
