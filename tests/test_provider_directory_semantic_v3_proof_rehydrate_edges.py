# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Proof, hash, and rehydration edges for semantic-content datasets."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.provider_directory_semantic_v3_coverage_support import (
    LEGACY_CONTRACT,
    PROJECTION_DATE,
    SEMANTIC_CONTRACT,
    ZERO_HASH,
    candidate,
    practitioner_payload,
    semantic_parent_metadata,
)


importer = importlib.import_module("process.provider_directory_fhir")
proof_store = importlib.import_module("process.provider_directory_proof_store")
resource_hash = importlib.import_module("process.provider_directory_resource_hash")
rehydrate = importlib.import_module("process.provider_directory_dataset_rehydrate")
rehydrate_scope = importlib.import_module(
    "process.provider_directory_dataset_rehydrate_scope"
)


def test_twin_root_contract_rejects_proof_scope_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importer,
        "_dataset_proof_resource_scope",
        lambda *_args: ("Location",),
    )
    baseline_by_field = {
        "publication_metadata_json": semantic_parent_metadata()
    }

    with pytest.raises(RuntimeError, match="verification_baseline_incompatible"):
        importer._assert_twin_root_resource_hash_contract(
            candidate(),
            baseline_by_field,
        )


def test_proof_record_rejects_contract_payload_and_hash_failures() -> None:
    base_row_by_field = {
        "resource_type": "Organization",
        "resource_id": "resource-edge",
        "payload_hash": ZERO_HASH,
        "payload_json": {},
    }
    with pytest.raises(proof_store.ProviderDirectoryProofStoreError):
        proof_store._proof_record(base_row_by_field, "unknown-contract")

    invalid_practitioner_by_field = {
        **base_row_by_field,
        "resource_type": "Practitioner",
    }
    with pytest.raises(
        proof_store.ProviderDirectoryProofStoreError,
        match="semantic proof payload is invalid",
    ):
        proof_store._proof_record(
            invalid_practitioner_by_field,
            SEMANTIC_CONTRACT,
        )

    with pytest.raises(
        proof_store.ProviderDirectoryProofStoreError,
        match="payload hash changed",
    ):
        proof_store._proof_record(base_row_by_field, SEMANTIC_CONTRACT)


def test_proof_parent_metadata_rejects_invalid_json() -> None:
    with pytest.raises(
        proof_store.ProviderDirectoryProofStoreError,
        match="parent resource scope is invalid",
    ):
        proof_store._decoded_proof_parent_metadata(
            {"publication_metadata_json": "{"}
        )


def test_proof_parent_hash_identity_fences_contract_and_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        proof_store,
        "persisted_resource_hash_contract",
        lambda metadata: resource_hash.persisted_resource_hash_contract(metadata),
    )
    cases = (
        ({"resource_hash_contract": "unknown-contract"}, ["Practitioner"]),
        (
            {
                "resource_hash_contract": SEMANTIC_CONTRACT,
                "proof_resource_scope": [],
            },
            ["Practitioner"],
        ),
        (
            {
                "resource_hash_contract": SEMANTIC_CONTRACT,
                "proof_resource_scope": ["Location"],
            },
            ["Practitioner"],
        ),
        (
            {
                "resource_hash_contract": LEGACY_CONTRACT,
                "proof_resource_scope": None,
            },
            ["Practitioner"],
        ),
    )

    for metadata, selected_resources in cases:
        with pytest.raises(proof_store.ProviderDirectoryProofStoreError):
            proof_store._validated_proof_parent_hash_identity(
                metadata,
                selected_resources,
            )


@pytest.mark.asyncio
async def test_proof_shard_persistence_fences_expected_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        proof_store,
        "_locked_dataset_proof_lineage",
        AsyncMock(
            return_value=(
                "endpoint-edge",
                "root-edge",
                ["source-edge"],
                ["Practitioner"],
                ["Practitioner"],
                SEMANTIC_CONTRACT,
            )
        ),
    )

    with pytest.raises(
        proof_store.ProviderDirectoryProofStoreError,
        match="hash contract changed",
    ):
        await proof_store.persist_dataset_proof_shard(
            SimpleNamespace(),
            "healthcare",
            [],
            dataset_id="dataset-edge",
            expected_resource_hash_contract=LEGACY_CONTRACT,
        )


def test_empty_proof_record_group_is_a_noop() -> None:
    accumulator = proof_store._ResourceProofAccumulator()
    assert accumulator.add_record_group([], SimpleNamespace()) is None
    assert accumulator.resource_count == 0


@pytest.mark.parametrize("value", (None, "2026-13-01", "2020-W01-1"))
def test_proof_projection_date_rejects_invalid_or_noncanonical_iso(value) -> None:
    with pytest.raises(
        proof_store.ProviderDirectoryProofStoreError,
        match="projection date is invalid",
    ):
        proof_store._validated_semantic_projection_as_of(value)


def test_expected_proof_contract_fences_semantic_and_legacy_shapes() -> None:
    with pytest.raises(
        proof_store.ProviderDirectoryProofStoreError,
        match="semantic proof contract changed",
    ):
        proof_store._assert_expected_semantic_proof(
            {},
            PROJECTION_DATE,
            ["Practitioner"],
        )

    with pytest.raises(
        proof_store.ProviderDirectoryProofStoreError,
        match="expected semantic projection date is invalid",
    ):
        proof_store._assert_expected_legacy_proof({}, PROJECTION_DATE, None)

    with pytest.raises(
        proof_store.ProviderDirectoryProofStoreError,
        match="expected semantic projection date is invalid",
    ):
        proof_store._assert_expected_proof_contract(
            {},
            None,
            PROJECTION_DATE,
            None,
        )

    with pytest.raises(
        proof_store.ProviderDirectoryProofStoreError,
        match="expected proof contract is invalid",
    ):
        proof_store._assert_expected_proof_contract(
            {},
            "unknown-contract",
            None,
            None,
        )


def test_metadata_shards_reject_resource_outside_proof_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        proof_store,
        "_validated_shard_descriptor",
        lambda *_args, **_kwargs: {
            "shard_id": ZERO_HASH,
            "resource_counts": {"Location": 1},
        },
    )
    lineage = proof_store._ProofLineage(
        dataset_id="dataset-edge",
        endpoint_id="endpoint-edge",
        acquisition_root_run_id="root-edge",
        source_ids=["source-edge"],
        selected_resources=["Practitioner"],
        proof_resource_scope=["Practitioner"],
    )

    with pytest.raises(
        proof_store.ProviderDirectoryProofStoreError,
        match="shard resource scope changed",
    ):
        proof_store._validate_metadata_shards(
            {"shards": [{}], "shard_count": 1},
            lineage,
        )


def test_practitioner_hash_helpers_reject_invalid_name_and_hash_shapes() -> None:
    assert resource_hash.canonical_practitioner_names(None) == []

    for names in ("name", ["name"]):
        with pytest.raises(ValueError, match="practitioner_names_invalid"):
            resource_hash.canonical_practitioner_names(names)

    assert resource_hash._practitioner_primary_name_projection(
        [{"given": "Taylor"}]
    )["given_names"] == []

    with pytest.raises(ValueError, match="practitioner_payload_invalid"):
        resource_hash.practitioner_semantic_base_payload({})

    with pytest.raises(ValueError, match="practitioner_hash_invalid"):
        resource_hash.composed_practitioner_semantic_sha256("z" * 64, [])


@pytest.mark.parametrize(
    "fhir_meta",
    (
        None,
        {"lastUpdated": "2026-08-09T00:00:00Z"},
        {"versionId": "1"},
    ),
)
def test_practitioner_semantic_merge_covers_optional_provenance_shapes(
    fhir_meta,
) -> None:
    first = practitioner_payload(fhir_meta=fhir_meta)
    second = practitioner_payload(fhir_meta=fhir_meta)

    merged = resource_hash.merge_practitioner_semantic_payloads(first, second)

    if fhir_meta is None:
        assert merged["fhir_meta"] is None
    else:
        assert merged["fhir_meta"] == fhir_meta


def test_semantic_resource_merge_rejects_content_conflict() -> None:
    with pytest.raises(ValueError, match="resource_payload_conflict"):
        resource_hash.merge_semantic_resource_payloads(
            {"resource_id": "resource-edge", "active": True},
            {"resource_id": "resource-edge", "active": False},
        )


def _rehydration_request() -> SimpleNamespace:
    return SimpleNamespace(
        dataset_id="dataset-edge",
        acquisition_root_run_id="root-edge",
    )


def test_rehydration_proof_scope_fences_contract_subset_and_presence() -> None:
    scope_fields_by_name = {"endpoint_id": "endpoint-edge"}
    request = _rehydration_request()
    cases = (
        (
            {
                "proof_resource_scope": ["Location"],
                "source_ids": ["source-edge"],
            },
            ["Practitioner"],
            SEMANTIC_CONTRACT,
        ),
        (
            {
                "proof_resource_scope": None,
                "source_ids": ["source-edge"],
            },
            ["Practitioner"],
            LEGACY_CONTRACT,
        ),
        (
            {
                "proof_resource_scope": ["Practitioner"],
                "source_ids": ["source-edge"],
            },
            ["Practitioner"],
            SEMANTIC_CONTRACT,
        ),
    )

    for metadata, selected_resources, contract in cases:
        with pytest.raises(ValueError):
            rehydrate_scope._retained_resource_types(
                request,
                scope_fields_by_name,
                metadata,
                selected_resources,
                ["source-edge"],
                contract,
                PROJECTION_DATE if contract == SEMANTIC_CONTRACT else None,
            )


@pytest.mark.parametrize("value", ("2026-13-01", "2020-W01-1"))
def test_rehydration_projection_date_rejects_invalid_or_noncanonical_iso(value) -> None:
    with pytest.raises(ValueError, match="semantic_projection_as_of_invalid"):
        rehydrate_scope._semantic_projection_as_of(
            {"semantic_projection_as_of": value},
            SEMANTIC_CONTRACT,
        )


def test_rehydration_payload_validation_rejects_bad_subset_hash_and_contract() -> None:
    assert (
        rehydrate._validate_payload(
            object,
            "resource-edge",
            ZERO_HASH,
            {"resource_id": "resource-edge"},
            resource_hash_contract=LEGACY_CONTRACT,
            acquired_resource_sha256="invalid",
        )
        == "payload_hash_mismatch"
    )
    assert (
        rehydrate._validate_payload(
            object,
            "resource-edge",
            ZERO_HASH,
            {"resource_id": "resource-edge"},
            resource_hash_contract="unknown-contract",
        )
        == "payload_hash_mismatch"
    )
