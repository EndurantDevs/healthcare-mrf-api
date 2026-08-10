# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical completion and sealed replay proofs for reviewed FHIR subsets."""

from __future__ import annotations

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
)
from process.provider_directory_fhir_census_execution import (
    SERVER_ISSUED_SUBSET_FETCH_MODE,
)
from process.provider_directory_fhir_subset_completion import (
    build_subset_replay_evidence,
    canonical_sha256,
    validate_subset_completion_proof_pair,
    validate_subset_replay_evidence_pair,
)
from tests.provider_directory_fhir_subset_completion_support import (
    CUTOFF,
    PAGE_COUNT,
    build_completed_execution_proof as _completed_execution_proof,
    build_coverage_inputs,
    build_dataset_candidate as _candidate,
    build_execution_proof as _execution_proof,
    build_finalization_inputs,
    build_proof_pair as _proof_pair,
    build_subset_contract as _contract,
    build_transport_coordinate_rows,
    importer,
)


@pytest.mark.parametrize(
    "drift",
    (
        {"continuation_strategy": "wrong"},
        {"strategy_version": "wrong"},
        {"traversal_version": "wrong"},
        {"canonicalization_version": "wrong"},
        {"completion_scopes": ("wrong",)},
        {"page_count": 0},
        {"campaign_id": None},
        {"resources": ("Organization",)},
    ),
)
def test_v3_predicate_requires_every_reviewed_identity_field(drift):
    assert _contract(**drift).is_server_issued_subset_v3 is False


def test_completion_is_root_neutral_but_replay_evidence_is_per_root():
    proof_a, sha_a, execution_a = _proof_pair(hop_prefix="1")
    proof_b, sha_b, execution_b = _proof_pair(hop_prefix="9")

    assert proof_a == proof_b
    assert sha_a == sha_b
    replay_a, replay_sha_a = build_subset_replay_evidence(
        resource_proof_by_type=execution_a,
        completion_proof=proof_a,
        completion_sha256=sha_a,
    )
    replay_b, replay_sha_b = build_subset_replay_evidence(
        resource_proof_by_type=execution_b,
        completion_proof=proof_b,
        completion_sha256=sha_b,
    )
    assert replay_a != replay_b
    assert replay_sha_a != replay_sha_b
    validate_subset_replay_evidence_pair(
        replay_a,
        replay_sha_a,
        proof_a,
        sha_a,
    )
    validate_subset_replay_evidence_pair(
        replay_b,
        replay_sha_b,
        proof_b,
        sha_b,
    )
    content_a = importer.EndpointDatasetContentProof(
        dataset_hash=proof_a["dataset"]["hash"],
        resource_count=proof_a["dataset"]["count"],
        resource_hashes=proof_a["dataset"]["resource_hashes"],
        resource_counts=proof_a["dataset"]["resource_counts"],
        completion_proof=proof_a,
        completion_proof_sha256=sha_a,
    )
    content_b = copy.deepcopy(content_a)
    contract = _contract()
    twin_a = importer._twin_root_content_proof(
        _candidate(contract, root_run_id="root-a"),
        content_a,
    )
    twin_b = importer._twin_root_content_proof(
        _candidate(contract, root_run_id="root-b"),
        content_b,
    )
    assert importer._twin_root_mismatch_fields(twin_a, twin_b) == []


def test_subset_payload_hash_is_neutral_to_page_transport_coordinates():
    (
        model,
        first_page_row_by_field,
        continuation_row_by_field,
        changed_row_by_field,
    ) = build_transport_coordinate_rows()
    first = importer._endpoint_dataset_resource_rows(
        model,
        [first_page_row_by_field],
        dataset_id="dataset-a",
        resource_hash_contract=importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )[0]
    continuation = importer._endpoint_dataset_resource_rows(
        model,
        [continuation_row_by_field],
        dataset_id="dataset-b",
        resource_hash_contract=importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )[0]
    changed = importer._endpoint_dataset_resource_rows(
        model,
        [changed_row_by_field],
        dataset_id="dataset-c",
        resource_hash_contract=importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )[0]

    assert first["payload_hash"] == continuation["payload_hash"]
    assert first["acquired_resource_sha256"] == continuation[
        "acquired_resource_sha256"
    ]
    assert changed["payload_hash"] != first["payload_hash"]
    assert changed["acquired_resource_sha256"] != first[
        "acquired_resource_sha256"
    ]
    assert all(
        not key.startswith("_acquired_")
        for key in first["payload_json"]
    )


@pytest.mark.asyncio
async def test_successful_v3_finalization_persists_completion_and_replay():
    _, candidate, diagnostic_by_type, content_proof = build_finalization_inputs()
    completed_content = importer._content_proof_with_subset_completion(
        candidate,
        diagnostic_by_type,
        content_proof,
    )
    verification_metadata_by_field = {
        importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
            "result": "baseline_recorded"
        }
    }
    metadata = importer._dataset_validation_metadata(
        candidate,
        diagnostic_by_type,
        completed_content,
        {},
        {},
        verification_metadata_by_field,
        {},
    )
    connection = SimpleNamespace(status=AsyncMock(return_value="UPDATE 1"))

    await importer._store_endpoint_dataset_verification_result(
        connection,
        candidate,
        None,
        completed_content,
        metadata,
        importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
    )

    persisted = connection.status.await_args.kwargs
    assert persisted["completion_proof_sha256"] == (
        completed_content.completion_proof_sha256
    )
    assert importer.SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_KEY in metadata
    assert (
        importer.SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_SHA256_KEY
        in metadata
    )
    assert importer.SERVER_ISSUED_SUBSET_COVERAGE_KEY in metadata


def test_status_projection_is_neutral_and_internal_replay_is_separable():
    execution = _execution_proof()
    execution.update(
        cutoff=CUTOFF,
        page_count=PAGE_COUNT,
        campaign_id="synthetic-reviewed-subset-v3",
    )

    safe_proof = importer._sanitized_server_issued_subset_execution_proof(
        execution
    )
    replay = importer._server_issued_subset_internal_replay_evidence(
        execution
    )
    coverage = importer._server_issued_subset_coverage(execution)
    sanitized_diagnostics = importer._sanitized_resource_diagnostics(
        {
            "Organization": {
                "server_issued_subset_completeness": safe_proof,
                importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY: replay,
            }
        }
    )

    assert safe_proof is not None
    assert "continuation_hop_sha256" not in safe_proof
    assert replay == {
        "continuation_hop_sha256": execution[
            "continuation_hop_sha256"
        ]
    }
    assert coverage is not None
    assert coverage["continuation"]["chain_sha256"] == canonical_sha256(
        execution["continuation_shape_sha256"]
    )
    assert (
        importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY
        not in sanitized_diagnostics["Organization"]
    )


def test_terminal_diagnostic_validation_rejoins_internal_replay_evidence():
    contract = _contract()
    diagnostic_by_type = {}
    for resource_type in SERVER_ISSUED_SUBSET_RESOURCE_TYPES:
        proof = _completed_execution_proof(contract, resource_type)
        diagnostic_by_type[resource_type] = {
            "fetch_mode": SERVER_ISSUED_SUBSET_FETCH_MODE,
            "rows_fetched": 1,
            "pages_fetched": 3,
            "server_issued_subset_completeness": (
                importer._sanitized_server_issued_subset_execution_proof(
                    proof
                )
            ),
            importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY: (
                importer._server_issued_subset_internal_replay_evidence(
                    proof
                )
            ),
        }
    source_record_by_field = {CURRENT_VERSION_CENSUS_CONTRACT_FIELD: contract}

    importer._validate_current_version_census_diagnostics(
        [source_record_by_field],
        diagnostic_by_type,
    )
    diagnostic_by_type["Organization"].pop(
        importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY
    )
    with pytest.raises(RuntimeError, match="proof_incomplete"):
        importer._validate_current_version_census_diagnostics(
            [source_record_by_field],
            diagnostic_by_type,
        )


@pytest.mark.parametrize(
    ("field_name", "forged_value"),
    (
        ("proof_state", "forged_not_verified"),
        ("twin_state", "forged_matched"),
        ("unresolved_reference_count", 999),
    ),
)
def test_sanitized_coverage_rejects_forged_resource_status(
    field_name,
    forged_value,
):
    (
        proof,
        proof_sha256,
        candidate,
        diagnostic_by_type,
        content_proof,
        relation_proof_by_type,
        verification_metadata_by_field,
    ) = build_coverage_inputs()
    metadata = {
        **verification_metadata_by_field,
        **importer._subset_dataset_coverage_metadata(
            candidate,
            diagnostic_by_type,
            content_proof,
            relation_proof_by_type,
            verification_metadata_by_field,
        ),
    }
    importer._validate_subset_dataset_coverage(
        metadata,
        proof,
        proof_sha256,
    )
    tampered = copy.deepcopy(metadata)
    tampered[importer.SERVER_ISSUED_SUBSET_COVERAGE_KEY]["resources"][
        "Organization"
    ][field_name] = forged_value

    with pytest.raises(ValueError, match="subset_coverage_invalid"):
        importer._validate_subset_dataset_coverage(
            tampered,
            proof,
            proof_sha256,
        )


@pytest.mark.parametrize(
    "mutate",
    (
        lambda proof: proof.update(page_count=251),
        lambda proof: proof["resources"]["Organization"].update(pages=2),
        lambda proof: proof["resources"]["Organization"].update(
            logical_terminal_offset=251
        ),
        lambda proof: proof["resources"]["Organization"].update(
            page_entry_counts=[1, 0]
        ),
        lambda proof: proof["resources"].update(Unknown={}),
        lambda proof: proof.update(cutoff="2026-08-01T12:00:00Z"),
    ),
)
def test_recomputed_sha_does_not_make_tampered_completion_valid(mutate):
    proof, _proof_sha256, _execution = _proof_pair()
    tampered = copy.deepcopy(proof)
    mutate(tampered)

    with pytest.raises(ValueError):
        validate_subset_completion_proof_pair(
            tampered,
            canonical_sha256(tampered),
        )


@pytest.mark.parametrize(
    "mutate",
    (
        lambda replay: replay["resources"]["Organization"].update(
            continuation_hop_sha256=[]
        ),
        lambda replay: replay["resources"]["Organization"].update(
            continuation_hop_sha256=list(
                reversed(
                    replay["resources"]["Organization"][
                        "continuation_hop_sha256"
                    ]
                )
            )
        ),
        lambda replay: replay["resources"]["Organization"].update(
            continuation_shape_sha256=["f" * 64, "b" * 64]
        ),
        lambda replay: replay.update(completion_proof_sha256="f" * 64),
    ),
)
def test_recomputed_sha_does_not_make_tampered_replay_valid(mutate):
    proof, proof_sha256, execution = _proof_pair()
    replay, _replay_sha256 = build_subset_replay_evidence(
        resource_proof_by_type=execution,
        completion_proof=proof,
        completion_sha256=proof_sha256,
    )
    tampered = copy.deepcopy(replay)
    mutate(tampered)

    with pytest.raises(ValueError):
        validate_subset_replay_evidence_pair(
            tampered,
            canonical_sha256(tampered),
            proof,
            proof_sha256,
        )
