# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Leaf-level fail-closed tests for reviewed server-issued traversal."""

from __future__ import annotations

import copy

import pytest

from process import provider_directory_fhir_census_execution as census_execution
from process import provider_directory_fhir_census_resume as census_resume
from process import provider_directory_fhir_manual_catalog as manual_catalog
from process import provider_directory_fhir_subset_canonical as canonical
from process import provider_directory_fhir_subset_completion as completion
from process import provider_directory_fhir_subset_execution as subset_execution
from process import provider_directory_fhir_subset_identity as subset_identity
from process import provider_directory_fhir_subset_replay as replay
from process.provider_directory_fhir_census_contract import (
    SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY,
    SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
    SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
)
from tests.provider_directory_fhir_subset_completion_support import (
    PAGE_COUNT,
    build_execution_proof,
    build_proof_pair,
    build_subset_contract,
)


def test_contract_url_and_resume_require_bound_v3_page_geometry():
    contract = build_subset_contract()

    with pytest.raises(ValueError, match="page_count_identity_mismatch"):
        contract.start_url("Organization", PAGE_COUNT - 1)
    with pytest.raises(ValueError, match="resume_identity_invalid"):
        census_resume.resume_prior_page_entry_count(contract, {})


def test_initial_and_terminal_counts_reject_each_v3_drift_class():
    contract = build_subset_contract()
    with pytest.raises(ValueError, match="page_count_identity_mismatch"):
        census_execution.current_version_census_initial_proof(
            contract,
            "Organization",
            2,
            expected_page_count=PAGE_COUNT - 1,
        )

    cases = (
        dict(pre_count=2, post_count=3, processed_rows=1, unique_candidate_rows=1),
        dict(pre_count=2, post_count=2, processed_rows=2, unique_candidate_rows=1),
        dict(pre_count=1, post_count=1, processed_rows=2, unique_candidate_rows=2),
    )
    assert [
        census_execution._completion_failure(True, **case)
        for case in cases
    ] == [
        "census_drift",
        "duplicate_resource_ids",
        "returned_count_exceeds_advertised",
    ]


def test_manual_fixed_identity_rejects_reviewed_field_drift():
    raw_config_by_field = {
        "continuation_strategy": (
            SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY
        ),
        "semantics": "wrong",
        "strategy_version": SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        "traversal_version": SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
        "canonicalization_version": (
            SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION
        ),
        "completion_scopes": list(SERVER_ISSUED_SUBSET_COMPLETION_SCOPES),
    }

    with pytest.raises(RuntimeError, match="v3_identity_invalid"):
        manual_catalog._validated_manual_fixed_identity(raw_config_by_field)


def test_payload_canonicalization_rejects_numbers_keys_and_types():
    with pytest.raises(ValueError, match="payload_number_invalid"):
        canonical._canonical_payload_number(object())
    with pytest.raises(ValueError, match="payload_number_invalid"):
        canonical._canonical_payload_number(float("nan"))
    with pytest.raises(ValueError, match="payload_key_invalid"):
        canonical.canonical_payload_json({1: "invalid"})
    with pytest.raises(ValueError, match="payload_type_invalid"):
        canonical.canonical_payload_json({"invalid"})


def test_completion_canonicalization_rejects_time_and_private_values():
    assert canonical._is_canonical_utc_instant(None) is False
    assert canonical._is_canonical_utc_instant("not-an-instant") is False
    with pytest.raises(ValueError, match="completion_proof_private"):
        canonical._assert_root_neutral({"run_id": "private-root"})
    with pytest.raises(ValueError, match="completion_proof_invalid"):
        canonical._assert_root_neutral(1.5)


def test_completion_envelope_and_dataset_digest_fail_closed():
    with pytest.raises(ValueError, match="completion_proof_invalid"):
        canonical.validate_subset_completion_proof_pair(None, "f" * 64)

    proof, _sha256, _execution = build_proof_pair()
    tampered = copy.deepcopy(proof)
    tampered["dataset"]["count"] += 1
    with pytest.raises(ValueError, match="completion_dataset_invalid"):
        canonical.validate_subset_completion_proof_pair(
            tampered,
            canonical.canonical_sha256(tampered),
        )


def test_completion_geometry_rejects_missing_negative_and_inconsistent_fields():
    with pytest.raises(ValueError, match="completion_geometry_invalid"):
        completion._geometry_from_execution_proof({})

    execution = build_execution_proof()
    negative = copy.deepcopy(execution)
    negative["terminal_page_geometry"]["empty_pages"] = -1
    with pytest.raises(ValueError, match="completion_geometry_invalid"):
        completion._geometry_from_execution_proof(negative)

    inconsistent = copy.deepcopy(execution)
    inconsistent["page_entry_counts"] = [1]
    with pytest.raises(ValueError, match="completion_geometry_invalid"):
        completion._geometry_from_execution_proof(inconsistent)


def _completion_arguments():
    contract = build_subset_contract()
    resources = contract.resources
    return {
        "contract": contract,
        "resource_proof_by_type": {
            resource_type: build_execution_proof()
            for resource_type in resources
        },
        "dataset_hash": "e" * 64,
        "resource_count": len(resources),
        "resource_hash_by_type": dict.fromkeys(resources, "c" * 64),
        "acquired_resource_hash_by_type": dict.fromkeys(resources, "d" * 64),
        "resource_count_by_type": dict.fromkeys(resources, 1),
    }


def test_completion_builder_rejects_counts_resources_and_dataset_mismatch():
    arguments = _completion_arguments()
    invalid_counts = copy.deepcopy(arguments)
    invalid_counts["resource_proof_by_type"]["Organization"][
        "advertised_post"
    ] = 3
    with pytest.raises(ValueError, match="completion_counts_invalid"):
        completion.build_subset_completion_proof(**invalid_counts)

    invalid_resource_arguments_by_field = {
        **arguments,
        "dataset_hash": "bad",
    }
    with pytest.raises(ValueError, match="completion_resources_invalid"):
        completion.build_subset_completion_proof(
            **invalid_resource_arguments_by_field
        )

    mismatched_dataset = copy.deepcopy(arguments)
    mismatched_dataset["resource_count_by_type"]["Organization"] = 0
    mismatched_dataset["resource_count"] -= 1
    with pytest.raises(ValueError, match="completion_dataset_invalid"):
        completion.build_subset_completion_proof(**mismatched_dataset)


def test_subset_execution_rejects_terminal_and_checkpoint_shape_drift():
    with pytest.raises(ValueError, match="page_geometry_invalid"):
        subset_execution.subset_completed_fields(
            {},
            pre_count=2,
            post_count=2,
            unique_candidate_rows=1,
            pages_processed=3,
            terminal_page_entry_count=0,
        )
    assert subset_execution.has_valid_subset_completed_fields(
        {},
        {},
        PAGE_COUNT,
    ) is False
    assert subset_execution.has_valid_subset_completed_fields(
        {"terminal_page_geometry": {"pages_processed": 0}},
        {},
        PAGE_COUNT,
    ) is False
    with pytest.raises(ValueError, match="page_geometry_invalid"):
        subset_execution.append_subset_checkpoint_evidence(
            {"contract_version": 3},
            {},
            pages_processed=1,
            page_entry_count=1,
            expected_page_count=PAGE_COUNT,
            continuation_identity_sha256="1" * 64,
            continuation_shape_sha256="a" * 64,
        )


def test_subset_source_identity_rejects_missing_metadata_and_dimensions():
    with pytest.raises(ValueError, match="source_scope_invalid"):
        subset_identity.server_issued_subset_source_scope_payload(
            {},
            ("synthetic-source",),
            "cutoff",
            "https://directory.example.test/fhir",
        )
    source_record_by_field = {
        "source_id": "synthetic-source",
        "endpoint_id": "synthetic-endpoint",
        "metadata_json": {},
    }
    with pytest.raises(ValueError, match="source_scope_invalid"):
        subset_identity.server_issued_subset_source_scope_payload(
            source_record_by_field,
            ("other-source",),
            "cutoff",
            "https://directory.example.test/fhir",
        )
    with pytest.raises(ValueError, match="identity_not_reviewed"):
        subset_identity.validated_subset_identity_values({})


def test_subset_replay_rejects_resource_shape_and_scope_drift():
    proof, proof_sha256, execution_by_type = build_proof_pair()
    resource_type = next(iter(proof["resources"]))
    with pytest.raises(ValueError, match="replay_evidence_invalid"):
        replay._replay_resource_evidence({}, proof["resources"][resource_type])
    with pytest.raises(ValueError, match="replay_evidence_invalid"):
        replay.build_subset_replay_evidence(
            resource_proof_by_type={resource_type: execution_by_type[resource_type]},
            completion_proof=proof,
            completion_sha256=proof_sha256,
        )
    with pytest.raises(ValueError, match="replay_evidence_invalid"):
        replay._validate_replay_resource(None, proof["resources"][resource_type])

    replay_evidence, replay_sha256 = replay.build_subset_replay_evidence(
        resource_proof_by_type=execution_by_type,
        completion_proof=proof,
        completion_sha256=proof_sha256,
    )
    malformed = copy.deepcopy(replay_evidence)
    malformed["resources"].pop(resource_type)
    with pytest.raises(ValueError, match="replay_evidence_invalid"):
        replay.validate_subset_replay_evidence_pair(
            malformed,
            canonical.canonical_sha256(malformed),
            proof,
            proof_sha256,
        )
