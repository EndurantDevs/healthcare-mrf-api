# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed tests for typed Profile evidence and semantic reduction."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from process.provider_directory_projection_contribution import (
    _checked_add,
    _stream_semantic_pairs,
    _validate_outcome_consistency,
    reduce_semantic_outcomes,
    validated_semantic_outcome_proof,
)
from process.provider_directory_projection_semantic_evidence import (
    _normalized_json_array,
    normalized_profile_contribution,
    normalized_semantic_contribution,
    normalized_semantic_pair_stream,
    normalized_semantic_winner,
    streamed_npi_proof,
    validate_semantic_pair,
)
from process.provider_directory_projection_summary import (
    semantic_source_summary,
    validated_semantic_source_summary,
)
from process.provider_directory_projection_typed_evidence import (
    _is_valid_coding_list,
)
from process.provider_directory_projection_types import (
    ProjectionSemanticOutcomeProof,
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_semantic_support import (
    candidate_semantic_outcome,
    digest,
    semantic_pair,
    semantic_rows_and_contributions,
)


def test_semantic_text_and_json_arrays_reject_noncanonical_evidence():
    resource, _contribution = semantic_pair("Organization", "o-1")
    resource["resource_type"] = " Organization "
    with pytest.raises(ProviderDirectoryProjectionError, match="resource_type_invalid"):
        normalized_semantic_winner(resource)

    with pytest.raises(ProviderDirectoryProjectionError, match="profile_array_invalid"):
        _normalized_json_array([("tuple-becomes-list",)], "profile_array")


def test_inline_profile_requires_a_nonempty_typed_mapping():
    resource, _contribution = semantic_pair("Organization", "o-1")
    resource["profile_evidence_json"] = "not-a-map"
    with pytest.raises(ProviderDirectoryProjectionError, match="profile_evidence_invalid"):
        normalized_semantic_winner(resource)

    resource, _contribution = semantic_pair("Organization", "o-1")
    resource["profile_evidence_json"] = {"names": []}
    with pytest.raises(ProviderDirectoryProjectionError, match="profile_evidence_invalid"):
        normalized_semantic_winner(resource)


def test_semantic_contribution_rejects_unscoped_npi_and_missing_columns():
    resource, _contribution = semantic_pair(
        "Location",
        "l-1",
        npi=1_234_567_890,
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="summary_npi_invalid"):
        normalized_semantic_contribution(resource)

    resource, _contribution = semantic_pair("Organization", "o-1")
    del resource["summary_address_count"]
    with pytest.raises(ProviderDirectoryProjectionError, match="contribution_missing"):
        normalized_semantic_contribution(resource)


def test_semantic_winner_and_profile_contribution_require_complete_rows():
    with pytest.raises(ProviderDirectoryProjectionError, match="resource_fields_missing"):
        normalized_semantic_winner(None)
    with pytest.raises(ProviderDirectoryProjectionError, match="fields_missing"):
        normalized_profile_contribution(None)

    _resource, contribution = semantic_pair("Practitioner", "p-1")
    contribution["direct_npi"] = True
    with pytest.raises(ProviderDirectoryProjectionError, match="direct_npi_invalid"):
        normalized_profile_contribution(contribution)


@pytest.mark.parametrize(
    "raw_codings",
    (
        None,
        [],
        ["not-a-map"],
        [{}],
        [{"unknown": "x", "code": "123"}],
        [{"system": "taxonomy"}],
        [{"code": "123", "userSelected": "true"}],
        [{"code": " "}],
    ),
)
def test_specialty_coding_lists_reject_each_malformed_shape(raw_codings):
    assert _is_valid_coding_list(raw_codings) is False


def test_specialty_coding_list_accepts_typed_multifield_codings():
    assert _is_valid_coding_list(
        [
            {
                "system": "http://nucc.org/provider-taxonomy",
                "code": "207Q00000X",
                "display": "Family Medicine",
                "userSelected": True,
            },
            {"code": "208D00000X"},
        ]
    )


def test_profile_text_fields_and_location_flags_remain_strictly_typed():
    resource, contribution = semantic_pair(
        "Practitioner",
        "p-1",
        evidence_by_name={"names": [{"text": ""}]},
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="names_evidence_invalid"):
        reduce_semantic_outcomes((resource,), (contribution,))

    resource, contribution = semantic_pair("Location", "l-1")
    resource["summary_addressed_location"] = None
    with pytest.raises(ProviderDirectoryProjectionError, match="location_flag_invalid"):
        reduce_semantic_outcomes((resource,), (contribution,))


def test_pair_validator_catches_post_normalization_address_count_drift():
    raw_resource, raw_contribution = semantic_pair(
        "Practitioner",
        "p-1",
        evidence_by_name={"addresses": [{"line": ["1 Main St"]}]},
    )
    resource = normalized_semantic_winner(raw_resource)
    contribution = normalized_profile_contribution(raw_contribution)
    resource["summary_address_count"] = 0
    with pytest.raises(ProviderDirectoryProjectionError, match="address_count_mismatch"):
        validate_semantic_pair(resource, contribution)


@pytest.mark.parametrize("raw_pair", (None, (), ({},), ({}, {}, {})))
def test_pair_stream_rejects_non_pair_iterables(raw_pair):
    stream = normalized_semantic_pair_stream((raw_pair,))
    with pytest.raises(ProviderDirectoryProjectionError, match="pair_invalid"):
        next(iter(stream))


def test_streamed_npi_proof_rejects_malformed_groups_and_total_overflow():
    with pytest.raises(ProviderDirectoryProjectionError, match="occurrence_invalid"):
        streamed_npi_proof((None,))
    with pytest.raises(ProviderDirectoryProjectionError, match="count_invalid"):
        streamed_npi_proof(
            (
                (1_234_567_890, 2**63 - 1),
                (1_234_567_891, 1),
            )
        )


def test_streaming_reducer_requires_nonempty_strictly_ordered_pairs():
    with pytest.raises(ProviderDirectoryProjectionError, match="pair_set_mismatch"):
        _stream_semantic_pairs(())

    resources, contributions = semantic_rows_and_contributions()
    normalized_pairs = [
        (
            normalized_semantic_winner(resource),
            normalized_profile_contribution(contribution),
        )
        for resource, contribution in zip(resources, contributions, strict=True)
    ]
    normalized_pairs.sort(
        key=lambda pair: (pair[0]["resource_type"], pair[0]["resource_id"]),
        reverse=True,
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="strictly_sorted"):
        _stream_semantic_pairs(normalized_pairs)


def test_checked_semantic_aggregates_reject_int64_overflow_and_inconsistency():
    with pytest.raises(ProviderDirectoryProjectionError, match="address_records_invalid"):
        _checked_add(2**63 - 1, 1, "address_records")
    outcome_map = {
        "individual_practitioners": 0,
        "organization_resources": 0,
        "practitioner_role_resources": 0,
        "addressed_locations": 0,
        "geocoded_locations": 1,
    }
    with pytest.raises(ProviderDirectoryProjectionError, match="outcomes_inconsistent"):
        _validate_outcome_consistency({}, outcome_map)


def test_outcome_proof_validator_rejects_shape_count_and_hash_types():
    outcome = candidate_semantic_outcome()
    with pytest.raises(ProviderDirectoryProjectionError, match="proof_invalid"):
        validated_semantic_outcome_proof(None)

    malformed_proof = replace(outcome, proof=None)
    with pytest.raises(ProviderDirectoryProjectionError, match="proof_invalid"):
        validated_semantic_outcome_proof(malformed_proof)

    zero_resources = replace(outcome, resource_count=0)
    with pytest.raises(ProviderDirectoryProjectionError, match="count_mismatch"):
        validated_semantic_outcome_proof(zero_resources)

    invalid_resource_counts = replace(outcome, resource_counts={})
    with pytest.raises(ProviderDirectoryProjectionError, match="count_mismatch"):
        validated_semantic_outcome_proof(invalid_resource_counts)

    invalid_hash = replace(outcome, canonical_row_sha256=None)
    with pytest.raises(ProviderDirectoryProjectionError, match="proof_invalid"):
        validated_semantic_outcome_proof(invalid_hash)


def test_source_summary_rejects_nonmapping_and_field_injection():
    outcome = candidate_semantic_outcome()
    summary = semantic_source_summary(
        semantic_adapter_contract_id="test-decoder.v1",
        transform_contract_id="test-transform.v1",
        dataset_hash=digest("dataset"),
        outcome_proof=outcome,
    )
    expected_map = {
        "expected_semantic_adapter_contract_id": "test-decoder.v1",
        "expected_transform_contract_id": "test-transform.v1",
        "expected_dataset_hash": digest("dataset"),
        "expected_outcome_proof": outcome,
    }
    with pytest.raises(ProviderDirectoryProjectionError, match="summary_invalid"):
        validated_semantic_source_summary(None, **expected_map)
    injected = deepcopy(summary)
    injected["unbound_count"] = 1
    with pytest.raises(ProviderDirectoryProjectionError, match="fields_mismatch"):
        validated_semantic_source_summary(injected, **expected_map)


def test_outcome_proof_dataclass_cannot_bypass_mapping_requirement():
    forged = ProjectionSemanticOutcomeProof(
        canonical_row_sha256=digest("rows"),
        profile_contribution_sha256=digest("profile"),
        distinct_npi_sha256=digest("npi"),
        resource_count=1,
        resource_counts={"Organization": 1},
        outcome_counts={},
        proof_sha256=digest("proof"),
        proof=[],
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="proof_invalid"):
        validated_semantic_outcome_proof(forged)
