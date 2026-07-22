# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy

import pytest

import process.provider_directory_physical_projection as projection_facade
from process.provider_directory_projection_contract import canonical_row_digest
from process.provider_directory_projection_contribution import (
    MAX_REFERENCE_REDUCER_ROWS,
    SEMANTIC_OUTCOME_PROOF_CONTRACT_ID,
    SEMANTIC_STREAMING_REDUCER_CONTRACT_ID,
    reduce_semantic_outcomes,
    reduce_streaming_semantic_outcomes,
    validated_semantic_outcome_proof,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_semantic_support import (
    digest,
    semantic_pair,
    semantic_rows_and_contributions,
)


def _ordered_streams():
    resources, contributions = semantic_rows_and_contributions()
    contribution_by_identity = {
        (row["resource_type"], row["resource_id"]): row
        for row in contributions
    }
    ordered_resources = sorted(
        resources,
        key=lambda row: (row["resource_type"], row["resource_id"]),
    )
    npi_occurrence_by_value: dict[int, int] = {}
    for resource in ordered_resources:
        npi = resource["summary_npi"]
        if npi is not None:
            npi_occurrence_by_value[npi] = (
                npi_occurrence_by_value.get(npi, 0) + 1
            )
    pairs = (
        (
            resource,
            contribution_by_identity[
                (resource["resource_type"], resource["resource_id"])
            ],
        )
        for resource in ordered_resources
    )
    return pairs, iter(sorted(npi_occurrence_by_value.items()))


def test_public_projection_facade_excludes_candidate_semantic_outputs():
    candidate_symbols = {
        "INLINE_PROFILE_EVIDENCE_COLUMNS",
        "INLINE_PROFILE_EVIDENCE_CONTRACT_ID",
        "ProjectionSemanticOutcomeProof",
        "SEMANTIC_CONTRIBUTION_COLUMNS",
        "SEMANTIC_CONTRIBUTION_CONTRACT_ID",
        "SEMANTIC_EVIDENCE_HASH_FIELD",
        "SEMANTIC_GEOCODE_EVIDENCE_KEY",
        "SEMANTIC_OUTCOME_FIELDS",
        "SEMANTIC_OUTCOME_PROOF_CONTRACT_ID",
        "SEMANTIC_RELATIONSHIP_EVIDENCE_KEY",
        "SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID",
        "SEMANTIC_STREAMING_REDUCER_CONTRACT_ID",
        "SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID",
        "normalized_semantic_contribution",
        "normalized_semantic_evidence",
        "reduce_semantic_outcomes",
        "reduce_streaming_semantic_outcomes",
        "semantic_source_summary",
        "validated_semantic_outcome_proof",
        "validated_semantic_source_summary",
    }

    assert candidate_symbols.isdisjoint(projection_facade.__all__)
    assert all(
        not hasattr(projection_facade, symbol) for symbol in candidate_symbols
    )


def test_candidate_semantic_reducer_is_deterministic_exact_and_row_bound():
    resources, contributions = semantic_rows_and_contributions()
    proof = reduce_semantic_outcomes(resources, contributions)
    replay = reduce_semantic_outcomes(
        reversed(resources),
        reversed(contributions),
    )
    ordered_resources = sorted(
        resources,
        key=lambda resource: (
            resource["resource_type"],
            resource["resource_id"],
            resource["source_rank"],
            resource["payload_hash"],
        ),
    )
    canonical_row_sha256, resource_count = canonical_row_digest(ordered_resources)

    assert validated_semantic_outcome_proof(proof) == proof
    assert replay == proof
    assert proof.canonical_row_sha256 == canonical_row_sha256
    assert proof.resource_count == resource_count == 6
    assert proof.resource_counts == {
        "InsurancePlan": 1,
        "Location": 1,
        "Organization": 1,
        "OrganizationAffiliation": 1,
        "Practitioner": 1,
        "PractitionerRole": 1,
    }
    assert proof.outcome_counts == {
        "distinct_npis": 2,
        "individual_practitioners": 1,
        "organization_resources": 1,
        "address_records": 2,
        "addressed_locations": 1,
        "geocoded_locations": 1,
        "practitioner_role_resources": 1,
        "network_plan_links": 3,
        "organization_affiliation_links": 2,
        "specialty_records": 3,
        "contact_records": 1,
        "reference_links": 8,
    }


def test_candidate_streaming_and_reference_reducers_are_equivalent():
    resources, contributions = semantic_rows_and_contributions()
    reference_proof = reduce_semantic_outcomes(resources, contributions)
    semantic_pairs, npi_occurrences = _ordered_streams()

    streamed_proof = reduce_streaming_semantic_outcomes(
        semantic_pairs,
        npi_occurrences,
    )

    assert SEMANTIC_STREAMING_REDUCER_CONTRACT_ID.endswith(
        "semantic-streaming-reducer.v1"
    )
    assert (
        streamed_proof.proof["streaming_reducer_contract_id"]
        == SEMANTIC_STREAMING_REDUCER_CONTRACT_ID
    )
    assert streamed_proof == reference_proof


def test_reference_reducer_has_a_hard_row_cap():
    def oversized_resources():
        for row_index in range(MAX_REFERENCE_REDUCER_ROWS + 1):
            resource, _contribution = semantic_pair(
                "Organization",
                f"organization-{row_index:05d}",
            )
            yield resource

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="reference_reducer_row_limit_exceeded",
    ):
        reduce_semantic_outcomes(oversized_resources(), ())


@pytest.mark.parametrize(
    "npi_occurrences, expected_error",
    (
        ([(1_234_567_890, 2**63)], "npi_occurrence_invalid"),
        ([(1_234_567_890, 1)], "npi_occurrence_stream_mismatch"),
        (
            [(1_987_654_321, 1), (1_234_567_890, 2)],
            "npi_occurrences_not_strictly_sorted",
        ),
    ),
)
def test_streaming_reducer_rejects_untrusted_npi_aggregate_stream(
    npi_occurrences,
    expected_error,
):
    semantic_pairs, _expected_npi_occurrences = _ordered_streams()

    with pytest.raises(ProviderDirectoryProjectionError, match=expected_error):
        reduce_streaming_semantic_outcomes(
            semantic_pairs,
            npi_occurrences,
        )


def test_candidate_profile_arrays_bind_independently_of_physical_row_hash():
    resources, contributions = semantic_rows_and_contributions()
    baseline_proof = reduce_semantic_outcomes(resources, contributions)
    extra_specialty_map = {"code": "261Q00000X"}
    resources[0]["profile_evidence_json"]["specialties"].append(
        extra_specialty_map
    )
    contributions[0]["specialties_json"].append(extra_specialty_map)

    changed_proof = reduce_semantic_outcomes(resources, contributions)

    assert changed_proof.canonical_row_sha256 == baseline_proof.canonical_row_sha256
    assert (
        changed_proof.profile_contribution_sha256
        != baseline_proof.profile_contribution_sha256
    )
    assert changed_proof.outcome_counts["specialty_records"] == 4
    assert changed_proof.proof_sha256 != baseline_proof.proof_sha256


@pytest.mark.parametrize(
    "mutation, expected_error",
    (
        ("duplicate_winner", "semantic_winner_duplicate"),
        ("duplicate_contribution", "profile_contribution_duplicate"),
        ("missing_contribution", "semantic_pair_set_mismatch"),
        ("orphan_contribution", "semantic_pair_set_mismatch"),
    ),
)
def test_candidate_semantic_reducer_requires_exact_one_to_one_pairing(
    mutation,
    expected_error,
):
    resources, contributions = semantic_rows_and_contributions()
    if mutation == "duplicate_winner":
        resources.append(deepcopy(resources[0]))
    elif mutation == "duplicate_contribution":
        contributions.append(deepcopy(contributions[0]))
    elif mutation == "missing_contribution":
        contributions.pop()
    else:
        contributions[0]["resource_id"] = "orphan"

    with pytest.raises(ProviderDirectoryProjectionError, match=expected_error):
        reduce_semantic_outcomes(resources, contributions)


@pytest.mark.parametrize(
    "field_name, replacement",
    (
        ("proof_partition_id", digest("other-partition")),
        ("payload_hash", digest("other-payload")),
        ("source_rank", "99999999:other"),
        ("direct_npi", 1_111_111_111),
        ("active", False),
        ("effective_start", "2025-01-01"),
        ("effective_end", "2026-12-31"),
        ("observed_at", "2026-07-21T00:00:00Z"),
    ),
)
def test_candidate_semantic_reducer_rejects_cross_wired_profile_identity(
    field_name,
    replacement,
):
    resources, contributions = semantic_rows_and_contributions()
    contributions[0][field_name] = replacement

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="semantic_pair_identity_mismatch",
    ):
        reduce_semantic_outcomes(resources, contributions)


def test_candidate_reducer_rejects_array_and_address_summary_mismatch():
    resources, contributions = semantic_rows_and_contributions()
    contributions[0]["specialties_json"].append({"code": "extra"})
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="semantic_pair_evidence_mismatch",
    ):
        reduce_semantic_outcomes(resources, contributions)

    resources, contributions = semantic_rows_and_contributions()
    extra_address_map = {"line": ["3 Main St"]}
    resources[0]["profile_evidence_json"]["addresses"].append(extra_address_map)
    contributions[0]["addresses_json"].append(extra_address_map)
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="semantic_contribution_inconsistent",
    ):
        reduce_semantic_outcomes(resources, contributions)


@pytest.mark.parametrize(
    "mutation, expected_error",
    (
        ("array_type", "specialties_json_invalid"),
        ("array_nan", "specialties_json_invalid"),
        ("count_bool", "summary_address_count_invalid"),
        ("count_overflow", "summary_address_count_invalid"),
        ("geocode_without_address", "semantic_contribution_inconsistent"),
        ("active_string", "active_invalid"),
        ("unknown_inline_field", "profile_evidence_invalid"),
    ),
)
def test_candidate_semantic_reducer_rejects_noncanonical_or_untyped_rows(
    mutation,
    expected_error,
):
    resources, contributions = semantic_rows_and_contributions()
    mutation_action_by_name = {
        "array_type": lambda: contributions[0].__setitem__(
            "specialties_json",
            ("not", "json"),
        ),
        "array_nan": lambda: contributions[0].__setitem__(
            "specialties_json",
            [float("nan")],
        ),
        "count_bool": lambda: resources[0].__setitem__(
            "summary_address_count",
            True,
        ),
        "count_overflow": lambda: resources[0].__setitem__(
            "summary_address_count",
            2**31,
        ),
        "geocode_without_address": lambda: resources[0].__setitem__(
            "summary_geocoded_location",
            True,
        ),
        "active_string": lambda: resources[0].__setitem__("active", "true"),
        "unknown_inline_field": lambda: resources[0][
            "profile_evidence_json"
        ].__setitem__("unknown", []),
    }
    mutation_action_by_name[mutation]()

    with pytest.raises(ProviderDirectoryProjectionError, match=expected_error):
        reduce_semantic_outcomes(resources, contributions)


def test_candidate_outcome_validator_rejects_mutated_count_or_payload():
    resources, contributions = semantic_rows_and_contributions()
    proof = reduce_semantic_outcomes(resources, contributions)
    proof.outcome_counts["specialty_records"] += 1

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="semantic_outcome_proof_mismatch",
    ):
        validated_semantic_outcome_proof(proof)


@pytest.mark.parametrize("invalid_count", (True, 2**63))
def test_candidate_outcome_validator_rejects_bool_or_overflow(invalid_count):
    resources, contributions = semantic_rows_and_contributions()
    proof = reduce_semantic_outcomes(resources, contributions)
    proof.outcome_counts["specialty_records"] = invalid_count

    with pytest.raises(ProviderDirectoryProjectionError, match="invalid"):
        validated_semantic_outcome_proof(proof)
