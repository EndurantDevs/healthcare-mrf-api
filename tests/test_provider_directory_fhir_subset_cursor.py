# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opaque replay and neutral continuation geometry for subset v3."""

from __future__ import annotations

import copy

import pytest

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY,
    SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
)
from process.provider_directory_fhir_subset_identity import (
    SERVER_ISSUED_SUBSET_BOUNDED_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION,
)
from process.provider_directory_fhir_census_execution import (
    current_version_census_checkpoint_proof,
    current_version_census_completed_proof,
    current_version_census_initial_proof,
    resolved_current_version_census_next_url,
    validated_current_version_census_completed_proof,
    validated_current_version_census_resume_url,
)


PAGE_COUNT = 250


def _contract() -> CurrentVersionCensusContract:
    resources = SERVER_ISSUED_SUBSET_RESOURCE_TYPES
    return CurrentVersionCensusContract(
        source_id="synthetic-source",
        cutoff="2026-08-01T12:00:00.000000Z",
        resources=resources,
        expected_nonempty_resources=resources,
        start_urls=tuple(
            (
                resource_type,
                f"https://directory.example.test/fhir/{resource_type}",
            )
            for resource_type in resources
        ),
        continuation_strategy=(
            SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY
        ),
        strategy_version=SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION,
        contract_version=3,
        semantics=SERVER_ISSUED_SUBSET_SEMANTICS,
        page_count=PAGE_COUNT,
        traversal_version=SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
        canonicalization_version=(
            SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION
        ),
        completion_scopes=SERVER_ISSUED_SUBSET_BOUNDED_COMPLETION_SCOPES,
        campaign_id="synthetic-reviewed-subset-v3",
    )


def _cursor(token: str, offset: int) -> str:
    return (
        "https://directory.example.test/fhir?"
        f"_getpages={token}&_getpagesoffset={offset}&_count={PAGE_COUNT}"
    )


def _first_continuation(token: str, *, entries: int = 1):
    contract = _contract()
    return resolved_current_version_census_next_url(
        contract,
        "Organization",
        contract.start_url("Organization", PAGE_COUNT),
        _cursor(token, PAGE_COUNT),
        page_entry_count=entries,
        expected_page_count=PAGE_COUNT,
        pre_total=2,
    )


def test_opaque_tokens_are_replay_specific_but_shape_is_root_neutral():
    first = _first_continuation("opaque-root-a")
    second = _first_continuation("opaque-root-b")

    assert first.identity != second.identity
    assert first.shape_identity == second.shape_identity
    assert first.offset == second.offset == PAGE_COUNT


def test_resume_binds_same_offset_to_exact_persisted_opaque_token():
    contract = _contract()
    start_url = contract.start_url("Organization", PAGE_COUNT)
    continuation = _first_continuation("opaque-root-a")
    proof = current_version_census_initial_proof(
        contract,
        "Organization",
        2,
        expected_page_count=PAGE_COUNT,
    )
    proof = current_version_census_checkpoint_proof(
        proof,
        pages_processed=1,
        rows_processed=1,
        page_entry_count=1,
        expected_page_count=PAGE_COUNT,
        continuation_identity_sha256=continuation.identity,
        continuation_shape_sha256=continuation.shape_identity,
    )

    assert validated_current_version_census_resume_url(
        contract,
        "Organization",
        start_url,
        continuation.url,
        pages_processed=1,
        rows_processed=1,
        expected_page_count=PAGE_COUNT,
        proof=proof,
    ) == continuation.url
    with pytest.raises(ValueError, match="resume_identity_invalid"):
        validated_current_version_census_resume_url(
            contract,
            "Organization",
            start_url,
            _cursor("substituted-token", PAGE_COUNT),
            pages_processed=1,
            rows_processed=1,
            expected_page_count=PAGE_COUNT,
            proof=proof,
        )


@pytest.mark.parametrize(
    "missing_field",
    ("page_entry_counts", "continuation_hop_sha256", "continuation_shape_sha256"),
)
def test_resume_rejects_missing_ordered_evidence_before_transport(missing_field):
    contract = _contract()
    continuation = _first_continuation("opaque-root-a")
    proof = current_version_census_initial_proof(
        contract,
        "Organization",
        2,
        expected_page_count=PAGE_COUNT,
    )
    proof = current_version_census_checkpoint_proof(
        proof,
        pages_processed=1,
        rows_processed=1,
        page_entry_count=1,
        expected_page_count=PAGE_COUNT,
        continuation_identity_sha256=continuation.identity,
        continuation_shape_sha256=continuation.shape_identity,
    )
    malformed = copy.deepcopy(proof)
    malformed.pop(missing_field)

    with pytest.raises(ValueError, match="page_geometry_invalid"):
        validated_current_version_census_resume_url(
            contract,
            "Organization",
            contract.start_url("Organization", PAGE_COUNT),
            continuation.url,
            pages_processed=1,
            rows_processed=1,
            expected_page_count=PAGE_COUNT,
            proof=malformed,
        )


def test_sparse_and_empty_pages_remain_valid_until_terminal_no_next():
    contract = _contract()
    first = _first_continuation("opaque-root-a", entries=1)
    second = resolved_current_version_census_next_url(
        contract,
        "Organization",
        first.url,
        _cursor("opaque-root-a", PAGE_COUNT * 2),
        page_entry_count=0,
        expected_page_count=PAGE_COUNT,
        pre_total=2,
    )
    proof = current_version_census_initial_proof(
        contract,
        "Organization",
        2,
        expected_page_count=PAGE_COUNT,
    )
    proof = current_version_census_checkpoint_proof(
        proof,
        pages_processed=1,
        rows_processed=1,
        page_entry_count=1,
        expected_page_count=PAGE_COUNT,
        continuation_identity_sha256=first.identity,
        continuation_shape_sha256=first.shape_identity,
    )
    proof = current_version_census_checkpoint_proof(
        proof,
        pages_processed=2,
        rows_processed=1,
        page_entry_count=0,
        expected_page_count=PAGE_COUNT,
        continuation_identity_sha256=second.identity,
        continuation_shape_sha256=second.shape_identity,
    )
    completed = current_version_census_completed_proof(
        proof,
        post_count=2,
        processed_rows=1,
        unique_candidate_rows=1,
        pages_processed=3,
        expected_page_count=PAGE_COUNT,
        terminal_page_entry_count=0,
    )

    validated = validated_current_version_census_completed_proof(
        completed,
        contract,
        "Organization",
        rows_processed=1,
        pages_processed=3,
    )
    assert validated["verified"] is True
    assert validated["deficit"] == 1
    assert validated["page_entry_counts"] == [1, 0, 0]
