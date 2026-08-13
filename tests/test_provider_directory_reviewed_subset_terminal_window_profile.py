# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary proof for the reviewed terminal-window drift profile."""

from __future__ import annotations

import copy

import pytest

from process.provider_directory_fhir_census_execution import (
    current_version_census_completed_proof,
    current_version_census_initial_proof,
    validated_current_version_census_completed_proof,
)
from process.provider_directory_fhir_subset_completion import (
    build_subset_completion_proof,
    canonical_sha256,
    validate_subset_completion_proof_pair,
)
from process.provider_directory_fhir_subset_identity import (
    SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE_BASIS_POINTS,
    SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE_PAGES,
    SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
    reviewed_subset_advertised_count_decrease_limit,
)
from process.provider_directory_fhir_subset_execution import (
    reviewed_subset_completion_constraints,
)
from process.provider_directory_fhir_subset_profiles import (
    is_advertised_pre_in_terminal_window,
)
from tests.provider_directory_fhir_subset_completion_support import (
    PAGE_COUNT,
    build_subset_contract,
)


def _current_contract():
    return build_subset_contract(
        strategy_version=SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        completion_scopes=SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
        campaign_id="synthetic-terminal-window-profile",
    )


def _terminal_proof(
    *,
    pre_count: int,
    post_count: int,
    pages_processed: int,
) -> tuple[object, dict[str, object]]:
    contract = _current_contract()
    proof = current_version_census_initial_proof(
        contract,
        "PractitionerRole",
        pre_count,
        expected_page_count=PAGE_COUNT,
    )
    checkpoint_pages = pages_processed - 1
    proof["page_geometry"] = {
        "version": 2,
        "page_count": PAGE_COUNT,
        "checkpointed_pages": checkpoint_pages,
        "checkpointed_rows": 0,
        "logical_next_offset": checkpoint_pages * PAGE_COUNT,
        "sparse_pages": checkpoint_pages,
        "empty_pages": checkpoint_pages,
    }
    proof["page_entry_counts"] = [0] * checkpoint_pages
    proof["continuation_hop_sha256"] = ["a" * 64] * checkpoint_pages
    proof["continuation_shape_sha256"] = ["b" * 64] * checkpoint_pages
    completed = current_version_census_completed_proof(
        proof,
        post_count=post_count,
        processed_rows=0,
        unique_candidate_rows=0,
        pages_processed=pages_processed,
        expected_page_count=PAGE_COUNT,
        terminal_page_entry_count=0,
    )
    return contract, completed


def test_terminal_window_profile_uses_closed_page_and_percentage_cap():
    assert SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE_PAGES == 20
    assert SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE_BASIS_POINTS == 100
    assert reviewed_subset_advertised_count_decrease_limit(
        SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
        pre_count=512_034,
        page_count=PAGE_COUNT,
    ) == 5_000
    assert reviewed_subset_advertised_count_decrease_limit(
        SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
        pre_count=99,
        page_count=PAGE_COUNT,
    ) == 1
    assert reviewed_subset_advertised_count_decrease_limit(
        SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
        pre_count=101,
        page_count=PAGE_COUNT,
    ) == 2
    for crossover_pre_count in (500_000, 500_001):
        assert reviewed_subset_advertised_count_decrease_limit(
            SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
            SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
            pre_count=crossover_pre_count,
            page_count=PAGE_COUNT,
        ) == 5_000
    assert reviewed_subset_advertised_count_decrease_limit(
        "unsupported-profile",
        (),
        pre_count=1,
        page_count=PAGE_COUNT,
    ) is None
    assert not is_advertised_pre_in_terminal_window(1, None)


def test_terminal_window_profile_rejects_invalid_count_without_coercion():
    contract = _current_contract()
    with pytest.raises(ValueError, match="current_version_census_profile_invalid"):
        contract.advertised_count_decrease_limit(True)

    proof = current_version_census_initial_proof(
        contract,
        "PractitionerRole",
        1,
        expected_page_count=PAGE_COUNT,
    )
    proof["pre_count"] = True
    with pytest.raises(ValueError, match="current_version_census_profile_invalid"):
        reviewed_subset_completion_constraints(proof)


@pytest.mark.parametrize(
    ("decrease", "verified"),
    ((0, True), (2_056, True), (5_000, True), (5_001, False)),
)
def test_terminal_window_profile_has_closed_live_scale_bound(
    decrease,
    verified,
):
    contract, completed = _terminal_proof(
        pre_count=512_034,
        post_count=512_034 - decrease,
        pages_processed=2_049,
    )

    assert completed["verified"] is verified
    if verified:
        assert "failure" not in completed
        validated_current_version_census_completed_proof(
            completed,
            contract,
            "PractitionerRole",
            rows_processed=0,
            pages_processed=2_049,
        )
    else:
        assert completed["failure"] == "census_drift"


@pytest.mark.parametrize(
    ("decrease", "verified"),
    ((6, True), (7, False)),
)
def test_terminal_window_profile_has_closed_percentage_bound(
    decrease,
    verified,
):
    contract, completed = _terminal_proof(
        pre_count=502,
        post_count=502 - decrease,
        pages_processed=3,
    )

    assert completed["verified"] is verified
    if verified:
        validated_current_version_census_completed_proof(
            completed,
            contract,
            "PractitionerRole",
            rows_processed=0,
            pages_processed=3,
        )
    else:
        assert completed["failure"] == "census_drift"


def test_terminal_window_profile_rejects_early_source_exhaustion():
    contract, completed = _terminal_proof(
        pre_count=512_251,
        post_count=509_978,
        pages_processed=2_048,
    )

    assert completed["verified"] is False
    assert completed["failure"] == "terminal_count_window_mismatch"
    forged = copy.deepcopy(completed)
    forged["verified"] = True
    forged.pop("failure")
    with pytest.raises(ValueError, match="completed_proof_invalid"):
        validated_current_version_census_completed_proof(
            forged,
            contract,
            "PractitionerRole",
            rows_processed=0,
            pages_processed=2_048,
        )


@pytest.mark.parametrize(
    ("pre_count", "verified"),
    (
        (511_999, False),
        (512_000, True),
        (512_250, True),
        (512_499, True),
        (512_500, False),
    ),
)
def test_terminal_window_profile_has_inclusive_terminal_envelope(
    pre_count,
    verified,
):
    _contract_value, completed = _terminal_proof(
        pre_count=pre_count,
        post_count=pre_count,
        pages_processed=2_049,
    )

    assert completed["verified"] is verified
    if verified:
        assert "failure" not in completed
    else:
        assert completed["failure"] == "terminal_count_window_mismatch"


def _completion_arguments(contract, execution_proof):
    resources = contract.resources
    return {
        "contract": contract,
        "resource_proof_by_type": dict.fromkeys(resources, execution_proof),
        "dataset_hash": "c" * 64,
        "resource_count": 0,
        "resource_hash_by_type": dict.fromkeys(resources, "d" * 64),
        "acquired_resource_hash_by_type": dict.fromkeys(resources, "e" * 64),
        "resource_count_by_type": dict.fromkeys(resources, 0),
    }


def test_terminal_window_profile_builds_and_revalidates_canonical_proof():
    contract, execution_proof = _terminal_proof(
        pre_count=512_034,
        post_count=507_034,
        pages_processed=2_049,
    )

    proof, proof_sha256 = build_subset_completion_proof(
        **_completion_arguments(contract, execution_proof)
    )
    assert validate_subset_completion_proof_pair(proof, proof_sha256) == (
        proof,
        proof_sha256,
    )

    tampered = copy.deepcopy(proof)
    tampered_resource = tampered["resources"]["PractitionerRole"]
    tampered_resource["advertised_pre"] = 512_500
    tampered_resource["advertised_post"] = 512_500
    tampered_resource["deficit"] = 512_500
    with pytest.raises(ValueError, match="completion_resource_invalid"):
        validate_subset_completion_proof_pair(
            tampered,
            canonical_sha256(tampered),
        )


def test_terminal_window_profile_builder_rejects_forged_early_exhaustion():
    contract, execution_proof = _terminal_proof(
        pre_count=512_251,
        post_count=509_978,
        pages_processed=2_048,
    )
    execution_proof["verified"] = True
    execution_proof.pop("failure")

    with pytest.raises(ValueError, match="completion_counts_invalid"):
        build_subset_completion_proof(
            **_completion_arguments(contract, execution_proof)
        )


def test_terminal_window_canonical_proof_uses_percentage_bound():
    contract, execution_proof = _terminal_proof(
        pre_count=502,
        post_count=496,
        pages_processed=3,
    )
    proof, proof_sha256 = build_subset_completion_proof(
        **_completion_arguments(contract, execution_proof)
    )
    assert validate_subset_completion_proof_pair(proof, proof_sha256) == (
        proof,
        proof_sha256,
    )

    tampered = copy.deepcopy(proof)
    tampered["resources"]["PractitionerRole"]["advertised_post"] = 495
    with pytest.raises(ValueError, match="completion_resource_invalid"):
        validate_subset_completion_proof_pair(
            tampered,
            canonical_sha256(tampered),
        )
