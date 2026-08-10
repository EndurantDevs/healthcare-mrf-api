# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary tests for versioned reviewed-subset census drift profiles."""

from __future__ import annotations

import copy
import importlib

import pytest

from process import provider_directory_fhir_manual_catalog as manual_catalog
from process.provider_directory_fhir_census_execution import (
    current_version_census_completed_proof,
    current_version_census_initial_proof,
)
from process.provider_directory_fhir_subset_identity import (
    CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD,
    SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
    SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE,
    reviewed_subset_max_advertised_count_decrease,
    validated_subset_identity_values,
)
from tests.provider_directory_fhir_subset_completion_support import (
    PAGE_COUNT,
    build_subset_contract,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _completed_proof(
    *,
    legacy: bool,
    pre_count: int,
    post_count: int,
    returned_count: int,
) -> dict[str, object]:
    contract = build_subset_contract(
        strategy_version=(
            SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION
            if legacy
            else build_subset_contract().strategy_version
        ),
        completion_scopes=(
            SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES
            if legacy
            else build_subset_contract().completion_scopes
        ),
    )
    initial = current_version_census_initial_proof(
        contract,
        "Organization",
        pre_count,
        expected_page_count=PAGE_COUNT,
    )
    return current_version_census_completed_proof(
        initial,
        post_count=post_count,
        processed_rows=returned_count,
        unique_candidate_rows=returned_count,
        pages_processed=1,
        expected_page_count=PAGE_COUNT,
        terminal_page_entry_count=returned_count,
    )


def test_profile_tuple_selects_exact_count_decrease_bound():
    current_contract = build_subset_contract()

    assert reviewed_subset_max_advertised_count_decrease(
        SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
        SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    ) == 0
    assert reviewed_subset_max_advertised_count_decrease(
        current_contract.strategy_version,
        current_contract.completion_scopes,
    ) == SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE
    assert reviewed_subset_max_advertised_count_decrease(
        SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
        current_contract.completion_scopes,
    ) is None
    assert reviewed_subset_max_advertised_count_decrease(
        current_contract.strategy_version,
        SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    ) is None


def test_legacy_profile_still_rejects_one_count_decrease():
    proof = _completed_proof(
        legacy=True,
        pre_count=2,
        post_count=1,
        returned_count=1,
    )

    assert proof["verified"] is False
    assert proof["failure"] == "census_drift"


@pytest.mark.parametrize(
    ("pre_count", "post_count", "returned_count", "verified", "failure"),
    (
        (2, 2, 1, True, None),
        (2, 1, 1, True, None),
        (1, 2, 1, False, "census_drift"),
        (3, 1, 1, False, "census_drift"),
        (2, 1, 2, False, "returned_count_exceeds_advertised"),
    ),
)
def test_bounded_profile_has_closed_monotone_count_rule(
    pre_count,
    post_count,
    returned_count,
    verified,
    failure,
):
    proof = _completed_proof(
        legacy=False,
        pre_count=pre_count,
        post_count=post_count,
        returned_count=returned_count,
    )

    assert proof["verified"] is verified
    if failure is None:
        assert "failure" not in proof
    else:
        assert proof["failure"] == failure


def test_subset_metadata_accepts_only_complete_allowlisted_profile_tuple():
    contract = build_subset_contract()
    metadata = {
        "provider_directory_current_version_census_contract_version": 3,
        "provider_directory_current_version_census_page_count": PAGE_COUNT,
        CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD: contract.strategy_version,
        "provider_directory_current_version_census_traversal_version": (
            contract.traversal_version
        ),
        "provider_directory_current_version_census_canonicalization_version": (
            contract.canonicalization_version
        ),
        CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD: list(
            contract.completion_scopes
        ),
        "provider_directory_verification_campaign_id": contract.campaign_id,
    }

    assert validated_subset_identity_values(metadata)[2] == (
        contract.strategy_version
    )
    mixed = copy.deepcopy(metadata)
    mixed[CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD] = list(
        SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES
    )
    with pytest.raises(ValueError, match="identity_not_reviewed"):
        validated_subset_identity_values(mixed)


def test_artifact_source_sql_binds_profile_to_completion_proof():
    sql = importer._artifact_subset_source_identity_sql(
        "source.metadata_json::jsonb"
    )

    assert "= dataset.completion_proof_json ->> 'strategy_version'" in sql
    assert "= dataset.completion_proof_json -> 'completion_scopes'" in sql


def test_source_profile_parser_preserves_legacy_reviewed_identity():
    source_id = manual_catalog.reviewed_manual_census_source_id()
    metadata = manual_catalog.reviewed_manual_census_seed_rows(source_id)[0][
        "metadata_json"
    ]

    assert importer._is_reviewed_subset_source_metadata(metadata)
    legacy_metadata = copy.deepcopy(metadata)
    legacy_metadata[CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD] = (
        SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION
    )
    legacy_metadata[CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD] = list(
        SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES
    )
    assert importer._is_reviewed_subset_source_metadata(legacy_metadata)
