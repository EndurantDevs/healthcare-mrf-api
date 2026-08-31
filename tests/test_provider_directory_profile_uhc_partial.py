# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed Profile authority for reviewed retry-exhausted UHC coverage."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from process import provider_directory_profile_uhc_flex as flex_profile
from process.provider_directory_dataset_scoped_publication import (
    LEGACY_PRACTITIONER_VARIANT,
    ROOTED_COMBINED_VARIANT,
)
from process.provider_directory_fhir_root_policy import ReviewedRootPolicy
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_twin_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID
from process.uhc_flex_practitioner_single_root_contract import (
    UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
)
from tests.provider_directory_profile_uhc_flex_test_support import (
    GRAPH_DATASET_ID,
    GRAPH_ENDPOINT_ID,
    _dataset_rows,
    _flex_metadata,
    _readiness_record,
    _rooted_dataset_rows,
    _rooted_metadata,
    _rooted_readiness_record,
)


def _partial_flex_metadata() -> dict[str, object]:
    metadata_by_field = _flex_metadata()
    metadata_by_field.update(
        {
            "admission_contract_id": (
                UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID
            ),
            "cohort_complete": False,
            "retry_exhausted_count": 1,
            "provider_directory_reviewed_root_policy_v1": (
                ReviewedRootPolicy(1).document()
            ),
        }
    )
    metadata_by_field.pop("baseline_acquisition_id")
    metadata_by_field.pop("baseline_run_id")
    return metadata_by_field


def _fence_dataset(readiness: SimpleNamespace, variant: str) -> SimpleNamespace:
    return SimpleNamespace(
        dataset_scoped_ready=True,
        dataset_scoped_variant=variant,
        dataset_scoped_cohort_complete=readiness.cohort_complete,
        dataset_id=readiness.dataset_id,
        endpoint_id=readiness.endpoint_id,
        source_id=readiness.source_id,
        dataset_hash=readiness.dataset_hash,
        resource_count=readiness.resource_count,
        semantic_projection_as_of=readiness.semantic_projection_as_of,
        source_authority_id=readiness.source_authority_id,
        admission_id=readiness.admission_id,
        operation_key=readiness.operation_key,
        reviewed_root_policy=ReviewedRootPolicy(1),
    )


def test_retry_exhausted_flex_publication_is_profile_ready() -> None:
    metadata_by_field = _partial_flex_metadata()
    dataset_row_by_field = _dataset_rows()[0]
    dataset_row_by_field.update(
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        publication_metadata_json=metadata_by_field,
        dataset_scoped_cohort_complete=False,
    )
    readiness = _readiness_record(cohort_complete=False, retry_exhausted_count=1)

    assert flex_profile.is_uhc_flex_dataset_row_ready(dataset_row_by_field)
    assert flex_profile.is_uhc_flex_dataset_readiness_matching(
        readiness,
        dataset_row_by_field,
    )
    assert flex_profile.is_uhc_flex_fence_dataset_ready(
        _fence_dataset(readiness, LEGACY_PRACTITIONER_VARIANT),
        readiness,
    )
    readiness.retry_exhausted_count = 2
    assert not flex_profile.is_uhc_flex_dataset_readiness_matching(
        readiness,
        dataset_row_by_field,
    )


@pytest.mark.parametrize(
    "mutation",
    ("missing_count", "zero_count", "twin_admission", "complete_with_count"),
)
def test_retry_exhausted_flex_profile_metadata_fails_closed(mutation: str) -> None:
    metadata_by_field = _partial_flex_metadata()
    if mutation == "missing_count":
        metadata_by_field.pop("retry_exhausted_count")
    elif mutation == "zero_count":
        metadata_by_field["retry_exhausted_count"] = 0
    elif mutation == "twin_admission":
        metadata_by_field = _flex_metadata()
        metadata_by_field.update(
            cohort_complete=False,
            retry_exhausted_count=1,
        )
    else:
        metadata_by_field["cohort_complete"] = True

    assert not flex_profile.is_uhc_flex_publication_metadata_valid(
        metadata_by_field,
        dataset_id=metadata_by_field["dataset_id"],
        endpoint_id=metadata_by_field["endpoint_id"],
        evidence_run_id=metadata_by_field["acquisition_root_run_id"],
    )


def _partial_rooted_metadata() -> dict[str, object]:
    metadata_by_field = _rooted_metadata()
    metadata_by_field.update(
        admission_contract_id=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID
        ),
        attempt_id=None,
        comparison_acquisition_id=None,
        cohort_complete=False,
        retry_exhausted_count=1,
        provider_directory_reviewed_root_policy_v1=(
            ReviewedRootPolicy(1).document()
        ),
        acquisition_operation_key="a" * 64,
    )
    return metadata_by_field


def test_retry_exhausted_rooted_publication_is_profile_ready() -> None:
    metadata_by_field = _partial_rooted_metadata()
    dataset_row_by_field = _rooted_dataset_rows()[0]
    dataset_row_by_field.update(
        source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        publication_metadata_json=metadata_by_field,
        dataset_scoped_cohort_complete=False,
    )
    readiness = _rooted_readiness_record(
        cohort_complete=False,
        retry_exhausted_count=1,
    )

    assert flex_profile.is_uhc_flex_dataset_row_ready(dataset_row_by_field)
    assert flex_profile.is_uhc_flex_dataset_readiness_matching(
        readiness,
        dataset_row_by_field,
    )
    fence_dataset = _fence_dataset(readiness, ROOTED_COMBINED_VARIANT)
    assert flex_profile.is_uhc_flex_fence_dataset_ready(fence_dataset, readiness)
    fence_dataset.reviewed_root_policy = None
    assert not flex_profile.is_uhc_flex_fence_dataset_ready(fence_dataset, readiness)

    twin_metadata_by_field = _rooted_metadata()
    twin_metadata_by_field.update(
        cohort_complete=False,
        retry_exhausted_count=1,
    )
    assert not flex_profile.is_uhc_flex_publication_metadata_valid(
        twin_metadata_by_field,
        dataset_id=GRAPH_DATASET_ID,
        endpoint_id=GRAPH_ENDPOINT_ID,
        evidence_run_id=twin_metadata_by_field["acquisition_root_run_id"],
    )
