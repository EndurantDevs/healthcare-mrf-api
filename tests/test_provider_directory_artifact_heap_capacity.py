# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Physical projection for the bounded artifact heap before its indexes exist."""

from dataclasses import replace

import pytest

from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_bounded_capacity import _bounded_geometry
from tests.test_provider_directory_profile_capacity_projection import _projection_geometry


def _heap_input():
    return capacity.ProviderDirectoryProfileScratchInput(
        relation_name="artifact_scope",
        inserted_rows=1,
        inserted_logical_bytes=160,
        toastable_column_count=21,
        main_index_pages=(),
        toast_index_pages=(1,),
    )


def test_bounded_artifact_heap_projects_real_heap_and_toast_without_main_index():
    projection = capacity.project_profile_scratch_capacity(_bounded_geometry()[0], _heap_input(), enforce_caps=False)
    assert projection.inserted_toast_chunks_upper == 22
    assert projection.growth_bytes == 382 * 8192
    assert projection.wal_bytes == 565 * 3 * 8192


def test_bounded_artifact_heap_still_enforces_requested_caps():
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError, match="scratch_projection_growth_exceeded"):
        capacity.project_profile_scratch_capacity(_bounded_geometry()[0], _heap_input())


@pytest.mark.parametrize("relation", ["evidence_stage", "affected_npi_stage", "profile_stage"])
def test_other_bounded_scratch_classes_still_require_main_indexes(relation):
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError, match="scratch_projection_input_invalid"):
        capacity.project_profile_scratch_capacity(
            _bounded_geometry()[0], replace(_heap_input(), relation_name=relation), enforce_caps=False
        )


def test_unbounded_artifact_scope_still_requires_main_indexes():
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError, match="scratch_projection_input_invalid"):
        capacity.project_profile_scratch_capacity(_projection_geometry(), _heap_input(), enforce_caps=False)


@pytest.mark.parametrize("updates", [{"main_index_pages": (False,)}, {"toast_index_pages": (0,)}])
def test_bounded_artifact_heap_still_rejects_malformed_index_pages(updates):
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError, match="scratch_projection_input_invalid"):
        capacity.project_profile_scratch_capacity(
            _bounded_geometry()[0], replace(_heap_input(), **updates), enforce_caps=False
        )
