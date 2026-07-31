# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Physical data, scratch, metadata, and WAL projection contracts."""

from __future__ import annotations

import pytest

from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_capacity import (
    _geometry_payload,
    _relation_byte_caps,
)

def _projection_geometry(
    *,
    evidence_growth_cap: int = 10_000_000,
) -> capacity.ProviderDirectoryProfileCapacityGeometry:
    relation_caps = _relation_byte_caps()
    for relation_cap in relation_caps[:4]:
        relation_cap["max_scratch_bytes"] = 10_000_000
        relation_cap["max_wal_bytes"] = 10_000_000
    for relation_cap in relation_caps[4:]:
        relation_cap["max_target_growth_bytes"] = 10_000_000
        relation_cap["max_deleted_logical_bytes"] = 10_000_000
        relation_cap["max_wal_bytes"] = 10_000_000
    relation_caps[4]["max_target_growth_bytes"] = evidence_growth_cap
    return capacity.validated_capacity_geometry(
        _geometry_payload(relation_byte_caps=relation_caps)
    )


def _projection_inputs():
    return (
        capacity.ProviderDirectoryProfileTargetDeltaInput(
            relation_name="evidence_target",
            inserted_rows=2,
            inserted_toast_chunks=1,
            deleted_rows=1,
            deleted_logical_bytes=100,
            deleted_toast_chunks=0,
            main_index_pages=(1, 8),
            toast_index_pages=(1,),
        ),
        capacity.ProviderDirectoryProfileTargetDeltaInput(
            relation_name="profile_target",
            inserted_rows=0,
            inserted_toast_chunks=0,
            deleted_rows=0,
            deleted_logical_bytes=0,
            deleted_toast_chunks=0,
            main_index_pages=(1,),
            toast_index_pages=(1,),
        ),
    )


def test_pg18_delta_projection_uses_fixed_heap_btree_and_wal_formula():
    projection = capacity.project_profile_delta_capacity(
        _projection_geometry(),
        _projection_inputs(),
    )

    evidence = projection.targets[0]
    assert evidence.target_growth_bytes == 64 * 8192
    assert evidence.deleted_logical_bytes == 100
    assert evidence.wal_bytes == 103 * 3 * 8192
    assert projection.target_data_bytes == evidence.target_growth_bytes
    assert projection.wal_bytes == evidence.wal_bytes


def test_pg18_delta_projection_refuses_one_byte_below_growth_cap():
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="delta_projection_target_growth_exceeded:evidence_target",
    ):
        capacity.project_profile_delta_capacity(
            _projection_geometry(
                evidence_growth_cap=(64 * 8192) - 1,
            ),
            _projection_inputs(),
        )


def test_pg18_scratch_projection_bounds_heap_toast_indexes_and_wal():
    projection = capacity.project_profile_scratch_capacity(
        _projection_geometry(),
        capacity.ProviderDirectoryProfileScratchInput(
            relation_name="evidence_stage",
            inserted_rows=2,
            inserted_logical_bytes=1997,
            toastable_column_count=2,
            main_index_pages=(1,),
            toast_index_pages=(1,),
        ),
    )

    assert projection.inserted_toast_chunks_upper == 6
    assert projection.growth_bytes == 106 * 8192
    assert projection.wal_bytes == 158 * 3 * 8192


def test_pg18_scratch_projection_enforces_named_relation_wal_cap():
    relation_caps = _relation_byte_caps()
    for relation_cap in relation_caps[:4]:
        relation_cap["max_scratch_bytes"] = 10_000_000
        relation_cap["max_wal_bytes"] = 10_000_000
    relation_caps[1]["max_wal_bytes"] = (158 * 3 * 8192) - 1
    geometry = capacity.validated_capacity_geometry(
        _geometry_payload(relation_byte_caps=relation_caps)
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="scratch_projection_wal_exceeded:evidence_stage",
    ):
        capacity.project_profile_scratch_capacity(
            geometry,
            capacity.ProviderDirectoryProfileScratchInput(
                relation_name="evidence_stage",
                inserted_rows=2,
                inserted_logical_bytes=1997,
                toastable_column_count=2,
                main_index_pages=(1,),
                toast_index_pages=(1,),
            ),
        )


def test_pg18_metadata_projection_is_formula_derived_below_signed_caps():
    projection = capacity.project_profile_delta_metadata_capacity(
        _projection_geometry(),
        (
            capacity.ProviderDirectoryProfileMetadataMutationInput(
                relation_name="build_checkpoint",
                operation="update",
                payload_upper_bytes=64 * 1024,
                deleted_toast_chunks=2,
                main_index_pages=(1, 1),
                toast_index_pages=(1,),
            ),
            capacity.ProviderDirectoryProfileMetadataMutationInput(
                relation_name="serving_generation",
                operation="update",
                payload_upper_bytes=10_000,
                deleted_toast_chunks=2,
                main_index_pages=(1,),
                toast_index_pages=(1,),
            ),
            capacity.ProviderDirectoryProfileMetadataMutationInput(
                relation_name="delta_receipt",
                operation="insert",
                payload_upper_bytes=64 * 1024,
                deleted_toast_chunks=0,
                main_index_pages=(1, 1),
                toast_index_pages=(1,),
            ),
        ),
        pending_commit_items=4,
    )

    assert 0 < projection.data_bytes <= (
        capacity.METADATA_DATA_UPPER_BOUND_BYTES
    )
    assert 0 < projection.wal_bytes
    assert projection.commit_envelope_bytes == 5 * 8192
    assert (
        projection.wal_bytes + projection.commit_envelope_bytes
        <= capacity.METADATA_WAL_UPPER_BOUND_BYTES
    )
