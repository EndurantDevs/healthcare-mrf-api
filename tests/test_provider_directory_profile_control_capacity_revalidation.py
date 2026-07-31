# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Control WAL revalidation and progress-accounting contracts."""

from __future__ import annotations

import dataclasses

import pytest

from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_capacity_projection import _projection_geometry
from tests.test_provider_directory_profile_control_capacity import (
    _bound_control_wal_projection,
    _control_metadata_input,
    _control_operations_by_name,
    _control_wal_plan_input,
)

def test_control_data_revalidation_requires_exact_geometry_upper_bound():
    geometry, projection = _bound_control_wal_projection()
    changed_geometry = capacity.revalidate_capacity_geometry(
        dataclasses.replace(
            geometry,
            control_metadata_data_upper_bound_bytes=(
                geometry.control_metadata_data_upper_bound_bytes + 1
            ),
        )
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="control_metadata_data_projection_geometry_bound_mismatch",
    ):
        capacity.revalidate_profile_control_wal_projection(
            changed_geometry,
            projection,
        )


def test_control_data_revalidation_rejects_formula_tamper():
    geometry, projection = _bound_control_wal_projection()
    operations = list(projection.operations)
    operation_index = next(
        index
        for index, operation in enumerate(operations)
        if operation.operation_name == "capacity_consumption_insert"
    )
    operation = operations[operation_index]
    operations[operation_index] = dataclasses.replace(
        operation,
        metadata_data_bytes_per_operation=(
            operation.metadata_data_bytes_per_operation + 1
        ),
        metadata_data_bytes=operation.metadata_data_bytes + 1,
    )
    tampered = dataclasses.replace(
        projection,
        operations=tuple(operations),
        total_control_metadata_data_bytes=(
            projection.total_control_metadata_data_bytes + 1
        ),
    )
    tampered_geometry = capacity.revalidate_capacity_geometry(
        dataclasses.replace(
            geometry,
            control_metadata_data_upper_bound_bytes=(
                tampered.total_control_metadata_data_bytes
            ),
        )
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="control_wal_projection_formula_changed",
    ):
        capacity.revalidate_profile_control_wal_projection(
            tampered_geometry,
            tampered,
        )


def test_control_wal_revalidation_rejects_self_consistent_formula_tamper():
    geometry, projection = _bound_control_wal_projection()
    operations = list(projection.operations)
    operation_index = next(
        index
        for index, operation in enumerate(operations)
        if operation.operation_name == "capacity_consumption_insert"
    )
    operation = operations[operation_index]
    operations[operation_index] = dataclasses.replace(
        operation,
        metadata_wal_bytes=operation.metadata_wal_bytes + 1,
        wal_bytes_per_operation=operation.wal_bytes_per_operation + 1,
        wal_bytes=operation.wal_bytes + 1,
    )
    tampered = dataclasses.replace(
        projection,
        operations=tuple(operations),
        pre_cutover_wal_bytes=projection.pre_cutover_wal_bytes + 1,
        total_control_wal_bytes=projection.total_control_wal_bytes + 1,
    )
    tampered_geometry = capacity.revalidate_capacity_geometry(
        dataclasses.replace(
            geometry,
            control_wal_upper_bound_bytes=tampered.total_control_wal_bytes,
        )
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="control_wal_projection_formula_changed",
    ):
        capacity.revalidate_profile_control_wal_projection(
            tampered_geometry,
            tampered,
        )


def test_remaining_control_wal_tracks_committed_counts_and_failure_release():
    _, projection = _bound_control_wal_projection()
    operations = _control_operations_by_name(projection)
    completed_by_operation = {
        "artifact_scope_layout": 10,
        "evidence_payload": 2,
    }
    consumed = (
        10 * operations["artifact_scope_layout"].wal_bytes_per_operation
        + 2 * operations["evidence_payload"].wal_bytes_per_operation
    )

    assert capacity.remaining_profile_control_wal_bytes(
        projection,
        completed_by_operation,
    ) == projection.total_control_wal_bytes - consumed
    assert capacity.remaining_profile_control_wal_bytes(
        projection,
        completed_by_operation,
        failure_reserve_released=True,
    ) == (
        projection.total_control_wal_bytes
        - consumed
        - projection.failure_reserve_wal_bytes
    )


@pytest.mark.parametrize(
    ("completed", "reason"),
    [
        ({"missing_operation": 1}, "completed_operation_unknown"),
        ({"evidence_payload": 5}, "completed_operation_exceeded"),
        ({"evidence_payload": True}, "completed_operation_count"),
    ],
)
def test_remaining_control_wal_rejects_invalid_progress(completed, reason):
    _, projection = _bound_control_wal_projection()

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match=reason,
    ):
        capacity.remaining_profile_control_wal_bytes(
            projection,
            completed,
        )


@pytest.mark.parametrize(
    ("completed", "failure_released", "reason"),
    [
        ([], False, "completed_operation_invalid"),
        ({}, 1, "failure_release_invalid"),
    ],
)
def test_remaining_control_wal_requires_exact_progress_types(
    completed,
    failure_released,
    reason,
):
    _, projection = _bound_control_wal_projection()

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match=reason,
    ):
        capacity.remaining_profile_control_wal_bytes(
            projection,
            completed,
            failure_reserve_released=failure_released,
        )


def test_control_wal_projection_rejects_artifact_batch_order_drift():
    plan_input = _control_wal_plan_input()
    reordered = dataclasses.replace(
        plan_input,
        artifact_batch_counts=tuple(
            reversed(plan_input.artifact_batch_counts)
        ),
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="artifact_batch_order",
    ):
        capacity.profile_control_wal_plan_input_hash(reordered)


def test_control_wal_projection_keeps_final_cutover_projection_separate():
    geometry, control = _bound_control_wal_projection()
    final_metadata = capacity.project_profile_delta_metadata_capacity(
        geometry,
        (
            _control_metadata_input("build_checkpoint", "update"),
            _control_metadata_input("serving_generation", "update"),
            _control_metadata_input("delta_receipt", "insert"),
        ),
        pending_commit_items=1,
    )

    assert control.final_cutover_contract_id == (
        capacity.CUTOVER_FORECAST_CONTRACT_ID
    )
    assert final_metadata.wal_bytes > 0
    assert final_metadata.commit_envelope_bytes > 0
    assert control.total_control_wal_bytes == sum(
        operation.wal_bytes for operation in control.operations
    )
