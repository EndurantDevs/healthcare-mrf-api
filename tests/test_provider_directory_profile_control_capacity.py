# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Control-plane WAL projection contracts for Profile builds."""

from __future__ import annotations

import dataclasses

import pytest

from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_capacity_projection import _projection_geometry

def _control_metadata_input(relation_name, operation, **overrides):
    metadata_by_field = {
        "relation_name": relation_name,
        "operation": operation,
        "payload_upper_bytes": 1_024,
        "deleted_toast_chunks": 0,
        "main_index_pages": (1,),
        "toast_index_pages": (1,),
    }
    metadata_by_field.update(overrides)
    return capacity.ProviderDirectoryProfileMetadataMutationInput(
        **metadata_by_field
    )


def _control_wal_plan_input(
    *,
    artifact_batch_counts=(2, 1, 0, 0, 0, 0, 0, 0, 0),
    evidence_batch_count=4,
    compact_batch_count=3,
    affected_source_count=2,
    admission_row_lock_count=2,
    cutover_row_lock_count=12,
    **overrides,
):
    plan_by_field = {
        "artifact_batch_counts": tuple(
            capacity.ProfileControlArtifactBatchCount(
                artifact_name=artifact_name,
                batch_count=batch_count,
            )
            for artifact_name, batch_count in zip(
                capacity.CONTROL_WAL_ARTIFACT_SCOPE_NAMES,
                artifact_batch_counts,
                strict=True,
            )
        ),
        "artifact_scope_recovery_contract_id": (
            capacity.ARTIFACT_SCOPE_RECOVERY_CONTRACT_ID
        ),
        "evidence_batch_count": evidence_batch_count,
        "compact_batch_count": compact_batch_count,
        "affected_source_count": affected_source_count,
        "admission_row_lock_count": admission_row_lock_count,
        "cutover_row_lock_count": cutover_row_lock_count,
        "build_checkpoint_insert": _control_metadata_input(
            "build_checkpoint",
            "insert",
        ),
        "build_checkpoint_update": _control_metadata_input(
            "build_checkpoint",
            "update",
        ),
        "import_run_update": _control_metadata_input(
            "import_run",
            "update",
        ),
        "capacity_consumption_insert": _control_metadata_input(
            "capacity_consumption",
            "insert",
        ),
    }
    plan_by_field.update(overrides)
    return capacity.ProfileControlWalPlanInput(**plan_by_field)


def _control_operations_by_name(projection):
    return {
        operation.operation_name: operation
        for operation in projection.operations
    }


def _bound_control_wal_projection(plan_input=None):
    """Build the exact two-pass geometry without hashing the projection."""

    resolved_plan_input = plan_input or _control_wal_plan_input()
    seed_geometry = capacity.revalidate_capacity_geometry(
        dataclasses.replace(
            _projection_geometry(),
            control_wal_plan_input_hash=(
                capacity.profile_control_wal_plan_input_hash(
                    resolved_plan_input
                )
            ),
            control_wal_upper_bound_bytes=1,
            control_metadata_data_upper_bound_bytes=1,
        )
    )
    seed_projection = capacity.project_profile_control_wal_capacity(
        seed_geometry,
        resolved_plan_input,
    )
    geometry = capacity.revalidate_capacity_geometry(
        dataclasses.replace(
            seed_geometry,
            control_wal_upper_bound_bytes=(
                seed_projection.total_control_wal_bytes
            ),
            control_metadata_data_upper_bound_bytes=(
                seed_projection.total_control_metadata_data_bytes
            ),
        )
    )
    projection = capacity.project_profile_control_wal_capacity(
        geometry,
        resolved_plan_input,
    )
    return geometry, projection


def test_control_wal_projection_derives_exact_batch_operation_counts():
    _, projection = _bound_control_wal_projection()
    operations = _control_operations_by_name(projection)

    assert operations["artifact_scope_payload"].operation_count == 3
    assert operations["evidence_payload"].operation_count == 4
    assert operations["evidence_checkpoint_advance"].operation_count == 4
    assert (
        operations["evidence_import_run_progress"].operation_count
        == 4
    )
    assert operations["affected_npi_payload"].operation_count == 3
    assert operations["profile_payload"].operation_count == 3
    assert operations["profile_checkpoint_advance"].operation_count == 3
    assert (
        operations["profile_import_run_progress"].operation_count
        == 3
    )
    assert operations["evidence_progress_start"].operation_count == 1
    assert operations["profile_progress_start"].operation_count == 1
    assert operations["profile_checkpoint_failure_reserve"].operation_count == 1


def test_control_wal_projection_binds_fixed_catalog_statement_envelopes():
    _, projection = _bound_control_wal_projection()
    operations = _control_operations_by_name(projection)
    layout = operations["artifact_scope_layout"]
    stage_reinitialize = operations["profile_stage_reinitialize"]
    stage_initialize = operations["profile_stage_initialize"]

    assert layout.operation_count == (
        capacity.CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT
    )
    assert layout.fixed_statement_count == (
        capacity.CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT
    )
    assert layout.fixed_statement_wal_bytes == (
        capacity.CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT
        * capacity.CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT
    )
    assert stage_reinitialize.operation_count == 1
    assert stage_reinitialize.metadata_mutation_count == 1
    assert stage_reinitialize.fixed_statement_count == (
        capacity.CONTROL_WAL_PROFILE_STAGE_REINITIALIZE_DROP_STATEMENT_COUNT
    )
    assert stage_reinitialize.commit_count == 0
    assert stage_initialize.operation_count == 1
    assert stage_initialize.metadata_mutation_count == 1
    assert stage_initialize.fixed_statement_count == (
        capacity.CONTROL_WAL_PROFILE_STAGE_LAYOUT_STATEMENT_COUNT
    )
    assert operations["artifact_scope_analyze"].operation_count == 9
    assert operations["admission_row_lock"].operation_count == 2
    assert operations["capacity_consumption_insert"].operation_count == 3
    assert operations["cutover_row_lock"].operation_count == 12
    assert operations["admission_row_lock"].wal_bytes_per_operation == (
        capacity.CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE
    )
    assert operations["profile_stage_drop"].operation_count == 3
    assert operations["artifact_scope_drop"].operation_count == 9


def test_control_wal_projection_reserves_each_payload_commit_envelope():
    geometry, projection = _bound_control_wal_projection()
    operations = _control_operations_by_name(projection)

    for operation_name, expected_count in (
        ("artifact_scope_payload", 3),
        ("evidence_payload", 4),
        ("affected_npi_payload", 3),
        ("profile_payload", 3),
    ):
        operation = operations[operation_name]
        assert operation.metadata_wal_bytes == 0
        assert operation.metadata_data_bytes == 0
        assert operation.fixed_statement_wal_bytes == 0
        assert operation.commit_count == expected_count
        assert operation.commit_envelope_bytes == (
            expected_count * geometry.postgres_block_size_bytes
        )


def test_control_projection_reserves_repeated_metadata_data_growth():
    geometry, projection = _bound_control_wal_projection()
    operations = _control_operations_by_name(projection)

    for operation_name in (
        "capacity_consumption_insert",
        "profile_stage_reinitialize",
        "profile_stage_initialize",
        "evidence_progress_start",
        "evidence_checkpoint_advance",
        "evidence_import_run_progress",
        "evidence_checkpoint_complete",
        "profile_progress_start",
        "profile_checkpoint_advance",
        "profile_import_run_progress",
        "profile_checkpoint_ready",
        "profile_checkpoint_retire",
        "profile_checkpoint_failure_reserve",
    ):
        operation = operations[operation_name]
        assert operation.metadata_data_bytes_per_operation > 0
        assert operation.metadata_data_bytes == (
            operation.operation_count
            * operation.metadata_data_bytes_per_operation
        )
    assert projection.total_control_metadata_data_bytes == sum(
        operation.metadata_data_bytes for operation in projection.operations
    )
    assert geometry.control_metadata_data_upper_bound_bytes == (
        projection.total_control_metadata_data_bytes
    )
    assert (
        geometry.reservation_bytes_by_storage_class["data"]
        >= projection.total_control_metadata_data_bytes
    )


def test_control_wal_payload_and_hash_bind_order_geometry_and_counts():
    plan_input = _control_wal_plan_input()
    geometry, projection = _bound_control_wal_projection(plan_input)
    projection_by_field = capacity.profile_control_wal_projection_payload(
        projection
    )

    assert projection_by_field["final_cutover_contract_id"] == (
        capacity.CUTOVER_FORECAST_CONTRACT_ID
    )
    assert projection_by_field["plan_input"]["artifact_batch_counts"][0] == {
        "artifact_name": "source",
        "batch_count": 2,
    }
    assert projection_by_field["plan_input"]["build_checkpoint_insert"][
        "main_index_pages"
    ] == [1]
    assert projection_by_field["total_control_metadata_data_bytes"] == (
        projection.total_control_metadata_data_bytes
    )
    assert " " not in (
        capacity.canonical_profile_control_wal_projection_json(
            projection
        )
    )
    assert len(capacity.profile_control_wal_projection_hash(projection)) == 64
    assert (
        capacity.revalidate_profile_control_wal_projection(
            geometry,
            projection,
        )
        == projection
    )
    changed_plan_input = _control_wal_plan_input(evidence_batch_count=5)
    _, changed = _bound_control_wal_projection(
        changed_plan_input,
    )
    assert capacity.profile_control_wal_projection_hash(changed) != (
        capacity.profile_control_wal_projection_hash(projection)
    )


def test_control_wal_plan_input_hash_is_canonical_and_geometry_independent():
    plan_input = _control_wal_plan_input()
    payload = capacity.profile_control_wal_plan_input_payload(plan_input)
    canonical_json = (
        capacity.canonical_profile_control_wal_plan_input_json(plan_input)
    )

    assert payload["artifact_batch_counts"][0] == {
        "artifact_name": "source",
        "batch_count": 2,
    }
    assert "capacity_geometry_hash" not in canonical_json
    assert "control_wal_upper_bound_bytes" not in canonical_json
    assert "control_metadata_data_upper_bound_bytes" not in canonical_json
    assert " " not in canonical_json
    assert len(capacity.profile_control_wal_plan_input_hash(plan_input)) == 64
    assert capacity.profile_control_wal_plan_input_hash(
        dataclasses.replace(plan_input, evidence_batch_count=5)
    ) != capacity.profile_control_wal_plan_input_hash(plan_input)


def test_control_wal_projection_requires_signed_plan_input_hash():
    plan_input = _control_wal_plan_input()
    geometry, _ = _bound_control_wal_projection(plan_input)
    changed_plan_input = dataclasses.replace(
        plan_input,
        compact_batch_count=plan_input.compact_batch_count + 1,
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="control_wal_plan_input_hash_mismatch",
    ):
        capacity.project_profile_control_wal_capacity(
            geometry,
            changed_plan_input,
        )


def test_control_wal_revalidation_requires_exact_geometry_upper_bound():
    geometry, projection = _bound_control_wal_projection()
    changed_geometry = capacity.revalidate_capacity_geometry(
        dataclasses.replace(
            geometry,
            control_wal_upper_bound_bytes=(
                geometry.control_wal_upper_bound_bytes + 1
            ),
        )
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="control_wal_projection_geometry_bound_mismatch",
    ):
        capacity.revalidate_profile_control_wal_projection(
            changed_geometry,
            projection,
        )
