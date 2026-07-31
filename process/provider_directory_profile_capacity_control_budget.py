# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Build bounded control-WAL operation budgets."""

from __future__ import annotations

import dataclasses
from typing import Any

from process.provider_directory_profile_capacity_geometry import _error
from process.provider_directory_profile_capacity_physical import (
    _metadata_mutation_projection,
)
from process.provider_directory_profile_capacity_target import (
    _btree_insert_growth_pages,
    _checked_add,
)
from process.provider_directory_profile_capacity_types import (
    ARTIFACT_SCOPE_RECOVERY_CONTRACT_ID,
    CONTROL_WAL_ARTIFACT_SCOPE_NAMES,
    CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT,
    CONTROL_WAL_CHECKPOINT_INITIAL_INSERT_COUNT,
    CONTROL_WAL_CHECKPOINT_PHASE_STATE_UPDATE_COUNT,
    CONTROL_WAL_CHECKPOINT_REINITIALIZE_DELETE_COUNT,
    CONTROL_WAL_CHECKPOINT_RETIRE_UPDATE_COUNT,
    CONTROL_WAL_FAILURE_CHECKPOINT_UPDATE_COUNT,
    CONTROL_WAL_FINAL_CUTOVER_CHECKPOINT_UPDATE_COUNT,
    CONTROL_WAL_IMPORT_RUN_PHASE_START_UPDATE_COUNT,
    CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE,
    METADATA_PAYLOAD_UPPER_BOUND_BYTES,
    POSTGRES_BLOCK_SIZE_BYTES,
    ProfileControlArtifactBatchCount,
    ProfileControlWalPlanInput,
    ProviderDirectoryProfileCapacityGeometry,
    ProviderDirectoryProfileControlWalOperation,
    ProviderDirectoryProfileMetadataMutationInput,
    _MAX_SIGNED_BIGINT,
)

def _control_wal_nonnegative_integer(value: Any, field_name: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= _MAX_SIGNED_BIGINT
    ):
        raise _error("control_wal_projection_input_invalid:" + field_name)
    return value


def _control_wal_product(*values: int) -> int:
    result = 1
    for value in values:
        if value < 0:
            raise _error("control_wal_projection_overflow")
        result *= value
        if result > _MAX_SIGNED_BIGINT:
            raise _error("control_wal_projection_overflow")
    return result


def _validate_control_wal_metadata_input(
    mutation: ProviderDirectoryProfileMetadataMutationInput,
    *,
    relation_name: str,
    operation: str,
) -> None:
    if (
        not isinstance(
            mutation,
            ProviderDirectoryProfileMetadataMutationInput,
        )
        or mutation.relation_name != relation_name
        or mutation.operation != operation
        or not isinstance(mutation.payload_upper_bytes, int)
        or isinstance(mutation.payload_upper_bytes, bool)
        or not 0
        <= mutation.payload_upper_bytes
        <= METADATA_PAYLOAD_UPPER_BOUND_BYTES
        or not isinstance(mutation.deleted_toast_chunks, int)
        or isinstance(mutation.deleted_toast_chunks, bool)
        or mutation.deleted_toast_chunks < 0
        or not isinstance(mutation.main_index_pages, tuple)
        or not mutation.main_index_pages
        or not isinstance(mutation.toast_index_pages, tuple)
        or any(
            not isinstance(page_count, int)
            or isinstance(page_count, bool)
            or page_count < 1
            for page_count in (
                mutation.main_index_pages
                + mutation.toast_index_pages
            )
        )
    ):
        raise _error(
            "control_wal_projection_metadata_input_invalid:"
            + relation_name
        )


def _validate_control_artifact_batches(
    artifact_batches: Any,
) -> None:
    if (
        not isinstance(artifact_batches, tuple)
        or tuple(
            artifact_batch.artifact_name
            for artifact_batch in artifact_batches
            if isinstance(
                artifact_batch,
                ProfileControlArtifactBatchCount,
            )
        )
        != CONTROL_WAL_ARTIFACT_SCOPE_NAMES
        or len(artifact_batches)
        != CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT
    ):
        raise _error(
            "control_wal_projection_input_invalid:artifact_batch_order"
        )
    for artifact_batch in artifact_batches:
        if not isinstance(
            artifact_batch,
            ProfileControlArtifactBatchCount,
        ):
            raise _error(
                "control_wal_projection_input_invalid:artifact_batch"
            )
        _control_wal_nonnegative_integer(
            artifact_batch.batch_count,
            "artifact_batch_count",
        )


def _validate_control_wal_plan_input(
    plan_input: ProfileControlWalPlanInput,
) -> None:
    if not isinstance(
        plan_input,
        ProfileControlWalPlanInput,
    ):
        raise _error("control_wal_projection_input_invalid:plan")
    _validate_control_artifact_batches(
        plan_input.artifact_batch_counts
    )
    if (
        plan_input.artifact_scope_recovery_contract_id
        != ARTIFACT_SCOPE_RECOVERY_CONTRACT_ID
    ):
        raise _error(
            "control_wal_projection_input_invalid:"
            "artifact_scope_recovery_contract"
        )
    for field_name in (
        "evidence_batch_count",
        "compact_batch_count",
        "affected_source_count",
        "admission_row_lock_count",
        "cutover_row_lock_count",
    ):
        _control_wal_nonnegative_integer(
            getattr(plan_input, field_name),
            field_name,
        )
    _validate_control_wal_metadata_input(
        plan_input.build_checkpoint_insert,
        relation_name="build_checkpoint",
        operation="insert",
    )
    _validate_control_wal_metadata_input(
        plan_input.build_checkpoint_update,
        relation_name="build_checkpoint",
        operation="update",
    )
    _validate_control_wal_metadata_input(
        plan_input.import_run_update,
        relation_name="import_run",
        operation="update",
    )
    _validate_control_wal_metadata_input(
        plan_input.capacity_consumption_insert,
        relation_name="capacity_consumption",
        operation="insert",
    )


def _final_control_index_page_bounds(
    existing_pages: tuple[int, ...],
    inserted_entries: int,
) -> tuple[int, ...]:
    return tuple(
        _checked_add(
            page_count,
            _btree_insert_growth_pages(
                (page_count,),
                inserted_entries,
            ),
        )
        for page_count in existing_pages
    )


def _control_metadata_projection_per_operation(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    mutation: ProviderDirectoryProfileMetadataMutationInput,
    *,
    sequence_operation_count: int,
) -> tuple[int, int]:
    """Use final projected B-tree depth for repeated data and WAL bounds."""

    if sequence_operation_count < 1:
        return 0, 0
    inserted_toast_chunks = (
        mutation.payload_upper_bytes
        + geometry.postgres_toast_max_chunk_size_bytes
        - 1
    ) // geometry.postgres_toast_max_chunk_size_bytes
    conservative_mutation = dataclasses.replace(
        mutation,
        main_index_pages=_final_control_index_page_bounds(
            mutation.main_index_pages,
            sequence_operation_count,
        ),
        toast_index_pages=_final_control_index_page_bounds(
            mutation.toast_index_pages,
            _control_wal_product(
                inserted_toast_chunks,
                sequence_operation_count,
            ),
        ),
    )
    return _metadata_mutation_projection(
        geometry,
        conservative_mutation,
    )


def _empty_control_wal_operation(
    phase: str,
    operation_name: str,
) -> ProviderDirectoryProfileControlWalOperation:
    return ProviderDirectoryProfileControlWalOperation(
        phase=phase,
        operation_name=operation_name,
        operation_count=0,
        metadata_mutation_count=0,
        fixed_statement_count=0,
        commit_count=0,
        metadata_data_bytes=0,
        metadata_wal_bytes=0,
        fixed_statement_wal_bytes=0,
        commit_envelope_bytes=0,
        metadata_data_bytes_per_operation=0,
        wal_bytes_per_operation=0,
        wal_bytes=0,
    )


def _control_operation_totals(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    operation_count: int,
    metadata_data_per_operation: int,
    metadata_wal_per_operation: int,
    fixed_statements_per_operation: int,
    fixed_wal_per_operation: int,
    commits_per_operation: int,
) -> tuple[int, int, int, int, int, int, int, int, int, int]:
    metadata_mutation_count = (
        operation_count
        if metadata_data_per_operation or metadata_wal_per_operation
        else 0
    )
    fixed_statement_count = _control_wal_product(
        operation_count,
        fixed_statements_per_operation,
    )
    commit_count = _control_wal_product(
        operation_count,
        commits_per_operation,
    )
    metadata_wal_bytes = _control_wal_product(
        operation_count,
        metadata_wal_per_operation,
    )
    metadata_data_bytes = _control_wal_product(
        operation_count,
        metadata_data_per_operation,
    )
    fixed_statement_wal_bytes = _control_wal_product(
        operation_count,
        fixed_wal_per_operation,
    )
    commit_envelope_bytes = _control_wal_product(
        commit_count,
        geometry.postgres_block_size_bytes,
    )
    wal_bytes_per_operation = _checked_add(
        metadata_wal_per_operation,
        fixed_wal_per_operation,
        commits_per_operation * geometry.postgres_block_size_bytes,
    )
    wal_bytes = _control_wal_product(
        operation_count,
        wal_bytes_per_operation,
    )
    return (
        metadata_mutation_count,
        fixed_statement_count,
        commit_count,
        metadata_data_bytes,
        metadata_wal_bytes,
        fixed_statement_wal_bytes,
        commit_envelope_bytes,
        metadata_data_per_operation,
        wal_bytes_per_operation,
        wal_bytes,
    )


def _control_wal_operation(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    operation_identity: tuple[str, str],
    *,
    operation_count: int,
    metadata_data_bytes_per_operation: int = 0,
    metadata_wal_bytes_per_operation: int = 0,
    fixed_statements_per_operation: int = 0,
    fixed_wal_bytes_per_operation: int = 0,
    commits_per_operation: int = 1,
) -> ProviderDirectoryProfileControlWalOperation:
    phase, operation_name = operation_identity
    operation_count = _control_wal_nonnegative_integer(
        operation_count,
        operation_name,
    )
    for field_name, input_upper_bound in (
        (
            "metadata_data_bytes_per_operation",
            metadata_data_bytes_per_operation,
        ),
        ("metadata_wal_bytes_per_operation", metadata_wal_bytes_per_operation),
        ("fixed_statements_per_operation", fixed_statements_per_operation),
        ("fixed_wal_bytes_per_operation", fixed_wal_bytes_per_operation),
        ("commits_per_operation", commits_per_operation),
    ):
        _control_wal_nonnegative_integer(input_upper_bound, field_name)
    if operation_count == 0:
        return _empty_control_wal_operation(phase, operation_name)
    operation_totals = _control_operation_totals(
        geometry,
        operation_count,
        metadata_data_bytes_per_operation,
        metadata_wal_bytes_per_operation,
        fixed_statements_per_operation,
        fixed_wal_bytes_per_operation,
        commits_per_operation,
    )
    return ProviderDirectoryProfileControlWalOperation(
        phase=phase,
        operation_name=operation_name,
        operation_count=operation_count,
        metadata_mutation_count=operation_totals[0],
        fixed_statement_count=operation_totals[1],
        commit_count=operation_totals[2],
        metadata_data_bytes=operation_totals[3],
        metadata_wal_bytes=operation_totals[4],
        fixed_statement_wal_bytes=operation_totals[5],
        commit_envelope_bytes=operation_totals[6],
        metadata_data_bytes_per_operation=operation_totals[7],
        wal_bytes_per_operation=operation_totals[8],
        wal_bytes=operation_totals[9],
    )


def _metadata_control_operation(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    phase: str,
    operation_name: str,
    operation_count: int,
    metadata_data_bytes: int,
    metadata_wal_bytes: int,
    *,
    commits_per_operation: int = 1,
) -> ProviderDirectoryProfileControlWalOperation:
    return _control_wal_operation(
        geometry,
        (phase, operation_name),
        operation_count=operation_count,
        metadata_data_bytes_per_operation=metadata_data_bytes,
        metadata_wal_bytes_per_operation=metadata_wal_bytes,
        commits_per_operation=commits_per_operation,
    )


def _fixed_control_operation(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    phase: str,
    operation_name: str,
    operation_count: int,
    fixed_wal_bytes: int,
) -> ProviderDirectoryProfileControlWalOperation:
    return _control_wal_operation(
        geometry,
        (phase, operation_name),
        operation_count=operation_count,
        fixed_statements_per_operation=1,
        fixed_wal_bytes_per_operation=fixed_wal_bytes,
    )


def _commit_control_operation(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    operation_name: str,
    operation_count: int,
) -> ProviderDirectoryProfileControlWalOperation:
    return _control_wal_operation(
        geometry,
        ("pre_cutover", operation_name),
        operation_count=operation_count,
    )


def _row_lock_control_operation(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    operation_name: str,
    operation_count: int,
) -> ProviderDirectoryProfileControlWalOperation:
    return _control_wal_operation(
        geometry,
        ("pre_cutover", operation_name),
        operation_count=operation_count,
        fixed_statements_per_operation=1,
        fixed_wal_bytes_per_operation=(
            CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE
        ),
        commits_per_operation=0,
    )


def _control_metadata_mutation_bounds(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    plan_input: ProfileControlWalPlanInput,
) -> tuple[
    tuple[int, int],
    tuple[int, int],
    tuple[int, int],
    tuple[int, int],
]:
    checkpoint_sequence_count = _checked_add(
        CONTROL_WAL_CHECKPOINT_INITIAL_INSERT_COUNT,
        plan_input.evidence_batch_count,
        plan_input.compact_batch_count,
        CONTROL_WAL_CHECKPOINT_PHASE_STATE_UPDATE_COUNT,
        CONTROL_WAL_CHECKPOINT_REINITIALIZE_DELETE_COUNT,
        CONTROL_WAL_FINAL_CUTOVER_CHECKPOINT_UPDATE_COUNT,
        CONTROL_WAL_FAILURE_CHECKPOINT_UPDATE_COUNT,
        CONTROL_WAL_CHECKPOINT_RETIRE_UPDATE_COUNT,
    )
    import_run_sequence_count = _checked_add(
        plan_input.evidence_batch_count,
        plan_input.compact_batch_count,
        CONTROL_WAL_IMPORT_RUN_PHASE_START_UPDATE_COUNT,
    )
    checkpoint_insert = _control_metadata_projection_per_operation(
        geometry,
        plan_input.build_checkpoint_insert,
        sequence_operation_count=checkpoint_sequence_count,
    )
    checkpoint_update = _control_metadata_projection_per_operation(
        geometry,
        plan_input.build_checkpoint_update,
        sequence_operation_count=checkpoint_sequence_count,
    )
    import_run_update = _control_metadata_projection_per_operation(
        geometry,
        plan_input.import_run_update,
        sequence_operation_count=import_run_sequence_count,
    )
    capacity_consumption_insert = (
        _control_metadata_projection_per_operation(
            geometry,
            plan_input.capacity_consumption_insert,
            sequence_operation_count=1,
        )
    )
    return (
        checkpoint_insert,
        checkpoint_update,
        import_run_update,
        capacity_consumption_insert,
    )
