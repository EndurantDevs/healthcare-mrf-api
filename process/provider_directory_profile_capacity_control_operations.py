# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Assemble the ordered Provider Directory Profile control-WAL ledger."""

from __future__ import annotations

from process.provider_directory_profile_capacity_control_budget import (
    _commit_control_operation,
    _control_wal_operation,
    _fixed_control_operation,
    _metadata_control_operation,
    _row_lock_control_operation,
)
from process.provider_directory_profile_capacity_target import _checked_add
from process.provider_directory_profile_capacity_types import (
    CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT,
    CONTROL_WAL_AFFECTED_NPI_DELTA_STATEMENT_COUNT,
    CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT,
    CONTROL_WAL_ARTIFACT_SCOPE_NAMES,
    CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT,
    CONTROL_WAL_CHECKPOINT_RETIRE_UPDATE_COUNT,
    CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT,
    CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT,
    CONTROL_WAL_FAILURE_CHECKPOINT_UPDATE_COUNT,
    CONTROL_WAL_PROFILE_STAGE_ANALYZE_STATEMENT_COUNT,
    CONTROL_WAL_PROFILE_STAGE_DROP_STATEMENT_COUNT,
    CONTROL_WAL_PROFILE_STAGE_LAYOUT_STATEMENT_COUNT,
    CONTROL_WAL_PROFILE_STAGE_REINITIALIZE_DROP_STATEMENT_COUNT,
    ProfileControlWalPlanInput,
    ProviderDirectoryProfileCapacityGeometry,
    ProviderDirectoryProfileControlWalOperation,
    _CONTROL_WAL_OPERATION_ORDER,
)

def _artifact_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    artifact_batch_count: int,
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    return (
        _fixed_control_operation(
            geometry,
            "pre_cutover",
            "artifact_scope_recovery_drop",
            CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT,
            CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _fixed_control_operation(
            geometry,
            "pre_cutover",
            "artifact_scope_layout",
            CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT,
            CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _commit_control_operation(
            geometry,
            "artifact_scope_payload",
            artifact_batch_count,
        ),
        _fixed_control_operation(
            geometry,
            "pre_cutover",
            "artifact_scope_analyze",
            CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT,
            CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
    )


def _stage_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    checkpoint_insert: tuple[int, int],
    checkpoint_update: tuple[int, int],
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    reinitialize = _control_wal_operation(
        geometry,
        ("pre_cutover", "profile_stage_reinitialize"),
        operation_count=1,
        metadata_data_bytes_per_operation=checkpoint_update[0],
        metadata_wal_bytes_per_operation=checkpoint_update[1],
        fixed_statements_per_operation=(
            CONTROL_WAL_PROFILE_STAGE_REINITIALIZE_DROP_STATEMENT_COUNT
        ),
        fixed_wal_bytes_per_operation=(
            CONTROL_WAL_PROFILE_STAGE_REINITIALIZE_DROP_STATEMENT_COUNT
            * CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT
        ),
        commits_per_operation=0,
    )
    initialize = _control_wal_operation(
        geometry,
        ("pre_cutover", "profile_stage_initialize"),
        operation_count=1,
        metadata_data_bytes_per_operation=checkpoint_insert[0],
        metadata_wal_bytes_per_operation=checkpoint_insert[1],
        fixed_statements_per_operation=(
            CONTROL_WAL_PROFILE_STAGE_LAYOUT_STATEMENT_COUNT
        ),
        fixed_wal_bytes_per_operation=(
            CONTROL_WAL_PROFILE_STAGE_LAYOUT_STATEMENT_COUNT
            * CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT
        ),
    )
    return reinitialize, initialize


def _evidence_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    evidence_batch_count: int,
    checkpoint_update: tuple[int, int],
    import_run_update: tuple[int, int],
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    return (
        _metadata_control_operation(
            geometry, "pre_cutover", "evidence_progress_start",
            1, *import_run_update,
        ),
        _commit_control_operation(
            geometry, "evidence_payload", evidence_batch_count,
        ),
        _metadata_control_operation(
            geometry, "pre_cutover", "evidence_checkpoint_advance",
            evidence_batch_count, *checkpoint_update,
        ),
        _metadata_control_operation(
            geometry, "pre_cutover", "evidence_import_run_progress",
            evidence_batch_count, *import_run_update,
        ),
        _fixed_control_operation(
            geometry, "pre_cutover", "evidence_stage_analyze", 1,
            CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _metadata_control_operation(
            geometry, "pre_cutover", "evidence_checkpoint_complete",
            1, *checkpoint_update,
        ),
    )


def _affected_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    affected_source_count: int,
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    affected_payload_count = _checked_add(
        affected_source_count,
        CONTROL_WAL_AFFECTED_NPI_DELTA_STATEMENT_COUNT,
    )
    return (
        _commit_control_operation(
            geometry,
            "affected_npi_payload",
            affected_payload_count,
        ),
        _fixed_control_operation(
            geometry, "pre_cutover", "affected_npi_analyze", 1,
            CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
    )


def _profile_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    compact_batch_count: int,
    checkpoint_update: tuple[int, int],
    import_run_update: tuple[int, int],
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    return (
        _metadata_control_operation(
            geometry, "pre_cutover", "profile_progress_start",
            1, *import_run_update,
        ),
        _commit_control_operation(
            geometry, "profile_payload", compact_batch_count,
        ),
        _metadata_control_operation(
            geometry, "pre_cutover", "profile_checkpoint_advance",
            compact_batch_count, *checkpoint_update,
        ),
        _metadata_control_operation(
            geometry, "pre_cutover", "profile_import_run_progress",
            compact_batch_count, *import_run_update,
        ),
        _fixed_control_operation(
            geometry, "pre_cutover", "profile_stage_analyze", 1,
            CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _metadata_control_operation(
            geometry, "pre_cutover", "profile_checkpoint_ready",
            1, *checkpoint_update,
        ),
    )


def _terminal_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    checkpoint_update: tuple[int, int],
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    return (
        _metadata_control_operation(
            geometry, "post_cutover", "profile_checkpoint_retire",
            CONTROL_WAL_CHECKPOINT_RETIRE_UPDATE_COUNT,
            *checkpoint_update,
        ),
        _fixed_control_operation(
            geometry, "post_cutover", "profile_stage_drop",
            CONTROL_WAL_PROFILE_STAGE_DROP_STATEMENT_COUNT,
            CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _fixed_control_operation(
            geometry, "post_cutover", "artifact_scope_drop",
            CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT,
            CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _metadata_control_operation(
            geometry, "failure_reserve",
            "profile_checkpoint_failure_reserve",
            CONTROL_WAL_FAILURE_CHECKPOINT_UPDATE_COUNT,
            *checkpoint_update,
        ),
    )


def _control_wal_phase_total(
    operations: tuple[ProviderDirectoryProfileControlWalOperation, ...],
    phase: str,
) -> int:
    return _checked_add(
        *(
            operation.wal_bytes
            for operation in operations
            if operation.phase == phase
        )
    )


def _control_wal_operation_ledger(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    plan_input: ProfileControlWalPlanInput,
    metadata_mutation_bounds: tuple[
        tuple[int, int],
        tuple[int, int],
        tuple[int, int],
        tuple[int, int],
    ],
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    """Return the closed, ordered WAL operation ledger for one build."""

    artifact_batch_count = _checked_add(
        *(entry.batch_count for entry in plan_input.artifact_batch_counts)
    )
    return (
        _row_lock_control_operation(
            geometry,
            "admission_row_lock",
            plan_input.admission_row_lock_count,
        ),
        _metadata_control_operation(
            geometry,
            "pre_cutover",
            "capacity_consumption_insert",
            3,
            *metadata_mutation_bounds[3],
        ),
        *_artifact_control_operations(geometry, artifact_batch_count),
        *_stage_control_operations(
            geometry,
            metadata_mutation_bounds[0],
            metadata_mutation_bounds[1],
        ),
        *_evidence_control_operations(
            geometry,
            plan_input.evidence_batch_count,
            metadata_mutation_bounds[1],
            metadata_mutation_bounds[2],
        ),
        *_affected_control_operations(
            geometry,
            plan_input.affected_source_count,
        ),
        *_profile_control_operations(
            geometry,
            plan_input.compact_batch_count,
            metadata_mutation_bounds[1],
            metadata_mutation_bounds[2],
        ),
        _row_lock_control_operation(
            geometry,
            "cutover_row_lock",
            plan_input.cutover_row_lock_count,
        ),
        *_terminal_control_operations(
            geometry,
            metadata_mutation_bounds[1],
        ),
    )
