# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bind observed PostgreSQL identity to deployment capacity limits."""

from __future__ import annotations

import dataclasses

from process import provider_directory_profile_capacity as capacity
from process.provider_directory_profile_capacity_runtime_config import _configuration_error
from process.provider_directory_profile_capacity_runtime_types import (
    ProviderDirectoryProfileCapacityGeometryInputs,
    ProviderDirectoryProfileCapacityLimits,
)

def build_capacity_geometry(
    limits: ProviderDirectoryProfileCapacityLimits,
    inputs: ProviderDirectoryProfileCapacityGeometryInputs,
) -> capacity.ProviderDirectoryProfileCapacityGeometry:
    """Build and revalidate the exact geometry to be matched by a lease."""

    if not isinstance(limits, ProviderDirectoryProfileCapacityLimits):
        raise _configuration_error("limits_type_invalid")
    if not isinstance(
        inputs,
        ProviderDirectoryProfileCapacityGeometryInputs,
    ):
        raise _configuration_error("inputs_type_invalid")
    if not 0 <= inputs.artifact_scope_projected_rows <= (
        limits.max_artifact_scope_rows
    ):
        raise _configuration_error("artifact_scope_rows_exceeded")
    geometry_by_field = dataclasses.asdict(inputs)
    projected_rows = geometry_by_field.pop("artifact_scope_projected_rows")
    geometry_by_field.update(
        {
            "contract_id": capacity.CAPACITY_GEOMETRY_CONTRACT_ID,
            "materialization_mode": capacity.PROFILE_MATERIALIZATION_MODE,
            "postgres_toast_max_chunk_size_bytes": (
                capacity.POSTGRES_TOAST_MAX_CHUNK_SIZE_BYTES
            ),
            "postgres_maxalign_bytes": capacity.POSTGRES_MAXALIGN_BYTES,
            "postgres_btree_version": capacity.POSTGRES_BTREE_VERSION,
            "physical_projection_contract_id": (
                capacity.PHYSICAL_PROJECTION_CONTRACT_ID
            ),
            "metadata_data_upper_bound_bytes": (
                capacity.METADATA_DATA_UPPER_BOUND_BYTES
            ),
            "metadata_wal_upper_bound_bytes": (
                capacity.METADATA_WAL_UPPER_BOUND_BYTES
            ),
            "artifact_scope_batch_size": limits.artifact_scope_batch_size,
            "pool_reserve_connections": limits.pool_reserve_connections,
            "max_parallel_workers_per_gather": 0,
            "max_parallel_maintenance_workers": 0,
            "work_mem_bytes": limits.work_mem_bytes,
            "maintenance_work_mem_bytes": limits.maintenance_work_mem_bytes,
            "temp_file_limit_bytes": limits.temp_file_limit_bytes,
            "max_build_seconds": limits.max_build_seconds,
            "statement_timeout_ms": limits.statement_timeout_ms,
            "lock_timeout_ms": limits.lock_timeout_ms,
            "minimum_remaining_bytes": limits.minimum_remaining_bytes,
            "max_artifact_scope_rows": projected_rows,
            "max_evidence_rows": limits.max_evidence_rows,
            "max_affected_npis": limits.max_affected_npis,
            "max_profile_rows": limits.max_profile_rows,
            "relation_byte_caps": [
                dataclasses.asdict(relation_cap)
                for relation_cap in limits.relation_byte_caps
            ],
        }
    )
    return capacity.validated_capacity_geometry(geometry_by_field)
