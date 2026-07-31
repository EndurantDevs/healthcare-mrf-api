# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate and identify Provider Directory Profile capacity geometry."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from typing import Any, Mapping

from process.provider_directory_profile_capacity_geometry_contract import (
    _assert_relation_cap_shape,
    _bounded_integer,
    _database_system_identifier,
    _error,
    _exact_fields,
    _exact_hash,
    _exact_text,
    _nonnegative_bigint,
    _positive_bigint,
    _profile_as_of,
    _validated_relation_cap_sequence,
    _validated_single_relation_cap,
)
from process.provider_directory_profile_capacity_types import (
    CAPACITY_GEOMETRY_CONTRACT_ID,
    METADATA_DATA_UPPER_BOUND_BYTES,
    METADATA_WAL_UPPER_BOUND_BYTES,
    PHYSICAL_PROJECTION_CONTRACT_ID,
    POSTGRES_BLOCK_SIZE_BYTES,
    POSTGRES_BTREE_VERSION,
    POSTGRES_MAXALIGN_BYTES,
    POSTGRES_SUPPORTED_MAJOR,
    POSTGRES_TOAST_MAX_CHUNK_SIZE_BYTES,
    PROFILE_CONTROL_CONNECTION_RESERVE,
    PROFILE_DEDICATED_ADVISORY_LOCK_CONNECTIONS,
    PROFILE_MATERIALIZATION_MODE,
    PROFILE_MINIMUM_POOL_RESERVE,
    PROFILE_STRATEGY_VERSION,
    ProviderDirectoryProfileCapacityGeometry,
    _GEOMETRY_FIELDS,
    _GEOMETRY_HASH_DOMAIN,
    _HASH_FIELDS,
    _MAX_OID,
    _MAX_POOL_SIZE,
    _MAX_SIGNED_BIGINT,
    _MAX_WORKERS,
    _POSITIVE_ROW_CAP_FIELDS,
)

def _assert_wave_geometry(
    geometry: ProviderDirectoryProfileCapacityGeometry,
) -> None:
    wave_pairs = (
        (geometry.artifact_scope_wave_count, geometry.artifact_scope_worker_count),
        (geometry.evidence_wave_count, geometry.evidence_worker_count),
        (geometry.compact_wave_count, geometry.compact_worker_count),
    )
    if any((waves == 0) != (workers == 0) for waves, workers in wave_pairs):
        raise _error("wave_worker_geometry_invalid")
    if (
        geometry.pool_reserve_connections
        < PROFILE_MINIMUM_POOL_RESERVE
    ):
        raise _error("profile_control_pool_reserve_invalid")
    available_connections = (
        geometry.database_pool_size
        - geometry.pool_reserve_connections
    )
    if available_connections < geometry.maximum_worker_count:
        raise _error("worker_pool_reserve_invalid")
    if (
        PROFILE_DEDICATED_ADVISORY_LOCK_CONNECTIONS
        + PROFILE_CONTROL_CONNECTION_RESERVE
        + geometry.maximum_worker_count
        > geometry.database_pool_size
    ):
        raise _error("worker_pool_physical_capacity_invalid")


def _assert_execution_limits(
    geometry: ProviderDirectoryProfileCapacityGeometry,
) -> None:
    """Apply session, PostgreSQL ABI, WAL, and metadata limit checks."""

    _assert_session_execution_limits(geometry)
    _assert_postgres_storage_limits(geometry)
    _assert_postgres_wal_limits(geometry)
    _assert_metadata_capacity_limits(geometry)


def _assert_session_execution_limits(
    geometry: ProviderDirectoryProfileCapacityGeometry,
) -> None:
    if geometry.max_parallel_workers_per_gather != 0:
        raise _error("query_parallelism_not_zero")
    if geometry.max_parallel_maintenance_workers != 0:
        raise _error("maintenance_parallelism_not_zero")
    if (
        geometry.work_mem_bytes % 1024 != 0
        or geometry.maintenance_work_mem_bytes % 1024 != 0
        or geometry.temp_file_limit_bytes % 1024 != 0
    ):
        raise _error("memory_setting_not_kib_aligned")
    if any(
        cap.max_temp_bytes != geometry.temp_file_limit_bytes
        for cap in geometry.relation_byte_caps
    ):
        raise _error("relation_temp_cap_not_session_limit")
    if geometry.lock_timeout_ms >= geometry.statement_timeout_ms:
        raise _error("lock_timeout_not_below_statement_timeout")
    if geometry.statement_timeout_ms >= geometry.max_build_seconds * 1000:
        raise _error("statement_timeout_not_below_build_timeout")


def _assert_postgres_storage_limits(
    geometry: ProviderDirectoryProfileCapacityGeometry,
) -> None:
    if (
        geometry.postgres_server_version_num // 10_000
        != POSTGRES_SUPPORTED_MAJOR
        or geometry.postgres_block_size_bytes
        != POSTGRES_BLOCK_SIZE_BYTES
        or geometry.postgres_wal_block_size_bytes
        != POSTGRES_BLOCK_SIZE_BYTES
        or geometry.postgres_toast_max_chunk_size_bytes
        != POSTGRES_TOAST_MAX_CHUNK_SIZE_BYTES
        or geometry.postgres_maxalign_bytes != POSTGRES_MAXALIGN_BYTES
        or geometry.postgres_btree_version != POSTGRES_BTREE_VERSION
    ):
        raise _error("postgres_storage_abi_unsupported")
    if (
        geometry.postgres_wal_segment_size_bytes
        < geometry.postgres_wal_block_size_bytes
        or geometry.postgres_wal_segment_size_bytes
        % geometry.postgres_wal_block_size_bytes
        != 0
    ):
        raise _error("postgres_wal_segment_geometry_invalid")


def _assert_postgres_wal_limits(
    geometry: ProviderDirectoryProfileCapacityGeometry,
) -> None:
    if not geometry.postgres_full_page_writes:
        raise _error("postgres_full_page_writes_required")
    if geometry.postgres_wal_level != "replica":
        raise _error("postgres_wal_level_unsupported")
    if geometry.postgres_default_toast_compression not in {"pglz", "lz4"}:
        raise _error("postgres_toast_compression_unsupported")
    if geometry.postgres_wal_compression not in {
        "off",
        "pglz",
        "lz4",
        "zstd",
    }:
        raise _error("postgres_wal_compression_unsupported")


def _assert_metadata_capacity_limits(
    geometry: ProviderDirectoryProfileCapacityGeometry,
) -> None:
    if (
        geometry.metadata_wal_upper_bound_bytes
        != METADATA_WAL_UPPER_BOUND_BYTES
        or geometry.metadata_data_upper_bound_bytes
        != METADATA_DATA_UPPER_BOUND_BYTES
    ):
        raise _error("metadata_upper_bound_invalid")


def _validated_scalar_geometry(
    geometry_map: Mapping[str, Any],
) -> None:
    """Validate scalar identity, storage, row, and PostgreSQL coordinates."""

    _validate_scalar_identity(geometry_map)
    _validate_scalar_flags(geometry_map)
    _validate_scalar_oids(geometry_map)
    _validate_scalar_row_caps(geometry_map)
    _validate_scalar_postgres_bounds(geometry_map)


def _validate_scalar_identity(geometry_map: Mapping[str, Any]) -> None:
    for name in _HASH_FIELDS:
        _exact_hash(geometry_map, name)
    _database_system_identifier(geometry_map)
    _profile_as_of(geometry_map)
    for name in ("database_name", "tablespace_name"):
        _exact_text(geometry_map, name, maximum_length=63)
    for name in (
        "postgres_wal_compression",
        "postgres_wal_level",
        "postgres_default_toast_compression",
    ):
        _exact_text(geometry_map, name, maximum_length=32)
    if (
        geometry_map.get("physical_projection_contract_id")
        != PHYSICAL_PROJECTION_CONTRACT_ID
    ):
        raise _error("physical_projection_contract_invalid")


def _validate_scalar_flags(geometry_map: Mapping[str, Any]) -> None:
    for name in (
        "postgres_full_page_writes",
        "postgres_wal_log_hints",
        "postgres_data_checksums",
    ):
        if not isinstance(geometry_map.get(name), bool):
            raise _error(name + "_invalid")


def _validate_scalar_oids(geometry_map: Mapping[str, Any]) -> None:
    for name in (
        "database_oid",
        "tablespace_oid",
        "evidence_target_oid",
        "profile_target_oid",
        "build_checkpoint_oid",
        "serving_generation_oid",
        "delta_receipt_oid",
        "import_run_oid",
        "capacity_consumption_oid",
    ):
        _bounded_integer(geometry_map, name, minimum=1, maximum=_MAX_OID)


def _validate_scalar_row_caps(geometry_map: Mapping[str, Any]) -> None:
    _nonnegative_bigint(geometry_map, "max_artifact_scope_rows")
    _nonnegative_bigint(
        geometry_map,
        "artifact_scope_projected_logical_bytes",
    )
    for name in _POSITIVE_ROW_CAP_FIELDS:
        _positive_bigint(geometry_map, name)


def _validate_scalar_postgres_bounds(
    geometry_map: Mapping[str, Any],
) -> None:
    for name in (
        "postgres_server_version_num",
        "postgres_block_size_bytes",
        "postgres_toast_max_chunk_size_bytes",
        "postgres_maxalign_bytes",
        "postgres_btree_version",
        "postgres_wal_block_size_bytes",
        "postgres_wal_segment_size_bytes",
        "postgres_checkpoint_timeout_seconds",
        "postgres_max_wal_size_bytes",
        "metadata_wal_upper_bound_bytes",
        "metadata_data_upper_bound_bytes",
        "control_wal_upper_bound_bytes",
        "control_metadata_data_upper_bound_bytes",
    ):
        _positive_bigint(geometry_map, name)


def _validated_execution_geometry(
    geometry_map: Mapping[str, Any],
) -> None:
    _positive_bigint(geometry_map, "profile_schema_version")
    for name in (
        "artifact_scope_wave_count", "evidence_wave_count", "compact_wave_count"
    ):
        _nonnegative_bigint(geometry_map, name)
    for name in (
        "artifact_scope_worker_count", "evidence_worker_count", "compact_worker_count"
    ):
        _bounded_integer(geometry_map, name, minimum=0, maximum=_MAX_WORKERS)
    _bounded_integer(
        geometry_map, "database_pool_size",
        minimum=PROFILE_MINIMUM_POOL_RESERVE + 1,
        maximum=_MAX_POOL_SIZE,
    )
    _bounded_integer(
        geometry_map, "pool_reserve_connections",
        minimum=PROFILE_MINIMUM_POOL_RESERVE,
        maximum=_MAX_POOL_SIZE - 1,
    )
    for name in (
        "max_parallel_workers_per_gather",
        "max_parallel_maintenance_workers",
    ):
        _bounded_integer(geometry_map, name, minimum=0, maximum=_MAX_WORKERS)
    for name in (
        "artifact_scope_batch_size",
        "work_mem_bytes",
        "maintenance_work_mem_bytes",
        "temp_file_limit_bytes",
        "max_build_seconds",
        "statement_timeout_ms",
        "lock_timeout_ms",
        "minimum_remaining_bytes",
    ):
        _positive_bigint(geometry_map, name)


def validated_capacity_geometry(
    geometry_map: Mapping[str, Any],
) -> ProviderDirectoryProfileCapacityGeometry:
    """Validate exact executable geometry and return its immutable form."""
    _exact_fields(geometry_map, _GEOMETRY_FIELDS, name="geometry")
    if geometry_map.get("contract_id") != CAPACITY_GEOMETRY_CONTRACT_ID:
        raise _error("geometry_contract_invalid")
    if geometry_map.get("materialization_mode") != PROFILE_MATERIALIZATION_MODE:
        raise _error("materialization_mode_invalid")
    if geometry_map.get("profile_strategy_version") != PROFILE_STRATEGY_VERSION:
        raise _error("profile_strategy_version_invalid")
    _validated_scalar_geometry(geometry_map)
    _validated_execution_geometry(geometry_map)
    relation_caps = _validated_relation_cap_sequence(
        geometry_map.get("relation_byte_caps")
    )
    scalar_map = dict(geometry_map)
    scalar_map["relation_byte_caps"] = relation_caps
    geometry = ProviderDirectoryProfileCapacityGeometry(**scalar_map)
    if len(
        {
            geometry.evidence_target_oid,
            geometry.profile_target_oid,
            geometry.build_checkpoint_oid,
            geometry.serving_generation_oid,
            geometry.delta_receipt_oid,
            geometry.import_run_oid,
            geometry.capacity_consumption_oid,
        }
    ) != 7:
        raise _error("target_oid_collision")
    _assert_wave_geometry(geometry)
    _assert_execution_limits(geometry)
    reservation_bytes = geometry.reservation_bytes_by_storage_class.values()
    if any(
        reserved_bytes > _MAX_SIGNED_BIGINT
        for reserved_bytes in reservation_bytes
    ):
        raise _error("storage_class_reservation_overflow")
    return geometry


def capacity_geometry_payload(
    geometry: ProviderDirectoryProfileCapacityGeometry,
) -> dict[str, Any]:
    """Return the exact JSON-compatible executable plan payload."""
    if not isinstance(geometry, ProviderDirectoryProfileCapacityGeometry):
        raise _error("geometry_type_invalid")
    payload = {
        field.name: getattr(geometry, field.name)
        for field in dataclasses.fields(geometry)
    }
    payload["relation_byte_caps"] = [
        dataclasses.asdict(relation) for relation in geometry.relation_byte_caps
    ]
    return payload


def canonical_capacity_geometry_json(
    geometry: ProviderDirectoryProfileCapacityGeometry,
) -> str:
    """Return canonical executable-plan JSON for durable checkpoints."""
    validated_geometry = revalidate_capacity_geometry(geometry)
    return json.dumps(
        capacity_geometry_payload(validated_geometry),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def capacity_geometry_hash(
    geometry: ProviderDirectoryProfileCapacityGeometry,
) -> str:
    """Return the deterministic executable-plan identity."""
    canonical_geometry = canonical_capacity_geometry_json(geometry)
    hash_input = f"{_GEOMETRY_HASH_DOMAIN}:{canonical_geometry}"
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()


def revalidate_capacity_geometry(
    geometry: ProviderDirectoryProfileCapacityGeometry,
) -> ProviderDirectoryProfileCapacityGeometry:
    """Revalidate a retained plan before each executable wave."""
    return validated_capacity_geometry(capacity_geometry_payload(geometry))
