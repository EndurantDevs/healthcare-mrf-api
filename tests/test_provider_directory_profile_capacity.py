# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Executable capacity geometry contracts for Provider Directory Profiles."""

from __future__ import annotations

import copy
import dataclasses

import pytest

from process import provider_directory_profile_capacity as capacity

def _relation_byte_caps():
    caps_by_name = {
        "artifact_scope": (100, 0, 0, 1024, 5),
        "evidence_stage": (200, 0, 0, 1024, 10),
        "affected_npi_stage": (300, 0, 0, 1024, 15),
        "profile_stage": (400, 0, 0, 1024, 20),
        "evidence_target": (0, 500, 50, 1024, 25),
        "profile_target": (0, 600, 60, 1024, 30),
    }
    return [
        {
            "relation_name": relation_name,
            "max_scratch_bytes": cap_values[0],
            "max_target_growth_bytes": cap_values[1],
            "max_deleted_logical_bytes": cap_values[2],
            "max_temp_bytes": cap_values[3],
            "max_wal_bytes": cap_values[4],
        }
        for relation_name, cap_values in caps_by_name.items()
    ]


def _postgres_geometry_by_field() -> dict[str, object]:
    """Return the PostgreSQL ABI identity used by capacity fixtures."""
    return {
        "database_system_identifier": "7527713908662902214",
        "database_oid": 16401,
        "database_name": "healthporta",
        "tablespace_oid": 1663,
        "tablespace_name": "pg_default",
        "evidence_target_oid": 199329911,
        "profile_target_oid": 199329928,
        "postgres_server_version_num": 180002,
        "postgres_block_size_bytes": 8192,
        "postgres_toast_max_chunk_size_bytes": 1996,
        "postgres_maxalign_bytes": 8,
        "postgres_btree_version": 4,
        "postgres_wal_block_size_bytes": 8192,
        "postgres_wal_segment_size_bytes": 16 * 1024 * 1024,
        "postgres_full_page_writes": True,
        "postgres_wal_compression": "off",
        "postgres_wal_level": "replica",
        "postgres_wal_log_hints": False,
        "postgres_data_checksums": False,
        "postgres_default_toast_compression": "pglz",
        "postgres_checkpoint_timeout_seconds": 300,
        "postgres_max_wal_size_bytes": 1024 * 1024 * 1024,
    }


def _relation_geometry_by_field() -> dict[str, object]:
    """Return physical relation and metadata projection identities."""
    return {
        "evidence_target_storage_fingerprint": "2" * 64,
        "profile_target_storage_fingerprint": "3" * 64,
        "build_checkpoint_oid": 199329935,
        "serving_generation_oid": 199329940,
        "delta_receipt_oid": 199329950,
        "import_run_oid": 199329960,
        "capacity_consumption_oid": 199329970,
        "build_checkpoint_storage_fingerprint": "6" * 64,
        "serving_generation_storage_fingerprint": "4" * 64,
        "delta_receipt_storage_fingerprint": "5" * 64,
        "import_run_storage_fingerprint": "8" * 64,
        "capacity_consumption_storage_fingerprint": "9" * 64,
        "control_wal_plan_input_hash": "a" * 64,
        "control_wal_upper_bound_bytes": 12_345,
        "control_metadata_data_upper_bound_bytes": 23_456,
        "physical_projection_contract_id": capacity.PHYSICAL_PROJECTION_CONTRACT_ID,
        "metadata_data_upper_bound_bytes": capacity.METADATA_DATA_UPPER_BOUND_BYTES,
        "metadata_wal_upper_bound_bytes": capacity.METADATA_WAL_UPPER_BOUND_BYTES,
    }


def _execution_geometry_by_field() -> dict[str, object]:
    """Return worker, memory, timeout, and relation-bound fixtures."""
    return {
        "evidence_wave_count": 32,
        "compact_wave_count": 32,
        "artifact_scope_wave_count": 16,
        "evidence_worker_count": 2,
        "compact_worker_count": 2,
        "artifact_scope_worker_count": 2,
        "artifact_scope_batch_size": 25_000,
        "artifact_scope_projection_hash": "7" * 64,
        "artifact_scope_projected_logical_bytes": 750_000_000,
        "database_pool_size": 16,
        "pool_reserve_connections": 4,
        "max_parallel_workers_per_gather": 0,
        "max_parallel_maintenance_workers": 0,
        "work_mem_bytes": 4 * 1024 * 1024,
        "maintenance_work_mem_bytes": 64 * 1024 * 1024,
        "temp_file_limit_bytes": 1024,
        "max_build_seconds": 6 * 60 * 60,
        "statement_timeout_ms": 15 * 60 * 1000,
        "lock_timeout_ms": 5_000,
        "minimum_remaining_bytes": 100 * 1024 * 1024 * 1024,
        "max_artifact_scope_rows": 25_000_000,
        "max_evidence_rows": 10_000_000,
        "max_affected_npis": 2_000_000,
        "max_profile_rows": 5_000_000,
        "relation_byte_caps": _relation_byte_caps(),
    }


def _geometry_payload(**overrides):
    """Return one complete exact capacity geometry mapping."""
    geometry_map = {
        "contract_id": capacity.CAPACITY_GEOMETRY_CONTRACT_ID,
        "selection_proof_id": "a" * 64,
        "profile_input_digest": "b" * 64,
        "materialization_mode": capacity.PROFILE_MATERIALIZATION_MODE,
        "profile_schema_version": 4,
        "profile_strategy_version": capacity.PROFILE_STRATEGY_VERSION,
        "executable_plan_hash": "c" * 64,
        "profile_as_of": "2026-07-30",
        "current_source_vector_hash": "d" * 64,
        "desired_source_vector_hash": "e" * 64,
        "current_context_vector_hash": "f" * 64,
        "desired_context_vector_hash": "0" * 64,
        "sql_contract_digest": "1" * 64,
        **_postgres_geometry_by_field(),
        **_relation_geometry_by_field(),
        **_execution_geometry_by_field(),
    }
    geometry_map.update(overrides)
    return geometry_map


def test_valid_geometry_binds_executable_plan_and_storage_classes():
    geometry_map = _geometry_payload()
    geometry = capacity.validated_capacity_geometry(
        dict(reversed(tuple(geometry_map.items())))
    )

    assert geometry.materialization_mode == "source_delta"
    assert geometry.maximum_worker_count == 2
    assert geometry.reservation_bytes_by_storage_class == {
        "data": (
            2_100
            + capacity.METADATA_DATA_UPPER_BOUND_BYTES
            + 23_456
        ),
        "temp": 2_048,
        "wal": (
            105
            + capacity.METADATA_WAL_UPPER_BOUND_BYTES
            + 12_345
        ),
    }
    assert capacity.capacity_geometry_payload(geometry) == geometry_map
    canonical_json = capacity.canonical_capacity_geometry_json(geometry)
    assert " " not in canonical_json
    assert len(capacity.capacity_geometry_hash(geometry)) == 64
    assert capacity.revalidate_capacity_geometry(geometry) == geometry


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_geometry_rejects_non_exact_outer_fields(mutation):
    geometry_map = _geometry_payload()
    if mutation == "missing":
        geometry_map.pop("sql_contract_digest")
    else:
        geometry_map["unexpected"] = "not allowed"

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="geometry_fields_invalid",
    ):
        capacity.validated_capacity_geometry(geometry_map)


def test_geometry_rejects_non_kib_aligned_temp_limit():
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="memory_setting_not_kib_aligned",
    ):
        capacity.validated_capacity_geometry(
            _geometry_payload(temp_file_limit_bytes=1025)
        )


def test_temp_reservation_covers_each_concurrent_backend():
    geometry = capacity.validated_capacity_geometry(
        _geometry_payload()
    )

    assert geometry.maximum_worker_count == 2
    assert geometry.reservation_bytes_by_storage_class["temp"] == 2 * 1024


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("selection_proof_id", "2" * 64),
        ("profile_input_digest", "2" * 64),
        ("executable_plan_hash", "2" * 64),
        ("profile_as_of", "2026-07-29"),
        ("current_source_vector_hash", "2" * 64),
        ("desired_source_vector_hash", "2" * 64),
        ("current_context_vector_hash", "2" * 64),
        ("desired_context_vector_hash", "2" * 64),
        ("sql_contract_digest", "2" * 64),
        ("evidence_target_oid", 199329912),
        ("profile_target_oid", 199329929),
        ("evidence_wave_count", 31),
        ("compact_worker_count", 1),
        ("artifact_scope_wave_count", 15),
        ("artifact_scope_worker_count", 1),
        ("artifact_scope_batch_size", 24_999),
        ("artifact_scope_projection_hash", "8" * 64),
        ("artifact_scope_projected_logical_bytes", 749_999_999),
        ("control_wal_plan_input_hash", "b" * 64),
        ("control_wal_upper_bound_bytes", 12_346),
        ("control_metadata_data_upper_bound_bytes", 23_457),
        ("import_run_oid", 199329961),
        ("capacity_consumption_oid", 199329971),
        ("import_run_storage_fingerprint", "b" * 64),
        ("capacity_consumption_storage_fingerprint", "c" * 64),
        ("max_build_seconds", 21_599),
        ("statement_timeout_ms", 899_999),
        ("lock_timeout_ms", 4_999),
        ("minimum_remaining_bytes", 99 * 1024 * 1024 * 1024),
        ("max_evidence_rows", 9_999_999),
    ],
)
def test_geometry_hash_changes_for_execution_identity_drift(
    field_name,
    replacement,
):
    baseline = capacity.validated_capacity_geometry(_geometry_payload())
    changed = capacity.validated_capacity_geometry(
        _geometry_payload(**{field_name: replacement})
    )

    assert capacity.capacity_geometry_hash(changed) != (
        capacity.capacity_geometry_hash(baseline)
    )


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"contract_id": "wrong"}, "geometry_contract_invalid"),
        ({"materialization_mode": "full_swap"}, "materialization_mode_invalid"),
        ({"profile_strategy_version": "v3"}, "strategy_version_invalid"),
        ({"profile_schema_version": 0}, "profile_schema_version_invalid"),
        ({"profile_as_of": "2026-7-30"}, "profile_as_of_invalid"),
        ({"profile_as_of": "2026-02-30"}, "profile_as_of_invalid"),
        ({"evidence_target_oid": 199329928}, "target_oid_collision"),
        ({"import_run_oid": 199329950}, "target_oid_collision"),
        (
            {"capacity_consumption_oid": 199329960},
            "target_oid_collision",
        ),
        (
            {"control_wal_plan_input_hash": "not-a-hash"},
            "control_wal_plan_input_hash_invalid",
        ),
        (
            {"control_wal_upper_bound_bytes": 0},
            "control_wal_upper_bound_bytes_invalid",
        ),
        (
            {"control_metadata_data_upper_bound_bytes": 0},
            "control_metadata_data_upper_bound_bytes_invalid",
        ),
        ({"max_artifact_scope_rows": -1}, "max_artifact_scope_rows_invalid"),
        (
            {"artifact_scope_projected_logical_bytes": -1},
            "artifact_scope_projected_logical_bytes_invalid",
        ),
        ({"max_evidence_rows": 0}, "max_evidence_rows_invalid"),
        ({"max_affected_npis": 0}, "max_affected_npis_invalid"),
        ({"max_profile_rows": 0}, "max_profile_rows_invalid"),
    ],
)
def test_geometry_rejects_invalid_execution_identity(overrides, reason):
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match=reason,
    ):
        capacity.validated_capacity_geometry(_geometry_payload(**overrides))


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"database_pool_size": 4}, "database_pool_size_invalid"),
        (
            {"database_pool_size": 5, "evidence_worker_count": 2},
            "worker_pool_reserve_invalid",
        ),
        ({"evidence_worker_count": 3}, "evidence_worker_count_invalid"),
        (
            {"artifact_scope_worker_count": 3},
            "artifact_scope_worker_count_invalid",
        ),
        (
            {
                "artifact_scope_wave_count": 0,
                "artifact_scope_worker_count": 1,
            },
            "wave_worker_geometry_invalid",
        ),
        (
            {"evidence_wave_count": 0, "evidence_worker_count": 1},
            "wave_worker_geometry_invalid",
        ),
        (
            {"evidence_wave_count": 1, "evidence_worker_count": 0},
            "wave_worker_geometry_invalid",
        ),
        (
            {"max_parallel_workers_per_gather": 1},
            "query_parallelism_not_zero",
        ),
        (
            {"max_parallel_maintenance_workers": 1},
            "maintenance_parallelism_not_zero",
        ),
        ({"work_mem_bytes": 1025}, "memory_setting_not_kib_aligned"),
        (
            {"maintenance_work_mem_bytes": 1025},
            "memory_setting_not_kib_aligned",
        ),
        ({"temp_file_limit_bytes": 0}, "temp_file_limit_bytes_invalid"),
        ({"artifact_scope_batch_size": 0}, "batch_size_invalid"),
        ({"max_build_seconds": 0}, "max_build_seconds_invalid"),
        ({"statement_timeout_ms": 0}, "statement_timeout_ms_invalid"),
        ({"lock_timeout_ms": 0}, "lock_timeout_ms_invalid"),
        ({"minimum_remaining_bytes": 0}, "minimum_remaining_bytes_invalid"),
        (
            {"lock_timeout_ms": 15 * 60 * 1000},
            "lock_timeout_not_below_statement_timeout",
        ),
        (
            {"statement_timeout_ms": 6 * 60 * 60 * 1000},
            "statement_timeout_not_below_build_timeout",
        ),
    ],
)
def test_geometry_rejects_unsafe_worker_and_session_limits(
    overrides,
    reason,
):
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match=reason,
    ):
        capacity.validated_capacity_geometry(_geometry_payload(**overrides))


def test_one_worker_fits_five_connection_pool():
    geometry = capacity.validated_capacity_geometry(
        _geometry_payload(
            database_pool_size=5,
            artifact_scope_worker_count=1,
            evidence_worker_count=1,
            compact_worker_count=1,
        )
    )

    assert geometry.maximum_worker_count == 1


def test_two_workers_fit_six_connection_pool_with_four_reserved():
    geometry = capacity.validated_capacity_geometry(
        _geometry_payload(
            database_pool_size=6,
            pool_reserve_connections=4,
        )
    )

    assert geometry.maximum_worker_count == 2
    assert (
        geometry.database_pool_size
        - geometry.pool_reserve_connections
        == geometry.maximum_worker_count
    )
    assert (
        capacity.PROFILE_DEDICATED_ADVISORY_LOCK_CONNECTIONS
        + capacity.PROFILE_CONTROL_CONNECTION_RESERVE
        + geometry.maximum_worker_count
        <= geometry.database_pool_size
    )
    assert geometry.reservation_bytes_by_storage_class["temp"] == 2 * 1024


@pytest.mark.parametrize("mutation", ["missing", "extra", "reordered"])
def test_relation_caps_require_exact_fields_and_order(mutation):
    relation_caps = _relation_byte_caps()
    if mutation == "missing":
        relation_caps[0].pop("max_wal_bytes")
        reason = "relation_fields_invalid"
    elif mutation == "extra":
        relation_caps[0]["unexpected"] = 1
        reason = "relation_fields_invalid"
    else:
        relation_caps[0], relation_caps[1] = relation_caps[1], relation_caps[0]
        reason = "relation_order_invalid"

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match=reason,
    ):
        capacity.validated_capacity_geometry(
            _geometry_payload(relation_byte_caps=relation_caps)
        )


@pytest.mark.parametrize(
    ("relation_index", "field_name", "replacement", "reason"),
    [
        (0, "max_scratch_bytes", 0, "scratch_relation_cap_invalid"),
        (
            0,
            "max_target_growth_bytes",
            1,
            "scratch_relation_target_cap_invalid",
        ),
        (4, "max_scratch_bytes", 1, "target_relation_scratch_cap_invalid"),
        (4, "max_target_growth_bytes", 0, "target_relation_cap_invalid"),
        (
            4,
            "max_deleted_logical_bytes",
            0,
            "target_relation_cap_invalid",
        ),
        (2, "max_temp_bytes", 0, "relation_temp_cap_invalid"),
        (2, "max_wal_bytes", 0, "relation_wal_cap_invalid"),
        (
            2,
            "max_temp_bytes",
            1025,
            "relation_temp_cap_not_session_limit",
        ),
        (
            2,
            "max_temp_bytes",
            1023,
            "relation_temp_cap_not_session_limit",
        ),
    ],
)
def test_relation_caps_fail_closed(
    relation_index,
    field_name,
    replacement,
    reason,
):
    relation_caps = _relation_byte_caps()
    relation_caps[relation_index][field_name] = replacement

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match=reason,
    ):
        capacity.validated_capacity_geometry(
            _geometry_payload(relation_byte_caps=relation_caps)
        )


def test_revalidation_rejects_tampered_immutable_geometry():
    geometry = capacity.validated_capacity_geometry(_geometry_payload())
    tampered_geometry = dataclasses.replace(
        geometry,
        executable_plan_hash="9" * 64,
        relation_byte_caps=geometry.relation_byte_caps[:-1],
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="relation_byte_caps_invalid",
    ):
        capacity.revalidate_capacity_geometry(tampered_geometry)
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="relation_byte_caps_invalid",
    ):
        capacity.capacity_geometry_hash(tampered_geometry)
