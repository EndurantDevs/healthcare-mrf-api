# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Runtime configuration proof for bounded Profile capacity geometry."""

from __future__ import annotations

import asyncio
import datetime
import dataclasses
import importlib
import json
from types import SimpleNamespace

import pytest

from db.connection import Database
from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_capacity_runtime as runtime


importer = importlib.import_module("process.provider_directory_fhir")


def _relation_cap(relation_name: str) -> dict[str, object]:
    is_scratch = relation_name in {
        "artifact_scope",
        "evidence_stage",
        "affected_npi_stage",
        "profile_stage",
    }
    return {
        "relation_name": relation_name,
        "max_scratch_bytes": 10_000 if is_scratch else 0,
        "max_target_growth_bytes": 0 if is_scratch else 20_000,
        "max_deleted_logical_bytes": 0 if is_scratch else 30_000,
        "max_temp_bytes": 1_024,
        "max_wal_bytes": 40_000,
    }


def _limits_payload(**overrides: object) -> dict[str, object]:
    limits_by_field: dict[str, object] = {
        "contract_id": runtime.CAPACITY_LIMITS_CONTRACT_ID,
        "artifact_scope_batch_size": 100_000,
        "pool_reserve_connections": 4,
        "work_mem_bytes": 4 * 1024 * 1024,
        "maintenance_work_mem_bytes": 64 * 1024 * 1024,
        "temp_file_limit_bytes": 1_024,
        "max_build_seconds": 3_600,
        "statement_timeout_ms": 60_000,
        "lock_timeout_ms": 5_000,
        "minimum_remaining_bytes": 1_000_000,
        "max_artifact_scope_rows": 5_000_000,
        "max_evidence_rows": 10_000_000,
        "max_affected_npis": 2_000_000,
        "max_profile_rows": 2_000_000,
        "relation_byte_caps": [
            _relation_cap(relation_name)
            for relation_name in (
                "artifact_scope",
                "evidence_stage",
                "affected_npi_stage",
                "profile_stage",
                "evidence_target",
                "profile_target",
            )
        ],
    }
    limits_by_field.update(overrides)
    return limits_by_field


def _geometry_database_by_field() -> dict[str, object]:
    """Return the PostgreSQL ABI identity bound into a capacity geometry."""
    return {
        "database_system_identifier": "7527713908662902214",
        "database_oid": 16401,
        "database_name": "healthporta",
        "tablespace_oid": 1663,
        "tablespace_name": "pg_default",
        "postgres_server_version_num": 180002,
        "postgres_block_size_bytes": 8192,
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


def _geometry_relation_by_field() -> dict[str, object]:
    """Return serving and control relation identities for geometry tests."""
    return {
        "evidence_target_oid": 20001,
        "profile_target_oid": 20002,
        "evidence_target_storage_fingerprint": "9" * 64,
        "profile_target_storage_fingerprint": "a" * 64,
        "build_checkpoint_oid": 20003,
        "serving_generation_oid": 20004,
        "delta_receipt_oid": 20005,
        "import_run_oid": 20006,
        "capacity_consumption_oid": 20007,
        "build_checkpoint_storage_fingerprint": "d" * 64,
        "serving_generation_storage_fingerprint": "b" * 64,
        "delta_receipt_storage_fingerprint": "c" * 64,
        "import_run_storage_fingerprint": "f" * 64,
        "capacity_consumption_storage_fingerprint": "0" * 64,
    }


def _geometry_inputs(
    **overrides: object,
) -> runtime.ProviderDirectoryProfileCapacityGeometryInputs:
    """Build an exact immutable geometry-input fixture."""
    geometry_by_field: dict[str, object] = {
        "selection_proof_id": "1" * 64,
        "profile_input_digest": "2" * 64,
        "profile_schema_version": 1,
        "profile_strategy_version": capacity.PROFILE_STRATEGY_VERSION,
        "executable_plan_hash": "3" * 64,
        "profile_as_of": "2026-07-30",
        "current_source_vector_hash": "4" * 64,
        "desired_source_vector_hash": "5" * 64,
        "current_context_vector_hash": "6" * 64,
        "desired_context_vector_hash": "7" * 64,
        "sql_contract_digest": "8" * 64,
        **_geometry_database_by_field(),
        **_geometry_relation_by_field(),
        "control_wal_plan_input_hash": "1" * 64,
        "control_wal_upper_bound_bytes": 50_000,
        "control_metadata_data_upper_bound_bytes": 60_000,
        "artifact_scope_wave_count": 8,
        "evidence_wave_count": 58,
        "compact_wave_count": 200,
        "artifact_scope_worker_count": 2,
        "evidence_worker_count": 2,
        "compact_worker_count": 2,
        "database_pool_size": 16,
        "artifact_scope_projected_rows": 250_000,
        "artifact_scope_projected_logical_bytes": 7_500_000,
        "artifact_scope_projection_hash": "e" * 64,
    }
    return runtime.ProviderDirectoryProfileCapacityGeometryInputs(
        **{**geometry_by_field, **overrides}
    )


def test_configured_limits_have_no_implicit_defaults(monkeypatch):
    monkeypatch.delenv(runtime.CAPACITY_LIMITS_ENV, raising=False)
    with pytest.raises(
        runtime.ProviderDirectoryProfileCapacityConfigurationError,
        match="configuration_missing",
    ):
        runtime.configured_capacity_limits()

    limits = runtime.configured_capacity_limits(
        json.dumps(_limits_payload())
    )
    assert limits.artifact_scope_batch_size == 100_000
    assert len(limits.relation_byte_caps) == 6


@pytest.mark.parametrize("mutation", ("missing", "extra", "bool_zero"))
def test_limits_contract_rejects_shape_and_boolean_zero(mutation):
    payload = _limits_payload()
    if mutation == "missing":
        payload.pop("max_profile_rows")
    elif mutation == "extra":
        payload["unexpected"] = 1
    else:
        payload["relation_byte_caps"][-1]["max_scratch_bytes"] = False

    with pytest.raises(
        runtime.ProviderDirectoryProfileCapacityConfigurationError,
    ):
        runtime.validated_capacity_limits(payload)


def _assert_capacity_reservation_totals(geometry) -> None:
    """Assert the exact data, temporary, and WAL reservation totals."""
    assert geometry.max_artifact_scope_rows == 250_000
    assert geometry.artifact_scope_projected_logical_bytes == 7_500_000
    assert geometry.artifact_scope_projection_hash == "e" * 64
    assert geometry.minimum_remaining_bytes == 1_000_000
    assert geometry.artifact_scope_batch_size == 100_000
    assert geometry.maximum_worker_count == 2
    assert geometry.import_run_oid == 20006
    assert geometry.capacity_consumption_oid == 20007
    assert geometry.import_run_storage_fingerprint == "f" * 64
    assert geometry.capacity_consumption_storage_fingerprint == "0" * 64
    assert geometry.control_wal_plan_input_hash == "1" * 64
    assert geometry.control_wal_upper_bound_bytes == 50_000
    assert geometry.control_metadata_data_upper_bound_bytes == 60_000
    assert (
        geometry.reservation_bytes_by_storage_class
        == {
            "data": (
                80_000
                + capacity.METADATA_DATA_UPPER_BOUND_BYTES
                + 60_000
            ),
            "temp": 2_048,
            "wal": (
                240_000
                + capacity.METADATA_WAL_UPPER_BOUND_BYTES
                + 50_000
            ),
        }
    )


def test_geometry_binds_runtime_limits_and_exact_artifact_projection():
    limits = runtime.validated_capacity_limits(_limits_payload())
    geometry = runtime.build_capacity_geometry(limits, _geometry_inputs())
    _assert_capacity_reservation_totals(geometry)
    original_hash = capacity.capacity_geometry_hash(geometry)
    changed_limits = dataclasses.replace(
        limits,
        minimum_remaining_bytes=1_000_001,
    )
    changed_geometry = runtime.build_capacity_geometry(
        changed_limits,
        _geometry_inputs(),
    )
    assert capacity.capacity_geometry_hash(changed_geometry) != original_hash

    changed_control_geometry = runtime.build_capacity_geometry(
        limits,
        _geometry_inputs(control_wal_upper_bound_bytes=50_001),
    )
    assert capacity.capacity_geometry_hash(changed_control_geometry) != (
        original_hash
    )
    changed_control_data_geometry = runtime.build_capacity_geometry(
        limits,
        _geometry_inputs(
            control_metadata_data_upper_bound_bytes=60_001
        ),
    )
    assert capacity.capacity_geometry_hash(
        changed_control_data_geometry
    ) != original_hash


def test_geometry_rejects_artifact_projection_above_deployment_cap():
    limits = runtime.validated_capacity_limits(
        _limits_payload(max_artifact_scope_rows=249_999)
    )
    with pytest.raises(
        runtime.ProviderDirectoryProfileCapacityConfigurationError,
        match="artifact_scope_rows_exceeded",
    ):
        runtime.build_capacity_geometry(limits, _geometry_inputs())


def test_geometry_accepts_exact_zero_row_noop_artifact_scope():
    geometry = runtime.build_capacity_geometry(
        runtime.validated_capacity_limits(_limits_payload()),
        _geometry_inputs(artifact_scope_projected_rows=0),
    )
    assert geometry.max_artifact_scope_rows == 0


@pytest.mark.asyncio
async def test_postgres_applies_exact_finite_capacity_settings(monkeypatch):
    """Prove the frozen KiB/ms settings round-trip on PostgreSQL."""

    database = Database()
    try:
        await database.connect()
        await database.scalar("SELECT 1;")
    except Exception:
        await database.disconnect()
        pytest.skip("capacity runtime proof needs PostgreSQL")
    if (
        await database.scalar(
            "SELECT has_parameter_privilege("
            "current_user, 'temp_file_limit', 'SET'"
            ");"
        )
        is not True
    ):
        await database.disconnect()
        pytest.skip(
            "capacity runtime proof needs temp_file_limit SET privilege"
        )
    geometry = runtime.build_capacity_geometry(
        runtime.validated_capacity_limits(_limits_payload()),
        _geometry_inputs(),
    )
    admission = SimpleNamespace(
        geometry=geometry,
        lease=SimpleNamespace(
            max_build_deadline=(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(minutes=30)
            )
        ),
    )
    monkeypatch.setattr(importer, "db", database)
    try:
        async with database.transaction():
            await importer._apply_provider_directory_profile_capacity_settings(
                admission
            )
    finally:
        await database.disconnect()


@pytest.mark.asyncio
async def test_capacity_settings_fail_before_mutation_without_temp_privilege(
    monkeypatch,
):
    """An unprivileged runtime must not silently lose the signed temp cap."""

    class MissingPrivilegeDatabase:
        async def scalar(self, statement):
            assert "has_parameter_privilege" in statement
            return json.loads("false")

        async def status(self, _statement):
            raise AssertionError("capacity settings must not be mutated")

    geometry = runtime.build_capacity_geometry(
        runtime.validated_capacity_limits(_limits_payload()),
        _geometry_inputs(),
    )
    admission = SimpleNamespace(
        geometry=geometry,
        lease=SimpleNamespace(
            max_build_deadline=(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(minutes=30)
            )
        ),
    )
    monkeypatch.setattr(importer, "db", MissingPrivilegeDatabase())

    with pytest.raises(
        RuntimeError,
        match="capacity_temp_file_limit_privilege_missing",
    ):
        await importer._apply_provider_directory_profile_capacity_settings(
            admission
        )


@pytest.mark.asyncio
async def test_capacity_settings_refuse_database_clock_deadline_reserve(
    monkeypatch,
):
    """No transaction starts work inside the fixed commit reserve."""

    class DeadlineDatabase:
        async def scalar(self, statement):
            assert "has_parameter_privilege" in statement
            return json.loads("true")

        async def first(self, statement, **_params):
            assert "clock_timestamp()" in statement
            return SimpleNamespace(_mapping={"remaining_ms": 1_000})

        async def status(self, _statement):
            raise AssertionError("expired capacity settings must not mutate")

    geometry = runtime.build_capacity_geometry(
        runtime.validated_capacity_limits(_limits_payload()),
        _geometry_inputs(),
    )
    admission = SimpleNamespace(
        geometry=geometry,
        lease=SimpleNamespace(
            max_build_deadline=(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(seconds=1)
            )
        ),
    )
    monkeypatch.setattr(importer, "db", DeadlineDatabase())

    with pytest.raises(
        importer.ProviderDirectoryCapacityLeaseError,
        match="deadline_reached",
    ):
        await importer._apply_provider_directory_profile_capacity_settings(
            admission
        )


@pytest.mark.asyncio
async def test_two_postgres_workers_each_receive_the_signed_temp_cap(
    monkeypatch,
):
    """Prove both admitted backends are bounded without consuming the reserve."""

    database = Database()
    try:
        await database.connect()
        can_set_temp_limit = await database.scalar(
            "SELECT has_parameter_privilege("
            "current_user, 'temp_file_limit', 'SET'"
            ");"
        )
    except Exception:
        await database.disconnect()
        pytest.skip("two-worker capacity proof needs PostgreSQL")
    if can_set_temp_limit is not True:
        await database.disconnect()
        pytest.skip(
            "two-worker capacity proof needs temp_file_limit SET privilege"
        )
    geometry = runtime.build_capacity_geometry(
        runtime.validated_capacity_limits(_limits_payload()),
        _geometry_inputs(
            database_pool_size=6,
            artifact_scope_worker_count=2,
            evidence_worker_count=2,
            compact_worker_count=2,
        ),
    )
    admission = _capacity_admission(geometry)
    monkeypatch.setattr(importer, "db", database)

    async def bounded_worker():
        async with database.transaction():
            await importer._apply_provider_directory_profile_capacity_settings(
                admission
            )
            return int(
                await database.scalar(
                    "SELECT pg_size_bytes("
                    "current_setting('temp_file_limit')"
                    ")::bigint;"
                )
            )

    try:
        observed_limits = await asyncio.gather(
            bounded_worker(),
            bounded_worker(),
        )
    finally:
        await database.disconnect()

    assert observed_limits == [
        geometry.temp_file_limit_bytes,
        geometry.temp_file_limit_bytes,
    ]
    assert geometry.database_pool_size == 6
    assert geometry.pool_reserve_connections == 4


def _capacity_admission(geometry):
    """Return an admitted geometry with a future build deadline."""
    return SimpleNamespace(
        geometry=geometry,
        lease=SimpleNamespace(
            max_build_deadline=(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(minutes=30)
            )
        ),
    )
