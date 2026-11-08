# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Offline ownership, accounting and lifecycle tests for native maintenance fencing."""

from __future__ import annotations

import asyncio
import dataclasses
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_control_maintenance as maintenance
from tests.test_provider_directory_profile_control_capacity import (
    _bound_control_wal_projection,
    _control_wal_plan_input,
)


class Connection:
    def __init__(self, events):
        self.events = events
        self.active = False
        self.conflict = False
        self.rollback_gate = None
        self.identity_count = 5
        self.held_count = 5

    def in_transaction(self):
        return self.active

    async def begin(self):
        self.events.append("begin")
        self.active = True

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.events.append((sql, parameters))
        if sql.startswith("LOCK TABLE") and self.conflict:
            raise RuntimeError("lock_not_available")
        return SimpleNamespace(scalar_one=lambda: self.held_count if "pg_locks" in sql else self.identity_count)

    async def commit(self):
        self.events.append("commit")
        self.active = False

    async def rollback(self):
        self.events.append("rollback_start")
        if self.rollback_gate is not None:
            await self.rollback_gate.wait()
        self.active = False
        self.events.append("rollback_done")


def _maintenance_identity(geometry):
    """Keep all native fence OIDs and database identity in the synthetic fixture."""
    identity = SimpleNamespace(
        **{
            name + "_oid": getattr(geometry, name + "_oid")
            for name in (
                "build_checkpoint",
                "serving_generation",
                "delta_receipt",
                "import_run",
                "capacity_consumption",
            )
        }
    )
    identity.database_name = geometry.database_name
    return identity


def fixture():
    """Build recorded native-fence callbacks around one synthetic connection."""
    geometry, projection = _bound_control_wal_projection()
    events = []
    connection = Connection(events)
    identity = _maintenance_identity(geometry)
    admission = SimpleNamespace(
        geometry=geometry,
        control_wal_projection=projection,
        database_identity=identity,
        admitted_identity=object(),
        wal_tracker=SimpleNamespace(owned_control_maintenance_outcome=None),
    )

    @asynccontextmanager
    async def window(relation):
        events.append("window_start")
        yield
        events.append("window_settle")

    @asynccontextmanager
    async def transaction():
        events.append("separate_validation_transaction")
        yield

    async def reserve(_, *, control_operation_counts):
        events.append(("reserve", control_operation_counts))

    async def guard(source, expected):
        assert source is admission.admitted_identity and expected is identity
        assert connection.active
        events.append("admitted_identity_verified")

    fhir = SimpleNamespace(
        _provider_directory_profile_capacity_admission=lambda: admission,
        _provider_directory_profile_sql_contract_digest=lambda: geometry.sql_contract_digest,
        _provider_directory_database_pool_capacity=lambda: geometry.database_pool_size,
        db=SimpleNamespace(
            engine=SimpleNamespace(pool=SimpleNamespace(size=lambda: 1, _max_overflow=geometry.database_pool_size - 1))
        ),
        _schema=lambda: "sample",
        _unscoped_qt=lambda schema, name: f'"{schema}"."{name}"',
        _provider_directory_profile_checkpoint_ref=lambda schema: f'"{schema}"."checkpoint"',
        _provider_directory_profile_serving_generation_ref=lambda schema: f'"{schema}"."serving"',
        _provider_directory_profile_delta_receipt_ref=lambda schema: f'"{schema}"."receipt"',
        ImportRun=SimpleNamespace(__tablename__="import_run"),
        ProviderDirectoryProfileCapacityLeaseConsumption=SimpleNamespace(__tablename__="consumption"),
        _profile_capacity_mutation_window=window,
        _provider_directory_profile_capacity_transaction=transaction,
        _reserve_provider_directory_profile_wal_budget=reserve,
        _profile_capacity_remaining_ms=AsyncMock(return_value=1000),
        _admission_database_guard=guard,
    )
    return fhir, connection, events, admission


async def test_independent_advisory_connection_spans_every_preparation_phase_and_releases_before_cutover():
    fhir, connection, events, _ = fixture()
    async with maintenance.control_maintenance_fence(fhir, connection):
        for phase in ("artifact", "evidence", "affected", "profile", "prepare_indexes"):
            assert connection.active
            events.append(phase)
        await maintenance.release_before_cutover(fhir)
        assert not connection.active
        events.append("stronger_cutover_locks")
    lock_sql = next(x[0] for x in events if isinstance(x, tuple) and x[0].startswith("LOCK TABLE"))
    assert "IN SHARE UPDATE EXCLUSIVE MODE NOWAIT" in lock_sql
    assert lock_sql.count('"sample".') == 5
    assert events.index("admitted_identity_verified") < events.index("artifact")
    assert events.index("profile") < events.index("commit") < events.index("stronger_cutover_locks")
    assert events.count("commit") == 1 and "rollback_start" not in events
    assert ("reserve", {"control_maintenance_acquire": 1}) in events
    assert ("reserve", {"control_maintenance_release": 1}) in events


@pytest.mark.parametrize(
    "change", ["digest", "pool", "engine_pool", "active_transaction", "oid", "duplicate_oid", "old_pool_reserve"]
)
async def test_invalid_custody_refuses_before_native_lock_or_payload(change):
    fhir, connection, events, admission = fixture()
    if change == "digest":
        fhir._provider_directory_profile_sql_contract_digest = lambda: "0" * 64
    if change == "pool":
        fhir._provider_directory_database_pool_capacity = lambda: 99
    if change == "engine_pool":
        fhir.db.engine.pool._max_overflow = 99
    if change == "active_transaction":
        connection.active = True
    if change == "oid":
        admission.database_identity.import_run_oid += 1
    if change == "duplicate_oid":
        admission.database_identity.import_run_oid = admission.database_identity.build_checkpoint_oid
    if change == "old_pool_reserve":
        admission.geometry = dataclasses.replace(admission.geometry, pool_reserve_connections=3)
    with pytest.raises((RuntimeError, capacity.ProviderDirectoryProfileCapacityError)):
        async with maintenance.control_maintenance_fence(fhir, connection):
            pytest.fail("payload entered")
    assert not any(isinstance(x, tuple) and x[0].startswith("LOCK TABLE") for x in events)


@pytest.mark.parametrize("failure", ["lock", "identity", "payload", "release", "cancel"])
async def test_failure_refuses_and_rolls_back_before_connection_can_be_unlocked(failure):
    fhir, connection, events, _ = fixture()
    if failure == "lock":
        connection.conflict = True
    if failure == "identity":
        fhir._admission_database_guard = AsyncMock(side_effect=RuntimeError("stale_identity"))
    if failure == "release":
        connection.commit = AsyncMock(side_effect=RuntimeError("commit_failed"))
    error = asyncio.CancelledError if failure == "cancel" else RuntimeError
    with pytest.raises(error):
        async with maintenance.control_maintenance_fence(fhir, connection):
            if failure == "cancel":
                raise asyncio.CancelledError
            if failure == "payload":
                raise RuntimeError("payload_failed")
    assert not connection.active
    assert events[-1] == "rollback_done"
    assert maintenance._ACTIVE.get() is None


async def test_repeated_cancellation_drains_rollback():
    fhir, connection, events, _ = fixture()
    connection.rollback_gate = asyncio.Event()

    async def worker():
        async with maintenance.control_maintenance_fence(fhir, connection):
            raise asyncio.CancelledError

    task = asyncio.create_task(worker())
    while "rollback_start" not in events:
        await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    connection.rollback_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not connection.active and events[-1] == "rollback_done"


async def test_another_task_cannot_release_owner_connection():
    fhir, connection, _, _ = fixture()
    async with maintenance.control_maintenance_fence(fhir, connection):
        with pytest.raises(RuntimeError, match="owner_changed"):
            await asyncio.create_task(maintenance.release_before_cutover(fhir))
        assert connection.active


async def test_missing_admission_does_not_start_connection_or_apply_fence():
    fhir, connection, events, _ = fixture()
    fhir._provider_directory_profile_capacity_admission = lambda: None
    async with maintenance.control_maintenance_fence(fhir, connection):
        assert not connection.active
    assert events == []


def test_signed_fence_plan_preserves_pool_and_native_envelopes():
    plan = _control_wal_plan_input()
    plan_by_field = capacity.profile_control_wal_plan_input_payload(plan)
    assert plan_by_field["control_maintenance_fence_contract_id"] == capacity.CONTROL_MAINTENANCE_FENCE_CONTRACT_ID
    assert plan_by_field["control_maintenance_extra_connections"] == 0
    geometry, projection = _bound_control_wal_projection(plan)
    operations_by_name = {operation.operation_name: operation for operation in projection.operations}
    assert operations_by_name["control_maintenance_acquire"].fixed_statement_wal_bytes == 65536
    assert operations_by_name["control_maintenance_acquire"].commit_count == 0
    assert operations_by_name["control_maintenance_release"].commit_envelope_bytes == 8192
    assert operations_by_name["control_maintenance_release"].operation_count == 1
    assert geometry.pool_reserve_connections == 4
    old_plan_by_field = dict(plan_by_field)
    old_plan_by_field.pop("control_maintenance_fence_contract_id")
    old_plan_by_field.pop("control_maintenance_extra_connections")
    import hashlib
    import json

    from process.provider_directory_profile_capacity_types import _CONTROL_WAL_PLAN_INPUT_HASH_DOMAIN

    old_hash = hashlib.sha256(
        (
            _CONTROL_WAL_PLAN_INPUT_HASH_DOMAIN
            + ":"
            + json.dumps(old_plan_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        ).encode()
    ).hexdigest()
    assert old_hash != geometry.control_wal_plan_input_hash
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError):
        capacity.revalidate_profile_control_wal_projection(
            dataclasses.replace(geometry, control_wal_plan_input_hash=old_hash), projection
        )


async def test_wrong_actual_name_oid_database_or_schema_refuses_before_table_lock():
    fhir, connection, events, _ = fixture()
    connection.identity_count = 4
    with pytest.raises(RuntimeError, match="relation_changed"):
        async with maintenance.control_maintenance_fence(fhir, connection):
            pytest.fail("payload entered")
    assert not any(isinstance(x, tuple) and x[0].startswith("LOCK TABLE") for x in events)
    assert events[-1] == "rollback_done"


async def test_actual_preparation_facade_releases_before_publisher_and_advisory_unlock(monkeypatch):
    import importlib

    importer = importlib.import_module("process.provider_directory_fhir")
    preparation = importlib.import_module("process.provider_directory_artifact_bundle_preparation")
    fhir, connection, events, admission = fixture()
    for name in (
        "_provider_directory_profile_capacity_admission",
        "_provider_directory_profile_sql_contract_digest",
        "_provider_directory_database_pool_capacity",
        "_schema",
        "_profile_capacity_mutation_window",
        "_reserve_provider_directory_profile_wal_budget",
        "_profile_capacity_remaining_ms",
        "_provider_directory_profile_capacity_transaction",
        "_admission_database_guard",
    ):
        monkeypatch.setattr(importer, name, getattr(fhir, name))
    monkeypatch.setattr(importer.db, "engine", fhir.db.engine)
    monkeypatch.setattr(importer, "_acquire_provider_directory_artifact_build_lock", AsyncMock(return_value=connection))
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", AsyncMock(return_value=None))

    async def unlock(conn, key, *, maintenance_admission=None):
        assert maintenance_admission is admission
        assert conn is connection and not conn.active
        events.append("advisory_unlock")

    monkeypatch.setattr(importer, "_release_provider_directory_artifact_build_lock", unlock)

    @asynccontextmanager
    async def prepare(*args, **kwargs):
        async with importer._provider_directory_artifact_scope_guard("sample"):
            assert connection.active
            events.append("actual_preparation")
            yield ("bundle", {"prepared": True})
            events.append("cleanup")

    monkeypatch.setattr(preparation, "prepare_artifact_bundle", prepare)
    async with importer._prepare_artifact_bundle_from_fence(
        object(), object(), artifact_resource_types=frozenset()
    ) as prepared_bundle:
        assert prepared_bundle == ("bundle", {"prepared": True})
        assert not connection.active
        events.append("actual_publisher")
    assert (
        events.index("actual_preparation")
        < events.index("commit")
        < events.index("actual_publisher")
        < events.index("advisory_unlock")
    )
    assert admission.geometry.pool_reserve_connections == 4


async def test_expired_deadline_prevents_begin_and_lock():
    fhir, connection, events, _ = fixture()
    fhir._profile_capacity_remaining_ms = AsyncMock(side_effect=RuntimeError("deadline_expired"))
    with pytest.raises(RuntimeError, match="deadline_expired"):
        async with maintenance.control_maintenance_fence(fhir, connection):
            pytest.fail("payload entered")
    assert "begin" not in events and "rollback_done" in events


@pytest.mark.parametrize("loss", ["transaction", "native_locks"])
async def test_lost_native_lease_refuses_while_lifecycle_flag_still_claims_acquisition(loss):
    fhir, connection, events, _ = fixture()
    with pytest.raises(RuntimeError, match="lease_lost"):
        async with maintenance.control_maintenance_fence(fhir, connection):
            await maintenance.assert_held(fhir)
            if loss == "transaction":
                connection.active = False
            else:
                connection.held_count = 4
            await maintenance.assert_held(fhir)
            pytest.fail("lost lease allowed payload")
    assert events[-1] == "rollback_done"


async def test_real_mutation_facade_checks_native_lease_inside_original_window(monkeypatch):
    import importlib

    importer = importlib.import_module("process.provider_directory_fhir")
    fhir, connection, events, _ = fixture()
    monkeypatch.setattr(
        importer.profile_capacity_projection,
        "mutation_window",
        lambda *args: fhir._profile_capacity_mutation_window(None),
    )

    async with maintenance.control_maintenance_fence(fhir, connection):
        lease = maintenance._ACTIVE.get()
        lease.fhir = importer
        monkeypatch.setattr(importer, "_profile_capacity_remaining_ms", fhir._profile_capacity_remaining_ms)
        async with importer._profile_capacity_mutation_window("profile_stage"):
            events.append("actual_window_payload")
        lease.fhir = fhir
    lock_observation = next(i for i, event in enumerate(events) if isinstance(event, tuple) and "pg_locks" in event[0])
    assert events[lock_observation - 1] == "window_start"
    assert lock_observation < events.index("actual_window_payload")
