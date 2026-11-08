# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bound native control-table maintenance to an existing advisory connection."""

from __future__ import annotations

import asyncio
import contextvars
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from decimal import Decimal

from sqlalchemy import text

from process.provider_directory_profile_capacity_types import _MAX_SIGNED_BIGINT, CONTROL_MAINTENANCE_FENCE_CONTRACT_ID

SQL_CONTRACT = (
    CONTROL_MAINTENANCE_FENCE_CONTRACT_ID + ":existing-artifact-scope-connection:extra-connections=0:"
    "build_checkpoint,serving_generation,delta_receipt,import_run,capacity_consumption:"
    "LOCK TABLE IN SHARE UPDATE EXCLUSIVE MODE NOWAIT:"
    "recheck-admitted-identity:verify-native-locks-every-window:release-before-prepared-bundle-yield"
)
_ACTIVE = contextvars.ContextVar("profile_control_maintenance_fence", default=None)


@dataclass
class _Lease:
    fhir: object
    connection: object
    admission: object
    owner: object
    released: bool = False
    has_acquired: bool = False
    coordinates: tuple = ()
    query_lock: object = field(default_factory=asyncio.Lock)
    owned_context: object | None = None
    owned_outcome: object | None = None
    owned_finished: bool = False
    owned_custody: dict | None = None


async def _rollback_drained(lease):
    task = asyncio.create_task(lease.connection.rollback())
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    task.result()
    lease.released = True


async def assert_held(fhir):
    """Refuse a lost native lease before a bounded worker window proceeds."""
    lease = _ACTIVE.get()
    if lease is None or lease.released or not lease.has_acquired:
        return
    if lease.fhir is not fhir or not lease.connection.in_transaction():
        raise RuntimeError("provider_directory_profile_maintenance_lease_lost")
    async with lease.query_lock:
        async with asyncio.timeout(await fhir._profile_capacity_remaining_ms(lease.admission) / 1000):
            lock_result = await lease.connection.execute(
                text(
                    "SELECT count(DISTINCT relation)::integer FROM pg_locks "
                    "WHERE pid = pg_backend_pid() AND database = CAST(:database_oid AS oid) "
                    "AND locktype = 'relation' AND mode = 'ShareUpdateExclusiveLock' AND granted "
                    "AND relation = ANY(CAST(:relation_oids AS oid[]))"
                ),
                {
                    "database_oid": lease.admission.geometry.database_oid,
                    "relation_oids": [oid for oid, _ in lease.coordinates],
                },
            )
            if lock_result.scalar_one() != 5:
                raise RuntimeError("provider_directory_profile_maintenance_lease_lost")


def _begin_owned_maintenance_custody(lease):
    """Hold the original long-lived owner across every intervening parent."""
    tracker = lease.admission.wal_tracker
    preceding = getattr(tracker, "owned_control_maintenance_custody", None)
    if preceding is not None and preceding.get("consumed") is not True:
        raise RuntimeError("provider_directory_profile_maintenance_custody_unconsumed")
    lease.owned_custody = {
        "lease": lease,
        "task": lease.owner,
        "connection": lease.connection,
        "admission": lease.admission,
        "acquisition_window": lease.fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get(),
        "original_outcome": None,
        "retained_driver": None,
        "retained_pid": None,
        "outcome": None,
        "terminal_window": None,
        "identity_check_identity": ("maintenance_identity_check", lease, lease.admission.admitted_identity),
        "identity_check_group": None,
        "original_identity_check_group": None,
        "ordinary_body_physical_wal_upper_bytes": None,
        "whole_owner_wal_readiness": "incomplete",
        "unobserved_owner_phases": ("preparation", "restoration", "sampling_tail"),
        "status": "incomplete",
        "consumed": False,
        "accounting_authority": False,
        "reservation_refund": False,
    }
    tracker.owned_control_maintenance_custody = lease.owned_custody


def _retain_maintenance_origin(lease, outcome):
    """Keep the native object and endpoint returned by the actual owner."""
    custody_by_field = lease.owned_custody
    if custody_by_field is not None and custody_by_field["original_outcome"] is None and outcome is not None:
        custody_by_field["original_outcome"] = outcome
        custody_by_field["retained_driver"] = outcome.retained_driver
        custody_by_field["retained_pid"] = outcome.retained_pid


def _ordinary_body_physical_wal_upper(measurement, geometry):
    """Bound reviewed PG18.2/18.6 ordinary encoded records, including alignment/page headers."""
    record_bytes, records = measurement.wal_bytes_delta, measurement.wal_records_delta
    if (
        geometry.postgres_server_version_num not in (180002, 180006)
        or geometry.postgres_block_size_bytes != 8192
        or geometry.postgres_wal_block_size_bytes != 8192
        or type(record_bytes) is not Decimal
        or not record_bytes.is_finite()
        or record_bytes != record_bytes.to_integral_value()
        or not 0 <= record_bytes <= _MAX_SIGNED_BIGINT
        or type(records) is not int
        or not 0 <= records <= _MAX_SIGNED_BIGINT
        or (records == 0 and record_bytes != 0)
        or record_bytes < 25 * records
    ):
        raise RuntimeError("provider_directory_profile_maintenance_ordinary_wal_invalid")
    aligned_upper = int(record_bytes) + 7 * records
    upper = aligned_upper + 40 * ((aligned_upper + 8151) // 8152 + records)
    if upper > _MAX_SIGNED_BIGINT:
        raise RuntimeError("provider_directory_profile_maintenance_ordinary_wal_overflow")
    return upper


def _maintenance_identity_check_outcome(lease, custody_by_field):
    """Require the distinct original callback owner retained before window clearing."""
    from process.provider_directory_backend_wal_diagnostic import BackendWalDiagnosticResult
    from process.provider_directory_owned_wal_transaction import OwnedWalTransaction

    group = custody_by_field["identity_check_group"]
    outcome = None if not isinstance(group, dict) else group.get("original_outcome")
    if not (
        isinstance(group, dict)
        and group is custody_by_field["original_identity_check_group"]
        and group.get("identity") is group.get("original_identity") is custody_by_field["identity_check_identity"]
        and group.get("task") is lease.owner is asyncio.current_task()
        and group.get("admission") is lease.admission
        and group.get("window") is custody_by_field["acquisition_window"]
        and group.get("body_complete") is True
        and group.get("failure") is None
        and group.get("consumed") is True
        and group.get("status") == "complete_mixed_unclassified"
        and isinstance(outcome, OwnedWalTransaction)
        and group.get("outcome") is outcome
        and outcome is not custody_by_field["original_outcome"]
        and group.get("session") is outcome.session
        and group.get("connection") is outcome.connection
        and outcome.connection is not lease.connection
        and group.get("driver") is outcome.retained_driver
        and outcome.retained_driver is not None
        and outcome.retained_driver is not custody_by_field["retained_driver"]
        and type(group.get("pid")) is int
        and group["pid"] == outcome.retained_pid
        and outcome.retained_pid != custody_by_field["retained_pid"]
        and outcome.is_committed
        and outcome.cleanup_complete
        and outcome.status == "committed_measured"
        and isinstance(outcome.measurement, BackendWalDiagnosticResult)
        and outcome.measurement.baseline.identity == outcome.measurement.final.identity
        and outcome.retained_pid == outcome.measurement.final.identity.pid
        and all(
            getattr(outcome.measurement.final.identity, name)
            == getattr(custody_by_field["original_outcome"].measurement.final.identity, name)
            for name in ("database_oid", "database_name", "system_identifier")
        )
    ):
        raise RuntimeError("provider_directory_profile_maintenance_identity_custody_incomplete")
    return outcome


def _maintenance_body_physical_wal_upper(lease, outcomes):
    """Check both original body bounds against the unchanged signed allocations."""
    from process.provider_directory_profile_capacity_control_projection import revalidate_profile_control_wal_projection

    projection = revalidate_profile_control_wal_projection(
        lease.admission.geometry, lease.admission.control_wal_projection
    )
    allocations = [
        operation
        for operation in projection.operations
        if operation.operation_name in {"control_maintenance_acquire", "control_maintenance_release"}
    ]
    if len(allocations) != 2 or any(operation.operation_count != 1 for operation in allocations):
        raise RuntimeError("provider_directory_profile_maintenance_allocation_invalid")
    body_upper = sum(
        _ordinary_body_physical_wal_upper(observed.measurement, lease.admission.geometry) for observed in outcomes
    )
    if body_upper > _MAX_SIGNED_BIGINT:
        raise RuntimeError("provider_directory_profile_maintenance_ordinary_wal_overflow")
    if body_upper > sum(operation.wal_bytes for operation in allocations):
        raise RuntimeError("provider_directory_profile_maintenance_body_wal_overrun")
    return body_upper


def _consume_owned_maintenance_custody(lease, outcome):
    """Complete once at terminal release, never inside an earlier parent."""
    from process.provider_directory_backend_wal_diagnostic import BackendWalDiagnosticResult
    from process.provider_directory_owned_wal_transaction import OwnedWalTransaction

    custody_by_field = lease.owned_custody
    current_window = lease.fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
    acquisition_window = None if custody_by_field is None else custody_by_field["acquisition_window"]
    measured = (
        isinstance(outcome, OwnedWalTransaction)
        and outcome.is_committed
        and outcome.cleanup_complete
        and outcome.status == "committed_measured"
        and isinstance(outcome.measurement, BackendWalDiagnosticResult)
        and outcome.measurement.baseline.identity == outcome.measurement.final.identity
        and outcome.retained_pid == outcome.measurement.final.identity.pid
    )
    matched = (
        measured
        and isinstance(custody_by_field, dict)
        and lease.admission.wal_tracker.owned_control_maintenance_custody is custody_by_field
        and custody_by_field["lease"] is lease
        and custody_by_field["task"] is asyncio.current_task() is lease.owner
        and custody_by_field["connection"] is lease.connection is outcome.connection
        and custody_by_field["admission"] is lease.admission
        and custody_by_field["original_outcome"] is outcome is custody_by_field["outcome"]
        and custody_by_field["retained_driver"] is outcome.retained_driver
        and outcome.retained_driver is not None
        and type(custody_by_field["retained_pid"]) is int
        and custody_by_field["retained_pid"] == outcome.retained_pid
        and isinstance(acquisition_window, tuple)
        and len(acquisition_window) == 2
        and acquisition_window[1] is None
        and isinstance(current_window, tuple)
        and len(current_window) == 2
        and current_window[1] is None
        and custody_by_field["terminal_window"] is current_window
        and current_window is not acquisition_window
        and custody_by_field["consumed"] is False
    )
    if not measured or not matched:
        raise RuntimeError("provider_directory_profile_maintenance_custody_incomplete")
    identity_outcome = _maintenance_identity_check_outcome(lease, custody_by_field)
    body_upper = _maintenance_body_physical_wal_upper(lease, (outcome, identity_outcome))
    custody_by_field["ordinary_body_physical_wal_upper_bytes"] = body_upper
    custody_by_field["ordinary_body_native_outcomes"] = (outcome, identity_outcome)
    # Preparation/restoration and sampling tails are outside these observations.
    custody_by_field["whole_owner_wal_readiness"] = "incomplete"
    custody_by_field["status"] = "complete_long_lived_mixed_unclassified"
    custody_by_field["consumed"] = True


def _retain_owned_outcome(lease, outcome, failure):
    """Retain actual custody before any diagnostic serialization can fail."""
    lease.owned_outcome = outcome
    lease.admission.wal_tracker.owned_control_maintenance_outcome = outcome
    _retain_maintenance_origin(lease, outcome)
    if lease.owned_custody is not None:
        lease.owned_custody["outcome"] = outcome
        lease.owned_custody["terminal_window"] = lease.fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
    if outcome is not None:
        try:
            lease.fhir._profile_owned_transaction_capture(outcome, failure)
        except BaseException:
            if failure is None:
                raise
            failure.add_note("provider_directory_profile_maintenance_capture_failed")
    if failure is None and lease.owned_custody is not None:
        _consume_owned_maintenance_custody(lease, outcome)


async def _finish_owned_lease(lease, failure=None):
    """Exit exactly once at release or failure and preserve the primary error."""
    context, lease.owned_context = lease.owned_context, None
    if context is None:
        return
    lease.owned_finished = True
    try:
        await context.__aexit__(
            type(failure) if failure is not None else None,
            failure,
            failure.__traceback__ if failure is not None else None,
        )
    except BaseException as caught:
        _retain_owned_outcome(lease, getattr(caught, "outcome", lease.owned_outcome), caught)
        if failure is None:
            raise
    else:
        _retain_owned_outcome(lease, lease.owned_outcome, failure)
    finally:
        if lease.owned_outcome is not None and lease.owned_outcome.is_committed:
            lease.released = True


async def release_before_cutover(fhir):
    """Release the maintenance transaction before stronger publication locks."""
    lease = _ACTIVE.get()
    if lease is None or lease.released:
        return
    if lease.fhir is not fhir or lease.owner is not asyncio.current_task():
        raise RuntimeError("provider_directory_profile_maintenance_owner_changed")
    async with fhir._profile_capacity_mutation_window(None):
        await fhir._reserve_provider_directory_profile_wal_budget(
            lease.admission, control_operation_counts={"control_maintenance_release": 1}
        )
        async with asyncio.timeout(await fhir._profile_capacity_remaining_ms(lease.admission) / 1000):
            if lease.owned_context is not None:
                await _finish_owned_lease(lease)
            else:
                await lease.connection.commit()
    lease.released = True


def _coordinates(fhir, connection, admission):
    """Bind server-selected names, physical identities and the existing pool."""
    geometry = admission.geometry
    if (
        geometry.sql_contract_digest != fhir._provider_directory_profile_sql_contract_digest()
        or geometry.database_pool_size != fhir._provider_directory_database_pool_capacity()
        or connection.in_transaction()
    ):
        raise RuntimeError("provider_directory_profile_maintenance_binding_changed")
    from process.provider_directory_profile_capacity_geometry import _assert_wave_geometry

    _assert_wave_geometry(geometry)
    pool = fhir.db.engine.pool
    if pool.size() + pool._max_overflow != geometry.database_pool_size:
        raise RuntimeError("provider_directory_profile_maintenance_pool_changed")
    schema = fhir._schema()
    identity = admission.database_identity
    refs_by_name = {
        "build_checkpoint": fhir._provider_directory_profile_checkpoint_ref(schema),
        "serving_generation": fhir._provider_directory_profile_serving_generation_ref(schema),
        "delta_receipt": fhir._provider_directory_profile_delta_receipt_ref(schema),
        "import_run": fhir._unscoped_qt(schema, fhir.ImportRun.__tablename__),
        "capacity_consumption": fhir._unscoped_qt(
            schema, fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__
        ),
    }
    coordinates = sorted((getattr(identity, name + "_oid"), ref) for name, ref in refs_by_name.items())
    if len({oid for oid, _ in coordinates}) != 5 or any(
        type(oid) is not int or not 0 < oid < 2**32 for oid, _ in coordinates
    ):
        raise RuntimeError("provider_directory_profile_maintenance_identity_invalid")
    if any(getattr(geometry, name + "_oid") != getattr(identity, name + "_oid") for name in refs_by_name):
        raise RuntimeError("provider_directory_profile_maintenance_identity_changed")
    return coordinates


async def _check_admitted_identity(fhir, admission, lease):
    """Retain the original callback group before the acquisition window clears."""
    identity = None if lease is None or lease.owned_custody is None else lease.owned_custody["identity_check_identity"]
    transaction = (
        fhir._provider_directory_profile_capacity_transaction(control_identity=identity)
        if identity is not None
        else fhir._provider_directory_profile_capacity_transaction()
    )
    async with transaction:
        if identity is not None:
            groups = [
                group
                for group in admission.wal_tracker.owned_control_transaction_groups
                if group["identity"] is identity
            ]
            if len(groups) != 1:
                raise RuntimeError("provider_directory_profile_maintenance_identity_custody_incomplete")
            lease.owned_custody["identity_check_group"] = groups[0]
            lease.owned_custody["original_identity_check_group"] = groups[0]
        await fhir._admission_database_guard(admission.admitted_identity, admission.database_identity)


async def _acquire(fhir, connection, admission, coordinates, *, lease=None):
    geometry = admission.geometry
    async with fhir._profile_capacity_mutation_window(None):
        await fhir._reserve_provider_directory_profile_wal_budget(
            admission, control_operation_counts={"control_maintenance_acquire": 1}
        )
        remaining = await fhir._profile_capacity_remaining_ms(admission)
        async with asyncio.timeout(remaining / 1000):
            if lease is not None and admission.geometry.bounded_admission:
                _begin_owned_maintenance_custody(lease)
                lease.owned_context = fhir.profile_owned_wal.registry_retained_wal_transaction(connection)
                try:
                    lease.owned_outcome = await lease.owned_context.__aenter__()
                    _retain_maintenance_origin(lease, lease.owned_outcome)
                except BaseException as failure:
                    lease.owned_context = None
                    lease.owned_finished = True
                    _retain_owned_outcome(lease, getattr(failure, "outcome", None), failure)
                    raise
            else:
                await connection.begin()
            for name, setting_ms in (
                ("transaction_timeout", remaining),
                ("statement_timeout", min(remaining, geometry.statement_timeout_ms)),
                ("lock_timeout", min(remaining, geometry.lock_timeout_ms)),
            ):
                await connection.execute(
                    text("SELECT set_config(:name, :value, true)"), {"name": name, "value": f"{setting_ms}ms"}
                )
            identity_result = await connection.execute(
                text(
                    "SELECT count(*)::integer FROM unnest(CAST(:refs AS text[]), CAST(:oids AS bigint[])) "
                    "AS expected(ref, oid) JOIN pg_class AS c ON c.oid = expected.oid "
                    "AND c.oid = to_regclass(expected.ref) JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relkind = 'r' AND c.relpersistence = 'p' "
                    "AND current_database() = :database"
                ),
                {
                    "refs": [ref for _, ref in coordinates],
                    "oids": [oid for oid, _ in coordinates],
                    "schema": fhir._schema(),
                    "database": admission.database_identity.database_name,
                },
            )
            if identity_result.scalar_one() != 5:
                raise RuntimeError("provider_directory_profile_maintenance_relation_changed")
            await connection.execute(
                text(
                    "LOCK TABLE " + ", ".join(ref for _, ref in coordinates) + " IN SHARE UPDATE EXCLUSIVE MODE NOWAIT"
                )
            )
            await _check_admitted_identity(fhir, admission, lease)


@asynccontextmanager
async def control_maintenance_fence(fhir, connection):
    """Reuse the independently owned artifact-scope advisory connection."""
    admission = fhir._provider_directory_profile_capacity_admission()
    if admission is None:
        yield
        return
    if _ACTIVE.get() is not None:
        raise RuntimeError("provider_directory_profile_maintenance_nested")
    coordinates = _coordinates(fhir, connection, admission)
    lease = _Lease(fhir, connection, admission, asyncio.current_task(), coordinates=tuple(coordinates))
    token = _ACTIVE.set(lease)
    try:
        await _acquire(fhir, connection, admission, coordinates, lease=lease)
        lease.has_acquired = True
        yield
        await release_before_cutover(fhir)
    except BaseException as failure:
        if lease.owned_context is not None:
            await _finish_owned_lease(lease, failure)
        raise
    finally:
        try:
            if not lease.released and not lease.owned_finished:
                await _rollback_drained(lease)
        finally:
            _ACTIVE.reset(token)
