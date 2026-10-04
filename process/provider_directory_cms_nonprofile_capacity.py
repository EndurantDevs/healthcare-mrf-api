# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify independent aggregate reservations using the existing capacity authority.

The authority must issue a distinct nonprofile geometry and fresh signed phase
observations. Profile-only operator input and unsigned storage callbacks cannot
produce this admission. Original consumed lease bytes remain immutable.
"""

from __future__ import annotations

import datetime
import importlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from typing import Any

from process import provider_directory_profile_capacity_runtime as capacity_runtime
from process import provider_directory_profile_initial as profile_initial
from process.provider_directory_cms_preparation import (
    NonprofileAdmission,
    NonprofileAdmissionCheck,
    NonprofileAdmissionPlan,
    NonprofileAdmissionReceipt,
    OwnedRelation,
    desired_fence_hash,
)
from process.provider_directory_cms_storage_continuation import (
    StorageContinuationRequest,
    VerifiedStorageContinuation,
    request_storage_continuation,
    verify_storage_continuation,
)
from process.provider_directory_profile_capacity_attestation import (
    CapacityLeaseConsumptionBinding,
    VerifiedDatabaseCapacityLease,
    assert_database_capacity_lease_reservation,
    capacity_lease_consumption_values,
    verify_database_capacity_lease,
)
from process.provider_directory_profile_runtime_observation import (
    assert_capacity_lease_matches_runtime_observation,
    observe_profile_runtime,
)

_PURPOSE = "cms_nonprofile"
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")
_PHASES = frozenset({"pre_scratch", "pre_logging", "readiness", "cutover"})


@dataclass(frozen=True)
class PausedProfileCapacity:
    """Retain exact admitted identity, tracker and measured admission WAL."""

    admission: Any
    spent_wal_bytes: int


async def pause_profile_capacity(fhir: Any, admission: Any) -> PausedProfileCapacity:
    """Measure own admission WAL before separately budgeted full artifacts begin."""
    async with admission.wal_tracker.lock:
        spent = await fhir._provider_directory_profile_current_wal_bytes(admission)
        if type(spent) is not int or spent < 0 or spent > admission.geometry.reservation_bytes_by_storage_class["wal"]:
            raise _error("profile_admission_wal_invalid")
    return PausedProfileCapacity(admission, spent)


async def resume_profile_capacity(
    fhir: Any, paused: PausedProfileCapacity, execution: Any, fence: Any, resource_fence: Any, types: frozenset[str]
) -> Any:
    """Recheck exact capacity and preserve all own WAL while resuming its window."""
    admission = paused.admission
    identity = await fhir._profile_admission_identity(
        fence, serving_state=profile_initial.targets(admission.admitted_identity)
    )
    if identity != admission.admitted_identity:
        raise _error("profile_build_identity_changed")
    workload = await fhir._profile_admission_workload(identity, fence, resource_fence, types)
    geometry = fhir._profile_admission_geometry(workload, fhir._profile_admission_inputs(execution, identity, workload))
    if geometry.geometry != admission.geometry or geometry.control_wal_projection != admission.control_wal_projection:
        raise _error("profile_geometry_changed")
    async with fhir.db.transaction():
        if identity.initial_targets is not None:
            await fhir._lock_profile_capacity_preflight_state(fhir._schema())
        await fhir.assert_profile_selection_current_in_transaction(
            execution.attestation, fhir._provider_directory_profile_selection_catalog()
        )
        await fhir._lock_and_verify_artifact_dataset_fence(fence)
        await _assert_resumed_profile_state(fhir, identity, admission.geometry)
        database_identity, _runtime = await fhir._profile_admission_runtime_state(
            identity,
            admission.database_identity,
            admission.geometry,
            admission.lease,
        )
        await fhir._provider_directory_profile_capacity_acceptance_time(admission.lease)
        async with admission.wal_tracker.lock:
            resumed = replace(
                admission, initial_wal_lsn=database_identity.wal_lsn, initial_wal_offset_bytes=paused.spent_wal_bytes
            )
    await fhir._assert_provider_directory_profile_wal_budget(resumed)
    return resumed


async def _assert_resumed_profile_state(fhir: Any, identity: Any, geometry: Any) -> None:
    """Recheck the original predecessor or initial physical snapshot under its locks."""
    if identity.initial_targets is None:
        observed_serving = await fhir._provider_directory_profile_serving_state(fhir._schema(), for_update=True)
        fhir._assert_provider_directory_profile_capacity_serving_state(identity.serving_state, observed_serving)
        return
    observed_targets = await profile_initial.capture_targets(fhir, fhir._schema())
    receipt_layout = await profile_initial.receipt_layout(fhir, fhir._schema())
    if (
        observed_targets != identity.initial_targets
        or receipt_layout.relation_oid != geometry.initial_receipt_oid
        or receipt_layout.exact_fingerprint != geometry.initial_receipt_storage_fingerprint
    ):
        raise _error("profile_initial_target_changed")


def _error(reason: str) -> RuntimeError:
    """Use neutral closed failure names for every fail-closed boundary."""
    return RuntimeError("provider_directory_nonprofile_capacity_" + reason)


def _validate_plan(plan: NonprofileAdmissionPlan) -> None:
    """Require pinned native inputs, signed phase budgets and bounded temp costs."""
    if not isinstance(plan, NonprofileAdmissionPlan):
        raise _error("plan_required")
    hash_fields = (
        plan.selection_proof_id,
        plan.desired_fence_hash,
        plan.artifact_scope_projection_hash,
        plan.native_address_input_hash,
        plan.paired_profile_lease_digest,
    )
    if any(not isinstance(plan_value, str) or not _HASH.fullmatch(plan_value) for plan_value in hash_fields):
        raise _error("plan_identity_invalid")
    for scope_names in (plan.publish_targets, plan.resource_types, plan.native_address_targets):
        if (
            not scope_names
            or scope_names != tuple(sorted(set(scope_names)))
            or any(
                not isinstance(plan_value, str) or not _IDENTIFIER.fullmatch(plan_value) for plan_value in scope_names
            )
        ):
            raise _error("plan_scope_invalid")
    positive_values = (
        plan.batch_size,
        plan.worker_count,
        plan.minimum_remaining_bytes,
        plan.required_build_seconds,
        plan.temp_file_limit_bytes_per_backend,
        plan.logging_wal_upper_bound_bytes,
        plan.cutover_wal_upper_bound_bytes,
    )
    if any(type(plan_value) is not int or not 0 < plan_value < 2**63 for plan_value in positive_values):
        raise _error("plan_budget_invalid")
    reservation_by_class = dict(plan.reservation_bytes)
    if (
        tuple(reservation_by_class) != ("data", "temp", "wal")
        or len(plan.reservation_bytes) != 3
        or any(
            type(plan_value) is not int or not 0 < plan_value < 2**63 for plan_value in reservation_by_class.values()
        )
    ):
        raise _error("plan_budget_invalid")
    if plan.temp_file_limit_bytes_per_backend % 1024 or (
        plan.worker_count * plan.temp_file_limit_bytes_per_backend > reservation_by_class["temp"]
        or plan.logging_wal_upper_bound_bytes + plan.cutover_wal_upper_bound_bytes > reservation_by_class["wal"]
    ):
        raise _error("plan_budget_invalid")


async def _database_observation(fhir: Any, plan: NonprofileAdmissionPlan) -> dict[str, Any]:
    """Read effective limits on the actual bounded backend, preserving any caller settings."""
    native = importlib.import_module("process.entity_address_unified")

    settings = (
        ("temp_file_limit", f"{plan.temp_file_limit_bytes_per_backend // 1024}kB"),
        ("max_parallel_workers_per_gather", "0"),
        ("max_parallel_maintenance_workers", "0"),
    )
    async with native.entity_address_tuned_transaction(fhir.db, settings, native._sql_literal, native.logger):
        observation = await _read_database_observation(fhir)
        if (
            observation["query_parallel_workers"]
            or observation["maintenance_parallel_workers"]
            or (
                type(observation["temp_limit_bytes"]) is not int
                or not 0 < observation["temp_limit_bytes"] <= plan.temp_file_limit_bytes_per_backend
            )
        ):
            raise _error("execution_limits_changed")
        return observation


async def _read_database_observation(fhir: Any) -> dict[str, Any]:
    """Read native database, effective tablespaces and actual session temp limits."""
    row = await fhir.db.first("""
        SELECT control.system_identifier::text AS database_system_identifier,
               d.oid::bigint AS database_oid, d.datname AS database_name,
               d.dattablespace::bigint AS data_tablespace_oid, data.spcname AS data_tablespace_name,
               temp.oid::bigint AS temp_tablespace_oid, temp.spcname AS temp_tablespace_name,
               pg_current_wal_insert_lsn()::text AS wal_lsn,
               current_setting('temp_file_limit') AS temp_limit,
               current_setting('max_parallel_workers_per_gather')::integer AS query_parallel_workers,
               current_setting('max_parallel_maintenance_workers')::integer AS maintenance_parallel_workers
          FROM pg_database d CROSS JOIN pg_control_system() control
          JOIN pg_tablespace data ON data.oid=d.dattablespace
          JOIN pg_tablespace temp ON temp.spcname=COALESCE(NULLIF(current_setting('temp_tablespaces'),''),data.spcname)
         WHERE d.datname=current_database();
    """)
    if row is None:
        raise _error("database_observation_missing")
    values_by_field = dict(fhir._pagination_checkpoint_row_mapping(row))
    if values_by_field.get("temp_limit") == "-1":
        raise _error("temp_limit_unbounded")
    values_by_field["temp_limit_bytes"] = await fhir.db.scalar(
        "SELECT pg_size_bytes(:limit)::bigint;",
        limit=values_by_field["temp_limit"],
    )
    return values_by_field


def _verify_envelope(
    envelope: Mapping[str, Any], geometry_hash: str, observation: Mapping[str, Any], now: datetime.datetime
) -> VerifiedDatabaseCapacityLease:
    """Independently verify a complete existing signed envelope against local identity."""
    return verify_database_capacity_lease(
        envelope,
        trust=capacity_runtime.configured_capacity_lease_trust(),
        now=now,
        expected_capacity_geometry_hash=geometry_hash,
        expected_database_system_identifier=observation["database_system_identifier"],
        expected_database_oid=observation["database_oid"],
        expected_database_name=observation["database_name"],
    )


def _assert_signed_plan(
    fhir: Any, execution: Any, plan: NonprofileAdmissionPlan, lease: VerifiedDatabaseCapacityLease
) -> None:
    """Reject a Profile geometry or signed declaration differing from the exact plan."""
    receipt = lease.signing_preflight_guard["healthcare_receipt"]
    if receipt["capacity_geometry"] != plan.payload or (
        receipt["profile_execution_identity"] != fhir._profile_capacity_expected_execution_identity(execution)
        or receipt["required_reservation_bytes_by_storage_class"] != dict(plan.reservation_bytes)
        or receipt["artifact_scope_projection"].get("projection_hash") != plan.artifact_scope_projection_hash
    ):
        raise _error("signed_plan_changed")


def _assert_runtime_storage(
    plan: NonprofileAdmissionPlan, lease: VerifiedDatabaseCapacityLease, observation: Mapping[str, Any]
) -> None:
    """Require finite effective temp limits and no unbudgeted parallel backends."""
    if (
        observation["query_parallel_workers"] != 0
        or observation["maintenance_parallel_workers"] != 0
        or (
            type(observation["temp_limit_bytes"]) is not int
            or not 0 < observation["temp_limit_bytes"] <= plan.temp_file_limit_bytes_per_backend
        )
    ):
        raise _error("execution_limits_changed")
    expected_by_usage = {
        usage: (observation[usage + "_tablespace_oid"], observation[usage + "_tablespace_name"])
        for usage in ("data", "temp")
    }
    if {entry.usage: (entry.tablespace_oid, entry.tablespace_name) for entry in lease.tablespaces} != expected_by_usage:
        raise _error("tablespaces_changed")
    if any(
        observation[name] != getattr(lease, name)
        for name in (
            "database_system_identifier",
            "database_oid",
            "database_name",
        )
    ):
        raise _error("database_changed")


def _assert_paired_reservation(
    nonprofile: VerifiedDatabaseCapacityLease,
    profile: VerifiedDatabaseCapacityLease,
    minimum: int,
    *,
    storage: VerifiedStorageContinuation | None = None,
) -> None:
    """Account both independent reservations on each possibly colocated volume."""
    if nonprofile.reservation_id == profile.reservation_id or nonprofile.attestation_id == profile.attestation_id:
        raise _error("profile_reservation_reused")
    nonprofile_volumes_by_class = {volume.volume_class: volume.volume_digest for volume in nonprofile.volumes}
    profile_volumes_by_class = {volume.volume_class: volume.volume_digest for volume in profile.volumes}
    if (
        nonprofile.tablespace_identity_hash != profile.tablespace_identity_hash
        or nonprofile_volumes_by_class != profile_volumes_by_class
    ):
        raise _error("paired_storage_changed")
    reserved_by_volume: dict[str, int] = {}
    for lease in (nonprofile, profile):
        for volume in lease.volumes:
            reserved_by_volume[volume.volume_digest] = (
                reserved_by_volume.get(volume.volume_digest, 0) + volume.reserved_bytes
            )
    observations = (
        storage.volume_observations
        if storage is not None
        else tuple(
            (
                volume.volume_class,
                volume.volume_digest,
                volume.available_bytes,
                volume.available_after_all_reservations_bytes,
            )
            for volume in nonprofile.volumes
        )
    )
    for _class, digest, available, remaining in observations:
        if available < reserved_by_volume[digest] + minimum or remaining < minimum:
            raise _error("aggregate_remaining_capacity_too_small")


@dataclass
class _CapacityProducer:
    """Hold original signed bytes and verify untrusted fresh transport on every phase."""

    fhir: Any
    execution: Any
    fence: Any
    run_id: str
    lease: VerifiedDatabaseCapacityLease
    profile_lease: VerifiedDatabaseCapacityLease
    plan: NonprofileAdmissionPlan
    fresh_storage_envelope: Callable[[StorageContinuationRequest], Awaitable[Mapping[str, Any]]]
    initial_wal_lsn: str
    consumption_by_field: dict[str, Any] = field(default_factory=dict)
    last_observed_at: datetime.datetime | None = None
    continuation_digests: list[str] = field(default_factory=list)
    paused_profile: PausedProfileCapacity | None = None
    preparation_wal_end_bytes: int | None = None
    resumed_profile: Any = None
    cutover_wal_start_bytes: int | None = None
    _cutover_active: bool = field(default=False, init=False)
    _cutover_request: NonprofileAdmissionCheck | None = field(default=None, init=False)
    _cutover_witness: VerifiedStorageContinuation | None = field(default=None, init=False)
    _cutover_observation: Mapping[str, Any] | None = field(default=None, init=False)
    _cutover_binding: Any = field(default=None, init=False)

    @asynccontextmanager
    async def cutover_operation(self):
        """Keep one verified witness only for the owner's next atomic transaction."""
        if self._cutover_active or self.fhir.db._transaction_binding() is not None:
            raise _error("cutover_operation_changed")
        self._cutover_active = True
        try:
            yield
        finally:
            self._cutover_active = False
            self._cutover_request = self._cutover_witness = self._cutover_observation = self._cutover_binding = None

    async def assert_cutover(self, relations: tuple[OwnedRelation, ...] | None) -> None:
        """Recheck the same transaction locally without another authority request."""
        request, witness = self._cutover_request, self._cutover_witness
        binding = self.fhir.db._transaction_binding()
        if (
            not self._cutover_active
            or request is None
            or witness is None
            or binding is None
            or self._cutover_observation is None
            or request.lease != self.lease
            or request.plan != self.plan
            or self.profile_lease.lease_digest != self.plan.paired_profile_lease_digest
        ):
            raise _error("cutover_operation_changed")
        if self._cutover_binding is None:
            self._cutover_binding = binding
        if self._cutover_binding is not binding:
            raise _error("cutover_operation_changed")
        await self._assert_consumption()
        if relations is None:
            await self._assert_wal_budget(request)
        else:
            if tuple((relation.schema, relation.relation, relation.oid) for relation in relations) != tuple(
                (relation.schema, relation.relation, relation.oid) for relation in request.relations
            ):
                raise _error("cutover_relations_changed")
            await self._assert_physical(replace(request, relations=relations), self._cutover_observation)
        now = await self.fhir.db.scalar("SELECT clock_timestamp();")
        if not isinstance(now, datetime.datetime) or now.tzinfo is None:
            raise _error("continuation_expired_during_check")
        if now >= min(self.lease.max_build_deadline, self.profile_lease.max_build_deadline):
            raise _error("paired_profile_deadline_reached")
        if now >= witness.expires_at:
            raise _error("continuation_expired_during_check")

    async def pause_profile(self, admission: Any) -> PausedProfileCapacity:
        """Verify the paired admission before suspending its measured WAL window."""
        if (
            self.paused_profile is not None
            or admission.run_id != self.run_id
            or admission.lease.lease_digest != self.profile_lease.lease_digest
            or getattr(admission, "initial_wal_offset_bytes", 0) != 0
        ):
            raise _error("paired_profile_admission_changed")
        self.paused_profile = await pause_profile_capacity(self.fhir, admission)
        return self.paused_profile

    async def resume_profile(self, paused: PausedProfileCapacity, resource_fence: Any, types: frozenset[str]) -> Any:
        """Seal CMS preparation at the unchanged Profile meter's exact starting LSN."""
        if paused is not self.paused_profile or self.resumed_profile is not None:
            raise _error("paired_profile_admission_changed")
        resumed = await resume_profile_capacity(self.fhir, paused, self.execution, self.fence, resource_fence, types)
        preparation_wal = await self._current_wal_bytes(end_lsn=resumed.initial_wal_lsn)
        self._remaining_logging_wal("readiness", preparation_wal)
        self.preparation_wal_end_bytes = preparation_wal
        self.resumed_profile = resumed
        return resumed

    async def check_phase(self, request: NonprofileAdmissionCheck) -> NonprofileAdmissionReceipt:
        """Consume once and recheck signed storage, runtime, identity and WAL."""
        if request.phase not in _PHASES or request.lease != self.lease or request.plan != self.plan:
            raise _error("phase_identity_changed")
        if self._cutover_active and (
            self.fhir.db._transaction_binding() is not None or self._cutover_witness is not None
        ):
            raise _error("cutover_operation_changed")
        started_at = await self.fhir._profile_capacity_preflight_clock()
        storage_request = request_storage_continuation(
            self.lease, run_id=self.run_id, phase=request.phase, requested_at=started_at
        )
        envelope = await self.fresh_storage_envelope(storage_request)
        observation = await _database_observation(self.fhir, self.plan)
        now = await self.fhir._profile_capacity_preflight_clock()
        fresh = verify_storage_continuation(
            envelope,
            request=storage_request,
            lease=self.lease,
            trust=capacity_runtime.configured_capacity_lease_trust(),
            now=now,
        )
        self._assert_refresh(fresh, now)
        _assert_runtime_storage(self.plan, self.lease, observation)
        _assert_paired_reservation(self.lease, self.profile_lease, self.plan.minimum_remaining_bytes, storage=fresh)
        runtime = await observe_profile_runtime(self.fhir.db)
        assert_capacity_lease_matches_runtime_observation(self.lease, runtime)
        assert_capacity_lease_matches_runtime_observation(self.profile_lease, runtime)
        if request.phase == "pre_scratch":
            await self._consume(runtime)
        else:
            await self._assert_consumption()
        await self._assert_physical(request, observation)
        if await self.fhir._profile_capacity_preflight_clock() >= fresh.expires_at:
            raise _error("continuation_expired_during_check")
        self.last_observed_at = fresh.observed_at
        self.continuation_digests.append(fresh.witness_digest)
        if request.phase == "cutover" and self._cutover_active:
            if self._cutover_witness is not None:
                raise _error("cutover_operation_changed")
            self._cutover_request, self._cutover_witness, self._cutover_observation = request, fresh, observation
        return NonprofileAdmissionReceipt(
            request.phase,
            self.lease.lease_digest,
            self.lease.reservation_id,
            self.plan.capacity_geometry_hash,
            request.relations,
            request.logging_relations,
        )

    def _assert_refresh(self, fresh: VerifiedStorageContinuation, now: datetime.datetime) -> None:
        """Demand newly observed authority data without changing the original ledger row."""
        if fresh.witness_digest in self.continuation_digests or (
            self.last_observed_at is not None and fresh.observed_at < self.last_observed_at
        ):
            raise _error("fresh_observation_required")
        if now >= min(self.profile_lease.max_build_deadline, self.lease.max_build_deadline):
            raise _error("paired_profile_deadline_reached")

    async def _consume(self, runtime: Mapping[str, Any]) -> None:
        """Persist exact preflight and lease use under existing global admission locks."""
        if self.consumption_by_field:
            raise _error("same_run_replay_unsupported")
        fhir, schema = self.fhir, self.fhir._schema()
        async with fhir.db.transaction():
            await fhir.db.status("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            await fhir._lock_profile_capacity_preflight_state(schema)
            await fhir._lock_provider_directory_profile_capacity_control_run(schema=schema, run_id=self.run_id)
            await fhir.assert_profile_selection_current_in_transaction(
                self.execution.attestation,
                fhir._provider_directory_profile_selection_catalog(),
            )
            await fhir._lock_and_verify_artifact_dataset_fence(self.fence)
            observed_runtime = await observe_profile_runtime(fhir.db)
            assert_capacity_lease_matches_runtime_observation(self.lease, observed_runtime)
            if dict(observed_runtime) != dict(runtime):
                raise _error("runtime_changed_during_admission")
            await self._consume_preflight(runtime)
            accepted_at = await fhir._provider_directory_profile_capacity_acceptance_time(self.lease)
            binding = CapacityLeaseConsumptionBinding(
                self.run_id,
                "pdpb_" + self.plan.capacity_geometry_hash[:32],
                self.plan.capacity_geometry_hash,
                self.plan.selection_proof_id,
                self.plan.desired_fence_hash,
                self.execution.attestation.source_context_digest,
                self.plan.desired_profile_as_of,
            )
            values_by_field = {
                "admission_purpose": _PURPOSE,
                **capacity_lease_consumption_values(self.lease, binding, accepted_at=accepted_at),
            }
            await fhir._consume_provider_directory_profile_capacity_lease(schema=schema, values_by_name=values_by_field)
        self.consumption_by_field = values_by_field

    async def _consume_preflight(self, runtime: Mapping[str, Any]) -> None:
        """Exclude only the separately verified pending paired Profile receipt."""
        fhir, schema = self.fhir, self.fhir._schema()
        row = await fhir.db.first(
            f"SELECT * FROM {fhir._profile_capacity_preflight_receipt_ref(schema)} "
            "WHERE receipt_sha256=:receipt_sha256 FOR UPDATE;",
            receipt_sha256=self.lease.nonce,
        )
        if row is None:
            raise _error("preflight_missing")
        receipt_row = fhir._pagination_checkpoint_row_mapping(row)
        receipt = fhir._profile_capacity_preflight_stored_receipt(receipt_row, self.lease)
        if receipt != self.lease.signing_preflight_guard["healthcare_receipt"]:
            raise _error("preflight_changed")
        now = await fhir._profile_capacity_preflight_clock()
        fhir._assert_profile_capacity_receipt_open(receipt_row, self.lease, now)
        await fhir._assert_profile_capacity_receipt_storage(schema, receipt)
        await self._assert_paired_preflight()
        query = fhir._profile_capacity_quiescence_sql(schema).replace(
            "AND request_sha256 <> :request_sha256",
            "AND request_sha256 <> :request_sha256 AND request_sha256 <> :paired_request_sha256",
        )
        params_by_name = _quiescence_parameters(fhir, self, receipt, now)
        counters = await fhir.db.first(query, **params_by_name)
        if counters is None or any(value != 0 for value in fhir._pagination_checkpoint_row_mapping(counters).values()):
            raise _error("competing_admission")
        if receipt["runtime_observation"] != dict(runtime):
            raise _error("preflight_runtime_changed")
        await fhir._mark_profile_capacity_receipt_consumed(schema, self.run_id, self.lease, now)

    async def _assert_paired_preflight(self) -> None:
        """Permit exactly one open, durable, signed Profile receipt for this selection."""
        receipt = self.profile_lease.signing_preflight_guard["healthcare_receipt"]
        row = await self.fhir.db.first(
            f"SELECT * FROM {self.fhir._profile_capacity_preflight_receipt_ref(self.fhir._schema())} "
            "WHERE receipt_sha256=:receipt_sha256 FOR UPDATE;",
            receipt_sha256=self.profile_lease.nonce,
        )
        if row is None:
            raise _error("paired_preflight_missing")
        values_by_field = self.fhir._pagination_checkpoint_row_mapping(row)
        stored = self.fhir._profile_capacity_preflight_stored_receipt(values_by_field, self.profile_lease)
        if stored != receipt or stored[
            "profile_execution_identity"
        ] != self.fhir._profile_capacity_expected_execution_identity(self.execution):
            raise _error("paired_preflight_changed")
        self.fhir._assert_profile_capacity_receipt_open(
            values_by_field, self.profile_lease, await self.fhir._profile_capacity_preflight_clock()
        )

    async def _assert_consumption(self) -> None:
        """Require the same immutable full original lease bytes for every later phase."""
        if not self.consumption_by_field:
            raise _error("consumption_missing")
        table = self.fhir._unscoped_qt(
            self.fhir._schema(), self.fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__
        )
        rows = await self.fhir.db.all(
            f"SELECT * FROM {table} WHERE run_id=:run_id AND admission_purpose='cms_nonprofile';", run_id=self.run_id
        )
        identities = [
            self.fhir._provider_directory_profile_capacity_consumption_identity(
                self.fhir._pagination_checkpoint_row_mapping(row)
            )
            for row in rows
        ]
        if identities != [
            self.fhir._provider_directory_profile_capacity_consumption_identity(self.consumption_by_field)
        ]:
            raise _error("consumption_changed")

    async def _assert_physical(self, request: NonprofileAdmissionCheck, observation: Mapping[str, Any]) -> None:
        """Measure aggregate native relations and conservative cluster-wide emitted WAL."""
        data_bytes = sum(relation.total_bytes for relation in request.relations)
        if data_bytes > dict(self.plan.reservation_bytes)["data"]:
            raise _error("data_budget_exceeded")
        await self._assert_wal_budget(request)
        from process import provider_directory_cms_archive as archive
        from process import provider_directory_cms_native_layout as native_layout

        for relation in request.relations:
            if archive.is_archive_relation(relation.relation):
                layout = await archive.capture_archive_layout(self.fhir, relation)
            elif native_layout.is_native_relation(relation.relation, self.plan.native_address_targets):
                layout = await native_layout.capture_native_layout(
                    self.fhir, relation, self.plan.native_address_targets
                )
            else:
                layout = await self.fhir._provider_directory_profile_relation_storage_fingerprint(
                    relation.oid, expected_persistence=relation.persistence
                )
            if layout.relation_oid != relation.oid or layout.effective_tablespace_oids != (
                observation["data_tablespace_oid"],
            ):
                raise _error("physical_tablespace_changed")

    async def _assert_wal_budget(self, request: NonprofileAdmissionCheck) -> None:
        """Spend each CMS phase once, excluding only the separately admitted Profile window."""
        wal_bytes = await self._current_wal_bytes()
        if self.resumed_profile is not None:
            profile = self.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get()
            if profile is not self.resumed_profile:
                raise _error("paired_profile_admission_changed")
            await self._assert_active_profile(profile)
            if self.cutover_wal_start_bytes is None:
                await self.fhir._assert_provider_directory_profile_wal_budget(profile)
                if request.phase == "cutover":
                    self.cutover_wal_start_bytes = wal_bytes
        elif request.phase == "cutover":
            raise _error("paired_profile_preparation_required")
        elif self.paused_profile is not None:
            await self._assert_active_profile(self.paused_profile.admission)
        remaining_logging = self._remaining_logging_wal(request.phase, wal_bytes)
        if request.phase == "pre_logging":
            measured_by_coordinate = {(relation.schema, relation.relation): relation for relation in request.relations}
            logging_relations = request.logging_relations
            if (
                not logging_relations
                or len(set(logging_relations)) != len(logging_relations)
                or any(
                    name not in measured_by_coordinate or measured_by_coordinate[name].persistence != "u"
                    for name in logging_relations
                )
            ):
                raise _error("logging_targets_invalid")
            # Reserve three measured copies; validate headroom for the largest logging operation.
            # This operational margin is not a universal rewrite-WAL prediction.
            rewrite_bytes = sum(measured_by_coordinate[name].total_bytes for name in logging_relations)
            if 3 * rewrite_bytes > remaining_logging:
                raise _error("measured_logging_reserve_exceeded")
        elif request.logging_relations:
            raise _error("logging_targets_invalid")
        preparation_wal = self.preparation_wal_end_bytes
        if preparation_wal is None:
            preparation_wal = wal_bytes
        if self.paused_profile is not None:
            preparation_wal -= self.paused_profile.spent_wal_bytes
        cutover_wal = 0 if self.cutover_wal_start_bytes is None else wal_bytes - self.cutover_wal_start_bytes
        if not 0 <= cutover_wal <= self.plan.cutover_wal_upper_bound_bytes:
            raise _error("cutover_wal_budget_exceeded")
        remaining_cutover = self.plan.cutover_wal_upper_bound_bytes - cutover_wal
        if (
            preparation_wal + cutover_wal + remaining_logging + remaining_cutover
            > dict(self.plan.reservation_bytes)["wal"]
        ):
            raise _error("aggregate_wal_budget_exceeded")

    async def _current_wal_bytes(self, *, end_lsn: str | None = None) -> int:
        """Measure cluster WAL through now or an exact paired meter boundary."""
        wal_bytes = await self.fhir.db.scalar(
            "SELECT pg_wal_lsn_diff(COALESCE(CAST(CAST(:end AS text) AS pg_lsn),pg_current_wal_insert_lsn()),"
            "CAST(CAST(:start AS text) AS pg_lsn))::bigint;",
            end=end_lsn,
            start=self.initial_wal_lsn,
        )
        if type(wal_bytes) is not int or wal_bytes < 0:
            raise _error("wal_observation_invalid")
        return wal_bytes

    def _remaining_logging_wal(self, phase: str, wal_bytes: int) -> int:
        """Charge CMS admission, scratch and logging; Profile keeps its measured admission bytes."""
        end_bytes = self.preparation_wal_end_bytes
        if end_bytes is None:
            end_bytes = wal_bytes
        elif phase == "pre_logging":
            raise _error("logging_phase_closed")
        spent = end_bytes - (self.paused_profile.spent_wal_bytes if self.paused_profile is not None else 0)
        if end_bytes > wal_bytes or not 0 <= spent <= self.plan.logging_wal_upper_bound_bytes:
            raise _error("logging_wal_budget_exceeded")
        if self.preparation_wal_end_bytes is not None:
            return 0
        return self.plan.logging_wal_upper_bound_bytes - spent

    async def _assert_active_profile(self, profile: Any) -> None:
        """Exclude a Profile window only after verifying its exact durable admission."""
        if profile.run_id != self.run_id or profile.lease.lease_digest != self.profile_lease.lease_digest:
            raise _error("active_profile_changed")
        table = self.fhir._unscoped_qt(
            self.fhir._schema(), self.fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__
        )
        row = await self.fhir.db.first(
            f"SELECT attestation_id,lease_digest,selection_proof_id FROM {table} "
            "WHERE run_id=:run_id AND admission_purpose='profile';",
            run_id=self.run_id,
        )
        if row is None or dict(self.fhir._pagination_checkpoint_row_mapping(row)) != {
            "attestation_id": self.profile_lease.attestation_id,
            "lease_digest": self.profile_lease.lease_digest,
            "selection_proof_id": self.plan.selection_proof_id,
        }:
            raise _error("active_profile_consumption_changed")


def _quiescence_parameters(
    fhir: Any, producer: _CapacityProducer, receipt: Mapping[str, Any], now: datetime.datetime
) -> dict[str, Any]:
    """Reuse closed existing competing-owner boundaries with one exact paired hash."""
    quiescence = receipt["quiescence"]
    return {
        "active_statuses": quiescence["active_profile_run_statuses"],
        "profile_params": json.dumps(
            {
                "provider_directory_profile_contract_id": fhir.PROFILE_EXECUTION_CONTRACT_ID,
                "publish_artifacts_only": True,
                "publish_artifacts_targets": ["profile"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        "current_run_id": producer.run_id,
        "observed_at": now,
        "request_sha256": receipt["request_sha256"],
        "paired_request_sha256": producer.profile_lease.signing_preflight_guard["healthcare_receipt"]["request_sha256"],
    }


async def produce_nonprofile_admission(
    fhir: Any,
    execution: Any,
    fence: Any,
    *,
    run_id: str,
    assigned_envelope: Mapping[str, Any],
    profile_envelope: Mapping[str, Any],
    signed_plan: NonprofileAdmissionPlan,
    fresh_storage_envelope: Callable[[StorageContinuationRequest], Awaitable[Mapping[str, Any]]],
) -> NonprofileAdmission:
    """Produce phase authority from two independently verified existing signed leases."""
    _validate_plan(signed_plan)
    if not callable(fresh_storage_envelope) or re.fullmatch(r"run_[0-9a-f]{32}", run_id) is None:
        raise _error("authority_transport_required")
    if (
        execution.attestation.desired_cms_dataset is None
        or execution.attestation.operation != "publish"
        or (
            signed_plan.selection_proof_id != execution.attestation.proof_id
            or signed_plan.desired_profile_as_of != execution.attestation.desired_profile_as_of
            or signed_plan.desired_fence_hash != desired_fence_hash(fence)
        )
    ):
        raise _error("selection_changed")
    observation = await _database_observation(fhir, signed_plan)
    now = await fhir._profile_capacity_preflight_clock()
    lease = _verify_envelope(assigned_envelope, signed_plan.capacity_geometry_hash, observation, now)
    _assert_signed_plan(fhir, execution, signed_plan, lease)
    profile = _verify_envelope(profile_envelope, profile_envelope["lease"]["capacity_geometry_hash"], observation, now)
    if profile.lease_digest != signed_plan.paired_profile_lease_digest:
        raise _error("paired_profile_lease_changed")
    _assert_paired_reservation(lease, profile, signed_plan.minimum_remaining_bytes)
    _assert_runtime_storage(signed_plan, lease, observation)
    assert_database_capacity_lease_reservation(
        lease,
        required_bytes_by_storage_class=dict(signed_plan.reservation_bytes),
        minimum_remaining_bytes=signed_plan.minimum_remaining_bytes,
        required_build_seconds=signed_plan.required_build_seconds,
    )
    runtime = await observe_profile_runtime(fhir.db)
    assert_capacity_lease_matches_runtime_observation(lease, runtime)
    assert_capacity_lease_matches_runtime_observation(profile, runtime)
    producer = _CapacityProducer(
        fhir, execution, fence, run_id, lease, profile, signed_plan, fresh_storage_envelope, observation["wal_lsn"]
    )
    return NonprofileAdmission(
        lease,
        signed_plan,
        producer.check_phase,
        pause_profile=producer.pause_profile,
        resume_profile=producer.resume_profile,
        paired_profile_lease=profile,
        cutover_operation=producer.cutover_operation,
        check_cutover=producer.assert_cutover,
    )
