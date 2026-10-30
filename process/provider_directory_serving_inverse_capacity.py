# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh standalone inverse admission on the existing lease and consumption boundary."""

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace

from sqlalchemy import text

from process import provider_directory_serving_inverse_capacity_contract as contract
from process import provider_directory_serving_inverse_capacity_inputs as inputs
from process import provider_directory_serving_inverse_capacity_preflight as preflight
from process import provider_directory_serving_inverse_capacity_signing as signing
from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_cms_nonprofile_capacity import (
    _assert_runtime_storage,
    _database_observation,
    _read_database_observation,
    _verify_envelope,
)
from process.provider_directory_cms_storage_continuation import (
    request_storage_continuation,
    verify_storage_continuation,
)
from process.provider_directory_output_snapshot_catalog import decoded, encoded
from process.provider_directory_owned_wal_transaction import registry_owned_wal_transaction
from process.provider_directory_profile_capacity_attestation import (
    CapacityLeaseConsumptionBinding,
    assert_database_capacity_lease_reservation,
    capacity_lease_consumption_values,
)
from process.provider_directory_profile_capacity_runtime import configured_capacity_lease_trust
from process.provider_directory_profile_runtime_observation import (
    assert_capacity_lease_matches_runtime_observation,
    observe_profile_runtime,
)


def task_execution(task):
    """A recover request has one exact event and no acquisition or Profile command."""
    fields = {
        recovery.OPERATION_PARAM,
        recovery.GENERATION_PARAM,
        "source_ids",
        contract.EVENT_FIELD,
        contract.EXECUTION_FIELD,
    }
    contract.require(type(task) is dict and fields <= set(task) <= fields | {"run_id"}, "request")
    contract.require(task[recovery.OPERATION_PARAM] == "recover", "request")
    envelope = task[contract.EXECUTION_FIELD]
    contract.require(type(envelope) is dict and type(envelope.get("lease")) is dict, "envelope")
    guard = envelope["lease"].get("signing_preflight_guard")
    contract.require(type(guard) is dict and guard.get("contract_id") == contract.SIGNING_GUARD, "purpose")
    request = contract.validated_inverse_preflight_request(guard["healthcare_request"])
    execution = request.recovery_execution
    contract.require(
        task[recovery.GENERATION_PARAM] == execution["generation"]
        and task[contract.EVENT_FIELD] == execution["published_event_sha256"]
        and task["source_ids"] == execution["source_ids"],
        "operation_changed",
    )
    return request


def _plan(geometry):
    """Supply only shared runtime-storage settings, never a synthetic Profile plan."""
    return SimpleNamespace(temp_file_limit_bytes_per_backend=geometry["temp_file_limit_bytes_per_backend"])


def _binding(run_id, geometry):
    provenance = geometry["scope"]["profile_provenance"]
    return CapacityLeaseConsumptionBinding(
        run_id,
        "pdpb_" + signing.geometry_sha256(geometry)[:32],
        signing.geometry_sha256(geometry),
        provenance["selection_proof_id"],
        provenance["source_vector_hash"],
        provenance["source_context_vector_hash"],
        provenance["profile_as_of"],
    )


async def _run(fhir, run_id, task):
    await fhir._lock_provider_directory_profile_capacity_control_run(schema=fhir._schema(), run_id=run_id)
    row = await fhir.db.first(
        f"SELECT params FROM {fhir._unscoped_qt(fhir._schema(), fhir.ImportRun.__tablename__)} WHERE run_id=:run",
        run=run_id,
    )
    contract.require(row is not None, "run_missing")
    params = decoded(fhir._pagination_checkpoint_row_mapping(row)["params"])
    expected_by_field = {key: value for key, value in task.items() if key != "run_id"}
    contract.require(params == expected_by_field or params == task, "run_operation_changed")


@dataclass
class InverseAdmission:
    """Verified fresh authority remains on its original task and single mutation transaction."""

    fhir: object
    request: object
    run_id: str
    lease: object
    geometry: dict
    captured: object
    transport: object
    initial_wal_lsn: str
    task: object = field(default_factory=asyncio.current_task)
    consumption: dict = field(default_factory=dict)
    continuation_digests: set = field(default_factory=set)
    last_observed_at: object = None
    witness: object = None
    binding: object = None
    actor_json: str | None = None
    cutover_start_wal: int | None = None
    preparation_wal: int | None = None
    peak_data_bytes: int = 0
    started_at: float = field(default_factory=time.monotonic)

    @property
    def execution(self):
        """Return the exact standalone operation carried by the signed request."""
        return self.request.recovery_execution

    @property
    def inverse(self):
        """Link a recovered event to its actual worker and immutable consumption."""
        return dict(
            operation_sha256=contract.operation_sha256(self.execution),
            run_id=self.run_id,
            lease_digest=self.lease.lease_digest,
        )

    def _original(self):
        contract.require(self.task is asyncio.current_task(), "owner_task_changed")
        signed = contract.validated_inverse_preflight_request(self.lease.signing_preflight_guard["healthcare_request"])
        contract.require(
            signed == self.request
            and self.geometry == self.lease.signing_preflight_guard["healthcare_receipt"]["capacity_geometry"]
            and signing.geometry_sha256(self.geometry) == self.lease.capacity_geometry_hash,
            "signed_admission_changed",
        )

    async def refresh(self, phase):
        """Obtain signed storage before the short cut, retaining any original SHARE owner."""
        self._original()
        binding = self.fhir.db._transaction_binding()
        contract.require(
            phase in {"pre_scratch", "readiness", "cutover"}
            and (binding is None or binding is self.binding and self.cutover_start_wal is None),
            "phase",
        )
        now = await self.fhir._profile_capacity_preflight_clock()
        request = request_storage_continuation(self.lease, run_id=self.run_id, phase=phase, requested_at=now)
        envelope = await self.transport(request)
        now = await self.fhir._profile_capacity_preflight_clock()
        witness = verify_storage_continuation(
            envelope, request=request, lease=self.lease, trust=configured_capacity_lease_trust(), now=now
        )
        contract.require(
            witness.witness_digest not in self.continuation_digests
            and (self.last_observed_at is None or witness.observed_at >= self.last_observed_at),
            "fresh_observation_required",
        )
        contract.require(now < self.lease.max_build_deadline, "deadline")
        self.continuation_digests.add(witness.witness_digest)
        self.last_observed_at = witness.observed_at
        reserved_by_field = {}
        for volume in self.lease.volumes:
            reserved_by_field[volume.volume_digest] = (
                reserved_by_field.get(volume.volume_digest, 0) + volume.reserved_bytes
            )
        minimum = self.geometry["minimum_remaining_bytes"]
        contract.require(
            all(
                available >= reserved_by_field[digest] + minimum and remaining >= minimum
                for _kind, digest, available, remaining in witness.volume_observations
            ),
            "physical_headroom",
        )
        self.witness = witness
        return witness

    async def consume(self, task, runtime):
        """Commit one genuine fresh consumption before any inverse row/hash work."""
        self._original()
        fhir = self.fhir
        schema = fhir._schema()
        contract.require(not self.consumption and fhir.db._transaction_binding() is None, "consumption_replay")
        async with registry_owned_wal_transaction(fhir.db) as outcome:
            session = outcome.session
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
            await preflight.configure_preparation(fhir, self.request.limits_payload)
            await fhir._lock_profile_capacity_preflight_state(schema)
            await _run(fhir, self.run_id, task)
            observed = await inputs.capture_inverse_inputs(fhir, self.execution)
            contract.require(
                inputs.has_same_captured_inputs(self.geometry["scope"], observed.payload), "inputs_changed"
            )
            actual_runtime = await observe_profile_runtime(fhir.db)
            assert_capacity_lease_matches_runtime_observation(self.lease, actual_runtime)
            contract.require(runtime == actual_runtime, "runtime_changed")
            preflight_row = await fhir.db.first(
                f"SELECT * FROM {fhir._profile_capacity_preflight_receipt_ref(schema)} WHERE receipt_sha256=:receipt FOR UPDATE",
                receipt=self.lease.nonce,
            )
            contract.require(preflight_row is not None, "preflight_missing")
            stored = fhir._pagination_checkpoint_row_mapping(preflight_row)
            receipt = preflight.read_inverse_receipt_row(stored, self.lease)
            now = await fhir._profile_capacity_preflight_clock()
            fhir._assert_profile_capacity_receipt_open(stored, self.lease, now)
            await fhir._assert_profile_capacity_receipt_storage(schema, receipt)
            await preflight._quiescence(fhir, self.request, run_id=self.run_id)
            contract.require(receipt["runtime_observation"] == actual_runtime, "preflight_runtime_changed")
            await fhir._mark_profile_capacity_receipt_consumed(schema, self.run_id, self.lease, now)
            accepted = await fhir._provider_directory_profile_capacity_acceptance_time(self.lease)
            consumption_by_field = {
                "admission_purpose": contract.PURPOSE,
                **capacity_lease_consumption_values(
                    self.lease, _binding(self.run_id, self.geometry), accepted_at=accepted
                ),
            }
            await fhir._consume_provider_directory_profile_capacity_lease(
                schema=schema, values_by_name=consumption_by_field
            )
        self.consumption = consumption_by_field
        self.captured = observed

    async def assert_consumption(self):
        """Require the same immutable full original bytes, not just its nonce."""
        table = self.fhir._unscoped_qt(
            self.fhir._schema(), self.fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__
        )
        rows = await self.fhir.db.all(
            f"SELECT * FROM {table} WHERE run_id=:run AND admission_purpose=:purpose",
            run=self.run_id,
            purpose=contract.PURPOSE,
        )
        identity = self.fhir._provider_directory_profile_capacity_consumption_identity
        contract.require(
            self.consumption
            and [identity(self.fhir._pagination_checkpoint_row_mapping(row)) for row in rows]
            == [identity(self.consumption)],
            "consumption_changed",
        )

    async def _wal(self):
        value = await self.fhir.db.scalar(
            "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:start AS text) AS pg_lsn))::bigint",
            start=self.initial_wal_lsn,
        )
        return contract.integer(value)

    async def boundary(self):
        """Measure actual native/TOAST/index growth and cluster WAL on the admitted owner."""
        self._original()
        current = self.fhir.db._transaction_binding()
        contract.require(
            current is not None and current is self.binding and self.witness is not None, "transaction_changed"
        )
        _session, _driver, actor = await receipts.ordinary(self.fhir.db)
        contract.require(
            encoded(actor) == self.actor_json and actor["owner"] == self.geometry["scope"]["owner_oid"], "actor_changed"
        )
        await self.assert_consumption()
        now = await self.fhir._profile_capacity_preflight_clock()
        contract.require(
            now < min(self.witness.expires_at, self.lease.max_build_deadline)
            and time.monotonic() < self.started_at + self.request.limits_payload["max_prepare_seconds"],
            "deadline",
        )
        observation = await _read_database_observation(self.fhir)
        _assert_runtime_storage(_plan(self.geometry), self.lease, observation)
        inventory = await inputs.assert_scope_native(
            self.fhir, self.geometry["scope"], observation["data_tablespace_oid"]
        )
        baseline_by_oid = {entry["oid"]: entry["baseline_total_bytes"] for entry in self.geometry["scope"]["relations"]}
        measured = sum(
            max(0, inventory[oid]["total_bytes"] - baseline_bytes) for oid, baseline_bytes in baseline_by_oid.items()
        )
        self.peak_data_bytes = max(self.peak_data_bytes, measured)
        contract.require(self.peak_data_bytes <= dict(self.geometry["reservation_bytes"])["data"], "data_budget")
        spent = await self._wal()
        if self.cutover_start_wal is None:
            contract.require(spent <= self.geometry["preparation_wal_upper_bound_bytes"], "preparation_wal_budget")
        else:
            contract.require(
                spent >= self.cutover_start_wal
                and spent - self.cutover_start_wal <= self.geometry["cutover_wal_upper_bound_bytes"],
                "cutover_wal_budget",
            )
        reserve = self.geometry["commit_wal_upper_bound_bytes"]
        contract.require(spent + reserve <= dict(self.geometry["reservation_bytes"])["wal"], "commit_wal_reserve")
        return spent

    @asynccontextmanager
    async def mutation(self):
        """One original Builder owns preparation, all bounded writes and the atomic commit."""
        self._original()
        contract.require(self.fhir.db._transaction_binding() is None and self.consumption, "transaction_changed")
        await self.refresh("cutover")
        async with (
            asyncio.timeout_at(self.started_at + self.request.limits_payload["max_prepare_seconds"]) as wall,
            registry_owned_wal_transaction(self.fhir.db),
        ):
            await preflight.configure_preparation(self.fhir, self.request.limits_payload)
            self.binding = self.fhir.db._transaction_binding()
            _session, _driver, actor = await receipts.ordinary(self.fhir.db)
            self.actor_json = encoded(actor)
            await self.boundary()
            try:
                yield wall
                await self.boundary()
            finally:
                self.binding = None
                self.actor_json = None

    async def begin_cutover(self, wall):
        """Keep the established two-second cut and statement/lock ceilings."""
        from process.provider_directory_serving_inverse_commit import assert_commit_reserve

        await assert_commit_reserve(self.fhir, self.geometry, await preflight._native_sizes(self.fhir))
        self.preparation_wal = await self.boundary()
        self.cutover_start_wal = self.preparation_wal
        limits = self.request.limits_payload
        wall.reschedule(min(wall.when(), asyncio.get_running_loop().time() + limits["max_cutover_seconds"]))
        await self.fhir.db.status(
            "SELECT set_config('statement_timeout',:value,true)", value=str(limits["statement_timeout_ms"]) + "ms"
        )
        await self.fhir.db.status(
            "SELECT set_config('lock_timeout',:value,true)", value=str(limits["lock_timeout_ms"]) + "ms"
        )
        await self.boundary()


async def produce_inverse_admission(fhir, task, *, control_run_id, fresh_storage_envelope):
    """Keep native verification/consumption and mutation on one bounded preparation wall."""
    request = task_execution(task)
    started_at = time.monotonic()
    async with asyncio.timeout(request.limits_payload["max_prepare_seconds"]):
        return await _produce_inverse_admission(
            fhir,
            task,
            request=request,
            control_run_id=control_run_id,
            fresh_storage_envelope=fresh_storage_envelope,
            started_at=started_at,
        )


async def _produce_inverse_admission(fhir, task, *, request, control_run_id, fresh_storage_envelope, started_at):
    """Verify standalone authority without loading or borrowing a forward Profile lease."""
    run_id = contract.run_handle(task.get("run_id"))
    contract.require(
        run_id == control_run_id and callable(fresh_storage_envelope) and fhir.db._transaction_binding() is None,
        "run_owner",
    )
    envelope = task[contract.EXECUTION_FIELD]
    raw = envelope["lease"]["signing_preflight_guard"]["healthcare_receipt"]["capacity_geometry"]
    geometry = contract.validated_inverse_geometry(
        raw, limits=request.limits_payload, execution=request.recovery_execution
    )
    observation = await _database_observation(fhir, _plan(geometry))
    now = await fhir._profile_capacity_preflight_clock()
    lease = _verify_envelope(envelope, signing.geometry_sha256(geometry), observation, now)
    contract.require(
        lease.signing_preflight_guard["healthcare_request"] == request.request_payload, "signed_operation_changed"
    )
    _assert_runtime_storage(_plan(geometry), lease, observation)
    assert_database_capacity_lease_reservation(
        lease,
        required_bytes_by_storage_class=dict(geometry["reservation_bytes"]),
        minimum_remaining_bytes=geometry["minimum_remaining_bytes"],
        required_build_seconds=geometry["required_build_seconds"],
    )
    runtime = await observe_profile_runtime(fhir.db)
    assert_capacity_lease_matches_runtime_observation(lease, runtime)
    admission = InverseAdmission(
        fhir,
        request,
        run_id,
        lease,
        geometry,
        None,
        fresh_storage_envelope,
        observation["wal_lsn"],
        started_at=started_at,
    )
    await admission.refresh("pre_scratch")
    await admission.consume(task, runtime)
    return admission


async def consumed_inverse_lease(fhir, inverse, *, generation, published_event_sha256):
    """Read historical committed consumption without creating fresh mutation authority."""
    from process.provider_directory_profile_capacity_attestation import verify_database_capacity_lease

    contract.require(
        type(inverse) is dict and set(inverse) == {"operation_sha256", "run_id", "lease_digest"}, "inverse_link"
    )
    contract.run_handle(inverse["run_id"])
    contract.hash64(inverse["lease_digest"])
    contract.hash64(inverse["operation_sha256"])
    table = fhir._unscoped_qt(fhir._schema(), fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__)
    consumed_rows = await fhir.db.all(
        f"SELECT * FROM {table} WHERE run_id=:run AND admission_purpose=:purpose",
        run=inverse["run_id"],
        purpose=contract.PURPOSE,
    )
    contract.require(len(consumed_rows) == 1, "inverse_consumption_missing")
    consumed_by_field = fhir._pagination_checkpoint_row_mapping(consumed_rows[0])
    body = decoded(consumed_by_field["canonical_lease_json"])
    envelope_by_field = {"lease": body, "signature": consumed_by_field["signature"]}
    trust = configured_capacity_lease_trust()
    lease = verify_database_capacity_lease(
        envelope_by_field,
        trust=trust,
        now=consumed_by_field["accepted_at"],
        expected_capacity_geometry_hash=consumed_by_field["capacity_geometry_hash"],
        expected_database_system_identifier=trust.database_system_identifier,
        expected_database_oid=trust.database_oid,
        expected_database_name=trust.database_name,
    )
    request = contract.validated_inverse_preflight_request(lease.signing_preflight_guard["healthcare_request"])
    execution = request.recovery_execution
    geometry = contract.validated_inverse_geometry(
        lease.signing_preflight_guard["healthcare_receipt"]["capacity_geometry"],
        limits=request.limits_payload,
        execution=execution,
    )
    expected_by_field = {
        "admission_purpose": contract.PURPOSE,
        **capacity_lease_consumption_values(
            lease, _binding(inverse["run_id"], geometry), accepted_at=consumed_by_field["accepted_at"]
        ),
    }
    identity = fhir._provider_directory_profile_capacity_consumption_identity
    contract.require(
        identity(consumed_by_field) == identity(expected_by_field)
        and lease.lease_digest == inverse["lease_digest"]
        and contract.operation_sha256(execution) == inverse["operation_sha256"]
        and execution["generation"] == generation
        and execution["published_event_sha256"] == published_event_sha256,
        "inverse_consumption_changed",
    )
    return lease


def validated_inverse_retry_params(current_by_field, overrides_by_field, run_id):
    """Rotate only freshly assigned capacity while preserving the original recover operation."""
    current = task_execution(dict(current_by_field))
    contract.require(
        type(overrides_by_field) is dict and set(overrides_by_field) == {contract.EXECUTION_FIELD},
        "retry_fresh_capacity_required",
    )
    retry_by_field = {**current_by_field, **overrides_by_field}
    if "run_id" in retry_by_field:
        retry_by_field["run_id"] = contract.run_handle(run_id)
    fresh = task_execution(retry_by_field)
    contract.require(fresh.recovery_execution == current.recovery_execution, "retry_operation_changed")
    previous = current_by_field[contract.EXECUTION_FIELD]["lease"]
    replacement = retry_by_field[contract.EXECUTION_FIELD]["lease"]
    contract.require(
        all(
            previous.get(key) != replacement.get(key) and replacement.get(key)
            for key in ("attestation_id", "reservation_id", "nonce")
        ),
        "retry_fresh_capacity_required",
    )
    return retry_by_field
