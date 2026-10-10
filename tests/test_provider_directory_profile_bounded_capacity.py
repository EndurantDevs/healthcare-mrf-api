# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded admission, unresolved exposure, and immutable version boundaries."""

import asyncio
import importlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_selection as selection
from tests.test_provider_directory_profile_capacity_projection import _projection_geometry, _projection_inputs
from tests.test_provider_directory_profile_control_capacity import _bound_control_wal_projection
from tests.test_provider_directory_profile_cutover_capacity import (
    _cutover_actual_by_field,
    _cutover_forecast_by_field,
    _cutover_metadata_projection,
    _cutover_target_projection,
)
from tests.test_provider_directory_profile_resume_lineage import _dataset, _source_context
from tests.test_provider_directory_profile_selection_attestation import _execution

fhir = importlib.import_module("process.provider_directory_fhir")


def _bounded_geometry(*, window_size=2, data_cap=1000, wal_cap=1000):
    geometry, original_projection = _bound_control_wal_projection()
    geometry = replace(
        geometry,
        physical_projection_contract_id=capacity.BOUNDED_ADMISSION_CONTRACT_ID,
        artifact_scope_batch_size=window_size,
        relation_byte_caps=tuple(
            replace(
                cap,
                max_wal_bytes=wal_cap,
                max_scratch_bytes=data_cap if cap.max_scratch_bytes else 0,
                max_target_growth_bytes=data_cap if cap.max_target_growth_bytes else 0,
            )
            for cap in geometry.relation_byte_caps
        ),
    )
    geometry_plan = original_projection.plan_input
    projection = capacity.project_profile_control_wal_capacity(geometry, geometry_plan)
    geometry = replace(
        geometry,
        control_wal_upper_bound_bytes=projection.total_control_wal_bytes,
        control_metadata_data_upper_bound_bytes=projection.total_control_metadata_data_bytes,
    )
    return geometry, capacity.project_profile_control_wal_capacity(geometry, geometry_plan)


@pytest.fixture
def admitted_window(monkeypatch):
    geometry, projection = _bounded_geometry()
    tracker = fhir._ProviderDirectoryProfileWalTracker()
    state = SimpleNamespace(wal=0, sizes={"stage": 100}, expired=False)
    admission = fhir._ProviderDirectoryProfileCapacityAdmission(
        geometry=geometry,
        control_wal_projection=projection,
        wal_tracker=tracker,
        initial_wal_lsn="0/1",
        initial_wal_offset_bytes=0,
        lease=SimpleNamespace(),
        database_identity=SimpleNamespace(),
        build_id="synthetic-build",
        run_id="synthetic-run",
    )

    async def remaining(_):
        if state.expired:
            raise RuntimeError("deadline_reached")
        return 1000

    monkeypatch.setattr(fhir, "_profile_capacity_remaining_ms", remaining)
    monkeypatch.setattr(
        fhir, "_provider_directory_profile_current_wal_bytes", AsyncMock(side_effect=lambda _: state.wal)
    )
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_capacity_relation_bytes",
        AsyncMock(side_effect=lambda refs: sum(state.sizes[r] for r in refs)),
    )
    token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
    try:
        yield admission, state
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)


async def _reserve(admission, *, wal=700, growth=50):
    await fhir._reserve_profile_capacity_growth(admission, "evidence_stage", growth)
    await fhir._reserve_provider_directory_profile_wal_budget(admission, relation_wal_bytes={"evidence_stage": wal})


def test_diagnostic_projection_cannot_reinterpret_legacy_admission():
    inputs = (replace(_projection_inputs()[0], inserted_rows=1000), _projection_inputs()[1])
    legacy = _projection_geometry()
    bounded = replace(legacy, physical_projection_contract_id=capacity.BOUNDED_ADMISSION_CONTRACT_ID)
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError):
        capacity.project_profile_delta_capacity(legacy, inputs, enforce_caps=False)
    diagnostic = capacity.project_profile_delta_capacity(bounded, inputs, enforce_caps=False)
    assert diagnostic.wal_bytes > bounded.relation_byte_caps[4].max_wal_bytes
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError):
        capacity.project_profile_delta_capacity(bounded, inputs)
    assert capacity.capacity_geometry_hash(bounded) != capacity.capacity_geometry_hash(legacy)


@pytest.mark.asyncio
async def test_settled_actual_allows_next_window_when_cumulative_forecasts_do_not(admitted_window):
    admission, state = admitted_window
    # This isolated window schedules no profile-target replacement.
    await fhir._complete_profile_capacity_relation_class("profile_target")
    for _ in range(2):
        async with fhir._profile_capacity_mutation_window("evidence_stage", ("stage",)):
            await _reserve(admission)
            state.wal += 100
            state.sizes["stage"] += 10
    assert admission.wal_tracker.accounted_relation_wal_bytes == {"evidence_stage": 200}
    assert not admission.wal_tracker.pending_relation_wal_bytes
    assert not admission.wal_tracker.pending_growth_bytes
    assert admission.initial_wal_lsn == "0/1"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["wal", "data", "deadline", "cancel", "observe"])
async def test_uncertain_or_overrun_window_retains_every_charge(admitted_window, monkeypatch, failure):
    admission, state = admitted_window
    error = asyncio.CancelledError if failure == "cancel" else RuntimeError
    with pytest.raises(error):
        async with fhir._profile_capacity_mutation_window("evidence_stage", ("stage",)):
            await _reserve(admission)
            if failure == "cancel":
                raise asyncio.CancelledError()
            state.wal = 1001 if failure == "wal" else 100
            state.sizes["stage"] = 151 if failure == "data" else 110
            state.expired = failure == "deadline"
            if failure == "observe":
                monkeypatch.setattr(
                    fhir,
                    "_provider_directory_profile_current_wal_bytes",
                    AsyncMock(side_effect=RuntimeError("observation_missing")),
                )
    tracker = admission.wal_tracker
    assert tracker.pending_relation_wal_bytes == {"evidence_stage": 700}
    assert tracker.pending_growth_bytes == {"evidence_stage": 50}
    assert tracker.accounted_relation_wal_bytes == {}
    assert tracker.unresolved_window
    with pytest.raises(RuntimeError, match="window_unresolved"):
        async with fhir._profile_capacity_mutation_window("evidence_stage", ("stage",)):
            pytest.fail("unresolved admission reached DML")


@pytest.mark.asyncio
async def test_one_byte_short_refuses_before_mutation(admitted_window):
    admission, state = admitted_window
    has_mutated = False
    with pytest.raises(RuntimeError, match="relation_wal_projected"):
        async with fhir._profile_capacity_mutation_window("evidence_stage", ("stage",)):
            await _reserve(admission, wal=1001)
            has_mutated = True
    assert not has_mutated and state.wal == 0


@pytest.mark.asyncio
async def test_all_nine_artifact_relations_and_pending_growth_share_the_cap(admitted_window):
    admission, state = admitted_window
    state.sizes = {str(i): 100 for i in range(9)}
    admission.wal_tracker.relation_refs_by_class["artifact_scope"] = set(state.sizes)
    with pytest.raises(RuntimeError, match="window_growth_projected"):
        async with fhir._profile_capacity_mutation_window("artifact_scope", ("0",)):
            await fhir._reserve_profile_capacity_growth(admission, "artifact_scope", 100)
            await fhir._reserve_profile_capacity_growth(admission, "artifact_scope", 1)
            pytest.fail("aggregate data underflow reached DML")
    assert admission.wal_tracker.pending_growth_bytes == {"artifact_scope": 100}


@pytest.mark.asyncio
async def test_reservation_keeps_control_metadata_and_full_unfinished_relation_caps(admitted_window, monkeypatch):
    admission, _ = admitted_window
    tracker = admission.wal_tracker
    tracker.pending_relation_wal_bytes["profile_stage"] = 20
    tracker.pending_control_wal_bytes[object()] = 70
    tracker.pending_metadata_wal_bytes = 100
    tracker.accounted_metadata_wal_bytes = 100
    validate = AsyncMock()
    monkeypatch.setattr(fhir, "_validate_profile_total_wal_budget", validate)
    token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set((object(), "evidence_stage"))
    try:
        await fhir._reserve_provider_directory_profile_wal_budget(admission, relation_wal_bytes={"evidence_stage": 400})
    finally:
        fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)
    assert validate.call_args.args[1] == 70 + 100
    assert validate.call_args.args[2] >= fhir._profile_relation_wal_candidate(admission, {})[2]
    assert tracker.pending_relation_wal_bytes == {"profile_stage": 20, "evidence_stage": 400}


@pytest.mark.asyncio
async def test_worker_barrier_precedes_settlement(admitted_window):
    admission, state = admitted_window
    # This isolated window schedules no profile-target replacement.
    await fhir._complete_profile_capacity_relation_class("profile_target")
    release, first_done = asyncio.Event(), asyncio.Event()

    async def worker(wait):
        if wait:
            await release.wait()
        state.wal += 50
        state.sizes["stage"] += 1
        if not wait:
            first_done.set()

    async def wave():
        async with fhir._profile_capacity_mutation_window("evidence_stage", ("stage",)):
            await _reserve(admission)
            await fhir._gather_provider_directory_profile_tasks(
                [asyncio.create_task(worker(False)), asyncio.create_task(worker(True))]
            )

    task = asyncio.create_task(wave())
    await asyncio.wait_for(first_done.wait(), timeout=5)
    assert not admission.wal_tracker.accounted_relation_wal_bytes
    assert admission.wal_tracker.pending_relation_wal_bytes == {"evidence_stage": 700}
    release.set()
    await task
    assert admission.wal_tracker.accounted_relation_wal_bytes == {"evidence_stage": 100}


@pytest.mark.asyncio
async def test_target_growth_uses_the_original_baseline(admitted_window):
    admission, state = admitted_window
    # This isolated window schedules no profile-target replacement.
    await fhir._complete_profile_capacity_relation_class("profile_target")
    tracker = admission.wal_tracker
    state.sizes["target"] = 10_000
    tracker.target_bytes_before["evidence_target"] = 10_000
    async with fhir._profile_capacity_mutation_window("evidence_target", ("target",)):
        await fhir._reserve_profile_capacity_growth(admission, "evidence_target", 500)
        await fhir._reserve_provider_directory_profile_wal_budget(
            admission, relation_wal_bytes={"evidence_target": 500}
        )
        state.sizes["target"] += 100
        state.wal += 50
    with pytest.raises(RuntimeError, match="window_growth_projected"):
        async with fhir._profile_capacity_mutation_window("evidence_target", ("target",)):
            await fhir._reserve_profile_capacity_growth(admission, "evidence_target", 901)


@pytest.mark.asyncio
async def test_relation_writes_require_a_bounded_gate(admitted_window):
    admission, _ = admitted_window
    with pytest.raises(RuntimeError, match="relation_window_required"):
        await fhir._reserve_provider_directory_profile_wal_budget(admission, relation_wal_bytes={"evidence_stage": 1})


def _bounded_cutover_receipt(offset, *, inflated=False):
    geometry = replace(_projection_geometry(), physical_projection_contract_id=capacity.BOUNDED_ADMISSION_CONTRACT_ID)
    target_projection, metadata = _cutover_target_projection(geometry), _cutover_metadata_projection(geometry)
    forecast = _cutover_forecast_by_field(geometry, target_projection, metadata)
    forecast.update(
        contract_id=geometry.cutover_forecast_contract_id,
        admission_wal_start_lsn="0/1",
        admission_wal_offset_bytes=offset,
        wal_bytes_before=offset,
    )
    actual = _cutover_actual_by_field(metadata, "f" * 64)
    actual.update(contract_id=geometry.cutover_actual_contract_id, wal_observed_lsn="0/65", cutover_wal_bytes=100)
    settled = dict.fromkeys((cap.relation_name for cap in geometry.relation_byte_caps), 0)
    settled.update(evidence_target=10, evidence_stage=90 + offset + int(inflated))
    metadata_bound = metadata.wal_bytes + metadata.commit_envelope_bytes
    actual["wal_ledger"] = {
        "settled_relation_wal_bytes": settled,
        "pending_relation_wal_bytes": dict.fromkeys(settled, 0),
        "accounted_control_wal_bytes": 0,
        "pending_control_wal_bytes": 0,
        "accounted_metadata_wal_bytes": metadata_bound,
        "pending_metadata_wal_bytes": metadata_bound,
    }
    actual["target_windows"] = {
        projection.relation_name: {
            "window_count": int(projection.relation_name == "evidence_target"),
            "inserted_rows": int(projection.relation_name == "evidence_target"),
            "deleted_rows": 0,
            "inserted_toast_chunks": 0,
            "deleted_toast_chunks": 0,
            "deleted_logical_bytes": 0,
            "projected_wal_bytes": projection.wal_bytes,
            "projected_growth_bytes": projection.target_growth_bytes,
            "observed_wal_bytes": settled[projection.relation_name],
            "windows_hash": "a" * 64,
        }
        for projection in target_projection.targets
    }
    forecast_hash, _ = fhir._profile_cutover_hashes(forecast, actual)
    actual["forecast_hash"] = forecast_hash
    _, actual_hash = fhir._profile_cutover_hashes(forecast, actual)
    receipt_by_field = {
        **actual,
        "build_id": forecast["build_id"],
        "evidence_inserted": 1,
        "evidence_deleted": 0,
        "profile_inserted": 0,
        "profile_deleted": 0,
        "cutover_forecast_json": forecast,
        "cutover_forecast_hash": forecast_hash,
        "cutover_actual_json": actual,
        "cutover_actual_hash": actual_hash,
        "cutover_wal_start_lsn": actual["wal_start_lsn"],
        "cutover_wal_observed_lsn": actual["wal_observed_lsn"],
    }
    return geometry, receipt_by_field, forecast["run_id"]


@pytest.mark.parametrize("offset", [0, 25])
def test_bounded_replay_rejects_rehashed_scratch_settlement_above_observed_wal(offset):
    geometry, receipt, run_id = _bounded_cutover_receipt(offset)
    assert fhir._provider_directory_profile_cutover_receipt_identity(receipt, geometry=geometry, expected_run_id=run_id)
    geometry, receipt, run_id = _bounded_cutover_receipt(offset, inflated=True)
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="receipt_semantics_invalid") as error:
        fhir._provider_directory_profile_cutover_receipt_identity(receipt, geometry=geometry, expected_run_id=run_id)
    assert "cutover_settled_wal_exceeds_observed" in str(error.value.__cause__)


def _final_metadata_state(admission, state):
    tracker = admission.wal_tracker
    tracker.accounted_relation_wal_bytes.update(
        {cap.relation_name: cap.max_wal_bytes for cap in admission.geometry.relation_byte_caps}
    )
    tracker.accounted_relation_wal_bytes["evidence_stage"] -= 100
    tracker.accounted_relation_wal_bytes["profile_stage"] -= 10
    tracker.pending_relation_wal_bytes["profile_stage"] = 10
    tracker.pending_control_wal_bytes["unresolved-owner"] = 7
    tracker.accounted_control_operation_counts.update(
        {
            operation.operation_name: operation.operation_count
            for operation in admission.control_wal_projection.operations
        }
    )
    tracker.accounted_metadata_wal_bytes = tracker.pending_metadata_wal_bytes = 100
    forecast = SimpleNamespace(
        target_projection=SimpleNamespace(wal_bytes=0),
        metadata_projection=SimpleNamespace(wal_bytes=80, commit_envelope_bytes=20),
        wal_start_lsn="0/1",
    )
    state.wal = (
        admission.geometry.reservation_bytes_by_storage_class["wal"]
        - (admission.geometry.metadata_wal_upper_bound_bytes - tracker.accounted_metadata_wal_bytes)
        - fhir._profile_relation_wal_candidate(admission, {})[2]
        - sum(tracker.pending_control_wal_bytes.values())
        - forecast.metadata_projection.commit_envelope_bytes
    )
    return forecast


@pytest.mark.asyncio
async def test_final_metadata_validates_candidate_before_releasing_body(admitted_window, monkeypatch):
    admission, state = admitted_window
    forecast = _final_metadata_state(admission, state)
    tracker = admission.wal_tracker
    tracker.unresolved_window = True
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value=50))
    original_validate = fhir._validate_profile_delta_total_wal

    async def validate(candidate, forecast):
        await original_validate(candidate, forecast)
        assert tracker.lock.locked() and tracker.mutation_lock.locked()
        assert tracker.pending_metadata_wal_bytes == 100
        assert candidate.wal_tracker.pending_metadata_wal_bytes == 20

    monkeypatch.setattr(fhir, "_validate_profile_delta_total_wal", validate)
    await fhir._validate_profile_delta_final_wal(admission, forecast, metadata_wal_start_lsn="0/2")
    assert tracker.pending_metadata_wal_bytes == 20
    assert tracker.accounted_metadata_wal_bytes == 100
    assert tracker.pending_relation_wal_bytes == {"profile_stage": 10}
    assert tracker.pending_control_wal_bytes == {"unresolved-owner": 7}
    assert tracker.unresolved_window


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    ["negative_sample", "observe", "total_observe", "deadline", "final_deadline", "total", "cancel", "reserve"],
)
async def test_final_metadata_failure_retains_charges_and_poison(admitted_window, monkeypatch, failure):
    admission, state = admitted_window
    forecast = _final_metadata_state(admission, state)
    tracker = admission.wal_tracker
    observe = AsyncMock(return_value=-1 if failure == "negative_sample" else 50)
    if failure in {"observe", "cancel"}:
        observe.side_effect = asyncio.CancelledError() if failure == "cancel" else RuntimeError("observation_missing")
    monkeypatch.setattr(fhir.db, "scalar", observe)
    if failure == "total_observe":
        monkeypatch.setattr(
            fhir,
            "_provider_directory_profile_current_wal_bytes",
            AsyncMock(side_effect=RuntimeError("observation_missing")),
        )
    if failure == "final_deadline":

        async def current_wal(_):
            state.expired = True
            return state.wal

        monkeypatch.setattr(fhir, "_provider_directory_profile_current_wal_bytes", current_wal)
    state.expired = failure == "deadline"
    state.wal += int(failure == "total")
    if failure == "reserve":
        tracker.pending_metadata_wal_bytes = 99
    pending_before = tracker.pending_metadata_wal_bytes
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
        await fhir._validate_profile_delta_final_wal(admission, forecast, metadata_wal_start_lsn="0/2")
    assert tracker.pending_metadata_wal_bytes == pending_before
    assert tracker.accounted_metadata_wal_bytes == 100
    assert tracker.pending_relation_wal_bytes == {"profile_stage": 10}
    assert tracker.pending_control_wal_bytes == {"unresolved-owner": 7}
    assert tracker.unresolved_window
    with pytest.raises(RuntimeError, match="window_unresolved"):
        async with fhir._profile_capacity_mutation_window("evidence_stage", ("stage",)):
            pytest.fail("failed metadata validation admitted a relation write")


@pytest.mark.asyncio
async def test_legacy_final_metadata_preserves_pending_accounting(admitted_window, monkeypatch):
    admission, state = admitted_window
    forecast = _final_metadata_state(admission, state)
    legacy = replace(
        admission,
        geometry=replace(admission.geometry, physical_projection_contract_id=capacity.PHYSICAL_PROJECTION_CONTRACT_ID),
    )
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value=50))
    await fhir._validate_profile_delta_final_wal(legacy, forecast)
    assert legacy.wal_tracker.pending_metadata_wal_bytes == 100
    assert not legacy.wal_tracker.unresolved_window
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(side_effect=RuntimeError("legacy_observation_failure")))
    with pytest.raises(RuntimeError, match="legacy_observation_failure"):
        await fhir._validate_profile_delta_final_wal(legacy, forecast)
    assert legacy.wal_tracker.pending_metadata_wal_bytes == 100
    assert not legacy.wal_tracker.unresolved_window


@pytest.mark.asyncio
async def test_final_metadata_waits_for_quiescent_mutation_window(admitted_window, monkeypatch):
    admission, state = admitted_window
    forecast = _final_metadata_state(admission, state)
    observe = AsyncMock(return_value=50)
    monkeypatch.setattr(fhir.db, "scalar", observe)
    async with admission.wal_tracker.mutation_lock:
        task = asyncio.create_task(fhir._validate_profile_delta_final_wal(admission, forecast))
        await asyncio.sleep(0)
        observe.assert_not_awaited()
        assert admission.wal_tracker.pending_metadata_wal_bytes == 100
    await asyncio.wait_for(task, timeout=5)
    assert admission.wal_tracker.pending_metadata_wal_bytes == 20


def _authorized_recovery_build_ids():
    """Real selection coordinates, rather than a caller-supplied build salt."""
    original = _execution()
    renewed_attestation_by_field = {
        **original.attestation.payload,
        "authority_revision": original.attestation.authority_revision + 1,
    }
    renewed = replace(
        original,
        attestation=selection.validated_profile_selection_attestation(renewed_attestation_by_field),
        generation=original.generation + 1,
    )

    def build_id(execution):
        lineage = fhir._provider_directory_profile_resume_lineage_hash(
            fhir.ProviderDirectoryArtifactDatasetFence((_dataset(),)),
            ["source-a"],
            ["source-a"],
            ["dataset-a"],
            (_source_context(),),
            has_existing_artifacts=True,
            selection_execution=execution,
        )
        return "pdpb_" + lineage[:32]

    return build_id(original), build_id(renewed)


def test_new_authorized_selection_has_a_fresh_build_without_changing_retained_bytes():
    original_build, renewed_build = _authorized_recovery_build_ids()
    assert original_build != renewed_build
    assert (original_build, renewed_build) == _authorized_recovery_build_ids()


def _affected_payload_setup(admitted_window, monkeypatch, wal_cap):
    """Seed this payload-only proof; leave the ordinary 1,000-byte fixture intact."""
    from contextlib import asynccontextmanager

    from tests.test_provider_directory_profile_delta_coverage_edges_05 import _affected_stage_build

    admission, state = admitted_window
    geometry, projection = _bounded_geometry(wal_cap=wal_cap)
    admission = replace(admission, geometry=geometry, control_wal_projection=projection)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
    build = replace(_affected_stage_build(), materialization_mode="source_delta", affected_npi_stage="affected_stage")
    relation = fhir._provider_directory_profile_build_ref(build, build.affected_npi_stage)
    state.sizes[relation] = 100
    events = []

    @asynccontextmanager
    async def transaction(**kwargs):
        events.append("transaction")
        yield

    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_transaction", transaction)
    monkeypatch.setattr(fhir.db, "first", AsyncMock(return_value={"projected_rows": 0, "projected_logical_bytes": 0}))
    monkeypatch.setattr(fhir.db, "status", AsyncMock(return_value="INSERT 0 0"))

    async def payload(module, custody):
        return module._coerce_rowcount(await module.db.status(custody["statement"], **custody["params"]))

    monkeypatch.setattr(fhir.profile_payload_custody, "execute", payload)
    return admission, state, build, events


@pytest.mark.asyncio
@pytest.mark.parametrize("wal_cap", [1000, 73727])
async def test_affected_lock_envelope_refuses_before_transaction(admitted_window, monkeypatch, wal_cap):
    admission, _, build, events = _affected_payload_setup(admitted_window, monkeypatch, wal_cap)
    storage = AsyncMock()
    monkeypatch.setattr(fhir, "_assert_provider_directory_profile_stage_storage_identity", storage)
    with pytest.raises(RuntimeError, match="window_wal_projected"):
        await fhir._execute_affected_npi_insert(
            build, projection_sql="SELECT projection", insert_sql="INSERT", params={}
        )
    assert events == []
    storage.assert_not_awaited()
    fhir.db.status.assert_not_awaited()
    assert not admission.wal_tracker.accounted_control_operation_counts
    assert not admission.wal_tracker.pending_control_wal_bytes
    assert admission.wal_tracker.unresolved_window


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "storage", "projection", "insert", "cancel", "deadline"])
async def test_affected_reserve_precedes_lock_and_retains_failure_charge(admitted_window, monkeypatch, failure):
    admission, state, build, events = _affected_payload_setup(admitted_window, monkeypatch, 73728)

    async def checkpoint_first(sql, **parameters):
        if "FOR SHARE" in sql:
            assert sum(admission.wal_tracker.pending_control_wal_bytes.values()) == 73728
            events.append("checkpoint_lock")
            return {"affected_npi_stage_oid": 17, "affected_npi_stage_storage_fingerprint": "f" * 64}
        return {"projected_rows": 0, "projected_logical_bytes": 0}

    monkeypatch.setattr(fhir.db, "first", checkpoint_first)
    monkeypatch.setattr(fhir, "_provider_directory_profile_stage_storage_fingerprint", AsyncMock(return_value="f" * 64))

    async def storage(*args, **kwargs):
        assert fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()[1] == "affected_npi_stage"
        assert sum(admission.wal_tracker.pending_control_wal_bytes.values()) == 73728
        assert admission.wal_tracker.accounted_control_operation_counts == {"affected_npi_payload": 1}
        await fhir._assert_profile_stage_storage(*args, **kwargs)
        if failure == "storage":
            raise RuntimeError("storage_failed")
        if failure == "cancel":
            raise asyncio.CancelledError()
        if failure == "deadline":
            state.expired = True

    async def projection(*args):
        events.append("projection")
        if failure == "projection":
            raise RuntimeError("projection_failed")

    monkeypatch.setattr(fhir, "_assert_provider_directory_profile_stage_storage_identity", storage)
    monkeypatch.setattr(fhir, "_admit_affected_npi_projection", projection)
    if failure == "insert":
        monkeypatch.setattr(fhir.db, "status", AsyncMock(side_effect=RuntimeError("insert_failed")))
    invocation = fhir._execute_affected_npi_insert(
        build, projection_sql="SELECT projection", insert_sql="INSERT", params={}
    )
    if failure:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
            await invocation
        assert sum(admission.wal_tracker.pending_control_wal_bytes.values()) == 73728
        assert admission.wal_tracker.unresolved_window
    else:
        assert await invocation == 0
        assert not admission.wal_tracker.pending_control_wal_bytes
        assert not admission.wal_tracker.unresolved_window
    assert events[:2] == ["transaction", "checkpoint_lock"]
    assert admission.wal_tracker.accounted_control_operation_counts == {"affected_npi_payload": 1}
