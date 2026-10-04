# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Initial publication refuses drift and preserves uncertain completion charges."""

import asyncio
import datetime
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_replay as cms_replay
from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as contract
from process import provider_directory_profile_initial_guards as guards
from process import provider_directory_profile_selection as selection
from tests.provider_directory_profile_capacity_signing_guard_test_support import synthetic_profile_execution
from tests.test_provider_directory_profile_bounded_capacity import admitted_window
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_capacity_runtime import _geometry_inputs
from tests.test_provider_directory_profile_control_capacity import _control_wal_plan_input
from tests.test_provider_directory_profile_initial import _geometry, _target, fhir
from tests.test_provider_directory_profile_initial_replay import _selection


def _initial_geometry(execution, initial_targets):
    geometry = replace(
        _geometry(),
        initial_target_state_sha256=contract.target_state_sha256(initial_targets.payload),
        selection_proof_id=execution.attestation.proof_id,
        profile_input_digest=execution.attestation.profile_input_digest,
        profile_schema_version=execution.attestation.profile_schema_version,
        profile_strategy_version=execution.attestation.profile_strategy_version,
        profile_as_of=execution.attestation.desired_profile_as_of,
        sql_contract_digest=fhir._provider_directory_profile_sql_contract_digest(),
        control_wal_plan_input_hash=capacity.profile_control_wal_plan_input_hash(_control_wal_plan_input()),
    )
    projection = capacity.project_profile_control_wal_capacity(geometry, _control_wal_plan_input())
    geometry = replace(
        geometry,
        control_wal_upper_bound_bytes=projection.total_control_wal_bytes,
        control_metadata_data_upper_bound_bytes=projection.total_control_metadata_data_bytes,
    )
    return geometry, capacity.project_profile_control_wal_capacity(geometry, _control_wal_plan_input())


def _initial_build(execution, geometry):
    source_pairs = tuple((pair["source_id"], pair["dataset_id"]) for pair in execution.attestation.pairs)
    build_id = "pdpb_" + "a" * 32
    return fhir._ProviderDirectoryProfileBuild(
        schema="synthetic",
        generation_id="pdprofile_" + hashlib.sha256(f"{build_id}:{geometry.profile_as_of}".encode()).hexdigest()[:32],
        source_ids=tuple(source_id for source_id, _dataset in source_pairs),
        retained_source_ids=(),
        dataset_ids=tuple(dataset for _source, dataset in source_pairs),
        profile_as_of=geometry.profile_as_of,
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
        build_id=build_id,
        owner_run_id="run_" + "b" * 32,
        desired_source_vector=source_pairs,
        desired_source_context_vector=tuple((source_id, "12" * 32) for source_id, _dataset in source_pairs),
        desired_source_vector_hash=geometry.desired_source_vector_hash,
        desired_source_context_vector_hash=geometry.desired_context_vector_hash,
        selection_proof_id=execution.attestation.proof_id,
        authority_revision=execution.attestation.authority_revision,
        capacity_geometry_hash=capacity.capacity_geometry_hash(geometry),
        capacity_geometry_json=capacity.canonical_capacity_geometry_json(geometry),
    )


def _initial_stages(build, initial_targets):
    return [
        fhir.ProviderDirectoryPreparedArtifactStage(
            build.schema,
            table,
            relation,
            AsyncMock(),
            fhir.ProviderDirectoryArtifactBuildFence(oid),
            profile_initial_build=build,
        )
        for table, relation, oid in (
            (build.evidence_stage, fhir.profile_artifact.PROFILE_EVIDENCE_TABLE, initial_targets.evidence_target_oid),
            (build.profile_stage, fhir.profile_artifact.PROFILE_TABLE, initial_targets.profile_target_oid),
        )
    ]


def _initial_layouts(geometry):
    return {
        oid: SimpleNamespace(
            relation_oid=oid,
            exact_fingerprint=fingerprint,
            effective_tablespace_oids=(geometry.tablespace_oid,),
            main_index_pages=(1,),
            toast_index_pages=(1,),
        )
        for oid, fingerprint in (
            (20001, "aa" * 32),
            (20002, "bb" * 32),
            (30001, "ee" * 32),
            (30002, "ff" * 32),
            (geometry.initial_receipt_oid, geometry.initial_receipt_storage_fingerprint),
            (geometry.serving_generation_oid, "78" * 32),
        )
    }


def _initial_database(geometry):

    async def scalar(statement, **values):
        if "pg_total_relation_size" in statement:
            return 8192
        if "AS regclass" in statement:
            return geometry.initial_receipt_oid
        if "pg_current_wal_insert_lsn" in statement:
            return "0/1"
        return False

    return SimpleNamespace(
        scalar=AsyncMock(side_effect=scalar),
        first=AsyncMock(return_value=None),
        all=AsyncMock(return_value=[]),
        status=AsyncMock(return_value="INSERT 0 1"),
    )


def _install_initial_relation_mocks(monkeypatch, build, layout_by_oid):
    stage_oid_by_table = {
        build.evidence_stage: 30001,
        build.profile_stage: 30002,
        fhir.profile_artifact.PROFILE_EVIDENCE_TABLE: 30001,
        fhir.profile_artifact.PROFILE_TABLE: 30002,
    }
    monkeypatch.setattr(
        fhir,
        "_provider_directory_relation_oid",
        AsyncMock(side_effect=lambda _schema, table: stage_oid_by_table.get(table)),
    )
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_relation_storage_fingerprint",
        AsyncMock(side_effect=lambda oid, **_kwargs: layout_by_oid[oid]),
    )


@pytest.fixture
def initial_case(admitted_window, monkeypatch):
    """Bind a synthetic initial admission and restore its executor context after each case."""
    admission, _state = admitted_window
    execution = _selection("2026-07-30")
    initial_targets = contract.InitialTargets(20001, 20002, _target())
    geometry, projection = _initial_geometry(execution, initial_targets)
    build = _initial_build(execution, geometry)
    admission = replace(
        admission,
        geometry=geometry,
        control_wal_projection=projection,
        build_id=build.build_id,
        run_id=build.owner_run_id,
        admitted_identity=SimpleNamespace(initial_targets=initial_targets),
        lease=SimpleNamespace(attestation_id="synthetic-attestation", lease_digest="34" * 32, nonce="56" * 32),
    )
    layout_by_oid = _initial_layouts(geometry)
    database = _initial_database(geometry)
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setattr(
        fhir, "_profile_capacity_observed_settings", AsyncMock(return_value={"statement_timeout_ms": 8000})
    )
    monkeypatch.setattr(fhir, "_apply_provider_directory_profile_capacity_settings", AsyncMock())
    monkeypatch.setattr(fhir, "_provider_directory_profile_serving_state", AsyncMock(return_value=None))
    monkeypatch.setattr(fhir, "_locked_profile_adoption_target_oids", AsyncMock(return_value=(20002, 20001)))
    _install_initial_relation_mocks(monkeypatch, build, layout_by_oid)
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_stage_metrics",
        AsyncMock(return_value={"evidence_rows": 2, "profile_rows": 1}),
    )
    for name in (
        "_admission_database_guard",
        "_assert_provider_directory_profile_checkpoint_ready",
        "_assert_provider_directory_profile_capacity_consumption",
        "_assert_provider_directory_profile_capacity_scratch",
        "_lock_profile_capacity_preflight_state",
        "_delete_provider_directory_profile_build_checkpoint",
        "_validate_profile_delta_total_wal",
    ):
        monkeypatch.setattr(fhir, name, AsyncMock())
    admission_token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
    execution_token = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution)
    try:
        yield SimpleNamespace(
            admission=admission,
            geometry=geometry,
            target=initial_targets,
            build=build,
            stages=_initial_stages(build, initial_targets),
            fence=fhir.ProviderDirectoryArtifactDatasetFence(()),
            db=database,
            layout_by_oid=layout_by_oid,
            execution=execution,
        )
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(execution_token)
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(admission_token)


async def _receipt(case):
    cutover = await initial.prepare_cutover(fhir, case.stages, case.fence)
    case.db.status.reset_mock()
    serving = initial._serving_values(fhir, cutover)
    _sources, _contexts, payload = await initial._initial_commit_payload(
        fhir, cutover, case.build, case.admission, serving, case.fence
    )
    return {
        "build_id": case.build.build_id,
        "run_id": case.build.owner_run_id,
        "generation_id": case.build.generation_id,
        "attestation_id": case.admission.lease.attestation_id,
        "payload": payload,
        "valid_hash": True,
        "publication_xid": 123,
        "committed_at": datetime.datetime(2026, 7, 30, tzinfo=datetime.timezone.utc),
    }


def _current_serving(case, receipt):
    return SimpleNamespace(
        **receipt["payload"]["serving"],
        source_vector=case.build.desired_source_vector,
        source_context_vector=case.build.desired_source_context_vector,
        capacity_geometry_hash=case.build.capacity_geometry_hash,
        capacity_geometry_json=case.build.capacity_geometry_json,
    )


@pytest.mark.parametrize(
    "existing,observations", [("serving", []), ("common_history", [True, True]), ("receipt", [False, True])]
)
async def test_existing_publication_blocks_initial_target_capture(initial_case, existing, observations):
    case = initial_case
    if existing == "serving":
        fhir._provider_directory_profile_serving_state.return_value = object()
    case.db.scalar.side_effect = observations
    with pytest.raises(RuntimeError, match="initial_" + existing + "_exists"):
        await initial.capture_targets(fhir, case.build.schema)
    fhir._locked_profile_adoption_target_oids.assert_not_awaited()
    case.db.status.assert_not_awaited()


@pytest.mark.parametrize("fault", ["descriptor", "stage_pair", "no_admission", "owner", "build", "targets"])
async def test_metadata_lock_requires_exact_stage_and_executor_binding(initial_case, monkeypatch, fault):
    case = initial_case
    stages, admission = case.stages, case.admission
    if fault == "descriptor":
        stages = [replace(stage, profile_initial_build=None) for stage in stages]
    elif fault == "stage_pair":
        stages = stages[:1]
    elif fault == "no_admission":
        admission = None
    else:
        admission = replace(
            admission,
            **{
                "owner": {"run_id": "run_" + "c" * 32},
                "build": {"build_id": "pdpb_" + "d" * 32},
                "targets": {"admitted_identity": SimpleNamespace(initial_targets=None)},
            }[fault],
        )
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
    with pytest.raises(RuntimeError, match="initial_(descriptor_required|stage_pair_invalid|admission_required)"):
        await initial.lock_metadata(fhir, stages)
    fhir._lock_profile_capacity_preflight_state.assert_not_awaited()
    case.db.status.assert_not_awaited()


async def test_metadata_lock_and_forecast_bind_both_replacement_stages(initial_case):
    case = initial_case
    await initial.lock_metadata(fhir, case.stages)
    assert "NOWAIT" in case.db.status.await_args.args[0]
    cutover = await initial.prepare_cutover(fhir, case.stages, case.fence)
    assert cutover["oids"] == {stage.target_relation: oid for stage, oid in zip(case.stages, (30001, 30002))}
    assert cutover["counts"] == {"evidence_rows": 2, "profile_rows": 1}
    assert cutover["forecast"]["initial_target_state_sha256"] == contract.target_state_sha256(case.target.payload)
    assert cutover["forecast_hash"] == fhir._identity_hash(cutover["forecast"])
    assert cutover["projection"].commit_envelope_bytes == case.geometry.postgres_block_size_bytes * 5
    fhir._assert_provider_directory_profile_checkpoint_ready.assert_awaited_once_with(
        case.build, case.stages[0].build_fence, case.stages[1].build_fence
    )
    scratch = fhir._assert_provider_directory_profile_capacity_scratch
    assert scratch.await_count == 2
    scratch.assert_has_awaits(
        [
            call(
                "evidence_stage",
                (fhir._unscoped_qt(case.build.schema, case.build.evidence_stage),),
                observed_rows=2,
                maximum_rows=case.geometry.max_evidence_rows,
            ),
            call(
                "profile_stage",
                (fhir._unscoped_qt(case.build.schema, case.build.profile_stage),),
                observed_rows=1,
                maximum_rows=case.geometry.max_profile_rows,
            ),
        ]
    )


def test_initial_geometry_inputs_bind_the_snapshot_and_receipt_storage(initial_case):
    case = initial_case
    ordinary = _geometry_inputs(tablespace_oid=case.geometry.tablespace_oid)
    inputs = initial.geometry_inputs(
        fhir, case.admission.admitted_identity, ordinary, case.layout_by_oid[case.geometry.initial_receipt_oid]
    )
    assert inputs.initial_target_state_sha256 == contract.target_state_sha256(case.target.payload)
    assert inputs.initial_receipt_oid == case.geometry.initial_receipt_oid
    assert inputs.initial_receipt_storage_fingerprint == case.geometry.initial_receipt_storage_fingerprint
    assert inputs.selection_proof_id == ordinary.selection_proof_id


async def test_commit_payload_requires_the_publication_owner_dataset_fence(initial_case):
    case = initial_case
    cutover = await initial.prepare_cutover(fhir, case.stages, case.fence)
    case.db.status.reset_mock()
    with pytest.raises(RuntimeError, match="initial_fence_missing"):
        await initial._initial_commit_payload(
            fhir, cutover, case.build, case.admission, initial._serving_values(fhir, cutover), None
        )
    case.db.status.assert_not_awaited()


@pytest.mark.parametrize("observed", [None, 0, -1, "8000", True])
async def test_initial_preparation_refuses_unbounded_or_invalid_sql_ceiling(initial_case, observed):
    fhir._profile_capacity_observed_settings.return_value = {"statement_timeout_ms": observed}
    with pytest.raises(RuntimeError, match="initial_statement_timeout_invalid"):
        await initial.prepare_cutover(fhir, initial_case.stages, initial_case.fence)
    fhir._apply_provider_directory_profile_capacity_settings.assert_not_awaited()
    fhir._provider_directory_profile_stage_metrics.assert_not_awaited()
    initial_case.db.status.assert_not_awaited()


@pytest.mark.parametrize("candidate", [False, True])
@pytest.mark.parametrize("remaining_wall", [0.25, 30])
async def test_initial_live_wall_preserves_short_budget_and_signed_deadline(
    initial_case, monkeypatch, candidate, remaining_wall
):
    case = initial_case
    cutover = await initial.prepare_cutover(fhir, case.stages, case.fence)
    monkeypatch.setattr(fhir, "_profile_capacity_remaining_ms", AsyncMock(return_value=1500))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + remaining_wall
    timeout = SimpleNamespace(when=lambda: deadline, reschedule=Mock())
    fence = fhir.ProviderDirectoryArtifactDatasetFence((), should_select_validated_candidates=candidate)
    before = loop.time()
    await initial.arm_cutover(fhir, cutover, timeout, fence)
    armed = timeout.reschedule.call_args.args[0]
    assert armed <= deadline
    assert armed <= loop.time() + (12 if candidate else 2)
    if remaining_wall < 2:
        assert armed == deadline
    else:
        assert armed >= before + (12 if candidate else 2)
    case.db.status.assert_awaited_with("SET LOCAL statement_timeout='500ms'")


@pytest.mark.parametrize(
    "fault", [None, "target", "layout", "stage", "checkpoint", "consumption", "database", "serving"]
)
async def test_initial_live_guards_refuse_drift_without_repeating_census(initial_case, monkeypatch, fault):
    case = initial_case
    capture = AsyncMock(wraps=initial.capture_targets)
    monkeypatch.setattr(initial, "capture_targets", capture)
    cutover = await initial.prepare_cutover(fhir, case.stages, case.fence)
    live_oid_by_target = {stage.target_relation: stage.build_fence.target_oid for stage in case.stages}
    stage_oid_by_table = {stage.stage_table: cutover["oids"][stage.target_relation] for stage in case.stages}
    if fault == "target":
        live_oid_by_target[case.stages[0].target_relation] += 1
    if fault == "layout":
        case.layout_by_oid[20001].exact_fingerprint = "90" * 32
    if fault == "stage":
        stage_oid_by_table[case.stages[0].stage_table] += 1
    if fault in {"checkpoint", "consumption", "database"}:
        name = {
            "checkpoint": "_assert_provider_directory_profile_checkpoint_ready",
            "consumption": "_assert_provider_directory_profile_capacity_consumption",
            "database": "_admission_database_guard",
        }[fault]
        getattr(fhir, name).side_effect = RuntimeError("synthetic_guard_refused")
    if fault == "serving":
        fhir._provider_directory_profile_serving_state.return_value = object()
    fhir._provider_directory_relation_oid.side_effect = lambda _schema, name: {
        **live_oid_by_target,
        **stage_oid_by_table,
    }.get(name)
    wal_read = case.db.scalar.side_effect

    async def scalar(statement, **values):
        return "0/2" if "pg_current_wal_insert_lsn" in statement else await wal_read(statement, **values)

    case.db.scalar.side_effect = scalar
    if fault:
        error_by_fault = {
            "target": "provider_directory_profile_initial_targets_changed",
            "layout": "provider_directory_profile_initial_targets_changed",
            "stage": "provider_directory_profile_initial_stage_changed",
            "checkpoint": "synthetic_guard_refused",
            "consumption": "synthetic_guard_refused",
            "database": "synthetic_guard_refused",
            "serving": "provider_directory_profile_initial_serving_exists",
        }
        with pytest.raises(RuntimeError, match="^" + error_by_fault[fault] + "$"):
            await initial.begin_cutover(fhir, cutover, case.stages)
        assert cutover["wal_start"] == "0/1"
    else:
        await initial.begin_cutover(fhir, cutover, case.stages)
        assert cutover["wal_start"] == "0/2"
    capture.assert_awaited_once()
    fhir._provider_directory_profile_stage_metrics.assert_awaited_once()


async def test_initial_composite_preparation_uses_earlier_paired_deadline(initial_case, monkeypatch):
    case = initial_case
    events = []

    @asynccontextmanager
    async def authorized(*args):
        events.append("authorized")
        yield

    @asynccontextmanager
    async def transaction():
        events.append("transaction")
        yield object()

    admission = SimpleNamespace(publication=authorized)
    prepared = SimpleNamespace(
        fhir=fhir,
        stages=case.stages,
        profile_delta=None,
        fence=case.fence,
        nonprofile_admission=admission,
        assert_ready=AsyncMock(),
    )
    monkeypatch.setattr(case.db, "transaction", transaction, raising=False)
    monkeypatch.setattr(fhir, "_profile_capacity_remaining_ms", AsyncMock(return_value=15000))
    monkeypatch.setattr(fhir, "_configure_provider_directory_artifact_promotion", AsyncMock())
    monkeypatch.setattr(initial, "lock_metadata", AsyncMock())
    monkeypatch.setattr(publication, "remaining_build_seconds", AsyncMock(return_value=0.25))
    monkeypatch.setattr(publication, "_lock_retained_relations", AsyncMock())
    monkeypatch.setattr(publication.receipts, "assert_native_dependencies", AsyncMock())
    monkeypatch.setattr(publication.receipts, "read_current_receipt", AsyncMock(return_value=None))
    execution = SimpleNamespace(attestation=SimpleNamespace(desired_cms_dataset=None))
    async with publication._publication_transaction(fhir, execution, prepared, {}, None, None) as (_session, timeout):
        assert 0 < timeout.when() - asyncio.get_running_loop().time() <= 0.25
        assert events == ["authorized", "transaction"]
        prepared.assert_ready.assert_awaited_once_with(cutover=True)
    publication.remaining_build_seconds.assert_awaited_once_with(fhir, admission)


async def test_initial_composite_refuses_expired_witness_before_live_locks(initial_case, monkeypatch):
    prepared = SimpleNamespace(
        stages=initial_case.stages,
        profile_delta=None,
        assert_ready=AsyncMock(side_effect=RuntimeError("synthetic_witness_expired")),
    )
    monkeypatch.setattr(
        initial_case.db, "_transaction_binding", lambda: SimpleNamespace(session=object()), raising=False
    )
    monkeypatch.setattr(publication.coverage, "assert_sealed_cms_candidate_coverage", AsyncMock())
    lock_live = AsyncMock()
    monkeypatch.setattr(publication, "_lock_live_swap_relations", lock_live)

    async def prepared_bundle(*args, **options):
        await options["before_swaps"]()

    monkeypatch.setattr(publication, "apply_prepared_artifact_bundle", prepared_bundle)
    with pytest.raises(RuntimeError, match="synthetic_witness_expired"):
        await publication._apply_prepared_results(fhir, None, prepared, None, {}, None, None)
    prepared.assert_ready.assert_awaited_once_with(cutover=True)
    lock_live.assert_not_awaited()


@pytest.mark.parametrize(
    "fault",
    [
        "targets",
        "receipt_oid",
        "receipt_fingerprint",
        "tablespace",
        "old_target",
        "checkpoint",
        "consumption",
        "unresolved",
    ],
)
async def test_cutover_refuses_drift_before_publication(initial_case, monkeypatch, fault):
    case = initial_case
    error = fault
    if fault == "targets":
        case.layout_by_oid[20001].exact_fingerprint = "90" * 32
        error = "targets_changed"
    elif fault.startswith("receipt_") or fault == "tablespace":
        layout = case.layout_by_oid[case.geometry.initial_receipt_oid]
        setattr(
            layout,
            {
                "receipt_oid": "relation_oid",
                "receipt_fingerprint": "exact_fingerprint",
                "tablespace": "effective_tablespace_oids",
            }[fault],
            {"receipt_oid": 29001, "receipt_fingerprint": "90" * 32, "tablespace": (20001,)}[fault],
        )
        error = "receipt_storage_changed"
    elif fault == "old_target":
        fhir._provider_directory_relation_oid.side_effect = None
        fhir._provider_directory_relation_oid.return_value = 40001
        error = "old_target_conflict"
    elif fault == "unresolved":
        case.admission.wal_tracker.unresolved_window = True
        error = "capacity_window_unresolved"
    else:
        name = {
            "checkpoint": "_assert_provider_directory_profile_checkpoint_ready",
            "consumption": "_assert_provider_directory_profile_capacity_consumption",
        }[fault]
        getattr(fhir, name).side_effect = RuntimeError(fault + "_changed")
    with pytest.raises(RuntimeError, match=error):
        await initial.prepare_cutover(fhir, case.stages, case.fence)
    assert all("IN SHARE MODE NOWAIT" in invocation.args[0] for invocation in case.db.status.await_args_list)
    fhir._delete_provider_directory_profile_build_checkpoint.assert_not_awaited()


async def test_unrelated_stages_do_not_select_initial_mode(initial_case):
    stage = replace(initial_case.stages[0], target_relation="synthetic_other", profile_initial_build=None)
    assert await initial.prepare_cutover(fhir, [stage], None) is None
    await initial.lock_metadata(fhir, [stage])
    async with initial.swap_window(fhir, None):
        assert fhir._provider_directory_profile_capacity_admission() is initial_case.admission
    assert await initial.is_committed(fhir, [stage])
    initial_case.db.status.assert_not_awaited()


async def test_missing_serving_insert_does_not_write_receipt_or_retire_checkpoint(initial_case):
    case = initial_case
    cutover = await initial.prepare_cutover(fhir, case.stages, case.fence)
    case.db.status.reset_mock()
    case.db.status.return_value = "INSERT 0 0"
    with pytest.raises(RuntimeError, match="initial_serving_insert_missing"):
        await initial.finish_cutover(fhir, cutover, case.fence)
    assert case.db.status.await_count == 1
    assert "serving_generation" in case.db.status.await_args.args[0]
    fhir._delete_provider_directory_profile_build_checkpoint.assert_not_awaited()
    assert case.admission.wal_tracker.unresolved_window
    assert case.admission.wal_tracker.pending_metadata_wal_bytes == (
        cutover["projection"].wal_bytes + cutover["projection"].commit_envelope_bytes
    )


async def test_metadata_charge_cannot_settle_without_commit_reservation(admitted_window):
    admission, _state = admitted_window
    projection = capacity.ProviderDirectoryProfileMetadataProjection(100, 500, 100)
    with pytest.raises(RuntimeError, match="metadata_reservation_missing"):
        async with initial.metadata_window(fhir, admission, projection):
            admission.wal_tracker.pending_metadata_wal_bytes = 500
    assert admission.wal_tracker.unresolved_window
    assert admission.wal_tracker.pending_metadata_wal_bytes == 500


@pytest.mark.parametrize("fault", [None, "missing", "hash", "contract", "checkpoint", "common", "binding", "bounds"])
async def test_committed_receipt_requires_retired_checkpoint_and_closed_provenance(initial_case, fault):
    case = initial_case
    receipt = await _receipt(case)
    receipt_payload = receipt["payload"]
    if fault == "hash":
        receipt["valid_hash"] = False
    if fault == "contract":
        receipt_payload["contract_id"] = "synthetic.invalid"
    if fault == "binding":
        receipt_payload["run_id"] = "run_" + "c" * 32
    if fault == "bounds":
        receipt_payload["forecast"]["metadata_data_bytes"] = case.geometry.metadata_data_upper_bound_bytes + 1
        receipt_payload["actual"]["forecast_hash"] = fhir._identity_hash(receipt_payload["forecast"])
    if fault == "common":
        receipt_payload["source_vector"] = [{"source_id": "cms-npd", "dataset_id": "synthetic-dataset"}]
        receipt_payload["common_receipt_required"] = True
    receipt["payload"] = contract.canonical_json(receipt_payload)
    case.db.first.return_value = None if fault == "missing" else receipt
    case.db.scalar.side_effect = [case.geometry.initial_receipt_oid, fault == "checkpoint", fault != "common"]
    error_by_fault = {
        "hash": "receipt_invalid",
        "contract": "receipt_invalid",
        "checkpoint": "checkpoint_not_retired",
        "common": "common_receipt_missing",
        "binding": "receipt_binding_changed",
        "bounds": "receipt_bounds_invalid",
    }
    if fault in error_by_fault:
        with pytest.raises(RuntimeError, match=error_by_fault[fault]):
            await initial._committed_receipt(fhir, case.build.schema, case.build.build_id)
    else:
        committed_receipt = await initial._committed_receipt(fhir, case.build.schema, case.build.build_id)
        assert committed_receipt is None if fault == "missing" else committed_receipt["payload"] == receipt_payload
    case.db.status.assert_not_awaited()


@pytest.mark.parametrize("fault", ["missing", "generation", "owner", "attestation", "invalid", "cancel"])
async def test_completion_refuses_unbound_receipts_and_propagates_cancellation(initial_case, monkeypatch, fault):
    case = initial_case
    receipt = await _receipt(case)
    if fault in {"generation", "owner", "attestation"}:
        receipt[{"generation": "generation_id", "owner": "run_id", "attestation": "attestation_id"}[fault]] = "changed"
    fhir._provider_directory_profile_serving_state.return_value = _current_serving(case, receipt)
    reader = AsyncMock(return_value=None if fault == "missing" else receipt)
    if fault in {"invalid", "cancel"}:
        reader.side_effect = asyncio.CancelledError() if fault == "cancel" else RuntimeError("invalid_receipt")
    monkeypatch.setattr(initial, "_committed_receipt", reader)
    consumed = AsyncMock()
    monkeypatch.setattr(fhir, "_replay_bound_consumption", consumed)
    if fault == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await initial.is_committed(fhir, case.stages)
    else:
        assert not await initial.is_committed(fhir, case.stages)
    consumed.assert_not_awaited()
    case.db.status.assert_not_awaited()


@pytest.mark.parametrize("fault", ["missing", "target_oid", "fingerprint"])
async def test_current_receipt_rejects_missing_serving_or_replaced_targets(initial_case, fault):
    case = initial_case
    receipt = await _receipt(case)
    if fault != "missing":
        if fault == "target_oid":
            receipt["payload"]["serving"]["evidence_target_oid"] = 30002
        fhir._provider_directory_profile_serving_state.return_value = _current_serving(case, receipt)
        if fault == "fingerprint":
            case.layout_by_oid[30001].exact_fingerprint = "90" * 32
    error = "receipt_not_current" if fault == "missing" else "receipt_targets_changed"
    with pytest.raises(RuntimeError, match="^provider_directory_profile_initial_" + error + "$"):
        await initial._assert_current_receipt(fhir, case.build.schema, receipt)
    case.db.status.assert_not_awaited()


def _legacy_result():
    result = selection.profile_selection_result(
        synthetic_profile_execution(),
        profile_generation_id="pdprofile_" + "a" * 32,
        profile_rows=1,
        profile_source_evidence_rows=2,
        profile_as_of="2026-07-30",
    )
    result.pop("profile_as_of")
    return result


@pytest.mark.parametrize("fault", [None, "unfinished", "unattested"])
async def test_legacy_adoption_requires_finished_attested_history(initial_case, monkeypatch, fault):
    case = initial_case
    legacy_result = _legacy_result()
    case.db.first.return_value = (
        None
        if fault == "unattested"
        else {
            "run_id": "run_" + "c" * 32,
            "finished_at": None if fault == "unfinished" else "2026-07-30",
            "result": json.dumps(legacy_result),
        }
    )
    case.db.scalar.side_effect = None
    case.db.scalar.return_value = fault == "unattested"
    adoption = AsyncMock()
    monkeypatch.setattr(fhir, "_profile_adoption_targets", adoption)
    if fault:
        with pytest.raises(RuntimeError, match="history_unfinished|unattested_targets"):
            await initial._capture_legacy_history(fhir, "synthetic", "profile", "evidence")
        adoption.assert_not_awaited()
    else:
        history, evidence_rows, profile_rows = await initial._capture_legacy_history(
            fhir, "synthetic", "profile", "evidence"
        )
        assert (evidence_rows, profile_rows) == (2, 1)
        assert history["result"] == legacy_result and history["profile_as_of"] is None
        assert history["result_sha256"] == contract.result_sha256(legacy_result)
        adoption.assert_awaited_once_with(
            "synthetic", generation_id=legacy_result["profile_generation_id"], profile_rows=1, evidence_rows=2
        )


@pytest.mark.parametrize("fault", ["sources", "fence", "common", "context"])
async def test_replay_source_authority_cannot_be_inferred(initial_case, monkeypatch, fault):
    case = initial_case
    receipt = await _receipt(case)
    if fault == "sources":
        receipt["payload"]["source_vector"] = []
    if fault == "fence":
        receipt["payload"]["common_receipt_required"] = False
    if fault == "common":
        receipt["payload"]["common_receipt_required"] = True
        monkeypatch.setattr(cms_replay, "_registered_execution", AsyncMock())
        monkeypatch.setattr(cms_replay, "_historical_common", AsyncMock(return_value=None))
    source_context = (
        ((), ()) if fault == "context" else (case.build.desired_source_vector, case.build.desired_source_context_vector)
    )
    monkeypatch.setattr(
        fhir, "_provider_directory_profile_replay_source_context", AsyncMock(return_value=source_context)
    )
    error = {
        "sources": "replay_sources_changed",
        "context": "replay_sources_changed",
        "fence": "fence_missing",
        "common": "common_receipt_missing",
    }[fault]
    with pytest.raises(RuntimeError, match="^provider_directory_profile_initial_" + error + "$"):
        await initial._assert_replay_sources(
            fhir, "synthetic", receipt, case.execution, case.fence if fault in {"sources", "context"} else None, True
        )
    case.db.status.assert_not_awaited()


@pytest.mark.parametrize("fault", ["consumption", "geometry", "lease"])
async def test_replay_binds_original_owner_geometry_and_consumed_lease(initial_case, monkeypatch, fault):
    case = initial_case
    receipt = await _receipt(case)
    monkeypatch.setattr(
        fhir, "_replay_current_consumption", AsyncMock(return_value=object() if fault == "consumption" else None)
    )
    consumed = AsyncMock(return_value={"synthetic": "consumed"})
    monkeypatch.setattr(fhir, "_replay_bound_consumption", consumed)
    monkeypatch.setattr(fhir, "_assert_replay_owner", AsyncMock())
    if fault == "geometry":
        receipt["payload"]["capacity_geometry"] = _geometry_payload()
    lease = SimpleNamespace(
        attestation_id="different-attestation" if fault == "lease" else case.admission.lease.attestation_id,
        lease_digest=case.admission.lease.lease_digest,
        nonce=case.admission.lease.nonce,
        signing_preflight_guard={"healthcare_receipt": {"capacity_geometry": receipt["payload"]["capacity_geometry"]}},
    )
    verifier = Mock(return_value=lease)
    monkeypatch.setattr(fhir, "_verified_provider_directory_profile_replay_lease", verifier)
    error = {
        "consumption": "provider_directory_profile_replay_current_consumption_conflict",
        "geometry": "provider_directory_profile_initial_replay_geometry_invalid",
        "lease": "provider_directory_profile_initial_replay_binding_changed",
    }[fault]
    with pytest.raises(RuntimeError, match="^" + error + "$"):
        await initial._replay_authority(fhir, "synthetic", "consumption", "run_" + "c" * 32, case.execution, receipt)
    if fault == "consumption":
        consumed.assert_not_awaited()
    else:
        consumed.assert_awaited_once_with("consumption", receipt["run_id"], receipt["build_id"])
    if fault in {"consumption", "geometry"}:
        verifier.assert_not_called()
    else:
        verifier.assert_called_once()


@pytest.mark.parametrize("rows", [[], [{"build_id": "a"}, {"build_id": "b"}]])
async def test_replay_requires_one_unambiguous_receipt(initial_case, monkeypatch, rows):
    case = initial_case
    monkeypatch.setattr(fhir, "_replay_control_run", AsyncMock())
    case.db.all.return_value = rows
    if rows:
        with pytest.raises(RuntimeError, match="replay_ambiguous"):
            await initial.committed_replay(fhir, "synthetic", "consumption", "run", case.execution, case.fence)
    else:
        assert (
            await initial.committed_replay(fhir, "synthetic", "consumption", "run", case.execution, case.fence) is None
        )
    case.db.first.assert_not_awaited()


@pytest.mark.parametrize("fault", ["fence", "uncommitted"])
async def test_current_replay_requires_the_exact_committed_dataset_fence(initial_case, monkeypatch, fault):
    case = initial_case
    receipt = await _receipt(case)
    if fault == "fence":
        receipt["payload"]["dataset_fence_sha256"] = "90" * 32
        receipt["payload"]["forecast"]["dataset_fence_sha256"] = "90" * 32
        receipt["payload"]["actual"]["forecast_hash"] = fhir._identity_hash(receipt["payload"]["forecast"])
    case.db.first.return_value = receipt
    case.db.all.return_value = [{"build_id": receipt["build_id"]}]
    case.db.scalar.side_effect = [case.geometry.initial_receipt_oid, False, True]
    fhir._provider_directory_profile_serving_state.return_value = _current_serving(case, receipt)
    for name, observation in (
        ("_replay_control_run", None),
        ("_replay_current_consumption", None),
        ("_replay_bound_consumption", {}),
        ("_assert_replay_owner", None),
        ("_provider_directory_profile_capacity_database_identity", case.geometry),
        (
            "_provider_directory_profile_replay_source_context",
            (case.build.desired_source_vector, case.build.desired_source_context_vector),
        ),
        ("_is_provider_directory_dataset_cutover_committed", False),
    ):
        monkeypatch.setattr(fhir, name, AsyncMock(return_value=observation))
    lease = SimpleNamespace(
        **vars(case.admission.lease),
        signing_preflight_guard={"healthcare_receipt": {"capacity_geometry": receipt["payload"]["capacity_geometry"]}},
    )
    monkeypatch.setattr(fhir, "_verified_provider_directory_profile_replay_lease", Mock(return_value=lease))
    monkeypatch.setattr(fhir, "_assert_provider_directory_profile_capacity_tablespaces", Mock())
    monkeypatch.setattr(fhir, "_assert_replay_timeline", Mock())
    with pytest.raises(RuntimeError, match="initial_replay_dataset_changed"):
        await initial.committed_replay(
            fhir, "synthetic", "consumption", case.build.owner_run_id, case.execution, case.fence
        )
    if fault == "fence":
        fhir._is_provider_directory_dataset_cutover_committed.assert_not_awaited()
    else:
        fhir._is_provider_directory_dataset_cutover_committed.assert_awaited_once_with(case.fence)
    case.db.status.assert_not_awaited()


@pytest.mark.parametrize("fault", ["selection", "serving", "database"])
async def test_replay_selection_and_database_identity_must_still_match(initial_case, monkeypatch, fault):
    case = initial_case
    receipt = await _receipt(case)
    if fault == "database":
        tablespaces = Mock()
        monkeypatch.setattr(fhir, "_assert_provider_directory_profile_capacity_tablespaces", tablespaces)
        with pytest.raises(RuntimeError, match="replay_database_changed"):
            initial._assert_replay_database(
                fhir,
                replace(case.geometry, database_oid=case.geometry.database_oid + 1),
                case.geometry,
                case.admission.lease,
                {},
                {},
            )
        tablespaces.assert_not_called()
    else:
        if fault == "selection":
            receipt["payload"]["serving"]["control_generation"] += 1
            fhir._provider_directory_profile_serving_state.return_value = _current_serving(case, receipt)
        error = "replay_selection_changed" if fault == "selection" else "replay_serving_missing"
        with pytest.raises(RuntimeError, match="^provider_directory_profile_initial_" + error + "$"):
            await initial._replay_selection_state(
                fhir, "synthetic", receipt["payload"]["serving"], case.execution, receipt
            )


@pytest.mark.parametrize(
    "change,error",
    [
        ({"generation": True}, "legacy_result_invalid"),
        ({"status": "failed"}, "legacy_result_invalid"),
        ({"row_counts": {"profile_rows": 1, "profile_source_evidence_rows": 2, "extra": 1}}, "legacy_result_invalid"),
    ],
)
def test_legacy_result_rejects_ambiguous_completion(change, error):
    with pytest.raises(ValueError, match=error):
        contract.validated_legacy_result({**_legacy_result(), **change})


@pytest.mark.parametrize(
    "change,error",
    [
        ({"extra": 1}, "target_fields_invalid"),
        ({"serving_singleton_absent": 1}, "target_absence_invalid"),
        ({"evidence_target_oid": True}, "target_oid_invalid"),
        ({"profile_target_oid": 2**32}, "target_oid_invalid"),
        ({"profile_target_oid": 20001}, "target_oid_collision"),
        ({"resolution": "inferred"}, "resolution_invalid"),
        ({"profile_target_storage_fingerprint": "not-a-fingerprint"}, "target_fingerprint_invalid"),
        ({"evidence_target_bytes": True}, "target_size_invalid"),
    ],
)
def test_target_snapshot_is_closed_and_preserves_absence_identity(change, error):
    with pytest.raises(ValueError, match=error):
        contract.validated_target_state({**_target(), **change})


@pytest.mark.parametrize("fault", ["digest", "rows"])
def test_legacy_snapshot_must_bind_the_whole_result_and_physical_counts(fault):
    result = _legacy_result()
    target_by_field = {
        **_target(),
        "resolution": "legacy_as_of_unknown",
        "evidence_rows": 2,
        "profile_rows": 1,
        "historical_publication": {
            "run_id": "run_" + "c" * 32,
            "result_sha256": contract.result_sha256(result),
            "result": result,
            "profile_as_of": None,
            "temporal_metadata": "not_recorded_by_producer",
        },
    }
    if fault == "digest":
        target_by_field["historical_publication"]["result_sha256"] = "90" * 32
    else:
        target_by_field["evidence_rows"] += 1
    with pytest.raises(ValueError, match="legacy_history_binding_invalid"):
        contract.validated_target_state(target_by_field)
    assert "profile_as_of" not in result


@pytest.mark.parametrize("keys", [["1", "2", "3"], ["1", "2", "3", "3"], ["1", "2", "3", "4", "5"]])
def test_receipt_requires_exactly_four_distinct_uniqueness_indexes(keys):
    indexes = [{"relation_oid": 29000, "indkey": key} for key in keys]
    indexes.append({"relation_oid": 29001, "indkey": "4"})
    with pytest.raises(RuntimeError, match="initial_receipt_storage_shape_changed"):
        guards._assert_receipt_indexes(29000, indexes)


def test_initial_geometry_identifies_its_observed_receipt_contract():
    geometry = _geometry()
    assert geometry.cutover_forecast_contract_id == contract.FORECAST_CONTRACT
    assert geometry.cutover_actual_contract_id == contract.ACTUAL_CONTRACT
