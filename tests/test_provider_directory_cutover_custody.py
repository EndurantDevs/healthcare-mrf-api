# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consumed atomic cutover owners preserve native commit and acknowledgement truth."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.test_provider_directory_control_custody import control_wave as control_wave
from tests.test_provider_directory_owned_evidence_wave import custody, fhir
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave

receipt = importlib.import_module("process.provider_directory_profile_serving_receipt")

pytestmark = pytest.mark.asyncio


def install_existing_savepoints(monkeypatch, database):
    """Keep the pre-existing nested transaction and limits behavior in the native fixture."""

    @asynccontextmanager
    async def begin_nested(session):
        assert session.in_transaction() and not session.in_nested_transaction()
        session.has_nested_transaction = True
        session.bind.events.append("savepoint_begin")
        try:
            yield
        finally:
            session.has_nested_transaction = False
            session.bind.events.append("savepoint_end")

    async def limits(_admission):
        session = database._transaction_binding().session
        assert session.in_transaction()
        session.bind.events.append("limits")

    monkeypatch.setattr(custody.AsyncSession, "begin_nested", begin_nested)
    monkeypatch.setattr(fhir, "_apply_provider_directory_profile_capacity_settings", limits)


@pytest.fixture
def cutover_wave(control_wave, monkeypatch):
    """Retain the original caller bodies around one real owner in the pool-one fixture."""
    state, database, admission, *_ = control_wave
    events = []

    install_existing_savepoints(monkeypatch, database)
    stages = (SimpleNamespace(schema="synthetic", stage_table="scratch", target_relation="target"),)
    delta = SimpleNamespace(generation_id="synthetic_generation")
    fence = SimpleNamespace(identity="synthetic_fence")
    monkeypatch.setattr(fhir, "_ordered_provider_directory_artifact_bundle", lambda original: original)
    monkeypatch.setattr(
        fhir, "_provider_directory_artifact_bundle_context", lambda *_: ("synthetic", ("target",), "1s", "2s")
    )

    async def configure(*_):
        events.append("configure")
        await fhir._apply_provider_directory_profile_capacity_settings(admission)

    async def lock_metadata(*_):
        events.append("lock_metadata")

    @asynccontextmanager
    async def continuity(*_):
        events.append("continuity_enter")
        try:
            yield
        finally:
            events.append("continuity_exit")

    async def apply(original_stages, *, profile_delta, cutover_timeout):
        assert original_stages is stages and profile_delta is delta
        assert custody.current_owned_wal_transaction(database) is not None
        assert database._transaction_binding().session.in_transaction()
        events.append("target_metadata")
        await database.status("UPDATE synthetic_target SET generation_id=:generation_id;", generation_id="new")
        return None

    monkeypatch.setattr(fhir, "_configure_provider_directory_artifact_promotion", configure)
    monkeypatch.setattr(fhir.profile_initial, "lock_metadata", lock_metadata)
    monkeypatch.setattr(fhir.profile_initial, "build_from_stages", Mock(return_value=None))
    monkeypatch.setattr(receipt, "ordinary_profile_receipt_continuity", continuity)
    monkeypatch.setattr(fhir, "_apply_prepared_artifact_bundle_in_transaction", apply)
    token = fhir._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(fence)
    try:
        yield state, database, admission, stages, delta, fence, events
    finally:
        fhir._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(token)


async def promote(fixture):
    _, _, _, stages, delta, _, _ = fixture
    await fhir._promote_provider_directory_artifact_bundle_transaction(stages, profile_delta=delta)


def outer_group(fixture):
    return fixture[2].wal_tracker.owned_control_transaction_groups[0]


async def test_atomic_bundle_has_one_original_native_owner_and_identity(cutover_wave):
    state, database, _, stages, delta, fence, events = cutover_wave
    await promote(cutover_wave)
    group = outer_group(cutover_wave)
    outcome = group["original_outcome"]
    assert group["identity"][0:2] == ("artifact_cutover", "bundle")
    assert group["identity"][2][0] is stages and group["identity"][2][1] is delta
    assert group["identity"][3] is fence and group["outcome"] is outcome
    assert group["session"] is outcome.session and group["connection"] is state.connections[0]
    assert group["driver"] is outcome.retained_driver and group["pid"] == outcome.retained_pid
    assert group["consumed"] and outcome.is_committed and outcome.cleanup_complete
    assert not group["accounting_authority"] and not group["reservation_refund"]
    assert events == ["configure", "lock_metadata", "continuity_enter", "target_metadata", "continuity_exit"]
    assert database._transaction_binding() is None
    assert state.connections[0].events.count("begin") == state.connections[0].events.count("commit") == 1


@pytest.mark.parametrize("mode", ["rollback", "cancel", "postcommit_sample", "unknown_commit", "restore_failure"])
async def test_atomic_owner_failure_preserves_native_outcome_and_held_exposure(cutover_wave, mode):
    state, _, _, _, _, _, events = cutover_wave
    state.mode = mode
    with pytest.raises(asyncio.CancelledError if mode == "cancel" else Exception):
        await promote(cutover_wave)
    group = outer_group(cutover_wave)
    outcome = group["original_outcome"]
    assert not group["consumed"] and group["status"] == "incomplete" and group["failure"] is not None
    assert outcome.cleanup_complete and events[-1] == "continuity_exit"
    assert not group["accounting_authority"] and not group["reservation_refund"]
    if mode in ["postcommit_sample", "restore_failure"]:
        assert outcome.is_committed and outcome.status == "committed_accounting_incomplete"
    elif mode == "unknown_commit":
        assert not outcome.is_committed and outcome.commit_state == "attempted"
    else:
        assert outcome.commit_state == "rolled_back"
    assert state.checkedout == 0 and all(connection.closed for connection in state.connections)


async def test_original_cutover_body_failure_object_survives_owner_wrapper(cutover_wave, monkeypatch):
    primary = OSError("synthetic cutover body failure")

    async def fail(*_, **__):
        raise primary

    monkeypatch.setattr(fhir, "_apply_prepared_artifact_bundle_in_transaction", fail)
    with pytest.raises(OSError) as caught:
        await promote(cutover_wave)
    group = outer_group(cutover_wave)
    assert caught.value is primary and group["failure"] is primary
    assert group["original_outcome"].commit_state == "rolled_back" and not group["consumed"]


@pytest.mark.parametrize("mode", ["postcommit_sample", "unknown_commit"])
async def test_original_semantic_ack_verifier_keeps_native_truth_held(cutover_wave, monkeypatch, mode):
    state, _, _, stages, delta, _, _ = cutover_wave
    state.mode = mode
    identities = (object(),)
    verify = AsyncMock(return_value=True)
    monkeypatch.setattr(
        fhir, "_capture_provider_directory_artifact_promotion_identities", AsyncMock(return_value=identities)
    )
    monkeypatch.setattr(fhir.profile_initial, "preparation_timeout_seconds", AsyncMock(return_value=1))
    monkeypatch.setattr(fhir, "_provider_directory_artifact_transaction_timeout_seconds", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(fhir, "_is_artifact_bundle_promotion_committed", verify)
    await fhir._promote_provider_directory_artifact_bundle(stages, profile_delta=delta)
    verify.assert_awaited_once_with(stages, identities, cutover_wave[5], delta)
    group = outer_group(cutover_wave)
    outcome = group["original_outcome"]
    assert not group["consumed"] and group["status"] == "incomplete"
    assert outcome.commit_state == ("confirmed" if mode == "postcommit_sample" else "attempted")
    assert outcome.status == (
        "committed_accounting_incomplete" if mode == "postcommit_sample" else "commit_uncertain_accounting_incomplete"
    )


async def test_borrowed_cutover_does_not_claim_future_outer_completion(cutover_wave):
    _, database, _, _, _, _, _ = cutover_wave
    async with custody.registry_owned_wal_transaction(database) as outcome:
        await promote(cutover_wave)
        group = outer_group(cutover_wave)
        assert group["original_outcome"] is outcome and not group["consumed"]
        assert not outcome.is_committed and outcome.measurement is None
    assert outcome.is_committed and outcome.cleanup_complete
    assert not group["consumed"] and group["status"] == "incomplete"


async def test_nested_control_is_not_completed_before_atomic_owner_exit(cutover_wave, monkeypatch):
    _, database, admission, stages, delta, _, _ = cutover_wave

    async def apply(original_stages, *, profile_delta, cutover_timeout):
        assert original_stages is stages and profile_delta is delta
        await fhir._provider_directory_profile_capacity_status("UPDATE synthetic_checkpoint SET cursor=2;")
        outer, inner = admission.wal_tracker.owned_control_transaction_groups
        assert inner["original_outcome"] is outer["original_outcome"]
        assert not inner["consumed"] and not outer["original_outcome"].is_committed
        assert database._transaction_binding().session.in_transaction()

    monkeypatch.setattr(fhir, "_apply_prepared_artifact_bundle_in_transaction", apply)
    await promote(cutover_wave)
    outer, inner = admission.wal_tracker.owned_control_transaction_groups
    assert outer["consumed"] and outer["original_outcome"].is_committed
    assert inner["original_outcome"] is outer["original_outcome"] and not inner["consumed"]


async def test_existing_cancellation_verifier_runs_without_losing_cancellation(cutover_wave, monkeypatch):
    state, _, _, stages, delta, fence, _ = cutover_wave
    state.mode = "cancel"
    identities = (object(),)
    resolver = AsyncMock()
    monkeypatch.setattr(
        fhir, "_capture_provider_directory_artifact_promotion_identities", AsyncMock(return_value=identities)
    )
    monkeypatch.setattr(fhir.profile_initial, "preparation_timeout_seconds", AsyncMock(return_value=1))
    monkeypatch.setattr(fhir, "_provider_directory_artifact_transaction_timeout_seconds", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(fhir, "_resolve_initial_cutover_cancellation", resolver)
    with pytest.raises(asyncio.CancelledError) as caught:
        await fhir._promote_provider_directory_artifact_bundle(stages, profile_delta=delta)
    resolver.assert_awaited_once_with(stages, fence, identities)
    group = outer_group(cutover_wave)
    assert group["failure"] is caught.value and not group["consumed"]
    assert group["original_outcome"].commit_state == "rolled_back"


async def test_original_single_stage_boundary_has_exact_native_owner(cutover_wave, monkeypatch):
    state, database, admission, _, _, fence, _ = cutover_wave
    events = []
    status = database.status

    async def original_status(statement, **params):
        if "limits" not in state.connections[0].events:
            await fhir._apply_provider_directory_profile_capacity_settings(admission)
        return await status(statement, **params)

    async def emit(name, *args):
        assert custody.current_owned_wal_transaction(database) is not None
        events.append(name)
        await database.status("UPDATE synthetic_metadata SET operation=:operation;", operation=name)

    monkeypatch.setattr(database, "status", original_status)
    monkeypatch.setattr(database, "scalar", AsyncMock(return_value=True))
    for name in (
        "_acquire_provider_directory_artifact_cutover_lock",
        "_verify_active_profile_selection_at_cutover",
        "_lock_artifact_cutover_fence",
        "_assert_provider_directory_artifact_build_fence",
        "_lock_provider_directory_artifact_tables",
        "_install_provider_directory_prepared_stage",
        "_finish_provider_directory_prepared_stage",
        "_record_address_alias_artifact_generation",
        "_promote_provider_directory_artifact_datasets",
    ):

        async def operation(*args, _name=name):
            await emit(_name, *args)

        monkeypatch.setattr(fhir, name, operation)
    monkeypatch.setattr(fhir, "_tighten_provider_directory_artifact_cutover_timeout", Mock())
    rename = AsyncMock()
    build_fence = SimpleNamespace(target_oid=123)
    await fhir._promote_provider_directory_artifact_stage_transaction(
        "synthetic", "scratch", "target", rename, build_fence
    )
    group = outer_group(cutover_wave)
    prepared = group["original_identity"][2]
    assert group["identity"][0:2] == ("artifact_cutover", "stage") and group["identity"][3] is fence
    assert prepared.stage_table == "scratch" and prepared.target_relation == "target"
    assert prepared.rename_stage_indexes is rename and prepared.build_fence is build_fence
    assert group["consumed"] and group["original_outcome"].is_committed
    assert events[-3:] == [
        "_finish_provider_directory_prepared_stage",
        "_record_address_alias_artifact_generation",
        "_promote_provider_directory_artifact_datasets",
    ]
    assert state.connections[0].events.count("begin") == state.connections[0].events.count("commit") == 1


async def test_borrowed_owner_without_native_custody_remains_unknown(cutover_wave, monkeypatch):
    state, database, _, _, _, _, _ = cutover_wave

    async def apply(*_, **__):
        assert custody.current_owned_wal_transaction(database) is None
        assert database._transaction_binding().session.in_transaction()
        await database.status("UPDATE synthetic_target SET generation_id='new';")

    monkeypatch.setattr(fhir, "_apply_prepared_artifact_bundle_in_transaction", apply)
    connection = database.engine.connect()
    await connection.start()
    session = custody.AsyncSession(bind=connection, expire_on_commit=False, autoflush=False)
    await session.begin()
    try:
        async with database.bind_existing_session(session):
            await promote(cutover_wave)
        group = outer_group(cutover_wave)
        assert group["original_outcome"] is None and group["outcome"] is None
        assert group["body_complete"] and not group["consumed"] and group["status"] == "incomplete"
        assert not group["accounting_authority"] and not group["reservation_refund"]
        assert connection.events.count("savepoint_begin") == connection.events.count("savepoint_end") == 1
    finally:
        await session.rollback()
        await session.close()
        await connection.close()
    assert state.checkedout == 0
