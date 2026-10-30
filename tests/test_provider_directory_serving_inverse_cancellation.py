# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual inverse ownership and cleanup with the existing host-only native transport."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_common_serving_recovery as common
from process import provider_directory_owned_wal_transaction as owned
from process import provider_directory_serving_inverse_capacity as capacity
from tests.test_provider_directory_owned_evidence_wave import Session, fhir
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave
from tests.test_provider_directory_serving_inverse_capacity_admission import RUN, task
from tests.test_provider_directory_serving_inverse_capacity_signing import verify


def _admission():
    params = task()
    lease = verify(params[capacity.contract.EXECUTION_FIELD])
    geometry = lease.signing_preflight_guard["healthcare_receipt"]["capacity_geometry"]
    return capacity.InverseAdmission(fhir, capacity.task_execution(params), RUN, lease, geometry, None, None, "0/1")


def _cleanup_barriers(monkeypatch):
    state = SimpleNamespace(
        rollback_started=asyncio.Event(),
        allow_rollback=asyncio.Event(),
        close_started=asyncio.Event(),
        allow_close=asyncio.Event(),
        rollback_finished=False,
        close_finished=False,
    )
    original_rollback, original_close = Session.rollback, Session.close

    async def rollback(session):
        state.rollback_started.set()
        await state.allow_rollback.wait()
        await original_rollback(session)
        state.rollback_finished = True

    async def close(session):
        state.close_started.set()
        await state.allow_close.wait()
        await original_close(session)
        state.close_finished = True

    monkeypatch.setattr(Session, "rollback", rollback)
    monkeypatch.setattr(Session, "close", close)
    return state


async def _ordinary(database, admission):
    binding = database._transaction_binding()
    outcome = owned.current_owned_wal_transaction(database)
    assert outcome is not None and binding.session is outcome.session
    assert outcome.retained_driver is binding.session.bind.driver
    actor_by_field = dict(
        session="builder",
        current="builder",
        owner=admission.geometry["scope"]["owner_oid"],
        backend=outcome.retained_pid,
        xid="8",
        database_oid=admission.geometry["scope"]["database_oid"],
    )
    return binding.session, outcome.retained_driver, actor_by_field


def _mutation_observers(monkeypatch, admission, state):
    async def ordinary(database):
        return await _ordinary(database, admission)

    async def boundary():
        session, _driver, actor = await ordinary(admission.fhir.db)
        assert admission.binding.session is session
        assert admission.actor_json == capacity.encoded(actor)
        assert admission.initial_wal_lsn == "0/1"
        state.boundaries += 1

    monkeypatch.setattr(capacity.receipts, "ordinary", ordinary)
    monkeypatch.setattr(admission, "refresh", AsyncMock())
    monkeypatch.setattr(admission, "boundary", boundary)


def _owner_setup(monkeypatch, state):
    async def execute(session, statement, parameters=None):
        assert str(statement) == "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
        assert owned.current_owned_wal_transaction(fhir.db).session is session

    async def stop_before_consumption(*_args, **_kwargs):
        assert owned.current_owned_wal_transaction(fhir.db) is not None
        state.body_started.set()
        await state.never.wait()

    monkeypatch.setattr(Session, "execute", execute)
    monkeypatch.setattr(capacity.preflight, "configure_preparation", AsyncMock())
    monkeypatch.setattr(fhir, "_lock_profile_capacity_preflight_state", AsyncMock())
    monkeypatch.setattr(fhir, "ensure_database", AsyncMock())
    monkeypatch.setattr(capacity, "_run", stop_before_consumption)


async def _cancel_through_cleanup(worker, cleanup):
    worker.cancel("body cancellation")
    await asyncio.wait_for(cleanup.rollback_started.wait(), 2)
    worker.cancel("rollback cancellation")
    await asyncio.sleep(0)
    assert not worker.done() and not cleanup.rollback_finished
    cleanup.allow_rollback.set()
    await asyncio.wait_for(cleanup.close_started.wait(), 2)
    worker.cancel("close cancellation")
    await asyncio.sleep(0)
    assert not worker.done() and not cleanup.close_finished
    cleanup.allow_close.set()
    with pytest.raises(owned.OwnedWalTransactionCancelled):
        await asyncio.wait_for(worker, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["consumption", "mutation"])
async def test_repeated_inverse_cancellation_drains_original_owner_before_readback(owned_wave, monkeypatch, phase):
    pool, database, *_rest = owned_wave
    cleanup = _cleanup_barriers(monkeypatch)
    state = SimpleNamespace(body_started=asyncio.Event(), never=asyncio.Event(), boundaries=0, readbacks=0)
    _owner_setup(monkeypatch, state)

    async def recover_once(*_args):
        state.admission = admission = _admission()
        if phase == "consumption":
            await admission.consume(task(), {})
        else:
            admission.consumption = {"synthetic_consumed": True}
            _mutation_observers(monkeypatch, admission, state)
            async with admission.mutation():
                state.body_started.set()
                await state.never.wait()

    async def readback(*_args):
        assert cleanup.rollback_finished and cleanup.close_finished
        assert pool.checkedout == 0 and pool.connections[0].closed
        assert pool.connections[0].driver.closed
        state.readbacks += 1
        return None

    monkeypatch.setattr(common, "_recover_once", recover_once)
    monkeypatch.setattr(common, "_readback", readback)
    context_by_field = {"control_run_id": RUN}
    worker = asyncio.create_task(common.maybe_recover_common(fhir, context_by_field, task()))
    await asyncio.wait_for(state.body_started.wait(), 2)
    await _cancel_through_cleanup(worker, cleanup)
    assert state.readbacks == 1 and not context_by_field.get("context", {}).get("audit")
    assert database._transaction_binding() is None and owned.current_owned_wal_transaction(database) is None
    assert pool.connections[0].events.count("rollback") == 1 and "commit" not in pool.connections[0].events
    assert state.admission.binding is None and state.admission.actor_json is None
    if phase == "consumption":
        assert state.admission.consumption == {} and state.admission.captured is None
    else:
        assert state.boundaries == 1


@pytest.mark.asyncio
async def test_unconsumed_inverse_cannot_open_mutation_owner(owned_wave):
    pool, _database, *_rest = owned_wave
    admission = _admission()
    with pytest.raises(ValueError, match="transaction_changed"):
        async with admission.mutation():
            raise AssertionError("unconsumed authority entered mutation")
    assert pool.connections == [] and admission.consumption == {}
