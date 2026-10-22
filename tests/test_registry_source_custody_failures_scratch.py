# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Issued reader failures retain their errors and drain before family cleanup."""

import asyncio
import inspect

import pytest

from process import provider_directory_artifact_scratch_reader as reader
from tests.test_provider_directory_artifact_scratch_reader import case as case


async def test_reader_scheduling_failure_closes_unscheduled_work_and_discards_ticket(case, monkeypatch):
    async def operation():
        pytest.fail("unscheduled work must never run")

    def refuse_task(*args, **kwargs):
        raise RuntimeError("synthetic task scheduler failure")

    pending = operation()
    with case.issued(), monkeypatch.context() as patch:
        patch.setattr(reader.asyncio, "create_task", refuse_task)
        with pytest.raises(RuntimeError, match="task scheduler failure"):
            reader.create_scratch_read_task(case.directory, pending, reader_slots=case.slots)
    assert inspect.getcoroutinestate(pending) == inspect.CORO_CLOSED
    assert not reader._TICKETS
    assert not case.events


@pytest.mark.parametrize("resource_types", [frozenset({"Location"}), frozenset({"Practitioner"})])
async def test_child_projection_requires_its_issued_full_resource_scope(case, resource_types):
    case.admission.plan.resource_types = ("Location",)

    async def operation():
        return await reader.child_scope_projection(case.directory, case.fence, case.fence, resource_types)

    with case.issued():
        child = reader.create_scratch_read_task(case.directory, operation(), reader_slots=case.slots)
        if resource_types == frozenset({"Location"}):
            assert await child is case.projection
        else:
            with pytest.raises(RuntimeError, match="scratch_child_plan_changed"):
                await child
    assert [event[0] for event in case.events].count("close") == 1
    assert not reader._TICKETS


async def test_body_error_and_final_membership_refusal_both_survive_child_cleanup(case, monkeypatch):
    body_error = ValueError("synthetic projection failed")
    custody_error = RuntimeError("synthetic membership changed")
    original_verify = reader.inputs._verify_native_membership

    async def verify(*args):
        await original_verify(*args)
        case.count += 1
        if case.count == 2:
            raise custody_error

    async def operation():
        raise body_error

    monkeypatch.setattr(reader.inputs, "_verify_native_membership", verify)
    with case.issued():
        child = reader.create_scratch_read_task(case.directory, operation(), reader_slots=case.slots)
        with pytest.raises(ExceptionGroup) as observed:
            await child
    assert observed.value.exceptions == (body_error, custody_error)
    assert [event[0] for event in case.events].count("close") == 1
    assert not reader._TICKETS


async def test_issuer_cancellation_waits_for_child_transaction_cleanup(case):
    started, closing, release_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()
    issuer = asyncio.current_task()

    async def operation():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            closing.set()
            await release_cleanup.wait()

    async def cancel_issuer():
        await asyncio.wait_for(closing.wait(), 1)
        issuer.cancel()
        await asyncio.sleep(0)
        assert not child.done()
        release_cleanup.set()

    with case.issued():
        child = reader.create_scratch_read_task(case.directory, operation(), reader_slots=case.slots)
        await asyncio.wait_for(started.wait(), 1)
        canceller = asyncio.create_task(cancel_issuer())
        try:
            with pytest.raises(asyncio.CancelledError):
                await reader.drain_scratch_children(case.directory, case.scope)
        finally:
            issuer.uncancel()
            release_cleanup.set()
            await canceller
    assert child.cancelled()
    assert [event[0] for event in case.events].count("close") == 1
    assert not reader._TICKETS
