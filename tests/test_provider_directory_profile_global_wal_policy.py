# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Global WAL accounting keeps future caps and unresolved owner exposure."""

import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_capacity_control_projection as policy

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture
def admission(monkeypatch):
    cap = SimpleNamespace(
        relation_name="evidence_stage", max_wal_bytes=100, max_scratch_bytes=100, max_target_growth_bytes=0
    )
    geometry = SimpleNamespace(
        bounded_admission=True,
        relation_byte_caps=(cap,),
        metadata_wal_upper_bound_bytes=100,
        reservation_bytes_by_storage_class={"wal": 300},
    )
    admitted = SimpleNamespace(
        geometry=geometry, wal_tracker=fhir._ProviderDirectoryProfileWalTracker(), control_wal_projection=object()
    )
    state = SimpleNamespace(wal=0, control_remaining=80)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admitted)
    monkeypatch.setattr(fhir, "_profile_capacity_remaining_ms", AsyncMock(return_value=1000))
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_relation_bytes", AsyncMock(return_value=0))
    monkeypatch.setattr(
        fhir, "_provider_directory_profile_current_wal_bytes", AsyncMock(side_effect=lambda _: state.wal)
    )
    monkeypatch.setattr(
        fhir.profile_capacity, "remaining_profile_control_wal_bytes", lambda *_: state.control_remaining
    )
    return admitted, state


@pytest.mark.asyncio
async def test_global_bytes_never_spend_the_unfinished_relation_forecast(admission):
    admitted, state = admission
    state.wal = 80
    admitted.wal_tracker.accounted_relation_wal_bytes["evidence_stage"] = 80
    assert fhir._profile_relation_wal_candidate(admitted, {})[2] == 100
    with pytest.raises(RuntimeError, match="total_wal_exceeded"):
        await policy.assert_wal_budget(fhir, admitted)
    assert 80 + 80 + 100 + 100 == 360


@pytest.mark.asyncio
async def test_concurrent_global_local_overrun_uses_real_signed_headroom(admission):
    admitted, state = admission
    admitted.wal_tracker.accounted_relation_wal_bytes["evidence_stage"] = 20
    await policy.complete_relation_class(fhir, admitted, "evidence_stage")
    owner = object()
    admitted.wal_tracker.pending_control_wal_bytes[owner] = 40
    state.wal, state.control_remaining = 70, 60
    await policy._settle_mutation_window(fhir, admitted, owner, None, 20, 0)
    assert not admitted.wal_tracker.pending_control_wal_bytes
    assert 70 + 60 + 100 == 230


@pytest.mark.asyncio
async def test_cancelled_owner_retains_all_exposure(admission):
    admitted, _ = admission
    with pytest.raises(asyncio.CancelledError):
        async with policy.mutation_window(fhir, "evidence_stage", ("stage",)):
            owner = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()[0]
            admitted.wal_tracker.pending_control_wal_bytes[owner] = 20
            admitted.wal_tracker.pending_relation_wal_bytes["evidence_stage"] = 40
            admitted.wal_tracker.pending_growth_bytes["evidence_stage"] = 1
            raise asyncio.CancelledError()
    tracker = admitted.wal_tracker
    assert tracker.unresolved_window
    assert list(tracker.pending_control_wal_bytes.values()) == [20]
    assert tracker.pending_relation_wal_bytes == {"evidence_stage": 40}
    assert tracker.pending_growth_bytes == {"evidence_stage": 1}
    with pytest.raises(RuntimeError, match="completion_unresolved"):
        await policy.complete_relation_class(fhir, admitted, "evidence_stage")


@pytest.mark.asyncio
async def test_completed_class_refuses_reservation_and_mutation(admission):
    admitted, _ = admission
    await policy.complete_relation_class(fhir, admitted, "evidence_stage")
    assert fhir._profile_relation_wal_candidate(admitted, {})[2] == 0
    with pytest.raises(RuntimeError, match="relation_completed"):
        fhir._profile_relation_wal_candidate(admitted, {"evidence_stage": 1})
    with pytest.raises(RuntimeError, match="relation_completed"):
        async with policy.mutation_window(fhir, "evidence_stage"):
            pytest.fail("completed class admitted a writer")


@pytest.mark.asyncio
@pytest.mark.parametrize("pending", ["wal", "growth", "active"])
async def test_completion_refuses_pending_or_active_owner(admission, pending):
    admitted, _ = admission
    tracker = admitted.wal_tracker
    if pending == "wal":
        tracker.pending_relation_wal_bytes["evidence_stage"] = 1
    elif pending == "growth":
        tracker.pending_growth_bytes["evidence_stage"] = 1
    else:
        await tracker.mutation_lock.acquire()
    try:
        with pytest.raises(RuntimeError, match="completion_unresolved"):
            await policy.complete_relation_class(fhir, admitted, "evidence_stage")
    finally:
        if tracker.mutation_lock.locked():
            tracker.mutation_lock.release()
    assert not tracker.completed_relation_classes
