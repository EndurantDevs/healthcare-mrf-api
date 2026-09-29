# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Monitored CMS phase spending remains independent of the paired Profile budget."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_nonprofile_capacity as capacity
from process.provider_directory_cms_preparation import NonprofileAdmissionCheck, OwnedRelation
from tests.test_provider_directory_cms_nonprofile_capacity import _producer, _signed_plan


def _fully_allocated_producer():
    """Use the production split, leaving no unassigned allowance to hide double counting."""
    producer = _producer()
    producer.plan = replace(producer.plan, cutover_wal_upper_bound_bytes=50_000)
    producer.lease = _signed_plan(producer.plan)
    return producer


def _check(producer, phase, relations=(), logging_relations=()):
    return NonprofileAdmissionCheck(phase, producer.lease, producer.plan, relations, logging_relations)


def _resumed_profile(producer, preparation_wal=40_000):
    """Retain an exact active Profile window and its independent budget checker."""
    profile = SimpleNamespace(lease=producer.profile_lease, run_id=producer.run_id)
    producer.resumed_profile = profile
    producer.preparation_wal_end_bytes = preparation_wal
    producer.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(profile)
    producer._assert_active_profile = AsyncMock()
    producer.fhir._assert_provider_directory_profile_wal_budget = AsyncMock()
    return profile


@pytest.mark.asyncio
async def test_admission_and_scratch_wal_spend_preparation_ceiling_once():
    producer = _fully_allocated_producer()
    assert (
        sum((producer.plan.logging_wal_upper_bound_bytes, producer.plan.cutover_wal_upper_bound_bytes))
        == dict(producer.plan.reservation_bytes)["wal"]
    )
    for phase, measured_wal in (("pre_scratch", 512), ("readiness", 4096), ("readiness", 50_000)):
        producer.fhir.db.scalar.return_value = measured_wal
        await producer._assert_physical(_check(producer, phase), {})
    producer.fhir.db.scalar.return_value = 50_001
    with pytest.raises(RuntimeError, match="logging_wal_budget_exceeded"):
        await producer._assert_physical(_check(producer, "readiness"), {})


@pytest.mark.asyncio
async def test_preparation_cannot_borrow_the_paired_profile_reservation():
    producer = _fully_allocated_producer()
    producer.paused_profile = capacity.PausedProfileCapacity(SimpleNamespace(), spent_wal_bytes=0)
    producer._assert_active_profile = AsyncMock()
    producer.fhir.db.scalar.return_value = 50_001
    with pytest.raises(RuntimeError, match="logging_wal_budget_exceeded"):
        await producer._assert_wal_budget(_check(producer, "readiness"))
    producer._assert_active_profile.assert_awaited_once_with(producer.paused_profile.admission)


@pytest.mark.asyncio
async def test_only_measured_paired_admission_wal_is_excluded_from_preparation():
    producer = _fully_allocated_producer()
    profile = SimpleNamespace()
    producer.paused_profile = capacity.PausedProfileCapacity(profile, spent_wal_bytes=5000)
    producer._assert_active_profile = AsyncMock()
    producer.fhir.db.scalar.return_value = 55_000
    await producer._assert_wal_budget(_check(producer, "readiness"))
    producer._assert_active_profile.assert_awaited_once_with(profile)
    producer.fhir.db.scalar.return_value = 55_001
    with pytest.raises(RuntimeError, match="logging_wal_budget_exceeded"):
        await producer._assert_wal_budget(_check(producer, "readiness"))
    producer._assert_active_profile.side_effect = RuntimeError("unproved paired admission")
    producer.fhir.db.scalar.return_value = 55_000
    with pytest.raises(RuntimeError, match="unproved paired admission"):
        await producer._assert_wal_budget(_check(producer, "readiness"))


@pytest.mark.asyncio
async def test_paired_admission_window_cannot_be_replaced_or_include_an_older_offset(monkeypatch):
    producer = _fully_allocated_producer()
    profile = SimpleNamespace(run_id=producer.run_id, lease=producer.profile_lease, initial_wal_offset_bytes=0)
    paused = capacity.PausedProfileCapacity(profile, spent_wal_bytes=5000)
    pause = AsyncMock(return_value=paused)
    monkeypatch.setattr(capacity, "pause_profile_capacity", pause)
    assert await producer.pause_profile(profile) is paused
    with pytest.raises(RuntimeError, match="paired_profile_admission_changed"):
        await producer.pause_profile(profile)
    another_producer = _fully_allocated_producer()
    profile.initial_wal_offset_bytes = 1
    with pytest.raises(RuntimeError, match="paired_profile_admission_changed"):
        await another_producer.pause_profile(profile)
    pause.assert_awaited_once()


@pytest.mark.asyncio
async def test_pending_rewrite_uses_remaining_allowance_and_only_its_measured_heap():
    producer = _fully_allocated_producer()
    target = OwnedRelation("synthetic", "pending_stage", 1, 3000, "u")
    scratch = OwnedRelation("synthetic", "scratch_scope", 2, 60_000, "u")
    logged = OwnedRelation("synthetic", "previous_stage", 3, 30_000, "p")
    request = _check(producer, "pre_logging", (target, scratch, logged), ((target.schema, target.relation),))
    producer.fhir.db.scalar.return_value = 41_000
    await producer._assert_wal_budget(request)
    producer.fhir.db.scalar.return_value = 41_001
    with pytest.raises(RuntimeError, match="measured_logging_reserve_exceeded"):
        await producer._assert_wal_budget(request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "targets",
    [(), (("synthetic", "missing"),), (("synthetic", "logged"),), (("synthetic", "pending"),) * 2],
)
async def test_rewrite_requires_exact_unique_unlogged_owned_targets(targets):
    producer = _fully_allocated_producer()
    relations = (
        OwnedRelation("synthetic", "pending", 1, 1000, "u"),
        OwnedRelation("synthetic", "logged", 2, 1000, "p"),
    )
    with pytest.raises(RuntimeError, match="logging_targets_invalid"):
        await producer._assert_wal_budget(_check(producer, "pre_logging", relations, targets))


@pytest.mark.asyncio
async def test_readiness_cannot_carry_a_rewrite_reservation():
    producer = _fully_allocated_producer()
    with pytest.raises(RuntimeError, match="logging_targets_invalid"):
        await producer._assert_wal_budget(_check(producer, "readiness", logging_relations=(("synthetic", "heap"),)))


@pytest.mark.asyncio
async def test_profile_window_is_excluded_once_and_cutover_spends_only_cms_allowance():
    producer = _fully_allocated_producer()
    profile = _resumed_profile(producer)
    producer.fhir.db.scalar.return_value = 400_000
    await producer._assert_wal_budget(_check(producer, "readiness"))
    assert producer.cutover_wal_start_bytes is None
    producer.fhir.db.scalar.return_value = 500_000
    await producer._assert_wal_budget(_check(producer, "cutover"))
    assert producer.cutover_wal_start_bytes == 500_000
    producer.fhir.db.scalar.return_value = 550_000
    await producer._assert_wal_budget(_check(producer, "cutover"))
    assert producer.cutover_wal_start_bytes == 500_000
    assert producer.fhir._assert_provider_directory_profile_wal_budget.await_count == 2
    producer._assert_active_profile.assert_awaited_with(profile)
    producer.fhir.db.scalar.return_value = 550_001
    with pytest.raises(RuntimeError, match="cutover_wal_budget_exceeded"):
        await producer._assert_wal_budget(_check(producer, "cutover"))


@pytest.mark.asyncio
async def test_profile_window_requires_the_same_admission_and_its_own_remaining_budget():
    producer = _fully_allocated_producer()
    profile = _resumed_profile(producer)
    producer.fhir.db.scalar.return_value = 500_000
    producer.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(SimpleNamespace())
    with pytest.raises(RuntimeError, match="paired_profile_admission_changed"):
        await producer._assert_wal_budget(_check(producer, "cutover"))
    producer.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(profile)
    producer.fhir._assert_provider_directory_profile_wal_budget.side_effect = RuntimeError("profile budget exceeded")
    with pytest.raises(RuntimeError, match="profile budget exceeded"):
        await producer._assert_wal_budget(_check(producer, "cutover"))
    assert producer.cutover_wal_start_bytes is None


@pytest.mark.asyncio
async def test_cutover_cannot_skip_the_paired_profile_build():
    producer = _fully_allocated_producer()
    with pytest.raises(RuntimeError, match="paired_profile_preparation_required"):
        await producer._assert_wal_budget(_check(producer, "cutover"))


@pytest.mark.asyncio
async def test_logging_cannot_reopen_after_profile_starts():
    producer = _fully_allocated_producer()
    _resumed_profile(producer)
    producer.fhir.db.scalar.return_value = 40_000
    with pytest.raises(RuntimeError, match="logging_phase_closed"):
        await producer._assert_wal_budget(_check(producer, "pre_logging"))


@pytest.mark.asyncio
async def test_resume_uses_the_profiles_exact_lsn_without_changing_its_meter(monkeypatch):
    producer = _fully_allocated_producer()
    paused = capacity.PausedProfileCapacity(SimpleNamespace(), spent_wal_bytes=512)
    producer.paused_profile = paused
    resumed = SimpleNamespace(initial_wal_lsn="0/FFFF", initial_wal_offset_bytes=512)
    resume = AsyncMock(return_value=resumed)
    monkeypatch.setattr(capacity, "resume_profile_capacity", resume)
    producer.fhir.db.scalar.return_value = 41_000
    assert await producer.resume_profile(paused, "resource-fence", frozenset({"Practitioner"})) is resumed
    assert producer.preparation_wal_end_bytes == 41_000
    assert producer.resumed_profile is resumed
    assert producer.fhir.db.scalar.call_args.kwargs == {"end": "0/FFFF", "start": "0/10"}
    assert resumed.initial_wal_lsn == "0/FFFF" and resumed.initial_wal_offset_bytes == 512
    with pytest.raises(RuntimeError, match="paired_profile_admission_changed"):
        await producer.resume_profile(paused, "resource-fence", frozenset({"Practitioner"}))


@pytest.mark.asyncio
async def test_resume_rejects_preparation_overspend_before_profile_work(monkeypatch):
    producer = _fully_allocated_producer()
    paused = capacity.PausedProfileCapacity(SimpleNamespace(), spent_wal_bytes=512)
    producer.paused_profile = paused
    monkeypatch.setattr(
        capacity, "resume_profile_capacity", AsyncMock(return_value=SimpleNamespace(initial_wal_lsn="0/FFFF"))
    )
    producer.fhir.db.scalar.return_value = 50_513
    with pytest.raises(RuntimeError, match="logging_wal_budget_exceeded"):
        await producer.resume_profile(paused, "resource-fence", frozenset({"Practitioner"}))
    assert producer.preparation_wal_end_bytes is None and producer.resumed_profile is None
