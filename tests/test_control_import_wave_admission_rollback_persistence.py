"""Persistence ordering for one V4 absent-admission retirement."""

from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock

import pytest

from api import control_import_wave_admission_rollback as rollback
from api import control_import_wave_recovery as recovery
from api import control_import_wave_supersession as supersession
from api import control_import_waves as waves
from db.models import PTGImportWaveAdmissionRollback
from process.ptg_wave_admission_rollback_supersession import (
    PTGWaveAdmissionRollbackConflict,
)
from tests.ptg_wave_supersession_fixtures import admission_rollback_proof
from tests.test_control_import_waves_persistence import (
    _Result,
    _Session,
    _install_admission_dependencies,
)


@pytest.mark.asyncio
async def test_persistence_reobserves_and_stores_canonical_tombstone(
    monkeypatch,
):
    proof = admission_rollback_proof(
        successor_wave_id="successor-wave",
        intent_count=17,
    )
    session = _Session()
    attest = AsyncMock(return_value=proof)
    monkeypatch.setattr(
        rollback,
        "attest_locked_admission_rollback_supersession",
        attest,
    )

    await rollback.persist_admission_rollback_supersession(
        session,
        {
            "wave_id": "successor-wave",
            "admission_rollback_supersession": proof,
        },
        now=dt.datetime(2026, 8, 8, 4, 5, 6),
        redis="redis-observer",
    )

    attest.assert_awaited_once_with(
        session,
        proof["predecessor"],
        "successor-wave",
        proof,
        redis="redis-observer",
    )
    stored = session.added[0]
    assert isinstance(stored, PTGImportWaveAdmissionRollback)
    assert stored.predecessor_wave_id == proof["predecessor"]["wave_id"]
    assert stored.successor_wave_id == "successor-wave"
    assert stored.recovery_evidence == proof
    assert stored.recovery_evidence_sha256 == proof["proof_digest"]
    assert b'"proof_digest"' not in stored.recovery_evidence_canonical
    assert stored.created_at == dt.datetime(
        2026,
        8,
        8,
        4,
        5,
        6,
        tzinfo=dt.UTC,
    )
    assert session.flush_count == 1


@pytest.mark.asyncio
async def test_v4_orders_both_retirements_before_capacity_and_successor(
    monkeypatch,
):
    session = _Session(_Result(rows=[]))
    request, _wave = _install_admission_dependencies(monkeypatch, session)
    request["supersession"] = {"proof": "v7"}
    request["admission_rollback_supersession"] = {"proof": "v9"}
    ordered_events = []

    async def persist_recoveries(*_args, **_kwargs):
        ordered_events.extend(("v7", "v9"))

    async def capacity(*_args, **_kwargs):
        ordered_events.append("capacity")

    async def persist_successor(*_args, **_kwargs):
        ordered_events.append("successor")

    monkeypatch.setattr(
        waves,
        "persist_admission_recoveries",
        persist_recoveries,
    )
    monkeypatch.setattr(waves, "require_wave_admission_capacity", capacity)
    monkeypatch.setattr(waves, "_persist_wave_intents", persist_successor)

    _response, created = await waves.admit_import_wave(
        {"signed": True},
        redis="redis-observer",
    )

    assert created is True
    assert ordered_events == ["v7", "v9", "capacity", "successor"]


@pytest.mark.asyncio
async def test_recovery_helper_checks_collision_then_orders_v7_and_v9(
    monkeypatch,
):
    ordered_events = []

    async def collision(*_args, **_kwargs):
        ordered_events.append("collision")
        return None

    async def persist_v7(*_args, **_kwargs):
        ordered_events.append("v7")

    async def persist_v9(*_args, **_kwargs):
        ordered_events.append("v9")

    monkeypatch.setattr(recovery, "find_admission_retirement_collision", collision)
    monkeypatch.setattr(recovery, "persist_admission_supersession", persist_v7)
    monkeypatch.setattr(
        recovery,
        "persist_admission_rollback_supersession",
        persist_v9,
    )

    await recovery.persist_admission_recoveries(
        object(),
        {"wave_id": "successor-wave"},
        now=dt.datetime(2026, 8, 8),
        redis=object(),
    )

    assert ordered_events == ["collision", "v7", "v9"]


@pytest.mark.asyncio
async def test_recovery_collision_stops_both_retirement_writes(monkeypatch):
    collision = AsyncMock(return_value=object())
    persist_v7 = AsyncMock()
    persist_v9 = AsyncMock()
    monkeypatch.setattr(recovery, "find_admission_retirement_collision", collision)
    monkeypatch.setattr(recovery, "persist_admission_supersession", persist_v7)
    monkeypatch.setattr(
        recovery,
        "persist_admission_rollback_supersession",
        persist_v9,
    )

    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="permanently retired",
    ):
        await recovery.persist_admission_recoveries(
            object(),
            {"wave_id": "successor-wave"},
            now=dt.datetime(2026, 8, 8),
            redis=object(),
        )

    persist_v7.assert_not_awaited()
    persist_v9.assert_not_awaited()


@pytest.mark.asyncio
async def test_v3_retirement_is_a_noop_without_a_supersession_proof():
    session = _Session()

    await supersession.persist_admission_supersession(
        session,
        {"wave_id": "successor-wave"},
        now=dt.datetime(2026, 8, 8),
    )

    assert session.added == []
    assert session.flush_count == 0


@pytest.mark.asyncio
async def test_retired_request_stops_before_recovery_or_capacity(monkeypatch):
    session = _Session(_Result(rows=[]))
    _request, _wave = _install_admission_dependencies(monkeypatch, session)
    waves.persist_admission_recoveries.side_effect = (
        PTGWaveAdmissionRollbackConflict(
            "import wave admission identity is permanently retired"
        )
    )

    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="permanently retired",
    ):
        await waves.admit_import_wave({"signed": True})

    waves.persist_admission_recoveries.assert_awaited_once()
    waves.require_wave_admission_capacity.assert_not_awaited()
    waves._persist_wave_intents.assert_not_awaited()
