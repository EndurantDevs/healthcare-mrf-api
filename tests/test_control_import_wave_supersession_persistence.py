"""Admission persistence checks for successor-bound recovery proofs."""

from __future__ import annotations

import datetime as dt
import types
from unittest.mock import AsyncMock

import pytest

from api import control_import_wave_supersession as supersession
from api import control_import_wave_recovery as recovery
from api import control_import_waves as waves
from db.models import PTGImportWaveSupersession
from tests.test_control_import_waves_persistence import (
    _Result,
    _Session,
    _install_admission_dependencies,
)


def test_supersession_timestamp_preserves_an_aware_instant_in_utc():
    source_timezone = dt.timezone(dt.timedelta(hours=2))
    source_timestamp = dt.datetime(2026, 8, 8, 3, 2, 3, tzinfo=source_timezone)

    assert supersession._as_aware_utc(source_timestamp) == dt.datetime(
        2026, 8, 8, 1, 2, 3, tzinfo=dt.UTC
    )


@pytest.mark.asyncio
async def test_recovery_admission_revalidates_and_persists_supersession_first(
    monkeypatch,
):
    session = _Session(_Result(rows=[]))
    request, _wave = _install_admission_dependencies(monkeypatch, session)
    monkeypatch.setattr(
        recovery,
        "find_admission_retirement_collision",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        waves,
        "persist_admission_recoveries",
        recovery.persist_admission_recoveries,
    )
    supersession_proof_map = {
        "predecessor": {"wave_id": "predecessor-wave"},
        "proof_digest": "d" * 64,
    }
    request["supersession"] = supersession_proof_map
    witness = types.SimpleNamespace(
        as_mapping=lambda: {"bound": True},
        evidence_mapping=lambda: {"bound": True},
        proof_digest="d" * 64,
    )
    attest = AsyncMock(return_value=witness)
    monkeypatch.setattr(
        supersession,
        "attest_locked_logical_preclaim_supersession",
        attest,
    )

    _response, created = await waves.admit_import_wave(
        {"signed": True},
        redis="synthetic-redis",
    )

    assert created is True
    attest.assert_awaited_once_with(
        session,
        "predecessor-wave",
        "wave-unit",
        supersession_proof_map,
        redis="synthetic-redis",
    )
    persisted_supersession = session.added[0]
    assert isinstance(persisted_supersession, PTGImportWaveSupersession)
    assert persisted_supersession.predecessor_wave_id == "predecessor-wave"
    assert persisted_supersession.successor_wave_id == "wave-unit"
    assert persisted_supersession.recovery_evidence == {"bound": True}
    assert persisted_supersession.recovery_evidence_canonical == b'{"bound":true}'
    assert persisted_supersession.recovery_evidence_sha256 == "d" * 64
    assert persisted_supersession.created_at.tzinfo is dt.UTC
    assert session.flush_count == 2
    waves.require_wave_admission_capacity.assert_awaited_once()
