"""Runtime edge coverage for locked logical pre-claim revalidation."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import ptg_wave_preclaim_supersession_runtime as runtime
from process.ptg_wave_preclaim_supersession import (
    PTGWavePreclaimSupersessionConflict,
)
from tests.test_ptg_wave_preclaim_supersession import (
    _attest,
    _intents_and_runs,
    _wave,
)
from tests.test_ptg_wave_preclaim_supersession_runtime import (
    _Context,
    _ReadSession,
    _Result,
    _snapshot,
)


@pytest.mark.asyncio
async def test_stored_proof_rejects_a_different_successor(monkeypatch):
    session = _ReadSession(
        _Result(SimpleNamespace(successor_wave_id="other-successor"))
    )
    monkeypatch.setattr(runtime.db, "session", lambda: _Context(session))

    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="another successor"):
        await runtime.get_logical_preclaim_supersession_candidate(
            "predecessor-wave",
            "successor-wave",
            redis=object(),
        )


@pytest.mark.asyncio
async def test_locked_revalidation_returns_the_exact_current_witness(monkeypatch):
    witness = _attest()
    snapshot = _snapshot()
    monkeypatch.setattr(runtime, "_supersession_row", AsyncMock(return_value=None))
    monkeypatch.setattr(
        runtime,
        "_load_preclaim_database_snapshot",
        AsyncMock(return_value=snapshot),
    )
    monkeypatch.setattr(
        runtime,
        "_observe_external_preclaim_state",
        AsyncMock(return_value=witness),
    )

    result = await runtime.attest_locked_logical_preclaim_supersession(
        object(),
        "predecessor-wave",
        "successor-wave",
        witness.as_mapping(),
        redis=object(),
    )

    assert result is witness


@pytest.mark.asyncio
async def test_snapshot_loader_preserves_fixed_database_read_order(monkeypatch):
    wave = _wave()
    intents, runs = _intents_and_runs(wave)
    load_wave = AsyncMock(return_value=wave)
    load_intents = AsyncMock(return_value=tuple(intents))
    load_related = AsyncMock(return_value=(tuple(runs), (), ()))
    worker_events = AsyncMock(return_value=[3])
    monkeypatch.setattr(runtime, "_load_quarantined_predecessor", load_wave)
    monkeypatch.setattr(runtime, "_load_predecessor_intents", load_intents)
    monkeypatch.setattr(runtime, "_load_preclaim_related_rows", load_related)
    monkeypatch.setattr(runtime, "_worker_start_event_ordinals", worker_events)
    session = object()

    snapshot = await runtime._load_preclaim_database_snapshot(
        session,
        "predecessor-wave",
        lock_rows=True,
    )

    assert snapshot.wave is wave
    assert snapshot.intents == tuple(intents)
    assert snapshot.runs == tuple(runs)
    assert snapshot.worker_start_event_ordinals == (3,)
    load_wave.assert_awaited_once_with(session, "predecessor-wave", lock_rows=True)
    load_intents.assert_awaited_once_with(session, "predecessor-wave", lock_rows=True)
    load_related.assert_awaited_once_with(
        session,
        "predecessor-wave",
        tuple(intents),
        lock_rows=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("lock_rows", (False, True))
async def test_quarantined_predecessor_loader_honors_lock_mode(lock_rows):
    wave = _wave()
    quarantine = SimpleNamespace(reason=runtime._QUARANTINE_REASON)
    session = _ReadSession(_Result(wave), _Result(quarantine))

    assert await runtime._load_quarantined_predecessor(
        session,
        "predecessor-wave",
        lock_rows=lock_rows,
    ) is wave
    statement_texts = [str(statement).upper() for statement in session.statements]
    assert all(("FOR UPDATE" in text) is lock_rows for text in statement_texts)


@pytest.mark.asyncio
@pytest.mark.parametrize("lock_rows", (False, True))
async def test_intent_loader_honors_lock_mode_and_ordinal_order(lock_rows):
    wave = _wave()
    intents, _runs = _intents_and_runs(wave)
    session = _ReadSession(_Result(values=intents))

    loaded_intents = await runtime._load_predecessor_intents(
        session,
        "predecessor-wave",
        lock_rows=lock_rows,
    )

    assert loaded_intents == tuple(intents)
    statement_text = str(session.statements[0]).upper()
    assert ("FOR UPDATE" in statement_text) is lock_rows
    assert "ORDER BY" in statement_text


@pytest.mark.asyncio
@pytest.mark.parametrize("lock_rows", (False, True))
async def test_related_row_loader_honors_lock_mode_and_relation_order(lock_rows):
    wave = _wave()
    intents, runs = _intents_and_runs(wave)
    claims = [SimpleNamespace(ordinal=0)]
    outcomes = [SimpleNamespace(ordinal=0)]
    session = _ReadSession(
        _Result(values=runs),
        _Result(values=claims),
        _Result(values=outcomes),
    )

    loaded_rows = await runtime._load_preclaim_related_rows(
        session,
        "predecessor-wave",
        tuple(intents),
        lock_rows=lock_rows,
    )

    assert loaded_rows == (tuple(runs), tuple(claims), tuple(outcomes))
    statement_texts = [str(statement).upper() for statement in session.statements]
    assert len(statement_texts) == 3
    assert all(("FOR UPDATE" in text) is lock_rows for text in statement_texts)


@pytest.mark.asyncio
async def test_external_observation_requires_redis_and_kubernetes(monkeypatch):
    snapshot = _snapshot()
    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="Redis observer"):
        await runtime._observe_external_preclaim_state(
            snapshot,
            "successor-wave",
            redis=None,
        )

    monkeypatch.setattr(runtime, "get_wave_job", Mock(return_value=None))
    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="unavailable"):
        await runtime._observe_external_preclaim_state(
            snapshot,
            "successor-wave",
            redis=object(),
        )


@pytest.mark.asyncio
async def test_supersession_lookup_locks_and_wave_ids_fail_closed():
    session = _ReadSession(_Result(None))
    assert await runtime._supersession_row(
        session,
        "predecessor-wave",
        lock_row=True,
    ) is None
    assert "FOR UPDATE" in str(session.statements[0]).upper()

    for invalid_wave_id in (None, "", " bad", "x" * 65):
        with pytest.raises(PTGWavePreclaimSupersessionConflict, match="bounded string"):
            runtime._wave_id(invalid_wave_id, "wave ID")
