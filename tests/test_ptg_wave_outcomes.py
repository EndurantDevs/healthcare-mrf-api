"""Database-bound contracts for immutable exact-wave outcomes."""

from __future__ import annotations

import types

import pytest

from process import ptg_wave_outcomes as outcomes
from process.ptg_wave_outcome_contract import (
    _collection_digest,
    _record_digest,
    linkage_mapping_digest,
    sign_linkage_ack,
)


_WAVE_ID = "wave-unit"
_WAVE_DIGEST = "1" * 64


class _Result:
    def __init__(self, *, scalar=None, rows=(), rowcount=1):
        self._scalar = scalar
        self._rows = list(rows)
        self.rowcount = rowcount

    def scalar_one_or_none(self):
        return self._scalar

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _Session:
    def __init__(self, *results):
        self.results = list(results)
        self.added = []
        self.flush_count = 0

    async def execute(self, _statement):
        assert self.results, "unexpected database execute"
        return self.results.pop(0)

    def add(self, value):
        self.added.append(value)

    async def flush(self):
        self.flush_count += 1


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def _install_transaction(monkeypatch, session):
    monkeypatch.setattr(outcomes.db, "transaction", lambda: _Transaction(session))


def _wave(**overrides):
    fields_by_field = {
        "wave_id": _WAVE_ID,
        "wave_digest": _WAVE_DIGEST,
        "state": "executing",
        "state_version": 7,
        "intent_count": 2,
        "outcomes_digest": None,
        "linkage_ack": None,
        "linkage_ack_digest": None,
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _records(count=2):
    intents = [
        types.SimpleNamespace(
            ordinal=ordinal,
            run_id=f"run-{ordinal}",
            job_id=f"job-{ordinal}",
            source_file_import_id=f"source-{ordinal}",
            content_version="v1",
        )
        for ordinal in range(count)
    ]
    runs = [
        types.SimpleNamespace(
            status="succeeded",
            snapshot_id=f"snapshot-{ordinal}",
            import_id=f"source-{ordinal}",
        )
        for ordinal in range(count)
    ]
    claims = [
        types.SimpleNamespace(
            ordinal=ordinal,
            claim_status="started",
            failure_code=None,
        )
        for ordinal in range(count)
    ]
    return intents, runs, claims


def _outcome_rows(intents, runs):
    rows = []
    for intent, run in zip(intents, runs):
        record_by_field = {
            "ordinal": intent.ordinal,
            "run_id": intent.run_id,
            "job_id": intent.job_id,
            "source_file_import_id": intent.source_file_import_id,
            "content_version": intent.content_version,
            "status": run.status,
            "snapshot_id": run.snapshot_id,
            "import_id": run.import_id,
        }
        rows.append(types.SimpleNamespace(**record_by_field, outcome_digest=_record_digest(record_by_field)))
    return rows


@pytest.mark.asyncio
async def test_snapshot_persists_all_outcomes_and_transitions(monkeypatch):
    wave = _wave()
    intents, runs, claims = _records()
    session = _Session(
        _Result(scalar=wave),
        _Result(rows=list(zip(intents, runs))),
        _Result(rows=claims),
        _Result(rows=[]),
        _Result(rowcount=1),
    )
    _install_transaction(monkeypatch, session)

    digest = await outcomes.snapshot_terminal_outcomes(_WAVE_ID)

    expected_records = [
        {
            "ordinal": intent.ordinal,
            "run_id": intent.run_id,
            "job_id": intent.job_id,
            "source_file_import_id": intent.source_file_import_id,
            "content_version": intent.content_version,
            "status": run.status,
            "snapshot_id": run.snapshot_id,
            "import_id": run.import_id,
        }
        for intent, run in zip(intents, runs)
    ]
    assert digest == _collection_digest(
        "healthporta.ptg-wave.outcomes.v1",
        expected_records,
    )
    assert [entry.ordinal for entry in session.added] == [0, 1]
    assert [entry.outcome_digest for entry in session.added] == [
        _record_digest(outcome_record) for outcome_record in expected_records
    ]
    assert session.results == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("wave", "expected", "message"),
    [
        (None, None, "not admitted"),
        (_wave(state="awaiting_linkage", outcomes_digest=None), None, "lacks"),
        (_wave(state="awaiting_linkage", outcomes_digest="2" * 64), "2" * 64, None),
        (_wave(state="released"), None, "not expected"),
    ],
)
async def test_snapshot_handles_absence_replay_and_wrong_state(
    monkeypatch, wave, expected, message,
):
    session = _Session(_Result(scalar=wave))
    _install_transaction(monkeypatch, session)
    if message is None:
        assert await outcomes.snapshot_terminal_outcomes(_WAVE_ID) == expected
    else:
        with pytest.raises(outcomes.PTGWaveOutcomeConflict, match=message):
            await outcomes.snapshot_terminal_outcomes(_WAVE_ID)


@pytest.mark.asyncio
async def test_terminal_snapshot_requires_complete_ordered_runs_and_claims():
    wave = _wave()
    intents, runs, claims = _records()

    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="one ImportRun"):
        await outcomes._terminal_snapshot_records(
            _Session(_Result(rows=[(intents[0], runs[0])])), wave, _WAVE_ID,
        )

    intents[1].ordinal = 3
    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="missing an admitted ordinal"):
        await outcomes._terminal_snapshot_records(
            _Session(_Result(rows=list(zip(intents, runs)))), wave, _WAVE_ID,
        )
    intents[1].ordinal = 1

    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="worker-start claim"):
        await outcomes._terminal_snapshot_records(
            _Session(
                _Result(rows=list(zip(intents, runs))),
                _Result(rows=claims[:1]),
            ),
            wave,
            _WAVE_ID,
        )

    claims[1].ordinal = 3
    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="ordinals"):
        await outcomes._terminal_snapshot_records(
            _Session(
                _Result(rows=list(zip(intents, runs))),
                _Result(rows=claims),
            ),
            wave,
            _WAVE_ID,
        )


@pytest.mark.asyncio
async def test_terminal_snapshot_rechecks_claim_coverage_after_ordering(monkeypatch):
    wave = _wave()
    intents, runs, claims = _records()
    claims[1].ordinal = 3
    monkeypatch.setattr(outcomes, "_rows_by_ordinal", lambda rows: list(rows))
    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="cover every"):
        await outcomes._terminal_snapshot_records(
            _Session(
                _Result(rows=list(zip(intents, runs))),
                _Result(rows=claims),
            ),
            wave,
            _WAVE_ID,
        )


@pytest.mark.asyncio
async def test_snapshot_refuses_existing_rows_and_concurrent_transition():
    wave = _wave()
    record_by_field = {
        "ordinal": 0,
        "run_id": "run-0",
        "job_id": "job-0",
        "source_file_import_id": "source-0",
        "content_version": "v1",
        "status": "succeeded",
        "snapshot_id": "snapshot-0",
        "import_id": "source-0",
    }
    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="already exist"):
        await outcomes._persist_terminal_snapshot(
            _Session(_Result(rows=[object()])),
            wave,
            _WAVE_ID,
            [record_by_field],
            "2" * 64,
        )

    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="state changed"):
        await outcomes._persist_terminal_snapshot(
            _Session(_Result(rows=[]), _Result(rowcount=0)),
            wave,
            _WAVE_ID,
            [record_by_field],
            "2" * 64,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("after_ordinal", "limit", "message"),
    [
        (True, 10, "cursor"),
        (-2, 10, "cursor"),
        (None, True, "limit"),
        (None, 0, "limit"),
        (None, 501, "limit"),
    ],
)
async def test_outcome_page_rejects_invalid_cursor_and_limit(
    after_ordinal, limit, message,
):
    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match=message):
        await outcomes.get_wave_outcomes_page(
            _WAVE_ID,
            after_ordinal=after_ordinal,
            limit=limit,
        )


@pytest.mark.asyncio
async def test_outcome_page_requires_snapshot_and_pages_stable_rows(monkeypatch):
    execute_results = [_Result(scalar=None)]

    async def execute(_statement):
        return execute_results.pop(0)

    monkeypatch.setattr(outcomes.db, "execute", execute)
    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="not available"):
        await outcomes.get_wave_outcomes_page(_WAVE_ID)

    intents, runs, _claims = _records(3)
    rows = _outcome_rows(intents, runs)
    wave = _wave(intent_count=3, outcomes_digest="2" * 64)
    execute_results.extend([_Result(scalar=wave), _Result(rows=rows)])
    page = await outcomes.get_wave_outcomes_page(
        _WAVE_ID,
        after_ordinal=0,
        limit=2,
    )
    assert [item["ordinal"] for item in page["items"]] == [0, 1]
    assert page["next_ordinal"] == 1
    assert page["outcomes_digest"] == "2" * 64

    execute_results.extend([_Result(scalar=wave), _Result(rows=rows[:2])])
    final_page = await outcomes.get_wave_outcomes_page(_WAVE_ID, limit=2)
    assert final_page["next_ordinal"] is None


def _ack(wave, stable_outcomes, key=b"unit-linkage-key"):
    unsigned_by_field = {
        "schema_version": "healthporta.ptg-wave-linkage-ack.v1",
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "intent_count": wave.intent_count,
        "mapping_digest": linkage_mapping_digest(stable_outcomes),
        "outcomes_digest": wave.outcomes_digest,
    }
    return {**unsigned_by_field, "signature": sign_linkage_ack(unsigned_by_field, key=key)}


@pytest.mark.asyncio
async def test_linkage_ack_persists_once_and_replays_exactly(monkeypatch):
    intents, runs, _claims = _records()
    stable = _outcome_rows(intents, runs)
    wave = _wave(state="awaiting_linkage", outcomes_digest="2" * 64)
    ack = _ack(wave, stable)
    session = _Session(_Result(scalar=wave), _Result(rows=stable))
    _install_transaction(monkeypatch, session)
    digest = await outcomes.record_linkage_ack(_WAVE_ID, ack, key=b"unit-linkage-key")
    assert wave.linkage_ack == ack
    assert wave.linkage_ack_digest == digest
    assert session.flush_count == 1

    replay_session = _Session(_Result(scalar=wave), _Result(rows=stable))
    _install_transaction(monkeypatch, replay_session)
    assert await outcomes.record_linkage_ack(
        _WAVE_ID, ack, key=b"unit-linkage-key",
    ) == digest
    assert replay_session.flush_count == 0

    conflicting_by_field = dict(ack, signature="3" * 64)
    conflict_session = _Session(_Result(scalar=wave), _Result(rows=stable))
    _install_transaction(monkeypatch, conflict_session)
    with pytest.raises(outcomes.PTGWaveOutcomeConflict):
        await outcomes.record_linkage_ack(
            _WAVE_ID,
            conflicting_by_field,
            key=b"unit-linkage-key",
        )

    differently_signed = _ack(wave, stable, key=b"replacement-key")
    valid_conflict_session = _Session(_Result(scalar=wave), _Result(rows=stable))
    _install_transaction(monkeypatch, valid_conflict_session)
    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="first receipt"):
        await outcomes.record_linkage_ack(
            _WAVE_ID,
            differently_signed,
            key=b"replacement-key",
        )


@pytest.mark.asyncio
async def test_linkage_ack_requires_expected_wave_and_all_outcomes(monkeypatch):
    for wave in (
        None,
        _wave(state="executing", outcomes_digest="2" * 64),
        _wave(state="awaiting_linkage", outcomes_digest=None),
    ):
        session = _Session(_Result(scalar=wave))
        _install_transaction(monkeypatch, session)
        with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="not expected"):
            await outcomes.record_linkage_ack(_WAVE_ID, {}, key=b"key")

    wave = _wave(state="awaiting_linkage", outcomes_digest="2" * 64)
    session = _Session(_Result(scalar=wave), _Result(rows=[]))
    _install_transaction(monkeypatch, session)
    with pytest.raises(outcomes.PTGWaveOutcomeConflict, match="every stable"):
        await outcomes.record_linkage_ack(_WAVE_ID, {}, key=b"key")
