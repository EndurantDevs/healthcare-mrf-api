"""Read-only runtime and route coverage for logical pre-claim candidates."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import BadRequest, SanicException

from api import control_wave_routes as routes
from process import ptg_wave_preclaim_supersession_runtime as runtime
from process.ptg_wave_preclaim_supersession import (
    PTGWavePreclaimSupersessionConflict,
)
from tests.test_ptg_wave_preclaim_supersession import (
    _actual_job,
    _attest,
    _empty_redis_attestation,
    _intents_and_runs,
    _wave,
)


class _Result:
    def __init__(self, value=None, values=()):
        self.value = value
        self.values = list(values)

    def scalar_one_or_none(self):
        return self.value

    def scalars(self):
        return self

    def all(self):
        return list(self.values)


class _ReadSession:
    def __init__(self, *results):
        self.results = list(results)
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return self.results.pop(0)


class _Context:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _RedisAttestation:
    def __init__(self, mapping):
        self.mapping = mapping

    def as_mapping(self):
        return self.mapping


def _snapshot(*, claims=(), outcomes=(), worker_events=()):
    wave = _wave()
    intents, runs = _intents_and_runs(wave)
    return runtime._PreclaimDatabaseSnapshot(
        wave=wave,
        intents=tuple(intents),
        runs=tuple(runs),
        claims=tuple(claims),
        outcomes=tuple(outcomes),
        worker_start_event_ordinals=tuple(worker_events),
    )


@pytest.mark.asyncio
async def test_get_replays_stored_exact_proof_without_external_observation(
    monkeypatch,
):
    stored_proof = _attest().as_mapping()
    session = _ReadSession(
        _Result(
            SimpleNamespace(
                successor_wave_id="successor-wave",
                recovery_evidence=stored_proof,
            )
        )
    )
    monkeypatch.setattr(runtime.db, "session", lambda: _Context(session))
    get_job = Mock(side_effect=AssertionError("stored replay must not read Kubernetes"))
    redis_attest = AsyncMock(
        side_effect=AssertionError("stored replay must not read Redis")
    )
    monkeypatch.setattr(runtime, "get_wave_job", get_job)
    monkeypatch.setattr(runtime, "attest_ptg_small_wave_unclaimed_failure_redis", redis_attest)

    observed = await runtime.get_logical_preclaim_supersession_candidate(
        "predecessor-wave", "successor-wave", redis=object()
    )

    assert observed == stored_proof
    get_job.assert_not_called()
    redis_attest.assert_not_awaited()
    assert len(session.statements) == 1
    assert all(
        str(statement).lstrip().upper().startswith("SELECT")
        for statement in session.statements
    )


@pytest.mark.asyncio
async def test_get_requires_quarantine_and_never_observes_external_state(
    monkeypatch,
):
    session = _ReadSession(_Result(None), _Result(SimpleNamespace()), _Result(None))
    monkeypatch.setattr(runtime.db, "session", lambda: _Context(session))
    monkeypatch.setattr(
        runtime,
        "get_wave_job",
        Mock(side_effect=AssertionError("unquarantined candidate must not read Kubernetes")),
    )
    monkeypatch.setattr(
        runtime,
        "attest_ptg_small_wave_unclaimed_failure_redis",
        AsyncMock(side_effect=AssertionError("unquarantined candidate must not read Redis")),
    )

    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="quarantined"):
        await runtime.get_logical_preclaim_supersession_candidate(
            "predecessor-wave", "successor-wave", redis=object()
        )

    assert all(
        str(statement).lstrip().upper().startswith("SELECT")
        for statement in session.statements
    )


@pytest.mark.asyncio
async def test_fresh_get_binds_snapshot_kubernetes_and_redis_without_dml(
    monkeypatch,
):
    snapshot = _snapshot()
    session = _ReadSession(_Result(None))
    monkeypatch.setattr(runtime.db, "session", lambda: _Context(session))
    monkeypatch.setattr(runtime, "_load_preclaim_database_snapshot", AsyncMock(return_value=snapshot))
    job = _actual_job(snapshot.wave.kubernetes_manifest)
    get_job = Mock(return_value=job)
    manifest = object()
    restore = Mock(return_value=manifest)
    redis_attest = AsyncMock(
        return_value=_RedisAttestation(_empty_redis_attestation(snapshot.wave))
    )
    monkeypatch.setattr(runtime, "get_wave_job", get_job)
    monkeypatch.setattr(runtime, "restore_wave_manifest", restore)
    monkeypatch.setattr(runtime, "attest_ptg_small_wave_unclaimed_failure_redis", redis_attest)

    proof = await runtime.get_logical_preclaim_supersession_candidate(
        "predecessor-wave", "successor-wave", redis="redis-observer"
    )

    assert proof["predecessor"]["wave_id"] == snapshot.wave.wave_id
    assert proof["successor_wave_id"] == "successor-wave"
    get_job.assert_called_once_with(snapshot.wave.wave_digest)
    restore.assert_called_once()
    redis_attest.assert_awaited_once_with("redis-observer", manifest)
    assert all(
        str(statement).lstrip().upper().startswith("SELECT")
        for statement in session.statements
    )


@pytest.mark.asyncio
async def test_locked_admission_rejects_existing_supersession_and_proof_drift(
    monkeypatch,
):
    proof = _attest().as_mapping()
    session = object()
    existing = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(runtime, "_supersession_row", existing)

    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="immutable supersession"):
        await runtime.attest_locked_logical_preclaim_supersession(
            session, "predecessor-wave", "successor-wave", proof, redis=object()
        )

    existing.assert_awaited_once_with(session, "predecessor-wave", lock_row=True)

    snapshot = _snapshot()
    monkeypatch.setattr(runtime, "_supersession_row", AsyncMock(return_value=None))
    loader = AsyncMock(return_value=snapshot)
    monkeypatch.setattr(runtime, "_load_preclaim_database_snapshot", loader)
    drifted_proof_map = dict(proof)
    drifted_proof_map["proof_digest"] = "0" * 64
    monkeypatch.setattr(
        runtime,
        "_observe_external_preclaim_state",
        AsyncMock(
            return_value=SimpleNamespace(
                as_mapping=lambda: drifted_proof_map,
            )
        ),
    )

    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="differs"):
        await runtime.attest_locked_logical_preclaim_supersession(
            session, "predecessor-wave", "successor-wave", proof, redis=object()
        )

    loader.assert_awaited_once_with(session, "predecessor-wave", lock_rows=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("claims", "outcomes", "worker_events", "message"),
    (
        ([SimpleNamespace()], (), (), "no claims"),
        ((), [SimpleNamespace()], (), "no outcomes"),
        ((), (), (0,), "no worker start events"),
    ),
)
async def test_locked_admission_rejects_claim_outcome_or_worker_start_drift(
    monkeypatch, claims, outcomes, worker_events, message
):
    proof = _attest().as_mapping()
    snapshot = _snapshot(
        claims=claims,
        outcomes=outcomes,
        worker_events=worker_events,
    )
    monkeypatch.setattr(runtime, "_supersession_row", AsyncMock(return_value=None))
    monkeypatch.setattr(
        runtime, "_load_preclaim_database_snapshot", AsyncMock(return_value=snapshot)
    )
    monkeypatch.setattr(
        runtime, "get_wave_job", Mock(return_value=_actual_job(snapshot.wave.kubernetes_manifest))
    )
    monkeypatch.setattr(runtime, "restore_wave_manifest", Mock(return_value=object()))
    monkeypatch.setattr(
        runtime,
        "attest_ptg_small_wave_unclaimed_failure_redis",
        AsyncMock(return_value=_RedisAttestation(_empty_redis_attestation(snapshot.wave))),
    )

    with pytest.raises(PTGWavePreclaimSupersessionConflict, match=message):
        await runtime.attest_locked_logical_preclaim_supersession(
            object(), "predecessor-wave", "successor-wave", proof, redis=object()
        )


@pytest.mark.asyncio
async def test_logical_preclaim_route_is_get_only_and_forwards_exact_query(
    monkeypatch,
):
    registered_routes = []

    class _Blueprint:
        def listener(self, _name):
            return lambda function: function

        def get(self, path):
            return lambda function: registered_routes.append(
                ("GET", path, function)
            ) or function

        def post(self, path):
            return lambda function: registered_routes.append(
                ("POST", path, function)
            ) or function

    routes.register_control_wave_routes(_Blueprint())
    assert (
        "GET",
        "/import-waves/<wave_id>/logical-preclaim-supersession",
    ) in [(method, path) for method, path, _ in registered_routes]
    assert not any(
        method == "POST" and "logical-preclaim-supersession" in path
        for method, path, _ in registered_routes
    )

    candidate = AsyncMock(return_value={"proof_digest": "a" * 64})
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(routes, "get_logical_preclaim_supersession_candidate", candidate)
    monkeypatch.setattr(
        routes,
        "admit_import_wave",
        AsyncMock(side_effect=AssertionError("GET candidate must not admit or write")),
    )
    request = SimpleNamespace(
        args={"successor_wave_id": "successor-wave"},
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_redis="redis-observer")),
    )

    response = await routes.control_get_logical_preclaim_supersession(
        request, "predecessor-wave"
    )

    assert response.status == 200
    candidate.assert_awaited_once_with(
        "predecessor-wave", "successor-wave", redis="redis-observer"
    )

    with pytest.raises(BadRequest, match="successor_wave_id is required"):
        await routes.control_get_logical_preclaim_supersession(
            SimpleNamespace(args={}, app=request.app), "predecessor-wave"
        )


@pytest.mark.asyncio
async def test_logical_preclaim_route_maps_observation_drift_to_conflict(
    monkeypatch,
):
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "get_logical_preclaim_supersession_candidate",
        AsyncMock(
            side_effect=PTGWavePreclaimSupersessionConflict(
                "logical predecessor state changed"
            )
        ),
    )
    request = SimpleNamespace(
        args={"successor_wave_id": "successor-wave"},
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_redis=object())),
    )

    with pytest.raises(SanicException) as exc_info:
        await routes.control_get_logical_preclaim_supersession(
            request,
            "predecessor-wave",
        )

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_admission_route_maps_locked_proof_drift_to_conflict(monkeypatch):
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "admit_import_wave",
        AsyncMock(
            side_effect=PTGWavePreclaimSupersessionConflict(
                "signed logical pre-claim proof differs from current state"
            )
        ),
    )
    request = SimpleNamespace(
        headers={},
        body=b"{}",
        json={},
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_redis="redis-observer")),
    )

    with pytest.raises(SanicException) as exc_info:
        await routes.control_admit_import_wave(request)

    assert exc_info.value.status_code == 409
