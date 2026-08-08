"""GET-only runtime coverage for absent-admission retirement proofs."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, SanicException

from api import control_wave_routes as routes
from process import ptg_wave_admission_rollback_supersession_runtime as runtime
from process.ptg_wave_admission_rollback_supersession import (
    DATABASE_FIELDS,
    PTGWaveAdmissionRollbackConflict,
)
from tests.ptg_wave_supersession_fixtures import admission_rollback_proof


class _Context:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _Pipeline:
    def __init__(self, values):
        self.values = values
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    def zcard(self, key):
        self.calls.append(("zcard", key))

    def hlen(self, key):
        self.calls.append(("hlen", key))

    def get(self, key):
        self.calls.append(("get", key))

    async def execute(self, *, raise_on_error):
        assert raise_on_error is False
        return self.values


class _Redis:
    def __init__(self, values):
        self.pipe = _Pipeline(values)

    def pipeline(self, *, transaction):
        assert transaction is True
        return self.pipe


def _proof():
    return admission_rollback_proof(
        successor_wave_id="successor-wave",
        intent_count=17,
    )


def _stored_retirement(proof):
    predecessor = proof["predecessor"]
    return SimpleNamespace(
        predecessor_wave_id=predecessor["wave_id"],
        predecessor_idempotency_key=predecessor["idempotency_key"],
        predecessor_request_digest=predecessor["request_digest"],
        predecessor_wave_digest=predecessor["wave_digest"],
        predecessor_release_queue=predecessor["release_queue"],
        predecessor_intent_count=predecessor["intent_count"],
        successor_wave_id="successor-wave",
        recovery_evidence=proof,
    )


@pytest.mark.asyncio
async def test_get_replays_stored_proof_without_external_reads(monkeypatch):
    proof = _proof()
    session = object()
    monkeypatch.setattr(runtime.db, "session", lambda: _Context(session))
    monkeypatch.setattr(
        runtime,
        "_retirement_row",
        AsyncMock(return_value=_stored_retirement(proof)),
    )
    database = AsyncMock(side_effect=AssertionError("must not read database"))
    external = AsyncMock(side_effect=AssertionError("must not read external state"))
    monkeypatch.setattr(runtime, "_database_absence_observation", database)
    monkeypatch.setattr(runtime, "_external_absence_observation", external)

    result = await runtime.get_admission_rollback_supersession_candidate(
        proof["predecessor"],
        "successor-wave",
        redis=object(),
    )

    assert result == proof
    database.assert_not_awaited()
    external.assert_not_awaited()


@pytest.mark.asyncio
async def test_fresh_get_observes_only_absence_and_builds_exact_proof(monkeypatch):
    proof = _proof()
    predecessor = proof["predecessor"]
    session = object()
    monkeypatch.setattr(runtime.db, "session", lambda: _Context(session))
    monkeypatch.setattr(runtime, "_retirement_row", AsyncMock(return_value=None))
    database = AsyncMock(
        return_value={name: 0 for name in DATABASE_FIELDS}
    )
    external = AsyncMock(
        return_value=(proof["kubernetes"], proof["redis"])
    )
    monkeypatch.setattr(runtime, "_database_absence_observation", database)
    monkeypatch.setattr(runtime, "_external_absence_observation", external)

    result = await runtime.get_admission_rollback_supersession_candidate(
        predecessor,
        "successor-wave",
        redis="redis-observer",
    )

    assert result == proof
    database.assert_awaited_once_with(session, predecessor)
    external.assert_awaited_once_with(
        predecessor,
        redis="redis-observer",
    )


@pytest.mark.asyncio
async def test_locked_reobservation_rejects_tombstone_or_drift(monkeypatch):
    proof = _proof()
    predecessor = proof["predecessor"]
    session = object()
    retirement_row = AsyncMock(return_value=_stored_retirement(proof))
    monkeypatch.setattr(runtime, "_retirement_row", retirement_row)

    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="immutable admission rollback",
    ):
        await runtime.attest_locked_admission_rollback_supersession(
            session,
            predecessor,
            "successor-wave",
            proof,
            redis=object(),
        )

    retirement_row.assert_awaited_once_with(
        session,
        predecessor["wave_id"],
        lock_row=True,
    )

    monkeypatch.setattr(runtime, "_retirement_row", AsyncMock(return_value=None))
    database_counts_by_field = {name: 0 for name in DATABASE_FIELDS}
    monkeypatch.setattr(
        runtime,
        "_database_absence_observation",
        AsyncMock(return_value=database_counts_by_field),
    )
    drifted_redis_map = dict(proof["redis"])
    drifted_redis_map["queued_entry_count"] = 1
    monkeypatch.setattr(
        runtime,
        "_external_absence_observation",
        AsyncMock(return_value=(proof["kubernetes"], drifted_redis_map)),
    )

    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="Redis proof is not empty",
    ):
        await runtime.attest_locked_admission_rollback_supersession(
            session,
            predecessor,
            "successor-wave",
            proof,
            redis=object(),
        )


@pytest.mark.asyncio
async def test_redis_observation_reads_only_four_wave_scoped_keys():
    proof = _proof()
    predecessor = proof["predecessor"]
    redis = _Redis([0, 0, None, None])

    observed = await runtime._redis_absence_observation(redis, predecessor)

    assert observed == proof["redis"]
    assert [name for name, _key in redis.pipe.calls] == [
        "zcard",
        "hlen",
        "get",
        "get",
    ]
    assert all(
        predecessor["wave_digest"] in key
        for _name, key in redis.pipe.calls
    )


def test_route_registers_get_only_admission_rollback_endpoint():
    registered_routes = []

    class _Blueprint:
        def listener(self, _name):
            return lambda function: function

        def get(self, path):
            return (
                lambda function: registered_routes.append(("GET", path))
                or function
            )

        def post(self, path):
            return (
                lambda function: registered_routes.append(("POST", path))
                or function
            )

    routes.register_control_wave_routes(_Blueprint())
    assert (
        "GET",
        "/import-waves/<wave_id>/admission-rollback-supersession",
    ) in registered_routes
    assert not any(
        method == "POST" and "admission-rollback-supersession" in path
        for method, path in registered_routes
    )


@pytest.mark.asyncio
async def test_route_forwards_exact_descriptor(monkeypatch):
    proof = _proof()
    predecessor = proof["predecessor"]
    candidate = AsyncMock(return_value=proof)
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "get_admission_rollback_supersession_candidate",
        candidate,
    )
    query_by_field = {
        "successor_wave_id": "successor-wave",
        "idempotency_key": predecessor["idempotency_key"],
        "request_digest": predecessor["request_digest"],
        "wave_digest": predecessor["wave_digest"],
        "release_queue": predecessor["release_queue"],
        "intent_count": str(predecessor["intent_count"]),
    }
    request = SimpleNamespace(
        args=query_by_field,
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_redis="redis-observer")),
    )

    response = await routes.control_get_admission_rollback_supersession(
        request,
        predecessor["wave_id"],
    )

    assert response.status == 200
    candidate.assert_awaited_once_with(
        predecessor,
        "successor-wave",
        redis="redis-observer",
    )

    with pytest.raises(BadRequest, match="query fields are not exact"):
        await routes.control_get_admission_rollback_supersession(
            SimpleNamespace(args={}, app=request.app),
            predecessor["wave_id"],
        )


@pytest.mark.asyncio
async def test_route_maps_observation_drift_to_conflict(monkeypatch):
    proof = _proof()
    predecessor = proof["predecessor"]
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "get_admission_rollback_supersession_candidate",
        AsyncMock(
            side_effect=PTGWaveAdmissionRollbackConflict(
                "admission rollback state changed"
            )
        ),
    )
    request = SimpleNamespace(
        args={
            "successor_wave_id": "successor-wave",
            "idempotency_key": predecessor["idempotency_key"],
            "request_digest": predecessor["request_digest"],
            "wave_digest": predecessor["wave_digest"],
            "release_queue": predecessor["release_queue"],
            "intent_count": str(predecessor["intent_count"]),
        },
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_redis=object())),
    )

    with pytest.raises(SanicException) as exc_info:
        await routes.control_get_admission_rollback_supersession(
            request,
            predecessor["wave_id"],
        )

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "invalid_value"),
    (
        ("successor_wave_id", " retired-successor"),
        ("successor_wave_id", "x" * 65),
        ("successor_wave_id", "retired-request-unit"),
        ("intent_count", "+17"),
        ("intent_count", "017"),
        ("intent_count", " 17"),
    ),
)
async def test_route_rejects_noncanonical_query_before_observation(
    monkeypatch,
    field,
    invalid_value,
):
    predecessor = _proof()["predecessor"]
    candidate = AsyncMock()
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "get_admission_rollback_supersession_candidate",
        candidate,
    )
    query_by_field = {
        "successor_wave_id": "successor-wave",
        "idempotency_key": predecessor["idempotency_key"],
        "request_digest": predecessor["request_digest"],
        "wave_digest": predecessor["wave_digest"],
        "release_queue": predecessor["release_queue"],
        "intent_count": str(predecessor["intent_count"]),
    }
    query_by_field[field] = invalid_value
    request = SimpleNamespace(
        args=query_by_field,
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_redis=object())),
    )

    with pytest.raises(BadRequest):
        await routes.control_get_admission_rollback_supersession(
            request,
            predecessor["wave_id"],
        )

    candidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_route_rejects_repeated_query_key_before_observation(
    monkeypatch,
):
    predecessor = _proof()["predecessor"]
    candidate = AsyncMock()
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "get_admission_rollback_supersession_candidate",
        candidate,
    )

    class _RepeatedArguments(dict):
        def getlist(self, field):
            value = self[field]
            return [value, value] if field == "successor_wave_id" else [value]

    request = SimpleNamespace(
        args=_RepeatedArguments({
            "successor_wave_id": "successor-wave",
            "idempotency_key": predecessor["idempotency_key"],
            "request_digest": predecessor["request_digest"],
            "wave_digest": predecessor["wave_digest"],
            "release_queue": predecessor["release_queue"],
            "intent_count": str(predecessor["intent_count"]),
        }),
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_redis=object())),
    )

    with pytest.raises(BadRequest, match="must occur exactly once"):
        await routes.control_get_admission_rollback_supersession(
            request,
            predecessor["wave_id"],
        )

    candidate.assert_not_awaited()
