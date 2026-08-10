"""Focused contracts for exact-wave abandonment into ordinary admission."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, SanicException

from api import control_import_wave_abandonment as abandonment
from api import control_wave_routes as routes
from process import ptg_wave_materialized_preclaim_supersession_runtime as runtime
from process.ptg_parts import ptg_wave_admission_fence as fence
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from tests.test_ptg_wave_materialized_preclaim_supersession import (
    _attest,
)
from tests.test_ptg_wave_materialized_preclaim_supersession_runtime import (
    _Blueprint,
    _Result,
    _snapshot,
)


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _Session:
    def __init__(self, existing=None):
        self.existing = existing
        self.added = []
        self.flush_count = 0

    async def execute(self, _statement, _parameters=None):
        return _Result(self.existing)

    def add(self, row):
        self.added.append(row)

    async def flush(self):
        self.flush_count += 1


def _stored_quarantine(proof: dict, *, cutover_id="successor-wave"):
    return SimpleNamespace(
        reason="materialized_preclaim_failure",
        recovery_basis="materialized_preclaim_failure",
        cutover_id=cutover_id,
        recovery_evidence=proof,
        recovery_evidence_sha256=proof["proof_digest"],
    )


@pytest.mark.asyncio
async def test_first_abandonment_persists_exact_proof_and_audit(monkeypatch):
    proof = _attest()
    session = _Session()
    monkeypatch.setattr(
        abandonment.db,
        "transaction",
        lambda: _Transaction(session),
    )
    admission_lock = AsyncMock()
    observe = AsyncMock(return_value=proof)
    monkeypatch.setattr(abandonment, "acquire_ptg_admission_lock", admission_lock)
    monkeypatch.setattr(
        abandonment,
        "attest_locked_materialized_preclaim_abandonment",
        observe,
    )

    response, created = await abandonment.abandon_materialized_preclaim_wave(
        "materialized-wave",
        "successor-wave",
        redis="redis-observer",
    )

    assert created is True
    assert response == {
        "wave_id": "materialized-wave",
        "cutover_id": "successor-wave",
        "state": "abandoned",
        "quarantine_reason": "materialized_preclaim_failure",
        "quarantined_run_count": 13,
        "unclaimed_run_count": 13,
        "queued_run_count": 13,
        "claim_count": 0,
        "outcome_count": 0,
        "worker_start_event_count": 0,
        "redis_release_present": False,
        "proof_digest": proof["proof_digest"],
        "created": True,
    }
    admission_lock.assert_awaited_once_with(session)
    observe.assert_awaited_once_with(
        session,
        "materialized-wave",
        "successor-wave",
        redis="redis-observer",
    )
    assert session.flush_count == 1
    assert len(session.added) == 1
    quarantine_row = session.added[0]
    assert quarantine_row.predecessor_wave_id == "materialized-wave"
    assert quarantine_row.cutover_id == "successor-wave"
    assert quarantine_row.recovery_basis == "materialized_preclaim_failure"
    assert quarantine_row.recovery_evidence_sha256 == proof["proof_digest"]


@pytest.mark.asyncio
async def test_exact_replay_is_read_only_and_mismatch_conflicts(monkeypatch):
    proof = _attest()
    session = _Session(_stored_quarantine(proof))
    monkeypatch.setattr(
        abandonment.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(abandonment, "acquire_ptg_admission_lock", AsyncMock())
    observe = AsyncMock(side_effect=AssertionError("replay must not reobserve"))
    monkeypatch.setattr(
        abandonment,
        "attest_locked_materialized_preclaim_abandonment",
        observe,
    )

    response, created = await abandonment.abandon_materialized_preclaim_wave(
        "materialized-wave",
        "successor-wave",
        redis=object(),
    )

    assert created is False
    assert response["created"] is False
    assert response["proof_digest"] == proof["proof_digest"]
    assert session.added == []
    observe.assert_not_awaited()

    with pytest.raises(
        PTGWaveMaterializedPreclaimConflict,
        match="another recovery",
    ):
        await abandonment.abandon_materialized_preclaim_wave(
            "materialized-wave",
            "different-cutover",
            redis=object(),
        )


@pytest.mark.asyncio
async def test_cutover_identity_owned_by_another_wave_conflicts(monkeypatch):
    session = _Session()
    monkeypatch.setattr(
        abandonment.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(abandonment, "acquire_ptg_admission_lock", AsyncMock())
    cutover_owner = SimpleNamespace(predecessor_wave_id="other-wave")
    owner_lookup = AsyncMock(return_value=cutover_owner)
    monkeypatch.setattr(abandonment, "_locked_cutover_owner", owner_lookup)
    observe = AsyncMock(side_effect=AssertionError("collision must fail first"))
    monkeypatch.setattr(
        abandonment,
        "attest_locked_materialized_preclaim_abandonment",
        observe,
    )

    with pytest.raises(
        PTGWaveMaterializedPreclaimConflict,
        match="cutover ID is already bound",
    ):
        await abandonment.abandon_materialized_preclaim_wave(
            "materialized-wave",
            "shared-cutover",
            redis=object(),
        )

    owner_lookup.assert_awaited_once_with(session, "shared-cutover")
    observe.assert_not_awaited()
    assert session.added == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "wave_id,cutover_id",
    (
        ("", "cutover"),
        ("materialized-wave", "materialized-wave"),
        ("materialized-wave", " cutover"),
        ("materialized-wave", "x" * 65),
    ),
)
async def test_abandonment_rejects_invalid_identities(wave_id, cutover_id):
    with pytest.raises(ValueError):
        await abandonment.abandon_materialized_preclaim_wave(
            wave_id,
            cutover_id,
            redis=object(),
        )


@pytest.mark.asyncio
async def test_exact_replay_rejects_a_stored_digest_mismatch(monkeypatch):
    proof = _attest()
    stored = _stored_quarantine(proof)
    stored.recovery_evidence_sha256 = "0" * 64
    session = _Session(stored)
    monkeypatch.setattr(
        abandonment.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(abandonment, "acquire_ptg_admission_lock", AsyncMock())

    with pytest.raises(
        PTGWaveMaterializedPreclaimConflict,
        match="stored abandonment proof digest",
    ):
        await abandonment.abandon_materialized_preclaim_wave(
            "materialized-wave",
            "successor-wave",
            redis=object(),
        )


@pytest.mark.asyncio
async def test_locked_abandonment_rejects_recovery_and_observes_once(monkeypatch):
    with pytest.raises(
        PTGWaveMaterializedPreclaimConflict,
        match="must differ",
    ):
        await runtime.attest_locked_materialized_preclaim_abandonment(
            object(),
            "materialized-wave",
            "materialized-wave",
            redis=object(),
        )

    monkeypatch.setattr(runtime, "_supersession_row", AsyncMock(return_value=object()))
    with pytest.raises(
        PTGWaveMaterializedPreclaimConflict,
        match="immutable supersession",
    ):
        await runtime.attest_locked_materialized_preclaim_abandonment(
            object(),
            "materialized-wave",
            "successor-wave",
            redis=object(),
        )

    snapshot = _snapshot()
    proof = _attest()
    monkeypatch.setattr(runtime, "_supersession_row", AsyncMock(return_value=None))
    loader = AsyncMock(return_value=snapshot)
    observer = AsyncMock(return_value=proof)
    monkeypatch.setattr(runtime, "_load_snapshot", loader)
    monkeypatch.setattr(runtime, "_observe", observer)

    assert await runtime.attest_locked_materialized_preclaim_abandonment(
        "session",
        "materialized-wave",
        "successor-wave",
        redis="redis",
    ) == proof
    loader.assert_awaited_once_with(
        "session", "materialized-wave", lock_rows=True
    )
    observer.assert_awaited_once_with(
        snapshot, "successor-wave", redis="redis"
    )


@pytest.mark.asyncio
async def test_locked_abandonment_rejects_any_assigned_run(monkeypatch):
    snapshot = _snapshot()
    snapshot.runs[0].node_id = "assigned-node"
    monkeypatch.setattr(runtime, "_supersession_row", AsyncMock(return_value=None))
    monkeypatch.setattr(runtime, "_load_snapshot", AsyncMock(return_value=snapshot))
    observer = AsyncMock(side_effect=AssertionError("assigned run must fail first"))
    monkeypatch.setattr(runtime, "_observe", observer)

    with pytest.raises(
        PTGWaveMaterializedPreclaimConflict,
        match="runs must be unassigned",
    ):
        await runtime.attest_locked_materialized_preclaim_abandonment(
            "session",
            "materialized-wave",
            "successor-wave",
            redis="redis",
        )
    observer.assert_not_awaited()


@pytest.mark.asyncio
async def test_control_route_is_exact_authenticated_and_idempotent(monkeypatch):
    registered_routes = []
    routes.register_control_wave_routes(_Blueprint(registered_routes))
    path = "/import-waves/<wave_id>/materialized-preclaim-abandonment"
    assert ("POST", path) in [
        (method, route) for method, route, _handler in registered_routes
    ]

    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    service = AsyncMock(return_value=({"created": True}, True))
    monkeypatch.setattr(routes, "abandon_materialized_preclaim_wave", service)
    request = SimpleNamespace(
        json={"cutover_id": "successor-wave"},
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_redis="redis")),
    )
    response = await routes.control_abandon_materialized_preclaim_wave(
        request,
        "materialized-wave",
    )
    assert response.status == 201
    service.assert_awaited_once_with(
        "materialized-wave", "successor-wave", redis="redis"
    )

    for request_body in ({}, {"cutover_id": "x", "extra": True}, []):
        with pytest.raises(BadRequest):
            await routes.control_abandon_materialized_preclaim_wave(
                SimpleNamespace(json=request_body, app=request.app),
                "materialized-wave",
            )

    service.side_effect = PTGWaveMaterializedPreclaimConflict("unsafe")
    with pytest.raises(SanicException) as exc_info:
        await routes.control_abandon_materialized_preclaim_wave(
            request,
            "materialized-wave",
        )
    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_ordinary_capacity_is_blocked_before_and_clear_after(monkeypatch):
    monkeypatch.setattr(
        fence,
        "_capacity_owning_waves",
        AsyncMock(return_value=[("materialized-wave", "slots_waiting")]),
    )
    with pytest.raises(fence.PTGWaveCapacityConflict, match="reserved"):
        await fence.require_no_capacity_owning_wave(object())

    fence._capacity_owning_waves.return_value = []
    await fence.require_no_capacity_owning_wave(object())


@pytest.mark.asyncio
async def test_capacity_query_excludes_only_proof_bound_abandonment():
    class _Executor:
        statement = None

        async def scalar(self, _statement, _parameters):
            return "mrf.ptg_import_wave"

        async def all(self, statement):
            self.statement = statement
            return []

    executor = _Executor()
    assert await fence._capacity_owning_waves(executor) == []
    sql = str(executor.statement)
    assert "ptg_import_wave_quarantine.recovery_basis" in sql
    assert "ptg_import_wave_quarantine.reason" not in sql
