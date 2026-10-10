# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deadline control flow and real fence cleanup through synthetic SQL boundaries."""

import asyncio
import json
import threading
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from multidict import CIMultiDict

from api import control_registry_ptg_scope as routes
from process import registry_company_approval_fence as fence
from process import registry_ptg_scope_engine as engine
from tests.test_registry_ptg_producer_scope import _Result
from tests.test_registry_ptg_scope_engine import ID, _actor, _command, _envelope, _service

_AUTHORIZE = engine.RegistryPTGScopeAuthorityClient.authorize


class _Boundary:
    def __init__(self):
        self.events = []
        self.rows_by_identity = {}
        self.pending_by_identity = {}
        self.info = {}
        self.transaction = object()
        self.blocked_stage = None
        self.entered = asyncio.Event()
        self.commit_visible = False
        self.expire_stage = None
        self.expire = lambda: None
        self.connection = self._connection()

    def _connection(self):
        return SimpleNamespace(
            sync_connection=object(),
            start=AsyncMock(side_effect=lambda: self.events.append("start")),
            execution_options=AsyncMock(),
            commit=AsyncMock(),
            scalar=self.is_lock_successful,
            invalidate=AsyncMock(side_effect=lambda: self.events.append("invalidate")),
            close=AsyncMock(side_effect=lambda: self.events.append("close")),
            in_transaction=lambda: False,
        )

    async def is_lock_successful(self, statement, parameters):
        self.events.append("unlock" if "unlock" in str(statement) else "lock")
        return True

    async def pause(self, stage):
        self.events.append(stage)
        if self.blocked_stage == stage:
            self.entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.events.append("cancelled")
        if self.expire_stage == stage:
            self.expire()

    def get_transaction(self):
        return self.transaction

    @asynccontextmanager
    async def sessions(self, **options):
        if options:
            assert options == {"bind": self.connection}
        self.events.append("session")
        try:
            yield self
        finally:
            self.events.append("session_closed")

    @asynccontextmanager
    async def begin(self):
        try:
            yield self
            if self.commit_visible:
                self.rows_by_identity.update(self.pending_by_identity)
            await self.pause("commit")
            self.rows_by_identity.update(self.pending_by_identity)
        except BaseException:
            self.events.append("rollback")
            raise
        finally:
            self.pending_by_identity.clear()

    async def execute(self, statement, parameters=None):
        if "INSERT INTO" in str(statement):
            await self.pause("insert")
            identity = parameters["scope_id"]
            if identity not in self.rows_by_identity:
                self.pending_by_identity[identity] = {
                    "approval_json": json.loads(parameters["document"]),
                    "approval_sha256": parameters["approval_sha256"],
                }
        elif "SELECT approval_sha256" in str(statement):
            await self.pause("retain")
            record = self.rows_by_identity.get(parameters["scope_id"]) or self.pending_by_identity.get(
                parameters["scope_id"]
            )
            return _Result(rows=[record] if record else [])
        return _Result()


@pytest.fixture
def boundary(monkeypatch):
    state = _Boundary()
    monkeypatch.setattr(fence, "_source_engine", lambda sessions: SimpleNamespace(connect=lambda: state.connection))
    original, _ = _service()
    service = engine.RegistryPTGScopeEngineService(
        original.schema_name, state.sessions, state.sessions, original.store, original.authority
    )

    async def protected(*arguments, **options):
        await state.pause("acl")
        return '"synthetic_control"."registry_ptg_producer_scope"'

    async def source(session, schema_name, intent, ownership, actor):
        await state.pause("source")
        specification = engine.RegistryPTGScopeSourceSpecification(
            ID,
            schema_name,
            ownership["snapshot_id"],
            ownership["source_key"],
            intent["company_key"],
            intent["cohort_id"],
        )
        frozen = SimpleNamespace(
            source_key=ownership["source_key"], snapshot_id=ownership["snapshot_id"], snapshot_manifest_sha256="c" * 64
        )
        full, document = engine._resolved_command(specification, frozen, schema_name, intent, ownership, actor)
        return full, document, {"selected_dense_source_keys": [0]}

    async def company(*arguments):
        await state.pause("company")

    async def authorize(authority, envelope, *, deadline=None):
        assert envelope["actor"] == _actor() and deadline is not None
        await state.pause("authority")

    monkeypatch.setattr(engine, "_protected_store", protected)
    monkeypatch.setattr(engine, "_resolved_source", source)
    monkeypatch.setattr(engine, "_approved_company", company)
    monkeypatch.setattr(engine.RegistryPTGScopeAuthorityClient, "authorize", authorize)
    return SimpleNamespace(service=service, state=state)


def _request(service, operation, deadline=None):
    return SimpleNamespace(
        headers={
            "Authorization": "Bearer synthetic-control",
            **({engine.SCOPE_DEADLINE_HEADER: deadline} if deadline is not None else {}),
        },
        body=json.dumps(_envelope(operation)).encode(),
        args={},
        query_string="",
        app=SimpleNamespace(ctx=SimpleNamespace(registry_ptg_scope_engine=service)),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "encoded",
    ["", "1", "9" * 14, "9" * 100000, "0" * 13, "1e12", " 1234567890123", "１２３４５６７８９０１２３", 1234567890123],
)
async def test_malformed_hint_precedes_source(monkeypatch, boundary, encoded):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    response = await routes.approve_registry_ptg_scope(_request(boundary.service, "approve", encoded))
    assert response.status == 400 and not boundary.state.events


@pytest.mark.asyncio
async def test_authentication_precedes_deadline(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    monkeypatch.setattr(
        routes,
        "registry_ptg_scope_deadline",
        lambda *arguments: (_ for _ in ()).throw(AssertionError("unauthenticated budget")),
    )
    response = await routes.approve_registry_ptg_scope(SimpleNamespace(headers={}))
    assert response.status == 401


@pytest.mark.asyncio
async def test_duplicate_hint_precedes_session(monkeypatch, boundary):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    request = _request(boundary.service, "approve")
    request.headers = CIMultiDict(request.headers)
    for _ in range(2):
        request.headers.add(engine.SCOPE_DEADLINE_HEADER, str(int((time.time() + 1) * 1000)))
    response = await routes.approve_registry_ptg_scope(request)
    assert response.status == 400 and not boundary.state.events


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["preview", "approve"])
async def test_expired_hint_precedes_session(monkeypatch, boundary, operation):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    request = _request(boundary.service, operation, str(int((time.time() - 1) * 1000)))
    handler = routes.preview_registry_ptg_scope if operation == "preview" else routes.approve_registry_ptg_scope
    response = await handler(request)
    assert response.status == 504 and not boundary.state.events
    assert json.loads(response.body) == {"error": {"code": "registry_source_scope_deadline_expired"}}


@pytest.mark.asyncio
async def test_default_budget_cannot_widen():
    before = asyncio.get_running_loop().time()
    assert before < engine.registry_ptg_scope_deadline() <= before + 9.01
    assert engine.registry_ptg_scope_deadline(str(int((time.time() + 1000) * 1000))) <= before + 9.01
    assert engine._deadline(before + 1000) <= before + 9.01
    for invalid in [True, float("nan"), float("inf"), "9"]:
        with pytest.raises(ValueError):
            engine._deadline(invalid)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["source", "authority", "insert"])
async def test_precommit_expiry_cleans_fence(boundary, stage):
    boundary.state.blocked_stage = stage
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired) as refused:
        await boundary.service.approve(_envelope("approve"), deadline=asyncio.get_running_loop().time() + 0.01)
    assert not refused.value.outcome_unknown
    assert not boundary.state.rows_by_identity and not boundary.state.pending_by_identity and not boundary.state.info
    assert boundary.state.events[-2:] == ["invalidate", "close"] and "rollback" in boundary.state.events


@pytest.mark.asyncio
async def test_preview_expiry_closes_reader(boundary):
    boundary.state.blocked_stage = "source"
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired) as refused:
        await boundary.service.preview(_envelope(), deadline=asyncio.get_running_loop().time() + 0.01)
    assert not refused.value.outcome_unknown
    assert "rollback" in boundary.state.events and boundary.state.events[-1] == "session_closed"
    assert "insert" not in boundary.state.events


@pytest.mark.asyncio
async def test_expired_connect_is_drained(boundary):
    async def start():
        boundary.state.events.append("connecting")
        await asyncio.sleep(0.025)
        boundary.state.events.append("connected")

    boundary.state.connection.start = start
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired) as refused:
        await boundary.service.approve(_envelope("approve"), deadline=asyncio.get_running_loop().time() + 0.01)
    assert not refused.value.outcome_unknown
    assert boundary.state.events == ["connecting", "connected", "invalidate", "close"]
    assert not boundary.state.info and not boundary.state.rows_by_identity


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["authority", "retain"])
async def test_expiry_gates_insert_and_commit(monkeypatch, boundary, stage):
    def expire():
        monkeypatch.setattr(engine, "_remaining", lambda deadline: (_ for _ in ()).throw(TimeoutError()))

    boundary.state.expire = expire
    boundary.state.expire_stage = stage
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired) as refused:
        await boundary.service.approve(_envelope("approve"))
    assert not refused.value.outcome_unknown and "commit" not in boundary.state.events
    assert ("insert" in boundary.state.events) is (stage == "retain")
    assert not boundary.state.rows_by_identity and not boundary.state.info


@pytest.mark.asyncio
async def test_uncertain_commit_replays_exactly(boundary):
    boundary.state.blocked_stage = "commit"
    boundary.state.commit_visible = True
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired) as uncertain:
        await boundary.service.approve(_envelope("approve"), deadline=asyncio.get_running_loop().time() + 0.01)
    assert uncertain.value.outcome_unknown and len(boundary.state.rows_by_identity) == 1
    boundary.state.blocked_stage = None
    receipt = await boundary.service.approve(_envelope("approve"))
    assert receipt["command_sha256"] == engine._digest(_command())
    assert len(boundary.state.rows_by_identity) == 1 and boundary.state.events.count("authority") == 2
    changed = _envelope("approve")
    changed["command"]["reason"] = "Changed review"
    with pytest.raises(ValueError, match="idempotency_conflict"):
        await boundary.service.approve(changed)
    assert len(boundary.state.rows_by_identity) == 1 and not boundary.state.info


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["source", "authority", "insert", "commit"])
async def test_cancellation_retires_owned_connection(boundary, stage):
    boundary.state.blocked_stage = stage
    task = asyncio.create_task(boundary.service.approve(_envelope("approve")))
    await asyncio.wait_for(boundary.state.entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not boundary.state.rows_by_identity and not boundary.state.pending_by_identity and not boundary.state.info
    assert boundary.state.events[-2:] == ["invalidate", "close"]


@pytest.mark.asyncio
async def test_repeated_cancellation_drains_close(boundary):
    closing = asyncio.Event()
    close_calls = []

    async def close():
        close_calls.append("closing")
        if len(close_calls) == 1:
            closing.set()
            await asyncio.Event().wait()
        close_calls.append("closed")

    boundary.state.connection.close = close
    boundary.state.blocked_stage = "source"
    task = asyncio.create_task(boundary.service.approve(_envelope("approve")))
    await asyncio.wait_for(boundary.state.entered.wait(), 1)
    task.cancel()
    await asyncio.wait_for(closing.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert close_calls == ["closing", "closing", "closed"]
    assert boundary.state.connection.invalidate.await_count == 2
    assert not boundary.state.info and not boundary.state.rows_by_identity


@pytest.mark.asyncio
async def test_late_authority_never_retains(monkeypatch, boundary):
    finished = threading.Event()
    received_timeouts = []

    def request(authority, envelope, *, timeout_seconds=None):
        received_timeouts.append(timeout_seconds)
        threading.Event().wait(0.05)
        finished.set()
        return {
            "authorized": True,
            "actor": _actor(),
            "command_sha256": engine._digest(_command()),
            "policy_revision": 3,
        }

    monkeypatch.setattr(engine.RegistryPTGScopeAuthorityClient, "authorize", _AUTHORIZE)
    monkeypatch.setattr(engine.RegistryPTGScopeAuthorityClient, "_request", request)
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired) as refused:
        await boundary.service.approve(_envelope("approve"), deadline=asyncio.get_running_loop().time() + 0.01)
    assert not refused.value.outcome_unknown and "insert" not in boundary.state.events
    assert 0 < received_timeouts[0] <= 0.01
    assert boundary.state.events[-2:] == ["invalidate", "close"]
    assert await asyncio.to_thread(finished.wait, 0.5)


@pytest.mark.asyncio
async def test_postcommit_cleanup_failure_is_unknown(boundary):
    async def is_lock_successful(statement, parameters):
        if "unlock" in str(statement):
            raise ValueError("synthetic cleanup failure")
        return True

    boundary.state.connection.scalar = is_lock_successful
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired) as uncertain:
        await boundary.service.approve(_envelope("approve"))
    assert uncertain.value.outcome_unknown and len(boundary.state.rows_by_identity) == 1
    assert boundary.state.events[-2:] == ["invalidate", "close"] and not boundary.state.info


@pytest.mark.asyncio
async def test_uncertain_commit_route_is_unknown(monkeypatch, boundary):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    monkeypatch.setattr(engine, "SCOPE_REQUEST_TIMEOUT_SECONDS", 0.01)
    boundary.state.blocked_stage = "commit"
    boundary.state.commit_visible = True
    response = await routes.approve_registry_ptg_scope(_request(boundary.service, "approve"))
    assert response.status == 504 and len(boundary.state.rows_by_identity) == 1
    assert json.loads(response.body) == {"error": {"code": "registry_source_scope_outcome_unknown"}}
    assert response.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
async def test_authority_socket_uses_remaining(monkeypatch):
    timeouts = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *arguments):
            return None

        def read(self, size):
            assert size == 4097
            return json.dumps(
                {
                    "authorized": True,
                    "actor": _actor(),
                    "command_sha256": engine._digest(_command()),
                    "policy_revision": 3,
                }
            ).encode()

    class Opener:
        def open(self, request, timeout):
            timeouts.append(timeout)
            return Response()

    def opener(*handlers):
        assert any(isinstance(h, engine.ProxyHandler) and h.proxies == {} for h in handlers)
        assert any(isinstance(h, engine._NoRedirects) for h in handlers)
        return Opener()

    monkeypatch.setattr(engine, "build_opener", opener)
    authority = engine.RegistryPTGScopeAuthorityClient(
        "https://authority.example", "synthetic-service", timeout_seconds=0.5
    )
    assert (await authority.authorize(_envelope("approve"), deadline=asyncio.get_running_loop().time() + 0.1))[
        "authorized"
    ]
    assert 0 < timeouts[0] <= 0.1
