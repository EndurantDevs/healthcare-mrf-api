# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure transaction/Reader boundary regressions; native authority is verified separately."""

import asyncio
import json
from contextlib import asynccontextmanager, contextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import network_serving_read_context as reads
from api.endpoint import network_catalog, network_providers, network_serving
from db import connection as database
from process.network_serving_read import NetworkServingReadUnavailable, PinnedNetworkServingManifest


class _Driver:
    def __init__(self, *, active=False, modes=("read committed", "off")):
        self.active, self.modes, self.pid = active, modes, 42
        self.identity = (11, 22, "127.0.0.1", 5432, 42)
        self.direct_login = True
        self.checks = 0
        self.after_check = None

    def is_in_transaction(self):
        """Expose the native transaction state independently from SQLAlchemy."""
        return self.active

    def get_server_pid(self):
        """Expose the native backend independently from captured Reader metadata."""
        return self.pid

    async def fetchrow(self, sql):
        """Model only the real snapshot query, including native identity fields."""
        assert sql == reads._SNAPSHOT_SQL
        self.checks += 1
        snapshot = (*self.modes, self.direct_login, *self.identity[:-1], self.pid)
        if self.after_check is not None:
            self.after_check()
        return snapshot


class _Session:
    def __init__(self, *, active=False, modes=("read committed", "off")):
        self.driver = _Driver(active=active, modes=modes)
        self.transaction = object() if active else None
        self.nested = None
        self.info = {}
        self.statements = []
        self.begins = self.commits = self.rollbacks = 0

    def in_transaction(self):
        """Expose SQLAlchemy state separately from its native driver."""
        return self.transaction is not None

    def get_transaction(self):
        """Return the exact outer transaction object."""
        return self.transaction

    def get_nested_transaction(self):
        """Expose a savepoint without allowing this reader to create one."""
        return self.nested

    @asynccontextmanager
    async def begin(self):
        """Record ownership and retire only a transaction this session began here."""
        assert self.transaction is None
        self.begins += 1
        self.transaction, self.driver.active = object(), True
        try:
            yield self.transaction
        except BaseException:
            self.rollbacks += 1
            raise
        else:
            self.commits += 1
        finally:
            self.transaction, self.driver.active = None, False

    async def execute(self, statement):
        """Apply explicit mode setup while recording every SQLAlchemy statement."""
        self.statements.append(str(statement))
        if str(statement).startswith("SET TRANSACTION"):
            self.driver.modes = ("repeatable read", "on")

    async def connection(self):
        """Return the session's original driver wrapper."""
        return self

    async def get_raw_connection(self):
        """Expose driver changes so continuity checks can refuse them."""
        return SimpleNamespace(driver_connection=self.driver)


@contextmanager
def _bound_reader(session):
    session.info["api_reader_verified"] = True
    session.info["api_reader_identity"] = dict(zip(reads._IDENTITY_FIELDS, session.driver.identity, strict=True))
    binding = database._TransactionBinding(1, session, asyncio.current_task())
    session_token = database._SESSION.set(session)
    reader_token = database._READER.set(binding)
    try:
        yield binding
    finally:
        database._READER.reset(reader_token)
        database._SESSION.reset(session_token)


async def test_idle_session_owns_fresh_snapshot():
    session = _Session()
    async with reads.network_serving_read_context(session) as driver:
        assert driver is session.driver and driver.active
        assert session.statements == [
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "SET LOCAL lock_timeout='1s'",
        ]
    assert (session.begins, session.commits, session.rollbacks) == (1, 1, 0)
    assert session.transaction is None and not session.driver.active


@pytest.mark.parametrize("modes", [("repeatable read", "on"), ("serializable", "on")])
async def test_bound_reader_keeps_caller_transaction(modes):
    session = _Session(active=True, modes=modes)
    original = session.transaction
    with _bound_reader(session) as binding:
        metadata = session.info["api_reader_identity"]
        async with reads.network_serving_read_context(session) as driver:
            assert driver is session.driver and database.current_session() is session
        assert database._READER.get() is binding and session.info["api_reader_identity"] is metadata
    assert session.transaction is original and session.driver.active
    assert (session.begins, session.commits, session.rollbacks) == (0, 0, 0)
    assert session.statements == ["SET LOCAL lock_timeout='1s'"]


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off"), ("serializable", "off")])
async def test_active_wrong_modes_refuse_untouched(modes):
    session = _Session(active=True, modes=modes)
    original = session.transaction
    with _bound_reader(session), pytest.raises(NetworkServingReadUnavailable):
        async with reads.network_serving_read_context(session):
            pytest.fail("wrong transaction modes reached the reader")
    assert session.transaction is original and session.driver.active and session.statements == []
    assert (session.begins, session.commits, session.rollbacks) == (0, 0, 0)


async def test_unbound_active_flags_confer_no_admission():
    session = _Session(active=True, modes=("repeatable read", "on"))
    session.info["api_reader_verified"] = True
    session.info["api_reader_identity"] = dict(zip(reads._IDENTITY_FIELDS, session.driver.identity, strict=True))
    with pytest.raises(NetworkServingReadUnavailable):
        async with reads.network_serving_read_context(session):
            pytest.fail("unbound active session was admitted")
    assert session.driver.checks == 0 and session.begins == 0


async def test_foreign_session_and_child_task_refuse():
    owner = _Session(active=True, modes=("repeatable read", "on"))
    foreign = _Session(active=True, modes=("repeatable read", "on"))

    async def attempt(session):
        async with reads.network_serving_read_context(session):
            pytest.fail("foreign Reader ownership was accepted")

    with _bound_reader(owner):
        with pytest.raises(NetworkServingReadUnavailable):
            await attempt(foreign)
        with pytest.raises(NetworkServingReadUnavailable):
            await asyncio.create_task(attempt(owner))
    assert owner.driver.checks == foreign.driver.checks == 0


@pytest.mark.parametrize("field", reads._IDENTITY_FIELDS)
async def test_native_reader_identity_must_match(field):
    session = _Session(active=True, modes=("repeatable read", "on"))
    with _bound_reader(session):
        session.info["api_reader_identity"][field] = "different"
        with pytest.raises(NetworkServingReadUnavailable):
            async with reads.network_serving_read_context(session):
                pytest.fail("captured identity mismatch reached the reader")
    assert session.statements == [] and session.begins == 0


@pytest.mark.parametrize(
    "damage", ["transaction", "driver", "pid", "native_transaction", "modes", "metadata", "savepoint"]
)
async def test_successful_exit_checks_original_snapshot(damage):
    session = _Session(active=True, modes=("repeatable read", "on"))
    original = session.transaction
    with _bound_reader(session), pytest.raises(NetworkServingReadUnavailable):
        async with reads.network_serving_read_context(session):
            if damage == "transaction":
                session.transaction = object()
            if damage == "driver":
                session.driver = _Driver(active=True, modes=("repeatable read", "on"))
            if damage == "pid":
                session.driver.pid += 1
            if damage == "native_transaction":
                session.driver.active = False
            if damage == "modes":
                session.driver.modes = ("repeatable read", "off")
            if damage == "metadata":
                session.info["api_reader_identity"] = dict(session.info["api_reader_identity"])
            if damage == "savepoint":
                session.nested = object()
    assert (session.begins, session.commits, session.rollbacks) == (0, 0, 0)
    assert session.transaction is original or damage == "transaction"


async def test_mode_check_cannot_replace_transaction():
    session = _Session(active=True, modes=("repeatable read", "on"))
    session.driver.after_check = lambda: setattr(session, "transaction", object())
    with _bound_reader(session), pytest.raises(NetworkServingReadUnavailable):
        async with reads.network_serving_read_context(session):
            pytest.fail("transaction changed during native admission")
    assert session.statements == []


@pytest.mark.parametrize("damage", ["metadata", "login", "savepoint"])
async def test_reader_admission_refuses_context_drift(damage):
    session = _Session(active=True, modes=("repeatable read", "on"))
    with _bound_reader(session):
        if damage == "metadata":
            session.driver.after_check = lambda: session.info["api_reader_identity"].update(role_oid=99)
        if damage == "login":
            session.driver.direct_login = False
        if damage == "savepoint":
            session.nested = object()
        with pytest.raises(NetworkServingReadUnavailable):
            async with reads.network_serving_read_context(session):
                pytest.fail("changed Reader context was admitted")
    assert session.statements == [] and session.begins == session.rollbacks == session.commits == 0


@pytest.mark.parametrize("active", [False, True])
@pytest.mark.parametrize("failure", [RuntimeError("original read failure"), asyncio.CancelledError()])
async def test_body_failure_preserves_caller_ownership(active, failure):
    session = _Session(active=active, modes=("repeatable read", "on"))
    original = session.transaction
    with _bound_reader(session) if active else nullcontext():
        with pytest.raises(type(failure)) as caught:
            async with reads.network_serving_read_context(session):
                session.driver.after_check = lambda: pytest.fail("body failure triggered exit SQL")
                raise failure
    assert caught.value is failure
    assert session.transaction is original and session.driver.active is active
    assert (session.begins, session.commits, session.rollbacks) == ((0, 0, 0) if active else (1, 0, 1))


def _request(session, route):
    return SimpleNamespace(
        ctx=SimpleNamespace(sa_session=session),
        headers={"Authorization": "Bearer fixture-token"},
        query_string="network_ids=7" if route == "providers" else "",
        body=json.dumps(
            {"client_id": "fixture-client", "policy_revision": "0", "excluded_network_ids": [], "query": {}}
        ).encode()
        if route == "catalog"
        else b"",
    )


async def _call_route(session, route, monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "fixture-token")
    manifest = PinnedNetworkServingManifest(
        9,
        "11111111-1111-4111-8111-111111111111",
        "network_candidate_11111111111141118111111111111111",
        1,
        {},
        0,
        "b" * 64,
        20,
    )
    if route == "catalog":
        monkeypatch.setattr(
            network_catalog,
            "_read_catalog_page",
            AsyncMock(
                return_value={
                    "generation": "9",
                    "approved_custom_revision": "0",
                    "total": 0,
                    "items": [],
                }
            ),
        )
        return await network_catalog._catalog_response(_request(session, route), "list")
    module = network_providers if route == "providers" else network_serving
    monkeypatch.setattr(module, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    if route == "providers":
        monkeypatch.setattr(
            module,
            "_read_provider_page",
            AsyncMock(return_value={"generation_id": 9, "total_count": 0, "providers": []}),
        )
        return await module.network_provider_page(_request(session, route))
    return await module.serving_manifest(_request(session, route))


@pytest.mark.parametrize("route", ["catalog", "providers", "manifest"])
async def test_handlers_borrow_original_reader(route, monkeypatch):
    session = _Session(active=True, modes=("repeatable read", "on"))
    original = session.transaction
    with _bound_reader(session):
        reply = await _call_route(session, route, monkeypatch)
    assert reply.status == 200 and reply.headers["X-Network-Generation"] == "9"
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert session.transaction is original and session.driver.active
    assert (session.begins, session.commits, session.rollbacks) == (0, 0, 0)


@pytest.mark.parametrize("route", ["catalog", "providers", "manifest"])
async def test_handlers_keep_refusal_private(route, monkeypatch):
    session = _Session(active=True, modes=("read committed", "off"))
    with _bound_reader(session):
        reply = await _call_route(session, route, monkeypatch)
    prefix = {"catalog": "network_catalog", "providers": "network_provider", "manifest": "network_serving"}[route]
    assert reply.status == 503 and json.loads(reply.body) == {"error": {"code": prefix + "_unavailable"}}
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert session.driver.active and session.begins == session.rollbacks == session.commits == 0
