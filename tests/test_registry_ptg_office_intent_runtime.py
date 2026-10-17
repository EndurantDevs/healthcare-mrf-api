# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Installed durable factories and signed dispatch, with explicit native probe mocks."""

import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from asyncpg import InterfaceError, PostgresError
from sanic import Sanic
from sanic.exceptions import Forbidden

from api import control_registry_ptg_scope as routes
from process import registry_ptg_office_coordinator as coordinator
from process import registry_ptg_office_intent_view as view
from process import registry_ptg_scope_runtime as runtime
from tests.test_registry_ptg_office_intent_view import envelope as list_envelope
from tests.test_registry_ptg_office_intent_view import internal_result, records
from tests.test_registry_ptg_office_preparation import envelope as legacy_envelope
from tests.test_registry_ptg_office_preparation_runtime import _office_configuration
from tests.test_registry_ptg_office_preparation_runtime import owned as owned
from tests.test_registry_ptg_scope_runtime import _app, _Sessions


def configured():
    return {
        **_office_configuration(),
        "office_intents": {
            "management_dsn": "postgresql://office_management@database.example/synthetic?ssl=require",
            "queue_dsn": "postgresql://office_queue@database.example/synthetic?ssl=require",
        },
    }


@pytest.fixture
async def durable_app(owned, monkeypatch):
    native_probe = AsyncMock()
    monkeypatch.setattr(runtime, "_check_office_coordinator", native_probe)
    app, _ = _app(configured())
    await runtime._start_runtime(app)
    try:
        yield app, native_probe
    finally:
        await runtime._stop_runtime(app)


@pytest.mark.asyncio
async def test_installed_coordinator_owns_distinct_factories_pool_and_exact_shutdown(durable_app, owned):
    app, probe = durable_app
    installed = app.ctx.registry_ptg_office_coordinator
    assert type(installed) is coordinator.RegistryPTGOfficeIntentCoordinator
    assert installed.preparation is app.ctx.registry_ptg_office_preparation
    assert installed.management_role == "office_management" and installed.queue_role == "office_queue"
    assert installed.management_sessions.kw["bind"] is owned.engines[3]
    assert (
        len(owned.engines) == 4
        and installed.queue_pool.get_min_size() == 0
        and installed.queue_pool.get_max_size() == 1
    )
    assert installed.queue_pool.get_size() == 0
    probe.assert_awaited_once_with(installed)
    await runtime._stop_runtime(app)
    assert installed.queue_pool.is_closing() and owned.disposed == list(reversed(owned.engines))
    assert app.ctx.registry_ptg_office_coordinator is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda document: document["office_intents"].pop("queue_dsn"),
        lambda document: document["office_intents"].update(extra=True),
        lambda document: document.pop("publisher_dsn"),
        lambda document: document["office_intents"].update(
            queue_dsn="postgresql://office_management@database.example/synthetic?ssl=require"
        ),
        lambda document: document["office_intents"].update(
            queue_dsn="postgresql://scope_reader@database.example/synthetic?ssl=require"
        ),
        lambda document: document["office_intents"].update(
            management_dsn="postgresql://office_owner@database.example/synthetic?ssl=require"
        ),
        lambda document: document["office_intents"].update(
            queue_dsn="postgresql://office_queue@different.example/synthetic?ssl=require"
        ),
        lambda document: document["office_intents"].update(queue_dsn=True),
    ],
)
async def test_invalid_intent_configuration_refuses_before_ownership(owned, mutation):
    document = configured()
    mutation(document)
    with pytest.raises(ValueError, match="configuration_invalid"):
        await runtime.build_registry_ptg_scope_runtime(document)
    assert not owned.engines


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["constructor", "native", "cancel"])
async def test_constructor_native_failure_and_cancellation_close_pool_and_engines(owned, monkeypatch, failure):
    pools = []
    original = runtime.asyncpg.create_pool

    def create(*args, **kwargs):
        pool = original(*args, **kwargs)
        pools.append(pool)
        return pool

    monkeypatch.setattr(runtime.asyncpg, "create_pool", create)
    if failure == "constructor":
        monkeypatch.setattr(
            coordinator.RegistryPTGOfficeIntentCoordinator,
            "__post_init__",
            lambda _self: (_ for _ in ()).throw(ValueError("denied")),
        )
    else:
        monkeypatch.setattr(
            runtime,
            "_check_office_coordinator",
            AsyncMock(side_effect=asyncio.CancelledError() if failure == "cancel" else PermissionError("denied")),
        )
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else ValueError):
        await runtime.build_registry_ptg_scope_runtime(configured())
    assert len(pools) == 1 and pools[0].is_closing()
    assert owned.disposed == list(reversed(owned.engines))


@pytest.mark.asyncio
async def test_existing_coordinator_owner_refuses_before_factories(owned):
    app, _ = _app(configured())
    original = object()
    app.ctx.registry_ptg_office_coordinator = original
    with pytest.raises(ValueError, match="service_unconfigured"):
        await runtime._start_runtime(app)
    assert not owned.engines and app.ctx.registry_ptg_office_coordinator is original


def request(app, document):
    return SimpleNamespace(app=app, body=json.dumps(document).encode(), headers={}, args={}, query_string="")


async def prepare_documents():
    result = await internal_result(completed=True)
    document = list_envelope()
    document["selector"] = {
        name: value
        for name, value in result["intent"]["command"]["selector"].items()
        if name not in {"capture_id", "idempotency_key"}
    }
    document["read_limits"] = {"maximum_bytes": 1048576, "maximum_pages": 32, "maximum_coordinates": 100}
    return document, result


@pytest.mark.asyncio
async def test_signed_prepare_dispatches_only_coordinator_and_projects_original_handles(durable_app, monkeypatch):
    app, _ = durable_app
    document, result = await prepare_documents()
    calls = []

    async def prepare(installed, envelope, *, deadline):
        calls.append((installed, envelope))
        return result

    monkeypatch.setattr(coordinator.RegistryPTGOfficeIntentCoordinator, "prepare", prepare)
    monkeypatch.setattr(routes, "require_registry_auth", lambda _request: None)
    reply = await routes.prepare_registry_ptg_offices(request(app, document))
    assert reply.status == 200 and len(calls) == 1 and calls[0][0] is app.ctx.registry_ptg_office_coordinator
    payload = json.loads(reply.body)
    assert set(payload) == {"source_command_sha256", "scope_approval_sha256", "intent", "preparation"}
    assert payload["intent"]["selector"] == result["intent"]["command"]["selector"]
    assert "capture_id" not in calls[0][1]["selector"]


@pytest.mark.asyncio
async def test_legacy_direct_handle_prepare_refuses_even_with_installed_coordinator(durable_app, monkeypatch):
    app, _ = durable_app
    execute = AsyncMock()
    monkeypatch.setattr(coordinator.RegistryPTGOfficeIntentCoordinator, "prepare", execute)
    monkeypatch.setattr(routes, "require_registry_auth", lambda _request: None)
    assert (await routes.prepare_registry_ptg_offices(request(app, legacy_envelope()))).status == 400
    execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["projection", "serialization", "wrong_semantic"])
async def test_prepare_post_handoff_failures_are_unknown_without_replacement(durable_app, monkeypatch, failure):
    app, _ = durable_app
    document, result = await prepare_documents()
    calls = AsyncMock(return_value=result)
    monkeypatch.setattr(coordinator.RegistryPTGOfficeIntentCoordinator, "prepare", calls)
    monkeypatch.setattr(routes, "require_registry_auth", lambda _request: None)
    if failure == "projection":
        monkeypatch.setattr(
            view,
            "project_registry_ptg_office_prepare_result",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad reply")),
        )
    elif failure == "serialization":
        original = routes._reply

        def reply(payload, status=200):
            if "intent" in payload:
                raise TypeError("encoding failure")
            return original(payload, status)

        monkeypatch.setattr(routes, "_reply", reply)
    else:
        result["intent"]["semantic_sha256"] = "0" * 64
    reply = await routes.prepare_registry_ptg_offices(request(app, document))
    assert reply.status == 504 and json.loads(reply.body)["error"]["code"] == "registry_source_scope_outcome_unknown"
    assert calls.await_count == 1


@pytest.mark.asyncio
async def test_signed_list_route_and_authentication(durable_app, monkeypatch):
    app, _ = durable_app
    reply_by_field = {
        "source_command_sha256": view.capture_digest(list_envelope()["source_command"]),
        "scope_approval_sha256": list_envelope()["selector"]["scope_approval_sha256"],
        "items": [],
        "next_cursor": None,
    }
    calls = AsyncMock(return_value=reply_by_field)
    monkeypatch.setattr(coordinator.RegistryPTGOfficeIntentCoordinator, "list", calls)
    monkeypatch.setattr(routes, "require_registry_auth", lambda _request: None)
    reply = await routes.read_registry_ptg_office_intents(request(app, list_envelope()))
    assert reply.status == 200 and json.loads(reply.body) == reply_by_field and calls.await_count == 1

    def denied(_request):
        raise Forbidden("denied")

    monkeypatch.setattr(routes, "require_registry_auth", denied)
    assert (await routes.read_registry_ptg_office_intents(request(app, list_envelope()))).status == 401
    assert calls.await_count == 1


@pytest.mark.asyncio
async def test_publisher_only_configuration_keeps_reader_and_refuses_durable_prepare(owned, monkeypatch):
    app, _ = _app(_office_configuration())
    await runtime._start_runtime(app)
    try:
        monkeypatch.setattr(routes, "require_registry_auth", lambda _request: None)
        document, _ = await prepare_documents()
        assert (await routes.prepare_registry_ptg_offices(request(app, document))).status == 503
        assert routes._operation_service(app, "read_offices")[0] is app.ctx.registry_ptg_office_preparation
        assert routes._operation_service(app, "office_choices")[0] is app.ctx.registry_ptg_office_preparation
    finally:
        await runtime._stop_runtime(app)


class ListConnection:
    def __init__(self, request_records):
        self.request_records = request_records
        self.transactions = []
        self.fetch = AsyncMock(return_value=request_records)

    def is_in_transaction(self):
        return True

    @asynccontextmanager
    async def transaction(self, **options):
        self.transactions.append(options)
        yield self


class ListPool:
    def __init__(self, connection):
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "pre_auth", "custody", "post_auth", "identity", "timeout"])
async def test_list_method_reauthorizes_actual_source_before_and_after_bounded_read(monkeypatch, failure):
    connection = ListConnection(await records())
    installed = object.__new__(coordinator.RegistryPTGOfficeIntentCoordinator)
    object.__setattr__(
        installed,
        "preparation",
        SimpleNamespace(scope_service=SimpleNamespace(store=SimpleNamespace(control_schema="synthetic_registry"))),
    )
    object.__setattr__(installed, "queue_pool", ListPool(connection))
    identity = (42, "123456789")
    identities = [
        PermissionError("denied") if failure == "pre_auth" else identity,
        PermissionError("denied")
        if failure == "post_auth"
        else (43, "123456789")
        if failure == "identity"
        else identity,
    ]
    approved_source_mock = AsyncMock(side_effect=identities)
    custody = AsyncMock(side_effect=ValueError("custody") if failure == "custody" else None)
    monkeypatch.setattr(coordinator, "_approved_source", approved_source_mock)
    monkeypatch.setattr(coordinator, "_queue_permissions", custody)
    if failure == "timeout":
        connection.fetch.side_effect = TimeoutError()
    if failure:
        with pytest.raises(
            coordinator.RegistryPTGScopeDeadlineExpired
            if failure == "timeout"
            else PermissionError
            if failure in {"pre_auth", "post_auth"}
            else ValueError
        ) as error:
            await installed.list(list_envelope())
        if failure == "timeout":
            assert not error.value.outcome_unknown
    else:
        assert len((await installed.list(list_envelope()))["items"]) == 1
    assert connection.fetch.await_count == (0 if failure in {"pre_auth", "custody"} else 1)
    if connection.transactions:
        assert connection.transactions == [{"isolation": "read_committed", "readonly": True}]
    if not failure:
        assert approved_source_mock.await_count == 2 and custody.await_count == 1


def startup_factories(events):
    factories = [_Sessions(name, events) for name in ("reader", "publisher", "management")]
    for factory in factories:
        factory.kw = {"bind": SimpleNamespace(url=SimpleNamespace(username=factory.name))}
        factory.session.name = factory.name
    return factories


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "management", "identity", "queue"])
async def test_startup_proves_readonly_native_role_identity_and_global_queue_custody(monkeypatch, failure):
    """All native profiles are read-only, ordered and required before exposure."""
    from process import network_cms_registry_address_capture as capture
    from process import registry_management_permissions as permissions

    events = []
    factories = startup_factories(events)
    native = ("123456", 42)

    async def role(connection, expected):
        assert connection.name == expected
        events.append((expected, "role"))

    async def identity(connection):
        events.append((connection.name, "identity"))
        return ("different", 42) if failure == "identity" and connection.name == "publisher" else native

    async def management(connection, **options):
        events.append((connection.name, "management_profile"))
        assert options == {"api_role": "management", "owner_role": "owner", "control_schema": "control"}
        if failure == "management":
            raise PermissionError("management denied")

    async def queue(connection, installed, expected):
        events.append(("queue", "global_profile"))
        assert expected == native and connection.transactions == [{"isolation": "read_committed", "readonly": True}]
        if failure == "queue":
            raise PermissionError("queue denied")

    pool = ListPool(ListConnection([]))
    installed = SimpleNamespace(
        preparation=SimpleNamespace(
            scope_service=SimpleNamespace(reader_sessions=factories[0]), publisher_sessions=factories[1]
        ),
        management_sessions=factories[2],
        management_role="management",
        owner_role="owner",
        schema="control",
        queue_pool=pool,
    )
    monkeypatch.setattr(capture, "native_driver", AsyncMock(side_effect=lambda session: session))
    monkeypatch.setattr(coordinator, "_role", role)
    monkeypatch.setattr(coordinator, "_native_identity", identity)
    monkeypatch.setattr(coordinator, "_queue_permissions", queue)
    monkeypatch.setattr(permissions, "verify_registry_management_permissions", management)
    if failure:
        with pytest.raises(ValueError if failure == "identity" else PermissionError):
            await runtime._check_office_coordinator(installed)
    else:
        await runtime._check_office_coordinator(installed)
    for factory in factories:
        statements = [event[2] for event in events if event[:2] == (factory.name, "sql")]
        assert statements[:2] == [
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY",
            "SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)",
        ]
    assert ("management", "management_profile") in events
    assert (("queue", "global_profile") in events) == (failure not in {"management", "identity"})


@pytest.mark.asyncio
async def test_installer_failure_disposes_exact_owned_pool_and_all_engines(owned, monkeypatch):
    pools = []
    original = runtime.asyncpg.create_pool

    def create(*args, **kwargs):
        pool = original(*args, **kwargs)
        pools.append(pool)
        return pool

    def refuse(*_args, **_kwargs):
        raise ValueError("installation refused")

    monkeypatch.setattr(runtime.asyncpg, "create_pool", create)
    monkeypatch.setattr(runtime, "_check_office_coordinator", AsyncMock())
    monkeypatch.setattr(routes, "install_registry_ptg_scope_engine", refuse)
    app, _ = _app(configured())
    with pytest.raises(ValueError, match="installation refused"):
        await runtime._start_runtime(app)
    assert len(pools) == 1 and pools[0].is_closing()
    assert owned.disposed == list(reversed(owned.engines))
    assert getattr(app.ctx, "registry_ptg_scope_runtime", None) is None
    assert getattr(app.ctx, "registry_ptg_office_coordinator", None) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda document: document.update(read_limits={}),
        lambda document: document["selector"].update(limit=True),
        lambda document: document["selector"].update(after="not-a-request"),
    ],
)
async def test_signed_list_rejects_nonclosed_inputs_before_dispatch(durable_app, monkeypatch, mutation):
    app, _ = durable_app
    document = list_envelope()
    mutation(document)
    calls = AsyncMock()
    monkeypatch.setattr(coordinator.RegistryPTGOfficeIntentCoordinator, "list", calls)
    monkeypatch.setattr(routes, "require_registry_auth", lambda _request: None)
    reply = await routes.read_registry_ptg_office_intents(request(app, document))
    assert reply.status == 400
    calls.assert_not_awaited()


@pytest.mark.asyncio
async def test_actual_list_post_route_preserves_control_auth_method_and_closed_page(durable_app, monkeypatch):
    installed, _ = durable_app
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    reply_by_field = {
        "source_command_sha256": view.capture_digest(list_envelope()["source_command"]),
        "scope_approval_sha256": list_envelope()["selector"]["scope_approval_sha256"],
        "items": [],
        "next_cursor": None,
    }
    calls = AsyncMock(return_value=reply_by_field)
    monkeypatch.setattr(coordinator.RegistryPTGOfficeIntentCoordinator, "list", calls)
    app = Sanic("synthetic_office_intents_" + uuid4().hex)
    prefix = "/control/v1/registry/ptg-source-scopes"
    app.blueprint(routes.blueprint, url_prefix=prefix)
    routes.install_registry_ptg_scope_engine(
        app,
        installed.ctx.registry_ptg_scope_engine,
        office_preparation=installed.ctx.registry_ptg_office_preparation,
        office_coordinator=installed.ctx.registry_ptg_office_coordinator,
    )
    try:
        _, denied = await app.asgi_client.post(prefix + "/office-intents", json=list_envelope())
        assert denied.status_code == 401 and calls.await_count == 0
        _, allowed = await app.asgi_client.post(
            prefix + "/office-intents",
            json=list_envelope(),
            headers={"Authorization": "Bearer synthetic-control"},
        )
        assert allowed.status_code == 200 and allowed.json == reply_by_field and calls.await_count == 1
        _, wrong_method = await app.asgi_client.get(prefix + "/office-intents")
        assert wrong_method.status_code == 405 and calls.await_count == 1
    finally:
        Sanic.unregister_app(app)


@pytest.mark.asyncio
async def test_signed_list_refuses_nonplatform_actor_before_dispatch(durable_app, monkeypatch):
    app, _ = durable_app
    document = list_envelope()
    document["actor"]["client_id"] = "synthetic_client"
    calls = AsyncMock()
    monkeypatch.setattr(coordinator.RegistryPTGOfficeIntentCoordinator, "list", calls)
    monkeypatch.setattr(routes, "require_registry_auth", lambda _request: None)
    reply = await routes.read_registry_ptg_office_intents(request(app, document))
    assert reply.status == 403 and json.loads(reply.body) == {"error": {"code": "registry_access_denied"}}
    calls.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [PostgresError("synthetic private detail"), InterfaceError("synthetic private detail")]
)
async def test_list_native_driver_errors_are_sanitized_unavailable(durable_app, monkeypatch, failure):
    app, _ = durable_app
    calls = AsyncMock(side_effect=failure)
    monkeypatch.setattr(coordinator.RegistryPTGOfficeIntentCoordinator, "list", calls)
    monkeypatch.setattr(routes, "require_registry_auth", lambda _request: None)
    reply = await routes.read_registry_ptg_office_intents(request(app, list_envelope()))
    assert reply.status == 503 and json.loads(reply.body) == {"error": {"code": "registry_unavailable"}}
    assert calls.await_count == 1
