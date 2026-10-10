# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Server input, native-proof ordering and exact engine lifecycle boundaries."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic import Sanic

from api import control_registry_ptg_scope as routes
from process import registry_ptg_scope_runtime as runtime
from process.registry_ptg_scope_engine import RegistryPTGScopeEngineService


def _configured():
    return {
        "ptg_schema": "synthetic_ptg",
        "control_schema": "synthetic_control",
        "owner_role": "scope_owner",
        "approval_role": "scope_approver",
        "reader_dsn": "postgresql://scope_reader@database.example/synthetic?ssl=require",
        "approver_dsn": "postgresql+asyncpg://scope_approver@database.example/synthetic?ssl=require",
        "app_origin": "https://authority.example",
        "authority_token": "synthetic-service",
    }


class _Sessions:
    def __init__(self, name, events):
        self.name = name
        self.events = events
        self.session = SimpleNamespace(execute=AsyncMock(side_effect=self._execute), begin=self._transaction)

    async def _execute(self, statement):
        self.events.append((self.name, "sql", str(statement)))

    @asynccontextmanager
    async def _transaction(self):
        self.events.append((self.name, "transaction"))
        yield self.session

    @asynccontextmanager
    async def __call__(self):
        self.events.append((self.name, "session"))
        try:
            yield self.session
        finally:
            self.events.append((self.name, "session_closed"))


def _boundaries(monkeypatch, *, failure=None, app=None):
    events = []
    engines = []
    sessions = []

    def create_engine(url, **options):
        name = "reader" if not engines else "approver"
        if failure == "second-engine" and engines:
            raise RuntimeError("synthetic engine failure")
        engine = SimpleNamespace(url=url, options=options, dispose=AsyncMock())
        engine.dispose.side_effect = lambda: events.append((name, "disposed"))
        engines.append(engine)
        return engine

    def sessionmaker(engine, **options):
        if failure == "sessionmaker":
            raise RuntimeError("synthetic sessions failure")
        factory = _Sessions("reader" if len(sessions) == 0 else "approver", events)
        factory.options = options
        sessions.append(factory)
        return factory

    async def proof(session, store, *, write):
        if app is not None:
            assert getattr(app.ctx, "registry_ptg_scope_engine", None) is None
        events.append(("approver" if write else "reader", "proof", write))
        assert store.control_schema == "synthetic_control"
        if failure == ("write-proof" if write else "read-proof"):
            raise PermissionError("synthetic native ACL refusal")

    monkeypatch.setattr(runtime, "create_async_engine", Mock(side_effect=create_engine))
    monkeypatch.setattr(runtime, "async_sessionmaker", Mock(side_effect=sessionmaker))
    monkeypatch.setattr(runtime, "_protected_store", AsyncMock(side_effect=proof))
    return SimpleNamespace(events=events, engines=engines, sessions=sessions)


def _app(configured=None):
    listeners_by_event = {}
    app = SimpleNamespace(ctx=SimpleNamespace(), config={} if configured is None else {runtime.CONFIG_KEY: configured})
    app.listener = lambda name: lambda callback: listeners_by_event.setdefault(name, callback)
    return app, listeners_by_event


@pytest.mark.asyncio
async def test_absent_configuration_keeps_service_unavailable_without_database(monkeypatch):
    app, listeners = _app()
    boundary = _boundaries(monkeypatch)
    runtime.register_registry_ptg_scope_runtime(app)
    assert set(listeners) == {"before_server_start", "after_server_stop"}
    await listeners["before_server_start"](app)
    await listeners["after_server_stop"](app)
    assert getattr(app.ctx, "registry_ptg_scope_engine", None) is None
    assert boundary.engines == []
    request = SimpleNamespace(app=app, body=b"{}", args={}, query_string="", headers={})
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(routes, "_body", lambda _request, _operation: {})
    assert (await routes.preview_registry_ptg_scope(request)).status == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", [False, True], ids=["trusted-server-mapping", "deployed-json"])
async def test_startup_proves_both_roles_before_install_and_disposes_once(monkeypatch, encoded):
    configured = _configured()
    app, listeners = _app(json.dumps(configured) if encoded else configured)
    boundary = _boundaries(monkeypatch, app=app)
    runtime.register_registry_ptg_scope_runtime(app)
    await listeners["before_server_start"](app, None)
    service = app.ctx.registry_ptg_scope_engine
    assert type(service) is RegistryPTGScopeEngineService
    assert service.schema_name == configured["ptg_schema"]
    assert service.authority.token == configured["authority_token"]
    assert service.authority.base_url == configured["app_origin"]
    assert service.reader_sessions is boundary.sessions[0]
    assert service.approval_sessions is boundary.sessions[1]
    assert [call.kwargs["write"] for call in runtime._protected_store.await_args_list] == [False, True]
    for engine in boundary.engines:
        assert engine.options["execution_options"] == {"postgresql_readonly": False}
        assert engine.options["isolation_level"] == "REPEATABLE READ"
        assert engine.options["hide_parameters"] and not engine.options["echo"]
        assert engine.options["pool_size"] == 1 and engine.options["max_overflow"] == 0
    assert all("READ WRITE" in str(factory.session.execute.call_args.args[0]) for factory in boundary.sessions)
    retained = app.ctx.registry_ptg_scope_runtime
    await listeners["after_server_stop"](app)
    await listeners["after_server_stop"](app)
    await retained.close()
    assert app.ctx.registry_ptg_scope_engine is None and app.ctx.registry_ptg_scope_runtime is None
    assert [entry[:2] for entry in boundary.events if entry[1] == "disposed"] == [
        ("approver", "disposed"),
        ("reader", "disposed"),
    ]
    assert all(engine.dispose.await_count == 1 for engine in boundary.engines)


@pytest.mark.asyncio
async def test_actual_sanic_environment_loader_supplies_one_json_input(monkeypatch):
    encoded = json.dumps(_configured())
    monkeypatch.setenv("HLTHPRT_REGISTRY_PTG_SCOPE_RUNTIME", encoded)
    app = Sanic("synthetic_scope_runtime", env_prefix="HLTHPRT_")
    boundary = _boundaries(monkeypatch)
    assert app.config[runtime.CONFIG_KEY] == encoded
    try:
        runtime.register_registry_ptg_scope_runtime(app)
        await runtime._start_runtime(app)
        assert app.ctx.registry_ptg_scope_engine.authority.token == "synthetic-service"
    finally:
        await runtime._stop_runtime(app)
        Sanic.unregister_app(app)
    assert all(engine.dispose.await_count == 1 for engine in boundary.engines)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["second-engine", "sessionmaker", "read-proof", "write-proof"])
async def test_partial_startup_refusal_disposes_every_created_engine(monkeypatch, failure):
    app, _listeners = _app(json.dumps(_configured()))
    boundary = _boundaries(monkeypatch, failure=failure, app=app)
    with pytest.raises((RuntimeError, PermissionError)):
        await runtime._start_runtime(app)
    assert boundary.engines and all(engine.dispose.await_count == 1 for engine in boundary.engines)
    assert getattr(app.ctx, "registry_ptg_scope_engine", None) is None
    assert getattr(app.ctx, "registry_ptg_scope_runtime", None) is None
    assert sum(entry[1] == "session" for entry in boundary.events) == sum(
        entry[1] == "session_closed" for entry in boundary.events
    )


@pytest.mark.asyncio
async def test_failed_install_closes_owned_runtime_without_replacing_service(monkeypatch):
    app, _listeners = _app(_configured())
    boundary = _boundaries(monkeypatch)
    monkeypatch.setattr(
        routes, "install_registry_ptg_scope_engine", Mock(side_effect=ValueError("synthetic install refusal"))
    )
    with pytest.raises(ValueError, match="install refusal"):
        await runtime._start_runtime(app)
    assert all(engine.dispose.await_count == 1 for engine in boundary.engines)
    assert getattr(app.ctx, "registry_ptg_scope_engine", None) is None


@pytest.mark.asyncio
async def test_shutdown_attempts_both_disposals_even_if_one_fails(monkeypatch):
    boundary = _boundaries(monkeypatch)
    built = await runtime.build_registry_ptg_scope_runtime(_configured())
    boundary.engines[1].dispose.side_effect = RuntimeError("synthetic disposal refusal")
    with pytest.raises(RuntimeError, match="disposal refusal"):
        await built.close()
    await built.close()
    assert all(engine.dispose.await_count == 1 for engine in boundary.engines)


@pytest.mark.asyncio
async def test_cancelled_startup_closes_created_engines_without_install(monkeypatch):
    app, _listeners = _app(_configured())
    boundary = _boundaries(monkeypatch)
    monkeypatch.setattr(runtime, "_protected_store", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await runtime._start_runtime(app)
    assert all(engine.dispose.await_count == 1 for engine in boundary.engines)
    assert getattr(app.ctx, "registry_ptg_scope_engine", None) is None
    assert ("reader", "session_closed") in boundary.events


@pytest.mark.asyncio
async def test_server_configuration_is_copied_before_first_proof_await(monkeypatch):
    configured = _configured()
    boundary = _boundaries(monkeypatch)

    async def mutate(_session, _store, *, write):
        configured["ptg_schema"] = "changed_schema"
        configured["authority_token"] = "changed-token"

    monkeypatch.setattr(runtime, "_protected_store", AsyncMock(side_effect=mutate))
    built = await runtime.build_registry_ptg_scope_runtime(configured)
    assert built.service.schema_name == "synthetic_ptg" and built.service.authority.token == "synthetic-service"
    await built.close()
    assert all(engine.dispose.await_count == 1 for engine in boundary.engines)


@pytest.mark.asyncio
async def test_complete_duplicate_json_field_refuses_valid_remaining_document(monkeypatch):
    boundary = _boundaries(monkeypatch)
    encoded = json.dumps(_configured())[:-1] + ',"ptg_schema":"other_schema"}'
    with pytest.raises(ValueError, match="configuration_invalid"):
        await runtime.build_registry_ptg_scope_runtime(encoded)
    assert boundary.engines == []


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", [" " * 16385, '"' + "é" * 8192 + '"'])
async def test_metadata_bound_precedes_owned_json_parser(monkeypatch, encoded):
    boundary = _boundaries(monkeypatch)
    monkeypatch.setattr(runtime.json, "loads", Mock(side_effect=AssertionError("oversized JSON allocated")))
    with pytest.raises(ValueError, match="configuration_invalid"):
        await runtime.build_registry_ptg_scope_runtime(encoded)
    assert boundary.engines == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "configured", ["", "null", "[]", "{", '{"ptg_schema":"a","ptg_schema":"b"}', " " * 16385, {"unknown": "value"}]
)
async def test_malformed_configured_input_refuses_before_engine_creation(monkeypatch, configured):
    boundary = _boundaries(monkeypatch)
    with pytest.raises(ValueError, match="configuration_invalid"):
        await runtime.build_registry_ptg_scope_runtime(configured)
    assert boundary.engines == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field_name, replacement",
    [
        ("ptg_schema", "unsafe.schema"),
        ("control_schema", ""),
        ("owner_role", "scope_approver"),
        ("approval_role", "different_role"),
        ("reader_dsn", "not-a-dsn"),
        ("reader_dsn", "postgresql://scope_approver@database.example/synthetic?ssl=require"),
        ("reader_dsn", "postgresql://scope_owner@database.example/synthetic?ssl=require"),
        ("approver_dsn", "postgresql://scope_approver@different.example/synthetic?ssl=require"),
        ("reader_dsn", "sqlite:///synthetic"),
        ("reader_dsn", "postgresql:///synthetic"),
        ("reader_dsn", "postgresql://scope_reader@database.example:0/synthetic"),
        ("reader_dsn", "postgresql://scope_reader@database.example/synthetic?options=unsafe"),
        ("authority_token", "synthetic token"),
        ("authority_token", None),
        ("app_origin", "https://authority.example/path"),
        ("app_origin", "https://user@authority.example"),
        ("app_origin", "https://authority.example:0"),
        ("reader_dsn", "postgresql://scope_reader@database.example:65536/synthetic"),
    ],
)
async def test_unsafe_configuration_refuses_before_engine_creation(monkeypatch, field_name, replacement):
    configured = _configured()
    configured[field_name] = replacement
    boundary = _boundaries(monkeypatch)
    with pytest.raises(ValueError, match="configuration_invalid"):
        await runtime.build_registry_ptg_scope_runtime(json.dumps(configured))
    assert boundary.engines == []


@pytest.mark.asyncio
async def test_duplicate_extra_and_dense_documents_are_closed(monkeypatch):
    boundary = _boundaries(monkeypatch)
    for configured in [_configured() | {"dsn": "forbidden"}, _configured() | {"reader_dsn": ["forbidden"]}]:
        with pytest.raises(ValueError, match="configuration_invalid"):
            await runtime.build_registry_ptg_scope_runtime(configured)
    assert boundary.engines == []


@pytest.mark.asyncio
async def test_no_environment_database_fallback_or_existing_service_replacement(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_HOST", "ambient.invalid")
    monkeypatch.setenv("HLTHPRT_DB_USER", "ambient_role")
    boundary = _boundaries(monkeypatch)
    app, _listeners = _app(_configured())
    app.ctx.registry_ptg_scope_engine = object()
    with pytest.raises(ValueError, match="service_unconfigured"):
        await runtime._start_runtime(app)
    assert boundary.engines == []
    app.ctx.registry_ptg_scope_engine = None
    await runtime._start_runtime(app)
    assert [engine.url.host for engine in boundary.engines] == ["database.example", "database.example"]
    await runtime._stop_runtime(app)
    runtime.register_registry_ptg_scope_runtime(app)
    with pytest.raises(ValueError, match="already_registered"):
        runtime.register_registry_ptg_scope_runtime(app)
