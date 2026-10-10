# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real owned factories with mocked native probes; no connections or credentials."""

import asyncio
import json
import traceback
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from process import registry_ptg_office_capture as capture
from process import registry_ptg_scope_runtime as runtime
from process.registry_ptg_office_preparation import RegistryPTGOfficePreparationService
from tests.test_registry_ptg_scope_runtime import _app, _configured, _Sessions


def _office_configuration():
    return {
        **_configured(),
        "office_custody": {"owner_role": "office_owner", "publisher_role": "office_publisher"},
        "publisher_dsn": "postgresql://office_publisher@database.example/synthetic?ssl=require",
    }


@pytest.fixture
async def owned(monkeypatch):
    engines, disposed = [], []
    original_dispose = AsyncEngine.dispose
    original_probe = runtime._check_office_publisher
    state = SimpleNamespace(engines=engines, disposed=disposed, fail_role=None, native_publisher=original_probe)

    def create(url, **options):
        engine = create_async_engine(url, **options)
        engines.append(engine)
        return engine

    async def dispose(engine, *args, **kwargs):
        disposed.append(engine)
        await original_dispose(engine, *args, **kwargs)
        if engine.url.username == state.fail_role:
            raise RuntimeError("synthetic disposal failure")

    monkeypatch.setattr(runtime, "create_async_engine", create)
    monkeypatch.setattr(AsyncEngine, "dispose", dispose)
    store_probe = AsyncMock()
    publisher_probe = AsyncMock()
    monkeypatch.setattr(runtime, "_check_store", store_probe)
    monkeypatch.setattr(runtime, "_check_office_publisher", publisher_probe)
    state.store, state.publisher = store_probe, publisher_probe
    try:
        yield state
    finally:
        for engine in engines:
            if engine not in disposed:
                await original_dispose(engine)


@pytest.mark.asyncio
async def test_optional_publisher_installs_linked_owned_service(owned):
    app, _ = _app(json.dumps(_office_configuration()))
    await runtime._start_runtime(app)
    preparation = app.ctx.registry_ptg_office_preparation
    assert type(preparation) is RegistryPTGOfficePreparationService
    assert preparation.scope_service is app.ctx.registry_ptg_scope_engine
    assert preparation.scope_service.source_reviews_authority.__self__ is preparation.scope_service.authority
    assert preparation.publisher_sessions.kw["bind"] is owned.engines[2]
    assert preparation.scope_service.reader_sessions.kw["bind"] is owned.engines[0]
    assert len(owned.engines) == 3 and not owned.disposed
    assert [engine.url.username for engine in owned.engines] == ["scope_reader", "scope_approver", "office_publisher"]
    for engine in owned.engines:
        assert engine.pool.size() == 1 and engine.pool._max_overflow == 0 and not engine.pool.checkedout()
        assert engine.echo is False and engine.sync_engine.hide_parameters is True
    assert owned.store.await_count == 2
    owned.publisher.assert_awaited_once_with(preparation.scope_service, preparation.publisher_sessions)
    retained = app.ctx.registry_ptg_scope_runtime
    await runtime._stop_runtime(app)
    await runtime._stop_runtime(app)
    await retained.close()
    assert owned.disposed == list(reversed(owned.engines))
    assert app.ctx.registry_ptg_scope_engine is None and app.ctx.registry_ptg_office_preparation is None


@pytest.mark.asyncio
async def test_old_configuration_has_only_two_engines(owned):
    built = await runtime.build_registry_ptg_scope_runtime(_configured())
    assert built.office_preparation is None and len(owned.engines) == 2
    owned.publisher.assert_not_awaited()
    await built.close()
    assert owned.disposed == list(reversed(owned.engines))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "publisher",
    [
        None,
        True,
        "",
        "x" * 4097,
        "not-a-dsn",
        "postgresql://scope_reader@database.example/synthetic?ssl=require",
        "postgresql://scope_approver@database.example/synthetic?ssl=require",
        "postgresql://office_publisher@other.example/synthetic?ssl=require",
        "postgresql://office_publisher@database.example/other?ssl=require",
        "postgresql://office_publisher@database.example/synthetic?ssl=verify-full",
        "postgresql://office_publisher@database.example/synthetic?options=unsafe",
    ],
)
async def test_publisher_configuration_fails_before_engines(owned, publisher):
    configured = _office_configuration()
    configured["publisher_dsn"] = publisher
    with pytest.raises(ValueError, match="configuration_invalid"):
        await runtime.build_registry_ptg_scope_runtime(configured)
    assert not owned.engines


@pytest.mark.asyncio
async def test_publisher_requires_explicit_office_profile(owned):
    configured = _office_configuration()
    del configured["office_custody"]
    with pytest.raises(ValueError, match="configuration_invalid"):
        await runtime.build_registry_ptg_scope_runtime(configured)
    assert not owned.engines


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["engine", "sessionmaker", "proof"])
async def test_publisher_startup_denial_is_sanitized_and_disposed(monkeypatch, owned, phase):
    diagnostic = "synthetic-sensitive-detail"
    if phase == "engine":
        original = runtime.create_async_engine

        def create(url, **options):
            if url.username == "office_publisher":
                raise OSError(diagnostic)
            return original(url, **options)

        monkeypatch.setattr(runtime, "create_async_engine", create)
    elif phase == "sessionmaker":
        original = runtime.async_sessionmaker

        def factory(engine, **options):
            if engine.url.username == "office_publisher":
                raise RuntimeError(diagnostic)
            return original(engine, **options)

        monkeypatch.setattr(runtime, "async_sessionmaker", factory)
    else:
        owned.publisher.side_effect = PermissionError(diagnostic)
    app, _ = _app(_office_configuration())
    with pytest.raises(ValueError, match="registry_ptg_office_service_unconfigured") as observed:
        await runtime._start_runtime(app)
    assert diagnostic not in "".join(traceback.format_exception(observed.value))
    assert observed.value.__suppress_context__ is True
    assert owned.disposed == list(reversed(owned.engines))
    assert getattr(app.ctx, "registry_ptg_scope_engine", None) is None
    assert getattr(app.ctx, "registry_ptg_office_preparation", None) is None


@pytest.mark.asyncio
async def test_startup_cleanup_failure_stays_sanitized(owned):
    owned.publisher.side_effect = PermissionError("synthetic proof failure")
    owned.fail_role = "office_publisher"
    app, _ = _app(_office_configuration())
    with pytest.raises(ValueError, match="registry_ptg_office_service_unconfigured") as observed:
        await runtime._start_runtime(app)
    trace = "".join(traceback.format_exception(observed.value))
    assert "synthetic proof failure" not in trace and "synthetic disposal failure" not in trace
    assert owned.disposed == list(reversed(owned.engines))
    assert getattr(app.ctx, "registry_ptg_scope_engine", None) is None
    assert getattr(app.ctx, "registry_ptg_office_preparation", None) is None


@pytest.mark.asyncio
async def test_publisher_cancellation_disposes_all_factories(owned):
    owned.publisher.side_effect = asyncio.CancelledError
    app, _ = _app(_office_configuration())
    with pytest.raises(asyncio.CancelledError):
        await runtime._start_runtime(app)
    assert len(owned.engines) == 3 and owned.disposed == list(reversed(owned.engines))
    assert getattr(app.ctx, "registry_ptg_scope_engine", None) is None


@pytest.mark.asyncio
async def test_shutdown_attempts_every_disposal_after_failure(owned):
    built = await runtime.build_registry_ptg_scope_runtime(_office_configuration())
    owned.fail_role = "office_publisher"
    with pytest.raises(RuntimeError, match="disposal failure"):
        await built.close()
    await built.close()
    assert owned.disposed == list(reversed(owned.engines))


@pytest.mark.asyncio
async def test_shutdown_preserves_a_replaced_service_owner(owned):
    app, _ = _app(_office_configuration())
    await runtime._start_runtime(app)
    other_scope, other_office = object(), object()
    app.ctx.registry_ptg_scope_engine = other_scope
    app.ctx.registry_ptg_office_preparation = other_office
    await runtime._stop_runtime(app)
    assert app.ctx.registry_ptg_scope_engine is other_scope
    assert app.ctx.registry_ptg_office_preparation is other_office
    assert owned.disposed == list(reversed(owned.engines))


@pytest.mark.asyncio
async def test_existing_office_owner_prevents_startup(owned):
    app, _ = _app(_office_configuration())
    existing = object()
    app.ctx.registry_ptg_office_preparation = existing
    with pytest.raises(ValueError, match="service_unconfigured"):
        await runtime._start_runtime(app)
    assert not owned.engines and app.ctx.registry_ptg_office_preparation is existing


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "source-read", "source-lock"])
async def test_startup_consumes_source_privilege_and_custody_proofs(monkeypatch, owned, failure):
    configured = _office_configuration()
    del configured["publisher_dsn"]
    built = await runtime.build_registry_ptg_scope_runtime(configured)
    sessions = _Sessions("publisher", [])
    source_probe = AsyncMock(side_effect=PermissionError("source read closed") if failure == "source-read" else None)
    profile_by_field = {
        name: True
        for name in (
            "genuine",
            "flags",
            "set_closed",
            "readers_closed",
            "office_only",
            "publisher_writes_closed",
        )
    }
    profile_by_field.update(schema_count=0, schemas=[])
    if failure == "source-lock":
        profile_by_field["publisher_writes_closed"] = False
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=profile_by_field))
    monkeypatch.setattr(runtime, "_protected_store", source_probe)
    monkeypatch.setattr(capture, "_driver", AsyncMock(return_value=driver))
    try:
        if failure:
            with pytest.raises((PermissionError, ValueError)):
                await owned.native_publisher(built.service, sessions)
        else:
            await owned.native_publisher(built.service, sessions)
        source_probe.assert_awaited_once_with(
            sessions.session,
            built.service.store,
            write=False,
            table_name="registry_ptg_published_plan_scope",
        )
        statements = [str(call.args[0]) for call in sessions.session.execute.await_args_list]
        assert "REPEATABLE READ, READ WRITE" in statements[0] and "pg_catalog,pg_temp" in statements[1]
        assert not any("GRANT" in statement or "SET ROLE" in statement for statement in statements)
        if failure == "source-read":
            driver.fetchrow.assert_not_awaited()
        else:
            query, *parameters = driver.fetchrow.await_args.args
            assert "source_locks" in query and "(SELECT count(*) FROM source_locks)=6" in query
            assert parameters == [
                "office_owner",
                "office_publisher",
                ["scope_approver", "scope_reader"],
                True,
                "scope_owner",
                "synthetic_ptg",
            ]
    finally:
        await built.close()
