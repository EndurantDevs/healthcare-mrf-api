# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit server configuration and owned lifecycle for source review sessions."""

from __future__ import annotations

import json
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from types import SimpleNamespace
from urllib.parse import urlsplit

import asyncpg
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_address_projection import _identifier
from process.registry_ptg_producer_scope import (
    RegistryPTGProducerScopeStore,
    _protected_store,
)
from process.registry_ptg_scope_engine import (
    RegistryPTGScopeAuthorityClient,
    RegistryPTGScopeEngineService,
)

CONFIG_KEY = "REGISTRY_PTG_SCOPE_RUNTIME"
_FIELDS = frozenset(
    {
        "ptg_schema",
        "control_schema",
        "owner_role",
        "approval_role",
        "reader_dsn",
        "approver_dsn",
        "app_origin",
        "authority_token",
    }
)


def _invalid():
    return ValueError("registry_ptg_scope_runtime_configuration_invalid")


def _unique_fields(pairs):
    document_by_field = {}
    for name, configured in pairs:
        if name in document_by_field:
            raise _invalid()
        document_by_field[name] = configured
    return document_by_field


def _office_custody(configured):
    from process.registry_ptg_office_custody import RegistryPTGOfficeCustodyProfile

    document_by_field = configured.get("office_custody")
    if document_by_field is None:
        return None
    if type(document_by_field) is not dict or set(document_by_field) != {"owner_role", "publisher_role"}:
        raise _invalid()
    profile = RegistryPTGOfficeCustodyProfile(**document_by_field)
    if {profile.owner_role, profile.publisher_role} & {configured["owner_role"], configured["approval_role"]}:
        raise _invalid()
    return profile


def _configuration(configured):
    if type(configured) is str:
        try:
            if len(configured) > 16384 or len(configured.encode("utf-8")) > 16384:
                raise _invalid()
            configured = json.loads(configured, object_pairs_hook=_unique_fields)
        except ValueError, TypeError, UnicodeError, RecursionError:
            raise _invalid() from None
    if type(configured) is not dict or set(configured) not in (
        _FIELDS,
        _FIELDS | {"office_custody"},
        _FIELDS | {"office_custody", "publisher_dsn"},
        _FIELDS | {"office_custody", "publisher_dsn", "office_intents"},
    ):
        raise _invalid()
    if any(type(configured[name]) is not str or not 1 <= len(configured[name]) <= 4096 for name in _FIELDS):
        raise _invalid()
    configuration_by_field = dict(configured)
    office_custody = _office_custody(configured)
    if office_custody is not None:
        configuration_by_field["office_custody"] = office_custody
    try:
        for name in ("ptg_schema", "control_schema", "owner_role", "approval_role"):
            _identifier(configuration_by_field[name])
        if configuration_by_field["owner_role"] == configuration_by_field["approval_role"]:
            raise _invalid()
        authority = RegistryPTGScopeAuthorityClient(
            configuration_by_field["app_origin"],
            configuration_by_field["authority_token"],
        )
        origin_port = urlsplit(authority.base_url).port
        if (
            any(character.isspace() for character in configuration_by_field["authority_token"])
            or origin_port is not None
            and not 1 <= origin_port <= 65535
        ):
            raise _invalid()
        reader = _database_url(configuration_by_field["reader_dsn"])
        approver = _database_url(configuration_by_field["approver_dsn"])
        if (
            reader.username
            in {
                configuration_by_field["owner_role"],
                configuration_by_field["approval_role"],
            }
            or approver.username != configuration_by_field["approval_role"]
            or (reader.host, reader.port, reader.database, reader.query)
            != (approver.host, approver.port, approver.database, approver.query)
        ):
            raise _invalid()
        publisher = _publisher_url(configuration_by_field, reader)
        if "office_intents" in configuration_by_field:
            configuration_by_field["office_intent_urls"] = _intent_urls(configuration_by_field, reader, publisher)
    except ValueError, TypeError, UnicodeError, SQLAlchemyError:
        raise _invalid() from None
    return configuration_by_field, reader, approver, authority, publisher


def _database_url(dsn):
    if any(character.isspace() or not character.isprintable() for character in dsn):
        raise _invalid()
    url = make_url(dsn)
    if (
        url.drivername not in {"postgresql", "postgresql+asyncpg"}
        or not url.host
        or not url.username
        or not url.database
        or url.port is not None
        and not 1 <= url.port <= 65535
        or url.query
        and (set(url.query) != {"ssl"} or url.query["ssl"] not in {"require", "verify-full"})
    ):
        raise _invalid()
    _identifier(url.username)
    return url.set(drivername="postgresql+asyncpg")


def _publisher_url(configured, reader):
    if "publisher_dsn" not in configured:
        return None
    dsn = configured["publisher_dsn"]
    profile = configured.get("office_custody")
    if type(dsn) is not str or not 1 <= len(dsn) <= 4096 or profile is None:
        raise _invalid()
    publisher = _database_url(dsn)
    if (
        publisher.username != profile.publisher_role
        or publisher.username in {reader.username, configured["owner_role"], configured["approval_role"]}
        or (publisher.host, publisher.port, publisher.database, publisher.query)
        != (reader.host, reader.port, reader.database, reader.query)
    ):
        raise _invalid()
    return publisher


def _intent_urls(configured, reader, publisher):
    if "office_intents" not in configured:
        return None
    document = configured["office_intents"]
    if publisher is None or type(document) is not dict or set(document) != {"management_dsn", "queue_dsn"}:
        raise _invalid()
    if any(type(value) is not str or not 1 <= len(value) <= 4096 for value in document.values()):
        raise _invalid()
    management = _database_url(document["management_dsn"])
    queue = _database_url(document["queue_dsn"])
    forbidden_roles = {
        reader.username,
        configured["owner_role"],
        configured["approval_role"],
        configured["office_custody"].owner_role,
        publisher.username,
    }
    if management.username == queue.username or any(
        url.username in forbidden_roles
        or (url.host, url.port, url.database, url.query) != (reader.host, reader.port, reader.database, reader.query)
        for url in (management, queue)
    ):
        raise _invalid()
    return management, queue


@dataclass
class RegistryPTGScopeRuntime:
    """A service and its exact owned engine cleanup, closed once on shutdown."""

    service: RegistryPTGScopeEngineService
    _cleanup: AsyncExitStack = field(repr=False)
    office_preparation: object = field(default=None, repr=False)
    office_coordinator: object = field(default=None, repr=False)
    _is_closed: bool = field(default=False, repr=False)

    async def close(self):
        """Dispose every owned engine once, including when one disposal raises."""
        if not self._is_closed:
            self._is_closed = True
            await self._cleanup.aclose()


async def _check_store(sessions, store, *, write):
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE"))
        await _protected_store(session, store, write=write)


def _owned_sessions(url, cleanup):
    engine = create_async_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        echo=False,
        hide_parameters=True,
        isolation_level="REPEATABLE READ",
        execution_options={"postgresql_readonly": False},
    )
    cleanup.push_async_callback(engine.dispose)
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


async def _check_office_publisher(service, sessions):
    """Verify native source-lock and closed-family custody; never grant privileges."""
    from process.registry_ptg_office_approval import _reader_roles
    from process.registry_ptg_office_capture import _driver
    from process.registry_ptg_office_custody import verify_registry_ptg_office_custody

    profile = service.office_custody
    context = SimpleNamespace(
        office_custody=profile,
        owner_role=profile.owner_role,
        reader_roles=_reader_roles(service),
        scope_store=service.store,
        source_specification=SimpleNamespace(ptg_schema_name=service.schema_name),
    )
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE"))
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        await _protected_store(session, service.store, write=False, table_name="registry_ptg_published_plan_scope")
        await verify_registry_ptg_office_custody(await _driver(session), context, publisher=True)


async def _office_preparation(service, publisher, cleanup):
    if publisher is None:
        return None
    from process.registry_ptg_office_preparation import RegistryPTGOfficePreparationService

    try:
        sessions = _owned_sessions(publisher, cleanup)
        preparation = RegistryPTGOfficePreparationService(service, sessions)
        await _check_office_publisher(service, sessions)
        return preparation
    except Exception:
        raise ValueError("registry_ptg_office_service_unconfigured") from None


async def _check_office_coordinator(coordinator):
    from process.network_cms_registry_address_capture import native_driver
    from process.registry_management_permissions import verify_registry_management_permissions
    from process.registry_ptg_office_coordinator import _native_identity, _queue_permissions, _role

    identities = []
    for sessions, role, management in (
        (
            coordinator.preparation.scope_service.reader_sessions,
            coordinator.preparation.scope_service.reader_sessions.kw["bind"].url.username,
            False,
        ),
        (
            coordinator.preparation.publisher_sessions,
            coordinator.preparation.publisher_sessions.kw["bind"].url.username,
            False,
        ),
        (coordinator.management_sessions, coordinator.management_role, True),
    ):
        async with sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"))
            await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
            connection = await native_driver(session)
            await _role(connection, role)
            if management:
                await verify_registry_management_permissions(
                    connection, api_role=role, owner_role=coordinator.owner_role, control_schema=coordinator.schema
                )
            identities.append(await _native_identity(connection))
    if any(identity != identities[0] for identity in identities[1:]):
        raise ValueError("registry_ptg_office_database_changed")
    async with (
        coordinator.queue_pool.acquire() as connection,
        connection.transaction(isolation="read_committed", readonly=True),
    ):
        await _queue_permissions(connection, coordinator, identities[0])


async def _office_coordinator(preparation, urls, cleanup):
    if urls is None:
        return None
    from process.registry_ptg_office_coordinator import RegistryPTGOfficeIntentCoordinator

    management, queue = urls
    sessions = _owned_sessions(management, cleanup)
    # min_size=0 performs no connection IO before exact pool cleanup registration.
    pool = await asyncpg.create_pool(
        queue.set(drivername="postgresql").render_as_string(hide_password=False), min_size=0, max_size=1
    )
    cleanup.push_async_callback(pool.close)
    coordinator = RegistryPTGOfficeIntentCoordinator(
        preparation, sessions, pool, management.username, queue.username, preparation.scope_service.store.owner_role
    )
    await _check_office_coordinator(coordinator)
    return coordinator


async def build_registry_ptg_scope_runtime(configured):
    """Validate the one server input and native role closure before exposure.

    The configured authority_token is shared with the app authority endpoint.
    No environment, request database, grants or source admission is inferred here.
    """
    if configured is None:
        return None
    configured, reader, approver, authority, publisher = _configuration(configured)
    store = RegistryPTGProducerScopeStore(
        configured["owner_role"],
        configured["approval_role"],
        configured["control_schema"],
    )
    try:
        async with AsyncExitStack() as cleanup:
            reader_sessions = _owned_sessions(reader, cleanup)
            approval_sessions = _owned_sessions(approver, cleanup)
            await _check_store(reader_sessions, store, write=False)
            await _check_store(approval_sessions, store, write=True)
            service = RegistryPTGScopeEngineService(
                configured["ptg_schema"],
                reader_sessions,
                approval_sessions,
                store,
                authority,
                configured.get("office_custody"),
                source_reviews_authority=authority.authorize_source_reviews,
            )
            preparation = await _office_preparation(service, publisher, cleanup)
            coordinator = await _office_coordinator(preparation, configured.get("office_intent_urls"), cleanup)
            return RegistryPTGScopeRuntime(
                service, cleanup.pop_all(), office_preparation=preparation, office_coordinator=coordinator
            )
    except Exception:
        if publisher is not None:
            raise ValueError("registry_ptg_office_service_unconfigured") from None
        raise


def register_registry_ptg_scope_runtime(app):
    """Register worker-local startup and disposal; absent config leaves routes 503."""
    if getattr(app.ctx, "registry_ptg_scope_runtime_registered", False):
        raise ValueError("registry_ptg_scope_runtime_already_registered")
    app.ctx.registry_ptg_scope_runtime_registered = True
    app.listener("before_server_start")(_start_runtime)
    app.listener("after_server_stop")(_stop_runtime)


async def _start_runtime(app, _loop=None):
    from api.control_registry_ptg_scope import install_registry_ptg_scope_engine

    if (
        getattr(app.ctx, "registry_ptg_scope_engine", None) is not None
        or getattr(app.ctx, "registry_ptg_office_preparation", None) is not None
        or getattr(app.ctx, "registry_ptg_office_coordinator", None) is not None
    ):
        raise ValueError("registry_ptg_scope_service_unconfigured")
    runtime = await build_registry_ptg_scope_runtime(app.config.get(CONFIG_KEY))
    if runtime is not None:
        try:
            install_registry_ptg_scope_engine(
                app,
                runtime.service,
                office_preparation=runtime.office_preparation,
                office_coordinator=runtime.office_coordinator,
            )
        except BaseException:
            await runtime.close()
            raise
        app.ctx.registry_ptg_scope_runtime = runtime


async def _stop_runtime(app, _loop=None):
    runtime = getattr(app.ctx, "registry_ptg_scope_runtime", None)
    if runtime is not None:
        if getattr(app.ctx, "registry_ptg_scope_engine", None) is runtime.service:
            app.ctx.registry_ptg_scope_engine = None
        if getattr(app.ctx, "registry_ptg_office_preparation", None) is runtime.office_preparation:
            app.ctx.registry_ptg_office_preparation = None
        if getattr(app.ctx, "registry_ptg_office_coordinator", None) is runtime.office_coordinator:
            app.ctx.registry_ptg_office_coordinator = None
        app.ctx.registry_ptg_scope_runtime = None
        await runtime.close()
