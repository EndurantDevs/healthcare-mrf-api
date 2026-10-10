# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit server configuration and owned lifecycle for source review sessions."""

from __future__ import annotations

import json
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from urllib.parse import urlsplit

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
    except ValueError, TypeError, UnicodeError, SQLAlchemyError:
        raise _invalid() from None
    return configuration_by_field, reader, approver, authority


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


@dataclass
class RegistryPTGScopeRuntime:
    """A service and its exact owned engine cleanup, closed once on shutdown."""

    service: RegistryPTGScopeEngineService
    _cleanup: AsyncExitStack = field(repr=False)
    _is_closed: bool = field(default=False, repr=False)

    async def close(self):
        """Dispose both owned engines once, including when one disposal raises."""
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


async def build_registry_ptg_scope_runtime(configured):
    """Validate the one server input and native role closure before exposure.

    The configured authority_token is shared with the app authority endpoint.
    No environment, request database, grants or source admission is inferred here.
    """
    if configured is None:
        return None
    configured, reader, approver, authority = _configuration(configured)
    store = RegistryPTGProducerScopeStore(
        configured["owner_role"],
        configured["approval_role"],
        configured["control_schema"],
    )
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
        )
        return RegistryPTGScopeRuntime(service, cleanup.pop_all())


def register_registry_ptg_scope_runtime(app):
    """Register worker-local startup and disposal; absent config leaves routes 503."""
    if getattr(app.ctx, "registry_ptg_scope_runtime_registered", False):
        raise ValueError("registry_ptg_scope_runtime_already_registered")
    app.ctx.registry_ptg_scope_runtime_registered = True
    app.listener("before_server_start")(_start_runtime)
    app.listener("after_server_stop")(_stop_runtime)


async def _start_runtime(app, _loop=None):
    from api.control_registry_ptg_scope import install_registry_ptg_scope_engine

    if getattr(app.ctx, "registry_ptg_scope_engine", None) is not None:
        raise ValueError("registry_ptg_scope_service_unconfigured")
    runtime = await build_registry_ptg_scope_runtime(app.config.get(CONFIG_KEY))
    if runtime is not None:
        try:
            install_registry_ptg_scope_engine(app, runtime.service)
        except BaseException:
            await runtime.close()
            raise
        app.ctx.registry_ptg_scope_runtime = runtime


async def _stop_runtime(app, _loop=None):
    runtime = getattr(app.ctx, "registry_ptg_scope_runtime", None)
    if runtime is not None:
        app.ctx.registry_ptg_scope_engine = None
        app.ctx.registry_ptg_scope_runtime = None
        await runtime.close()
