# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Hold existing native publication fences before an accepted-source snapshot."""

from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.network_cms_registry_address_capture import native_driver
from process.registry_company_approval_fence import (
    _company_approval_connection,
    _release_connection_fence,
    require_registry_company_approval_fence,
)
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable
from process.registry_refresh_anchor_service import _current_pin, _read_engine

_ALIAS_KEY_SQL = "SELECT pg_catalog.hashtext('address_numeric_grid_alias_v1')::bigint"
_MRF_HEAD_TABLES = (
    "plan",
    "plan_attributes",
    "plan_benefits",
    "plan_prices",
    "plan_search_summary",
    "mrf_publication_receipt",
    "reference_family_result_generation",
)


@dataclass(frozen=True)
class RegistryMembershipRefreshHeadFences:
    """Fixed native metadata factories; ordinary source readers receive no write privileges."""

    aca_sessions: object = field(default=None, repr=False)
    generic_sessions: object = field(default=None, repr=False)

    def __post_init__(self):
        engines = [
            _read_engine(sessions) for sessions in (self.aca_sessions, self.generic_sessions) if sessions is not None
        ]
        # These locks overlap; sharing a single-connection pool would block the second checkout.
        if len({id(engine.pool) for engine in engines}) != len(engines):
            _unavailable()


def _unavailable():
    raise RegistryRefreshAnchorUnavailable("registry_refresh_source_fence_unavailable")


def _independent_reader(outer, child):
    """Keep a fresh snapshot on a separate pool while the caller holds its transaction."""
    if outer.get_bind().engine.pool is _read_engine(child).pool:
        _unavailable()


@asynccontextmanager
async def _alias_snapshot_connection(sessions):
    """Retire failed connections so an uncertain session lock never reenters the pool."""
    async with _company_approval_connection(sessions) as connection:
        await connection.execution_options(isolation_level="AUTOCOMMIT")
        key = await connection.scalar(text(_ALIAS_KEY_SQL))
        acquired = await connection.scalar(text("SELECT pg_catalog.pg_try_advisory_lock_shared(:key)"), {"key": key})
        await connection.commit()
        if acquired is not True:
            _unavailable()
        yield connection
        await _release_connection_fence(connection, key)


@asynccontextmanager
async def _aca_head_fence(readers, required):
    if not required:
        yield
        return
    if readers.aca_store is None or readers.aca_head_fence_sessions is None:
        _unavailable()
    async with _company_approval_connection(readers.aca_head_fence_sessions) as connection:
        async with readers.aca_head_fence_sessions(bind=connection) as session, session.begin():
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            relations = [f"{_identifier(readers.aca_source_schema)}.{_identifier(name)}" for name in _MRF_HEAD_TABLES]
            relations.append(f"{_identifier(readers.aca_store.control_schema)}.registry_aca_source_notification")
            for relation in relations:
                await session.execute(text(f"LOCK TABLE ONLY {relation} IN SHARE MODE NOWAIT"))
            yield


@asynccontextmanager
async def pinned_refresh_source_snapshot(session, approved, readers, *, aca_required, generic_bindings=()):
    """Company → native alias/family heads → fresh RO snapshot → caller publication locks."""
    await require_registry_company_approval_fence(session, readers.control_schema)
    if type(generic_bindings) is not tuple:
        _unavailable()
    _independent_reader(session, readers.source_reader_sessions)
    if aca_required:
        _independent_reader(session, readers.aca_head_fence_sessions)
    endpoints = await _generic_endpoint_discovery(session, approved, readers, generic_bindings)
    from process.registry_membership_refresh_generic_fence import generic_refresh_head_fence

    async with (
        _alias_snapshot_connection(readers.source_reader_sessions) as connection,
        _aca_head_fence(readers, aca_required),
        generic_refresh_head_fence(readers, endpoints),
    ):
        await connection.execution_options(isolation_level="REPEATABLE READ")
        async with readers.source_reader_sessions(bind=connection) as child, child.begin():
            await child.execute(text("SET TRANSACTION READ ONLY"))
            await child.execute(text("SET LOCAL statement_timeout='5s'"))
            await child.execute(text("SET LOCAL lock_timeout='1s'"))
            driver = await native_driver(child)
            child_pin = await _current_pin(driver, approved.approved_revision, readers.control_schema)
            if child_pin != approved:
                _unavailable()
            yield child


async def _generic_endpoint_discovery(session, approved, readers, bindings):
    """Finish immutable original-capture discovery before acquiring mutable head locks."""
    if not bindings:
        return ()
    if readers.head_fences.generic_sessions is None:
        _unavailable()
    _independent_reader(session, readers.head_fences.generic_sessions)
    from process.registry_membership_refresh_generic import generic_refresh_endpoints

    async with readers.source_reader_sessions() as child, child.begin():
        await child.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await child.execute(text("SET LOCAL statement_timeout='5s'"))
        await child.execute(text("SET LOCAL lock_timeout='1s'"))
        driver = await native_driver(child)
        if await _current_pin(driver, approved.approved_revision, readers.control_schema) != approved:
            _unavailable()
        endpoints = await generic_refresh_endpoints(
            driver, bindings, RegistryPTGGraphReadBudget(*readers.read_budget_limits)
        )
        if type(endpoints) is not tuple or not 0 < len(endpoints) <= 1000:
            _unavailable()
        return endpoints
