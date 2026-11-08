# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Hold existing native publication fences before an accepted-source snapshot."""

from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.network_membership_writer_closure import _protected_owner
from process.reference_family_archive import protected_publisher_owner
from process.registry_company_approval_fence import (
    _company_approval_connection,
    _release_connection_fence,
    require_registry_company_approval_fence,
)
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable
from process.registry_refresh_anchor_service import _current_pin, _database_target, _native_context, _read_engine

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
        if len({id(engine.pool) for engine in engines}) != len(engines):
            _unavailable()


def _unavailable():
    raise RegistryRefreshAnchorUnavailable("registry_refresh_source_fence_unavailable")


def _same_database(outer, child):
    outer_engine = outer.get_bind().engine
    child_engine = _read_engine(child)
    if (
        outer_engine is child_engine.sync_engine
        or outer_engine.pool is child_engine.pool
        or _database_target(outer_engine) != _database_target(child_engine)
    ):
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


async def _aca_lock_authority(session, readers, identity):
    """Use an existing native publisher, never expand an ordinary inbox role's rights."""
    await protected_publisher_owner(session)
    connection, observed_identity = await _native_context(session)
    if observed_identity != identity:
        _unavailable()
    owner = await connection.fetchrow(
        """SELECT o.*,n.oid AS schema_oid,c.oid AS table_oid,
        c.relowner=n.nspowner AND c.relkind='r' AND c.relpersistence='p' AS matching_owner,
        pg_has_role(current_user,n.nspowner,'USAGE') AS effective_owner
        FROM pg_namespace n JOIN pg_roles o ON o.oid=n.nspowner
        JOIN pg_class c ON c.relnamespace=n.oid AND c.relname='registry_aca_source_notification'
        WHERE n.nspname=$1""",
        readers.aca_store.control_schema,
    )
    if not _protected_owner(owner) or owner["matching_owner"] is not True or owner["effective_owner"] is not True:
        _unavailable()


@asynccontextmanager
async def _aca_head_fence(readers, required, identity):
    if not required:
        yield
        return
    if readers.aca_store is None or readers.aca_head_fence_sessions is None:
        _unavailable()
    async with _company_approval_connection(readers.aca_head_fence_sessions) as connection:
        async with readers.aca_head_fence_sessions(bind=connection) as session, session.begin():
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            await _aca_lock_authority(session, readers, identity)
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
    _same_database(session, readers.source_reader_sessions)
    if aca_required:
        _same_database(session, readers.aca_head_fence_sessions)
    _, identity = await _native_context(session)
    endpoints = await _generic_endpoint_discovery(session, approved, readers, generic_bindings, identity)
    from process.registry_membership_refresh_generic_fence import generic_refresh_head_fence

    async with (
        _alias_snapshot_connection(readers.source_reader_sessions) as connection,
        _aca_head_fence(readers, aca_required, identity),
        generic_refresh_head_fence(readers, endpoints, identity),
    ):
        await connection.execution_options(isolation_level="REPEATABLE READ")
        async with readers.source_reader_sessions(bind=connection) as child, child.begin():
            await child.execute(text("SET TRANSACTION READ ONLY"))
            driver, observed_identity = await _native_context(child)
            if identity != observed_identity:
                _unavailable()
            child_pin = await _current_pin(driver, approved.approved_revision, readers.control_schema)
            if child_pin != approved:
                _unavailable()
            yield child


async def _generic_endpoint_discovery(session, approved, readers, bindings, identity):
    """Finish immutable original-capture discovery before acquiring mutable head locks."""
    if not bindings:
        return ()
    if readers.head_fences.generic_sessions is None or (
        _read_engine(readers.head_fences.generic_sessions).url.username == session.get_bind().engine.url.username
    ):
        _unavailable()
    _same_database(session, readers.head_fences.generic_sessions)
    from process.registry_membership_refresh_generic import generic_refresh_endpoints

    async with readers.source_reader_sessions() as child, child.begin():
        await child.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        driver, observed_identity = await _native_context(child)
        if (
            observed_identity != identity
            or await _current_pin(driver, approved.approved_revision, readers.control_schema) != approved
        ):
            _unavailable()
        endpoints = await generic_refresh_endpoints(
            driver, bindings, RegistryPTGGraphReadBudget(*readers.read_budget_limits)
        )
        if type(endpoints) is not tuple or not 0 < len(endpoints) <= 1000:
            _unavailable()
        return endpoints
