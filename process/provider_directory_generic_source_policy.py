# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit publisher custody for source snapshots; no ambient-role elevation."""

import asyncio
import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from db.connection import Database
from db.plan_release_storage import storage_identifier
from process.plan_release_storage_custody import storage_profile, storage_roles
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_projection_stage import _copy_driver

_PUBLISHER = ContextVar("provider_directory_generic_source_publisher", default=None)
_OWNER = ContextVar("provider_directory_generic_source_owner", default=None)
_FACTORY = ContextVar("provider_directory_generic_source_transaction_factory", default=None)


def _require(condition, code):
    if not condition:
        raise GenericCandidateError("provider_directory_generic_source_" + code)


@dataclass(frozen=True)
class GenericSourcePolicy:
    """The existing enrolled namespace/preparation tuple, supplied by orchestration."""

    schema: str
    owner_role: str
    publisher_role: str
    runtime_roles: tuple[str, ...]
    control_runtime_roles: tuple[str, ...] = ()

    def __post_init__(self):
        storage_identifier(self.schema)
        storage_profile(self.owner_role, self.publisher_role, self.runtime_roles, self.control_runtime_roles)

    def role_arguments(self):
        """Preserve the complete existing role-policy verifier's argument names."""
        arguments_by_name = dict(
            owner_role=self.owner_role,
            publisher_role=self.publisher_role,
            reader_roles=self.runtime_roles,
        )
        if self.control_runtime_roles:
            arguments_by_name["control_reader_roles"] = self.control_runtime_roles
        return arguments_by_name


@dataclass(frozen=True)
class _Publisher:
    connection: object
    policy: GenericSourcePolicy
    task: object
    transaction_isolation: str
    factory: object


async def _genuine_publisher(connection, policy):
    _require(connection.is_in_transaction() is True, "transaction_required")
    actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    _require(
        actor is not None and actor["session"] == actor["current"] == policy.publisher_role,
        "genuine_publisher_required",
    )


@asynccontextmanager
async def bind_generic_source_publisher(connection, policy, *, verify_role_profile, transaction_isolation):
    """Bind only a separately supplied genuine physical publisher transaction.

    The caller's original resource/witness/checkpoint transaction must use this
    exact physical connection. Neither a pool, DSN nor an ordinary db session
    can authorize this capability. There is no runtime policy-file fallback.
    """
    _require(type(policy) is GenericSourcePolicy and callable(verify_role_profile), "explicit_policy_required")
    _require(_PUBLISHER.get() is None, "publisher_already_bound")
    _require(transaction_isolation in {"read committed", "repeatable read", "serializable"}, "isolation_required")
    await _genuine_publisher(connection, policy)
    _require(await connection.fetchval("SHOW transaction_isolation") == transaction_isolation, "isolation_changed")
    await storage_roles(connection, policy.schema, **policy.role_arguments())
    await verify_role_profile(connection, policy.schema, **policy.role_arguments())
    publisher = _Publisher(connection, policy, asyncio.current_task(), transaction_isolation, _FACTORY.get())
    token = _PUBLISHER.set(publisher)
    try:
        yield publisher
    finally:
        _PUBLISHER.reset(token)


def bound_publisher():
    """Reject inherited task contexts and missing orchestration before target IO."""
    publisher = _PUBLISHER.get()
    _require(publisher is not None and publisher.task is asyncio.current_task(), "publisher_connection_required")
    return publisher


@asynccontextmanager
async def bind_generic_source_transaction_factory(factory):
    """Propagate only trusted transaction creation, never a physical worker session."""
    _require(callable(getattr(factory, "transaction", None)), "transaction_factory_required")
    _require(_FACTORY.get() is None and _PUBLISHER.get() is None, "transaction_factory_already_bound")
    token = _FACTORY.set(factory)
    try:
        yield
    finally:
        _FACTORY.reset(token)


def source_transaction_available():
    """Allow preflight before IO; actual SQL still requires a genuine local binding."""
    if _PUBLISHER.get() is not None:
        return bound_publisher()
    _require(_FACTORY.get() is not None, "publisher_connection_required")
    return None


def source_producer_identity() -> tuple[str, str]:
    """Return baked (source commit, image digest) without authorizing a SQL session."""
    factory = _FACTORY.get()
    _require(factory is not None, "transaction_factory_required")
    current_policy = getattr(factory, "_require_current_policy", None)
    _require(callable(current_policy), "transaction_factory_required")
    current_policy()
    _require(type(getattr(factory, "policy", None)) is GenericSourcePolicy, "explicit_policy_required")
    identity = getattr(factory, "producer_identity", None)
    _require(
        type(identity) is tuple
        and len(identity) == 2
        and all(type(value) is str for value in identity)
        and re.fullmatch(r"[0-9a-f]{40}", identity[0])
        and re.fullmatch(r"sha256:[0-9a-f]{64}", identity[1]),
        "producer_identity_required",
    )
    if _PUBLISHER.get() is not None:
        publisher = bound_publisher()
        _require(publisher.factory is factory and publisher.policy is factory.policy, "policy_changed")
    return identity


def is_source_storage_required():
    """Choose sealed SOURCE reads for trusted orchestration, without granting IO."""
    if _PUBLISHER.get() is not None:
        bound_publisher()
        return True
    return _FACTORY.get() is not None


def _publication_factory():
    factory = _FACTORY.get()
    _require(factory is not None and _PUBLISHER.get() is None, "original_publication_required")
    _require(
        callable(getattr(factory, "publication_session_factory", None))
        and callable(getattr(factory, "bind_existing_transaction", None)),
        "publication_factory_required",
    )
    return factory


async def source_publication_sessions():
    """Choose the trusted final SOURCE pool before capture/WAL opens its transaction."""
    return await _publication_factory().publication_session_factory()


async def source_cohort_snapshot_custody(connection, policy, *, cohort_id=None):
    """Borrow only the existing trusted Factory for the original cohort producer body."""
    publisher = bound_publisher()
    factory = _FACTORY.get()
    _require(
        publisher.connection is connection and publisher.policy == policy and factory is not None,
        "cohort_original_publisher_required",
    )
    capture = getattr(factory, "cohort_snapshot_custody", None)
    _require(callable(capture), "cohort_factory_required")
    return await capture(connection, policy, cohort_id=cohort_id)


async def source_callable_capability(connection, policy):
    """Authenticate existing privileged callables through the enrolled factory."""
    publisher = bound_publisher()
    factory = _FACTORY.get()
    _require(
        publisher.connection is connection
        and publisher.policy is policy
        and factory is not None
        and publisher.factory is factory
        and getattr(factory, "policy", None) is policy,
        "callable_original_publisher_required",
    )
    capture = getattr(factory, "source_callable_capability", None)
    _require(callable(capture), "callable_factory_required")
    capability = await capture(connection, policy)
    _require(
        bound_publisher() is publisher and _FACTORY.get() is factory and getattr(factory, "policy", None) is policy,
        "callable_publisher_changed",
    )
    _require(
        type(capability) is tuple
        and all(type(oid) is int and 0 < oid < 2**32 for oid in capability)
        and len(set(capability)) == len(capability),
        "callable_capability_changed",
    )
    return capability


@asynccontextmanager
async def original_source_publication(database, session):
    """Authenticate the already-open original final owner, preserving its outcome."""
    factory = _publication_factory()
    async with factory.bind_existing_transaction(database, session, isolation="read committed"):
        _require(_FACTORY.get() is factory, "policy_changed")
        async with source_publication_body(database):
            yield session
    _require(_FACTORY.get() is factory, "policy_changed")


def _require_isolation(publisher, isolation):
    accepted_by_isolation = {
        "read committed": {"read committed", "repeatable read", "serializable"},
        "repeatable read": {"repeatable read", "serializable"},
        "serializable": {"serializable"},
    }
    _require(
        isolation in accepted_by_isolation and publisher.transaction_isolation in accepted_by_isolation[isolation],
        "isolation_changed",
    )


@asynccontextmanager
async def source_transaction(database, *, isolation="read committed"):
    """Borrow one genuine per-task publisher transaction established before its first SQL."""
    _require(isolation in {"read committed", "repeatable read", "serializable"}, "isolation_required")
    if _PUBLISHER.get() is not None:
        publisher = bound_publisher()
        _require_isolation(publisher, isolation)
        async with source_owner_transaction(database):
            yield database._transaction_binding().session
        return
    factory = _FACTORY.get()
    _require(factory is not None, "publisher_connection_required")
    async with factory.transaction(database, isolation=isolation):
        publisher = bound_publisher()
        _require_isolation(publisher, isolation)
        async with source_owner_transaction(database):
            yield database._transaction_binding().session


@asynccontextmanager
async def source_checkpoint_guard(database):
    """Borrow only a trusted session-lock owner and its short original transactions."""
    _require(isinstance(database, Database), "checkpoint_guard_database_required")
    factory = _FACTORY.get()
    _require(factory is not None and _PUBLISHER.get() is None and _OWNER.get() is None, "checkpoint_guard_required")
    guard = getattr(factory, "checkpoint_guard", None)
    _require(callable(guard), "checkpoint_guard_factory_required")
    async with guard(database) as issued:
        _require(
            _FACTORY.get() is factory and _PUBLISHER.get() is None and _OWNER.get() is None, "checkpoint_guard_changed"
        )
        yield issued
        _require(
            _FACTORY.get() is factory and _PUBLISHER.get() is None and _OWNER.get() is None, "checkpoint_guard_changed"
        )


def source_copy_session():
    """Return only the original task's active SQLAlchemy COPY owner for cleanup."""
    publisher = bound_publisher()
    owner = _OWNER.get()
    _require(owner is not None and owner[0] is publisher, "owner_transaction_required")
    return owner[1]


async def _publication_origin(database):
    """Read native identity only within the original fresh-statement Owner body."""
    publisher = bound_publisher()
    _require(publisher.transaction_isolation == "read committed", "read_committed_publication_required")
    session = source_copy_session()
    binding = database._transaction_binding()
    _require(binding is not None and binding.session is session, "original_transaction_required")
    connection = await _copy_driver(session)
    _require(connection is publisher.connection, "physical_transaction_changed")
    _require(connection.is_in_transaction() is True, "transaction_required")
    _require(await connection.fetchval("SHOW transaction_isolation") == "read committed", "isolation_changed")
    actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    _require(
        actor["session"] == publisher.policy.publisher_role and actor["current"] == publisher.policy.owner_role,
        "owner_transaction_changed",
    )
    origin = (connection.get_server_pid(), await connection.fetchval("SELECT pg_current_xact_id()::text"))
    return publisher, session, origin


async def require_source_publication_transaction(database):
    """Recheck the issued backend and top XID without accepting a replacement TX."""
    publisher, session, origin = await _publication_origin(database)
    owner = _OWNER.get()
    _require(len(owner) == 3 and owner[2] == origin, "publication_transaction_changed")
    return publisher


@asynccontextmanager
async def source_publication_body(database):
    """Bind final control work to the existing original Owner, without owning its TX."""
    publisher, session, origin = await _publication_origin(database)
    owner = _OWNER.get()
    if len(owner) == 3:
        _require(owner[2] == origin, "publication_transaction_changed")
        yield session
        await require_source_publication_transaction(database)
        return
    token = _OWNER.set((publisher, session, origin))
    try:
        yield session
        await require_source_publication_transaction(database)
    finally:
        _OWNER.reset(token)


@asynccontextmanager
async def _savepoint(connection):
    """A closed COPY transport already aborts its native transaction on the server."""
    transaction = connection.transaction()
    await transaction.start()
    try:
        yield
    except BaseException:
        if not connection.is_closed():
            await transaction.rollback()
        raise
    else:
        await transaction.commit()


@asynccontextmanager
async def source_owner_transaction(database):
    """Keep the existing physical owner and roll back rejected work in a savepoint."""
    publisher = bound_publisher()
    binding = database._transaction_binding()
    _require(binding is not None, "original_transaction_required")
    connection = await _copy_driver(binding.session)
    _require(connection is publisher.connection, "physical_transaction_changed")
    if _OWNER.get() is not None and _OWNER.get()[0] is publisher:
        actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
        _require(
            actor["session"] == publisher.policy.publisher_role and actor["current"] == publisher.policy.owner_role,
            "owner_transaction_changed",
        )
        async with _savepoint(connection):
            yield publisher
        return
    await _genuine_publisher(connection, publisher.policy)
    async with _savepoint(connection):
        await connection.execute(f"SET LOCAL ROLE {storage_identifier(publisher.policy.owner_role)}")
        token = _OWNER.set((publisher, binding.session))
        try:
            yield publisher
            await connection.execute("RESET ROLE")
        finally:
            _OWNER.reset(token)


@asynccontextmanager
async def source_publisher_body(session):
    """Temporarily use the original Publisher inside its same physical owner transaction."""
    publisher = bound_publisher()
    owner = _OWNER.get()
    _require(owner is not None and owner[0] is publisher and owner[1] is session, "original_transaction_required")
    connection = await _copy_driver(session)
    _require(connection is publisher.connection, "physical_transaction_changed")
    actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    _require(
        actor["session"] == publisher.policy.publisher_role and actor["current"] == publisher.policy.owner_role,
        "owner_transaction_changed",
    )
    async with _savepoint(connection):
        await connection.execute("RESET ROLE")
        await _genuine_publisher(connection, publisher.policy)
        token = _OWNER.set(None)
        try:
            yield publisher
            await _genuine_publisher(connection, publisher.policy)
            await connection.execute(f"SET LOCAL ROLE {storage_identifier(publisher.policy.owner_role)}")
        finally:
            _OWNER.reset(token)
