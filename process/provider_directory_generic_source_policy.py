# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured Source scopes and transactions without ambient-role elevation."""

import asyncio
import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from db.connection import ConnectionProxy, Database
from db.plan_release_storage import storage_identifier
from process.plan_release_storage_custody import storage_profile
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage

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
    storage_binding: object = None
    producer_identity: object = None


async def _genuine_publisher(connection, policy):
    """Require a transaction before changing any candidate rows."""
    _require(connection.is_in_transaction() is True, "transaction_required")


@asynccontextmanager
async def bind_generic_source_publisher(connection, policy, *, verify_role_profile=None, transaction_isolation):
    """Use the configured candidate schema in the caller's transaction."""
    _require(type(policy) is GenericSourcePolicy, "explicit_policy_required")
    _require(transaction_isolation in {"read committed", "repeatable read", "serializable"}, "isolation_required")
    _require(connection.is_in_transaction(), "transaction_required")
    factory = _FACTORY.get()
    storage_binding = getattr(factory, "storage_binding", None)
    if storage_binding is not None:
        _require(policy.schema == storage_binding.storage_schema, "schema_changed")
    publisher = _Publisher(
        connection,
        policy,
        asyncio.current_task(),
        transaction_isolation,
        factory,
        storage_binding,
        getattr(factory, "producer_identity", None),
    )
    token = _PUBLISHER.set(publisher)
    try:
        yield publisher
    finally:
        _PUBLISHER.reset(token)


def bound_publisher():
    """Return the publisher selected by the active import operation."""
    publisher = _PUBLISHER.get()
    _require(publisher is not None, "publisher_connection_required")
    return publisher


async def _bind_isolated_storage(publisher):
    """Resolve the configured candidate header without native identity metadata."""
    from process.provider_directory_source_storage_binding import verify_isolated_source_storage_coordinates

    await verify_isolated_source_storage_coordinates(publisher.connection, publisher.policy, publisher.storage_binding)


def current_source_storage_binding():
    """An isolated locator cannot route ordinary or inherited-task database IO."""
    if _PUBLISHER.get() is not None:
        return bound_publisher().storage_binding
    factory = _FACTORY.get()
    _require(getattr(factory, "storage_binding", None) is None, "isolated_transaction_required")
    return None


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


def _source_scope(factory):
    """Compare the configured policy, schema names and producer values."""
    _require(factory is not None, "transaction_factory_required")
    current_policy = getattr(factory, "_require_current_policy", None)
    _require(callable(current_policy), "transaction_factory_required")
    current_policy()
    configured = getattr(factory, "policy", None)
    _require(type(configured) is GenericSourcePolicy, "explicit_policy_required")
    return configured, getattr(factory, "storage_binding", None), getattr(factory, "producer_identity", None)


def _publisher_scope(publisher):
    return publisher.policy, publisher.storage_binding, publisher.producer_identity


def _require_current_source_scope(scope):
    _require(_source_scope(_FACTORY.get()) == scope, "policy_changed")


def _require_active_source_session(session):
    active = getattr(session, "in_transaction", None)
    if not callable(active):
        active = getattr(session, "is_in_transaction", None)
    _require(callable(active) and active() is True, "transaction_required")


def source_producer_identity() -> tuple[str, str]:
    """Return baked (source commit, image digest) without authorizing a SQL session."""
    factory = _FACTORY.get()
    scope = _source_scope(factory)
    identity = scope[2]
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
        _require(_publisher_scope(publisher) == scope, "policy_changed")
        _require_active_source_session(publisher.connection)
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
    _require(getattr(factory, "storage_binding", None) is None, "isolated_activation_unavailable")
    _require(
        callable(getattr(factory, "publication_session_factory", None))
        and callable(getattr(factory, "bind_existing_transaction", None)),
        "publication_factory_required",
    )
    return factory


async def source_publication_sessions():
    """Choose the trusted final SOURCE pool before capture/WAL opens its transaction."""
    return await _publication_factory().publication_session_factory()


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
    scope = _source_scope(factory)
    async with factory.bind_existing_transaction(database, session, isolation="read committed"):
        _require_current_source_scope(scope)
        _require_active_source_session(session)
        async with source_publication_body(database):
            yield session
    _require_current_source_scope(scope)


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
async def source_transaction(database, *, isolation="read committed", is_batch_loading=False):
    """Use the configured transaction for a complete batch and its checkpoint."""
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
    scope = _source_scope(factory)
    async with guard(database) as issued:
        _require_current_source_scope(scope)
        _require(_PUBLISHER.get() is None and _OWNER.get() is None, "checkpoint_guard_changed")
        yield issued
        _require_current_source_scope(scope)
        _require(_PUBLISHER.get() is None and _OWNER.get() is None, "checkpoint_guard_changed")


def source_copy_session():
    """Return the active COPY session within the configured Source scope."""
    publisher = bound_publisher()
    owner = _OWNER.get()
    _require(
        owner is not None and _publisher_scope(owner[0]) == _publisher_scope(publisher), "owner_transaction_required"
    )
    if publisher.factory is not None or _FACTORY.get() is not None:
        _require_current_source_scope(_publisher_scope(publisher))
    _require_active_source_session(publisher.connection)
    _require_active_source_session(owner[0].connection)
    _require_active_source_session(owner[1])
    return owner[1]


def source_copy_executor(executor):
    """Route named-source SQL through the current batch session."""
    if isinstance(getattr(bound_publisher(), "storage_binding", None), NamedFHIRSourceStorage):
        return ConnectionProxy(None, source_copy_session(), None)
    return executor


async def source_native_executor(executor):
    """Route named batches through their session; explicit legacy executors keep native SQL access."""
    return source_copy_executor(executor)


async def _publication_origin(database):
    """Use one read-committed transaction for the atomic pointer change."""
    publisher = bound_publisher()
    _require(publisher.transaction_isolation == "read committed", "read_committed_publication_required")
    session = source_copy_session()
    _require(publisher.connection.is_in_transaction(), "transaction_required")
    return publisher, session


async def require_source_publication_transaction(database):
    """Require active publication work before a pointer update."""
    publisher, _session = await _publication_origin(database)
    return publisher


@asynccontextmanager
async def source_publication_body(database):
    """Keep publication changes in the caller's transaction."""
    publisher, session = await _publication_origin(database)
    token = _OWNER.set((publisher, session))
    try:
        yield session
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
    """Execute candidate writes with the configured SQL role and a rollback savepoint."""
    publisher = bound_publisher()
    binding = database._transaction_binding()
    _require(binding is not None, "transaction_required")
    connection = publisher.connection
    if _OWNER.get() is not None:
        async with _savepoint(connection):
            yield publisher
        return
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
    """Execute publisher-only statements using PostgreSQL role permissions."""
    publisher = bound_publisher()
    async with _savepoint(publisher.connection):
        await publisher.connection.execute("RESET ROLE")
        token = _OWNER.set(None)
        try:
            yield publisher
            await publisher.connection.execute(f"SET LOCAL ROLE {storage_identifier(publisher.policy.owner_role)}")
        finally:
            _OWNER.reset(token)
