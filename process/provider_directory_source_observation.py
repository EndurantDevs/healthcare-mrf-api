# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Task-bound readonly observations from the trusted SOURCE factory."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from weakref import WeakKeyDictionary

from process import provider_directory_generic_source_policy as source
from process.provider_directory_output_snapshot_catalog import require_output
from process.provider_directory_projection_stage import _copy_driver

_ACTIVE = ContextVar("provider_directory_source_observation", default=None)
_ISSUED = WeakKeyDictionary()
_ISOLATIONS = {
    "read committed": {"read committed", "repeatable read", "serializable"},
    "repeatable read": {"repeatable read", "serializable"},
    "serializable": {"serializable"},
}
_ORIGIN_SQL = (
    "SELECT session_user::text AS session,current_user::text AS current,pg_backend_pid() AS backend,"
    "current_setting('transaction_read_only') AS readonly,"
    "current_setting('transaction_isolation') AS isolation,"
    "(SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() "
    "AND locktype='virtualxid' AND granted) AS transaction"
)


@dataclass(frozen=True, eq=False)
class _Observation:
    directory: object
    database: object
    factory: object
    policy: object
    task: object
    session: object
    connection: object
    isolation: str
    producer: tuple
    origin: tuple
    storage_binding: object


def _fields(observation):
    return tuple(getattr(observation, name) for name in observation.__dataclass_fields__)


def _storage_binding(directory, factory):
    from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

    binding = getattr(factory, "storage_binding", None)
    require_output(
        directory._schema() == factory.policy.schema
        if binding is None
        else type(binding) is FHIRSourceStorageBinding
        and directory._schema() == binding.catalog_schema
        and factory.policy.schema == binding.storage_schema,
        "source_observation_schema_changed",
    )
    return binding


def current_observation(directory):
    """Return only this task's issued original SOURCE readonly binding."""
    observation = _ACTIVE.get()
    if observation is None:
        return None
    require_output(type(observation) is _Observation, "source_observation_required")
    binding = directory.db._transaction_binding()
    require_output(
        _ISSUED.get(observation) == _fields(observation)
        and observation.directory is directory
        and observation.database is directory.db
        and observation.task is asyncio.current_task()
        and not observation.task.done()
        and source._FACTORY.get() is observation.factory
        and observation.factory.policy is observation.policy
        and source.source_producer_identity() == observation.producer
        and _storage_binding(directory, observation.factory) is observation.storage_binding
        and binding is not None
        and binding.session is observation.session
        and binding.borrowed is True
        and binding.owner_task is observation.task
        and source._PUBLISHER.get() is None
        and source._OWNER.get() is None,
        "source_observation_changed",
    )
    return observation


def current_storage_binding(catalog_schema):
    """Expose private coordinates only through the original readonly observation."""
    observation = _ACTIVE.get()
    if observation is None:
        return None
    require_output(type(observation) is _Observation, "source_observation_required")
    current_observation(observation.directory)
    require_output(observation.directory._schema() == catalog_schema, "source_observation_schema_changed")
    return observation.storage_binding


async def _native_origin(directory, factory, session, connection, isolation):
    require_output(
        source._FACTORY.get() is factory
        and await _copy_driver(session) is connection
        and connection.is_in_transaction() is True,
        "source_observation_connection_changed",
    )
    actor_by_field = await connection.fetchrow(_ORIGIN_SQL)
    require_output(
        actor_by_field is not None
        and actor_by_field["session"] == factory.policy.publisher_role
        and actor_by_field["current"] == factory.policy.owner_role
        and actor_by_field["readonly"] == "on"
        and actor_by_field["isolation"] == isolation
        and type(actor_by_field["backend"]) is int
        and actor_by_field["backend"] > 0
        and type(actor_by_field["transaction"]) is str
        and bool(actor_by_field["transaction"]),
        "source_observation_actor_changed",
    )
    storage_binding = _storage_binding(directory, factory)
    if storage_binding is not None:
        from process.provider_directory_source_storage_binding import verify_isolated_source_storage_coordinates

        await verify_isolated_source_storage_coordinates(connection, factory.policy, storage_binding)
    return actor_by_field["backend"], actor_by_field["transaction"]


async def _verify(directory, observation):
    require_output(current_observation(directory) is observation, "source_observation_changed")
    origin = await _native_origin(
        directory, observation.factory, observation.session, observation.connection, observation.isolation
    )
    require_output(
        origin == observation.origin and current_observation(directory) is observation,
        "source_observation_transaction_changed",
    )


async def capture_fhir_input(directory, session, schema):
    """Attest protected FHIR inputs on the issued readonly transaction."""
    observation = current_observation(directory)
    require_output(
        observation is not None and observation.session is session and observation.policy.schema == schema,
        "source_observation_connection_changed",
    )
    await _verify(directory, observation)
    capture = getattr(observation.factory, "observe_source_callable_capability", None)
    require_output(callable(capture), "source_observation_callable_factory_required")
    capability = await capture(observation.connection, observation.policy)
    require_output(
        type(capability) is tuple
        and all(type(oid) is int and 0 < oid < 2**32 for oid in capability)
        and len(set(capability)) == len(capability),
        "source_observation_callable_capability_changed",
    )
    await _verify(directory, observation)
    from process.provider_directory_generic_source_provision import capture_protected_fhir_input

    proof = await capture_protected_fhir_input(observation.connection, source_policy=observation.policy)
    await _verify(directory, observation)
    require_output(proof is not None, "source_observation_fhir_inputs_changed")
    return proof


@asynccontextmanager
async def _verified_body(directory, observation):
    await _verify(directory, observation)
    try:
        yield observation.session
    except BaseException as error:
        try:
            await _verify(directory, observation)
        except BaseException as verification_error:
            failures = BaseExceptionGroup("source_observation_and_verification_failed", [error, verification_error])
            if isinstance(error, asyncio.CancelledError):
                raise error from failures
            raise failures from error
        raise
    else:
        await _verify(directory, observation)


@asynccontextmanager
async def observation_transaction(directory, *, isolation):
    """Bind a real factory observation without manufacturing writer authority."""
    require_output(isolation in _ISOLATIONS and source.is_source_storage_required(), "source_observation_required")
    existing = current_observation(directory)
    if existing is not None:
        require_output(existing.isolation in _ISOLATIONS[isolation], "source_observation_isolation_changed")
        async with _verified_body(directory, existing) as session:
            yield session
        return
    factory = source._FACTORY.get()
    require_output(
        callable(getattr(factory, "observation_transaction", None))
        and directory.db._transaction_binding() is None
        and source._PUBLISHER.get() is None
        and source._OWNER.get() is None,
        "source_observation_own_transaction_required",
    )
    producer = source.source_producer_identity()
    task = asyncio.current_task()
    require_output(task is not None, "source_observation_task_required")
    async with factory.observation_transaction(isolation=isolation) as session:
        connection = await _copy_driver(session)
        async with directory.db.bind_existing_session(session):
            origin = await _native_origin(directory, factory, session, connection, isolation)
            observation = _Observation(
                directory, directory.db, factory, factory.policy, task, session, connection, isolation, producer, origin,
                _storage_binding(directory, factory),
            )
            _ISSUED[observation] = _fields(observation)
            token = _ACTIVE.set(observation)
            try:
                async with _verified_body(directory, observation) as bound:
                    yield bound
            finally:
                _ACTIVE.reset(token)
                _ISSUED.pop(observation, None)
