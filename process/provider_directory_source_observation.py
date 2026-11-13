# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Task-bound readonly observations from the trusted SOURCE factory."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from process import provider_directory_generic_source_policy as source
from process.provider_directory_output_snapshot_catalog import require_output
from process.provider_directory_projection_stage import _copy_driver
from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage

_ACTIVE = ContextVar("provider_directory_source_observation", default=None)
_ISOLATIONS = {
    "read committed": {"read committed", "repeatable read", "serializable"},
    "repeatable read": {"repeatable read", "serializable"},
    "serializable": {"serializable"},
}


@dataclass(frozen=True, eq=False)
class _Observation:
    directory: object
    factory: object
    policy: object
    session: object
    connection: object
    isolation: str
    storage_binding: object


def _storage_binding(directory, factory):
    from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding, NamedFHIRSourceStorage

    binding = getattr(factory, "storage_binding", None)
    require_output(
        directory._schema() == factory.policy.schema
        if binding is None
        else isinstance(binding, (FHIRSourceStorageBinding, NamedFHIRSourceStorage))
        and directory._schema() == binding.catalog_schema
        and factory.policy.schema == binding.storage_schema,
        "source_observation_schema_changed",
    )
    return binding


def current_observation(directory):
    """Return the readonly observation selected for the current logical import scope."""
    observation = _ACTIVE.get()
    if observation is not None:
        require_output(type(observation) is _Observation, "source_observation_required")
        require_output(
            _storage_binding(directory, observation.factory) == observation.storage_binding,
            "source_observation_schema_changed",
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


async def _require_active_transaction(directory, factory, session):
    """Require an active transaction without storing transport or actor identities."""
    transaction = session.get_transaction()
    require_output(transaction is not None and transaction.is_active, "source_observation_transaction_required")
    _storage_binding(directory, factory)


async def _verify(directory, observation):
    require_output(current_observation(directory) is not None, "source_observation_required")
    await _require_active_transaction(directory, observation.factory, observation.session)


async def capture_fhir_input(directory, session, schema):
    """Attest protected FHIR inputs on the issued readonly transaction."""
    observation = current_observation(directory)
    require_output(
        observation is not None
        and (isinstance(observation.storage_binding, NamedFHIRSourceStorage) or observation.session is session)
        and observation.policy.schema == schema,
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
    async with factory.observation_transaction(isolation=isolation) as session:
        connection = await _copy_driver(session)
        async with directory.db.bind_existing_session(session):
            await _require_active_transaction(directory, factory, session)
            observation = _Observation(
                directory,
                factory,
                factory.policy,
                session,
                connection,
                isolation,
                _storage_binding(directory, factory),
            )
            token = _ACTIVE.set(observation)
            try:
                async with _verified_body(directory, observation) as bound:
                    yield bound
            finally:
                _ACTIVE.reset(token)
