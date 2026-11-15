# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Issued short reads of the original completed nine-table Profile snapshot."""

import asyncio
import contextvars
import inspect
from contextlib import asynccontextmanager
from dataclasses import dataclass
from weakref import WeakKeyDictionary

from db import connection as database_context
from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_policy as source
from process import provider_directory_owned_wal_transaction as owned
from process import provider_directory_profile_native_copy as native_copy
from process import provider_directory_profile_source_inputs as inputs
from process.provider_directory_output_snapshot_catalog import encoded, require_output
from process.uhc_flex_practitioner_async_safety import drain_operation

_ACTIVE = contextvars.ContextVar("provider_directory_profile_scope_reads", default=None)
_SLOT = contextvars.ContextVar("provider_directory_profile_read_slot", default=None)
_READ = contextvars.ContextVar("provider_directory_profile_read_transaction", default=None)
_ISSUED = WeakKeyDictionary()


@dataclass(frozen=True, eq=False)
class _Scope:
    directory: object
    issuer: object
    admission: object
    projection: object
    lease: object
    lease_identity: tuple
    factory: object
    pin: object
    pin_state: object
    full: object
    resource: object
    full_json: str
    resource_json: str
    names_json: str
    producer: tuple


def _require_scope(directory):
    scope = _ACTIVE.get()
    scope_state_by_field = _ISSUED.get(scope) if scope is not None else None
    task = asyncio.current_task()
    require_output(
        scope_state_by_field is not None
        and scope_state_by_field["active"]
        and scope.directory is directory
        and not scope.issuer.done()
        and (task is scope.issuer or task in scope_state_by_field["tasks"])
        and source._FACTORY.get() is scope.factory
        and source.source_producer_identity() == scope.producer
        and directory._provider_directory_profile_capacity_admission() is scope.admission
        and inputs._ISSUED.get(scope.pin) is scope.pin_state
        and scope.pin_state["active"]
        and scope.pin_state["full"] is scope.full
        and scope.pin_state["resource"] is scope.resource
        and not scope.pin.task.done()
        and scope.pin.factory is scope.factory
        and scope.factory.policy is scope.pin.policy,
        "profile_read_scope_required",
    )
    require_output(
        scope.admission.lease is scope.lease
        and (scope.lease.lease_digest, scope.lease.reservation_id) == scope.lease_identity,
        "profile_read_lease_changed",
    )
    directory._assert_artifact_scope_projection(scope.projection)
    require_output(
        inputs._fence_json(scope.full) == scope.full_json == scope.pin.full_json
        and inputs._fence_json(scope.resource) == scope.resource_json == scope.pin.resource_json
        and directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.get() is scope.full
        and tuple(scope.resource.source_ids) == directory._PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS.get()
        and encoded(directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()) == scope.names_json,
        "profile_read_snapshot_changed",
    )
    return scope, scope_state_by_field


def is_active(directory):
    """Select this issued Profile family before any unrelated scratch reader."""
    if _ACTIVE.get() is None:
        return False
    _require_scope(directory)
    return True


async def _native_actor(scope, publisher, session):
    """Require the declared native transaction settings for each bounded read."""
    driver = publisher.connection
    require_output(session.in_transaction() and driver.is_in_transaction() is True, "profile_read_transaction_changed")
    require_output(
        await driver.fetchval("SHOW transaction_isolation") == publisher.transaction_isolation,
        "profile_read_transaction_changed",
    )
    return driver


async def _attest(scope, scope_state_by_field, *, reuse_catalog=False):
    """Recheck the signed input, logical names and completed nine-table data shape."""
    directory = scope.directory
    _require_scope(directory)
    publisher = source.bound_publisher()
    session = source.source_copy_session()
    require_output(
        session.in_transaction()
        and publisher.factory is scope.factory
        and publisher.policy is scope.pin.policy
        and publisher.policy.schema == directory._schema()
        and publisher.transaction_isolation in {"read committed", "repeatable read", "serializable"},
        "profile_read_transaction_changed",
    )
    await directory._provider_directory_profile_capacity_acceptance_time(scope.lease)
    driver = await _native_actor(scope, publisher, session)
    if reuse_catalog:
        active = _READ.get()
        require_output(active == (scope, asyncio.current_task()), "profile_read_transaction_inherited")
        return session
    names = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
    await driver.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(publisher.policy.schema, names[root]) for root in sorted(names))
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    target_bindings = tuple(
        (model, names[model.__tablename__]) for model in (directory.ProviderDirectorySource, *directory.RESOURCE_MODELS)
    )
    catalog_by_root = await native_copy._target_family_catalog(directory, publisher, target_bindings, phase="completed")
    require_output(all(type(facts) is dict for facts in catalog_by_root.values()), "profile_read_catalog_required")
    catalog_json = encoded(catalog_by_root)
    if scope_state_by_field["catalog_by_root"] is None:
        scope_state_by_field["catalog_by_root"] = catalog_json
    require_output(scope_state_by_field["catalog_by_root"] == catalog_json, "profile_read_catalog_changed")
    _require_scope(directory)
    return session


@asynccontextmanager
async def _verified_read(scope, scope_state_by_field):
    is_catalog_reused = _READ.get() is not None
    session = await _attest(scope, scope_state_by_field, reuse_catalog=is_catalog_reused)
    token = _READ.set((scope, asyncio.current_task()))
    try:
        try:
            yield session
        except BaseException as error:
            try:
                await _attest(scope, scope_state_by_field, reuse_catalog=is_catalog_reused)
            except BaseException as verification_error:
                failures = BaseExceptionGroup("profile_read_and_verification_failed", [error, verification_error])
                if isinstance(error, asyncio.CancelledError):
                    raise error from failures
                raise failures from error
            raise
        else:
            await _attest(scope, scope_state_by_field, reuse_catalog=is_catalog_reused)
    finally:
        _READ.reset(token)


@asynccontextmanager
async def read_transaction(directory):
    """Reuse the original owner body or open one fresh SOURCE transaction."""
    scope, scope_state_by_field = _require_scope(directory)
    active = _READ.get()
    if active is not None:
        require_output(active[:2] == (scope, asyncio.current_task()), "profile_read_transaction_inherited")
    if source._PUBLISHER.get() is not None:
        # bound_publisher and source_copy_session reject inherited or non-owner bindings.
        async with _verified_read(scope, scope_state_by_field) as session:
            yield session
    else:
        require_output(
            directory.db._transaction_binding() is None and source._OWNER.get() is None,
            "profile_read_binding_inherited",
        )
        async with source.source_transaction(directory.db, isolation="repeatable read"):
            async with _verified_read(scope, scope_state_by_field) as session:
                yield session


async def _drain(scope_state_by_field):
    tasks = tuple(scope_state_by_field["tasks"])
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _validated_admission(directory, projection, relation_by_table):
    """Keep the exact signed nine-root run names and completed projection."""
    admission = directory._provider_directory_profile_capacity_admission()
    require_output(
        admission is not None
        and admission.geometry.bounded_admission is True
        and type(projection) is directory._ProviderDirectoryArtifactScopeExactProjection,
        "profile_read_admission_required",
    )
    directory._assert_artifact_scope_projection(projection)
    models = (directory.ProviderDirectorySource, *directory.RESOURCE_MODELS)
    roots = {model.__tablename__ for model in models}
    require_output(
        len(models) == len(roots) == 9
        and len(projection.tables) == 9
        and {table.table_name for table in projection.tables} == roots
        and set(relation_by_table) == roots
        and len(set(relation_by_table.values())) == 9
        and all(
            relation_by_table[root] == directory._provider_directory_artifact_scope_table_name(root, admission.run_id)
            for root in roots
        ),
        "profile_read_family_changed",
    )
    return admission


@asynccontextmanager
async def prepared_scope(directory, projection, relation_by_table, full_fence, resource_fence):
    """Issue read authority only after the original signed family is completed."""
    require_output(_ACTIVE.get() is None and source.is_source_storage_required(), "profile_read_scope_required")
    admission = _validated_admission(directory, projection, relation_by_table)
    pin, pin_state = inputs._require_scope(directory)
    scope = _Scope(
        directory,
        asyncio.current_task(),
        admission,
        projection,
        admission.lease,
        (admission.lease.lease_digest, admission.lease.reservation_id),
        source._FACTORY.get(),
        pin,
        pin_state,
        full_fence,
        resource_fence,
        inputs._fence_json(full_fence),
        inputs._fence_json(resource_fence),
        encoded(relation_by_table),
        source.source_producer_identity(),
    )
    scope_state_by_field = dict(active=True, tasks={}, catalog_by_root=None)
    _ISSUED[scope] = scope_state_by_field
    token = _ACTIVE.set(scope)
    try:
        async with read_transaction(directory) as session:
            require_output(session.in_transaction(), "profile_read_transaction_changed")
        try:
            yield
        except BaseException:
            await drain_operation(_drain(scope_state_by_field), preserve_cancellation=False)
            raise
        else:
            await drain_operation(_drain(scope_state_by_field), preserve_cancellation=True)
            async with read_transaction(directory) as session:
                require_output(session.in_transaction(), "profile_read_transaction_changed")
    finally:
        scope_state_by_field["active"] = False
        _ACTIVE.reset(token)
        _ISSUED.pop(scope, None)


def _child_context():
    from process import provider_directory_artifact_scratch_reader as scratch_reader
    from process import provider_directory_output_snapshot_custody as custody

    context = contextvars.copy_context()
    for variable, value in (
        (database_context._TRANSACTION, ()),
        (database_context._READER, None),
        (database_context._SESSION, None),
        (source._PUBLISHER, None),
        (source._OWNER, None),
        (owned._ACTIVE_OWNED_TRANSACTION, None),
        (custody._OUTPUT, None),
        (inputs._ACTIVE, None),
        (scratch_reader._READ, None),
        (scratch_reader._SLOT, None),
        (_READ, None),
        (_SLOT, None),
    ):
        context.run(variable.set, value)
    return context


def is_reader_slot_active(reader_slots):
    """Report only an exact current-task permit; copied permits are rejected."""
    held = _SLOT.get()
    if held is None:
        return False
    require_output(held == (reader_slots, asyncio.current_task()), "profile_read_slot_changed")
    return True


@asynccontextmanager
async def projection_slot(reader_slots):
    """Reuse this child's acquired permit without opening a database transaction."""
    if is_reader_slot_active(reader_slots):
        yield
    else:
        async with reader_slots:
            yield


async def _run_child(directory, scope, operation, reader_slots, gate):
    await gate
    _require_scope(directory)
    async with reader_slots:
        token = _SLOT.set((reader_slots, asyncio.current_task()))
        try:
            require_output(_ACTIVE.get() is scope, "profile_read_ticket_changed")
            _require_scope(directory)
            return await operation
        finally:
            _SLOT.reset(token)


def create_read_task(directory, operation, *, reader_slots):
    """Issue a gated child and acquire capacity before its original owner opens."""
    try:
        scope, scope_state_by_field = _require_scope(directory)
        require_output(
            asyncio.current_task() is scope.issuer
            and inspect.iscoroutine(operation)
            and isinstance(reader_slots, asyncio.Semaphore),
            "profile_read_ticket_required",
        )
        gate = asyncio.get_running_loop().create_future()
        pending = _run_child(directory, scope, operation, reader_slots, gate)
        try:
            task = asyncio.create_task(pending, context=_child_context())
        except BaseException:
            pending.close()
            raise
        scope_state_by_field["tasks"][task] = operation
        task.add_done_callback(lambda _task: operation.close())
        gate.set_result(None)
        return task
    except BaseException:
        operation.close()
        raise
