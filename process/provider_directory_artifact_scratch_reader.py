# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Issuer-registered child work on freshly authenticated SOURCE transactions."""

import asyncio
import contextvars
import inspect
from contextlib import asynccontextmanager
from dataclasses import dataclass
from weakref import WeakKeyDictionary

from db import connection as database_context
from db.plan_release_storage import storage_relation
from process import provider_directory_artifact_scratch_inputs as inputs
from process import provider_directory_generic_source_policy as source
from process import provider_directory_output_snapshot_custody as custody
from process.provider_directory_output_snapshot_catalog import decoded, require_output

_TICKETS = WeakKeyDictionary()
_SLOT = contextvars.ContextVar("provider_directory_scratch_child_slot", default=None)
_READ = contextvars.ContextVar("provider_directory_scratch_child_read", default=None)


@dataclass(frozen=True, eq=False)
class _ReadTicket:
    directory: object
    scope: object
    issuer: object
    identity_json: str
    state: tuple
    factory: object
    projection: object
    fence: object
    resource_fence: object
    operation: object
    reader_slots: object


def _require_ticket(ticket):
    from process import provider_directory_cms_preparation as preparation

    state = _TICKETS.get(ticket)
    original = inputs._ISSUED.get(ticket.scope)
    require_output(
        state is not None
        and state["issued"] == tuple(getattr(ticket, field) for field in ticket.__dataclass_fields__)
        and state["task"] is asyncio.current_task()
        and not ticket.issuer.done()
        and original is not None
        and ticket.directory is original[0] is ticket.scope.directory
        and ticket.scope.admission is original[1]
        and ticket.issuer is original[2] is ticket.scope.task
        and ticket.identity_json == original[3]
        and ticket.state == original[4] == inputs._scope_state(ticket.scope)
        and source._FACTORY.get() is ticket.factory
        and preparation.desired_fence_hash(ticket.fence) == ticket.scope.admission.plan.desired_fence_hash
        and ticket.projection.projection_hash == ticket.scope.admission.plan.artifact_scope_projection_hash,
        "scratch_child_ticket_required",
    )
    ticket.scope.admission._assert_lease()


def _child_context(directory):
    """Clear physical and issuer-only authority before scheduling the child."""
    from process import provider_directory_artifact_scratch_scope as scratch
    from process import provider_directory_cms_preparation as preparation

    context = contextvars.copy_context()
    for variable, value in (
        (database_context._TRANSACTION, ()),
        (database_context._READER, None),
        (database_context._SESSION, None),
        (source._PUBLISHER, None),
        (source._OWNER, None),
        (custody._OUTPUT, None),
        (inputs._ACTIVE, None),
        (inputs._VERIFIED, None),
        (preparation._ACTIVE, None),
        (scratch._PROJECTION, None),
        (_READ, None),
        (_SLOT, None),
        (directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES, {}),
        (directory._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS, {}),
        (directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE, None),
        (directory._PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS, ()),
    ):
        context.run(variable.set, value)
    return context


def _issue_ticket(directory, scope, operation, reader_slots):
    from process import provider_directory_artifact_scratch_scope as scratch

    identity = inputs._require_issued(scope, directory)
    factory, captured = source._FACTORY.get(), scratch._PROJECTION.get()
    require_output(
        factory is not None
        and callable(getattr(factory, "transaction", None))
        and captured is not None
        and captured[0] is scope
        and inspect.iscoroutine(operation)
        and isinstance(reader_slots, asyncio.Semaphore),
        "scratch_child_ticket_required",
    )
    fence = directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.get()
    require_output(fence is not None, "scratch_child_fence_required")
    require_output(
        tuple(fence.source_ids) == directory._PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS.get(),
        "scratch_child_fence_changed",
    )
    return _ReadTicket(
        directory,
        scope,
        asyncio.current_task(),
        identity,
        inputs._scope_state(scope),
        factory,
        captured[1],
        fence,
        fence,
        operation,
        reader_slots,
    )


def _finish(ticket, _task):
    _TICKETS.pop(ticket, None)
    ticket.operation.close()


def create_scratch_read_task(directory, operation, *, reader_slots):
    """Keep ordinary fan-out; an enrolled child receives a distinct registered ticket."""
    try:
        from process import provider_directory_profile_scope_reads as profile_reads

        if profile_reads.is_active(directory):
            return profile_reads.create_read_task(directory, operation, reader_slots=reader_slots)
        scope = inputs.active_scratch_inputs(directory)
        if scope is None:
            return asyncio.create_task(operation)
        ticket = _issue_ticket(directory, scope, operation, reader_slots)
        gate = asyncio.get_running_loop().create_future()
        _TICKETS[ticket] = {
            "task": None,
            "issued": tuple(getattr(ticket, field) for field in ticket.__dataclass_fields__),
        }
        pending = _run_ticket(ticket, gate)
        try:
            task = asyncio.create_task(pending, context=_child_context(directory))
        except BaseException:
            pending.close()
            _TICKETS.pop(ticket, None)
            raise
        _TICKETS[ticket]["task"] = task
        task.add_done_callback(lambda completed: _finish(ticket, completed))
        gate.set_result(None)
        return task
    except BaseException:
        operation.close()
        raise


async def _verify(ticket, capability):
    _require_ticket(ticket)
    await custody._require_capability(capability)
    await inputs._verify_native_membership(ticket.directory, ticket.scope, capability, ticket.identity_json)
    _require_ticket(ticket)


@asynccontextmanager
async def _native_family(ticket):
    directory, scope = ticket.directory, ticket.scope
    _require_ticket(ticket)
    require_output(
        source._PUBLISHER.get() is None
        and source._OWNER.get() is None
        and custody._OUTPUT.get() is None
        and inputs._ACTIVE.get() is None
        and inputs._VERIFIED.get() is None
        and directory.db._transaction_binding() is None,
        "scratch_child_binding_inherited",
    )
    async with source.source_transaction(directory.db, isolation="repeatable read"):
        async with custody.output_snapshot_transaction(
            directory, operation=decoded(scope.operation_json), targets=inputs.TYPED_ROOTS
        ) as capability:
            source_copy = decoded(scope.source_copy_json)
            names = sorted([source_copy["name"], *(stage.relation for stage in scope.stages)])
            await capability.connection.execute(
                "LOCK TABLE "
                + ",".join(storage_relation(capability.policy.schema, name) for name in names)
                + " IN ACCESS SHARE MODE NOWAIT"
            )
            await _verify(ticket, capability)
            try:
                yield capability
            except BaseException as error:
                try:
                    await _verify(ticket, capability)
                except BaseException as verification_error:
                    raise BaseExceptionGroup(
                        "provider_directory_scratch_child_and_verification_failed", [error, verification_error]
                    ) from error
                raise
            else:
                await _verify(ticket, capability)


@asynccontextmanager
async def projection_slot(reader_slots):
    """Reuse only this child's exact acquired slot; ordinary callers still acquire it."""
    from process import provider_directory_profile_scope_reads as profile_reads

    if profile_reads.is_reader_slot_active(reader_slots):
        async with profile_reads.projection_slot(reader_slots):
            yield
        return
    held = _SLOT.get()
    if held is not None:
        require_output(held == (reader_slots, asyncio.current_task()), "scratch_child_slot_changed")
        yield
    else:
        async with reader_slots:
            yield


async def _run_ticket(ticket, gate):
    await gate
    _require_ticket(ticket)
    async with ticket.reader_slots:
        token = _SLOT.set((ticket.reader_slots, asyncio.current_task()))
        try:
            return await _run_native_ticket(ticket)
        finally:
            _SLOT.reset(token)


async def _run_native_ticket(ticket):
    async with _native_family(ticket) as capability:
        scope = ticket.scope
        source_copy = decoded(scope.source_copy_json)
        name_by_root = {inputs.SOURCE: source_copy["name"], **{stage.target: stage.relation for stage in scope.stages}}
        oid_by_root = {inputs.SOURCE: source_copy["copy"]["oid"], **{stage.target: stage.oid for stage in scope.stages}}
        token = _READ.set((ticket, capability))
        try:
            with ticket.directory._artifact_scope_tokens(
                ticket.fence, ticket.resource_fence, name_by_root, oid_by_root
            ):
                return await ticket.operation
        finally:
            _READ.reset(token)


async def child_scope_projection(directory, fence, resource_fence, resource_types):
    """Reattest only this registered child's exact full-scope read projection."""
    from process import provider_directory_cms_preparation as preparation

    active = _READ.get()
    if active is None:
        return None
    ticket, capability = active
    _require_ticket(ticket)
    plan = ticket.scope.admission.plan
    require_output(
        ticket.directory is directory
        and preparation.desired_fence_hash(fence)
        == preparation.desired_fence_hash(resource_fence)
        == plan.desired_fence_hash
        and tuple(sorted(resource_types)) == plan.resource_types
        and ticket.projection.projection_hash == plan.artifact_scope_projection_hash,
        "scratch_child_plan_changed",
    )
    await _verify(ticket, capability)
    return ticket.projection


async def drain_scratch_children(directory, scope):
    """Cancel and drain issued child transactions before the issuer drops its family."""
    child_tasks = [state["task"] for ticket, state in list(_TICKETS.items()) if ticket.scope is scope]
    child_tasks = [task for task in child_tasks if task is not None]
    for task in child_tasks:
        if not task.done():
            task.cancel()
    if not child_tasks:
        return
    inputs._require_issued(scope, directory)
    drained = asyncio.gather(*child_tasks, return_exceptions=True)
    cancellation = None
    while not drained.done():
        try:
            await asyncio.shield(drained)
        except asyncio.CancelledError as error:
            cancellation = error
    drained.result()
    if cancellation is not None:
        raise cancellation
