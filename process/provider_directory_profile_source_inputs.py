# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Issued physical SOURCE inputs for the original signed Profile workload."""

import asyncio
import contextvars
import copy
import inspect
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from uuid import uuid4
from weakref import WeakKeyDictionary

from db import connection as database_context
from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_policy as source
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_owned_wal_transaction as owned
from process.provider_directory_artifact_scratch_inputs import _source_headers, _source_pin
from process.provider_directory_generic_source_provision import verify_generic_source_family
from process.provider_directory_output_snapshot_catalog import decoded, encoded, require_output
from process.provider_directory_projection_stage import _copy_driver
from process.uhc_flex_practitioner_async_safety import drain_operation

_ACTIVE = ContextVar("provider_directory_profile_source_inputs", default=None)
_ISSUED = WeakKeyDictionary()


@dataclass(frozen=True, eq=False)
class _Inputs:
    directory: object
    factory: object
    policy: object
    connection: object
    task: object
    full_json: str
    resource_json: str
    producer: tuple
    probe: int


def _fence_json(fence):
    from process.provider_directory_cms_preparation import desired_fence_hash

    return encoded(
        dict(
            fence=desired_fence_hash(fence),
            select_validated=getattr(fence, "should_select_validated_candidates", False),
            datasets=[
                dict(
                    key=_dataset_key(dataset),
                    metadata=dataset.publication_metadata_hash,
                    resources=getattr(dataset, "resource_count", None),
                    validated=getattr(dataset, "validated_at", None),
                )
                for dataset in fence.datasets
            ],
        )
    )


def _require_scope(directory):
    scope = _ACTIVE.get()
    state_by_field = _ISSUED.get(scope) if scope is not None else None
    require_output(
        state_by_field is not None
        and state_by_field["active"]
        and scope.directory is directory
        and source._FACTORY.get() is scope.factory
        and scope.factory.policy is scope.policy
        and source.source_producer_identity() == scope.producer
        and (asyncio.current_task() is scope.task or asyncio.current_task() in state_by_field["tasks"])
        and not scope.task.done(),
        "profile_input_scope_required",
    )
    require_output(
        _fence_json(state_by_field["full"]) == scope.full_json
        and _fence_json(state_by_field["resource"]) == scope.resource_json,
        "profile_input_fence_changed",
    )
    return scope, state_by_field


def _dataset_key(dataset):
    return dataset.source_id, dataset.dataset_id, dataset.evidence_run_id


def binding(directory, dataset):
    """Return detached native storage only for this issued exact input coordinate."""
    if not source.is_source_storage_required():
        return None
    scope, state_by_field = _require_scope(directory)
    if getattr(dataset, "dataset_id", None) is None:
        matches = [key for key in state_by_field["bindings"] if key[0] == dataset.source_id]
        require_output(len(matches) == 1, "profile_input_source_ambiguous")
        key = matches[0]
    else:
        key = _dataset_key(dataset)
        require_output(key in state_by_field["coordinates"], "profile_input_dataset_changed")
    if hasattr(dataset, "endpoint_id"):
        require_output(
            dataset.endpoint_id == state_by_field["full_coordinates"][key].endpoint_id, "profile_input_endpoint_changed"
        )
    worker = state_by_field["tasks"].get(asyncio.current_task())
    if worker is not None and worker["kind"] == "materialize":
        _require_worker(directory, worker)
        batches = worker["projection"].batches
        require_output(any(_dataset_key(batch) == key for batch in batches), "profile_input_batch_changed")
        if hasattr(dataset, "batch_number"):
            require_output(any(dataset is batch for batch in batches), "profile_input_batch_changed")
    return copy.deepcopy(state_by_field["bindings"][key])


def dataset(directory, batch):
    """Return the exact originally selected dataset, including promotion authority."""
    if not source.is_source_storage_required():
        return batch
    captured = binding(directory, batch)
    scope, state_by_field = _require_scope(directory)
    identity = captured["identity"]
    key = (batch.source_id, identity["dataset_id"], identity["acquisition_root_run_id"])
    return state_by_field["full_coordinates"][key]


def resource_relation(directory, dataset):
    """Select the same authenticated physical resource child for count and COPY."""
    captured = binding(directory, dataset)
    name = storage.RESOURCE if captured is None else captured["tables"][storage.RESOURCE]["name"]
    return name


def _coordinates(full, resource):
    full_by_key = {_dataset_key(dataset): dataset for dataset in full.datasets}
    resource_by_key = {_dataset_key(dataset): dataset for dataset in resource.datasets}
    require_output(
        len(full_by_key) == len(full.datasets)
        and all(
            dataset.status == "published"
            or dataset.status == "validated"
            and getattr(dataset, "promote_on_cutover", False) is True
            and getattr(full, "should_select_validated_candidates", False) is True
            for dataset in full.datasets
        )
        and len(resource_by_key) == len(resource.datasets)
        and all(key in full_by_key and dataset == full_by_key[key] for key, dataset in resource_by_key.items()),
        "profile_input_resource_fence_changed",
    )
    return full_by_key, resource_by_key


async def _capture(scope, coordinates):
    headers_by_id = {}
    async for headers in _source_headers(scope, sorted({key[1] for key in coordinates})):
        headers_by_id.update({header["dataset_id"]: header for header in headers})
    require_output(set(headers_by_id) == {key[1] for key in coordinates}, "profile_input_header_missing")
    bindings_by_key = {}
    for key, dataset in coordinates.items():
        header = headers_by_id[dataset.dataset_id]
        captured = decoded(header["candidate_storage_json"])
        require_output(type(captured) is dict, "profile_input_binding_required")
        storage._validate_binding(captured, captured.get("identity"), phases=("published",))
        _source_pin(dataset, header, captured)
        metadata = decoded(header["publication_metadata_json"])
        require_output(
            header["status"] == dataset.status
            and (
                dataset.status == "published"
                or dataset.status == "validated"
                and getattr(dataset, "promote_on_cutover", False) is True
            )
            and header["is_current"] is dataset.is_current
            and header["publication_metadata_sha256"] == dataset.publication_metadata_hash
            and type(metadata) is dict
            and all(
                metadata.get(name) == expected_fact for name, expected_fact in captured["identity"]["metadata"].items()
            )
            and (
                captured["identity"]["dataset_id"],
                captured["identity"]["endpoint_id"],
                captured["identity"]["acquisition_root_run_id"],
            )
            == (dataset.dataset_id, dataset.endpoint_id, dataset.evidence_run_id),
            "profile_input_header_changed",
        )
        bindings_by_key[key] = captured
    names = sorted({proof["name"] for captured in bindings_by_key.values() for proof in captured["tables"].values()})
    if names:
        await scope.connection.execute(
            "LOCK TABLE "
            + ",".join(storage_relation(scope.policy.schema, name) for name in names)
            + " IN ACCESS SHARE MODE NOWAIT"
        )
    for captured in bindings_by_key.values():
        await storage._verify_tables(scope.connection, scope.policy, captured)
    return bindings_by_key


async def _drain(state_by_field):
    tasks = tuple(state_by_field["tasks"])
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


@asynccontextmanager
async def profile_source_input_scope(directory, full_fence, resource_fence):
    """Pin original physical inputs independently of every small payload transaction."""
    if not source.is_source_storage_required():
        yield
        return
    require_output(_ACTIVE.get() is None, "profile_input_scope_already_bound")
    producer = source.source_producer_identity()
    factory = source._FACTORY.get()
    require_output(callable(getattr(factory, "observation_transaction", None)), "profile_input_factory_required")
    full_by_key, coordinates = _coordinates(full_fence, resource_fence)
    async with factory.observation_transaction(isolation="repeatable read") as session:
        driver = await _copy_driver(session)
        scope = _Inputs(
            directory,
            factory,
            factory.policy,
            driver,
            asyncio.current_task(),
            _fence_json(full_fence),
            _fence_json(resource_fence),
            producer,
            int.from_bytes(uuid4().bytes[:8], "big", signed=True),
        )
        require_output(scope.policy.schema == directory._schema(), "profile_input_schema_changed")
        require_output(
            await driver.fetchval("SELECT pg_try_advisory_xact_lock($1::bigint)", scope.probe),
            "profile_input_probe_busy",
        )
        await verify_generic_source_family(driver, scope.policy)
        captured = await _capture(scope, full_by_key)
        state_by_field = dict(
            active=True,
            full=full_fence,
            resource=resource_fence,
            coordinates=coordinates,
            bindings=captured,
            full_coordinates=full_by_key,
            tasks={},
        )
        _ISSUED[scope] = state_by_field
        token = _ACTIVE.set(scope)
        try:
            try:
                async with read_transaction(directory):
                    await directory._verify_provider_directory_artifact_dataset_fence(full_fence)
                yield
            except BaseException:
                await drain_operation(_drain(state_by_field), preserve_cancellation=False)
                raise
            else:
                await drain_operation(_drain(state_by_field), preserve_cancellation=True)
        finally:
            state_by_field["active"] = False
            _ACTIVE.reset(token)
            _ISSUED.pop(scope, None)


@asynccontextmanager
async def _read_actor(directory):
    """Reuse only an issued observation or the original same-task SOURCE owner."""
    from process import provider_directory_source_observation as observation

    observed = observation.current_observation(directory)
    if observed is not None:
        yield observed
        require_output(observation.current_observation(directory) is observed, "profile_input_observer_changed")
    elif source._PUBLISHER.get() is not None:
        publisher = source.bound_publisher()
        binding = directory.db._transaction_binding()
        require_output(
            binding is not None and binding.session is source.source_copy_session(),
            "profile_input_original_session_required",
        )
        yield publisher
        require_output(
            source.bound_publisher() is publisher
            and directory.db._transaction_binding() is binding
            and source.source_copy_session() is binding.session,
            "profile_input_publisher_changed",
        )
    else:
        async with source.source_transaction(directory.db, isolation="repeatable read"):
            yield source.bound_publisher()


@asynccontextmanager
async def read_transaction(directory):
    """Read through the exact issued observation or genuine SOURCE owner."""
    if not source.is_source_storage_required():
        async with directory.db.transaction():
            yield
        return
    scope, state_by_field = _require_scope(directory)
    async with _read_actor(directory) as publisher:
        require_output(
            publisher.factory is scope.factory and publisher.policy is scope.policy, "profile_input_publisher_changed"
        )
        require_output(
            not await publisher.connection.fetchval("SELECT pg_try_advisory_xact_lock($1::bigint)", scope.probe),
            "profile_input_database_changed",
        )
        observed = _Inputs(
            directory,
            scope.factory,
            scope.policy,
            publisher.connection,
            scope.task,
            scope.full_json,
            scope.resource_json,
            scope.producer,
            scope.probe,
        )
        require_output(
            await _capture(observed, state_by_field["coordinates"])
            == {key: state_by_field["bindings"][key] for key in state_by_field["coordinates"]},
            "profile_input_binding_changed",
        )
        yield
        _require_scope(directory)


def _child_context():
    context = contextvars.copy_context()
    for variable, value in (
        (database_context._TRANSACTION, ()),
        (database_context._READER, None),
        (database_context._SESSION, None),
        (source._PUBLISHER, None),
        (source._OWNER, None),
        (owned._ACTIVE_OWNED_TRANSACTION, None),
    ):
        context.run(variable.set, value)
    return context


def _start_task(directory, operation, worker_by_field):
    try:
        scope, state_by_field = _require_scope(directory)
        require_output(
            asyncio.current_task() is scope.task and inspect.iscoroutine(operation), "profile_input_issuer_required"
        )
        gate = asyncio.get_running_loop().create_future()

        async def run():
            """Wait for registration before the original worker opens its fresh owner."""
            await gate
            _require_scope(directory)
            _require_worker(directory, worker_by_field)
            return await operation

        pending = run()
        try:
            task = asyncio.create_task(pending, context=_child_context())
        except BaseException:
            pending.close()
            raise
        state_by_field["tasks"][task] = worker_by_field
        task.add_done_callback(lambda completed: operation.close())
        gate.set_result(None)
        return task
    except BaseException:
        if inspect.iscoroutine(operation):
            operation.close()
        raise


def _require_worker(directory, worker_by_field):
    from process.provider_directory_profile_scope_copy import scope_projection

    admission = directory._provider_directory_profile_capacity_admission()
    projection = scope_projection(directory)
    require_output(
        admission is worker_by_field["admission"]
        and (admission.lease.lease_digest, admission.lease.reservation_id) == worker_by_field["lease"]
        and any(table is worker_by_field["projection"] for table in projection.tables)
        and directory._provider_directory_artifact_scope_table_name(
            worker_by_field["model"].__tablename__, admission.run_id
        )
        == worker_by_field["relation"],
        "profile_input_worker_changed",
    )


def create_materialization_task(directory, operation, *, model, relation, table_projection):
    """Register the original signed table worker without granting write authority."""
    if not source.is_source_storage_required():
        return asyncio.create_task(operation)
    try:
        admission = directory._provider_directory_profile_capacity_admission()
        require_output(
            admission is not None
            and any(model is item for item in directory.RESOURCE_MODELS)
            and table_projection.table_name == model.__tablename__,
            "profile_input_worker_required",
        )
        worker_by_field = dict(
            kind="materialize",
            model=model,
            relation=relation,
            projection=table_projection,
            admission=admission,
            lease=(admission.lease.lease_digest, admission.lease.reservation_id),
        )
        _require_worker(directory, worker_by_field)
        return _start_task(directory, operation, worker_by_field)
    except BaseException:
        if inspect.iscoroutine(operation):
            operation.close()
        raise
