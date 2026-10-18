# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Load the original signed Profile scope and verify its completed snapshot."""

import asyncio
import re
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar

from process import provider_directory_artifact_scratch_inputs as scratch_inputs
from process import provider_directory_generic_source_policy as source_policy
from process import provider_directory_profile_native_copy as native_copy
from process import provider_directory_profile_source_inputs as source_inputs
from process.provider_directory_dataset_rehydrate_pages import projected_pages
from process.uhc_flex_practitioner_async_safety import drain_operation

_SCOPE = ContextVar("provider_directory_profile_scope_copy", default=None)


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_profile_scope_copy_invalid")


def is_enabled(fhir):
    """SOURCE preparation requires its original signed bounded Profile admission."""
    if not source_policy.is_source_storage_required():
        return False
    admission = fhir._provider_directory_profile_capacity_admission()
    _require(admission is not None and admission.geometry.bounded_admission is True)
    return True


def scope_projection(fhir):
    """Keep the exact original frozen plan, including its ordered zero probes."""
    state = _SCOPE.get()
    _require(state is not None)
    projection = state["projection"]
    _require(type(projection) is fhir._ProviderDirectoryArtifactScopeExactProjection)
    fhir._assert_artifact_scope_projection(projection)
    return projection


@asynccontextmanager
async def input_transaction(fhir):
    """Keep legacy reads unchanged and use the issued SOURCE reader when enrolled."""
    if source_policy.is_source_storage_required():
        async with source_inputs.read_transaction(fhir):
            yield
        return
    yield


@asynccontextmanager
async def source_read_transaction(fhir):
    """Keep observations of closed SOURCE storage off the ordinary reader pool."""
    if not source_policy.is_source_storage_required():
        yield
        return
    from process import provider_directory_source_observation as observation

    observed = observation.current_observation(fhir)
    if observed is not None:
        yield
        _require(observation.current_observation(fhir) is observed)
    elif source_policy._PUBLISHER.get() is not None:
        publisher = source_policy.bound_publisher()
        binding = fhir.db._transaction_binding()
        _require(binding is not None and binding.session is source_policy.source_copy_session())
        yield
        _require(
            source_policy.bound_publisher() is publisher
            and fhir.db._transaction_binding() is binding
            and source_policy.source_copy_session() is binding.session
        )
    else:
        async with source_policy.source_transaction(fhir.db, isolation="repeatable read"):
            yield


@asynccontextmanager
async def preflight_transaction(fhir, *, isolation, readonly):
    """Preserve each original preflight mode on its genuine database actor."""
    _require(isolation in {"read committed", "repeatable read", "serializable"} and type(readonly) is bool)
    if source_policy.is_source_storage_required():
        from process import provider_directory_source_observation as observation

        transaction = (
            observation.observation_transaction(fhir, isolation=isolation)
            if readonly
            else source_policy.source_transaction(fhir.db, isolation=isolation)
        )
        async with transaction as session:
            yield session
    else:
        async with fhir.db.transaction() as session:
            yield session


@asynccontextmanager
async def prepared_read_transaction(fhir):
    """Keep completed Profile reads on their exact actor and snapshot."""
    if not source_policy.is_source_storage_required():
        yield
        return
    from process import provider_directory_profile_scope_reads as reads

    if reads.is_active(fhir):
        async with reads.read_transaction(fhir):
            yield
        return
    async with source_read_transaction(fhir):
        yield


async def loading_capacity(fhir, schema, relations, projection):
    """Keep preventive bytes/WAL checks without rescanning the growing payload."""
    if source_policy.is_source_storage_required():
        await fhir._assert_artifact_scope_usage(schema, relations, projection, check_rows=False)
    else:
        await fhir._assert_provider_directory_artifact_scope_observed_capacity(schema, relations, projection)


async def completed_capacity(fhir, schema, relations, projection, created_tables):
    """Preserve exact failed-candidate cleanup before any read exposure."""
    assert projection is not None
    try:
        await fhir._assert_provider_directory_artifact_scope_observed_capacity(schema, relations, projection)
    except BaseException as capacity_error:
        await fhir._cleanup_failed_artifact_scope(schema, created_tables, capacity_error)
        raise


async def resource_fence(fhir, fence, targets):
    """Resolve the unchanged source delta through its genuine database actor."""
    if source_policy.is_source_storage_required():
        async with source_policy.source_transaction(fhir.db, isolation="repeatable read"):
            return await fhir._provider_directory_profile_resource_scope_fence(fence, targets)
    return await fhir._provider_directory_profile_resource_scope_fence(fence, targets)


def create_materialization_task(fhir, operation, *, model, relation, table_projection):
    """Retain original worker waves while issuing each SOURCE worker explicitly."""
    try:
        if is_enabled(fhir):
            return source_inputs.create_materialization_task(
                fhir, operation, model=model, relation=relation, table_projection=table_projection
            )
        return asyncio.create_task(operation)
    except BaseException:
        operation.close()
        raise


async def gather_materialization_tasks(fhir, operations):
    """Drain partial task issuance before failure cleanup can touch their heaps."""
    tasks = []
    try:
        for operation, options_by_name in operations:
            tasks.append(create_materialization_task(fhir, operation, **options_by_name))
    except BaseException as failure:
        for operation, _options in operations[len(tasks) :]:
            operation.close()
        try:
            await drain_operation(_cancel_workers(tasks), preserve_cancellation=False)
        except BaseException as cleanup_failure:
            raise BaseExceptionGroup(
                "provider_directory_profile_worker_issuance_and_cleanup_failed", [failure, cleanup_failure]
            )
        raise
    await fhir._gather_provider_directory_profile_tasks(tasks)


async def _cancel_workers(tasks):
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@contextmanager
def materialization_scope(fhir, projection):
    """Close the candidate to every shared payload writer before validation."""
    if not is_enabled(fhir):
        yield
        return
    _require(_SCOPE.get() is None and type(projection) is fhir._ProviderDirectoryArtifactScopeExactProjection)
    fhir._assert_artifact_scope_projection(projection)
    token = _SCOPE.set({"projection": projection, "closed": False})
    try:
        yield
    finally:
        _SCOPE.reset(token)


@asynccontextmanager
async def payload_transaction(fhir, kind, relation_ref, batch, projection, statement, params_by_name, *, mutating=True):
    """Retain the original reservation and real owner through COPY settlement."""
    if not is_enabled(fhir):
        async with fhir.profile_artifact_custody.payload_transaction(
            fhir, kind, relation_ref, batch, projection, statement, params_by_name, mutating=mutating
        ):
            yield
        return
    _require(_SCOPE.get() is not None and _SCOPE.get()["closed"] is False)
    source_inputs.binding(fhir, batch)
    identity = ("artifact_scope_payload" if mutating else "artifact_scope_zero_probe", kind, relation_ref, batch)
    async with fhir._provider_directory_profile_capacity_transaction(control_identity=identity):
        outcome = fhir.profile_owned_wal.current_owned_wal_transaction(fhir.db)
        _require(outcome is not None)
        yield
    fhir.profile_control_custody.consume_owned_groups(fhir, outcome, None)


def _table_projection(fhir, model, batch):
    matching_tables = [table for table in scope_projection(fhir).tables if table.table_name == model.__tablename__]
    _require(len(matching_tables) == 1 and any(candidate is batch for candidate in matching_tables[0].batches))
    return matching_tables[0]


async def _target_oid(fhir, relation):
    qualified = fhir._unscoped_qt(fhir._schema(), relation)
    oid = await fhir.db.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=qualified)
    _require(type(oid) is int and oid > 0)
    return oid


async def copy_batch(fhir, model, batch, statement, params_by_name):
    """COPY bounded pages from the same selected physical input used by preflight."""
    _require(is_enabled(fhir) and _SCOPE.get() is not None and _SCOPE.get()["closed"] is False)
    admission = fhir._provider_directory_profile_capacity_admission()
    relation = fhir._provider_directory_artifact_scope_table_name(model.__tablename__, admission.run_id)
    table_projection = _table_projection(fhir, model, batch)
    source_binding = source_inputs.binding(fhir, batch)
    request = native_copy.NativeCopyRequest(
        model=model,
        relation=relation,
        target_oid=await _target_oid(fhir, relation),
        batch=batch,
        table_projection=table_projection,
        source_binding=source_binding,
        projection_sql=statement,
        projection_params=dict(params_by_name),
        scope_projection=scope_projection(fhir),
    )
    pages = (
        scratch_inputs._source_pages(fhir, statement, [batch.source_id])
        if model is fhir.ProviderDirectorySource
        else _resource_pages(fhir, model, batch)
    )
    outcome = fhir.profile_owned_wal.current_owned_wal_transaction(fhir.db)
    async with fhir.profile_artifact_custody.copy_payload_transaction(fhir, request, outcome):
        return await fhir.profile_artifact_custody.execute_copy(fhir, request, pages, outcome)


def _resource_pages(fhir, model, batch):
    """Page the frozen key range without rebuilding a large materialized batch."""
    statement = fhir._provider_directory_artifact_resource_select_sql(model, fhir._schema(), dataset=batch)
    statement = (
        "SELECT p.* FROM (" + statement + ") p "
        "WHERE (CAST(:frozen_after AS varchar) IS NULL OR p.resource_id>CAST(:frozen_after AS varchar)) "
        "AND p.resource_id<=CAST(:frozen_last AS varchar)"
    )
    params_by_name = dict(
        source_ids=[batch.source_id],
        dataset_ids=[batch.dataset_id],
        evidence_run_ids=[batch.evidence_run_id],
        resource_type=batch.resource_type,
        frozen_after=batch.after_resource_id,
        frozen_last=batch.last_resource_id,
    )
    return projected_pages(fhir.db, statement, params_by_name, model)


def _resource_expected(fhir, model, schema, fence, resource_types):
    statements, params_by_name = [], {}
    resource_type = fhir.RESOURCE_TYPES_BY_MODEL[model]
    for index, dataset in enumerate(fhir._artifact_scope_resource_datasets(fence, resource_types, resource_type)):
        statement = fhir._provider_directory_artifact_resource_select_sql(model, schema, dataset=dataset)
        values_by_name = dict(
            source_ids=[dataset.source_id],
            dataset_ids=[dataset.dataset_id],
            evidence_run_ids=[dataset.evidence_run_id],
            resource_type=resource_type,
        )
        statement = re.sub(r"(?<!:):([a-z_]+)\b", lambda match: ":d" + str(index) + "_" + match[1], statement)
        statements.append("(" + statement + ")")
        params_by_name.update({"d" + str(index) + "_" + key: value for key, value in values_by_name.items()})
    return " UNION ALL ".join(statements), params_by_name


def _parity_sql(fhir, model, qualified, expected):
    columns = ",".join(fhir._q(str(column.name)) for column in model.__table__.columns)
    keys = tuple(str(column.name) for column in model.__table__.primary_key.columns)
    _require(bool(keys))
    join = " AND ".join("a." + fhir._q(key) + "=e." + fhir._q(key) for key in keys)
    expected = expected or "SELECT " + columns + " FROM " + qualified + " WHERE FALSE"
    return (
        "WITH expected(" + columns + ") AS MATERIALIZED (" + expected + ") "
        "SELECT (SELECT count(*) FROM expected)=:expected_rows "
        "AND (SELECT count(*) FROM " + qualified + ")=:expected_rows "
        "AND NOT EXISTS (SELECT FROM expected e LEFT JOIN "
        + qualified
        + " a ON "
        + join
        + " WHERE a."
        + fhir._q(keys[0])
        + " IS NULL OR to_jsonb(a) IS DISTINCT FROM to_jsonb(e)) "
        "AND NOT EXISTS (SELECT FROM " + qualified + " a WHERE NOT EXISTS (SELECT FROM expected e WHERE " + join + "))"
    )


async def validate_snapshot(fhir, schema, plan, fence, resource_fence, resource_types):
    """Check completed native indexes and full tuples once after all writers drain."""
    if not is_enabled(fhir):
        return
    projection = scope_projection(fhir)
    _require(_SCOPE.get()["closed"] is False)
    _SCOPE.get()["closed"] = True
    async with fhir._provider_directory_profile_capacity_transaction(
        control_identity=("artifact_scope_snapshot_validation", projection.projection_hash)
    ):
        outcome = fhir.profile_owned_wal.current_owned_wal_transaction(fhir.db)
        for table in projection.tables:
            model = plan.model_by_table_name[table.table_name]
            relation = plan.relation_by_table[table.table_name]
            oid = await _target_oid(fhir, relation)
            before = await native_copy.completed_target_catalog(fhir, model, relation, oid, outcome=outcome)
            if model is fhir.ProviderDirectorySource:
                statement = fhir._provider_directory_artifact_source_select_sql(model, schema)
                params_by_name = {"source_ids": sorted(set(fence.source_ids))}
            else:
                statement, params_by_name = _resource_expected(fhir, model, schema, resource_fence, resource_types)
            qualified = fhir._unscoped_qt(schema, relation)
            valid = await fhir.db.scalar(
                _parity_sql(fhir, model, qualified, statement), **params_by_name, expected_rows=table.projected_rows
            )
            _require(valid is True)
            after = await native_copy.completed_target_catalog(fhir, model, relation, oid, outcome=outcome)
            _require(after == before)
