# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original-session authority and bounded parent fences for batch checkpoints."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar

from db.connection import ConnectionProxy, Database
from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_policy as source
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session, source_transaction
from process.provider_directory_generic_source_storage import HEADER, _require
from process.provider_directory_projection_stage import _copy_driver
from process.provider_directory_source_checkpoint_catalog import BULK_ROOT, capture_checkpoint_controls
from process.provider_directory_source_storage_binding import require_storage_schema

_CUSTODY = ContextVar("provider_directory_source_checkpoint_custody", default=None)
_BULK_IDENTITY = ContextVar("provider_directory_bulk_checkpoint_identity", default=None)
_BULK_IDENTITY_FIELDS = (
    "checkpoint_id",
    "canonical_api_base",
    "resource_type",
    "source_scope_hash",
    "strategy_version",
    "acquisition_root_run_id",
    "endpoint_id",
    "dataset_id",
    "start_url_hash",
)
RELEASE_BULK_CHECKPOINT_SQL = """
WITH released AS (
    DELETE FROM {acquisition}
     WHERE checkpoint_id = :checkpoint_id AND state = :state
       AND status_url_ciphertext IS NULL AND manifest_hash IS NULL
    RETURNING checkpoint_id
)
DELETE FROM {output} AS output USING released
 WHERE output.checkpoint_id = released.checkpoint_id;
"""


async def assert_bulk_checkpoint_guard_owner(directory, guard_connection, identity):
    """Probe the lock backend and read closed storage through its issued actor."""
    from process import provider_directory_profile_scope_copy as scope

    try:
        uses_source = source.is_source_storage_required()
        if uses_source:
            owns_lock = await guard_connection.scalar(
                directory.db.text(
                    "SELECT EXISTS(SELECT FROM pg_locks WHERE locktype='advisory' AND granted "
                    "AND mode='ExclusiveLock' AND pid=pg_backend_pid() AND objsubid=1 "
                    "AND classid=((hashtextextended(:checkpoint_id,0)>>32)&4294967295)::oid "
                    "AND objid=(hashtextextended(:checkpoint_id,0)&4294967295)::oid)"
                ),
                {"checkpoint_id": identity.checkpoint_id},
            )
            _require(owns_lock is True, "checkpoint_guard_lost")
        async with scope.source_read_transaction(directory):
            executor = source_copy_session() if uses_source else guard_connection
            is_owner = await executor.scalar(
                directory.db.text(
                    f"SELECT EXISTS(SELECT FROM {directory._bulk_acquisition_checkpoint_table_ref()} "
                    "WHERE checkpoint_id=:checkpoint_id AND owner_run_id=:owner_run_id "
                    "AND state NOT IN (:complete_state,:failed_state))"
                ),
                {
                    "checkpoint_id": identity.checkpoint_id,
                    "owner_run_id": identity.owner_run_id,
                    "complete_state": directory.BULK_EXPORT_CHECKPOINT_COMPLETE,
                    "failed_state": directory.BULK_EXPORT_CHECKPOINT_FAILED,
                },
            )
    except Exception as exc:
        raise RuntimeError("bulk_export_checkpoint_worker_guard_lost") from exc
    if is_owner is not True:
        raise RuntimeError("bulk_export_checkpoint_ownership_lost")


async def persist_control_progress(marker, *args, **kwargs):
    """Keep ordinary run-control writes outside an active Source transaction."""
    if source._PUBLISHER.get() is None:
        return await marker(*args, **kwargs)
    bound_publisher()
    from process.provider_directory_profile_source_inputs import _child_context

    context = _child_context()
    context.run(source._FACTORY.set, None)
    return await asyncio.create_task(marker(*args, **kwargs), context=context)


def _coordinates(values, limit):
    _require(not isinstance(values, (str, bytes)), "checkpoint_coordinates_invalid")
    values = tuple(values)
    _require(
        len(values) <= 256 and all(type(value) is str and 1 <= len(value) <= limit for value in values),
        "checkpoint_coordinates_invalid",
    )
    return sorted(set(values))


async def require_checkpoint_executor(executor, *, bulk_identity=None):
    """Connection-taking children must borrow the exact physical writer session."""
    publisher = bound_publisher()
    session = source_copy_session()
    if isinstance(executor, Database):
        binding = executor._transaction_binding()
        is_original = binding is not None and binding.session is session
    else:
        is_original = isinstance(executor, ConnectionProxy) and executor._connection is session
    _require(is_original and await _copy_driver(session) is publisher.connection, "checkpoint_transaction_changed")
    if bulk_identity is not None:
        _require(_BULK_IDENTITY.get() == _bulk_identity(bulk_identity), "checkpoint_identity_scope_changed")
        if getattr(publisher, "storage_binding", None) is not None:
            checkpoint = await publisher.connection.fetchrow(
                f"SELECT {','.join(_BULK_IDENTITY_FIELDS)} FROM {storage_relation(publisher.policy.schema, BULK_ROOT)} "
                "WHERE checkpoint_id=$1 FOR SHARE",
                bulk_identity.checkpoint_id,
            )
            _require(checkpoint is not None, "checkpoint_parent_missing")
            _assert_bulk_identity([checkpoint], _bulk_identity(bulk_identity))
    return publisher


def _bulk_identity(identity):
    identity_by_field = {field: getattr(identity, field, None) for field in _BULK_IDENTITY_FIELDS}
    _require(all(type(value) is str and value for value in identity_by_field.values()), "checkpoint_identity_invalid")
    return identity_by_field


def _assert_bulk_identity(rows, identity_by_field):
    if identity_by_field is not None:
        # Owner and retry lineage are mutable under the existing adoption policy.
        _require(
            all(all(row.get(field) == value for field, value in identity_by_field.items()) for row in rows),
            "checkpoint_identity_changed",
        )


def _require_checkpoint_parents(parents, dataset_ids, identity_by_field):
    """Preserve terminal guards and explicitly check private parent relationships."""
    _require(
        all(parent["status"] not in {"acquisition_abandoned", "acquisition_retired"} for parent in parents),
        "checkpoint_parent_abandoned",
    )
    _require(set(dataset_ids) <= {parent["dataset_id"] for parent in parents}, "checkpoint_parent_missing")
    if identity_by_field is not None:
        fields = ("dataset_id", "endpoint_id", "acquisition_root_run_id")
        _require(
            any(all(parent[field] == identity_by_field[field] for field in fields) for parent in parents),
            "checkpoint_parent_changed",
        )


async def fence_checkpoint_dataset_parents(publisher, dataset_ids, root_run_ids):
    """Keep mutable private checkpoints attached to an existing live root."""
    dataset_ids = _coordinates(dataset_ids, 96)
    root_run_ids = _coordinates(root_run_ids, 64)
    _require(len(dataset_ids) == len(root_run_ids) == 1, "checkpoint_coordinates_conflict")
    parents = await publisher.connection.fetch(
        f"SELECT dataset_id,status,acquisition_root_run_id FROM {storage_relation(publisher.policy.schema, HEADER)} "
        "WHERE dataset_id=ANY($1::varchar[]) OR acquisition_root_run_id=ANY($2::varchar[]) "
        'ORDER BY dataset_id COLLATE "C" FOR KEY SHARE',
        dataset_ids,
        root_run_ids,
    )
    _require_checkpoint_parents(parents, dataset_ids, None)
    _require(
        any(
            parent["dataset_id"] == dataset_ids[0] and parent["acquisition_root_run_id"] == root_run_ids[0]
            for parent in parents
        ),
        "checkpoint_parent_changed",
    )


async def fence_bulk_parents(publisher, dataset_ids, root_run_ids, checkpoint_ids, *, bulk_identity=None):
    """Fence both old/new dataset and root parents once before the bounded mutation."""
    connection, schema = publisher.connection, publisher.policy.schema
    dataset_ids = _coordinates(dataset_ids, 96)
    root_run_ids = _coordinates(root_run_ids, 64)
    checkpoint_ids = _coordinates(checkpoint_ids, 64)
    identity_by_field = _bulk_identity(bulk_identity) if bulk_identity is not None else None
    if identity_by_field is not None:
        _require(
            dataset_ids == [identity_by_field["dataset_id"]]
            and root_run_ids == [identity_by_field["acquisition_root_run_id"]]
            and checkpoint_ids == [identity_by_field["checkpoint_id"]],
            "checkpoint_coordinates_conflict",
        )
    columns = ",".join(_BULK_IDENTITY_FIELDS)
    existing = await connection.fetch(
        f"SELECT {columns} FROM {storage_relation(schema, BULK_ROOT)} "
        "WHERE checkpoint_id=ANY($1::varchar[]) ORDER BY checkpoint_id",
        checkpoint_ids,
    )
    _assert_bulk_identity(existing, identity_by_field)
    observed_by_checkpoint = {
        checkpoint_row["checkpoint_id"]: (checkpoint_row["dataset_id"], checkpoint_row["acquisition_root_run_id"])
        for checkpoint_row in existing
    }
    all_datasets = sorted(set(dataset_ids) | {checkpoint_row["dataset_id"] for checkpoint_row in existing})
    all_roots = sorted(set(root_run_ids) | {checkpoint_row["acquisition_root_run_id"] for checkpoint_row in existing})
    is_private = getattr(publisher, "storage_binding", None) is not None
    parent_columns = "dataset_id,status,endpoint_id,acquisition_root_run_id" if is_private else "dataset_id,status"
    parents = await connection.fetch(
        f"SELECT {parent_columns} FROM {storage_relation(schema, HEADER)} "
        "WHERE dataset_id=ANY($1::varchar[]) OR acquisition_root_run_id=ANY($2::varchar[]) "
        'ORDER BY dataset_id COLLATE "C" FOR SHARE',
        all_datasets,
        all_roots,
    )
    _require_checkpoint_parents(parents, all_datasets if is_private else (), identity_by_field if is_private else None)
    # Headers lock before checkpoint rows, matching terminal operators. A
    # concurrent coordinate change must retry instead of escaping the fence.
    locked = await connection.fetch(
        f"SELECT {columns} FROM {storage_relation(schema, BULK_ROOT)} "
        "WHERE checkpoint_id=ANY($1::varchar[]) ORDER BY checkpoint_id FOR UPDATE",
        checkpoint_ids,
    )
    _assert_bulk_identity(locked, identity_by_field)
    _require(
        all(
            (checkpoint_row["dataset_id"], checkpoint_row["acquisition_root_run_id"])
            == observed_by_checkpoint.get(
                checkpoint_row["checkpoint_id"],
                (dataset_ids[0], root_run_ids[0]) if len(dataset_ids) == len(root_run_ids) == 1 else None,
            )
            for checkpoint_row in locked
        ),
        "checkpoint_parent_changed",
    )


@asynccontextmanager
async def source_checkpoint_transaction(
    database, *, schema=None, dataset_ids=(), root_run_ids=(), checkpoint_ids=(), bulk_identity=None
):
    """One existing batch transaction; never hold it over fetching or worker dispatch."""
    if bulk_identity is not None:
        _require(not dataset_ids and not root_run_ids and not checkpoint_ids, "checkpoint_coordinates_conflict")
        identity_by_field = _bulk_identity(bulk_identity)
        dataset_ids = [identity_by_field["dataset_id"]]
        root_run_ids = [identity_by_field["acquisition_root_run_id"]]
        checkpoint_ids = [identity_by_field["checkpoint_id"]]
    async with source_transaction(database):
        publisher = await require_checkpoint_executor(database)
        storage_binding = getattr(publisher, "storage_binding", None)
        expected_schema = publisher.policy.schema if storage_binding is None else storage_binding.catalog_schema
        _require(schema is None or schema == expected_schema, "checkpoint_namespace_changed")
        if storage_binding is not None:
            require_storage_schema(publisher, expected_schema)
        token = None
        bulk_token = None
        if _CUSTODY.get() is not publisher:
            if storage_binding is None:
                await capture_checkpoint_controls(publisher.connection, publisher.policy)
            else:
                await capture_checkpoint_controls(
                    publisher.connection, publisher.policy, storage_binding=storage_binding
                )
            token = _CUSTODY.set(publisher)
        try:
            if checkpoint_ids:
                await fence_bulk_parents(
                    publisher, dataset_ids, root_run_ids, checkpoint_ids, bulk_identity=bulk_identity
                )
            elif storage_binding is not None and (dataset_ids or root_run_ids):
                await fence_checkpoint_dataset_parents(publisher, dataset_ids, root_run_ids)
            if bulk_identity is not None:
                bulk_token = _BULK_IDENTITY.set(identity_by_field)
            yield source_copy_session()
        finally:
            if bulk_token is not None:
                _BULK_IDENTITY.reset(bulk_token)
            if token is not None:
                _CUSTODY.reset(token)
