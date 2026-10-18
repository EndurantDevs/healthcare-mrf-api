# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep an authenticated immutable SOURCE in an independent original Reader."""

import asyncio
import copy
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from uuid import uuid4
from weakref import WeakKeyDictionary

from sqlalchemy import text

from db.connection import Database
from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_storage as storage
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_dataset_rehydrate_scope import _load_dataset_scope
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError, _payload_hash, _record_fields
from process.provider_directory_generic_source_policy import (
    bound_publisher,
    source_transaction,
    source_transaction_available,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

_PINNED_READERS = WeakKeyDictionary()


def _require(condition, code):
    if not condition:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_pin_" + code)


class _SourceReader:
    """Explicit same-task scans; never bind a request or Writer context."""

    def __init__(self, session, schema, tables_by_root):
        self._session, self.schema, self.tables_by_root = session, schema, tables_by_root
        self.task, self.is_active = asyncio.current_task(), True

    def _require_owner(self):
        _require(self.is_active and self.task is asyncio.current_task(), "reader_owner_changed")
        _require(self._session.in_transaction(), "reader_transaction_ended")

    def relation(self, root):
        """Select an exact pinned child rather than mutable parent routing."""
        self._require_owner()
        _require(root in self.tables_by_root, "relation_not_pinned")
        return storage_relation(self.schema, self.tables_by_root[root]["name"])

    async def all(self, statement, **params):
        """Read a bounded page on this original pinned transaction."""
        self._require_owner()
        return (await self._session.execute(text(statement), params)).all()

    async def first(self, statement, **params):
        """Read one result on this original pinned transaction."""
        self._require_owner()
        return (await self._session.execute(text(statement), params)).first()

    async def scalar(self, statement, **params):
        """Read one value on this original pinned transaction."""
        self._require_owner()
        return (await self._session.execute(text(statement), params)).scalar()

    def deactivate(self):
        """Invalidate the facade before releasing its physical pins."""
        self.is_active = False


@dataclass(frozen=True)
class PinnedDataset:
    """Current identity plus exact original Reader and native child receipts."""

    scope: object
    reader: _SourceReader
    relations: tuple

    def source_relation(self, directory):
        """Use only the issued, active pin and its original application database."""
        _require(type(self.reader) is _SourceReader, "reader_unpinned")
        self.reader._require_owner()
        captured = _PINNED_READERS.get(self.reader)
        _require(
            captured is not None
            and captured[:3] == (self.scope, directory, directory.db)
            and self.reader.tables_by_root == captured[3]["tables"],
            "reader_unpinned",
        )
        return self.reader.relation(storage.RESOURCE)

    def binding(self, directory):
        """Return detached issued native authority for the fresh final header fence."""
        self.source_relation(directory)
        return copy.deepcopy(_PINNED_READERS[self.reader][3])


async def _capture_source(runtime, request):
    scope = await _load_dataset_scope(runtime, request, lock=True)
    publisher = bound_publisher()
    connection, policy = publisher.connection, publisher.policy
    _require(policy.schema == runtime.schema, "schema_changed")
    encoded = await connection.fetchval(
        f"SELECT candidate_storage_json FROM {storage_relation(policy.schema, storage.HEADER)} WHERE dataset_id=$1",
        scope.dataset_id,
    )
    binding_by_field = storage._json(encoded)
    _require(type(binding_by_field) is dict and type(binding_by_field.get("identity")) is dict, "snapshot_required")
    identity = binding_by_field["identity"]
    storage._validate_binding(binding_by_field, identity, phases=("published",))
    _require(
        (identity["dataset_id"], identity["endpoint_id"], identity["acquisition_root_run_id"])
        == (scope.dataset_id, scope.endpoint_id, scope.acquisition_root_run_id),
        "source_identity_changed",
    )
    actual = await storage._locked(connection, policy, identity, phases=("published",))
    _require(actual == binding_by_field, "snapshot_changed")
    await storage._verify_tables(connection, policy, actual)
    return scope, policy, copy.deepcopy(actual)


async def _require_reader(session, reader_database, policy):
    actor_result = await session.execute(
        text(
            "SELECT session_user::text AS login,current_user::text AS actor,"
            "current_setting('transaction_read_only') AS readonly,"
            "current_setting('transaction_isolation') AS isolation"
        )
    )
    actor = actor_result.mappings().one()
    native = (await session.connection()).get_raw_connection
    driver = (await native()).driver_connection
    login = reader_database._reader_login[0]
    _require(
        actor["login"] == actor["actor"] == login
        and login in policy.runtime_roles
        and login not in policy.control_runtime_roles,
        "reader_identity_changed",
    )
    _require(actor["readonly"] == "on" and actor["isolation"] == "repeatable read", "reader_transaction_changed")
    await storage_roles(driver, policy.schema, **policy.role_arguments())
    return driver


async def _same_database(publisher, reader):
    """Reuse the native random transaction-lock identity probe, never a claim."""
    key = int.from_bytes(uuid4().bytes[:8], "big", signed=True)
    _require(await publisher.fetchval("SELECT pg_try_advisory_xact_lock($1::bigint)", key), "database_probe_busy")
    _require(not await reader.fetchval("SELECT pg_try_advisory_xact_lock($1::bigint)", key), "reader_database_changed")


async def _pin_children(reader, policy, binding_by_field):
    tables_by_root = binding_by_field["tables"]
    _require(set(tables_by_root) == set(storage.RELATIONS), "snapshot_changed")
    relations = ",".join(storage_relation(policy.schema, proof["name"]) for _, proof in sorted(tables_by_root.items()))
    await reader.execute(f"LOCK TABLE {relations} IN ACCESS SHARE MODE NOWAIT")
    await storage._verify_tables(reader, policy, binding_by_field)


async def _verify_pinned_header(reader, scope, binding_by_field):
    header = storage_relation(reader.schema, storage.HEADER)
    header_record = await reader.first(
        f"SELECT endpoint_id,acquisition_root_run_id,dataset_hash,resource_count,published_at,"
        f"publication_metadata_json,candidate_storage_json,status,is_current,superseded_at,"
        f"(SELECT count(*) FROM {header} current_header WHERE current_header.endpoint_id=:endpoint_id "
        f"AND current_header.is_current) AS current_count FROM {header} WHERE dataset_id=:dataset_id",
        endpoint_id=scope.endpoint_id,
        dataset_id=scope.dataset_id,
    )
    _require(header_record is not None, "source_identity_changed")
    observed = _record_fields(header_record)
    _require(
        tuple(
            observed[key]
            for key in ("endpoint_id", "acquisition_root_run_id", "dataset_hash", "resource_count", "published_at")
        )
        == (
            scope.endpoint_id,
            scope.acquisition_root_run_id,
            scope.dataset_hash,
            scope.resource_count,
            scope.published_at,
        )
        and observed["status"] == "published"
        and observed["is_current"] is True
        and observed["superseded_at"] is None
        and observed["current_count"] == 1
        and _payload_hash(storage._json(observed["publication_metadata_json"])) == scope.publication_metadata_hash
        and storage._json(observed["candidate_storage_json"]) == binding_by_field,
        "source_identity_changed",
    )


async def _close_reader(session):
    try:
        await drain_operation(session.rollback(), preserve_cancellation=True)
    finally:
        await drain_operation(session.close(), preserve_cancellation=False)


@asynccontextmanager
async def pinned_current_source(runtime, request, *, directory):
    """Release the short Publisher before yielding the separately pinned Reader."""
    _require(
        isinstance(runtime.database, Database)
        and directory is sys.modules.get("process.provider_directory_fhir")
        and directory.db is runtime.database
        and directory._schema() == runtime.schema,
        "runtime_changed",
    )
    _require(
        runtime.database._transaction_binding() is None and not runtime.database.has_reader_session(), "owner_bound"
    )
    _require(source_transaction_available() is None, "owner_bound")
    async with AsyncExitStack() as cleanup:
        async with source_transaction(runtime.database, isolation="repeatable read"):
            scope, policy, binding_by_field = await _capture_source(runtime, request)
            reader_database = await runtime.database._connect_reader()
            session = reader_database.session_factory()
            cleanup.push_async_callback(_close_reader, session)
            driver = await _require_reader(session, reader_database, policy)
            await _same_database(bound_publisher().connection, driver)
            await _pin_children(driver, policy, binding_by_field)
            reader = _SourceReader(session, runtime.schema, copy.deepcopy(binding_by_field["tables"]))
            cleanup.callback(reader.deactivate)
            await _verify_pinned_header(reader, scope, binding_by_field)
        receipts = tuple(
            (root, proof["name"], proof["oid"], proof["owner"], proof["layout_sha256"])
            for root, proof in sorted(binding_by_field["tables"].items())
        )
        _PINNED_READERS[reader] = (scope, directory, runtime.database, copy.deepcopy(binding_by_field))
        cleanup.callback(_PINNED_READERS.pop, reader)
        yield PinnedDataset(scope, reader, receipts)
