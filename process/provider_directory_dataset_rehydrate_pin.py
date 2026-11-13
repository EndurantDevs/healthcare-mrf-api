# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin a named immutable source with a repeatable-read PostgreSQL transaction."""

import copy
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import text

from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_dataset_rehydrate_scope import _load_dataset_scope
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError, _payload_hash, _record_fields
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_source_storage_binding import dataset_source_relations


def _require(condition, code):
    if not condition:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_pin_" + code)


class _SourceReader:
    """Read indexed pages from one selected group of qualified table names."""

    def __init__(self, session, schema, tables_by_root):
        self._session, self.schema, self.tables_by_root = session, schema, tables_by_root
        self.is_active = True

    def _require_owner(self):
        _require(self.is_active and self._session.in_transaction(), "reader_transaction_ended")

    def relation(self, root):
        """Resolve only a table in this active pinned generation."""
        self._require_owner()
        _require(root in self.tables_by_root, "relation_not_pinned")
        table = self.tables_by_root[root]
        return storage_relation(table["schema"], table["name"])

    async def all(self, statement, **params):
        """Read a complete bounded query within the pinned transaction."""
        self._require_owner()
        return (await self._session.execute(text(statement), params)).all()

    async def first(self, statement, **params):
        """Read one query result before the pin ends."""
        self._require_owner()
        return (await self._session.execute(text(statement), params)).first()

    async def scalar(self, statement, **params):
        """Read one scalar before the pin ends."""
        self._require_owner()
        return (await self._session.execute(text(statement), params)).scalar()

    def deactivate(self):
        """Prevent further reads after leaving the pin context."""
        self.is_active = False


@dataclass(frozen=True)
class PinnedDataset:
    """A logical published dataset and its selected named read transaction."""

    scope: object
    reader: _SourceReader
    relations: tuple

    def source_relation(self, directory):
        """Return the resource table selected by this pin."""
        return self.reader.relation(storage.RESOURCE)

    def binding(self, directory):
        """Copy the selected named layout while its transaction is active."""
        self.reader._require_owner()
        return {"tables": copy.deepcopy(self.reader.tables_by_root)}


async def _capture_source(runtime, request):
    scope = await _load_dataset_scope(runtime, request, lock=True)
    tables = await dataset_source_relations(runtime.database, runtime.schema, scope.dataset_id, storage.RELATIONS)
    return scope, bound_publisher().policy, {"tables": tables}


async def _verify_pinned_header(reader, scope, binding_by_field):
    header = storage_relation(reader.schema, storage.HEADER)
    header_row = await reader.first(
        f"SELECT endpoint_id,acquisition_root_run_id,dataset_hash,resource_count,published_at,"
        f"publication_metadata_json,status,is_current,superseded_at,"
        f"(SELECT count(*) FROM {header} current_header WHERE current_header.endpoint_id=:endpoint_id "
        f"AND current_header.is_current) AS current_count FROM {header} WHERE dataset_id=:dataset_id",
        endpoint_id=scope.endpoint_id,
        dataset_id=scope.dataset_id,
    )
    _require(header_row is not None, "source_identity_changed")
    observed = _record_fields(header_row)
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
        and _payload_hash(storage._json(observed["publication_metadata_json"])) == scope.publication_metadata_hash,
        "source_identity_changed",
    )
    tables = await dataset_source_relations(reader, reader.schema, scope.dataset_id, storage.RELATIONS)
    _require(tables == binding_by_field["tables"], "source_identity_changed")


@asynccontextmanager
async def pinned_current_source(runtime, request, *, directory):
    """Keep the selected generation readable while a replacement publishes."""
    async with source_transaction(runtime.database, isolation="repeatable read"):
        scope, _policy, binding = await _capture_source(runtime, request)
    async with runtime.database.reader_session(independent=True) as session:
        reader = _SourceReader(session, runtime.schema, binding["tables"])
        try:
            names = sorted(reader.relation(root) for root in binding["tables"])
            await session.execute(text("LOCK TABLE " + ",".join(names) + " IN ACCESS SHARE MODE NOWAIT"))
            await _verify_pinned_header(reader, scope, binding)
            relations = tuple(
                (root, table["schema"], table["name"]) for root, table in sorted(binding["tables"].items())
            )
            yield PinnedDataset(scope, reader, relations)
        finally:
            reader.deactivate()
