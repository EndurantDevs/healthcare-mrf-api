# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound isolated NPI buffers while retaining the legacy logical dedup windows."""

import asyncio
import sys
from contextlib import asynccontextmanager
from contextvars import ContextVar
from uuid import uuid4

from process.provider_directory_projection_stage import _copy_driver

RAW_ROWS = 128
RAW_BYTES = 4 * 1024**2
COPY_ROWS = 1000
COPY_BYTES = 1024**2
WINDOW_BYTES = 4 * 1024**3
_CURRENT = ContextVar("npi_isolated_write_window", default=None)


class NpiBatchError(RuntimeError):
    """The complete isolated payload does not fit its admitted resource bounds."""


def memory_bytes(value):
    """Count containers, native values and UTF-8 COPY conversion conservatively."""
    seen_ids = set()

    def size(item):
        """Count each referenced object once within this row."""
        if id(item) in seen_ids:
            return 0
        seen_ids.add(id(item))
        total = sys.getsizeof(item)
        if isinstance(item, str):
            total += len(item.encode("utf-8"))
        elif isinstance(item, dict):
            total += sum(size(key) + size(child) for key, child in item.items())
        elif isinstance(item, (list, tuple, set, frozenset)):
            total += sum(size(child) for child in item)
        return total

    return size(value)


def raw_batches(rows):
    """Yield complete rows within the fixed count and resident/conversion bounds."""
    batch, size = [], 0
    for row in rows:
        amount = memory_bytes(row) + 16
        if amount > RAW_BYTES:
            raise NpiBatchError("NPI isolated CSV row exceeds the parser byte budget")
        if batch and (len(batch) == RAW_ROWS or size + amount > RAW_BYTES):
            yield batch
            batch, size = [], 0
        batch.append(row)
        size += amount
    if batch:
        yield batch


def current():
    """Return the writer owned by the current isolated parser task."""
    return _CURRENT.get()


def _q(value):
    if not isinstance(value, str) or not value.isascii() or not value.replace("_", "a").isalnum():
        raise NpiBatchError("NPI isolated native identifier differs")
    return '"' + value + '"'


async def _settle_cleanup(awaitable):
    task = asyncio.create_task(awaitable)
    has_cancellation = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            has_cancellation = True
    task.result()
    if has_cancellation:
        raise asyncio.CancelledError


@asynccontextmanager
async def bind(database, models, cancel_check):
    """Reuse an owning writer or drain and clean up one complete isolated scope."""
    existing = current()
    if existing is not None:
        yield existing
        return
    writer = NativeWriteWindow(database, models, cancel_check)
    token = _CURRENT.set(writer)
    error = None
    try:
        yield writer
        await writer.finish_window()
    except BaseException as exc:
        error = exc
        raise
    finally:
        try:
            await writer.close(error)
        finally:
            _CURRENT.reset(token)


class NativeWriteWindow:
    """COPY bounded buffers, deduplicate once, then merge into private heaps."""

    def __init__(self, database, models, cancel_check):
        self.database, self.models, self.cancel_check = database, models, cancel_check
        self.manager = self.connection = None
        self.transaction = self.savepoint = None
        self.families = {}
        self.relations = []
        self.names = []
        self.ordinal = 0
        self.peak_temporary_bytes = 0
        self.prefix = "npi_buf_" + uuid4().hex[:12]

    async def _open(self):
        if self.connection is None:
            self.manager = self.database.acquire()
            proxy = await self.manager.__aenter__()
            self.connection = await _copy_driver(proxy if proxy.raw_connection is not None else proxy._connection)
            if not callable(getattr(self.connection, "copy_records_to_table", None)):
                raise NpiBatchError("NPI isolated writer requires native COPY")
            if proxy.raw_connection is None and not self.connection.is_in_transaction():
                await proxy.scalar("SELECT 1")
                if not self.connection.is_in_transaction():
                    raise NpiBatchError("NPI isolated borrowed transaction is not active")
            if self.connection.is_in_transaction():
                self.savepoint = self.prefix + "_save"
                await self.connection.execute(f"SAVEPOINT {_q(self.savepoint)}")
            else:
                self.transaction = self.connection.transaction()
                await self.transaction.start()

    async def _track(self, name):
        oid = await self.connection.fetchval("SELECT pg_catalog.to_regclass($1)::oid", "pg_temp." + name)
        if type(oid) is not int or oid <= 0:
            raise NpiBatchError("NPI isolated temporary relation identity differs")
        self.relations.append(oid)
        self.names.append(name)

    async def _reserve(self, incoming=0):
        observed = int(
            await self.connection.fetchval(
                "SELECT coalesce(sum(pg_catalog.pg_total_relation_size(oid)),0)::bigint FROM pg_catalog.pg_class WHERE oid=ANY($1::oid[])",
                self.relations,
            )
        )
        self.peak_temporary_bytes = max(self.peak_temporary_bytes, observed)
        # Reserve two full native sort/join work copies and conversion/index growth.
        if 3 * observed + 8 * incoming > WINDOW_BYTES:
            raise NpiBatchError("NPI isolated logical window exceeds its temporary disk budget")

    async def _family(self, kind):
        if kind not in self.families:
            model, rewrite = self.models(kind)
            columns = tuple(column.name for column in model.__table__.columns)
            natural_keys = tuple(model.__my_index_elements__)
            keys = (
                ("checksum",)
                if kind in {"npi_taxonomy_list", "npi_other_id_list", "npi_taxonomy_group_list"}
                else natural_keys
            )
            target = _q(model.__table__.schema) + "." + _q(model.__tablename__)
            stem = self.prefix + "_" + str(len(self.families))
            incoming, window = stem + "_in", stem + "_win"
            for name in (incoming, window):
                await self.connection.execute(
                    f"CREATE TEMP TABLE {_q(name)} (LIKE {target} INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                await self.connection.execute(f"ALTER TABLE {_q(name)} ADD COLUMN _npi_ordinal bigint NOT NULL")
                await self._track(name)
            await self.connection.execute(f"ALTER TABLE {_q(window)} ADD PRIMARY KEY ({','.join(map(_q, keys))})")
            self.families[kind] = (target, columns, natural_keys, keys, rewrite, incoming, window)
        return self.families[kind]

    async def add(self, payload):
        """COPY bounded normalized buffers into the current logical dedup window."""
        await self.cancel_check()
        if not any(payload.values()):
            return
        await self._open()
        for kind, rows in payload.items():
            if not rows:
                continue
            family = await self._family(kind)
            batch, size, fields = [], 0, None
            for row in rows:
                amount = memory_bytes(row)
                if amount > COPY_BYTES:
                    raise NpiBatchError("NPI isolated normalized row exceeds the COPY byte budget")
                if batch and (len(batch) == COPY_ROWS or size + amount > COPY_BYTES or tuple(row) != fields):
                    await self._copy(family, batch, fields, size)
                    batch, size = [], 0
                fields = tuple(row)
                if set(fields) - set(family[1]):
                    raise NpiBatchError("NPI isolated normalized columns differ")
                batch.append(row)
                size += amount
            if batch:
                await self._copy(family, batch, fields, size)

    async def _copy(self, family, rows, fields, size):
        await self.cancel_check()
        await self._reserve(size)
        _target, columns, _natural, keys, _rewrite, incoming, window = family

        async def records():
            """Convert one native COPY record at a time without another row array."""
            for row in rows:
                self.ordinal += 1
                yield (*[row[field] for field in fields], self.ordinal)

        await self.connection.copy_records_to_table(
            incoming, schema_name="pg_temp", columns=(*fields, "_npi_ordinal"), records=records()
        )
        await self.cancel_check()
        column_sql, key_sql = ",".join(map(_q, columns)), ",".join(map(_q, keys))
        updates = ",".join(f"{_q(column)}=excluded.{_q(column)}" for column in columns)
        await self.connection.execute(
            f"INSERT INTO {_q(window)} ({column_sql},_npi_ordinal) "
            f"SELECT DISTINCT ON ({key_sql}) {column_sql},min(_npi_ordinal) OVER(PARTITION BY {key_sql}) "
            f"FROM {_q(incoming)} ORDER BY {key_sql},_npi_ordinal DESC "
            f"ON CONFLICT ({key_sql}) DO UPDATE SET {updates}"
        )
        await self.connection.execute(f"TRUNCATE {_q(incoming)}")
        await self._reserve()

    async def finish_window(self):
        """Apply original family winners and close the temporary native window."""
        if self.connection is None:
            return
        await self.cancel_check()
        await self._reserve()
        for target, columns, natural_keys, _keys, rewrite, _incoming, window in self.families.values():
            matches = " AND ".join(f"o.{_q(key)}=w.{_q(key)}" for key in natural_keys)
            if rewrite:
                await self.connection.execute(
                    f"DELETE FROM {target} o WHERE EXISTS(SELECT 1 FROM {_q(window)} w WHERE {matches})"
                )
                condition = ""
                order = ",".join(f'w.{_q(key)}::text COLLATE "C" NULLS LAST' for key in natural_keys)
            else:
                condition = f"WHERE NOT EXISTS(SELECT 1 FROM {target} o WHERE {matches})"
                order = "w._npi_ordinal"
            fields = ",".join(map(_q, columns))
            await self.connection.execute(
                f"INSERT INTO {target} ({fields}) SELECT {','.join('w.' + _q(column) for column in columns)} "
                f"FROM {_q(window)} w {condition} ORDER BY {order}"
            )
            await self.cancel_check()
        await self.close()

    async def delete_selected(self, source_rows, cleanup_targets):
        """Keep weekly replacement's exact family/type filters without an NPI set in RAM."""
        if self.families:
            raise NpiBatchError("NPI isolated cleanup overlaps a logical write window")
        await self._open()
        incoming, selected = self.prefix + "_keys_in", self.prefix + "_keys"
        for name, key in ((incoming, ""), (selected, " PRIMARY KEY")):
            await self.connection.execute(f"CREATE TEMP TABLE {_q(name)} (npi bigint{key}) ON COMMIT DROP")
            await self._track(name)

        async def append(keys):
            """COPY one bounded weekly key buffer into the native unique keyset."""
            await self.cancel_check()
            await self._reserve(memory_bytes(keys))
            await self.connection.copy_records_to_table(
                incoming, schema_name="pg_temp", columns=("npi",), records=((value,) for value in keys)
            )
            await self.connection.execute(
                f"INSERT INTO {_q(selected)} SELECT DISTINCT npi FROM {_q(incoming)} WHERE true ON CONFLICT DO NOTHING"
            )
            await self.connection.execute(f"TRUNCATE {_q(incoming)}")

        keys = set()
        async for source_row in source_rows:
            if memory_bytes(source_row) + 16 > RAW_BYTES:
                raise NpiBatchError("NPI isolated weekly CSV row exceeds the parser byte budget")
            if source_row.get("NPI"):
                keys.add(int(source_row["NPI"]))
            if len(keys) == RAW_ROWS:
                await append(keys)
                keys.clear()
        if keys:
            await append(keys)
        await self._reserve()
        count = await self.connection.fetchval(f"SELECT count(*) FROM {_q(selected)}")
        for model, address_types in cleanup_targets:
            await self.cancel_check()
            target_relation = _q(model.__table__.schema) + "." + _q(model.__tablename__)
            condition = " AND type=ANY($1::text[])" if address_types else ""
            await self.connection.execute(
                f"DELETE FROM {target_relation} o WHERE EXISTS(SELECT 1 FROM {_q(selected)} s WHERE s.npi=o.npi){condition}",
                *([list(address_types)] if address_types else []),
            )
        await self.close()
        return count

    async def close(self, error=None):
        """Drain native cleanup while preserving a borrowed caller transaction."""
        manager, self.manager = self.manager, None
        connection, self.connection = self.connection, None
        transaction, self.transaction = self.transaction, None
        savepoint, self.savepoint = self.savepoint, None
        names, self.names = self.names, []
        self.families.clear()
        self.relations.clear()
        self.ordinal = 0
        if manager is not None:

            async def cleanup():
                """Drop exact temporary objects or roll back this writer's own scope."""
                try:
                    if savepoint is not None:
                        if error is not None:
                            await connection.execute(f"ROLLBACK TO SAVEPOINT {_q(savepoint)}")
                        else:
                            for name in reversed(names):
                                await connection.execute(f"DROP TABLE pg_temp.{_q(name)}")
                        await connection.execute(f"RELEASE SAVEPOINT {_q(savepoint)}")
                    elif transaction is not None:
                        if error is not None:
                            await transaction.rollback()
                        else:
                            await transaction.commit()
                finally:
                    await manager.__aexit__(
                        type(error) if error is not None else None, error, getattr(error, "__traceback__", None)
                    )

            await _settle_cleanup(cleanup())
